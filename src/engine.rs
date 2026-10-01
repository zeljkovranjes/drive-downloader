//! Transfer engines.
//!
//! S3 pipeline (all stages run concurrently):
//!
//!   listers (16 tasks, one per prefix) ──FileJob──▶ planner ──Chunk──▶ N workers ──▶ disk
//!
//! * Listing fans out over "folders" in parallel, and downloads start as soon as
//!   the first key is found.
//! * Small objects are one GET. Large objects are split into byte ranges that
//!   go into the same queue, so every connection stays busy whatever the mix
//!   of file sizes is.
//! * Range chunks are written straight to their offset in a pre-sized sparse
//!   file (no reassembly, no temp chunk files).
//! * A controller hill-climbs the number of active connections on measured
//!   throughput and backs off hard on SlowDown/503.
//!
//! Local/mapped drives use Win32 CopyFileExW from a pool of threads, gated by
//! the same controller.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64, AtomicUsize};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use aws_sdk_s3::Client;

use crate::s3::err_text;
use crate::util::{key_to_path, last_segment, mtime_close, sanitize_segment, write_all_at};

pub const MAX_CONNECTIONS: usize = 128;
pub const MAX_LOCAL_THREADS: usize = 32;
/// Objects up to this size are fetched with a single GET.
const SMALL_OBJECT: u64 = 8 << 20;
const RETRIES: u32 = 4;
const WRITE_BUF: usize = 1 << 20;
const LISTERS: usize = 16;
const LOCAL_SCANNERS: usize = 8;

static WORKER_IDS: AtomicU64 = AtomicU64::new(0);

/// Concurrency limit for one engine. Workers with index >= target sit idle.
pub struct Gate {
    pub name: &'static str,
    pub target: AtomicUsize,
    pub min: usize,
    pub max: usize,
    pub auto: bool,
    throttled: AtomicBool,
}

impl Gate {
    pub fn new(name: &'static str, initial: usize, max: usize, auto: bool) -> Arc<Self> {
        Arc::new(Self {
            name,
            target: AtomicUsize::new(initial.clamp(1, max)),
            min: if auto { 2.min(max) } else { 1 },
            max,
            auto,
            throttled: AtomicBool::new(false),
        })
    }
}

pub struct Stats {
    pub total_bytes: AtomicU64,
    pub total_files: AtomicU64,
    pub done_bytes: AtomicU64,
    pub done_files: AtomicU64,
    pub skipped: AtomicU64,
    pub failed: AtomicU64,
    scanners: AtomicU32,
    engines: AtomicU32,
    cancel: AtomicBool,
    cancel_flag: AtomicI32,
    active: Mutex<HashMap<u64, String>>,
    log: Mutex<Vec<String>>,
    pub started: Instant,
    elapsed_ms: AtomicU64,
    pub gates: Mutex<Vec<Arc<Gate>>>,
}

impl Stats {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            total_bytes: AtomicU64::new(0),
            total_files: AtomicU64::new(0),
            done_bytes: AtomicU64::new(0),
            done_files: AtomicU64::new(0),
            skipped: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            scanners: AtomicU32::new(0),
            engines: AtomicU32::new(0),
            cancel: AtomicBool::new(false),
            cancel_flag: AtomicI32::new(0),
            active: Mutex::new(HashMap::new()),
            log: Mutex::new(Vec::new()),
            started: Instant::now(),
            elapsed_ms: AtomicU64::new(0),
            gates: Mutex::new(Vec::new()),
        })
    }

    pub fn cancel(&self) {
        self.cancel.store(true, Relaxed);
        self.cancel_flag.store(1, Relaxed);
    }
    pub fn is_cancelled(&self) -> bool {
        self.cancel.load(Relaxed)
    }
    pub fn finished(&self) -> bool {
        self.engines.load(Relaxed) == 0
    }
    pub fn scanning(&self) -> bool {
        self.scanners.load(Relaxed) > 0
    }
    pub fn elapsed(&self) -> Duration {
        if self.finished() {
            Duration::from_millis(self.elapsed_ms.load(Relaxed))
        } else {
            self.started.elapsed()
        }
    }
    pub fn log(&self, msg: impl Into<String>) {
        self.log.lock().unwrap().push(msg.into());
    }
    pub fn drain_log(&self) -> Vec<String> {
        std::mem::take(&mut *self.log.lock().unwrap())
    }
    pub fn active_names(&self) -> Vec<String> {
        self.active.lock().unwrap().values().cloned().collect()
    }
    fn set_active(&self, wid: u64, name: String) {
        self.active.lock().unwrap().insert(wid, name);
    }
    fn clear_active(&self, wid: u64) {
        self.active.lock().unwrap().remove(&wid);
    }
    fn fail(&self, what: impl std::fmt::Display, err: impl std::fmt::Display) {
        self.failed.fetch_add(1, Relaxed);
        self.log(format!("FAILED  {what}  ({err})"));
    }
    fn add_total(&self, size: u64) {
        self.total_files.fetch_add(1, Relaxed);
        self.total_bytes.fetch_add(size, Relaxed);
    }
    fn mark_skipped(&self, size: u64) {
        self.done_bytes.fetch_add(size, Relaxed);
        self.done_files.fetch_add(1, Relaxed);
        self.skipped.fetch_add(1, Relaxed);
    }
    fn begin_engine(self: &Arc<Self>, gate: &Arc<Gate>) {
        self.engines.fetch_add(1, Relaxed);
        self.scanners.fetch_add(1, Relaxed);
        self.gates.lock().unwrap().push(gate.clone());
        spawn_controller(self.clone(), gate.clone());
    }
    fn engine_done(&self) {
        let ms = self.started.elapsed().as_millis() as u64;
        self.elapsed_ms.store(ms, Relaxed);
        self.engines.fetch_sub(1, Relaxed);
    }
}

/// Hill-climbing concurrency control: keep adding connections while that
/// buys throughput, back off while removing them costs little, halve on throttling.
fn spawn_controller(stats: Arc<Stats>, gate: Arc<Gate>) {
    std::thread::spawn(move || {
        let period = Duration::from_millis(1500);
        let mut last_b = stats.done_bytes.load(Relaxed);
        let mut last_t = Instant::now();
        let mut prev_tp = 0.0f64;
        let mut dir: i64 = 1;
        while !stats.finished() {
            std::thread::sleep(period);
            let now = Instant::now();
            let b = stats.done_bytes.load(Relaxed);
            let tp = b.saturating_sub(last_b) as f64 / (now - last_t).as_secs_f64();
            last_b = b;
            last_t = now;
            if !gate.auto {
                continue;
            }
            let cur = gate.target.load(Relaxed) as i64;
            if gate.throttled.swap(false, Relaxed) {
                gate.target.store(((cur / 2) as usize).max(gate.min), Relaxed);
                dir = -1;
                prev_tp = 0.0;
                continue;
            }
            if tp <= 0.0 {
                continue; // still listing, or idle
            }
            if prev_tp > 0.0 {
                if dir > 0 && tp < prev_tp * 1.03 {
                    dir = -1; // more connections didn't help
                } else if dir < 0 && tp < prev_tp * 0.97 {
                    dir = 1; // fewer connections hurt
                }
            }
            let step = (cur / 6).max(1);
            let next = (cur + dir * step).clamp(gate.min as i64, gate.max as i64);
            gate.target.store(next as usize, Relaxed);
            prev_tp = tp;
        }
    });
}

fn is_identical(dst: &Path, size: u64, mtime: Option<SystemTime>) -> bool {
    match fs::metadata(dst) {
        Ok(m) => {
            m.len() == size
                && match (mtime, m.modified().ok()) {
                    (Some(a), Some(b)) => mtime_close(a, b),
                    _ => false,
                }
        }
        Err(_) => false,
    }
}

fn part_path(dst: &Path) -> PathBuf {
    let mut s = dst.as_os_str().to_os_string();
    s.push(".part");
    PathBuf::from(s)
}

/// Mark a file sparse so writing a chunk far into it doesn't make NTFS
/// zero-fill everything before that offset first.
#[cfg(windows)]
fn make_sparse(f: &File) {
    use std::ffi::c_void;
    use std::os::windows::io::AsRawHandle;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn DeviceIoControl(h: *mut c_void, code: u32, inb: *mut c_void, insz: u32, outb: *mut c_void,
            outsz: u32, ret: *mut u32, ov: *mut c_void) -> i32;
    }
    const FSCTL_SET_SPARSE: u32 = 0x0009_00C4;
    let mut ret = 0u32;
    unsafe {
        DeviceIoControl(f.as_raw_handle() as *mut c_void, FSCTL_SET_SPARSE, std::ptr::null_mut(), 0,
            std::ptr::null_mut(), 0, &mut ret, std::ptr::null_mut());
    }
}
#[cfg(not(windows))]
fn make_sparse(_f: &File) {}

fn chunk_size_for(size: u64) -> u64 {
    (size / 64).clamp(8 << 20, 64 << 20)
}

// ============================================================================ local / mapped drives

struct LocalTask {
    src: PathBuf,
    dst: PathBuf,
    size: u64,
    mtime: Option<SystemTime>,
}

pub fn start_local(stats: &Arc<Stats>, items: Vec<(PathBuf, bool)>, dest: PathBuf, skip: bool, gate: Arc<Gate>) {
    stats.begin_engine(&gate);
    let stats = stats.clone();
    std::thread::spawn(move || {
        let (tx, rx) = crossbeam_channel::unbounded::<LocalTask>();
        let feed_done = Arc::new(AtomicBool::new(false));
        let workers: Vec<_> = (0..gate.max)
            .map(|id| {
                let (rx, stats, gate, feed_done) = (rx.clone(), stats.clone(), gate.clone(), feed_done.clone());
                std::thread::spawn(move || local_worker(id, rx, &stats, &gate, &feed_done, skip))
            })
            .collect();
        drop(rx);
        scan_local(&stats, &items, &dest, &tx);
        stats.scanners.fetch_sub(1, Relaxed);
        drop(tx);
        feed_done.store(true, Relaxed);
        for w in workers {
            let _ = w.join();
        }
        stats.engine_done();
    });
}

fn queue_local(stats: &Stats, tx: &crossbeam_channel::Sender<LocalTask>, src: PathBuf, dst: PathBuf, m: &fs::Metadata) {
    stats.add_total(m.len());
    let _ = tx.send(LocalTask { src, dst, size: m.len(), mtime: m.modified().ok() });
}

/// Parallel directory walk (directory listing is latency-bound on network drives).
fn scan_local(stats: &Stats, items: &[(PathBuf, bool)], dest: &Path, tx: &crossbeam_channel::Sender<LocalTask>) {
    let (dtx, drx) = crossbeam_channel::unbounded::<(PathBuf, PathBuf)>();
    let pending = AtomicUsize::new(0);
    for (src, is_dir) in items {
        let name = src.file_name().map(|n| n.to_os_string()).unwrap_or_else(|| "drive".into());
        let dst = dest.join(name);
        if *is_dir {
            pending.fetch_add(1, Relaxed);
            let _ = dtx.send((src.clone(), dst));
        } else {
            match fs::metadata(src) {
                Ok(m) => queue_local(stats, tx, src.clone(), dst, &m),
                Err(e) => stats.fail(src.display(), e),
            }
        }
    }
    std::thread::scope(|s| {
        for _ in 0..LOCAL_SCANNERS {
            s.spawn(|| loop {
                match drx.recv_timeout(Duration::from_millis(20)) {
                    Ok((sd, dd)) => {
                        if !stats.is_cancelled() {
                            scan_local_dir(stats, &sd, &dd, tx, &dtx, &pending);
                        }
                        pending.fetch_sub(1, Relaxed);
                    }
                    Err(_) => {
                        if pending.load(Relaxed) == 0 {
                            break;
                        }
                    }
                }
            });
        }
    });
}

fn scan_local_dir(
    stats: &Stats,
    sd: &Path,
    dd: &Path,
    tx: &crossbeam_channel::Sender<LocalTask>,
    dtx: &crossbeam_channel::Sender<(PathBuf, PathBuf)>,
    pending: &AtomicUsize,
) {
    if let Err(e) = fs::create_dir_all(dd) {
        return stats.fail(dd.display(), e);
    }
    let rd = match fs::read_dir(sd) {
        Ok(rd) => rd,
        Err(e) => return stats.fail(sd.display(), e),
    };
    for ent in rd {
        let ent = match ent {
            Ok(e) => e,
            Err(e) => {
                stats.fail(sd.display(), e);
                continue;
            }
        };
        let (s, d) = (ent.path(), dd.join(ent.file_name()));
        if ent.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            pending.fetch_add(1, Relaxed);
            let _ = dtx.send((s, d));
        } else {
            // On Windows this comes from the directory listing itself - no extra round trip.
            match ent.metadata() {
                Ok(m) => queue_local(stats, tx, s, d, &m),
                Err(e) => stats.fail(s.display(), e),
            }
        }
    }
}

fn local_worker(
    id: usize,
    rx: crossbeam_channel::Receiver<LocalTask>,
    stats: &Stats,
    gate: &Gate,
    feed_done: &AtomicBool,
    skip: bool,
) {
    let wid = WORKER_IDS.fetch_add(1, Relaxed);
    loop {
        // Parked above the concurrency limit; leave once the queue is drained for good.
        while id >= gate.target.load(Relaxed) && !stats.is_cancelled() {
            if feed_done.load(Relaxed) && rx.is_empty() {
                return;
            }
            std::thread::sleep(Duration::from_millis(30));
        }
        let Ok(t) = rx.recv() else { break };
        if stats.is_cancelled() {
            continue;
        }
        if skip && is_identical(&t.dst, t.size, t.mtime) {
            stats.mark_skipped(t.size);
            continue;
        }
        stats.set_active(wid, t.src.display().to_string());
        let r = copy_local_retry(stats, &t);
        stats.clear_active(wid);
        match r {
            Ok(()) => {
                stats.done_files.fetch_add(1, Relaxed);
            }
            Err(e) if !stats.is_cancelled() => stats.fail(t.src.display(), e),
            Err(_) => {}
        }
    }
}

fn copy_local_retry(stats: &Stats, t: &LocalTask) -> std::io::Result<()> {
    let mut last_err = None;
    for attempt in 0..RETRIES {
        if stats.is_cancelled() {
            return Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "cancelled"));
        }
        let mut added = 0u64;
        match copy_local_one(stats, t, attempt == 0, &mut added) {
            Ok(()) => return Ok(()),
            Err(e) => {
                stats.done_bytes.fetch_sub(added, Relaxed);
                if stats.is_cancelled() {
                    return Err(e);
                }
                if e.raw_os_error() == Some(5) {
                    // An old read-only copy is in the way.
                    if let Ok(m) = fs::metadata(&t.dst) {
                        let mut p = m.permissions();
                        #[allow(clippy::permissions_set_readonly_false)]
                        p.set_readonly(false);
                        let _ = fs::set_permissions(&t.dst, p);
                    }
                }
                last_err = Some(e);
                std::thread::sleep(Duration::from_millis(300 * (attempt as u64 + 1)));
            }
        }
    }
    Err(last_err.unwrap())
}

#[cfg(windows)]
fn verbatim(p: &Path) -> Vec<u16> {
    use std::os::windows::ffi::OsStrExt;
    let s: Vec<u16> = p.as_os_str().encode_wide().collect();
    let txt = String::from_utf16_lossy(&s);
    let mut out: Vec<u16> = if txt.starts_with(r"\\?\") {
        s
    } else if let Some(rest) = txt.strip_prefix(r"\\") {
        format!(r"\\?\UNC\{rest}").encode_utf16().collect()
    } else {
        r"\\?\".encode_utf16().chain(s).collect()
    };
    out.push(0);
    out
}

#[cfg(windows)]
fn copy_local_one(stats: &Stats, t: &LocalTask, allow_unbuffered: bool, added: &mut u64) -> std::io::Result<()> {
    use std::ffi::c_void;

    struct Ctx {
        stats: *const Stats,
        last: i64,
    }
    unsafe extern "system" fn progress(
        _total: i64, transferred: i64, _ss: i64, _st: i64, _n: u32, _r: u32,
        _hs: *mut c_void, _hd: *mut c_void, data: *mut c_void,
    ) -> u32 {
        let ctx = unsafe { &mut *(data as *mut Ctx) };
        let stats = unsafe { &*ctx.stats };
        let d = transferred - ctx.last;
        if d > 0 {
            ctx.last = transferred;
            stats.done_bytes.fetch_add(d as u64, Relaxed);
        }
        if stats.is_cancelled() { 1 } else { 0 } // PROGRESS_CANCEL / PROGRESS_CONTINUE
    }

    const COPY_FILE_ALLOW_DECRYPTED_DESTINATION: u32 = 0x8;
    const COPY_FILE_NO_BUFFERING: u32 = 0x1000;
    let mut flags = COPY_FILE_ALLOW_DECRYPTED_DESTINATION;
    if allow_unbuffered && t.size >= 128 << 20 {
        flags |= COPY_FILE_NO_BUFFERING;
    }
    let mut ctx = Ctx { stats, last: 0 };
    let (src, dst) = (verbatim(&t.src), verbatim(&t.dst));
    let ok = unsafe {
        crate::drives::CopyFileExW(src.as_ptr(), dst.as_ptr(), Some(progress),
            &mut ctx as *mut Ctx as *mut c_void, stats.cancel_flag.as_ptr(), flags)
    };
    *added = ctx.last.max(0) as u64;
    if ok != 0 { Ok(()) } else { Err(std::io::Error::last_os_error()) }
}

#[cfg(not(windows))]
fn copy_local_one(stats: &Stats, t: &LocalTask, _unbuffered: bool, added: &mut u64) -> std::io::Result<()> {
    let n = fs::copy(&t.src, &t.dst)?;
    stats.done_bytes.fetch_add(n, Relaxed);
    *added = n;
    Ok(())
}

// ============================================================================ S3

pub struct S3Item {
    pub bucket: String,
    pub key: String,
    pub is_dir: bool,
    pub size: u64,
    pub mtime: Option<SystemTime>,
}

struct FileJob {
    bucket: String,
    key: String,
    dst: PathBuf,
    size: u64,
    mtime: Option<SystemTime>,
}

struct BigFile {
    job: FileJob,
    tmp: PathBuf,
    handle: File,
    remaining: AtomicUsize,
    failed: AtomicBool,
}

enum Chunk {
    Whole(FileJob),
    Part { file: Arc<BigFile>, start: u64, end: u64 },
}

struct ListTask {
    bucket: String,
    prefix: String,
    root: String,
    base: PathBuf,
}

pub fn start_s3(
    rt: &tokio::runtime::Handle,
    stats: &Arc<Stats>,
    client: Client,
    items: Vec<S3Item>,
    dest: PathBuf,
    skip: bool,
    gate: Arc<Gate>,
) {
    stats.begin_engine(&gate);
    let stats = stats.clone();
    rt.spawn(async move {
        let (ftx, frx) = async_channel::unbounded::<FileJob>();
        let (ctx, crx) = async_channel::bounded::<Chunk>(4096);
        let mut workers = tokio::task::JoinSet::new();
        for id in 0..gate.max {
            workers.spawn(s3_worker(id, crx.clone(), client.clone(), stats.clone(), gate.clone(), skip));
        }
        drop(crx);
        let planner = tokio::spawn(planner(frx, ctx, stats.clone(), skip));
        list_all(&client, items, &dest, ftx, &stats).await;
        stats.scanners.fetch_sub(1, Relaxed);
        let _ = planner.await;
        while workers.join_next().await.is_some() {}
        stats.engine_done();
    });
}

async fn list_all(client: &Client, items: Vec<S3Item>, dest: &Path, ftx: async_channel::Sender<FileJob>, stats: &Arc<Stats>) {
    let (ptx, prx) = async_channel::unbounded::<ListTask>();
    let pending = Arc::new(AtomicUsize::new(0));
    for it in items {
        if !it.is_dir {
            let name = sanitize_segment(last_segment(&it.key)).unwrap_or_else(|| "_".into());
            stats.add_total(it.size);
            let _ = ftx
                .send(FileJob { bucket: it.bucket, key: it.key, dst: dest.join(name), size: it.size, mtime: it.mtime })
                .await;
        } else {
            let name = if it.key.is_empty() { it.bucket.as_str() } else { last_segment(&it.key) };
            let base = dest.join(sanitize_segment(name).unwrap_or_else(|| "_".into()));
            let _ = fs::create_dir_all(&base);
            pending.fetch_add(1, Relaxed);
            let _ = ptx.send(ListTask { bucket: it.bucket, prefix: it.key.clone(), root: it.key, base }).await;
        }
    }
    let mut set = tokio::task::JoinSet::new();
    for _ in 0..LISTERS {
        let (client, prx, ptx, ftx, pending, stats) =
            (client.clone(), prx.clone(), ptx.clone(), ftx.clone(), pending.clone(), stats.clone());
        set.spawn(async move {
            loop {
                match tokio::time::timeout(Duration::from_millis(20), prx.recv()).await {
                    Ok(Ok(task)) => {
                        if !stats.is_cancelled() {
                            list_prefix(&client, &task, &ptx, &ftx, &pending, &stats).await;
                        }
                        pending.fetch_sub(1, Relaxed);
                    }
                    Ok(Err(_)) => break,
                    Err(_) => {
                        if pending.load(Relaxed) == 0 {
                            break;
                        }
                    }
                }
            }
        });
    }
    drop(ftx);
    while set.join_next().await.is_some() {}
}

async fn list_prefix(
    client: &Client,
    t: &ListTask,
    ptx: &async_channel::Sender<ListTask>,
    ftx: &async_channel::Sender<FileJob>,
    pending: &AtomicUsize,
    stats: &Stats,
) {
    let mut pages = client
        .list_objects_v2()
        .bucket(&t.bucket)
        .prefix(&t.prefix)
        .delimiter("/")
        .into_paginator()
        .send();
    while let Some(page) = pages.next().await {
        if stats.is_cancelled() {
            return;
        }
        let page = match page {
            Ok(p) => p,
            Err(e) => return stats.fail(format!("s3://{}/{}", t.bucket, t.prefix), err_text(e)),
        };
        for cp in page.common_prefixes() {
            let Some(p) = cp.prefix() else { continue };
            let rel = p.strip_prefix(t.root.as_str()).unwrap_or(p);
            let _ = fs::create_dir_all(key_to_path(&t.base, rel)); // keeps empty folders
            pending.fetch_add(1, Relaxed);
            let _ = ptx
                .send(ListTask { bucket: t.bucket.clone(), prefix: p.to_string(), root: t.root.clone(), base: t.base.clone() })
                .await;
        }
        for o in page.contents() {
            let Some(key) = o.key() else { continue };
            if key.ends_with('/') {
                continue; // folder marker
            }
            let rel = key.strip_prefix(t.root.as_str()).unwrap_or(key);
            let dst = key_to_path(&t.base, rel);
            if dst == t.base {
                continue;
            }
            let size = o.size().unwrap_or(0).max(0) as u64;
            let mtime = o.last_modified().and_then(|d| crate::util::unix_to_systime(d.secs(), d.subsec_nanos()));
            stats.add_total(size);
            let _ = ftx
                .send(FileJob { bucket: t.bucket.clone(), key: key.to_string(), dst, size, mtime })
                .await;
        }
    }
}

async fn planner(frx: async_channel::Receiver<FileJob>, ctx: async_channel::Sender<Chunk>, stats: Arc<Stats>, skip: bool) {
    let mut made: HashSet<PathBuf> = HashSet::new();
    while let Ok(job) = frx.recv().await {
        if stats.is_cancelled() {
            break;
        }
        if let Some(parent) = job.dst.parent() {
            if made.insert(parent.to_path_buf()) {
                if let Err(e) = fs::create_dir_all(parent) {
                    stats.fail(parent.display(), e);
                    continue;
                }
            }
        }
        if job.size <= SMALL_OBJECT {
            // Skip check + file creation happen in the workers, in parallel.
            if ctx.send(Chunk::Whole(job)).await.is_err() {
                break;
            }
            continue;
        }
        if skip && is_identical(&job.dst, job.size, job.mtime) {
            stats.mark_skipped(job.size);
            continue;
        }
        let tmp = part_path(&job.dst);
        let handle = match File::create(&tmp) {
            Ok(f) => {
                make_sparse(&f);
                match f.set_len(job.size) {
                    Ok(()) => f,
                    Err(e) => {
                        stats.fail(job.dst.display(), e);
                        continue;
                    }
                }
            }
            Err(e) => {
                stats.fail(job.dst.display(), e);
                continue;
            }
        };
        let size = job.size;
        let chunk = chunk_size_for(size);
        let n = size.div_ceil(chunk);
        let file = Arc::new(BigFile { job, tmp, handle, remaining: AtomicUsize::new(n as usize), failed: AtomicBool::new(false) });
        for i in 0..n {
            let start = i * chunk;
            let end = (start + chunk).min(size);
            if ctx.send(Chunk::Part { file: file.clone(), start, end }).await.is_err() {
                return;
            }
        }
    }
}

async fn s3_worker(
    id: usize,
    crx: async_channel::Receiver<Chunk>,
    client: Client,
    stats: Arc<Stats>,
    gate: Arc<Gate>,
    skip: bool,
) {
    let wid = WORKER_IDS.fetch_add(1, Relaxed);
    loop {
        while id >= gate.target.load(Relaxed) && !stats.is_cancelled() {
            if crx.is_closed() && crx.is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        let Ok(chunk) = crx.recv().await else { break };
        if stats.is_cancelled() {
            continue;
        }
        match chunk {
            Chunk::Whole(job) => {
                if skip && is_identical(&job.dst, job.size, job.mtime) {
                    stats.mark_skipped(job.size);
                    continue;
                }
                stats.set_active(wid, job.key.clone());
                let r = whole_with_retry(&client, &job, &stats, &gate).await;
                stats.clear_active(wid);
                match r {
                    Ok(()) => {
                        stats.done_files.fetch_add(1, Relaxed);
                    }
                    Err(e) => {
                        let _ = fs::remove_file(part_path(&job.dst));
                        if !stats.is_cancelled() {
                            stats.fail(&job.key, e);
                        }
                    }
                }
            }
            Chunk::Part { file, start, end } => {
                if file.failed.load(Relaxed) {
                    continue;
                }
                let pct = start * 100 / file.job.size.max(1);
                stats.set_active(wid, format!("{} [{pct}%]", file.job.key));
                let r = part_with_retry(&client, &file, start, end, &stats, &gate).await;
                stats.clear_active(wid);
                match r {
                    Ok(()) => {
                        if file.remaining.fetch_sub(1, Relaxed) == 1 {
                            match finalize(&file) {
                                Ok(()) => {
                                    stats.done_files.fetch_add(1, Relaxed);
                                }
                                Err(e) => stats.fail(&file.job.key, e),
                            }
                        }
                    }
                    Err(e) => {
                        if !stats.is_cancelled() && !file.failed.swap(true, Relaxed) {
                            stats.fail(&file.job.key, e);
                        }
                    }
                }
            }
        }
    }
}

fn finalize(file: &BigFile) -> std::io::Result<()> {
    if let Some(t) = file.job.mtime {
        let _ = file.handle.set_modified(t);
    }
    fs::rename(&file.tmp, &file.job.dst)
}

/// GET (optionally ranged) and stream the body to `f` at `base_off`.
async fn fetch_into(
    client: &Client,
    job: &FileJob,
    range: Option<(u64, u64)>,
    f: &File,
    stats: &Stats,
    gate: &Gate,
    added: &mut u64,
) -> Result<u64, String> {
    let mut req = client.get_object().bucket(&job.bucket).key(&job.key);
    let base_off = range.map(|r| r.0).unwrap_or(0);
    if let Some((s, e)) = range {
        req = req.range(format!("bytes={}-{}", s, e - 1));
    }
    let resp = req.send().await.map_err(|e| {
        let t = err_text(e);
        if t.contains("SlowDown") || t.contains("503") || t.contains("TooManyRequests") {
            gate.throttled.store(true, Relaxed);
        }
        t
    })?;
    let mut body = resp.body;
    let mut off = base_off;
    let mut buf: Vec<u8> = Vec::with_capacity(WRITE_BUF);
    while let Some(bytes) = body.try_next().await.map_err(|e| e.to_string())? {
        if stats.is_cancelled() {
            return Err("cancelled".into());
        }
        stats.done_bytes.fetch_add(bytes.len() as u64, Relaxed);
        *added += bytes.len() as u64;
        if buf.is_empty() && bytes.len() >= WRITE_BUF {
            write_all_at(f, &bytes, off).map_err(|e| e.to_string())?;
            off += bytes.len() as u64;
            continue;
        }
        buf.extend_from_slice(&bytes);
        if buf.len() >= WRITE_BUF {
            write_all_at(f, &buf, off).map_err(|e| e.to_string())?;
            off += buf.len() as u64;
            buf.clear();
        }
    }
    if !buf.is_empty() {
        write_all_at(f, &buf, off).map_err(|e| e.to_string())?;
        off += buf.len() as u64;
    }
    Ok(off - base_off)
}

async fn backoff(attempt: u32) {
    tokio::time::sleep(Duration::from_millis(250 * 2u64.pow(attempt))).await;
}

async fn whole_with_retry(client: &Client, job: &FileJob, stats: &Stats, gate: &Gate) -> Result<(), String> {
    let tmp = part_path(&job.dst);
    let mut last = String::new();
    for attempt in 0..RETRIES {
        if stats.is_cancelled() {
            return Err("cancelled".into());
        }
        let mut added = 0u64;
        let r: Result<(), String> = async {
            let f = File::create(&tmp).map_err(|e| e.to_string())?;
            fetch_into(client, job, None, &f, stats, gate, &mut added).await?;
            if let Some(t) = job.mtime {
                let _ = f.set_modified(t);
            }
            drop(f);
            fs::rename(&tmp, &job.dst).map_err(|e| e.to_string())
        }
        .await;
        match r {
            Ok(()) => return Ok(()),
            Err(e) => {
                stats.done_bytes.fetch_sub(added, Relaxed);
                if stats.is_cancelled() {
                    return Err(e);
                }
                last = e;
                backoff(attempt).await;
            }
        }
    }
    Err(last)
}

async fn part_with_retry(client: &Client, file: &BigFile, start: u64, end: u64, stats: &Stats, gate: &Gate) -> Result<(), String> {
    let mut last = String::new();
    for attempt in 0..RETRIES {
        if stats.is_cancelled() || file.failed.load(Relaxed) {
            return Err("cancelled".into());
        }
        let mut added = 0u64;
        let r = fetch_into(client, &file.job, Some((start, end)), &file.handle, stats, gate, &mut added).await;
        match r {
            Ok(n) if n == end - start => return Ok(()),
            Ok(n) => {
                stats.done_bytes.fetch_sub(added, Relaxed);
                last = format!("short read: got {n} of {} bytes (object changed?)", end - start);
            }
            Err(e) => {
                stats.done_bytes.fetch_sub(added, Relaxed);
                if stats.is_cancelled() {
                    return Err(e);
                }
                last = e;
            }
        }
        backoff(attempt).await;
    }
    Err(last)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wait(stats: &Stats) {
        while !stats.finished() {
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn make_tree(root: &Path) -> usize {
        let mut n = 0;
        for d in ["a", "a/b", "a/b/c", "empty_dir", "x"] {
            fs::create_dir_all(root.join(d)).unwrap();
        }
        for i in 0..300 {
            let dir = ["a", "a/b", "a/b/c", "x"][i % 4];
            fs::write(root.join(dir).join(format!("f{i}.bin")), vec![(i % 251) as u8; i * 37]).unwrap();
            n += 1;
        }
        // Big file with a non-repeating pattern so misplaced chunks would be caught.
        let big: Vec<u8> = (0..(150u64 << 20) + 12345).map(|i| (i.wrapping_mul(2654435761) >> 13) as u8).collect();
        fs::write(root.join("a/big.bin"), &big).unwrap();
        n + 1
    }

    fn assert_same_tree(a: &Path, b: &Path) -> usize {
        let mut n = 0;
        for ent in fs::read_dir(a).unwrap().flatten() {
            let (pa, pb) = (ent.path(), b.join(ent.file_name()));
            if ent.file_type().unwrap().is_dir() {
                assert!(pb.is_dir(), "missing dir {}", pb.display());
                n += assert_same_tree(&pa, &pb);
            } else {
                assert_eq!(fs::read(&pa).unwrap(), fs::read(&pb).unwrap(), "content differs: {}", pb.display());
                n += 1;
            }
        }
        n
    }

    #[test]
    fn local_tree_copy_and_resume() {
        let base = std::env::temp_dir().join(format!("dd_local_{}", std::process::id()));
        let _ = fs::remove_dir_all(&base);
        let src = base.join("src");
        let dst = base.join("dst");
        let n = make_tree(&src);

        let stats = Stats::new();
        start_local(&stats, vec![(src.clone(), true)], dst.clone(), true, Gate::new("t", 4, 16, true));
        wait(&stats);
        assert_eq!(stats.failed.load(Relaxed), 0, "{:?}", stats.drain_log());
        assert_eq!(stats.done_files.load(Relaxed) as usize, n);
        assert_eq!(stats.done_bytes.load(Relaxed), stats.total_bytes.load(Relaxed));
        assert_eq!(assert_same_tree(&src, &dst.join("src")), n);
        assert!(dst.join("src/empty_dir").is_dir());

        let again = Stats::new();
        start_local(&again, vec![(src.clone(), true)], dst.clone(), true, Gate::new("t", 4, 16, true));
        wait(&again);
        assert_eq!(again.skipped.load(Relaxed) as usize, n);
        let _ = fs::remove_dir_all(&base);
    }

    /// Needs an S3 server. Env: DD_S3_ENDPOINT, DD_S3_KEY, DD_S3_SECRET,
    /// DD_S3_BUCKET, DD_S3_SOURCE (local copy of the bucket contents to compare with).
    #[test]
    #[ignore]
    fn s3_bucket_download_and_resume() {
        let env = |k: &str| std::env::var(k).unwrap_or_else(|_| panic!("set {k}"));
        let login = crate::s3::S3Login {
            endpoint: env("DD_S3_ENDPOINT"),
            region: "us-east-1".into(),
            access_key: env("DD_S3_KEY"),
            secret_key: env("DD_S3_SECRET"),
            bucket: env("DD_S3_BUCKET"),
            path_style: true,
            ..Default::default()
        };
        let source = PathBuf::from(env("DD_S3_SOURCE"));
        let dst = std::env::temp_dir().join(format!("dd_s3_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dst);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let client = rt.block_on(crate::s3::connect(&login)).expect("connect");
        let item = || vec![S3Item { bucket: login.bucket.clone(), key: String::new(), is_dir: true, size: 0, mtime: None }];

        let t0 = Instant::now();
        let stats = Stats::new();
        start_s3(rt.handle(), &stats, client.clone(), item(), dst.clone(), true, Gate::new("c", 16, 128, true));
        wait(&stats);
        let secs = t0.elapsed().as_secs_f64();
        let bytes = stats.done_bytes.load(Relaxed);
        println!("downloaded {} files, {} MB in {secs:.2}s = {:.0} MB/s",
            stats.done_files.load(Relaxed), bytes >> 20, bytes as f64 / 1048576.0 / secs);
        assert_eq!(stats.failed.load(Relaxed), 0, "{:?}", stats.drain_log());
        assert_eq!(bytes, stats.total_bytes.load(Relaxed));
        let n = assert_same_tree(&source, &dst.join(&login.bucket));
        assert_eq!(stats.done_files.load(Relaxed) as usize, n);

        let again = Stats::new();
        start_s3(rt.handle(), &again, client, item(), dst.clone(), true, Gate::new("c", 16, 128, true));
        wait(&again);
        assert_eq!(again.skipped.load(Relaxed) as usize, n, "{:?}", again.drain_log());
        let _ = fs::remove_dir_all(&dst);
    }
}
