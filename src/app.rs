use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::time::{Instant, SystemTime};

use aws_sdk_s3::Client;
use eframe::egui;
use egui_extras::{Column, TableBuilder};

use crate::drives::{DriveInfo, list_drives};
use crate::engine::{self, Gate, MAX_CONNECTIONS, MAX_LOCAL_THREADS, S3Item, Stats};
use crate::model::{Entry, Loc};
use crate::s3::{self, S3Login};
use crate::util::{fmt_eta, fmt_time, human, parent_prefix};

enum Msg {
    Drives(Vec<DriveInfo>),
    Listing { generation: u64, result: Result<Vec<Entry>, String>, secs: f64 },
    Connected(Result<Client, String>),
}

#[derive(PartialEq, Clone, Copy)]
enum Source {
    Drives,
    S3,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(default)]
struct Settings {
    dest: String,
    connections: usize,
    local_threads: usize,
    auto_tune: bool,
    skip_identical: bool,
    net_only: bool,
    login: S3Login,
}

impl Default for Settings {
    fn default() -> Self {
        let dest = std::env::var("USERPROFILE")
            .or_else(|_| std::env::var("HOME"))
            .map(|h| Path::new(&h).join("Downloads").display().to_string())
            .unwrap_or_default();
        Self {
            dest,
            connections: 32,
            local_threads: 8,
            auto_tune: true,
            skip_identical: true,
            net_only: false,
            login: S3Login { path_style: true, region: "us-east-1".into(), ..Default::default() },
        }
    }
}

struct Sel {
    is_dir: bool,
    size: Option<u64>,
    mtime: Option<SystemTime>,
}

pub struct App {
    rt: tokio::runtime::Runtime,
    ctx: egui::Context,
    tx: mpsc::Sender<Msg>,
    rx: mpsc::Receiver<Msg>,
    set: Settings,
    source: Source,

    drives: Vec<DriveInfo>,
    drives_loading: bool,
    drive_idx: Option<usize>,

    client: Option<Client>,
    connecting: bool,
    s3_status: String,

    cur: Option<Loc>,
    path_text: String,
    entries: Vec<Entry>,
    listing_gen: u64,
    loading: bool,
    browse_status: String,
    filter: String,
    selected: HashMap<Loc, Sel>,

    job: Option<Arc<Stats>>,
    speed_hist: VecDeque<(Instant, u64)>,
    summary_logged: bool,
    log: Vec<String>,
}

impl App {
    pub fn new(cc: &eframe::CreationContext<'_>, rt: tokio::runtime::Runtime) -> Self {
        let set: Settings = cc.storage.and_then(|s| eframe::get_value(s, eframe::APP_KEY)).unwrap_or_default();
        let (tx, rx) = mpsc::channel();
        let mut app = Self {
            rt,
            ctx: cc.egui_ctx.clone(),
            tx,
            rx,
            set,
            source: Source::Drives,
            drives: Vec::new(),
            drives_loading: false,
            drive_idx: None,
            client: None,
            connecting: false,
            s3_status: String::new(),
            cur: None,
            path_text: String::new(),
            entries: Vec::new(),
            listing_gen: 0,
            loading: false,
            browse_status: "Pick a drive, or switch to S3 / RustFS and log in.".into(),
            filter: String::new(),
            selected: HashMap::new(),
            job: None,
            speed_hist: VecDeque::new(),
            summary_logged: true,
            log: Vec::new(),
        };
        app.refresh_drives();
        app
    }

    // ------------------------------------------------------------------ background work

    fn refresh_drives(&mut self) {
        self.drives_loading = true;
        let (tx, ctx) = (self.tx.clone(), self.ctx.clone());
        std::thread::spawn(move || {
            let _ = tx.send(Msg::Drives(list_drives()));
            ctx.request_repaint();
        });
    }

    fn connect(&mut self) {
        self.connecting = true;
        self.s3_status = "Connecting...".into();
        let login = self.set.login.clone();
        let (tx, ctx) = (self.tx.clone(), self.ctx.clone());
        self.rt.spawn(async move {
            let r = s3::connect(&login).await;
            let _ = tx.send(Msg::Connected(r));
            ctx.request_repaint();
        });
    }

    fn navigate(&mut self, loc: Loc) {
        self.path_text = loc.display();
        self.cur = Some(loc.clone());
        self.entries.clear();
        self.filter.clear();
        self.listing_gen += 1;
        self.loading = true;
        self.browse_status = format!("Loading {} ...", loc.display());
        let generation = self.listing_gen;
        let (tx, ctx) = (self.tx.clone(), self.ctx.clone());
        let t0 = Instant::now();
        match loc {
            Loc::Local(path) => {
                std::thread::spawn(move || {
                    let result = list_local(&path);
                    let _ = tx.send(Msg::Listing { generation, result, secs: t0.elapsed().as_secs_f64() });
                    ctx.request_repaint();
                });
            }
            Loc::S3Root | Loc::S3 { .. } => {
                let Some(client) = self.client.clone() else {
                    self.loading = false;
                    self.browse_status = "Not connected to S3.".into();
                    return;
                };
                self.rt.spawn(async move {
                    let result = match &loc {
                        Loc::S3 { bucket, key } => s3::list_level(&client, bucket, key).await,
                        _ => s3::list_buckets(&client).await,
                    };
                    let _ = tx.send(Msg::Listing { generation, result, secs: t0.elapsed().as_secs_f64() });
                    ctx.request_repaint();
                });
            }
        }
    }

    fn poll(&mut self) {
        while let Ok(m) = self.rx.try_recv() {
            match m {
                Msg::Drives(d) => {
                    self.drives_loading = false;
                    self.drives = d;
                    // Network drives first.
                    self.drives.sort_by_key(|d| (!d.kind.starts_with("Network"), d.root.clone()));
                    self.drive_idx = None;
                }
                Msg::Listing { generation, result, secs } => {
                    if generation != self.listing_gen {
                        continue;
                    }
                    self.loading = false;
                    match result {
                        Ok(entries) => {
                            let nd = entries.iter().filter(|e| e.is_dir).count();
                            self.browse_status = format!(
                                "{} folders, {} files  ({secs:.2}s).  Tick boxes to select; double-click a folder to open it.",
                                nd,
                                entries.len() - nd
                            );
                            self.entries = entries;
                        }
                        Err(e) => self.browse_status = format!("Error: {e}"),
                    }
                }
                Msg::Connected(r) => {
                    self.connecting = false;
                    match r {
                        Ok(c) => {
                            self.client = Some(c);
                            self.s3_status = String::new();
                            let b = self.set.login.bucket.trim().to_string();
                            self.navigate(if b.is_empty() { Loc::S3Root } else { Loc::S3 { bucket: b, key: String::new() } });
                        }
                        Err(e) => self.s3_status = format!("Connection failed: {e}"),
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------ navigation helpers

    fn go_to_text(&mut self) {
        let t = self.path_text.trim().trim_matches('"').to_string();
        if t.is_empty() {
            return;
        }
        if let Some(rest) = t.strip_prefix("s3://") {
            if self.client.is_none() {
                self.browse_status = "Log in to S3 first.".into();
                return;
            }
            self.source = Source::S3;
            let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
            if bucket.is_empty() {
                self.navigate(Loc::S3Root);
            } else {
                let mut key = key.to_string();
                if !key.is_empty() && !key.ends_with('/') {
                    key.push('/');
                }
                self.navigate(Loc::S3 { bucket: bucket.into(), key });
            }
        } else {
            self.navigate(Loc::Local(PathBuf::from(t)));
        }
    }

    fn go_up(&mut self) {
        let up = match &self.cur {
            Some(Loc::Local(p)) => p.parent().map(|p| Loc::Local(p.to_path_buf())),
            Some(Loc::S3 { bucket, key }) if !key.is_empty() => {
                Some(Loc::S3 { bucket: bucket.clone(), key: parent_prefix(key) })
            }
            Some(Loc::S3 { .. }) if self.set.login.bucket.trim().is_empty() => Some(Loc::S3Root),
            _ => None,
        };
        if let Some(l) = up {
            self.navigate(l);
        }
    }

    fn toggle(selected: &mut HashMap<Loc, Sel>, e: &Entry, on: bool) {
        if on {
            selected.insert(e.loc.clone(), Sel { is_dir: e.is_dir, size: e.size, mtime: e.mtime });
        } else {
            selected.remove(&e.loc);
        }
    }

    // ------------------------------------------------------------------ downloading

    fn start_download(&mut self) {
        let dest = PathBuf::from(self.set.dest.trim());
        if self.set.dest.trim().is_empty() {
            self.log.push("Choose a destination folder first.".into());
            return;
        }
        if let Err(e) = std::fs::create_dir_all(&dest) {
            self.log.push(format!("Can't create {}: {e}", dest.display()));
            return;
        }
        let mut local = Vec::new();
        let mut remote = Vec::new();
        for (loc, s) in &self.selected {
            match loc {
                Loc::Local(p) => local.push((p.clone(), s.is_dir)),
                Loc::S3 { bucket, key } => remote.push(S3Item {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    is_dir: s.is_dir,
                    size: s.size.unwrap_or(0),
                    mtime: s.mtime,
                }),
                Loc::S3Root => {}
            }
        }
        if !remote.is_empty() && self.client.is_none() {
            self.log.push("Log in to S3 first.".into());
            return;
        }
        let auto = self.set.auto_tune;
        let stats = Stats::new();
        let mut parts = Vec::new();
        if !local.is_empty() {
            let n = self.set.local_threads;
            let gate = Gate::new("drive threads", n, if auto { MAX_LOCAL_THREADS } else { n }, auto);
            parts.push(format!("{} drive item(s)", local.len()));
            engine::start_local(&stats, local, dest.clone(), self.set.skip_identical, gate);
        }
        if !remote.is_empty() {
            let n = self.set.connections;
            let gate = Gate::new("S3 connections", n, if auto { MAX_CONNECTIONS } else { n }, auto);
            parts.push(format!("{} S3 item(s)", remote.len()));
            let client = self.client.clone().unwrap();
            engine::start_s3(self.rt.handle(), &stats, client, remote, dest.clone(), self.set.skip_identical, gate);
        }
        self.log.push(format!(
            "Downloading {} to {}  ({})",
            parts.join(" + "),
            dest.display(),
            if auto { "auto-tuned concurrency" } else { "fixed concurrency" }
        ));
        self.job = Some(stats);
        self.speed_hist.clear();
        self.summary_logged = false;
    }

    /// (progress 0..1, main line, "copying" line)
    fn job_status(&mut self) -> Option<(f32, String, String)> {
        let job = self.job.clone()?;
        self.log.extend(job.drain_log());
        let (db, tb) = (job.done_bytes.load(std::sync::atomic::Ordering::Relaxed), job.total_bytes.load(std::sync::atomic::Ordering::Relaxed));
        let ld = |a: &std::sync::atomic::AtomicU64| a.load(std::sync::atomic::Ordering::Relaxed);
        let (df, tf, sk, fl) = (ld(&job.done_files), ld(&job.total_files), ld(&job.skipped), ld(&job.failed));
        let scanning = job.scanning();
        let finished = job.finished();

        let now = Instant::now();
        self.speed_hist.push_back((now, db));
        while self.speed_hist.len() > 2 && now.duration_since(self.speed_hist[0].0).as_secs_f64() > 3.0 {
            self.speed_hist.pop_front();
        }
        let (t0, b0) = self.speed_hist[0];
        let dt = now.duration_since(t0).as_secs_f64();
        let speed = if dt > 0.0 { db.saturating_sub(b0) as f64 / dt } else { 0.0 };
        let frac = if tb > 0 { (db as f64 / tb as f64).min(1.0) as f32 } else { 0.0 };
        let eta = if scanning || speed <= 0.0 { None } else { Some(tb.saturating_sub(db) as f64 / speed) };
        let plus = if scanning { "+" } else { "" };

        let mut line = format!(
            "{}{} / {}{plus}    files {df}/{tf}{plus}    {}/s    ETA {}",
            human(db as f64),
            "",
            human(tb as f64),
            human(speed),
            fmt_eta(eta)
        );
        for g in job.gates.lock().unwrap().iter() {
            line += &format!("    {}: {}", g.name, g.target.load(std::sync::atomic::Ordering::Relaxed));
        }
        if sk > 0 {
            line += &format!("    skipped {sk}");
        }
        if fl > 0 {
            line += &format!("    failed {fl}");
        }
        if scanning {
            line += "    (still listing...)";
        }

        let mut active = job.active_names();
        active.sort();
        let current = if active.is_empty() {
            String::new()
        } else {
            let mut s = format!("{} active: {}", active.len(), active.join(",  "));
            if s.len() > 400 {
                let mut cut = 400;
                while !s.is_char_boundary(cut) {
                    cut -= 1;
                }
                s.truncate(cut);
                s += "...";
            }
            s
        };

        if finished && !self.summary_logged {
            self.summary_logged = true;
            let el = job.elapsed().as_secs_f64();
            let avg = if el > 0.0 { db as f64 / el } else { 0.0 };
            self.log.push(format!(
                "{}: {df} files ({sk} skipped, {fl} failed), {} in {}, average {}/s",
                if job.is_cancelled() { "CANCELLED" } else { "DONE" },
                human(db as f64),
                fmt_eta(Some(el)),
                human(avg)
            ));
        }
        Some((if finished && !job.is_cancelled() { 1.0 } else { frac }, line, current))
    }

    // ------------------------------------------------------------------ UI sections

    fn top_ui(&mut self, ui: &mut egui::Ui) {
        ui.add_space(4.0);
        ui.horizontal(|ui| {
            ui.heading("Drive Downloader");
            ui.separator();
            if ui.selectable_label(self.source == Source::Drives, "💽 Drives").clicked() {
                self.source = Source::Drives;
            }
            if ui.selectable_label(self.source == Source::S3, "☁ S3 / RustFS (direct)").clicked() {
                self.source = Source::S3;
            }
        });
        ui.add_space(2.0);

        match self.source {
            Source::Drives => {
                ui.horizontal(|ui| {
                    ui.label("Drive:");
                    let shown: Vec<usize> = (0..self.drives.len())
                        .filter(|&i| !self.set.net_only || self.drives[i].kind.starts_with("Network"))
                        .collect();
                    let text = match self.drive_idx.and_then(|i| self.drives.get(i)) {
                        Some(d) => d.display(),
                        None if self.drives_loading => "Scanning drives...".into(),
                        None if shown.is_empty() => "No matching drives".into(),
                        None => "Select a drive...".into(),
                    };
                    let mut pick = None;
                    egui::ComboBox::from_id_salt("drive")
                        .width((ui.available_width() - 230.0).max(200.0))
                        .selected_text(text)
                        .show_ui(ui, |ui| {
                            for &i in &shown {
                                if ui.selectable_label(Some(i) == self.drive_idx, self.drives[i].display()).clicked() {
                                    pick = Some(i);
                                }
                            }
                        });
                    if let Some(i) = pick {
                        self.drive_idx = Some(i);
                        let root = self.drives[i].root.clone();
                        self.navigate(Loc::Local(root));
                    }
                    ui.checkbox(&mut self.set.net_only, "Network only");
                    if ui.button("⟳ Refresh").clicked() {
                        self.refresh_drives();
                    }
                });
            }
            Source::S3 => {
                if self.client.is_some() {
                    ui.horizontal(|ui| {
                        let l = &self.set.login;
                        let ep = if l.endpoint.trim().is_empty() { "AWS S3".to_string() } else { l.endpoint.trim().to_string() };
                        ui.colored_label(egui::Color32::from_rgb(60, 170, 90), "● Connected");
                        ui.label(format!("{ep}   key: {}", if l.access_key.is_empty() { "(default chain)" } else { &l.access_key }));
                        if ui.button("Disconnect").clicked() {
                            self.client = None;
                            if matches!(self.cur, Some(Loc::S3 { .. } | Loc::S3Root)) {
                                self.cur = None;
                                self.entries.clear();
                            }
                            self.selected.retain(|k, _| matches!(k, Loc::Local(_)));
                        }
                    });
                } else {
                    let l = &mut self.set.login;
                    egui::Grid::new("login").num_columns(4).spacing([8.0, 4.0]).show(ui, |ui| {
                        ui.label("Endpoint");
                        ui.add(egui::TextEdit::singleline(&mut l.endpoint).hint_text("http://server:9000   (empty = AWS)").desired_width(320.0));
                        ui.label("Region");
                        ui.add(egui::TextEdit::singleline(&mut l.region).hint_text("us-east-1").desired_width(160.0));
                        ui.end_row();
                        ui.label("Access key");
                        ui.add(egui::TextEdit::singleline(&mut l.access_key).hint_text("empty = env / ~/.aws").desired_width(320.0));
                        ui.label("Secret key");
                        ui.add(egui::TextEdit::singleline(&mut l.secret_key).password(true).desired_width(160.0));
                        ui.end_row();
                        ui.label("Bucket");
                        ui.add(egui::TextEdit::singleline(&mut l.bucket).hint_text("optional - empty lists all buckets").desired_width(320.0));
                        ui.checkbox(&mut l.path_style, "Path-style URLs");
                        ui.label("(keep on for RustFS / MinIO)");
                        ui.end_row();
                    });
                    ui.horizontal(|ui| {
                        if ui.add_enabled(!self.connecting, egui::Button::new("🔑 Connect")).clicked() {
                            self.connect();
                        }
                        if self.connecting {
                            ui.spinner();
                        }
                        if !self.s3_status.is_empty() {
                            ui.colored_label(egui::Color32::from_rgb(220, 80, 80), &self.s3_status);
                        }
                    });
                }
            }
        }

        ui.add_space(2.0);
        ui.horizontal(|ui| {
            if ui.button("⬆ Up").clicked() {
                self.go_up();
            }
            let resp = ui.add(egui::TextEdit::singleline(&mut self.path_text).desired_width((ui.available_width() - 290.0).max(150.0)));
            if resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                self.go_to_text();
            }
            if ui.button("Go").clicked() {
                self.go_to_text();
            }
            if ui.button("⟳").on_hover_text("Reload folder").clicked() {
                if let Some(l) = self.cur.clone() {
                    self.navigate(l);
                }
            }
            ui.label("Filter:");
            ui.add(egui::TextEdit::singleline(&mut self.filter).desired_width(140.0));
        });
        ui.horizontal(|ui| {
            if self.loading {
                ui.spinner();
            }
            ui.weak(&self.browse_status);
        });
        ui.add_space(2.0);
    }

    fn table_ui(&mut self, ui: &mut egui::Ui) {
        let f = self.filter.to_lowercase();
        let visible: Vec<usize> = (0..self.entries.len())
            .filter(|&i| f.is_empty() || self.entries[i].name.to_lowercase().contains(&f))
            .collect();
        let all_on = !visible.is_empty() && visible.iter().all(|&i| self.selected.contains_key(&self.entries[i].loc));
        let mut toggle_all = None;
        let mut nav = None;
        {
            let entries = &self.entries;
            let selected = &mut self.selected;
            TableBuilder::new(ui)
                .striped(true)
                .auto_shrink([false, false])
                .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
                .column(Column::exact(26.0))
                .column(Column::remainder().at_least(200.0).clip(true))
                .column(Column::exact(100.0))
                .column(Column::exact(130.0))
                .header(22.0, |mut h| {
                    h.col(|ui| {
                        let mut a = all_on;
                        if ui.checkbox(&mut a, "").on_hover_text("Select all (filtered)").changed() {
                            toggle_all = Some(a);
                        }
                    });
                    h.col(|ui| {
                        ui.strong("Name");
                    });
                    h.col(|ui| {
                        ui.strong("Size");
                    });
                    h.col(|ui| {
                        ui.strong("Modified");
                    });
                })
                .body(|body| {
                    body.rows(22.0, visible.len(), |mut row| {
                        let e = &entries[visible[row.index()]];
                        let mut on = selected.contains_key(&e.loc);
                        row.col(|ui| {
                            if ui.checkbox(&mut on, "").changed() {
                                Self::toggle(selected, e, on);
                            }
                        });
                        row.col(|ui| {
                            let icon = if e.is_dir { "📁" } else { "📄" };
                            let r = ui.selectable_label(on, format!("{icon} {}", e.name));
                            if e.is_dir && r.double_clicked() {
                                nav = Some(e.loc.clone());
                            } else if r.clicked() {
                                Self::toggle(selected, e, !on);
                            }
                        });
                        row.col(|ui| {
                            if let Some(s) = e.size {
                                ui.label(human(s as f64));
                            }
                        });
                        row.col(|ui| {
                            ui.label(fmt_time(e.mtime));
                        });
                    });
                });
        }
        if let Some(a) = toggle_all {
            for &i in &visible {
                Self::toggle(&mut self.selected, &self.entries[i], a);
            }
        }
        if let Some(l) = nav {
            self.navigate(l);
        }
    }

    fn bottom_ui(&mut self, ui: &mut egui::Ui) {
        ui.add_space(4.0);
        let running = self.job.as_ref().is_some_and(|j| !j.finished());
        ui.horizontal(|ui| {
            let nd = self.selected.values().filter(|s| s.is_dir).count();
            let nf = self.selected.len() - nd;
            let bytes: u64 = self.selected.values().filter_map(|s| s.size).sum();
            let mut t = format!("Selected: {nd} folder(s), {nf} file(s)");
            if bytes > 0 {
                t += &format!("  ({} in files)", human(bytes as f64));
            }
            ui.strong(t);
            if ui.button("Clear selection").clicked() {
                self.selected.clear();
            }
        });
        ui.horizontal(|ui| {
            ui.label("Save to:");
            ui.add(egui::TextEdit::singleline(&mut self.set.dest).desired_width((ui.available_width() - 150.0).max(150.0)));
            if ui.button("Browse...").clicked() {
                let mut d = rfd::FileDialog::new();
                if Path::new(&self.set.dest).is_dir() {
                    d = d.set_directory(&self.set.dest);
                }
                if let Some(p) = d.pick_folder() {
                    self.set.dest = p.display().to_string();
                }
            }
            if ui.button("Open").clicked() {
                open_folder(Path::new(&self.set.dest));
            }
        });
        ui.horizontal(|ui| {
            ui.checkbox(&mut self.set.auto_tune, "Auto-tune concurrency")
                .on_hover_text("Continuously adjusts the number of parallel connections to whatever gives the highest throughput, and backs off if the server throttles.");
            ui.label(if self.set.auto_tune { "S3 start connections:" } else { "S3 connections:" });
            ui.add(egui::Slider::new(&mut self.set.connections, 1..=MAX_CONNECTIONS));
            ui.label("Drive threads:");
            ui.add(egui::Slider::new(&mut self.set.local_threads, 1..=MAX_LOCAL_THREADS));
            ui.checkbox(&mut self.set.skip_identical, "Skip identical files (resume)");
        });

        let status = self.job_status();
        ui.horizontal(|ui| {
            if ui.add_enabled(!running && !self.selected.is_empty(), egui::Button::new("⬇  Download selected")).clicked() {
                self.start_download();
            }
            if ui.add_enabled(running, egui::Button::new("✖ Cancel")).clicked() {
                if let Some(j) = &self.job {
                    j.cancel();
                    self.log.push("Cancelling...".into());
                }
            }
            let frac = status.as_ref().map(|s| s.0).unwrap_or(0.0);
            ui.add(egui::ProgressBar::new(frac).show_percentage().animate(running));
        });
        if let Some((_, line, current)) = &status {
            ui.monospace(line);
            ui.weak(current);
        }
        egui::ScrollArea::vertical().max_height(110.0).stick_to_bottom(true).auto_shrink([false, true]).show(ui, |ui| {
            if self.log.len() > 5000 {
                self.log.drain(..1000);
            }
            for l in &self.log {
                if l.starts_with("FAILED") {
                    ui.colored_label(egui::Color32::from_rgb(220, 80, 80), l);
                } else {
                    ui.label(l);
                }
            }
        });
        ui.add_space(4.0);
        if running {
            self.ctx.request_repaint_after(std::time::Duration::from_millis(200));
        }
    }
}

impl eframe::App for App {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.poll();
        egui::Panel::top("top").show(ui, |ui| self.top_ui(ui));
        egui::Panel::bottom("bottom").show(ui, |ui| self.bottom_ui(ui));
        egui::CentralPanel::default().show(ui, |ui| self.table_ui(ui));
    }

    fn save(&mut self, storage: &mut dyn eframe::Storage) {
        eframe::set_value(storage, eframe::APP_KEY, &self.set);
    }

    fn on_exit(&mut self) {
        if let Some(j) = &self.job {
            j.cancel();
        }
    }
}

fn list_local(path: &Path) -> Result<Vec<Entry>, String> {
    let rd = std::fs::read_dir(path).map_err(|e| e.to_string())?;
    let mut dirs = Vec::new();
    let mut files = Vec::new();
    for ent in rd.flatten() {
        let name = ent.file_name().to_string_lossy().into_owned();
        let is_dir = ent.file_type().map(|t| t.is_dir()).unwrap_or(false);
        let meta = ent.metadata().ok();
        let e = Entry {
            name,
            loc: Loc::Local(ent.path()),
            is_dir,
            size: if is_dir { None } else { meta.as_ref().map(|m| m.len()) },
            mtime: meta.and_then(|m| m.modified().ok()),
        };
        if is_dir { dirs.push(e) } else { files.push(e) }
    }
    dirs.sort_by_key(|e| e.name.to_lowercase());
    files.sort_by_key(|e| e.name.to_lowercase());
    dirs.extend(files);
    Ok(dirs)
}

fn open_folder(p: &Path) {
    if !p.is_dir() {
        return;
    }
    #[cfg(windows)]
    let _ = std::process::Command::new("explorer").arg(p).spawn();
    #[cfg(target_os = "macos")]
    let _ = std::process::Command::new("open").arg(p).spawn();
    #[cfg(all(unix, not(target_os = "macos")))]
    let _ = std::process::Command::new("xdg-open").arg(p).spawn();
}
