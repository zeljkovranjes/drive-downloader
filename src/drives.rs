use std::path::PathBuf;

use crate::util::human;

#[derive(Clone, Debug)]
pub struct DriveInfo {
    pub root: PathBuf,
    pub kind: &'static str,
    pub unc: String,
    pub label: String,
    pub free: Option<u64>,
    pub total: Option<u64>,
}

impl DriveInfo {
    pub fn display(&self) -> String {
        let mut s = format!("{}   [{}]", self.root.display(), self.kind);
        if !self.label.is_empty() {
            s += &format!("  {}", self.label);
        }
        if !self.unc.is_empty() {
            s += &format!("   ->  {}", self.unc);
        }
        if let (Some(f), Some(t)) = (self.free, self.total) {
            s += &format!("   ({} free of {})", human(f as f64), human(t as f64));
        }
        s
    }
}

#[cfg(windows)]
mod win {
    use std::ffi::c_void;

    pub type ProgressFn = unsafe extern "system" fn(
        i64, i64, i64, i64, u32, u32, *mut c_void, *mut c_void, *mut c_void,
    ) -> u32;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        pub fn GetLogicalDrives() -> u32;
        pub fn GetDriveTypeW(root: *const u16) -> u32;
        pub fn GetVolumeInformationW(
            root: *const u16, name: *mut u16, name_len: u32, serial: *mut u32,
            max_comp: *mut u32, flags: *mut u32, fs: *mut u16, fs_len: u32,
        ) -> i32;
        pub fn GetDiskFreeSpaceExW(root: *const u16, free: *mut u64, total: *mut u64, total_free: *mut u64) -> i32;
        pub fn SetErrorMode(mode: u32) -> u32;
        pub fn CopyFileExW(
            src: *const u16, dst: *const u16, progress: Option<ProgressFn>,
            data: *mut c_void, cancel: *mut i32, flags: u32,
        ) -> i32;
    }

    #[link(name = "mpr")]
    unsafe extern "system" {
        pub fn WNetGetConnectionW(local: *const u16, remote: *mut u16, len: *mut u32) -> u32;
    }

    pub fn wide(s: &str) -> Vec<u16> {
        s.encode_utf16().chain(std::iter::once(0)).collect()
    }

    pub fn from_wide(buf: &[u16]) -> String {
        let n = buf.iter().position(|&c| c == 0).unwrap_or(buf.len());
        String::from_utf16_lossy(&buf[..n])
    }
}

#[cfg(windows)]
pub use win::CopyFileExW;

#[cfg(windows)]
fn probe(letter: char) -> DriveInfo {
    use win::*;
    let root = format!("{letter}:\\");
    let wroot = wide(&root);
    let kind = match unsafe { GetDriveTypeW(wroot.as_ptr()) } {
        2 => "Removable",
        3 => "Local",
        4 => "Network",
        5 => "CD/DVD",
        6 => "RAM disk",
        _ => "Unknown",
    };
    let mut info = DriveInfo { root: PathBuf::from(&root), kind, unc: String::new(), label: String::new(), free: None, total: None };
    if kind == "Network" {
        let mut buf = [0u16; 1024];
        let mut len = buf.len() as u32;
        if unsafe { WNetGetConnectionW(wide(&format!("{letter}:")).as_ptr(), buf.as_mut_ptr(), &mut len) } == 0 {
            info.unc = from_wide(&buf);
        }
    }
    let mut label = [0u16; 261];
    let mut fs = [0u16; 261];
    let ok = unsafe {
        GetVolumeInformationW(wroot.as_ptr(), label.as_mut_ptr(), 261, std::ptr::null_mut(),
            std::ptr::null_mut(), std::ptr::null_mut(), fs.as_mut_ptr(), 261)
    };
    if ok != 0 {
        info.label = from_wide(&label);
        // rclone / WinFsp / other FUSE mounts report as a fixed disk, but they're remote.
        let fs_name = from_wide(&fs);
        if fs_name.to_ascii_uppercase().starts_with("FUSE") {
            info.kind = "Network (mount)";
            info.unc = fs_name;
        }
    }
    let (mut free, mut total) = (0u64, 0u64);
    if unsafe { GetDiskFreeSpaceExW(wroot.as_ptr(), &mut free, &mut total, std::ptr::null_mut()) } != 0 {
        info.free = Some(free);
        info.total = Some(total);
    }
    info
}

/// All drive letters. Each drive is probed on its own thread with a deadline,
/// so a dead network share can't hang the list.
#[cfg(windows)]
pub fn list_drives() -> Vec<DriveInfo> {
    use std::time::{Duration, Instant};
    // No "insert a disk" popups for empty card readers / DVD drives.
    unsafe { win::SetErrorMode(0x0001 | 0x8000) };
    let mask = unsafe { win::GetLogicalDrives() };
    let letters: Vec<char> = (0..26).filter(|i| mask >> i & 1 == 1).map(|i| (b'A' + i as u8) as char).collect();
    let (tx, rx) = std::sync::mpsc::channel();
    for &l in &letters {
        let tx = tx.clone();
        std::thread::spawn(move || {
            let _ = tx.send((l, probe(l)));
        });
    }
    drop(tx);
    let deadline = Instant::now() + Duration::from_secs(4);
    let mut got = std::collections::HashMap::new();
    while got.len() < letters.len() {
        let left = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(left) {
            Ok((l, d)) => {
                got.insert(l, d);
            }
            Err(_) => break,
        }
    }
    letters
        .into_iter()
        .map(|l| {
            got.remove(&l).unwrap_or(DriveInfo {
                root: PathBuf::from(format!("{l}:\\")),
                kind: "Not responding",
                unc: String::new(),
                label: String::new(),
                free: None,
                total: None,
            })
        })
        .collect()
}

#[cfg(not(windows))]
pub fn list_drives() -> Vec<DriveInfo> {
    let mut roots = vec![PathBuf::from("/")];
    for base in ["/mnt", "/media", "/Volumes"] {
        if let Ok(rd) = std::fs::read_dir(base) {
            roots.extend(rd.flatten().map(|e| e.path()));
        }
    }
    roots
        .into_iter()
        .map(|root| DriveInfo { root, kind: "Local", unc: String::new(), label: String::new(), free: None, total: None })
        .collect()
}
