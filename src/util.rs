use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn human(n: f64) -> String {
    let mut n = n;
    for unit in ["B", "KB", "MB", "GB", "TB"] {
        if n.abs() < 1024.0 || unit == "TB" {
            return if unit == "B" { format!("{n:.0} {unit}") } else { format!("{n:.1} {unit}") };
        }
        n /= 1024.0;
    }
    unreachable!()
}

pub fn fmt_eta(secs: Option<f64>) -> String {
    match secs {
        Some(s) if s.is_finite() && s >= 0.0 && s < 360_000.0 => {
            let s = s as u64;
            let (h, m, s) = (s / 3600, s % 3600 / 60, s % 60);
            if h > 0 { format!("{h}:{m:02}:{s:02}") } else { format!("{m:02}:{s:02}") }
        }
        _ => "--:--".into(),
    }
}

pub fn fmt_time(t: Option<SystemTime>) -> String {
    match t {
        Some(t) => chrono::DateTime::<chrono::Local>::from(t).format("%Y-%m-%d %H:%M").to_string(),
        None => String::new(),
    }
}

pub fn unix_to_systime(secs: i64, nanos: u32) -> Option<SystemTime> {
    if secs >= 0 {
        Some(UNIX_EPOCH + Duration::new(secs as u64, nanos))
    } else {
        None
    }
}

/// Make one S3 key segment safe as a Windows file name. Returns None for
/// segments that must be dropped ("", ".", "..") so keys can't escape the destination.
pub fn sanitize_segment(seg: &str) -> Option<String> {
    if seg.is_empty() || seg == "." || seg == ".." {
        return None;
    }
    let mut s: String = seg
        .chars()
        .map(|c| if matches!(c, '<' | '>' | ':' | '"' | '|' | '?' | '*' | '\\') || (c as u32) < 32 { '_' } else { c })
        .collect();
    while s.ends_with('.') || s.ends_with(' ') {
        s.pop();
    }
    if s.is_empty() { Some("_".into()) } else { Some(s) }
}

pub fn key_to_path(base: &Path, rel_key: &str) -> PathBuf {
    let mut p = base.to_path_buf();
    for seg in rel_key.split('/').filter_map(sanitize_segment) {
        p.push(seg);
    }
    p
}

/// `foo/bar/` -> `bar`, `foo/bar.txt` -> `bar.txt`
pub fn last_segment(key: &str) -> &str {
    let k = key.trim_end_matches('/');
    k.rsplit('/').next().unwrap_or(k)
}

/// Parent "folder" of an S3 prefix/key: `a/b/c/` -> `a/b/`, `a/` -> ``.
pub fn parent_prefix(prefix: &str) -> String {
    let k = prefix.trim_end_matches('/');
    match k.rfind('/') {
        Some(i) => k[..=i].to_string(),
        None => String::new(),
    }
}

pub fn mtime_close(a: SystemTime, b: SystemTime) -> bool {
    let d = match a.duration_since(b) {
        Ok(d) => d,
        Err(e) => e.duration(),
    };
    d <= Duration::from_secs(2)
}

/// Positional write that works with a shared handle from many tasks at once.
pub fn write_all_at(f: &std::fs::File, mut buf: &[u8], mut off: u64) -> std::io::Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        while !buf.is_empty() {
            let n = f.seek_write(buf, off)?;
            if n == 0 {
                return Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "write returned 0"));
            }
            buf = &buf[n..];
            off += n as u64;
        }
        Ok(())
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        let _ = &mut buf;
        let _ = &mut off;
        f.write_all_at(buf, off)
    }
}
