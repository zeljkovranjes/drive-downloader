use std::path::PathBuf;
use std::time::SystemTime;

/// A location the browser can show / a thing that can be downloaded.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Loc {
    Local(PathBuf),
    /// List of buckets.
    S3Root,
    /// A prefix ("folder", ends in `/` or is empty) or an object key.
    S3 { bucket: String, key: String },
}

impl Loc {
    pub fn display(&self) -> String {
        match self {
            Loc::Local(p) => p.display().to_string(),
            Loc::S3Root => "s3://".into(),
            Loc::S3 { bucket, key } => format!("s3://{bucket}/{key}"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub name: String,
    pub loc: Loc,
    pub is_dir: bool,
    pub size: Option<u64>,
    pub mtime: Option<SystemTime>,
}
