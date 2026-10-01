use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_config::timeout::TimeoutConfig;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, Region, RequestChecksumCalculation, ResponseChecksumValidation};
use aws_sdk_s3::error::DisplayErrorContext;

use crate::model::{Entry, Loc};
use crate::util::{last_segment, unix_to_systime};

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct S3Login {
    /// e.g. http://192.168.1.10:9000 for RustFS/MinIO. Empty = AWS.
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    #[serde(skip)]
    pub secret_key: String,
    pub bucket: String,
    pub path_style: bool,
    /// Keep the login (secret encrypted with Windows DPAPI) and auto-connect on launch.
    pub remember: bool,
    /// Secret key encrypted for the current Windows user, hex encoded.
    pub secret_protected: String,
}

pub fn err_text<E: std::error::Error>(e: E) -> String {
    DisplayErrorContext(e).to_string()
}

pub async fn connect(login: &S3Login) -> Result<Client, String> {
    let region = if login.region.trim().is_empty() { "us-east-1".to_string() } else { login.region.trim().to_string() };
    let mut loader = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(region))
        .timeout_config(TimeoutConfig::builder().connect_timeout(Duration::from_secs(10)).build());
    // Empty keys -> standard credential chain (env vars, ~/.aws/credentials, ...).
    if !login.access_key.trim().is_empty() {
        loader = loader.credentials_provider(Credentials::new(
            login.access_key.trim(),
            login.secret_key.trim(),
            None,
            None,
            "drive-downloader",
        ));
    }
    let sdk = loader.load().await;
    let mut conf = aws_sdk_s3::config::Builder::from(&sdk)
        .force_path_style(login.path_style)
        // Compatible servers (RustFS, MinIO, ...) don't all do the newer default checksums.
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .response_checksum_validation(ResponseChecksumValidation::WhenRequired);
    if !login.endpoint.trim().is_empty() {
        conf = conf.endpoint_url(login.endpoint.trim());
    }
    let client = Client::from_conf(conf.build());

    // Verify the login right away so the user gets a clear error.
    if login.bucket.trim().is_empty() {
        client.list_buckets().send().await.map_err(err_text)?;
    } else {
        client.head_bucket().bucket(login.bucket.trim()).send().await.map_err(err_text)?;
    }
    Ok(client)
}

pub async fn list_buckets(client: &Client) -> Result<Vec<Entry>, String> {
    let out = client.list_buckets().send().await.map_err(err_text)?;
    let mut v: Vec<Entry> = out
        .buckets()
        .iter()
        .filter_map(|b| b.name())
        .map(|name| Entry {
            name: name.to_string(),
            loc: Loc::S3 { bucket: name.to_string(), key: String::new() },
            is_dir: true,
            size: None,
            mtime: None,
        })
        .collect();
    v.sort_by_key(|e| e.name.to_lowercase());
    Ok(v)
}

/// One "folder" level: common prefixes become folders, objects become files.
pub async fn list_level(client: &Client, bucket: &str, prefix: &str) -> Result<Vec<Entry>, String> {
    let mut dirs = Vec::new();
    let mut files = Vec::new();
    let mut pages = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .delimiter("/")
        .into_paginator()
        .send();
    while let Some(page) = pages.next().await {
        let page = page.map_err(err_text)?;
        for cp in page.common_prefixes() {
            if let Some(p) = cp.prefix() {
                dirs.push(Entry {
                    name: last_segment(p).to_string(),
                    loc: Loc::S3 { bucket: bucket.to_string(), key: p.to_string() },
                    is_dir: true,
                    size: None,
                    mtime: None,
                });
            }
        }
        for o in page.contents() {
            let Some(key) = o.key() else { continue };
            if key == prefix || key.ends_with('/') {
                continue; // folder marker
            }
            files.push(Entry {
                name: last_segment(key).to_string(),
                loc: Loc::S3 { bucket: bucket.to_string(), key: key.to_string() },
                is_dir: false,
                size: o.size().map(|s| s.max(0) as u64),
                mtime: o.last_modified().and_then(|t| unix_to_systime(t.secs(), t.subsec_nanos())),
            });
        }
    }
    dirs.sort_by_key(|e| e.name.to_lowercase());
    files.sort_by_key(|e| e.name.to_lowercase());
    dirs.extend(files);
    Ok(dirs)
}
