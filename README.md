# drive-downloader

Fast GUI tool for downloading files and folders from S3 (RustFS, MinIO, AWS) or any drive letter, including network and rclone mounts.

## Features

- Connects to S3 directly: endpoint, access key and secret key.
- Downloads many files in parallel and splits large files into parallel range requests.
- Adjusts the number of connections to whatever gives the best throughput, and backs off when the server throttles.
- Lists folders in parallel and starts downloading before the listing finishes.
- Resumes interrupted downloads by skipping files that are already identical.
- Copies from local and mapped drives using the native Windows copy engine.

## Build

```
cargo build --release
```

The binary is `target/release/drive-downloader.exe`.

## Use

1. Pick **Drives** or **S3 / RustFS**. For RustFS, enter the endpoint (e.g. `http://server:9000`) and your keys, and leave "Path-style URLs" on.
2. Tick the files and folders you want. Double-click a folder to open it.
3. Choose a destination and click **Download selected**.
