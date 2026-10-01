#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod app;
mod drives;
mod engine;
mod model;
mod s3;
mod util;

use eframe::egui;

fn main() -> eframe::Result {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to start tokio runtime");
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("Drive Downloader")
            .with_inner_size([1200.0, 820.0])
            .with_min_inner_size([800.0, 500.0]),
        ..Default::default()
    };
    eframe::run_native("drive-downloader", options, Box::new(move |cc| Ok(Box::new(app::App::new(cc, rt)))))
}
