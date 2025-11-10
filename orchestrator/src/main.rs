// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Main runtime with Tokio.

use classifier::Classifier;
use core_types::config::AppConfig;
use data_client::DataClientRouter;
use flatfile_source::FlatfileSource;
use metrics::Metrics;
use nbbo_cache::NbboStore;
use storage::Storage;
use tokio::net::TcpListener;
use tui::Tui;
use ws_source::worker::WsWorker;
use tokio::sync::oneshot;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    let config = AppConfig::load().unwrap_or_else(|e| {
        eprintln!("Failed to load config, using defaults: {}", e);
        AppConfig::default()
    });
    let data_client = DataClientRouter::new();
    let nbbo_store = NbboStore::new();
    let classifier = Classifier::new();
    let storage = Storage::new(config.storage);
    let metrics = Arc::new(Metrics::new());  // Wrap in Arc to match the serve method signature

    // Set initial config reload timestamp
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    metrics.set_last_config_reload_ts_ns(now);

    // Create shutdown channel
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // Stub WS worker
    let ws_worker = WsWorker::new("ws://example.com"); // Placeholder URL
    let _stream = ws_worker.run().await;

    // Launch flatfile source
    let flatfile_source = FlatfileSource::new();
    let flatfile_config = config.flatfile.clone();
    let metrics_clone_for_flatfile = metrics.clone();
    tokio::spawn(async move {
        flatfile_source.run(flatfile_config).await;
    });

    // Stub metrics server
    let listener = TcpListener::bind("127.0.0.1:9090").await.unwrap();
    let metrics_clone = metrics.clone();  // Clone the Arc for the spawned task
    tokio::spawn(async move {
        metrics_clone.serve(listener).await;  // Use the cloned Arc
    });

    // Launch config reload watcher
    let metrics_clone_for_reload = metrics.clone();
    tokio::spawn(async move {
        let mut last_mtime = None;
        loop {
            if let Ok(metadata) = std::fs::metadata("config.toml") {
                let mtime = metadata.modified().unwrap().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as i64;
                if last_mtime.is_none() || last_mtime.unwrap() != mtime {
                    last_mtime = Some(mtime);
                    match AppConfig::load() {
                        Ok(new_config) => {
                            // Config reloaded successfully, update timestamp
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_nanos() as i64;
                            metrics_clone_for_reload.set_last_config_reload_ts_ns(now);
                            // Note: In a real implementation, you might need to update other components with new_config
                        }
                        Err(e) => {
                            eprintln!("Failed to reload config: {}", e);
                        }
                    }
                }
            }
            sleep(Duration::from_secs(5)).await; // Check every 5 seconds
        }
    });

    // Launch TUI dashboard
    let mut tui = Tui::new(metrics.clone(), shutdown_tx);
    tokio::spawn(async move {
        if let Err(e) = tui.run().await {
            eprintln!("TUI error: {}", e);
        }
    });

    // Wait for shutdown signal or ctrl_c
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = shutdown_rx => {},
    }
}
