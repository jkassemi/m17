// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Main runtime with Tokio.

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use classifier::Classifier;
use core_types::config::AppConfig;
use flatfile_source::FlatfileSource;
use flatfile_source::SourceTrait;
use futures::StreamExt;
use metrics::Metrics;
use nbbo_cache::NbboStore;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use storage::Storage;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tui::Tui;

#[tokio::main]
async fn main() {
    let config = AppConfig::load().expect("Failed to load config: required environment variables POLYGONIO_KEY, POLYGONIO_ACCESS_KEY_ID, POLYGONIO_SECRET_ACCESS_KEY must be set");
    let flatfile_source = FlatfileSource::new(Arc::new(config.flatfile)).await;
    let nbbo_store = NbboStore::new();
    let classifier = Classifier::new();
    let storage = Arc::new(Mutex::new(Storage::new(config.storage)));
    let metrics = Arc::new(Metrics::new()); // Wrap in Arc to match the serve method signature

    // Set initial config reload timestamp
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    metrics.set_last_config_reload_ts_ns(now);

    // Launch flatfile source
    let flatfile_config = config.flatfile.clone();
    let metrics_clone_for_flatfile = metrics.clone();
    tokio::spawn(async move {
        flatfile_source.run().await;
    });

    // Stub metrics server
    let listener = TcpListener::bind("127.0.0.1:9090").await.unwrap();
    let metrics_clone = metrics.clone(); // Clone the Arc for the spawned task
    tokio::spawn(async move {
        metrics_clone.serve(listener).await; // Use the cloned Arc
    });

    // Launch config reload watcher
    let metrics_clone_for_reload = metrics.clone();
    tokio::spawn(async move {
        let mut last_mtime = None;
        loop {
            if let Ok(metadata) = std::fs::metadata("config.toml") {
                let mtime = metadata
                    .modified()
                    .unwrap()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as i64;
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
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let mut tui = Tui::new(metrics.clone(), shutdown_tx);
    tokio::spawn(async move {
        if let Err(e) = tui.run().await {
            eprintln!("TUI error: {}", e);
        }
    });

    // Ingestion loop for equity trades
    let semaphore = Arc::new(Semaphore::new(2)); // Limit to 2 concurrent days
    for range in &config.flatfile.date_ranges {
        let start_ts_ns = range.start_ts_ns().unwrap_or(0);
        let end_ts_ns = range
            .end_ts_ns()
            .unwrap_or(Some(i64::MAX))
            .unwrap_or(i64::MAX);
        let start_date = if let Some(dt) = DateTime::<Utc>::from_timestamp(
            start_ts_ns / 1_000_000_000,
            (start_ts_ns % 1_000_000_000) as u32,
        ) {
            dt.naive_utc().date()
        } else {
            // Default to a recent date if invalid, e.g., 2023-01-01
            NaiveDate::from_ymd_opt(2023, 1, 1).unwrap()
        };
        let end_date = if let Some(dt) = DateTime::<Utc>::from_timestamp(
            end_ts_ns / 1_000_000_000,
            (end_ts_ns % 1_000_000_000) as u32,
        ) {
            dt.naive_utc().date()
        } else {
            // Default to today if invalid
            Utc::now().naive_utc().date()
        };
        let mut current_date = start_date;
        while current_date <= end_date {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let storage = storage.clone();
            let metrics = metrics.clone();
            let day_start_ns = current_date
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_nanos_opt()
                .unwrap_or(i64::MIN);
            let day_end_ns = if let Some(next_day) = current_date.succ_opt() {
                next_day
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc()
                    .timestamp_nanos_opt()
                    .unwrap_or(i64::MAX)
                    .saturating_sub(1)
            } else {
                i64::MAX
            };
            let scope = core_types::types::QueryScope {
                instruments: vec![], // All instruments
                time_range: (day_start_ns, day_end_ns),
                mode: "Historical".to_string(),
                quality_target: core_types::types::Quality::Prelim,
            };
            tokio::spawn(async move {
                let mut stream = flatfile_source.get_equity_trades(scope).await;
                while let Some(batch) = stream.next().await {
                    if let Err(e) = storage.lock().unwrap().write_equity_trades(&batch) {
                        eprintln!("Failed to write equity trades batch: {}", e);
                    }
                }
                metrics.set_flatfile_status(format!("Ingested day: {}", current_date));
                drop(permit);
            });
            current_date = current_date.succ_opt().unwrap();
        }
    }

    // Wait for shutdown signal or ctrl_c
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = shutdown_rx => {},
    }
}
