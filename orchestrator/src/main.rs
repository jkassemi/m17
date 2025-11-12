// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Main runtime with Tokio.
//!
//! Responsibilities:
//! - Load configuration and initialize shared infrastructure (metrics, storage, NBBO caches).
//! - Spawn ingestion services (flatfile, websocket, REST-derived helpers) and pass their handles
//!   to downstream engines without reimplementing service internals.
//! - Wire data flows between services (e.g., WS → Greeks → Storage, Flatfile → Greeks).
//! - Monitor lifecycle (start, graceful shutdown) but avoid owning business logic.
//!
//! Non-responsibilities (delegated to services/engines):
//! - Fetching or caching remote data (e.g., treasury yields, options universe) beyond spinning up
//!   the appropriate ingestion service.
//! - Performing per-record computations (Greeks, aggregations).
//! - Managing fine-grained retries/checkpointing for sources (each service owns its own policy).

use aggregations::AggregationsEngine;
use chrono::{DateTime, NaiveDate, Utc};
use classifier::Classifier;
use core_types::config::AppConfig;
use core_types::status::{OverallStatus, ServiceStatusHandle, StatusGauge};
use core_types::types::{Completeness, DataBatch, DataBatchMeta, Quality, Source, Watermark};
use flatfile_ingestion_service::FlatfileIngestionService;
use log::info;
use metrics::Metrics;
use nbbo_cache::NbboStore;
use realtime_ws_ingestion_service::RealtimeWsIngestionService;
use reqwest::Client;
use simplelog::*;
use std::fs::File;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use storage::Storage;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::sync::RwLock as TokioRwLock;
use tokio::time::sleep;
use treasury_ingestion_service::TreasuryIngestionService;
use tui::Tui;
const TREASURY_REFRESH_INTERVAL_SECS: u64 = 86_400;

#[tokio::main]
async fn main() {
    // Initialize file-based logging
    WriteLogger::init(
        LevelFilter::Info,
        Config::default(),
        File::create("orchestrator.log").unwrap(),
    )
    .unwrap();

    let config = AppConfig::load().unwrap_or_else(|err| {
        panic!("Failed to load config from config.toml: {}", err);
    });
    let mut ingest_min_date: Option<NaiveDate> = None;
    let mut ingest_max_date: Option<NaiveDate> = None;
    for range in &config.flatfile.date_ranges {
        let start_ts_ns = range.start_ts_ns().unwrap_or(0);
        if let Some(start_dt) = DateTime::<Utc>::from_timestamp(
            start_ts_ns / 1_000_000_000,
            (start_ts_ns % 1_000_000_000) as u32,
        ) {
            let start_date = start_dt.naive_utc().date();
            ingest_min_date = Some(match ingest_min_date {
                Some(existing) => existing.min(start_date),
                None => start_date,
            });
        }
        let end_ts_ns = if let Some(end) = range.end_ts_ns().ok().flatten() {
            end
        } else {
            Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX)
        };
        if let Some(end_dt) = DateTime::<Utc>::from_timestamp(
            end_ts_ns / 1_000_000_000,
            (end_ts_ns % 1_000_000_000) as u32,
        ) {
            let end_date = end_dt.naive_utc().date();
            ingest_max_date = Some(match ingest_max_date {
                Some(existing) => existing.max(end_date),
                None => end_date,
            });
        }
    }
    let metrics = Arc::new(Metrics::new());
    let shutdown_progress = ShutdownProgress::new(metrics.clone());
    metrics.set_metrics_port(config.metrics.port);
    metrics.spawn_service_metric_task(Duration::from_secs(5));
    let api_key = config
        .ws
        .api_key
        .clone()
        .expect("ws.api_key must be set for treasury + realtime ingestion");
    let treasury_service = TreasuryIngestionService::new(
        Client::new(),
        config.ws.rest_base_url.clone(),
        api_key.clone(),
    );
    let treasury_status = treasury_service.status_handle();
    let treasury_handle = treasury_service.handle();
    metrics.register_service_status(treasury_status.clone());
    if let (Some(start_date), Some(end_date)) = (ingest_min_date, ingest_max_date) {
        match treasury_service.prefetch_range(start_date, end_date).await {
            Ok(()) => info!(
                "Prefetched Massive treasury yields for {} through {}",
                start_date, end_date
            ),
            Err(err) => eprintln!("failed to prefetch treasury yields: {}", err),
        }
    }
    match treasury_service.refresh_latest().await {
        Ok(Some(date)) => info!(
            "loaded Massive treasury yields for latest trading day {}",
            date
        ),
        Ok(None) => eprintln!(
            "treasury yields endpoint returned no usable data; services will remain critical"
        ),
        Err(err) => {
            eprintln!("failed to fetch initial treasury yields: {}", err);
        }
    }
    treasury_service.spawn_refresh_loop(Duration::from_secs(TREASURY_REFRESH_INTERVAL_SECS));
    info!(
        "Loaded config with {} flatfile date ranges",
        config.flatfile.date_ranges.len()
    );
    for (i, range) in config.flatfile.date_ranges.iter().enumerate() {
        info!(
            "Range {}: start_ts={}, end_ts={:?}",
            i, range.start_ts, range.end_ts
        );
    }
    let nbbo_flatfile = Arc::new(TokioRwLock::new(NbboStore::new()));
    let nbbo_realtime = Arc::new(TokioRwLock::new(NbboStore::new()));
    let _classifier = Classifier::new();
    let storage = Arc::new(Mutex::new(Storage::new(config.storage)));

    let aggregator_sender = if !config.aggregations.symbol.trim().is_empty() {
        match AggregationsEngine::new(config.aggregations.clone()) {
            Ok(mut engine) => {
                let (tx, mut rx) = mpsc::channel(4096);
                let storage_clone = storage.clone();
                tokio::spawn(async move {
                    while let Some(event) = rx.recv().await {
                        let rows = engine.ingest(event);
                        if rows.is_empty() {
                            continue;
                        }
                        let watermark = rows.last().map(|r| r.window_end_ns).unwrap_or(0);
                        let batch = DataBatch {
                            rows,
                            meta: DataBatchMeta {
                                source: Source::Ws,
                                quality: Quality::Prelim,
                                watermark: Watermark {
                                    watermark_ts_ns: watermark,
                                    completeness: Completeness::Partial,
                                    hints: None,
                                },
                                schema_version: 1,
                            },
                        };
                        if let Err(e) = storage_clone.lock().unwrap().write_aggregations(&batch) {
                            eprintln!("aggregation write error: {}", e);
                        }
                    }
                });
                Some(tx)
            }
            Err(e) => {
                eprintln!("failed to initialize aggregations engine: {}", e);
                None
            }
        }
    } else {
        None
    };

    let flatfile_service = FlatfileIngestionService::new(
        Arc::new(config.flatfile.clone()),
        &config.ingest,
        metrics.clone(),
        storage.clone(),
        nbbo_flatfile.clone(),
        config.greeks.clone(),
        config.greeks.flatfile_underlying_staleness_us,
        config.ingest.concurrent_permits,
        treasury_handle.clone(),
    )
    .await;
    let flatfile_status = flatfile_service.status_handle();
    metrics.register_service_status(flatfile_status);
    flatfile_service.start(config.flatfile.date_ranges.clone());

    // Set initial config reload timestamp
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    metrics.set_last_config_reload_ts_ns(now);

    // Stub metrics server
    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.metrics.port))
        .await
        .unwrap();
    let metrics_clone = metrics.clone(); // Clone the Arc for the spawned task
    tokio::spawn(async move {
        if let Err(e) = metrics_clone.serve(listener).await {
            eprintln!("metrics server error: {}", e);
        }
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
                            info!(
                                "Config reloaded with {} flatfile date ranges",
                                new_config.flatfile.date_ranges.len()
                            );
                            for (i, range) in new_config.flatfile.date_ranges.iter().enumerate() {
                                info!(
                                    "Reloaded range {}: start_ts={}, end_ts={:?}",
                                    i, range.start_ts, range.end_ts
                                );
                            }
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
    let (tui_shutdown_tx, tui_shutdown_rx) = watch::channel(false);
    let mut tui = Tui::new(metrics.clone(), shutdown_tx, tui_shutdown_rx);
    let tui_handle = tokio::spawn(async move {
        if let Err(e) = tui.run().await {
            eprintln!("TUI error: {}", e);
        }
    });

    // Realtime websockets (quotes -> nbbo_realtime, options trades -> greeks + persist)
    let realtime_service = RealtimeWsIngestionService::new(
        config.ws.clone(),
        nbbo_realtime.clone(),
        storage.clone(),
        metrics.clone(),
        config.greeks.clone(),
        config.greeks.realtime_underlying_staleness_us,
        treasury_handle.clone(),
        aggregator_sender.clone(),
    );
    let realtime_status = realtime_service.status_handle();
    metrics.register_service_status(realtime_status);
    realtime_service.start();

    // Wait for shutdown signal or ctrl_c
    let shutdown_reason = tokio::select! {
        _ = tokio::signal::ctrl_c() => ShutdownReason::CtrlC,
        _ = shutdown_rx => ShutdownReason::TuiRequested,
    };
    shutdown_progress.set_phase(
        ShutdownPhase::SignalReceived,
        Some(shutdown_reason.description().to_string()),
    );

    shutdown_progress.set_phase(ShutdownPhase::NotifyingTui, None);
    let _ = tui_shutdown_tx.send(true);

    shutdown_progress.set_phase(ShutdownPhase::WaitingForTuiExit, None);
    match tui_handle.await {
        Ok(()) => {
            shutdown_progress.set_phase(
                ShutdownPhase::Complete,
                Some(format!(
                    "Shutdown complete after {}",
                    shutdown_reason.description()
                )),
            );
        }
        Err(e) => {
            eprintln!("TUI join error: {}", e);
            shutdown_progress.set_phase(
                ShutdownPhase::Complete,
                Some(format!("Shutdown complete with TUI join error: {}", e)),
            );
        }
    }
}

struct ShutdownProgress {
    status: ServiceStatusHandle,
}

impl ShutdownProgress {
    fn new(metrics: Arc<Metrics>) -> Self {
        let status = ServiceStatusHandle::new("orchestrator_shutdown");
        metrics.register_service_status(status.clone());
        let tracker = Self { status };
        tracker.set_phase(ShutdownPhase::Running, None);
        tracker
    }

    fn set_phase(&self, phase: ShutdownPhase, detail: Option<String>) {
        let max = (SHUTDOWN_PHASES.len().saturating_sub(1)).max(1);
        let idx = phase.index();
        let detail = detail.unwrap_or_else(|| phase.description().to_string());
        info!("shutdown phase {:?}: {}", phase, detail);
        let gauge = StatusGauge {
            label: "shutdown_progress".to_string(),
            value: idx as f64,
            max: Some(max as f64),
            unit: None,
            details: Some(detail.clone()),
        };
        self.status.update(|status| {
            status.gauges = vec![gauge];
            status.warnings.retain(|w| !w.starts_with("phase:"));
            status.warnings.push(format!("phase: {}", detail));
            status.overall = if matches!(phase, ShutdownPhase::Complete) {
                OverallStatus::Ok
            } else {
                OverallStatus::Warn
            };
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShutdownPhase {
    Running,
    SignalReceived,
    NotifyingTui,
    WaitingForTuiExit,
    Complete,
}

impl ShutdownPhase {
    fn description(&self) -> &'static str {
        match self {
            ShutdownPhase::Running => "orchestrator running",
            ShutdownPhase::SignalReceived => "shutdown signal received",
            ShutdownPhase::NotifyingTui => "notifying TUI to exit",
            ShutdownPhase::WaitingForTuiExit => "waiting for TUI task to finish",
            ShutdownPhase::Complete => "shutdown finished",
        }
    }

    fn index(&self) -> usize {
        SHUTDOWN_PHASES
            .iter()
            .position(|phase| phase == self)
            .unwrap_or(0)
    }
}

const SHUTDOWN_PHASES: [ShutdownPhase; 5] = [
    ShutdownPhase::Running,
    ShutdownPhase::SignalReceived,
    ShutdownPhase::NotifyingTui,
    ShutdownPhase::WaitingForTuiExit,
    ShutdownPhase::Complete,
];

enum ShutdownReason {
    CtrlC,
    TuiRequested,
}

impl ShutdownReason {
    fn description(&self) -> &'static str {
        match self {
            ShutdownReason::CtrlC => "Ctrl+C",
            ShutdownReason::TuiRequested => "TUI request",
        }
    }
}
