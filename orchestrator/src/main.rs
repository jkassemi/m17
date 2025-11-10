// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Main runtime with Tokio.

use aggregations::{AggregationEvent, AggregationsEngine};
use chrono::{DateTime, NaiveDate, Utc};
use classifier::Classifier;
mod greeks_mod {
    pub use classifier::greeks::*;
}
use core_types::config::AppConfig;
use core_types::types::{
    Completeness, DataBatch, DataBatchMeta, OptionTrade, Quality, Source, Watermark,
};
use flatfile_source::FlatfileSource;
use flatfile_source::SourceTrait;
use futures::StreamExt;
use log::info;
use metrics::Metrics;
use nbbo_cache::NbboStore;
use reqwest::{Client, Url};
use serde::Deserialize;
use simplelog::*;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::mem;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use storage::Storage;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::sync::RwLock as TokioRwLock;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::{interval, sleep};
use tui::Tui;
use ws_source::worker::{ResourceKind, SubscriptionSource, WsMessage, WsWorker};

const TREASURY_REFRESH_INTERVAL_SECS: u64 = 86_400;
type TreasuryCurveCache =
    Arc<TokioRwLock<HashMap<NaiveDate, Arc<greeks_mod::TreasuryCurve>>>>;
type TreasuryCurveState = Arc<TokioRwLock<Option<Arc<greeks_mod::TreasuryCurve>>>>;

#[tokio::main]
async fn main() {
    // Initialize file-based logging
    WriteLogger::init(
        LevelFilter::Info,
        Config::default(),
        File::create("orchestrator.log").unwrap(),
    )
    .unwrap();

    let config = AppConfig::load().expect("Failed to load config: required environment variables POLYGONIO_KEY, POLYGONIO_ACCESS_KEY_ID, POLYGONIO_SECRET_ACCESS_KEY must be set");
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
    let treasury_curve_cache: TreasuryCurveCache =
        Arc::new(TokioRwLock::new(HashMap::new()));
    let realtime_curve_state: TreasuryCurveState = Arc::new(TokioRwLock::new(None));
    if let Some(api_key) = config.ws.api_key.clone() {
        let rest_client = Client::new();
        if let (Some(start_date), Some(end_date)) = (ingest_min_date, ingest_max_date) {
            match fetch_treasury_curve_range(
                &rest_client,
                &config.ws.rest_base_url,
                &api_key,
                start_date,
                end_date,
            )
            .await
            {
                Ok(curves) => {
                    let mut cache = treasury_curve_cache.write().await;
                    for (date, curve) in curves {
                        cache.insert(date, Arc::new(curve));
                    }
                    info!(
                        "Prefetched Massive treasury yields for {} through {}",
                        start_date, end_date
                    );
                }
                Err(err) => eprintln!("failed to prefetch treasury yields: {}", err),
            }
        }
        match fetch_latest_treasury_curve(&rest_client, &config.ws.rest_base_url, &api_key).await {
            Ok(Some((date, curve))) => {
                info!(
                    "loaded Massive treasury yields for latest trading day {}",
                    date
                );
                let arc_curve = Arc::new(curve);
                treasury_curve_cache
                    .write()
                    .await
                    .insert(date, arc_curve.clone());
                *realtime_curve_state.write().await = Some(arc_curve);
            }
            Ok(None) => {
                eprintln!("treasury yields endpoint returned no usable data; using config risk_free_rate fallback");
            }
            Err(err) => {
                eprintln!("failed to fetch treasury yields: {}", err);
            }
        }
        tokio::spawn(run_treasury_curve_refresh_loop(
            rest_client.clone(),
            config.ws.rest_base_url.clone(),
            api_key.clone(),
            realtime_curve_state.clone(),
            treasury_curve_cache.clone(),
        ));
    }
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
    let metrics = Arc::new(Metrics::new());
    let flatfile_config = config.flatfile.clone();
    let flatfile_source = FlatfileSource::new(
        Arc::new(flatfile_config),
        Some(metrics.clone()),
        config.ingest.batch_size,
        config.ingest.progress_update_ms,
    )
    .await;
    let flatfile_source_clone = flatfile_source.clone();
    let nbbo_flatfile = Arc::new(TokioRwLock::new(NbboStore::new()));
    let nbbo_realtime = Arc::new(TokioRwLock::new(NbboStore::new()));
    let classifier = Classifier::new();
    let greeks_engine_rt = greeks_mod::GreeksEngine::new(
        config.greeks.clone(),
        nbbo_realtime.clone(),
        config.greeks.realtime_underlying_staleness_us,
        realtime_curve_state.clone(),
    );
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

    let mut planned_days: u64 = 0;
    for range in &config.flatfile.date_ranges {
        let start_ts_ns = range.start_ts_ns().unwrap_or(0);
        let end_ts_ns = if let Some(end) = range.end_ts_ns().ok().flatten() {
            end
        } else {
            Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX)
        };
        if let (Some(start_dt), Some(end_dt)) = (
            DateTime::<Utc>::from_timestamp(
                start_ts_ns / 1_000_000_000,
                (start_ts_ns % 1_000_000_000) as u32,
            ),
            DateTime::<Utc>::from_timestamp(
                end_ts_ns / 1_000_000_000,
                (end_ts_ns % 1_000_000_000) as u32,
            ),
        ) {
            let mut d = start_dt.naive_utc().date();
            let end_d = end_dt.naive_utc().date();
            while d <= end_d {
                planned_days += 1;
                d = d.succ_opt().unwrap();
            }
        }
    }
    metrics.add_planned_days(planned_days);

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

    // Ingestion loop for equity and option trades
    let semaphore = Arc::new(Semaphore::new(config.ingest.concurrent_days)); // Configurable concurrent days
    for range in &config.flatfile.date_ranges {
        let start_ts_ns = range.start_ts_ns().unwrap_or(0);
        let end_ts_ns = if let Some(end) = range.end_ts_ns().ok().flatten() {
            end
        } else {
            Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX)
        };
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
            info!(
                "Starting ingestion task for day: {}",
                current_date.format("%Y-%m-%d")
            );
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let storage_eq = storage.clone();
            let metrics_eq = metrics.clone();
            let flatfile_source_for_task = flatfile_source_clone.clone();
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
                info!("Processing day: {}", current_date.format("%Y-%m-%d"));
                let mut stream = flatfile_source_for_task.get_equity_trades(scope).await;
                let mut batch_count = 0u64;
                let mut row_count = 0u64;
                while let Some(mut batch) = stream.next().await {
                    batch_count += 1;
                    row_count += batch.rows.len() as u64;
                    metrics_eq.inc_batches(1);
                    metrics_eq.inc_rows(batch.rows.len() as u64);
                    // No greeks for equities
                    if let Err(e) = storage_eq.lock().unwrap().write_equity_trades(&batch) {
                        eprintln!("Failed to write equity trades batch: {}", e);
                    }
                }
                metrics_eq.inc_completed_day();
                let status_msg = format!(
                    "Ingested day: {} ({} batches, {} rows)",
                    current_date.format("%Y-%m-%d"),
                    batch_count,
                    row_count
                );
                metrics_eq.set_flatfile_status(status_msg.clone());
                info!("{}", status_msg);
                drop(permit);
            });

            // Also spawn flatfile NBBO + options ingestion for the same day
            let permit_opt = semaphore.clone().acquire_owned().await.unwrap();
            let storage_opt = storage.clone();
            let metrics_opt = metrics.clone();
            let flatfile_source_for_options = flatfile_source_clone.clone();
            let nbbo_flatfile_clone = nbbo_flatfile.clone();
            let scope_opt = core_types::types::QueryScope {
                instruments: vec![],
                time_range: (day_start_ns, day_end_ns),
                mode: "Historical".to_string(),
                quality_target: core_types::types::Quality::Prelim,
            };
            let treasury_cache_for_day = treasury_curve_cache.clone();
            let greeks_cfg = config.greeks.clone();
            let flatfile_staleness = config.greeks.flatfile_underlying_staleness_us;
            tokio::spawn(async move {
                info!("Seeding NBBO for day: {}", current_date.format("%Y-%m-%d"));
                // Seed NBBO cache from flatfile equities quotes for the day
                let mut nbbo_stream = flatfile_source_for_options
                    .get_nbbo(scope_opt.clone())
                    .await;
                let mut last_ts = None;
                while let Some(batch) = nbbo_stream.next().await {
                    let mut guard = nbbo_flatfile_clone.write().await;
                    for q in batch.rows.iter() {
                        guard.put(q);
                    }
                    if let Some(q) = batch.rows.last() {
                        last_ts = Some(q.quote_ts_ns);
                    }
                    if let Some(ts) = last_ts {
                        guard.prune_before(ts.saturating_sub(2_000_000_000));
                    } // keep ~2s tail
                }
                info!(
                    "Processing options (OPRA) day: {}",
                    current_date.format("%Y-%m-%d")
                );
                let day_curve = {
                    let cache = treasury_cache_for_day.read().await;
                    cache.get(&current_date).cloned()
                };
                if day_curve.is_none() {
                    eprintln!(
                        "missing treasury curve for {}; using config risk_free_rate fallback",
                        current_date
                    );
                }
                let day_curve_state: TreasuryCurveState =
                    Arc::new(TokioRwLock::new(day_curve));
                let greeks_engine_flat = greeks_mod::GreeksEngine::new(
                    greeks_cfg.clone(),
                    nbbo_flatfile_clone.clone(),
                    flatfile_staleness,
                    day_curve_state.clone(),
                );
                let mut stream = flatfile_source_for_options
                    .get_option_trades(scope_opt)
                    .await;
                let mut batch_count = 0u64;
                let mut row_count = 0u64;
                while let Some(mut batch) = stream.next().await {
                    batch_count += 1;
                    row_count += batch.rows.len() as u64;
                    metrics_opt.inc_batches(1);
                    metrics_opt.inc_rows(batch.rows.len() as u64);
                    greeks_engine_flat.enrich_batch(&mut batch.rows).await;
                    if let Err(e) = storage_opt.lock().unwrap().write_option_trades(&batch) {
                        eprintln!("Failed to write option trades batch: {}", e);
                    }
                }
                metrics_opt.inc_completed_day();
                let status_msg = format!(
                    "Ingested options day: {} ({} batches, {} rows)",
                    current_date.format("%Y-%m-%d"),
                    batch_count,
                    row_count
                );
                metrics_opt.set_flatfile_status(status_msg.clone());
                info!("{}", status_msg);
                drop(permit_opt);
            });
            current_date = current_date.succ_opt().unwrap();
        }
    }

    // Realtime websockets (quotes -> nbbo_realtime, options trades -> greeks + persist)
    {
        let ws_cfg = config.ws.clone();
        let underlying = ws_cfg.underlying_symbol.trim().to_string();
        let agg_symbol = config.aggregations.symbol.trim().to_string();
        if underlying.is_empty() {
            eprintln!("ws underlying_symbol is empty; skipping realtime feeds");
        } else {
            let quote_topic = format!("Q.{}", underlying);
            let quotes_worker = WsWorker::new(
                &ws_cfg.stocks_ws_url,
                ResourceKind::EquityQuotes,
                ws_cfg.api_key.clone(),
                SubscriptionSource::Static(vec![quote_topic]),
            );
            let nbbo_rt = nbbo_realtime.clone();
            tokio::spawn(async move {
                match quotes_worker.stream().await {
                    Ok(mut stream) => {
                        while let Some(msg) = stream.next().await {
                            if let WsMessage::Nbbo(nbbo) = msg {
                                let mut guard = nbbo_rt.write().await;
                                guard.put(&nbbo);
                                guard.prune_before(nbbo.quote_ts_ns.saturating_sub(2_000_000_000));
                            }
                        }
                    }
                    Err(err) => {
                        eprintln!("ws quotes stream error: {}", err);
                    }
                }
            });
        }

        if let Some(sender) = aggregator_sender.clone() {
            if !agg_symbol.is_empty() {
                let trades_worker = WsWorker::new(
                    &ws_cfg.stocks_ws_url,
                    ResourceKind::EquityTrades,
                    ws_cfg.api_key.clone(),
                    SubscriptionSource::Static(vec![format!("T.{}", agg_symbol)]),
                );
                tokio::spawn(async move {
                    match trades_worker.stream().await {
                        Ok(mut stream) => {
                            while let Some(msg) = stream.next().await {
                                if let WsMessage::EquityTrade(trade) = msg {
                                    let _ =
                                        sender.send(AggregationEvent::UnderlyingTrade(trade)).await;
                                }
                            }
                        }
                        Err(err) => eprintln!("ws equities trade stream error: {}", err),
                    }
                });
            }
        }

        let options_sub_rx = spawn_options_subscription_task(ws_cfg.clone());
        if let Some(options_rx) = options_sub_rx {
            let options_worker = WsWorker::new(
                &ws_cfg.options_ws_url,
                ResourceKind::OptionsTrades,
                ws_cfg.api_key.clone(),
                SubscriptionSource::Dynamic(options_rx),
            );
            let storage_rt = storage.clone();
            let metrics_rt = metrics.clone();
            let greeks_rt = greeks_engine_rt.clone();
            let batch_cap = std::cmp::max(1, ws_cfg.batch_size);
            let agg_sender_options = aggregator_sender.clone();
            let nbbo_lookup_for_agg = nbbo_realtime.clone();
            let staleness_us = config.greeks.realtime_underlying_staleness_us;
            tokio::spawn(async move {
                match options_worker.stream().await {
                    Ok(mut stream) => {
                        let mut pending: Vec<OptionTrade> = Vec::with_capacity(batch_cap);
                        while let Some(msg) = stream.next().await {
                            if let WsMessage::OptionTrade(trade) = msg {
                                if let Some(sender) = &agg_sender_options {
                                    let underlying_price = {
                                        let store = nbbo_lookup_for_agg.read().await;
                                        store
                                            .get_best_before(
                                                &trade.underlying,
                                                trade.trade_ts_ns,
                                                staleness_us,
                                            )
                                            .map(|q| q.bid)
                                    };
                                    let _ = sender
                                        .send(AggregationEvent::OptionTrade {
                                            trade: trade.clone(),
                                            underlying_price,
                                        })
                                        .await;
                                }
                                pending.push(trade);
                                if pending.len() >= batch_cap {
                                    persist_realtime_options(
                                        &mut pending,
                                        &greeks_rt,
                                        &storage_rt,
                                        &metrics_rt,
                                    )
                                    .await;
                                }
                            }
                        }
                        if !pending.is_empty() {
                            persist_realtime_options(
                                &mut pending,
                                &greeks_rt,
                                &storage_rt,
                                &metrics_rt,
                            )
                            .await;
                        }
                    }
                    Err(err) => {
                        eprintln!("ws options stream error: {}", err);
                    }
                }
            });
        } else {
            eprintln!("ws options subscription task not started; skipping options stream");
        }
    }

    // Wait for shutdown signal or ctrl_c
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = shutdown_rx => {},
    }
}

async fn persist_realtime_options(
    rows: &mut Vec<OptionTrade>,
    greeks: &greeks_mod::GreeksEngine,
    storage: &Arc<Mutex<Storage>>,
    metrics: &Arc<Metrics>,
) {
    if rows.is_empty() {
        return;
    }
    greeks.enrich_batch(rows).await;
    let watermark = rows.last().map(|t| t.trade_ts_ns).unwrap_or(0);
    let meta = DataBatchMeta {
        source: Source::Ws,
        quality: Quality::Prelim,
        watermark: Watermark {
            watermark_ts_ns: watermark,
            completeness: Completeness::Unknown,
            hints: None,
        },
        schema_version: 1,
    };
    let batch = DataBatch {
        rows: mem::take(rows),
        meta,
    };
    metrics.inc_batches(1);
    metrics.inc_rows(batch.rows.len() as u64);
    if let Err(e) = storage.lock().unwrap().write_option_trades(&batch) {
        eprintln!("Failed to write realtime option trades: {}", e);
    }
}

type BoxError = Box<dyn Error + Send + Sync>;

async fn fetch_latest_treasury_curve(
    client: &Client,
    rest_base_url: &str,
    api_key: &str,
) -> Result<Option<(NaiveDate, greeks_mod::TreasuryCurve)>, BoxError> {
    let mut url = Url::parse(rest_base_url)?;
    url.set_path("/fed/v1/treasury-yields");
    url.query_pairs_mut()
        .append_pair("limit", "1")
        .append_pair("sort", "date.desc")
        .append_pair("apiKey", api_key);
    let resp = client.get(url.clone()).send().await?;
    if !resp.status().is_success() {
        return Err(format!("treasury yields status {}", resp.status()).into());
    }
    let parsed: TreasuryYieldsResponse = resp.json().await?;
    if let Some(mut results) = parsed.results {
        if let Some(record) = results.pop() {
            return Ok(build_curve_from_record(record));
        }
    }
    Ok(None)
}

async fn fetch_treasury_curve_range(
    client: &Client,
    rest_base_url: &str,
    api_key: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<Vec<(NaiveDate, greeks_mod::TreasuryCurve)>, BoxError> {
    let mut url = Url::parse(rest_base_url)?;
    url.set_path("/fed/v1/treasury-yields");
    url.query_pairs_mut()
        .append_pair("limit", "5000")
        .append_pair("sort", "date.asc")
        .append_pair("date_gte", &start_date.to_string())
        .append_pair("date_lte", &end_date.to_string())
        .append_pair("apiKey", api_key);
    let mut curves = Vec::new();
    let mut next = Some(url);
    while let Some(current) = next {
        let resp = client.get(current.clone()).send().await?;
        if !resp.status().is_success() {
            return Err(format!("treasury yields status {}", resp.status()).into());
        }
        let parsed: TreasuryYieldsResponse = resp.json().await?;
        if let Some(results) = parsed.results {
            for record in results {
                if let Some((date, curve)) = build_curve_from_record(record) {
                    curves.push((date, curve));
                }
            }
        }
        next = if let Some(next_url) = parsed.next_url {
            let mut next_parsed = match Url::parse(&next_url) {
                Ok(u) => u,
                Err(_) => {
                    let mut base = Url::parse(rest_base_url)?;
                    base.set_path(&next_url);
                    base
                }
            };
            if !next_parsed.query_pairs().any(|(k, _)| k == "apiKey") {
                next_parsed.query_pairs_mut().append_pair("apiKey", api_key);
            }
            Some(next_parsed)
        } else {
            None
        };
    }
    Ok(curves)
}

async fn run_treasury_curve_refresh_loop(
    client: Client,
    rest_base_url: String,
    api_key: String,
    state: TreasuryCurveState,
    cache: TreasuryCurveCache,
) {
    let mut ticker = interval(Duration::from_secs(TREASURY_REFRESH_INTERVAL_SECS));
    ticker.tick().await; // consume immediate tick so we wait full interval next
    loop {
        ticker.tick().await;
        match fetch_latest_treasury_curve(&client, &rest_base_url, &api_key).await {
            Ok(Some((date, curve))) => {
                let arc_curve = Arc::new(curve);
                cache.write().await.insert(date, arc_curve.clone());
                *state.write().await = Some(arc_curve);
                info!("refreshed Massive treasury yields for {}", date);
            }
            Ok(None) => {
                eprintln!("treasury yields refresh returned no data");
            }
            Err(err) => {
                eprintln!("treasury yields refresh failed: {}", err);
            }
        }
    }
}

fn spawn_options_subscription_task(
    cfg: core_types::config::WsConfig,
) -> Option<watch::Receiver<Vec<String>>> {
    let api_key = cfg.api_key.clone()?;
    let symbol = cfg.underlying_symbol.trim().to_string();
    if symbol.is_empty() {
        return None;
    }
    let rest_base = cfg.rest_base_url.clone();
    let limit = cfg.options_contract_limit.min(1_000);
    let interval = std::cmp::max(60, cfg.options_refresh_interval_s);
    let (tx, rx) = watch::channel(Vec::new());
    tokio::spawn(async move {
        let client = Client::new();
        let mut first = true;
        loop {
            if !first {
                sleep(Duration::from_secs(interval)).await;
            } else {
                first = false;
            }
            match fetch_top_options_contracts(&client, &rest_base, &api_key, &symbol, limit).await {
                Ok(contracts) => {
                    let subs: Vec<String> =
                        contracts.into_iter().map(|c| format!("T.{}", c)).collect();
                    if tx.send(subs).is_err() {
                        break;
                    }
                }
                Err(err) => {
                    eprintln!("options subscription refresh failed: {}", err);
                }
            }
        }
    });
    Some(rx)
}

async fn fetch_top_options_contracts(
    client: &Client,
    rest_base_url: &str,
    api_key: &str,
    underlying: &str,
    limit: usize,
) -> Result<Vec<String>, BoxError> {
    let mut url = Url::parse(rest_base_url)?;
    url.set_path(&format!("/v3/snapshot/options/{}", underlying));
    url.query_pairs_mut()
        .append_pair("limit", "250")
        .append_pair("apiKey", api_key);
    let mut collected: Vec<(String, f64)> = Vec::new();
    let mut next = Some(url);
    while let Some(current) = next {
        let resp = client.get(current.clone()).send().await?;
        if !resp.status().is_success() {
            return Err(format!("snapshot status {}", resp.status()).into());
        }
        let parsed: ChainResponse = resp.json().await?;
        if let Some(results) = parsed.results {
            for item in results {
                if let Some(ticker) = item
                    .details
                    .as_ref()
                    .and_then(|d| d.ticker.as_ref())
                    .cloned()
                {
                    let score = item
                        .open_interest
                        .unwrap_or(0.0)
                        .max(item.day.as_ref().and_then(|d| d.volume).unwrap_or(0.0));
                    collected.push((ticker, score));
                }
            }
        }
        if collected.len() >= limit {
            break;
        }
        next = if let Some(next_url) = parsed.next_url {
            let mut next_parsed = match Url::parse(&next_url) {
                Ok(u) => u,
                Err(_) => {
                    let mut base = Url::parse(rest_base_url)?;
                    base.set_path(&next_url);
                    base
                }
            };
            if !next_parsed.query_pairs().any(|(k, _)| k == "apiKey") {
                next_parsed.query_pairs_mut().append_pair("apiKey", api_key);
            }
            Some(next_parsed)
        } else {
            None
        };
    }
    collected.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    collected.truncate(limit);
    Ok(collected.into_iter().map(|(t, _)| t).collect())
}

#[derive(Debug, Deserialize)]
struct ChainResponse {
    next_url: Option<String>,
    results: Option<Vec<ChainItem>>,
}

#[derive(Debug, Deserialize)]
struct ChainItem {
    open_interest: Option<f64>,
    day: Option<DayItem>,
    details: Option<DetailsItem>,
}

#[derive(Debug, Deserialize)]
struct DayItem {
    volume: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct DetailsItem {
    ticker: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TreasuryYieldsResponse {
    next_url: Option<String>,
    results: Option<Vec<TreasuryYieldRecord>>,
}

#[derive(Debug, Deserialize)]
struct TreasuryYieldRecord {
    #[allow(dead_code)]
    date: Option<String>,
    #[serde(rename = "yield_1_month")]
    yield_1_month: Option<f64>,
    #[serde(rename = "yield_3_month")]
    yield_3_month: Option<f64>,
    #[serde(rename = "yield_6_month")]
    yield_6_month: Option<f64>,
    #[serde(rename = "yield_1_year")]
    yield_1_year: Option<f64>,
    #[serde(rename = "yield_2_year")]
    yield_2_year: Option<f64>,
    #[serde(rename = "yield_3_year")]
    yield_3_year: Option<f64>,
    #[serde(rename = "yield_5_year")]
    yield_5_year: Option<f64>,
    #[serde(rename = "yield_7_year")]
    yield_7_year: Option<f64>,
    #[serde(rename = "yield_10_year")]
    yield_10_year: Option<f64>,
    #[serde(rename = "yield_20_year")]
    yield_20_year: Option<f64>,
    #[serde(rename = "yield_30_year")]
    yield_30_year: Option<f64>,
}

fn build_curve_from_record(
    record: TreasuryYieldRecord,
) -> Option<(NaiveDate, greeks_mod::TreasuryCurve)> {
    let date = record
        .date
        .as_ref()
        .and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())?;
    let mut points = Vec::new();
    if let Some(v) = record.yield_1_month {
        points.push((1.0 / 12.0, v / 100.0));
    }
    if let Some(v) = record.yield_3_month {
        points.push((0.25, v / 100.0));
    }
    if let Some(v) = record.yield_6_month {
        points.push((0.5, v / 100.0));
    }
    if let Some(v) = record.yield_1_year {
        points.push((1.0, v / 100.0));
    }
    if let Some(v) = record.yield_2_year {
        points.push((2.0, v / 100.0));
    }
    if let Some(v) = record.yield_3_year {
        points.push((3.0, v / 100.0));
    }
    if let Some(v) = record.yield_5_year {
        points.push((5.0, v / 100.0));
    }
    if let Some(v) = record.yield_7_year {
        points.push((7.0, v / 100.0));
    }
    if let Some(v) = record.yield_10_year {
        points.push((10.0, v / 100.0));
    }
    if let Some(v) = record.yield_20_year {
        points.push((20.0, v / 100.0));
    }
    if let Some(v) = record.yield_30_year {
        points.push((30.0, v / 100.0));
    }
    greeks_mod::TreasuryCurve::from_pairs(points).map(|curve| (date, curve))
}
