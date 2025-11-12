// Copyright (c) James Kassemi, SC, US. All rights reserved.
use chrono::{DateTime, NaiveDate, Timelike, Utc};
use core_types::config::{DateRange, FlatfileConfig, GreeksConfig, IngestConfig};
use core_types::status::{OverallStatus, ServiceStatusHandle, StatusGauge};
use core_types::types::QueryScope;
use flatfile_source::{FlatfileSource, SourceTrait};
use futures::StreamExt;
use greeks_engine::GreeksEngine;
use log::{error, info};
use metrics::Metrics;
use nbbo_cache::NbboStore;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use storage::Storage;
use tokio::sync::{OwnedSemaphorePermit, RwLock as TokioRwLock, Semaphore};
use treasury_ingestion_service::TreasuryServiceHandle;
use uuid::Uuid;

mod checkpoint;
use checkpoint::CheckpointManager;

const EQUITY_DATASET: &str = "equity_trades";
const OPTIONS_DATASET: &str = "options_trades";

/// Flatfile ingestion worker responsible for historical replay.
pub struct FlatfileIngestionService {
    source: Arc<FlatfileSource>,
    metrics: Arc<Metrics>,
    storage: Arc<Mutex<Storage>>,
    nbbo_store: Arc<TokioRwLock<NbboStore>>,
    greeks_cfg: GreeksConfig,
    flatfile_staleness_us: u32,
    concurrent_permits: usize,
    treasury: TreasuryServiceHandle,
    status: ServiceStatusHandle,
    checkpoints: Arc<CheckpointManager>,
}

impl FlatfileIngestionService {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        flatfile_cfg: Arc<FlatfileConfig>,
        ingest_cfg: &IngestConfig,
        metrics: Arc<Metrics>,
        storage: Arc<Mutex<Storage>>,
        nbbo_store: Arc<TokioRwLock<NbboStore>>,
        greeks_cfg: GreeksConfig,
        flatfile_staleness_us: u32,
        concurrent_permits: usize,
        treasury: TreasuryServiceHandle,
    ) -> Self {
        let source = FlatfileSource::new(
            flatfile_cfg,
            Some(metrics.clone()),
            ingest_cfg.batch_size,
            ingest_cfg.progress_update_ms,
        )
        .await;
        let status = ServiceStatusHandle::new("flatfile_ingestion");
        status.set_overall(OverallStatus::Warn);
        status.push_warning("flatfile ingestion not started");
        let checkpoints = Arc::new(CheckpointManager::new("checkpoints"));
        Self {
            source: Arc::new(source),
            metrics,
            storage,
            nbbo_store,
            greeks_cfg,
            flatfile_staleness_us,
            concurrent_permits: concurrent_permits.max(1),
            treasury,
            status,
            checkpoints,
        }
    }

    /// Spawn the ingestion service for the provided date ranges.
    pub fn start(self, date_ranges: Vec<DateRange>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let runner = self.source.clone();
            tokio::spawn(async move {
                runner.run().await;
            });
            self.process_ranges(date_ranges).await;
        })
    }

    pub fn status_handle(&self) -> ServiceStatusHandle {
        self.status.clone()
    }

    async fn process_ranges(&self, date_ranges: Vec<DateRange>) {
        let semaphore = Arc::new(Semaphore::new(self.concurrent_permits));
        let planned_days = self.count_planned_days(&date_ranges);
        self.metrics.add_planned_days(planned_days);
        self.status.clear_warnings_matching(|_| true);
        self.status.set_overall(OverallStatus::Ok);
        Self::update_progress_gauge(&self.status, &self.metrics);
        for range in date_ranges {
            self.process_range(range, semaphore.clone()).await;
        }
    }

    fn count_planned_days(&self, ranges: &[DateRange]) -> u64 {
        let mut total = 0u64;
        for range in ranges {
            if let Some((start, end)) = self.range_bounds(range) {
                let mut day = start;
                while day <= end {
                    total += 1;
                    day = day.succ_opt().unwrap();
                }
            }
        }
        total
    }

    async fn process_range(&self, range: DateRange, semaphore: Arc<Semaphore>) {
        let Some((mut current_date, end_date)) = self.range_bounds(&range) else {
            error!("invalid flatfile range {:?}", range);
            return;
        };
        let status = self.status.clone();
        let checkpoint_manager = self.checkpoints.clone();
        while current_date <= end_date {
            let permit_eq = semaphore.clone().acquire_owned().await.unwrap();
            let scope_eq = self.build_scope(&current_date);
            let source_eq = self.source.clone();
            let metrics_eq = self.metrics.clone();
            let storage_eq = self.storage.clone();
            let status_eq = status.clone();
            let checkpoint_eq = checkpoint_manager.clone();
            tokio::spawn(Self::process_equities_day(
                current_date,
                permit_eq,
                scope_eq,
                source_eq,
                metrics_eq,
                storage_eq,
                status_eq,
                checkpoint_eq,
            ));

            let permit_opt = semaphore.clone().acquire_owned().await.unwrap();
            let scope_opt = self.build_scope(&current_date);
            let source_opt = self.source.clone();
            let metrics_opt = self.metrics.clone();
            let storage_opt = self.storage.clone();
            let nbbo_store = self.nbbo_store.clone();
            let greeks_cfg = self.greeks_cfg.clone();
            let staleness = self.flatfile_staleness_us;
            let treasury = self.treasury.clone();
            let status_opt = status.clone();
            let checkpoint_opt = checkpoint_manager.clone();
            tokio::spawn(async move {
                Self::process_options_day(
                    current_date,
                    permit_opt,
                    scope_opt,
                    source_opt,
                    metrics_opt,
                    storage_opt,
                    nbbo_store,
                    greeks_cfg,
                    staleness,
                    treasury,
                    status_opt,
                    checkpoint_opt,
                )
                .await;
            });

            current_date = current_date.succ_opt().unwrap();
        }
    }

    fn range_bounds(&self, range: &DateRange) -> Option<(NaiveDate, NaiveDate)> {
        let start_ts = range.start_ts_ns().ok()?;
        let end_ts = range
            .end_ts_ns()
            .ok()
            .and_then(|opt| opt)
            .unwrap_or_else(|| Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX));
        let start_dt = DateTime::<Utc>::from_timestamp(
            start_ts / 1_000_000_000,
            (start_ts % 1_000_000_000) as u32,
        )?;
        let end_dt = DateTime::<Utc>::from_timestamp(
            end_ts / 1_000_000_000,
            (end_ts % 1_000_000_000) as u32,
        )?;
        Some((start_dt.naive_utc().date(), end_dt.naive_utc().date()))
    }

    fn build_scope(&self, date: &NaiveDate) -> QueryScope {
        let day_start = date
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .unwrap_or(i64::MIN);
        let day_end = date
            .succ_opt()
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .unwrap_or(i64::MAX)
            .saturating_sub(1);
        QueryScope {
            instruments: vec![],
            time_range: (day_start, day_end),
            mode: "Historical".to_string(),
            quality_target: core_types::types::Quality::Prelim,
        }
    }

    fn update_progress_gauge(status: &ServiceStatusHandle, metrics: &Arc<Metrics>) {
        let planned = metrics.planned_days() as f64;
        let completed = metrics.completed_days() as f64;
        status.set_gauges(vec![StatusGauge {
            label: "flatfile_days_complete".to_string(),
            value: completed,
            max: Some(planned.max(1.0)),
            unit: Some("days".to_string()),
            details: None,
        }]);
    }

    async fn process_equities_day(
        date: NaiveDate,
        _permit: OwnedSemaphorePermit,
        scope: QueryScope,
        source: Arc<FlatfileSource>,
        metrics: Arc<Metrics>,
        storage: Arc<Mutex<Storage>>,
        status: ServiceStatusHandle,
        checkpoint: Arc<CheckpointManager>,
    ) {
        info!("Processing equity day: {}", date.format("%Y-%m-%d"));
        let run_id = format!("raw-equities-{}-{}", date.format("%Y%m%d"), Uuid::new_v4());
        let mut stream = source.get_equity_trades(scope).await;
        let mut batch_count = 0u64;
        let mut row_count = 0u64;
        let mut progress = build_progress_map(checkpoint.load(EQUITY_DATASET, date));
        while let Some(mut batch) = stream.next().await {
            batch.meta.run_id = Some(run_id.clone());
            let mut persist_needed = false;
            batch.rows.retain(|trade| {
                let hour = hour_from_ts_ns(trade.trade_ts_ns);
                match observe_symbol(&mut progress, &trade.symbol, hour) {
                    ProgressAction::Skip => false,
                    ProgressAction::Process { persist } => {
                        persist_needed |= persist;
                        true
                    }
                }
            });
            if persist_needed {
                persist_checkpoint(&checkpoint, EQUITY_DATASET, date, &progress);
            }
            if batch.rows.is_empty() {
                continue;
            }
            batch_count += 1;
            row_count += batch.rows.len() as u64;
            metrics.inc_batches(1);
            metrics.inc_rows(batch.rows.len() as u64);
            if let Err(e) = storage.lock().unwrap().write_equity_trades(&batch) {
                error!("Failed to write equity trades batch: {}", e);
            }
        }
        if finalize_progress(&mut progress) {
            persist_checkpoint(&checkpoint, EQUITY_DATASET, date, &progress);
        }
        metrics.inc_completed_day();
        Self::update_progress_gauge(&status, &metrics);
        metrics.set_flatfile_status(format!(
            "Ingested equities day: {} ({} batches, {} rows)",
            date.format("%Y-%m-%d"),
            batch_count,
            row_count
        ));
        if let Err(err) = storage
            .lock()
            .unwrap()
            .finalize_manifest(EQUITY_DATASET, date, &run_id)
        {
            error!(
                "Failed to finalize equity manifest for {}: {}",
                date.format("%Y-%m-%d"),
                err
            );
        }
    }

    async fn process_options_day(
        date: NaiveDate,
        _permit: OwnedSemaphorePermit,
        scope: QueryScope,
        source: Arc<FlatfileSource>,
        metrics: Arc<Metrics>,
        storage: Arc<Mutex<storage::Storage>>,
        nbbo_store: Arc<TokioRwLock<NbboStore>>,
        greeks_cfg: GreeksConfig,
        flatfile_staleness_us: u32,
        treasury: TreasuryServiceHandle,
        status: ServiceStatusHandle,
        checkpoint: Arc<CheckpointManager>,
    ) {
        info!("Seeding NBBO for options day: {}", date.format("%Y-%m-%d"));
        let run_id = format!("raw-options-{}-{}", date.format("%Y%m%d"), Uuid::new_v4());
        let mut nbbo_stream = source.get_nbbo(scope.clone()).await;
        let mut last_ts = None;
        while let Some(batch) = nbbo_stream.next().await {
            let mut guard = nbbo_store.write().await;
            for q in batch.rows.iter() {
                guard.put(q);
            }
            if let Some(q) = batch.rows.last() {
                last_ts = Some(q.quote_ts_ns);
            }
            if let Some(ts) = last_ts {
                guard.prune_before(ts.saturating_sub(2_000_000_000));
            }
        }
        info!("Seeding option NBBO for day: {}", date.format("%Y-%m-%d"));
        let mut opt_nbbo_stream = source.get_option_nbbo(scope.clone()).await;
        let mut last_opt_ts = None;
        while let Some(batch) = opt_nbbo_stream.next().await {
            let mut guard = nbbo_store.write().await;
            for q in batch.rows.iter() {
                guard.put(q);
            }
            if let Some(q) = batch.rows.last() {
                last_opt_ts = Some(q.quote_ts_ns);
            }
            if let Some(ts) = last_opt_ts {
                guard.prune_before(ts.saturating_sub(2_000_000_000));
            }
        }
        info!("Processing options (OPRA) day: {}", date.format("%Y-%m-%d"));
        let curve_selection = treasury.curve_state_for_date_with_metadata(date).await;
        let day_curve_state = curve_selection.state.clone();
        {
            let curve_guard = day_curve_state.read().await;
            if curve_guard.is_none() {
                let msg = format!("missing treasury curve for {}", date);
                error!("{}", msg);
                status.set_overall(OverallStatus::Crit);
                status.push_error(msg.clone());
                metrics.flag_outdated_dependency(
                    OPTIONS_DATASET,
                    date,
                    "treasury_curve",
                    None,
                    Some(msg),
                );
            } else {
                status.clear_errors_matching(|m| m.contains("treasury curve"));
                status.set_overall(OverallStatus::Ok);
                if let Some(source_date) = curve_selection.source_date {
                    if source_date < date {
                        let lag = date.signed_duration_since(source_date).num_days();
                        metrics.flag_outdated_dependency(
                            OPTIONS_DATASET,
                            date,
                            "treasury_curve",
                            Some(source_date),
                            Some(format!(
                                "curve lagged by {} day(s); latest available {}",
                                lag, source_date
                            )),
                        );
                    } else {
                        metrics.clear_outdated_dependency(OPTIONS_DATASET, date);
                    }
                }
            }
        }
        let greeks_engine = GreeksEngine::new(
            greeks_cfg,
            nbbo_store,
            flatfile_staleness_us,
            day_curve_state,
        );
        let mut stream = source.get_option_trades(scope).await;
        let mut batch_count = 0u64;
        let mut row_count = 0u64;
        let mut progress = build_progress_map(checkpoint.load(OPTIONS_DATASET, date));
        while let Some(mut batch) = stream.next().await {
            batch.meta.run_id = Some(run_id.clone());
            let mut persist_needed = false;
            batch.rows.retain(|trade| {
                let symbol = if trade.underlying.is_empty() {
                    trade.contract.as_str()
                } else {
                    trade.underlying.as_str()
                };
                let hour = hour_from_ts_ns(trade.trade_ts_ns);
                match observe_symbol(&mut progress, symbol, hour) {
                    ProgressAction::Skip => false,
                    ProgressAction::Process { persist } => {
                        persist_needed |= persist;
                        true
                    }
                }
            });
            if persist_needed {
                persist_checkpoint(&checkpoint, OPTIONS_DATASET, date, &progress);
            }
            if batch.rows.is_empty() {
                continue;
            }
            batch_count += 1;
            row_count += batch.rows.len() as u64;
            metrics.inc_batches(1);
            metrics.inc_rows(batch.rows.len() as u64);
            let start = Instant::now();
            greeks_engine.enrich_batch(&mut batch.rows).await;
            metrics.observe_enrichment(batch.rows.len(), start.elapsed());
            if let Err(e) = storage.lock().unwrap().write_option_trades(&batch) {
                error!("Failed to write option trades batch: {}", e);
            }
        }
        if finalize_progress(&mut progress) {
            persist_checkpoint(&checkpoint, OPTIONS_DATASET, date, &progress);
        }
        metrics.inc_completed_day();
        Self::update_progress_gauge(&status, &metrics);
        metrics.set_flatfile_status(format!(
            "Ingested options day: {} ({} batches, {} rows)",
            date.format("%Y-%m-%d"),
            batch_count,
            row_count
        ));
        if let Err(err) = storage
            .lock()
            .unwrap()
            .finalize_manifest(OPTIONS_DATASET, date, &run_id)
        {
            error!(
                "Failed to finalize options manifest for {}: {}",
                date.format("%Y-%m-%d"),
                err
            );
        }
    }
}

#[derive(Clone, Debug)]
struct SymbolProgress {
    last_completed: i16,
    current_hour: Option<i16>,
}

impl SymbolProgress {
    fn new(initial: i16) -> Self {
        Self {
            last_completed: initial,
            current_hour: None,
        }
    }
}

enum ProgressAction {
    Skip,
    Process { persist: bool },
}

fn build_progress_map(existing: HashMap<String, i16>) -> HashMap<String, SymbolProgress> {
    existing
        .into_iter()
        .map(|(symbol, hour)| (symbol, SymbolProgress::new(hour)))
        .collect()
}

fn observe_symbol(
    progress: &mut HashMap<String, SymbolProgress>,
    symbol: &str,
    hour: i16,
) -> ProgressAction {
    let entry = progress
        .entry(symbol.to_string())
        .or_insert_with(|| SymbolProgress::new(-1));
    if hour <= entry.last_completed {
        return ProgressAction::Skip;
    }
    let mut persist = false;
    match entry.current_hour {
        Some(current) if hour > current => {
            let completed = hour.saturating_sub(1);
            if completed > entry.last_completed {
                entry.last_completed = completed;
                persist = true;
            }
            entry.current_hour = Some(hour);
        }
        None => {
            entry.current_hour = Some(hour);
        }
        _ => {}
    }
    ProgressAction::Process { persist }
}

fn finalize_progress(progress: &mut HashMap<String, SymbolProgress>) -> bool {
    let mut changed = false;
    for entry in progress.values_mut() {
        if let Some(current) = entry.current_hour.take() {
            if current > entry.last_completed {
                entry.last_completed = current;
                changed = true;
            }
        }
    }
    changed
}

fn snapshot_progress(progress: &HashMap<String, SymbolProgress>) -> HashMap<String, i16> {
    progress
        .iter()
        .filter_map(|(symbol, state)| {
            if state.last_completed >= 0 {
                Some((symbol.clone(), state.last_completed))
            } else {
                None
            }
        })
        .collect()
}

fn persist_checkpoint(
    manager: &CheckpointManager,
    dataset: &str,
    date: NaiveDate,
    progress: &HashMap<String, SymbolProgress>,
) {
    let snapshot = snapshot_progress(progress);
    manager.save(dataset, date, &snapshot);
}

fn hour_from_ts_ns(ts_ns: i64) -> i16 {
    let secs = ts_ns / 1_000_000_000;
    let nanos = (ts_ns % 1_000_000_000) as u32;
    match DateTime::<Utc>::from_timestamp(secs, nanos) {
        Some(dt) => dt.hour() as i16,
        None => 0,
    }
}
