use chrono::{DateTime, NaiveDate, Utc};
use classifier::greeks::GreeksEngine;
use core_types::config::{DateRange, FlatfileConfig, GreeksConfig, IngestConfig};
use core_types::status::{OverallStatus, ServiceStatusHandle, StatusGauge};
use core_types::types::QueryScope;
use flatfile_source::{FlatfileSource, SourceTrait};
use futures::StreamExt;
use log::{error, info};
use metrics::Metrics;
use nbbo_cache::NbboStore;
use std::sync::{Arc, Mutex};
use storage::Storage;
use tokio::sync::{OwnedSemaphorePermit, RwLock as TokioRwLock, Semaphore};
use treasury_ingestion_service::TreasuryServiceHandle;

/// Flatfile ingestion worker responsible for historical replay.
pub struct FlatfileIngestionService {
    source: Arc<FlatfileSource>,
    metrics: Arc<Metrics>,
    storage: Arc<Mutex<Storage>>,
    nbbo_store: Arc<TokioRwLock<NbboStore>>,
    greeks_cfg: GreeksConfig,
    flatfile_staleness_us: u32,
    concurrent_days: usize,
    treasury: TreasuryServiceHandle,
    status: ServiceStatusHandle,
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
        concurrent_days: usize,
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
        Self {
            source: Arc::new(source),
            metrics,
            storage,
            nbbo_store,
            greeks_cfg,
            flatfile_staleness_us,
            concurrent_days: concurrent_days.max(1),
            treasury,
            status,
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
        let semaphore = Arc::new(Semaphore::new(self.concurrent_days));
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
        while current_date <= end_date {
            let permit_eq = semaphore.clone().acquire_owned().await.unwrap();
            let scope_eq = self.build_scope(&current_date);
            let source_eq = self.source.clone();
            let metrics_eq = self.metrics.clone();
            let storage_eq = self.storage.clone();
            let status_eq = status.clone();
            tokio::spawn(Self::process_equities_day(
                current_date,
                permit_eq,
                scope_eq,
                source_eq,
                metrics_eq,
                storage_eq,
                status_eq,
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
    ) {
        info!("Processing equity day: {}", date.format("%Y-%m-%d"));
        let mut stream = source.get_equity_trades(scope).await;
        let mut batch_count = 0u64;
        let mut row_count = 0u64;
        while let Some(batch) = stream.next().await {
            batch_count += 1;
            row_count += batch.rows.len() as u64;
            metrics.inc_batches(1);
            metrics.inc_rows(batch.rows.len() as u64);
            if let Err(e) = storage.lock().unwrap().write_equity_trades(&batch) {
                error!("Failed to write equity trades batch: {}", e);
            }
        }
        metrics.inc_completed_day();
        Self::update_progress_gauge(&status, &metrics);
        metrics.set_flatfile_status(format!(
            "Ingested equities day: {} ({} batches, {} rows)",
            date.format("%Y-%m-%d"),
            batch_count,
            row_count
        ));
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
    ) {
        info!("Seeding NBBO for options day: {}", date.format("%Y-%m-%d"));
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
        info!("Processing options (OPRA) day: {}", date.format("%Y-%m-%d"));
        let day_curve_state = {
            let state = treasury.curve_state_for_date(date).await;
            if state.read().await.is_none() {
                let msg = format!("missing treasury curve for {}", date);
                error!("{}", msg);
                status.set_overall(OverallStatus::Crit);
                status.push_error(msg);
            } else {
                status.clear_errors_matching(|m| m.contains("treasury curve"));
                status.set_overall(OverallStatus::Ok);
            }
            state
        };
        let greeks_engine = GreeksEngine::new(
            greeks_cfg,
            nbbo_store,
            flatfile_staleness_us,
            day_curve_state,
        );
        let mut stream = source.get_option_trades(scope).await;
        let mut batch_count = 0u64;
        let mut row_count = 0u64;
        while let Some(mut batch) = stream.next().await {
            batch_count += 1;
            row_count += batch.rows.len() as u64;
            metrics.inc_batches(1);
            metrics.inc_rows(batch.rows.len() as u64);
            greeks_engine.enrich_batch(&mut batch.rows).await;
            if let Err(e) = storage.lock().unwrap().write_option_trades(&batch) {
                error!("Failed to write option trades batch: {}", e);
            }
        }
        metrics.inc_completed_day();
        Self::update_progress_gauge(&status, &metrics);
        metrics.set_flatfile_status(format!(
            "Ingested options day: {} ({} batches, {} rows)",
            date.format("%Y-%m-%d"),
            batch_count,
            row_count
        ));
    }
}
