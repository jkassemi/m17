mod source;
mod state;
mod writer;

pub use source::{BatchStream, GreeksEnrichmentSource, ManifestParquetSource};
pub use state::{
    EnrichmentBlockedReason, EnrichmentCheckpoint, EnrichmentRunState, EnrichmentStateError,
};
pub use writer::{GreeksOverlayWriter, StorageGreeksWriter};

use crate::source::GreeksEnrichmentSource as SourceTrait;
use crate::state::EnrichmentStateStore;
use crate::writer::GreeksOverlayWriter as WriterTrait;
use chrono::{NaiveDate, Utc};
use core_types::config::GreeksConfig;
use core_types::status::{OverallStatus, ServiceStatusHandle, StatusGauge};
use core_types::types::{
    EnrichmentDataset, GreeksOverlayRow, Nbbo, NbboState, OptionTrade, Quality, Source,
};
use core_types::uid::quote_uid;
use futures::StreamExt;
use greeks_engine::GreeksEngine;
use log::{error, info};
use nbbo_cache::NbboStore;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{mpsc, RwLock as TokioRwLock, Semaphore};
use tokio::task::JoinHandle;
use treasury_ingestion_service::{CurveStateSelection, TreasuryServiceHandle};

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[async_trait::async_trait]
pub trait CurveStateProvider: Send + Sync + 'static {
    async fn curve_state_for_date_with_metadata(&self, date: NaiveDate) -> CurveStateSelection;
}

#[async_trait::async_trait]
impl CurveStateProvider for TreasuryServiceHandle {
    async fn curve_state_for_date_with_metadata(&self, date: NaiveDate) -> CurveStateSelection {
        TreasuryServiceHandle::curve_state_for_date_with_metadata(self, date).await
    }
}

/// Message pushed into the enrichment queue.
#[derive(Debug, Clone)]
pub struct GreeksEnrichmentTask {
    pub dataset: EnrichmentDataset,
    pub date: NaiveDate,
    pub run_id: String,
}

impl GreeksEnrichmentTask {
    pub fn new(dataset: EnrichmentDataset, date: NaiveDate, run_id: impl Into<String>) -> Self {
        Self {
            dataset,
            date,
            run_id: run_id.into(),
        }
    }
}

/// Async handle used by orchestrator components to enqueue new runs.
#[derive(Clone)]
pub struct GreeksEnrichmentHandle {
    tx: mpsc::Sender<GreeksEnrichmentTask>,
    state_store: Arc<EnrichmentStateStore>,
}

impl GreeksEnrichmentHandle {
    pub async fn enqueue(&self, task: GreeksEnrichmentTask) -> Result<(), EnqueueError> {
        self.state_store.write_state(
            task.dataset,
            task.date,
            EnrichmentRunState::Pending {
                run_id: task.run_id.clone(),
            },
        )?;
        self.tx
            .send(task)
            .await
            .map_err(|_| EnqueueError::ChannelClosed)
    }
}

#[derive(Debug, Error)]
pub enum EnqueueError {
    #[error("failed to persist enrichment checkpoint: {0}")]
    State(#[from] EnrichmentStateError),
    #[error("service is shutting down")]
    ChannelClosed,
}

/// Public service wrapper responsible for wiring queues and exposing status.
pub struct GreeksEnrichmentService {
    status: ServiceStatusHandle,
    tx: mpsc::Sender<GreeksEnrichmentTask>,
    state_store: Arc<EnrichmentStateStore>,
    _runtime: JoinHandle<()>,
}

impl GreeksEnrichmentService {
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        cfg: GreeksConfig,
        staleness_us: u32,
        curve_provider: Arc<dyn CurveStateProvider>,
        source: Arc<dyn SourceTrait>,
        writer: Arc<dyn WriterTrait>,
        state_dir: impl Into<PathBuf>,
        max_concurrency: usize,
    ) -> Self {
        let status = ServiceStatusHandle::new("greeks_enrichment");
        status.set_overall(OverallStatus::Warn);
        status.push_warning("greeks enrichment idle");
        let (tx, rx) = mpsc::channel(512);
        let state_store = Arc::new(EnrichmentStateStore::new(state_dir));
        let inner = Arc::new(ServiceInner {
            cfg,
            staleness_us,
            curve_provider,
            source,
            writer,
            state_store: state_store.clone(),
            status: status.clone(),
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
        });
        let runtime = ServiceRuntime::spawn(inner, rx, max_concurrency.max(1));
        Self {
            status,
            tx,
            state_store,
            _runtime: runtime,
        }
    }

    pub fn handle(&self) -> GreeksEnrichmentHandle {
        GreeksEnrichmentHandle {
            tx: self.tx.clone(),
            state_store: self.state_store.clone(),
        }
    }

    pub fn status_handle(&self) -> ServiceStatusHandle {
        self.status.clone()
    }
}

struct ServiceInner {
    cfg: GreeksConfig,
    staleness_us: u32,
    curve_provider: Arc<dyn CurveStateProvider>,
    source: Arc<dyn SourceTrait>,
    writer: Arc<dyn WriterTrait>,
    state_store: Arc<EnrichmentStateStore>,
    status: ServiceStatusHandle,
    engine_version: String,
}

struct ServiceRuntime;

impl ServiceRuntime {
    fn spawn(
        inner: Arc<ServiceInner>,
        mut rx: mpsc::Receiver<GreeksEnrichmentTask>,
        max_concurrency: usize,
    ) -> JoinHandle<()> {
        let semaphore = Arc::new(Semaphore::new(max_concurrency));
        tokio::spawn(async move {
            while let Some(task) = rx.recv().await {
                let permit = match semaphore.clone().acquire_owned().await {
                    Ok(permit) => permit,
                    Err(_) => break,
                };
                let inner_clone = inner.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let task_clone = task.clone();
                    if let Err(err) = inner_clone.process_task(task).await {
                        inner_clone.handle_error(&task_clone, err);
                    }
                });
            }
        })
    }
}

impl ServiceInner {
    async fn process_task(&self, task: GreeksEnrichmentTask) -> Result<(), GreeksEnrichmentError> {
        self.state_store.write_state(
            task.dataset,
            task.date,
            EnrichmentRunState::InFlight {
                run_id: task.run_id.clone(),
            },
        )?;
        let curve_selection = self
            .curve_provider
            .curve_state_for_date_with_metadata(task.date)
            .await;
        {
            let guard = curve_selection.state.read().await;
            if guard.is_none() {
                self.state_store.write_state(
                    task.dataset,
                    task.date,
                    EnrichmentRunState::Blocked {
                        reason: EnrichmentBlockedReason::MissingTreasuryCurve,
                        run_id: Some(task.run_id.clone()),
                    },
                )?;
                return Err(GreeksEnrichmentError::MissingDependency(
                    "treasury curve unavailable".to_string(),
                ));
            }
        }

        let nbbo_store = Arc::new(TokioRwLock::new(NbboStore::new()));
        let greeks_engine = GreeksEngine::new(
            self.cfg.clone(),
            nbbo_store.clone(),
            self.staleness_us,
            curve_selection.state.clone(),
        );
        let mut stream = self
            .source
            .stream_batches(task.dataset, task.date, &task.run_id)
            .await
            .map_err(|err| GreeksEnrichmentError::Source(err.to_string()))?;
        let mut total_rows = 0usize;
        let curve_date = curve_selection
            .source_date
            .map(|d| d.format("%Y-%m-%d").to_string());
        let requested_date = task.date.format("%Y-%m-%d").to_string();
        while let Some(batch_result) = stream.next().await {
            let mut batch =
                batch_result.map_err(|err| GreeksEnrichmentError::Source(err.to_string()))?;
            if batch.rows.is_empty() {
                continue;
            }
            preload_nbbo_store(nbbo_store.clone(), &batch.rows).await;
            greeks_engine.enrich_batch(&mut batch.rows).await;
            clear_nbbo_store(nbbo_store.clone()).await;
            total_rows += batch.rows.len();
            let enriched_at_ns = current_time_ns();
            let rows: Vec<GreeksOverlayRow> = batch
                .rows
                .into_iter()
                .map(|trade| {
                    build_overlay_row(
                        trade,
                        &self.engine_version,
                        curve_date.as_deref(),
                        &requested_date,
                        &task.run_id,
                        enriched_at_ns,
                    )
                })
                .collect();
            self.writer
                .write_batch(task.dataset, task.date, &task.run_id, &rows)
                .map_err(|err| GreeksEnrichmentError::Writer(err.to_string()))?;
        }
        self.writer
            .finalize_run(task.dataset, task.date, &task.run_id)
            .map_err(|err| GreeksEnrichmentError::Writer(err.to_string()))?;
        self.state_store.write_state(
            task.dataset,
            task.date,
            EnrichmentRunState::Published {
                run_id: task.run_id.clone(),
            },
        )?;
        self.update_status_on_success(&task, total_rows);
        info!(
            "published {} derived Greeks rows for {} run {}",
            total_rows, task.dataset, task.run_id
        );
        Ok(())
    }

    fn handle_error(&self, task: &GreeksEnrichmentTask, err: GreeksEnrichmentError) {
        error!(
            "greeks enrichment task {} {} failed: {}",
            task.dataset, task.date, err
        );
        if let Some(reason) = err.as_blocked_reason() {
            if let Err(state_err) = self.state_store.write_state(
                task.dataset,
                task.date,
                EnrichmentRunState::Blocked {
                    reason,
                    run_id: Some(task.run_id.clone()),
                },
            ) {
                error!(
                    "failed to persist blocked state for {} {}: {}",
                    task.dataset, task.date, state_err
                );
            }
        }
        self.status.set_overall(OverallStatus::Warn);
        self.status.clear_errors_matching(|_| true);
        self.status
            .push_error(format!("{} {} failed: {}", task.dataset, task.date, err));
    }

    fn update_status_on_success(&self, task: &GreeksEnrichmentTask, rows: usize) {
        self.status
            .clear_warnings_matching(|msg| msg.contains("blocked"));
        self.status.clear_errors_matching(|_| true);
        self.status.set_overall(OverallStatus::Ok);
        self.status.set_gauges(vec![StatusGauge {
            label: format!("{}_last_rows", task.dataset),
            value: rows as f64,
            max: None,
            unit: Some("rows".into()),
            details: Some(format!(
                "published {} rows for {} run {}",
                rows, task.dataset, task.run_id
            )),
        }]);
    }
}

fn build_overlay_row(
    trade: OptionTrade,
    engine_version: &str,
    curve_date: Option<&str>,
    requested_date: &str,
    run_id: &str,
    enriched_at_ns: i64,
) -> GreeksOverlayRow {
    GreeksOverlayRow {
        trade_uid: trade.trade_uid,
        trade_ts_ns: trade.trade_ts_ns,
        contract: trade.contract,
        underlying: trade.underlying,
        delta: trade.delta,
        gamma: trade.gamma,
        vega: trade.vega,
        theta: trade.theta,
        iv: trade.iv,
        greeks_flags: trade.greeks_flags,
        nbbo_bid: trade.nbbo_bid,
        nbbo_ask: trade.nbbo_ask,
        nbbo_bid_sz: trade.nbbo_bid_sz,
        nbbo_ask_sz: trade.nbbo_ask_sz,
        nbbo_ts_ns: trade.nbbo_ts_ns,
        nbbo_age_us: trade.nbbo_age_us,
        nbbo_state: trade.nbbo_state,
        engine_version: engine_version.to_string(),
        treasury_curve_date: curve_date.map(|s| s.to_string()),
        treasury_curve_source_date: Some(requested_date.to_string()),
        enriched_at_ns,
        run_id: run_id.to_string(),
    }
}

async fn preload_nbbo_store(store: Arc<TokioRwLock<NbboStore>>, trades: &[OptionTrade]) {
    let mut guard = store.write().await;
    for trade in trades {
        if let Some(nbbo) = nbbo_from_trade(trade) {
            guard.put(&nbbo);
        }
    }
}

async fn clear_nbbo_store(store: Arc<TokioRwLock<NbboStore>>) {
    let mut guard = store.write().await;
    guard.data.clear();
}

fn nbbo_from_trade(trade: &OptionTrade) -> Option<Nbbo> {
    let bid = trade.underlying_nbbo_bid.or(trade.nbbo_bid)?;
    let ask = trade.underlying_nbbo_ask.or(trade.nbbo_ask)?;
    let quote_ts = trade.underlying_nbbo_ts_ns.or(trade.nbbo_ts_ns)?;
    let bid_sz = trade
        .underlying_nbbo_bid_sz
        .or(trade.nbbo_bid_sz)
        .unwrap_or(0);
    let ask_sz = trade
        .underlying_nbbo_ask_sz
        .or(trade.nbbo_ask_sz)
        .unwrap_or(0);
    let quote_uid = quote_uid(
        &trade.underlying,
        quote_ts,
        None,
        bid,
        ask,
        bid_sz,
        ask_sz,
        None,
        None,
        None,
    );
    Some(Nbbo {
        instrument_id: trade.underlying.clone(),
        quote_uid,
        quote_ts_ns: quote_ts,
        bid,
        ask,
        bid_sz,
        ask_sz,
        state: trade
            .underlying_nbbo_state
            .clone()
            .or_else(|| trade.nbbo_state.clone())
            .unwrap_or(NbboState::Normal),
        condition: None,
        best_bid_venue: None,
        best_ask_venue: None,
        source: Source::Flatfile,
        quality: Quality::Prelim,
        watermark_ts_ns: trade.watermark_ts_ns,
    })
}

#[derive(Debug, Error)]
pub enum GreeksEnrichmentError {
    #[error("source error: {0}")]
    Source(String),
    #[error("writer error: {0}")]
    Writer(String),
    #[error("missing dependency: {0}")]
    MissingDependency(String),
    #[error("state error: {0}")]
    State(#[from] EnrichmentStateError),
}

fn current_time_ns() -> i64 {
    Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000)
}

impl GreeksEnrichmentError {
    fn as_blocked_reason(&self) -> Option<EnrichmentBlockedReason> {
        match self {
            GreeksEnrichmentError::Source(msg) => Some(EnrichmentBlockedReason::SourceFailure(
                Some(trim_reason(msg)),
            )),
            GreeksEnrichmentError::Writer(msg) => Some(EnrichmentBlockedReason::WriterFailure(
                Some(trim_reason(msg)),
            )),
            GreeksEnrichmentError::MissingDependency(_) => None,
            GreeksEnrichmentError::State(_) => None,
        }
    }
}

fn trim_reason(reason: &str) -> String {
    const MAX_LEN: usize = 256;
    if reason.len() <= MAX_LEN {
        reason.to_string()
    } else {
        format!("{}…", &reason[..MAX_LEN])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use core_types::types::{
        AggressorSide, ClassMethod, Completeness, DataBatch, DataBatchMeta, OptionTrade, Quality,
        Source, Watermark,
    };
    use greeks_engine::TreasuryCurve;
    use std::sync::Mutex;
    use tempfile::tempdir;
    use tokio::sync::RwLock;
    use tokio_stream::wrappers::ReceiverStream;

    #[derive(Clone)]
    struct InMemorySource {
        batches: Arc<Vec<DataBatch<OptionTrade>>>,
    }

    #[async_trait]
    impl SourceTrait for InMemorySource {
        async fn stream_batches(
            &self,
            _dataset: EnrichmentDataset,
            _date: NaiveDate,
            _run_id: &str,
        ) -> Result<BatchStream, BoxError> {
            let (tx, rx) = tokio::sync::mpsc::channel(self.batches.len());
            for batch in self.batches.iter().cloned() {
                tx.send(Ok(batch)).await.unwrap();
            }
            drop(tx);
            Ok(Box::pin(ReceiverStream::new(rx)))
        }
    }

    #[derive(Clone, Default)]
    struct InMemoryWriter {
        rows: Arc<Mutex<Vec<GreeksOverlayRow>>>,
        finalized: Arc<Mutex<Vec<(EnrichmentDataset, NaiveDate, String)>>>,
    }

    impl InMemoryWriter {
        fn rows(&self) -> Vec<GreeksOverlayRow> {
            self.rows.lock().unwrap().clone()
        }
    }

    impl WriterTrait for InMemoryWriter {
        fn write_batch(
            &self,
            dataset: EnrichmentDataset,
            date: NaiveDate,
            run_id: &str,
            rows: &[GreeksOverlayRow],
        ) -> Result<(), BoxError> {
            let mut guard = self.rows.lock().unwrap();
            guard.extend_from_slice(rows);
            self.finalized
                .lock()
                .unwrap()
                .retain(|entry| entry.0 != dataset || entry.1 != date || entry.2 != run_id);
            Ok(())
        }

        fn finalize_run(
            &self,
            dataset: EnrichmentDataset,
            date: NaiveDate,
            run_id: &str,
        ) -> Result<(), BoxError> {
            self.finalized
                .lock()
                .unwrap()
                .push((dataset, date, run_id.to_string()));
            Ok(())
        }
    }

    struct StubCurveProvider {
        selection: CurveStateSelection,
    }

    #[async_trait]
    impl CurveStateProvider for StubCurveProvider {
        async fn curve_state_for_date_with_metadata(
            &self,
            _date: NaiveDate,
        ) -> CurveStateSelection {
            CurveStateSelection {
                state: self.selection.state.clone(),
                source_date: self.selection.source_date,
            }
        }
    }

    fn sample_trade() -> OptionTrade {
        OptionTrade {
            contract: "O:SPY240621C00450000".to_string(),
            trade_uid: [1u8; 16],
            contract_direction: 'C',
            strike_price: 450.0,
            underlying: "SPY".to_string(),
            trade_ts_ns: 1_700_000_000_000_000,
            price: 1.5,
            size: 10,
            conditions: vec![],
            exchange: 12,
            expiry_ts_ns: 1_720_000_000_000_000,
            aggressor_side: AggressorSide::Buyer,
            class_method: ClassMethod::NbboTouch,
            aggressor_offset_mid_bp: None,
            aggressor_offset_touch_ticks: None,
            aggressor_confidence: None,
            nbbo_bid: Some(1.4),
            nbbo_ask: Some(1.6),
            nbbo_bid_sz: Some(5),
            nbbo_ask_sz: Some(5),
            nbbo_ts_ns: Some(1_700_000_000_000_000),
            nbbo_age_us: Some(10),
            nbbo_state: None,
            underlying_nbbo_bid: Some(450.0),
            underlying_nbbo_ask: Some(450.5),
            underlying_nbbo_bid_sz: Some(100),
            underlying_nbbo_ask_sz: Some(200),
            underlying_nbbo_ts_ns: Some(1_700_000_000_000_000),
            underlying_nbbo_age_us: Some(8),
            underlying_nbbo_state: None,
            tick_size_used: None,
            delta: None,
            gamma: None,
            vega: None,
            theta: None,
            iv: None,
            greeks_flags: 0,
            source: Source::Flatfile,
            quality: Quality::Prelim,
            watermark_ts_ns: 1_700_000_000_000_000,
        }
    }

    fn sample_batch(run_id: &str) -> DataBatch<OptionTrade> {
        DataBatch {
            rows: vec![sample_trade()],
            meta: DataBatchMeta {
                source: Source::Flatfile,
                quality: Quality::Prelim,
                watermark: Watermark {
                    watermark_ts_ns: 1_700_000_000_000_000,
                    completeness: Completeness::Complete,
                    hints: None,
                },
                schema_version: 1,
                run_id: Some(run_id.to_string()),
            },
        }
    }

    fn stub_curve_provider(date: NaiveDate) -> Arc<dyn CurveStateProvider> {
        let curve = TreasuryCurve::from_pairs(vec![(1.0, 0.03)]).unwrap();
        let latest = Arc::new(RwLock::new(Some(Arc::new(curve))));
        Arc::new(StubCurveProvider {
            selection: CurveStateSelection {
                state: latest,
                source_date: Some(date),
            },
        })
    }

    #[tokio::test]
    async fn processes_task_and_writes_overlay() {
        let date = NaiveDate::from_ymd_opt(2024, 6, 10).unwrap();
        let source = Arc::new(InMemorySource {
            batches: Arc::new(vec![sample_batch("run-1")]),
        });
        let writer = Arc::new(InMemoryWriter::default());
        let tmpdir = tempdir().unwrap();
        let service = GreeksEnrichmentService::start(
            GreeksConfig::default(),
            1_000_000,
            stub_curve_provider(date),
            source,
            writer.clone(),
            tmpdir.path(),
            1,
        );
        let handle = service.handle();
        handle
            .enqueue(GreeksEnrichmentTask::new(
                EnrichmentDataset::Options,
                date,
                "run-1",
            ))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(writer.rows().len(), 1);
    }
}
