mod metrics;

pub use metrics::{QuoteBackfillMetrics, QuoteBackfillMetricsSnapshot};

use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{self, Read},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, Ordering as AtomicOrdering},
        Arc,
    },
    time::{Duration, Instant},
};

use arrow::{
    array::{
        Array, ArrayRef, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array, StringArray,
        UInt32Array,
    },
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use chrono::Utc;
use core_types::{
    raw::QuoteRecord,
    schema::nbbo_schema,
    types::{NbboState, Quality, Source},
    uid::quote_uid,
};
use crc32fast::Hasher as Crc32;
use engine_api::{
    Engine, EngineError, EngineHealth, EngineResult, HealthStatus, PriorityHookDescription,
};
use log::{debug, error, info, warn};
use parking_lot::Mutex;
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    errors::ParquetError,
};
use reqwest::{Client, Url};
use serde::Deserialize;
use thiserror::Error;
use time::OffsetDateTime;
use tokio::{runtime::Runtime, task::JoinHandle, time::sleep};
use tokio_util::sync::CancellationToken;
use window_space::{
    mapping::QuoteBatchPayload,
    payload::{PayloadMeta, PayloadType, SlotStatus},
    SlotWriteError, WindowIndex, WindowSpaceController, WindowSpaceError,
};

const QUOTE_SCHEMA_VERSION: u8 = 1;
const DEFAULT_POLL_SECS: u64 = 30;
const DEFAULT_WINDOW_AGE_SECS: u64 = 60;
const DEFAULT_MAX_WINDOWS_PER_CYCLE: usize = 32;
const DEFAULT_PAGE_LIMIT: usize = 5_000;

#[derive(Clone)]
pub struct QuoteBackfillConfig {
    pub label: String,
    pub state_dir: PathBuf,
    pub rest_base_url: String,
    pub api_key: String,
    pub poll_interval: Duration,
    pub min_window_age: Duration,
    pub max_windows_per_cycle: usize,
    pub page_limit: usize,
}

impl QuoteBackfillConfig {
    pub fn new(
        label: impl Into<String>,
        state_dir: impl Into<PathBuf>,
        rest_base_url: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Self {
        Self {
            label: label.into(),
            state_dir: state_dir.into(),
            rest_base_url: rest_base_url.into(),
            api_key: api_key.into(),
            poll_interval: Duration::from_secs(DEFAULT_POLL_SECS),
            min_window_age: Duration::from_secs(DEFAULT_WINDOW_AGE_SECS),
            max_windows_per_cycle: DEFAULT_MAX_WINDOWS_PER_CYCLE,
            page_limit: DEFAULT_PAGE_LIMIT,
        }
    }

    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    pub fn with_min_window_age(mut self, age: Duration) -> Self {
        self.min_window_age = age;
        self
    }

    pub fn with_max_windows_per_cycle(mut self, max: usize) -> Self {
        self.max_windows_per_cycle = max.max(1);
        self
    }

    pub fn with_page_limit(mut self, limit: usize) -> Self {
        self.page_limit = limit.clamp(1, 50_000);
        self
    }
}

pub struct QuoteBackfillEngine {
    inner: Arc<QuoteBackfillInner>,
}

impl QuoteBackfillEngine {
    pub fn new(
        config: QuoteBackfillConfig,
        controller: Arc<WindowSpaceController>,
        metrics: Arc<QuoteBackfillMetrics>,
    ) -> Self {
        Self {
            inner: QuoteBackfillInner::new(config, controller, metrics),
        }
    }
}

impl Engine for QuoteBackfillEngine {
    fn start(&self) -> EngineResult<()> {
        self.inner.start()
    }

    fn stop(&self) -> EngineResult<()> {
        self.inner.stop()
    }

    fn health(&self) -> EngineHealth {
        self.inner.health()
    }

    fn describe_priority_hooks(&self) -> PriorityHookDescription {
        PriorityHookDescription {
            supports_priority_regions: false,
            notes: Some("Backfills option quotes via REST when websocket coverage lags".into()),
        }
    }
}

struct QuoteBackfillInner {
    config: QuoteBackfillConfig,
    controller: Arc<WindowSpaceController>,
    client: Client,
    state: Mutex<EngineRuntimeState>,
    health: Mutex<EngineHealth>,
    batch_seq: AtomicU32,
    metrics: Arc<QuoteBackfillMetrics>,
}

impl QuoteBackfillInner {
    fn new(
        config: QuoteBackfillConfig,
        controller: Arc<WindowSpaceController>,
        metrics: Arc<QuoteBackfillMetrics>,
    ) -> Arc<Self> {
        let client = Client::builder()
            .user_agent("quote-backfill-engine/0.1")
            .build()
            .expect("reqwest client");
        Arc::new(Self {
            config,
            controller,
            client,
            state: Mutex::new(EngineRuntimeState::Stopped),
            health: Mutex::new(EngineHealth::new(
                HealthStatus::Stopped,
                Some("engine not started".into()),
            )),
            batch_seq: AtomicU32::new(0),
            metrics,
        })
    }

    fn start(self: &Arc<Self>) -> EngineResult<()> {
        let mut guard = self.state.lock();
        if matches!(*guard, EngineRuntimeState::Running(_)) {
            return Err(EngineError::AlreadyRunning);
        }
        self.set_health(HealthStatus::Starting, None);
        let runtime = Runtime::new().map_err(|err| EngineError::Failure {
            source: Box::new(err),
        })?;
        let cancel = CancellationToken::new();
        let runner = Arc::clone(self);
        let cancel_clone = cancel.clone();
        let handle = runtime.spawn(async move {
            runner.run(cancel_clone).await;
        });
        *guard = EngineRuntimeState::Running(RuntimeBundle {
            runtime,
            handle,
            cancel,
        });
        info!("[{}] quote backfill engine starting", self.config.label);
        Ok(())
    }

    fn stop(&self) -> EngineResult<()> {
        let mut guard = self.state.lock();
        let Some(bundle) = guard.take_running() else {
            return Err(EngineError::NotRunning);
        };
        bundle.cancel.cancel();
        if let Err(err) = RuntimeBundle::join(bundle) {
            error!(
                "[{}] quote backfill runtime join failed: {:?}",
                self.config.label, err
            );
        }
        *guard = EngineRuntimeState::Stopped;
        self.set_health(HealthStatus::Stopped, None);
        Ok(())
    }

    fn health(&self) -> EngineHealth {
        self.health.lock().clone()
    }

    fn set_health(&self, status: HealthStatus, detail: Option<String>) {
        let mut guard = self.health.lock();
        guard.status = status;
        guard.detail = detail;
    }

    async fn run(self: Arc<Self>, cancel: CancellationToken) {
        info!(
            "[{}] quote backfill runtime loop starting",
            self.config.label
        );
        self.set_health(HealthStatus::Ready, None);
        while !cancel.is_cancelled() {
            match self.cycle(&cancel).await {
                Ok(windows) => {
                    if windows > 0 {
                        debug!(
                            "[{}] backfilled {} window(s) via REST",
                            self.config.label, windows
                        );
                    }
                    self.set_health(HealthStatus::Ready, None);
                }
                Err(err) => {
                    warn!(
                        "[{}] quote backfill iteration failed: {}",
                        self.config.label, err
                    );
                    self.set_health(HealthStatus::Degraded, Some(err.to_string()));
                }
            }
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = sleep(self.config.poll_interval) => {}
            }
        }
        self.set_health(HealthStatus::Stopped, None);
        info!(
            "[{}] quote backfill runtime loop exiting",
            self.config.label
        );
    }

    async fn cycle(&self, cancel: &CancellationToken) -> Result<usize, QuoteBackfillError> {
        let targets = self.collect_targets()?;
        self.metrics.record_backlog(targets.len());
        self.metrics.record_windows_examined(targets.len());
        if targets.is_empty() {
            return Ok(0);
        }
        let mut completed = 0usize;
        for target in targets {
            if cancel.is_cancelled() {
                break;
            }
            match self.process_target(&target).await {
                Ok(true) => {
                    completed += 1;
                }
                Ok(false) => {
                    debug!(
                        "[{}] no quotes for {}@{}",
                        self.config.label, target.symbol, target.window_idx
                    );
                }
                Err(err) => {
                    self.metrics.record_window_failed();
                    warn!(
                        "[{}] backfill failed for {}@{}: {}",
                        self.config.label, target.symbol, target.window_idx, err
                    );
                }
            }
        }
        Ok(completed)
    }

    fn collect_targets(&self) -> Result<Vec<BackfillTarget>, QuoteBackfillError> {
        let now_ns = current_time_ns();
        let min_age_ns = duration_to_ns(self.config.min_window_age);
        let trade_space = self.controller.trade_window_space();
        let mut targets = Vec::new();
        for (symbol_id, symbol) in self.controller.symbols() {
            if targets.len() >= self.config.max_windows_per_cycle {
                break;
            }
            let mut symbol_targets = trade_space
                .with_symbol_rows(symbol_id, |rows| {
                    let mut collected = Vec::new();
                    for row in rows {
                        let trade_slot = row.option_trade_ref;
                        let quote_slot = row.option_quote_ref;
                        if trade_slot.payload_id == 0 || trade_slot.status != SlotStatus::Filled {
                            continue;
                        }
                        if matches!(
                            quote_slot.status,
                            SlotStatus::Filled | SlotStatus::Retire | SlotStatus::Retired
                        ) {
                            continue;
                        }
                        let window_idx = row.header.window_idx;
                        let Some(window_meta) = self.controller.window_meta(window_idx) else {
                            continue;
                        };
                        let start_ns = row.header.start_ts.saturating_mul(1_000_000_000);
                        let duration_ns =
                            (window_meta.duration_secs as i64).saturating_mul(1_000_000_000);
                        let end_ns = start_ns + duration_ns;
                        if now_ns < end_ns.saturating_add(min_age_ns) {
                            continue;
                        }
                        collected.push(BackfillTarget {
                            symbol: symbol.clone(),
                            window_idx,
                            window_start_ts: row.header.start_ts,
                            window_start_ns: start_ns,
                            window_end_ns: end_ns,
                            trade_payload_id: trade_slot.payload_id,
                            expected_quote_version: quote_slot.version,
                        });
                        if collected.len() >= self.config.max_windows_per_cycle {
                            break;
                        }
                    }
                    collected
                })
                .map_err(WindowSpaceError::from)
                .map_err(QuoteBackfillError::from)?;
            targets.append(&mut symbol_targets);
            if targets.len() >= self.config.max_windows_per_cycle {
                targets.truncate(self.config.max_windows_per_cycle);
                break;
            }
        }
        Ok(targets)
    }

    async fn process_target(&self, target: &BackfillTarget) -> Result<bool, QuoteBackfillError> {
        let payload = self.load_trade_payload(target.trade_payload_id)?;
        let contract_path = self.config.state_dir.join(&payload.artifact_uri);
        let contracts = load_contracts(&contract_path)?;
        if contracts.is_empty() {
            warn!(
                "[{}] trade payload {} had no contracts; skipping {}@{}",
                self.config.label, target.trade_payload_id, target.symbol, target.window_idx
            );
            return Ok(false);
        }
        let mut quotes = Vec::new();
        for contract in contracts {
            let mut fetched = self
                .fetch_contract_quotes(&contract, target.window_start_ns, target.window_end_ns)
                .await?;
            quotes.append(&mut fetched);
        }
        quotes.retain(|quote| {
            quote.quote_ts_ns >= target.window_start_ns && quote.quote_ts_ns < target.window_end_ns
        });
        if quotes.is_empty() {
            self.metrics.record_window_skipped();
            return Ok(false);
        }
        quotes.sort_by_key(|q| q.quote_ts_ns);
        self.persist_quotes(target, &quotes).await?;
        self.metrics.record_window_filled(quotes.len());
        info!(
            "[{}] backfilled {} quotes for {}@{}",
            self.config.label,
            quotes.len(),
            target.symbol,
            target.window_idx
        );
        Ok(true)
    }

    fn load_trade_payload(
        &self,
        payload_id: u32,
    ) -> Result<window_space::mapping::TradeBatchPayload, QuoteBackfillError> {
        let stores = self.controller.payload_stores();
        let payload = stores
            .trades
            .get(payload_id)
            .cloned()
            .ok_or(QuoteBackfillError::MissingTradePayload(payload_id))?;
        Ok(payload)
    }

    async fn fetch_contract_quotes(
        &self,
        contract: &str,
        window_start_ns: i64,
        window_end_ns: i64,
    ) -> Result<Vec<QuoteRecord>, QuoteBackfillError> {
        let mut quotes = Vec::new();
        let mut url = Url::parse(&self.config.rest_base_url)?;
        url.set_path(&format!("/v3/quotes/{contract}"));
        url.query_pairs_mut()
            .append_pair("timestamp.gte", &window_start_ns.to_string())
            .append_pair("timestamp.lt", &window_end_ns.to_string())
            .append_pair("limit", &self.config.page_limit.to_string())
            .append_pair("sort", "timestamp")
            .append_pair("order", "asc")
            .append_pair("apiKey", &self.config.api_key);
        let mut next = Some(url);
        while let Some(current) = next.take() {
            self.metrics.record_rest_attempt();
            let start = Instant::now();
            let response = match self.client.get(current.clone()).send().await {
                Ok(resp) => resp,
                Err(err) => {
                    self.metrics.record_rest_network_error(start.elapsed());
                    return Err(err.into());
                }
            };
            let status = response.status();
            if status.is_client_error() {
                self.metrics
                    .record_rest_client_error(status.as_u16(), start.elapsed());
                return Err(QuoteBackfillError::HttpStatus(status.as_u16()));
            }
            if status.is_server_error() {
                self.metrics
                    .record_rest_server_error(status.as_u16(), start.elapsed());
                return Err(QuoteBackfillError::HttpStatus(status.as_u16()));
            }
            let resp = match response.json::<RestQuoteResponse>().await {
                Ok(body) => {
                    self.metrics
                        .record_rest_success(status.as_u16(), start.elapsed());
                    body
                }
                Err(err) => {
                    self.metrics.record_rest_network_error(start.elapsed());
                    return Err(err.into());
                }
            };
            if let Some(results) = resp.results {
                for row in results {
                    if let Some(record) = row.into_record(contract) {
                        quotes.push(record);
                    }
                }
            }
            if let Some(next_url) = resp.next_url {
                next = Some(Url::parse(&next_url)?);
            }
        }
        Ok(quotes)
    }

    async fn persist_quotes(
        &self,
        target: &BackfillTarget,
        quotes: &[QuoteRecord],
    ) -> Result<(), QuoteBackfillError> {
        let batch = quote_batch(quotes)?;
        let relative = artifact_path(
            &self.config,
            &target.symbol,
            target.window_idx,
            target.window_start_ts,
        )?;
        let artifact = write_record_batch(self.config.state_dir.as_path(), &relative, &batch)?;
        let mut stores = self.controller.payload_stores();
        let payload = QuoteBatchPayload {
            schema_version: QUOTE_SCHEMA_VERSION,
            window_ts: target.window_start_ts,
            batch_id: self.batch_seq.fetch_add(1, AtomicOrdering::Relaxed) + 1,
            first_quote_ts: quotes.first().map(|q| q.quote_ts_ns).unwrap_or(0),
            last_quote_ts: quotes.last().map(|q| q.quote_ts_ns).unwrap_or(0),
            nbbo_sample_count: quotes.len() as u32,
            artifact_uri: relative.clone(),
            checksum: artifact.checksum,
        };
        let payload_id = stores.quotes.append(payload)?;
        drop(stores);
        let meta = PayloadMeta {
            payload_type: PayloadType::Quote,
            payload_id,
            version: target.expected_quote_version.wrapping_add(1),
            checksum: artifact.checksum,
            last_updated_ns: Some(current_time_ns()),
        };
        match self.controller.set_option_quote_ref(
            &target.symbol,
            target.window_idx,
            meta,
            Some(target.expected_quote_version),
        ) {
            Ok(_) => Ok(()),
            Err(err) if is_version_conflict(&err) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }
}

#[derive(Clone)]
struct BackfillTarget {
    symbol: String,
    window_idx: WindowIndex,
    window_start_ts: i64,
    window_start_ns: i64,
    window_end_ns: i64,
    trade_payload_id: u32,
    expected_quote_version: u32,
}

#[derive(Debug, Deserialize)]
struct RestQuoteResponse {
    #[serde(default)]
    next_url: Option<String>,
    #[serde(default)]
    results: Option<Vec<RestQuote>>,
}

#[derive(Debug, Deserialize)]
struct RestQuote {
    ask_exchange: Option<i32>,
    ask_price: Option<f64>,
    ask_size: Option<f64>,
    bid_exchange: Option<i32>,
    bid_price: Option<f64>,
    bid_size: Option<f64>,
    sequence_number: Option<u64>,
    sip_timestamp: Option<i64>,
}

impl RestQuote {
    fn into_record(self, instrument_id: &str) -> Option<QuoteRecord> {
        let bid = self.bid_price?;
        let ask = self.ask_price?;
        let ts = self.sip_timestamp?;
        let bid_sz = size_to_u32(self.bid_size);
        let ask_sz = size_to_u32(self.ask_size);
        let best_bid_venue = self.bid_exchange;
        let best_ask_venue = self.ask_exchange;
        let sequence = self.sequence_number;
        let uid = quote_uid(
            instrument_id,
            ts,
            sequence,
            bid,
            ask,
            bid_sz,
            ask_sz,
            best_bid_venue,
            best_ask_venue,
            None,
        );
        Some(QuoteRecord {
            instrument_id: instrument_id.to_string(),
            quote_uid: uid,
            quote_ts_ns: ts,
            bid,
            ask,
            bid_sz,
            ask_sz,
            state: classify_state(bid, ask),
            condition: None,
            best_bid_venue,
            best_ask_venue,
            source: Source::Rest,
            quality: Quality::Prelim,
            watermark_ts_ns: current_time_ns(),
        })
    }
}

fn classify_state(bid: f64, ask: f64) -> NbboState {
    if bid > ask {
        NbboState::Crossed
    } else if (ask - bid).abs() < f64::EPSILON {
        NbboState::Locked
    } else {
        NbboState::Normal
    }
}

fn size_to_u32(value: Option<f64>) -> u32 {
    value
        .unwrap_or_default()
        .max(0.0)
        .round()
        .clamp(0.0, u32::MAX as f64) as u32
}

fn load_contracts(path: &Path) -> Result<Vec<String>, QuoteBackfillError> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.build()?;
    let mut set = BTreeSet::new();
    while let Some(batch) = reader.next() {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(QuoteBackfillError::InvalidSchema)?;
        for idx in 0..column.len() {
            set.insert(column.value(idx).to_string());
        }
    }
    Ok(set.into_iter().collect())
}

fn quote_batch(records: &[QuoteRecord]) -> Result<RecordBatch, QuoteBackfillError> {
    let schema: SchemaRef = Arc::new(nbbo_schema());
    let instrument = Arc::new(StringArray::from(
        records
            .iter()
            .map(|r| r.instrument_id.clone())
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let quote_uid = Arc::new(FixedSizeBinaryArray::try_from_iter(
        records.iter().map(|r| r.quote_uid.as_ref()),
    )?) as ArrayRef;
    let quote_ts_ns = Arc::new(Int64Array::from(
        records.iter().map(|r| r.quote_ts_ns).collect::<Vec<_>>(),
    )) as ArrayRef;
    let bid = Arc::new(Float64Array::from(
        records.iter().map(|r| r.bid).collect::<Vec<_>>(),
    )) as ArrayRef;
    let ask = Arc::new(Float64Array::from(
        records.iter().map(|r| r.ask).collect::<Vec<_>>(),
    )) as ArrayRef;
    let bid_sz = Arc::new(UInt32Array::from(
        records.iter().map(|r| r.bid_sz).collect::<Vec<_>>(),
    )) as ArrayRef;
    let ask_sz = Arc::new(UInt32Array::from(
        records.iter().map(|r| r.ask_sz).collect::<Vec<_>>(),
    )) as ArrayRef;
    let state = Arc::new(StringArray::from(
        records
            .iter()
            .map(|r| format!("{:?}", r.state))
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let condition = Arc::new(Int32Array::from(
        records.iter().map(|r| r.condition).collect::<Vec<_>>(),
    )) as ArrayRef;
    let best_bid_venue = Arc::new(Int32Array::from(
        records.iter().map(|r| r.best_bid_venue).collect::<Vec<_>>(),
    )) as ArrayRef;
    let best_ask_venue = Arc::new(Int32Array::from(
        records.iter().map(|r| r.best_ask_venue).collect::<Vec<_>>(),
    )) as ArrayRef;
    let source = Arc::new(StringArray::from(
        records
            .iter()
            .map(|r| format!("{:?}", r.source))
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let quality = Arc::new(StringArray::from(
        records
            .iter()
            .map(|r| format!("{:?}", r.quality))
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let watermark = Arc::new(Int64Array::from(
        records
            .iter()
            .map(|r| r.watermark_ts_ns)
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let arrays = vec![
        instrument,
        quote_uid,
        quote_ts_ns,
        bid,
        ask,
        bid_sz,
        ask_sz,
        state,
        condition,
        best_bid_venue,
        best_ask_venue,
        source,
        quality,
        watermark,
    ];
    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn artifact_path(
    config: &QuoteBackfillConfig,
    symbol: &str,
    window_idx: WindowIndex,
    window_start_ts: i64,
) -> Result<String, QuoteBackfillError> {
    let dt = OffsetDateTime::from_unix_timestamp(window_start_ts)
        .map_err(|_| QuoteBackfillError::InvalidWindowTimestamp(window_start_ts))?;
    Ok(format!(
        "quote-rest/{}/{:04}/{:02}/{:02}/{:06}/{}.parquet",
        config.label,
        dt.year(),
        dt.month() as u8,
        dt.day(),
        window_idx,
        sanitized_symbol(symbol)
    ))
}

fn sanitized_symbol(symbol: &str) -> String {
    symbol
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

struct ArtifactInfo {
    checksum: u32,
}

fn write_record_batch(
    state_dir: &Path,
    relative_path: &str,
    batch: &RecordBatch,
) -> Result<ArtifactInfo, QuoteBackfillError> {
    let final_path = state_dir.join(relative_path);
    if let Some(parent) = final_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp_path = final_path.with_extension("tmp");
    if let Some(parent) = tmp_path.parent() {
        fs::create_dir_all(parent)?;
    }
    if tmp_path.exists() {
        fs::remove_file(&tmp_path)?;
    }
    let file = File::create(&tmp_path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(batch)?;
    writer.close()?;
    if final_path.exists() {
        fs::remove_file(&final_path)?;
    }
    fs::rename(&tmp_path, &final_path)?;
    let checksum = compute_checksum(&final_path)?;
    Ok(ArtifactInfo { checksum })
}

fn compute_checksum(path: &Path) -> Result<u32, QuoteBackfillError> {
    let mut file = File::open(path)?;
    let mut hasher = Crc32::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize())
}

fn current_time_ns() -> i64 {
    let now = Utc::now();
    now.timestamp_nanos_opt()
        .unwrap_or_else(|| now.timestamp() * 1_000_000_000)
}

fn duration_to_ns(duration: Duration) -> i64 {
    duration.as_nanos().min(i64::MAX as u128) as i64
}

fn is_version_conflict(err: &WindowSpaceError) -> bool {
    matches!(
        err,
        WindowSpaceError::SlotWrite(SlotWriteError::VersionConflict { .. })
    )
}

#[derive(Debug, Error)]
pub enum QuoteBackfillError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("window-space error: {0}")]
    Window(#[from] WindowSpaceError),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("http status {0}")]
    HttpStatus(u16),
    #[error("parquet error: {0}")]
    Parquet(#[from] ParquetError),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("url parse error: {0}")]
    Url(#[from] url::ParseError),
    #[error("missing trade payload id {0}")]
    MissingTradePayload(u32),
    #[error("invalid quote payload schema")]
    InvalidSchema,
    #[error("invalid window timestamp {0}")]
    InvalidWindowTimestamp(i64),
}

impl From<QuoteBackfillError> for EngineError {
    fn from(value: QuoteBackfillError) -> Self {
        EngineError::Failure {
            source: Box::new(value),
        }
    }
}

enum EngineRuntimeState {
    Stopped,
    Running(RuntimeBundle),
}

impl EngineRuntimeState {
    fn take_running(&mut self) -> Option<RuntimeBundle> {
        match std::mem::replace(self, EngineRuntimeState::Stopped) {
            EngineRuntimeState::Running(bundle) => Some(bundle),
            other => {
                *self = other;
                None
            }
        }
    }
}

struct RuntimeBundle {
    runtime: Runtime,
    handle: JoinHandle<()>,
    cancel: CancellationToken,
}

impl RuntimeBundle {
    fn join(bundle: RuntimeBundle) -> Result<(), tokio::task::JoinError> {
        let RuntimeBundle {
            runtime,
            handle,
            cancel: _,
        } = bundle;
        runtime.block_on(async { handle.await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use window_space::{WindowSpace, WindowSpaceConfig};

    #[test]
    fn artifact_path_sanitizes_symbol() {
        let config = QuoteBackfillConfig::new("dev", "/tmp", "https://api.massive.com", "test-key");
        let path = artifact_path(&config, "SPY 2025", 42, 1_700_000_000).unwrap();
        assert!(path.contains("SPY__2025"));
    }

    fn bootstrap_controller() -> (Arc<WindowSpaceController>, TempDir) {
        let dir = tempfile::tempdir().expect("temp dir");
        let window_space = WindowSpace::standard(1_700_000_000);
        let mut config = WindowSpaceConfig::new(dir.path().to_path_buf(), window_space);
        config.max_symbols = 8;
        let (controller, _) = WindowSpaceController::bootstrap(config).unwrap();
        (Arc::new(controller), dir)
    }

    #[test]
    fn config_page_limit_bounds() {
        let cfg = QuoteBackfillConfig::new("dev", "/tmp", "http://localhost", "k")
            .with_page_limit(100_000);
        assert_eq!(cfg.page_limit, 50_000);
    }
}
