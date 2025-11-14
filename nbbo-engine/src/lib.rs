use std::{
    fs::{self, File},
    io::{self, Read},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};

use arrow::array::FixedSizeBinaryArray;
use arrow::{
    array::{
        Array, ArrayRef, Float64Array, Int32Array, Int64Array, ListArray, StringArray, UInt32Array,
        UInt64Array,
    },
    record_batch::RecordBatch,
};
use chrono::{Datelike, NaiveDateTime};
use classifier::Classifier;
use core_types::{
    raw::{AggressorRecord, OptionTradeRecord, UnderlyingTradeRecord},
    schema::aggressor_record_schema,
    types::{ClassParams, EquityTrade, Nbbo, OptionTrade, Quality, Source},
};
use crc32fast::Hasher as Crc32;
use engine_api::{
    Engine, EngineError, EngineHealth, EngineResult, HealthStatus, PriorityHookDescription,
};
use log::{error, info, warn};
use nbbo_cache::NbboStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use thiserror::Error;
use window_space::{
    SlotWriteError, WindowSpaceController, WindowSpaceError,
    mapping::{AggressorPayload, QuoteBatchPayload, TradeBatchPayload},
    payload::{PayloadMeta, PayloadType, Slot, SlotKind, SlotStatus, TradeSlotKind},
    window::{WindowIndex, WindowMeta},
};

const AGGRESSOR_PAYLOAD_SCHEMA_VERSION: u8 = 1;
const DEFAULT_WINDOWS_PER_TICK: usize = 64;
const DEFAULT_IDLE_BACKOFF_MS: u64 = 250;
const DEFAULT_ERROR_BACKOFF_MS: u64 = 1_000;

#[derive(Clone, Debug)]
pub struct AggressorEngineConfig {
    pub label: String,
    pub windows_per_tick: usize,
    pub idle_backoff: Duration,
    pub error_backoff: Duration,
}

impl AggressorEngineConfig {
    pub fn for_label(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            ..Default::default()
        }
    }
}

impl Default for AggressorEngineConfig {
    fn default() -> Self {
        Self {
            label: "dev".to_string(),
            windows_per_tick: DEFAULT_WINDOWS_PER_TICK,
            idle_backoff: Duration::from_millis(DEFAULT_IDLE_BACKOFF_MS),
            error_backoff: Duration::from_millis(DEFAULT_ERROR_BACKOFF_MS),
        }
    }
}

#[derive(Default)]
pub struct AggressorMetrics {
    option_total: AtomicU64,
    underlying_total: AtomicU64,
}

impl AggressorMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_option(&self, count: u64) {
        self.option_total.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_underlying(&self, count: u64) {
        self.underlying_total.fetch_add(count, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> AggressorMetricsSnapshot {
        AggressorMetricsSnapshot {
            option_total: self.option_total.load(Ordering::Relaxed),
            underlying_total: self.underlying_total.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct AggressorMetricsSnapshot {
    pub option_total: u64,
    pub underlying_total: u64,
}

pub struct OptionAggressorEngine {
    inner: Arc<AggressorInner>,
}

pub struct UnderlyingAggressorEngine {
    inner: Arc<AggressorInner>,
}

impl OptionAggressorEngine {
    pub fn new(
        config: AggressorEngineConfig,
        controller: Arc<WindowSpaceController>,
        class_params: ClassParams,
        metrics: Arc<AggressorMetrics>,
    ) -> Self {
        Self {
            inner: AggressorInner::new(
                AggressorKind::Option,
                config,
                controller,
                class_params,
                metrics,
            ),
        }
    }
}

impl UnderlyingAggressorEngine {
    pub fn new(
        config: AggressorEngineConfig,
        controller: Arc<WindowSpaceController>,
        class_params: ClassParams,
        metrics: Arc<AggressorMetrics>,
    ) -> Self {
        Self {
            inner: AggressorInner::new(
                AggressorKind::Underlying,
                config,
                controller,
                class_params,
                metrics,
            ),
        }
    }
}

impl Engine for OptionAggressorEngine {
    fn start(&self) -> EngineResult<()> {
        AggressorInner::start(&self.inner)
    }

    fn stop(&self) -> EngineResult<()> {
        self.inner.stop()
    }

    fn health(&self) -> EngineHealth {
        self.inner.health()
    }

    fn describe_priority_hooks(&self) -> PriorityHookDescription {
        PriorityHookDescription::default()
    }
}

impl Engine for UnderlyingAggressorEngine {
    fn start(&self) -> EngineResult<()> {
        AggressorInner::start(&self.inner)
    }

    fn stop(&self) -> EngineResult<()> {
        self.inner.stop()
    }

    fn health(&self) -> EngineHealth {
        self.inner.health()
    }

    fn describe_priority_hooks(&self) -> PriorityHookDescription {
        PriorityHookDescription::default()
    }
}

struct AggressorInner {
    kind: AggressorKind,
    config: AggressorEngineConfig,
    controller: Arc<WindowSpaceController>,
    classifier: Classifier,
    class_params: ClassParams,
    state: Mutex<EngineRuntimeState>,
    health: Mutex<EngineHealth>,
    metrics: Arc<AggressorMetrics>,
}

impl AggressorInner {
    fn new(
        kind: AggressorKind,
        config: AggressorEngineConfig,
        controller: Arc<WindowSpaceController>,
        class_params: ClassParams,
        metrics: Arc<AggressorMetrics>,
    ) -> Arc<Self> {
        Arc::new(Self {
            kind,
            config,
            controller,
            classifier: Classifier::new(),
            class_params,
            state: Mutex::new(EngineRuntimeState::Stopped),
            health: Mutex::new(EngineHealth::new(HealthStatus::Stopped, None)),
            metrics,
        })
    }

    fn start(this: &Arc<Self>) -> EngineResult<()> {
        let mut guard = this.state.lock().unwrap();
        if matches!(*guard, EngineRuntimeState::Running(_)) {
            return Err(EngineError::AlreadyRunning);
        }
        this.set_health(HealthStatus::Starting, None);
        let cancel = Arc::new(AtomicBool::new(false));
        let runner = Arc::clone(this);
        let cancel_clone = Arc::clone(&cancel);
        let name = format!("{}-aggressor", this.kind.label());
        let handle = thread::Builder::new()
            .name(name.clone())
            .spawn(move || runner.run(cancel_clone))
            .map_err(|err| EngineError::Failure { source: err.into() })?;
        info!(
            "[{}] aggressor engine starting (windows_per_tick={})",
            this.kind.label(),
            this.config.windows_per_tick
        );
        *guard = EngineRuntimeState::Running(ThreadBundle { cancel, handle });
        Ok(())
    }

    fn stop(&self) -> EngineResult<()> {
        let mut guard = self.state.lock().unwrap();
        let Some(bundle) = guard.take_running() else {
            return Err(EngineError::NotRunning);
        };
        bundle.cancel.store(true, Ordering::Relaxed);
        if let Err(err) = bundle.handle.join() {
            error!(
                "[{}] aggressor engine thread join failed: {:?}",
                self.kind.label(),
                err
            );
        }
        self.set_health(HealthStatus::Stopped, None);
        *guard = EngineRuntimeState::Stopped;
        Ok(())
    }

    fn health(&self) -> EngineHealth {
        self.health.lock().unwrap().clone()
    }

    fn run(self: Arc<Self>, cancel: Arc<AtomicBool>) {
        self.set_health(HealthStatus::Ready, None);
        while !cancel.load(Ordering::Relaxed) {
            match self.process_pass() {
                Ok(WorkState::DidWork) => continue,
                Ok(WorkState::NoWork) => thread::sleep(self.config.idle_backoff),
                Err(err) => {
                    self.set_health(HealthStatus::Degraded, Some(err.to_string()));
                    error!("[{}] aggressor pass failed: {}", self.kind.label(), err);
                    thread::sleep(self.config.error_backoff);
                }
            }
        }
        self.set_health(HealthStatus::Stopped, None);
        info!("[{}] aggressor engine stopped", self.kind.label());
    }

    fn process_pass(&self) -> Result<WorkState, AggressorError> {
        let work = self.fetch_ready_windows(self.config.windows_per_tick)?;
        if work.is_empty() {
            return Ok(WorkState::NoWork);
        }
        for assignment in work {
            self.process_window(&assignment)?;
        }
        Ok(WorkState::DidWork)
    }

    fn fetch_ready_windows(&self, limit: usize) -> Result<Vec<WorkItem>, AggressorError> {
        let mut assignments = Vec::new();
        let symbols = self.controller.symbols();
        let trade_space = self.controller.trade_window_space();
        for (symbol_id, symbol) in symbols {
            trade_space
                .with_symbol_rows(symbol_id, |rows| {
                    for (idx, row) in rows.iter().enumerate() {
                        if assignments.len() >= limit {
                            break;
                        }
                        let output_slot = self.kind.output_slot_ref(row);
                        if matches!(output_slot.status, SlotStatus::Filled | SlotStatus::Pending) {
                            continue;
                        }
                        if !is_filled(self.kind.trade_slot_ref(row)) {
                            continue;
                        }
                        if !is_filled(self.kind.quote_slot_ref(row)) {
                            continue;
                        }
                        let trade_payload_id = self.kind.trade_slot_ref(row).payload_id;
                        let quote_payload_id = self.kind.quote_slot_ref(row).payload_id;
                        if trade_payload_id == 0 || quote_payload_id == 0 {
                            continue;
                        }
                        assignments.push(WorkItem {
                            symbol: symbol.clone(),
                            window_idx: idx as WindowIndex,
                            trade_payload_id,
                            quote_payload_id,
                            aggressor_version: output_slot.version,
                        });
                    }
                })
                .map_err(WindowSpaceError::from)?;
            if assignments.len() >= limit {
                break;
            }
        }
        Ok(assignments)
    }

    fn process_window(&self, work: &WorkItem) -> Result<(), AggressorError> {
        let slot_kind = SlotKind::Trade(self.kind.output_slot_kind());
        self.controller
            .mark_pending(&work.symbol, work.window_idx, slot_kind)?;

        let window_meta = self.controller.window_meta(work.window_idx).ok_or(
            AggressorError::MissingWindowMeta {
                window_idx: work.window_idx,
            },
        )?;

        let (trade_payload, quote_payload) =
            self.load_payloads(work.trade_payload_id, work.quote_payload_id)?;
        let trade_path = self
            .controller
            .config()
            .state_dir()
            .join(&trade_payload.artifact_uri);
        let quote_path = self
            .controller
            .config()
            .state_dir()
            .join(&quote_payload.artifact_uri);

        let nbbo = load_nbbo_quotes(&quote_path)?;
        if nbbo.is_empty() {
            warn!(
                "[{}] no quotes for symbol={} window_idx={}",
                self.kind.label(),
                work.symbol,
                work.window_idx
            );
        }
        let store = build_nbbo_store(&nbbo);
        let window_start_ns = window_meta.start_ts.saturating_mul(1_000_000_000);

        let records = match self.kind {
            AggressorKind::Option => {
                let trades = load_option_trades(&trade_path)?;
                self.classify_option_trades(trades, &store, window_start_ns)
            }
            AggressorKind::Underlying => {
                let trades = load_underlying_trades(&trade_path)?;
                self.classify_underlying_trades(trades, &store, window_start_ns)
            }
        };

        let batch = aggressor_batch(&records)?;
        let artifact = write_artifact(
            self.controller.config().state_dir(),
            &self.config.label,
            self.kind.artifact_family(),
            &work.symbol,
            &window_meta,
            work.window_idx,
            &batch,
        )?;

        let payload = AggressorPayload {
            schema_version: AGGRESSOR_PAYLOAD_SCHEMA_VERSION,
            window_ts: window_meta.start_ts,
            trade_payload_id: work.trade_payload_id,
            quote_payload_id: work.quote_payload_id,
            observation_count: records.len() as u32,
            artifact_uri: artifact.relative_path.clone(),
            checksum: artifact.checksum,
        };

        let payload_id = {
            let mut stores = self.controller.payload_stores();
            stores.aggressor.append(payload)?
        };

        let meta = PayloadMeta::new(PayloadType::Aggressor, payload_id, 1, artifact.checksum);
        if let Err(err) = self.kind.write_output_slot(
            &self.controller,
            &work.symbol,
            work.window_idx,
            meta,
            Some(work.aggressor_version),
        ) {
            if is_version_conflict(&err) {
                warn!(
                    "[{}] slot version conflict for symbol={} window_idx={}; another worker likely filled it",
                    self.kind.label(),
                    work.symbol,
                    work.window_idx
                );
            } else {
                self.controller
                    .clear_slot(&work.symbol, work.window_idx, slot_kind)?;
                return Err(err.into());
            }
        } else {
            self.controller.mark_retire_slot(
                &work.symbol,
                work.window_idx,
                SlotKind::Trade(self.kind.quote_slot_kind()),
            )?;
        }

        Ok(())
    }

    fn load_payloads(
        &self,
        trade_payload_id: u32,
        quote_payload_id: u32,
    ) -> Result<(TradeBatchPayload, QuoteBatchPayload), AggressorError> {
        let stores = self.controller.payload_stores();
        let trade = stores
            .trades
            .get(trade_payload_id)
            .ok_or(AggressorError::MissingPayload {
                payload_id: trade_payload_id,
            })?
            .clone();
        let quote = stores
            .quotes
            .get(quote_payload_id)
            .ok_or(AggressorError::MissingPayload {
                payload_id: quote_payload_id,
            })?
            .clone();
        Ok((trade, quote))
    }

    fn classify_option_trades(
        &self,
        records: Vec<OptionTradeRecord>,
        nbbo: &NbboStore,
        window_start_ns: i64,
    ) -> Vec<AggressorRecord> {
        let mut overlays = Vec::with_capacity(records.len());
        for record in records {
            let mut trade = option_trade_from_record(&record);
            self.classifier
                .classify_trade(&mut trade, nbbo, &self.class_params);
            overlays.push(AggressorRecord {
                instrument_id: trade.contract.clone(),
                underlying_symbol: if trade.underlying.is_empty() {
                    None
                } else {
                    Some(trade.underlying.clone())
                },
                trade_uid: trade.trade_uid,
                trade_ts_ns: trade.trade_ts_ns,
                window_start_ts_ns: window_start_ns,
                price: trade.price,
                size: trade.size,
                aggressor_side: trade.aggressor_side.clone(),
                class_method: trade.class_method.clone(),
                aggressor_offset_mid_bp: trade.aggressor_offset_mid_bp,
                aggressor_confidence: trade.aggressor_confidence,
                nbbo_bid: trade.nbbo_bid,
                nbbo_ask: trade.nbbo_ask,
                nbbo_bid_sz: trade.nbbo_bid_sz,
                nbbo_ask_sz: trade.nbbo_ask_sz,
                nbbo_ts_ns: trade.nbbo_ts_ns,
                nbbo_age_us: trade.nbbo_age_us,
                nbbo_state: trade.nbbo_state.clone(),
                tick_size_used: trade.tick_size_used,
                source: trade.source.clone(),
                quality: trade.quality.clone(),
                watermark_ts_ns: trade.watermark_ts_ns,
            });
        }
        self.metrics.record_option(overlays.len() as u64);
        overlays
    }

    fn classify_underlying_trades(
        &self,
        records: Vec<UnderlyingTradeRecord>,
        nbbo: &NbboStore,
        window_start_ns: i64,
    ) -> Vec<AggressorRecord> {
        let mut overlays = Vec::with_capacity(records.len());
        for record in records {
            let mut trade = equity_trade_from_record(&record);
            self.classifier
                .classify_trade(&mut trade, nbbo, &self.class_params);
            overlays.push(AggressorRecord {
                instrument_id: trade.symbol.clone(),
                underlying_symbol: Some(trade.symbol.clone()),
                trade_uid: trade.trade_uid,
                trade_ts_ns: trade.trade_ts_ns,
                window_start_ts_ns: window_start_ns,
                price: trade.price,
                size: trade.size,
                aggressor_side: trade.aggressor_side.clone(),
                class_method: trade.class_method.clone(),
                aggressor_offset_mid_bp: trade.aggressor_offset_mid_bp,
                aggressor_confidence: trade.aggressor_confidence,
                nbbo_bid: trade.nbbo_bid,
                nbbo_ask: trade.nbbo_ask,
                nbbo_bid_sz: trade.nbbo_bid_sz,
                nbbo_ask_sz: trade.nbbo_ask_sz,
                nbbo_ts_ns: trade.nbbo_ts_ns,
                nbbo_age_us: trade.nbbo_age_us,
                nbbo_state: trade.nbbo_state.clone(),
                tick_size_used: trade.tick_size_used,
                source: trade.source.clone(),
                quality: trade.quality.clone(),
                watermark_ts_ns: trade.watermark_ts_ns,
            });
        }
        self.metrics.record_underlying(overlays.len() as u64);
        overlays
    }

    fn set_health(&self, status: HealthStatus, detail: Option<String>) {
        let mut guard = self.health.lock().unwrap();
        guard.status = status;
        guard.detail = detail;
    }
}

fn is_filled(slot: &Slot) -> bool {
    matches!(
        slot.status,
        SlotStatus::Filled | SlotStatus::Retire | SlotStatus::Retired
    )
}

fn is_version_conflict(err: &WindowSpaceError) -> bool {
    matches!(
        err,
        WindowSpaceError::SlotWrite(SlotWriteError::VersionConflict { .. })
    )
}

fn build_nbbo_store(quotes: &[Nbbo]) -> NbboStore {
    let mut store = NbboStore::new();
    for quote in quotes {
        store.put(quote);
    }
    store
}

fn load_option_trades(path: &Path) -> Result<Vec<OptionTradeRecord>, AggressorError> {
    read_record_batch(path, parse_option_batch)
}

fn load_underlying_trades(path: &Path) -> Result<Vec<UnderlyingTradeRecord>, AggressorError> {
    read_record_batch(path, parse_underlying_batch)
}

fn load_nbbo_quotes(path: &Path) -> Result<Vec<Nbbo>, AggressorError> {
    read_record_batch(path, parse_quote_batch)
}

fn read_record_batch<T, F>(path: &Path, mut f: F) -> Result<Vec<T>, AggressorError>
where
    F: FnMut(&RecordBatch) -> Result<Vec<T>, AggressorError>,
{
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.build()?;
    let mut rows = Vec::new();
    while let Some(batch) = reader.next() {
        let batch = batch?;
        rows.extend(f(&batch)?);
    }
    Ok(rows)
}

fn parse_option_batch(batch: &RecordBatch) -> Result<Vec<OptionTradeRecord>, AggressorError> {
    let contract = string_col(batch, 0)?;
    let trade_uid = fixed_binary_col(batch, 1)?;
    let direction = string_col(batch, 2)?;
    let strike = float_col(batch, 3)?;
    let underlying = string_col(batch, 4)?;
    let trade_ts = int64_col(batch, 5)?;
    let participant_ts = int64_opt_col(batch, 6)?;
    let price = float_col(batch, 7)?;
    let size = uint32_col(batch, 8)?;
    let conditions = list_i32_col(batch, 9)?;
    let exchange = int32_col(batch, 10)?;
    let expiry = int64_col(batch, 11)?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for idx in 0..batch.num_rows() {
        let dir_str = direction.value(idx);
        let contract_direction = dir_str.chars().next().unwrap_or('C');
        rows.push(OptionTradeRecord {
            contract: contract.value(idx).to_string(),
            trade_uid: slice_uid(trade_uid.value(idx))?,
            contract_direction,
            strike_price: strike.value(idx),
            underlying: underlying.value(idx).to_string(),
            trade_ts_ns: trade_ts.value(idx),
            participant_ts_ns: participant_ts[idx],
            price: price.value(idx),
            size: size.value(idx),
            conditions: conditions[idx].clone(),
            exchange: exchange.value(idx),
            expiry_ts_ns: expiry.value(idx),
            source: Source::Flatfile,
            quality: Quality::Prelim,
            watermark_ts_ns: trade_ts.value(idx),
        });
    }
    Ok(rows)
}

fn parse_underlying_batch(
    batch: &RecordBatch,
) -> Result<Vec<UnderlyingTradeRecord>, AggressorError> {
    let symbol = string_col(batch, 0)?;
    let trade_uid = fixed_binary_col(batch, 1)?;
    let trade_ts = int64_col(batch, 2)?;
    let participant_ts = int64_opt_col(batch, 3)?;
    let price = float_col(batch, 4)?;
    let size = uint32_col(batch, 5)?;
    let conditions = list_i32_col(batch, 6)?;
    let exchange = int32_col(batch, 7)?;
    let trade_id = string_opt_col(batch, 8)?;
    let seq = uint64_opt_col(batch, 9)?;
    let tape = string_opt_col(batch, 10)?;
    let correction = int32_opt_col(batch, 11)?;
    let trf_id = string_opt_col(batch, 12)?;
    let trf_ts = int64_opt_col(batch, 13)?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for idx in 0..batch.num_rows() {
        rows.push(UnderlyingTradeRecord {
            symbol: symbol.value(idx).to_string(),
            trade_uid: slice_uid(trade_uid.value(idx))?,
            trade_ts_ns: trade_ts.value(idx),
            participant_ts_ns: participant_ts[idx],
            price: price.value(idx),
            size: size.value(idx),
            conditions: conditions[idx].clone(),
            exchange: exchange.value(idx),
            trade_id: trade_id[idx].clone(),
            seq: seq[idx],
            tape: tape[idx].clone(),
            correction: correction[idx],
            trf_id: trf_id[idx].clone(),
            trf_ts_ns: trf_ts[idx],
            source: Source::Flatfile,
            quality: Quality::Prelim,
            watermark_ts_ns: trade_ts.value(idx),
        });
    }
    Ok(rows)
}

fn parse_quote_batch(batch: &RecordBatch) -> Result<Vec<Nbbo>, AggressorError> {
    let instrument = string_col(batch, 0)?;
    let quote_uid = fixed_binary_col(batch, 1)?;
    let quote_ts = int64_col(batch, 2)?;
    let bid = float_col(batch, 3)?;
    let ask = float_col(batch, 4)?;
    let bid_sz = uint32_col(batch, 5)?;
    let ask_sz = uint32_col(batch, 6)?;
    let state = string_col(batch, 7)?;
    let condition = int32_opt_col(batch, 8)?;
    let best_bid = int32_opt_col(batch, 9)?;
    let best_ask = int32_opt_col(batch, 10)?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    for idx in 0..batch.num_rows() {
        rows.push(Nbbo {
            instrument_id: instrument.value(idx).to_string(),
            quote_uid: slice_uid(quote_uid.value(idx))?,
            quote_ts_ns: quote_ts.value(idx),
            bid: bid.value(idx),
            ask: ask.value(idx),
            bid_sz: bid_sz.value(idx),
            ask_sz: ask_sz.value(idx),
            state: parse_nbbo_state(state.value(idx)),
            condition: condition[idx],
            best_bid_venue: best_bid[idx],
            best_ask_venue: best_ask[idx],
            source: Source::Flatfile,
            quality: Quality::Prelim,
            watermark_ts_ns: quote_ts.value(idx),
        });
    }
    Ok(rows)
}

fn parse_nbbo_state(value: &str) -> core_types::types::NbboState {
    match value {
        "Locked" => core_types::types::NbboState::Locked,
        "Crossed" => core_types::types::NbboState::Crossed,
        _ => core_types::types::NbboState::Normal,
    }
}

fn option_trade_from_record(record: &OptionTradeRecord) -> OptionTrade {
    OptionTrade {
        contract: record.contract.clone(),
        trade_uid: record.trade_uid,
        contract_direction: record.contract_direction,
        strike_price: record.strike_price,
        underlying: record.underlying.clone(),
        trade_ts_ns: record.trade_ts_ns,
        price: record.price,
        size: record.size,
        conditions: record.conditions.clone(),
        exchange: record.exchange,
        expiry_ts_ns: record.expiry_ts_ns,
        aggressor_side: core_types::types::AggressorSide::Unknown,
        class_method: core_types::types::ClassMethod::Unknown,
        aggressor_offset_mid_bp: None,
        aggressor_offset_touch_ticks: None,
        aggressor_confidence: None,
        nbbo_bid: None,
        nbbo_ask: None,
        nbbo_bid_sz: None,
        nbbo_ask_sz: None,
        nbbo_ts_ns: None,
        nbbo_age_us: None,
        nbbo_state: None,
        tick_size_used: None,
        delta: None,
        gamma: None,
        vega: None,
        theta: None,
        iv: None,
        greeks_flags: 0,
        source: record.source.clone(),
        quality: record.quality.clone(),
        watermark_ts_ns: record.watermark_ts_ns,
    }
}

fn equity_trade_from_record(record: &UnderlyingTradeRecord) -> EquityTrade {
    EquityTrade {
        symbol: record.symbol.clone(),
        trade_uid: record.trade_uid,
        trade_ts_ns: record.trade_ts_ns,
        price: record.price,
        size: record.size,
        conditions: record.conditions.clone(),
        exchange: record.exchange,
        aggressor_side: core_types::types::AggressorSide::Unknown,
        class_method: core_types::types::ClassMethod::Unknown,
        aggressor_offset_mid_bp: None,
        aggressor_offset_touch_ticks: None,
        aggressor_confidence: None,
        nbbo_bid: None,
        nbbo_ask: None,
        nbbo_bid_sz: None,
        nbbo_ask_sz: None,
        nbbo_ts_ns: None,
        nbbo_age_us: None,
        nbbo_state: None,
        tick_size_used: None,
        source: record.source.clone(),
        quality: record.quality.clone(),
        watermark_ts_ns: record.watermark_ts_ns,
        trade_id: record.trade_id.clone(),
        seq: record.seq,
        participant_ts_ns: record.participant_ts_ns,
        tape: record.tape.clone(),
        correction: record.correction,
        trf_id: record.trf_id.clone(),
        trf_ts_ns: record.trf_ts_ns,
    }
}

fn aggressor_batch(records: &[AggressorRecord]) -> Result<RecordBatch, AggressorError> {
    let schema = Arc::new(aggressor_record_schema());
    let instrument: ArrayRef = Arc::new(StringArray::from(
        records
            .iter()
            .map(|r| r.instrument_id.clone())
            .collect::<Vec<_>>(),
    ));
    let underlying: ArrayRef = Arc::new(StringArray::from(
        records
            .iter()
            .map(|r| r.underlying_symbol.clone())
            .collect::<Vec<_>>(),
    ));
    let trade_uid: ArrayRef = Arc::new(FixedSizeBinaryArray::try_from_iter(
        records.iter().map(|r| r.trade_uid.as_ref()),
    )?);
    let trade_ts: ArrayRef = Arc::new(Int64Array::from(
        records.iter().map(|r| r.trade_ts_ns).collect::<Vec<_>>(),
    ));
    let window_ts: ArrayRef = Arc::new(Int64Array::from(
        records
            .iter()
            .map(|r| r.window_start_ts_ns)
            .collect::<Vec<_>>(),
    ));
    let price: ArrayRef = Arc::new(Float64Array::from(
        records.iter().map(|r| r.price).collect::<Vec<_>>(),
    ));
    let size: ArrayRef = Arc::new(UInt32Array::from(
        records.iter().map(|r| r.size).collect::<Vec<_>>(),
    ));
    let side: ArrayRef = Arc::new(StringArray::from(
        records
            .iter()
            .map(|r| format!("{:?}", r.aggressor_side))
            .collect::<Vec<_>>(),
    ));
    let method: ArrayRef = Arc::new(StringArray::from(
        records
            .iter()
            .map(|r| format!("{:?}", r.class_method))
            .collect::<Vec<_>>(),
    ));
    let offset_mid: ArrayRef = Arc::new(Int32Array::from(
        records
            .iter()
            .map(|r| r.aggressor_offset_mid_bp)
            .collect::<Vec<_>>(),
    ));
    let confidence: ArrayRef = Arc::new(Float64Array::from(
        records
            .iter()
            .map(|r| r.aggressor_confidence)
            .collect::<Vec<_>>(),
    ));
    let nbbo_bid: ArrayRef = Arc::new(Float64Array::from(
        records.iter().map(|r| r.nbbo_bid).collect::<Vec<_>>(),
    ));
    let nbbo_ask: ArrayRef = Arc::new(Float64Array::from(
        records.iter().map(|r| r.nbbo_ask).collect::<Vec<_>>(),
    ));
    let nbbo_bid_sz: ArrayRef = Arc::new(UInt32Array::from(
        records.iter().map(|r| r.nbbo_bid_sz).collect::<Vec<_>>(),
    ));
    let nbbo_ask_sz: ArrayRef = Arc::new(UInt32Array::from(
        records.iter().map(|r| r.nbbo_ask_sz).collect::<Vec<_>>(),
    ));
    let nbbo_ts: ArrayRef = Arc::new(Int64Array::from(
        records.iter().map(|r| r.nbbo_ts_ns).collect::<Vec<_>>(),
    ));
    let nbbo_age: ArrayRef = Arc::new(UInt32Array::from(
        records.iter().map(|r| r.nbbo_age_us).collect::<Vec<_>>(),
    ));
    let nbbo_state: ArrayRef = Arc::new(StringArray::from(
        records
            .iter()
            .map(|r| r.nbbo_state.as_ref().map(|s| format!("{:?}", s)))
            .collect::<Vec<_>>(),
    ));
    let tick: ArrayRef = Arc::new(Float64Array::from(
        records.iter().map(|r| r.tick_size_used).collect::<Vec<_>>(),
    ));
    let source: ArrayRef = Arc::new(StringArray::from(
        records
            .iter()
            .map(|r| format!("{:?}", r.source))
            .collect::<Vec<_>>(),
    ));
    let quality: ArrayRef = Arc::new(StringArray::from(
        records
            .iter()
            .map(|r| format!("{:?}", r.quality))
            .collect::<Vec<_>>(),
    ));
    let watermark: ArrayRef = Arc::new(Int64Array::from(
        records
            .iter()
            .map(|r| r.watermark_ts_ns)
            .collect::<Vec<_>>(),
    ));

    let arrays = vec![
        instrument,
        underlying,
        trade_uid,
        trade_ts,
        window_ts,
        price,
        size,
        side,
        method,
        offset_mid,
        confidence,
        nbbo_bid,
        nbbo_ask,
        nbbo_bid_sz,
        nbbo_ask_sz,
        nbbo_ts,
        nbbo_age,
        nbbo_state,
        tick,
        source,
        quality,
        watermark,
    ];

    RecordBatch::try_new(schema, arrays).map_err(AggressorError::from)
}

fn write_artifact(
    state_dir: &Path,
    label: &str,
    family: &str,
    symbol: &str,
    window_meta: &WindowMeta,
    window_idx: WindowIndex,
    batch: &RecordBatch,
) -> Result<ArtifactInfo, AggressorError> {
    #[allow(deprecated)]
    let date = NaiveDateTime::from_timestamp_opt(window_meta.start_ts, 0)
        .map(|dt| dt.date())
        .ok_or(AggressorError::InvalidWindowTimestamp {
            window_ts: window_meta.start_ts,
        })?;
    let relative_path = format!(
        "aggressor/artifacts/{}/{}/{:04}/{:02}/{:02}/{:06}/{}.parquet",
        family,
        label,
        date.year(),
        date.month(),
        date.day(),
        window_idx,
        sanitize(symbol)
    );
    let final_path = state_dir.join(&relative_path);
    if let Some(parent) = final_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp_path = final_path.with_extension("parquet.tmp");
    if tmp_path.exists() {
        fs::remove_file(&tmp_path)?;
    }
    {
        let file = File::create(&tmp_path)?;
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None)?;
        writer.write(batch)?;
        writer.close()?;
    }
    if final_path.exists() {
        fs::remove_file(&final_path)?;
    }
    fs::rename(&tmp_path, &final_path)?;
    let checksum = compute_checksum(&final_path)?;
    Ok(ArtifactInfo {
        relative_path,
        checksum,
    })
}

fn compute_checksum(path: &Path) -> Result<u32, AggressorError> {
    let mut file = File::open(path)?;
    let mut buf = [0u8; 64 * 1024];
    let mut hasher = Crc32::new();
    loop {
        let read = file.read(&mut buf)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(hasher.finalize())
}

fn sanitize(symbol: &str) -> String {
    symbol
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

#[derive(Clone, Debug)]
struct ArtifactInfo {
    relative_path: String,
    checksum: u32,
}

#[derive(Clone)]
struct WorkItem {
    symbol: String,
    window_idx: WindowIndex,
    trade_payload_id: u32,
    quote_payload_id: u32,
    aggressor_version: u32,
}

enum WorkState {
    DidWork,
    NoWork,
}

enum EngineRuntimeState {
    Stopped,
    Running(ThreadBundle),
}

impl EngineRuntimeState {
    fn take_running(&mut self) -> Option<ThreadBundle> {
        match std::mem::replace(self, EngineRuntimeState::Stopped) {
            EngineRuntimeState::Running(bundle) => Some(bundle),
            other => {
                *self = other;
                None
            }
        }
    }
}

struct ThreadBundle {
    cancel: Arc<AtomicBool>,
    handle: thread::JoinHandle<()>,
}

#[derive(Clone, Copy)]
enum AggressorKind {
    Option,
    Underlying,
}

impl AggressorKind {
    fn label(&self) -> &'static str {
        match self {
            AggressorKind::Option => "option",
            AggressorKind::Underlying => "underlying",
        }
    }

    fn artifact_family(&self) -> &'static str {
        match self {
            AggressorKind::Option => "options",
            AggressorKind::Underlying => "underlying",
        }
    }

    fn quote_slot_kind(&self) -> TradeSlotKind {
        match self {
            AggressorKind::Option => TradeSlotKind::OptionQuote,
            AggressorKind::Underlying => TradeSlotKind::UnderlyingQuote,
        }
    }

    fn output_slot_kind(&self) -> TradeSlotKind {
        match self {
            AggressorKind::Option => TradeSlotKind::OptionAggressor,
            AggressorKind::Underlying => TradeSlotKind::UnderlyingAggressor,
        }
    }

    fn trade_slot_ref<'a>(&self, row: &'a window_space::ledger::TradeWindowRow) -> &'a Slot {
        match self {
            AggressorKind::Option => &row.option_trade_ref,
            AggressorKind::Underlying => &row.underlying_trade_ref,
        }
    }

    fn quote_slot_ref<'a>(&self, row: &'a window_space::ledger::TradeWindowRow) -> &'a Slot {
        match self {
            AggressorKind::Option => &row.option_quote_ref,
            AggressorKind::Underlying => &row.underlying_quote_ref,
        }
    }

    fn output_slot_ref<'a>(&self, row: &'a window_space::ledger::TradeWindowRow) -> &'a Slot {
        match self {
            AggressorKind::Option => &row.option_aggressor_ref,
            AggressorKind::Underlying => &row.underlying_aggressor_ref,
        }
    }

    fn write_output_slot(
        &self,
        controller: &WindowSpaceController,
        symbol: &str,
        window_idx: WindowIndex,
        meta: PayloadMeta,
        expected_version: Option<u32>,
    ) -> Result<(), WindowSpaceError> {
        match self {
            AggressorKind::Option => {
                controller.set_option_aggressor_ref(symbol, window_idx, meta, expected_version)?;
            }
            AggressorKind::Underlying => {
                controller.set_underlying_aggressor_ref(
                    symbol,
                    window_idx,
                    meta,
                    expected_version,
                )?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum AggressorError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("window space error: {0}")]
    Ledger(#[from] WindowSpaceError),
    #[error("missing payload id {payload_id}")]
    MissingPayload { payload_id: u32 },
    #[error("missing window metadata for index {window_idx}")]
    MissingWindowMeta { window_idx: WindowIndex },
    #[error("invalid window timestamp {window_ts}")]
    InvalidWindowTimestamp { window_ts: i64 },
}

impl From<AggressorError> for EngineError {
    fn from(value: AggressorError) -> Self {
        EngineError::Failure {
            source: Box::new(value),
        }
    }
}

fn string_col<'a>(batch: &'a RecordBatch, index: usize) -> Result<&'a StringArray, AggressorError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            AggressorError::Arrow(arrow::error::ArrowError::SchemaError(
                "invalid string column".into(),
            ))
        })
}

fn string_opt_col(
    batch: &RecordBatch,
    index: usize,
) -> Result<Vec<Option<String>>, AggressorError> {
    let array = string_col(batch, index)?;
    Ok((0..array.len())
        .map(|idx| {
            if array.is_null(idx) {
                None
            } else {
                Some(array.value(idx).to_string())
            }
        })
        .collect())
}

fn fixed_binary_col<'a>(
    batch: &'a RecordBatch,
    index: usize,
) -> Result<&'a FixedSizeBinaryArray, AggressorError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| {
            AggressorError::Arrow(arrow::error::ArrowError::SchemaError(
                "invalid fixed binary column".into(),
            ))
        })
}

fn float_col(batch: &RecordBatch, index: usize) -> Result<&Float64Array, AggressorError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| {
            AggressorError::Arrow(arrow::error::ArrowError::SchemaError(
                "invalid float column".into(),
            ))
        })
}

fn int32_col(batch: &RecordBatch, index: usize) -> Result<&Int32Array, AggressorError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| {
            AggressorError::Arrow(arrow::error::ArrowError::SchemaError(
                "invalid i32 column".into(),
            ))
        })
}

fn int32_opt_col(batch: &RecordBatch, index: usize) -> Result<Vec<Option<i32>>, AggressorError> {
    let array = int32_col(batch, index)?;
    Ok((0..array.len())
        .map(|idx| {
            if array.is_null(idx) {
                None
            } else {
                Some(array.value(idx))
            }
        })
        .collect())
}

fn uint32_col(batch: &RecordBatch, index: usize) -> Result<&UInt32Array, AggressorError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| {
            AggressorError::Arrow(arrow::error::ArrowError::SchemaError(
                "invalid u32 column".into(),
            ))
        })
}

fn uint64_opt_col(batch: &RecordBatch, index: usize) -> Result<Vec<Option<u64>>, AggressorError> {
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            AggressorError::Arrow(arrow::error::ArrowError::SchemaError(
                "invalid u64 column".into(),
            ))
        })?;
    Ok((0..array.len())
        .map(|idx| {
            if array.is_null(idx) {
                None
            } else {
                Some(array.value(idx))
            }
        })
        .collect())
}

fn int64_col(batch: &RecordBatch, index: usize) -> Result<&Int64Array, AggressorError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            AggressorError::Arrow(arrow::error::ArrowError::SchemaError(
                "invalid i64 column".into(),
            ))
        })
}

fn int64_opt_col(batch: &RecordBatch, index: usize) -> Result<Vec<Option<i64>>, AggressorError> {
    let array = int64_col(batch, index)?;
    Ok((0..array.len())
        .map(|idx| {
            if array.is_null(idx) {
                None
            } else {
                Some(array.value(idx))
            }
        })
        .collect())
}

fn list_i32_col(batch: &RecordBatch, index: usize) -> Result<Vec<Vec<i32>>, AggressorError> {
    let list = batch
        .column(index)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            AggressorError::Arrow(arrow::error::ArrowError::SchemaError(
                "invalid list column".into(),
            ))
        })?;
    let values = list
        .values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| {
            AggressorError::Arrow(arrow::error::ArrowError::SchemaError(
                "invalid list value column".into(),
            ))
        })?;
    let mut out = Vec::with_capacity(list.len());
    let offsets = list.value_offsets();
    for idx in 0..list.len() {
        if !list.is_valid(idx) {
            out.push(Vec::new());
            continue;
        }
        let start = offsets[idx] as usize;
        let end = offsets[idx + 1] as usize;
        let mut row = Vec::with_capacity(end - start);
        for value_idx in start..end {
            row.push(values.value(value_idx));
        }
        out.push(row);
    }
    Ok(out)
}

fn slice_uid(bytes: &[u8]) -> Result<[u8; 16], AggressorError> {
    if bytes.len() != 16 {
        return Err(AggressorError::Arrow(
            arrow::error::ArrowError::SchemaError("invalid uid length".into()),
        ));
    }
    let mut uid = [0u8; 16];
    uid.copy_from_slice(bytes);
    Ok(uid)
}
