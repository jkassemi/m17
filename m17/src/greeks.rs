use std::{
    fs::{self, File},
    io::{self, Read},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use arrow::{
    array::{
        Array, ArrayRef, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array, ListArray,
        StringArray, UInt32Array,
    },
    datatypes::SchemaRef,
    error::ArrowError,
    record_batch::RecordBatch,
};
use chrono::{DateTime, Datelike, Utc};
use core_types::{
    config::GreeksConfig,
    raw::OptionTradeRecord,
    schema::greeks_overlay_schema,
    types::{GreeksOverlayRow, Nbbo, NbboState, OptionTrade, Quality, Source},
    uid::TradeUid,
};
use crc32fast::Hasher as Crc32;
use engine_api::{
    Engine, EngineError, EngineHealth, EngineResult, HealthStatus, PriorityHookDescription,
};
use greeks_engine::{GreeksEngine, TreasuryCurve};
use log::{error, info, warn};
use nbbo_cache::NbboStore;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    errors::ParquetError,
};
use thiserror::Error;
use tokio::{
    runtime::{Builder as RuntimeBuilder, Runtime},
    sync::RwLock,
};
use window_space::{
    WindowIndex, WindowSpaceController, WindowSpaceError,
    ledger::TradeWindowRow,
    mapping::{GreeksPayload, QuoteBatchPayload, RfRatePayload, TradeBatchPayload},
    payload::{EnrichmentSlotKind, PayloadMeta, PayloadType, Slot, SlotKind, SlotStatus},
};

const GREEKS_SCHEMA_VERSION: u8 = 1;
const IDLE_BACKOFF: Duration = Duration::from_secs(2);

pub struct GreeksMaterializationEngine {
    inner: Arc<GreeksInner>,
}

impl GreeksMaterializationEngine {
    pub fn new(
        controller: Arc<WindowSpaceController>,
        symbol: impl Into<String>,
        env_label: impl Into<String>,
        cfg: GreeksConfig,
        staleness_us: u32,
    ) -> Self {
        let inner = GreeksInner::new(
            controller,
            symbol.into(),
            env_label.into(),
            cfg,
            staleness_us,
        );
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl Engine for GreeksMaterializationEngine {
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
        PriorityHookDescription::default()
    }
}

struct GreeksInner {
    controller: Arc<WindowSpaceController>,
    symbol: String,
    env_label: String,
    cfg: GreeksConfig,
    staleness_us: u32,
    runner_state: Mutex<Option<RunnerHandle>>,
    health: Mutex<EngineHealth>,
}

struct RunnerHandle {
    stop: Arc<AtomicBool>,
    thread: thread::JoinHandle<()>,
}

impl GreeksInner {
    fn new(
        controller: Arc<WindowSpaceController>,
        symbol: String,
        env_label: String,
        cfg: GreeksConfig,
        staleness_us: u32,
    ) -> Self {
        Self {
            controller,
            symbol,
            env_label,
            cfg,
            staleness_us,
            runner_state: Mutex::new(None),
            health: Mutex::new(EngineHealth::new(HealthStatus::Stopped, None)),
        }
    }

    fn start(&self) -> EngineResult<()> {
        let mut guard = self.runner_state.lock().unwrap();
        if guard.is_some() {
            return Err(EngineError::AlreadyRunning);
        }
        let stop = Arc::new(AtomicBool::new(false));
        let runner = GreeksRunner::new(
            Arc::clone(&self.controller),
            self.symbol.clone(),
            self.env_label.clone(),
            self.cfg.clone(),
            self.staleness_us,
        );
        let stop_clone = Arc::clone(&stop);
        let thread = thread::spawn(move || runner.run(stop_clone));
        *guard = Some(RunnerHandle { stop, thread });
        self.set_health(HealthStatus::Ready, None);
        Ok(())
    }

    fn stop(&self) -> EngineResult<()> {
        let handle = {
            let mut guard = self.runner_state.lock().unwrap();
            guard.take()
        };
        let Some(handle) = handle else {
            return Err(EngineError::NotRunning);
        };
        handle.stop.store(true, Ordering::SeqCst);
        if let Err(err) = handle.thread.join() {
            error!("greeks engine thread join failed: {:?}", err);
        }
        self.set_health(HealthStatus::Stopped, None);
        Ok(())
    }

    fn health(&self) -> EngineHealth {
        self.health.lock().unwrap().clone()
    }

    fn set_health(&self, status: HealthStatus, detail: Option<String>) {
        let mut guard = self.health.lock().unwrap();
        guard.status = status;
        guard.detail = detail;
    }
}

struct GreeksRunner {
    controller: Arc<WindowSpaceController>,
    symbol: String,
    env_label: String,
    cfg: GreeksConfig,
    staleness_us: u32,
    state_dir: PathBuf,
}

impl GreeksRunner {
    fn new(
        controller: Arc<WindowSpaceController>,
        symbol: String,
        env_label: String,
        cfg: GreeksConfig,
        staleness_us: u32,
    ) -> Self {
        let state_dir = controller.config().state_dir().to_path_buf();
        Self {
            controller,
            symbol,
            env_label,
            cfg,
            staleness_us,
            state_dir,
        }
    }

    fn run(self, stop: Arc<AtomicBool>) {
        let runtime = build_runtime();
        let total_windows = self.controller.window_space().len() as WindowIndex;
        let mut window_idx: WindowIndex = 0;
        let mut made_progress = false;
        while !stop.load(Ordering::SeqCst) {
            match self.process_window(window_idx, &runtime) {
                Ok(true) => {
                    made_progress = true;
                }
                Ok(false) => {}
                Err(err) => {
                    error!(
                        "greeks-engine: window {} failed for {}: {}",
                        window_idx, self.symbol, err
                    );
                }
            }
            window_idx = window_idx.wrapping_add(1);
            if window_idx >= total_windows {
                window_idx = 0;
                if !made_progress {
                    thread::sleep(IDLE_BACKOFF);
                }
                made_progress = false;
            }
        }
        info!("greeks-engine: exiting runner for symbol {}", self.symbol);
    }

    fn process_window(
        &self,
        window_idx: WindowIndex,
        runtime: &Runtime,
    ) -> Result<bool, GreeksError> {
        let trade_row = self
            .controller
            .get_trade_row(&self.symbol, window_idx)
            .map_err(GreeksError::Controller)?;
        if !slot_ready(&trade_row.rf_rate)
            || !slot_ready(&trade_row.option_trade_ref)
            || !slot_ready(&trade_row.option_quote_ref)
            || !slot_ready(&trade_row.underlying_quote_ref)
        {
            return Ok(false);
        }
        let target_version = trade_row.rf_rate.version;
        if target_version == 0 {
            return Ok(false);
        }
        let enrichment_row = self
            .controller
            .get_enrichment_row(&self.symbol, window_idx)
            .map_err(GreeksError::Controller)?;
        let dependency_version = dependency_signature(&trade_row);
        if enrichment_row.greeks.status == SlotStatus::Pending {
            return Ok(false);
        }
        if enrichment_row.greeks.status == SlotStatus::Filled
            && enrichment_row.greeks.version == dependency_version
        {
            return Ok(false);
        }

        let payloads = {
            let stores = self.controller.payload_stores();
            let trades = stores
                .trades
                .get(trade_row.option_trade_ref.payload_id)
                .cloned()
                .ok_or(GreeksError::MissingPayload {
                    kind: "trade",
                    payload_id: trade_row.option_trade_ref.payload_id,
                })?;
            let option_quotes = stores
                .quotes
                .get(trade_row.option_quote_ref.payload_id)
                .cloned()
                .ok_or(GreeksError::MissingPayload {
                    kind: "option_quote",
                    payload_id: trade_row.option_quote_ref.payload_id,
                })?;
            let underlying_quotes = stores
                .quotes
                .get(trade_row.underlying_quote_ref.payload_id)
                .cloned()
                .ok_or(GreeksError::MissingPayload {
                    kind: "underlying_quote",
                    payload_id: trade_row.underlying_quote_ref.payload_id,
                })?;
            let rf = stores
                .rf_rate
                .get(trade_row.rf_rate.payload_id)
                .cloned()
                .ok_or(GreeksError::MissingPayload {
                    kind: "rf_rate",
                    payload_id: trade_row.rf_rate.payload_id,
                })?;
            (trades, option_quotes, underlying_quotes, rf)
        };

        self.controller
            .mark_pending(
                &self.symbol,
                window_idx,
                SlotKind::Enrichment(EnrichmentSlotKind::Greeks),
            )
            .map_err(GreeksError::Controller)?;

        let result = self.emit_greeks_payload(
            window_idx,
            dependency_version,
            trade_row.rf_rate.payload_id,
            &payloads.0,
            &payloads.1,
            &payloads.2,
            &payloads.3,
            runtime,
        );

        match result {
            Ok((payload_id, checksum)) => {
                let meta = PayloadMeta::new(
                    PayloadType::Greeks,
                    payload_id,
                    dependency_version,
                    checksum,
                );
                if let Err(err) = self.controller.set_greeks_ref(
                    &self.symbol,
                    window_idx,
                    meta,
                    Some(enrichment_row.greeks.version),
                ) {
                    error!(
                        "greeks-engine: slot write failed for {}:{} -> {}",
                        self.symbol, window_idx, err
                    );
                    let _ = self.controller.clear_slot(
                        &self.symbol,
                        window_idx,
                        SlotKind::Enrichment(EnrichmentSlotKind::Greeks),
                    );
                    return Err(GreeksError::from(err));
                }
                Ok(true)
            }
            Err(err) => {
                let _ = self.controller.clear_slot(
                    &self.symbol,
                    window_idx,
                    SlotKind::Enrichment(EnrichmentSlotKind::Greeks),
                );
                Err(err)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_greeks_payload(
        &self,
        window_idx: WindowIndex,
        dependency_version: u32,
        rf_rate_payload_id: u32,
        trade_payload: &TradeBatchPayload,
        option_quote_payload: &QuoteBatchPayload,
        underlying_quote_payload: &QuoteBatchPayload,
        rf_payload: &RfRatePayload,
        runtime: &Runtime,
    ) -> Result<(u32, u32), GreeksError> {
        let trades_path = self.state_dir.join(&trade_payload.artifact_uri);
        let option_quotes_path = self.state_dir.join(&option_quote_payload.artifact_uri);
        let underlying_quotes_path = self.state_dir.join(&underlying_quote_payload.artifact_uri);

        let mut trades = load_option_trades(trades_path.as_path())?
            .into_iter()
            .map(option_trade_from_record)
            .collect::<Vec<_>>();
        if trades.is_empty() {
            warn!(
                "greeks-engine: window {} for {} had no option trades; still marking filled",
                window_idx, self.symbol
            );
        }

        let option_nbbo = load_nbbo_quotes(option_quotes_path.as_path())?;
        let underlying_nbbo = load_nbbo_quotes(underlying_quotes_path.as_path())?;
        let nbbo_store = Arc::new(RwLock::new(NbboStore::new()));
        runtime.block_on(async {
            let mut guard = nbbo_store.write().await;
            for quote in option_nbbo.iter().chain(underlying_nbbo.iter()) {
                guard.put(quote);
            }
        });

        let curve = curve_from_payload(rf_payload)
            .ok_or(GreeksError::MissingTreasuryCurve { window_idx })?;
        let curve_state = Arc::new(RwLock::new(Some(curve)));
        let engine =
            GreeksEngine::new(self.cfg.clone(), nbbo_store, self.staleness_us, curve_state);
        runtime.block_on(async { engine.enrich_batch(&mut trades).await });

        let overlays = build_overlays(&trades, rf_payload, &self.env_label);
        let artifact_uri = greeks_artifact_path(
            &self.env_label,
            &self.symbol,
            window_idx,
            trade_payload.window_ts,
        );
        let checksum = write_greeks_artifact(self.state_dir.join(&artifact_uri), &overlays)?;

        let payload_id = {
            let mut stores = self.controller.payload_stores();
            stores.greeks.append(GreeksPayload {
                schema_version: GREEKS_SCHEMA_VERSION,
                window_ts: trade_payload.window_ts,
                rf_rate_payload_id,
                greeks_version: dependency_version,
                artifact_uri: artifact_uri.clone(),
                checksum,
            })?
        };

        info!(
            "greeks-engine: wrote {} rows for {} window {}",
            overlays.len(),
            self.symbol,
            window_idx
        );
        Ok((payload_id, checksum))
    }
}

fn build_runtime() -> Runtime {
    RuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

fn slot_ready(slot: &Slot) -> bool {
    matches!(slot.status, SlotStatus::Filled) && slot.payload_id != 0
}

fn dependency_signature(row: &TradeWindowRow) -> u32 {
    let mut hasher = Crc32::new();
    hasher.update(&row.rf_rate.payload_id.to_le_bytes());
    hasher.update(&row.option_trade_ref.payload_id.to_le_bytes());
    hasher.update(&row.option_quote_ref.payload_id.to_le_bytes());
    hasher.update(&row.underlying_quote_ref.payload_id.to_le_bytes());
    hasher.finalize()
}

fn greeks_artifact_path(
    label: &str,
    symbol: &str,
    window_idx: WindowIndex,
    window_ts_ns: i64,
) -> String {
    let timestamp = DateTime::<Utc>::from_timestamp(window_ts_ns / 1_000_000_000, 0)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap());
    format!(
        "greeks/artifacts/{label}/{symbol}/{:04}/{:02}/{:02}/{:06}.parquet",
        timestamp.year(),
        timestamp.month(),
        timestamp.day(),
        window_idx,
    )
}

fn write_greeks_artifact(path: PathBuf, rows: &[GreeksOverlayRow]) -> Result<u32, GreeksError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(&path)?;
    let schema: SchemaRef = Arc::new(greeks_overlay_schema());
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    let batch = overlays_to_record_batch(rows)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(file_checksum(&path)?)
}

fn overlays_to_record_batch(rows: &[GreeksOverlayRow]) -> Result<RecordBatch, GreeksError> {
    let len = rows.len();
    let mut trade_uids = Vec::with_capacity(len);
    let mut trade_ts_ns = Vec::with_capacity(len);
    let mut contract = Vec::with_capacity(len);
    let mut underlying = Vec::with_capacity(len);
    let mut delta = Vec::with_capacity(len);
    let mut gamma = Vec::with_capacity(len);
    let mut vega = Vec::with_capacity(len);
    let mut theta = Vec::with_capacity(len);
    let mut iv = Vec::with_capacity(len);
    let mut flags = Vec::with_capacity(len);
    let mut nbbo_bid = Vec::with_capacity(len);
    let mut nbbo_ask = Vec::with_capacity(len);
    let mut nbbo_bid_sz = Vec::with_capacity(len);
    let mut nbbo_ask_sz = Vec::with_capacity(len);
    let mut nbbo_ts_ns = Vec::with_capacity(len);
    let mut nbbo_age_us = Vec::with_capacity(len);
    let mut nbbo_state = Vec::with_capacity(len);
    let mut engine_version = Vec::with_capacity(len);
    let mut curve_date = Vec::with_capacity(len);
    let mut curve_source_date = Vec::with_capacity(len);
    let mut enriched_at_ns = Vec::with_capacity(len);
    let mut run_ids = Vec::with_capacity(len);
    for row in rows {
        trade_uids.push(row.trade_uid);
        trade_ts_ns.push(row.trade_ts_ns);
        contract.push(row.contract.clone());
        underlying.push(row.underlying.clone());
        delta.push(row.delta);
        gamma.push(row.gamma);
        vega.push(row.vega);
        theta.push(row.theta);
        iv.push(row.iv);
        flags.push(row.greeks_flags);
        nbbo_bid.push(row.nbbo_bid);
        nbbo_ask.push(row.nbbo_ask);
        nbbo_bid_sz.push(row.nbbo_bid_sz);
        nbbo_ask_sz.push(row.nbbo_ask_sz);
        nbbo_ts_ns.push(row.nbbo_ts_ns);
        nbbo_age_us.push(row.nbbo_age_us);
        nbbo_state.push(row.nbbo_state.as_ref().map(|state| format!("{:?}", state)));
        engine_version.push(row.engine_version.clone());
        curve_date.push(row.treasury_curve_date.clone());
        curve_source_date.push(row.treasury_curve_source_date.clone());
        enriched_at_ns.push(row.enriched_at_ns);
        run_ids.push(row.run_id.clone());
    }
    let schema: SchemaRef = Arc::new(greeks_overlay_schema());
    let trade_uid_array =
        FixedSizeBinaryArray::try_from_iter(trade_uids.iter().map(|uid| uid.as_ref()))?;
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(trade_uid_array),
        Arc::new(Int64Array::from(trade_ts_ns)),
        Arc::new(StringArray::from(contract)),
        Arc::new(StringArray::from(underlying)),
        Arc::new(Float64Array::from(delta)),
        Arc::new(Float64Array::from(gamma)),
        Arc::new(Float64Array::from(vega)),
        Arc::new(Float64Array::from(theta)),
        Arc::new(Float64Array::from(iv)),
        Arc::new(UInt32Array::from(flags)),
        Arc::new(Float64Array::from(nbbo_bid)),
        Arc::new(Float64Array::from(nbbo_ask)),
        Arc::new(UInt32Array::from(nbbo_bid_sz)),
        Arc::new(UInt32Array::from(nbbo_ask_sz)),
        Arc::new(Int64Array::from(nbbo_ts_ns)),
        Arc::new(UInt32Array::from(nbbo_age_us)),
        Arc::new(StringArray::from(nbbo_state)),
        Arc::new(StringArray::from(engine_version)),
        Arc::new(StringArray::from(curve_date)),
        Arc::new(StringArray::from(curve_source_date)),
        Arc::new(Int64Array::from(enriched_at_ns)),
        Arc::new(StringArray::from(run_ids)),
    ];
    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn option_trade_from_record(record: OptionTradeRecord) -> OptionTrade {
    OptionTrade {
        contract: record.contract,
        trade_uid: record.trade_uid,
        contract_direction: record.contract_direction,
        strike_price: record.strike_price,
        underlying: record.underlying,
        trade_ts_ns: record.trade_ts_ns,
        price: record.price,
        size: record.size,
        conditions: record.conditions,
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
        underlying_nbbo_bid: None,
        underlying_nbbo_ask: None,
        underlying_nbbo_bid_sz: None,
        underlying_nbbo_ask_sz: None,
        underlying_nbbo_ts_ns: None,
        underlying_nbbo_age_us: None,
        underlying_nbbo_state: None,
        tick_size_used: None,
        delta: None,
        gamma: None,
        vega: None,
        theta: None,
        iv: None,
        greeks_flags: 0,
        source: record.source,
        quality: record.quality,
        watermark_ts_ns: record.watermark_ts_ns,
    }
}

fn build_overlays(
    trades: &[OptionTrade],
    rf_payload: &RfRatePayload,
    env_label: &str,
) -> Vec<GreeksOverlayRow> {
    let enriched_at = current_time_ns();
    let run_id = format!("greeks-{env_label}-{}", enriched_at);
    let curve_date = timestamp_to_date_string(rf_payload.effective_ts);
    let curve_source = timestamp_to_date_string(rf_payload.effective_ts);
    trades
        .iter()
        .map(|trade| GreeksOverlayRow {
            trade_uid: trade.trade_uid,
            trade_ts_ns: trade.trade_ts_ns,
            contract: trade.contract.clone(),
            underlying: trade.underlying.clone(),
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
            nbbo_state: trade.nbbo_state.clone(),
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
            treasury_curve_date: curve_date.clone(),
            treasury_curve_source_date: curve_source.clone(),
            enriched_at_ns: enriched_at,
            run_id: run_id.clone(),
        })
        .collect()
}

fn timestamp_to_date_string(ts: i64) -> Option<String> {
    DateTime::<Utc>::from_timestamp(ts, 0).map(|dt| dt.date_naive().to_string())
}

fn current_time_ns() -> i64 {
    let now = Utc::now();
    now.timestamp_nanos_opt()
        .unwrap_or_else(|| now.timestamp() * 1_000_000_000)
}

fn curve_from_payload(payload: &RfRatePayload) -> Option<Arc<TreasuryCurve>> {
    const TENORS: [f64; 11] = [
        1.0 / 12.0,
        0.25,
        0.5,
        1.0,
        2.0,
        3.0,
        5.0,
        7.0,
        10.0,
        20.0,
        30.0,
    ];
    let mut points = Vec::new();
    for (idx, tenor) in TENORS.iter().enumerate() {
        if let Some(bps) = payload.tenor_bps.get(idx) {
            points.push((*tenor, *bps as f64 / 10_000.0));
        }
    }
    TreasuryCurve::from_pairs(points).map(Arc::new)
}

fn load_option_trades(path: &Path) -> Result<Vec<OptionTradeRecord>, GreeksError> {
    read_record_batch(path, parse_option_batch)
}

fn load_nbbo_quotes(path: &Path) -> Result<Vec<Nbbo>, GreeksError> {
    read_record_batch(path, parse_quote_batch)
}

fn read_record_batch<T, F>(path: &Path, mut parser: F) -> Result<Vec<T>, GreeksError>
where
    F: FnMut(&RecordBatch) -> Result<Vec<T>, GreeksError>,
{
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.build()?;
    let mut rows = Vec::new();
    while let Some(batch) = reader.next() {
        let batch = batch?;
        rows.extend(parser(&batch)?);
    }
    Ok(rows)
}

fn parse_option_batch(batch: &RecordBatch) -> Result<Vec<OptionTradeRecord>, GreeksError> {
    let contract = string_col(batch, 0)?;
    let trade_uid = fixed_binary_col(batch, 1)?;
    let direction = string_col(batch, 2)?;
    let strike = float_col(batch, 3)?;
    let underlying = string_col(batch, 4)?;
    let trade_ts = int64_col(batch, 5)?;
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
            participant_ts_ns: None,
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

fn parse_quote_batch(batch: &RecordBatch) -> Result<Vec<Nbbo>, GreeksError> {
    let instrument = string_col(batch, 0)?;
    let quote_uid = fixed_binary_col(batch, 1)?;
    let quote_ts = int64_col(batch, 2)?;
    let bid = float_col(batch, 3)?;
    let ask = float_col(batch, 4)?;
    let bid_sz = uint32_col(batch, 5)?;
    let ask_sz = uint32_col(batch, 6)?;
    let state = string_col(batch, 7)?;
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
            condition: None,
            best_bid_venue: None,
            best_ask_venue: None,
            source: Source::Flatfile,
            quality: Quality::Prelim,
            watermark_ts_ns: quote_ts.value(idx),
        });
    }
    Ok(rows)
}

fn parse_nbbo_state(value: &str) -> NbboState {
    match value {
        "Locked" => NbboState::Locked,
        "Crossed" => NbboState::Crossed,
        _ => NbboState::Normal,
    }
}

fn file_checksum(path: &Path) -> io::Result<u32> {
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

fn string_col(batch: &RecordBatch, idx: usize) -> Result<&StringArray, GreeksError> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ArrowError::SchemaError(format!("column {idx} not string")))
        .map_err(GreeksError::from)
}

fn float_col(batch: &RecordBatch, idx: usize) -> Result<&Float64Array, GreeksError> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| ArrowError::SchemaError(format!("column {idx} not f64")))
        .map_err(GreeksError::from)
}

fn int64_col(batch: &RecordBatch, idx: usize) -> Result<&Int64Array, GreeksError> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| ArrowError::SchemaError(format!("column {idx} not i64")))
        .map_err(GreeksError::from)
}

fn uint32_col(batch: &RecordBatch, idx: usize) -> Result<&UInt32Array, GreeksError> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| ArrowError::SchemaError(format!("column {idx} not u32")))
        .map_err(GreeksError::from)
}

fn int32_col(batch: &RecordBatch, idx: usize) -> Result<&Int32Array, GreeksError> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| ArrowError::SchemaError(format!("column {idx} not i32")))
        .map_err(GreeksError::from)
}

fn fixed_binary_col(batch: &RecordBatch, idx: usize) -> Result<&FixedSizeBinaryArray, GreeksError> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| ArrowError::SchemaError(format!("column {idx} not fixed binary")))
        .map_err(GreeksError::from)
}

fn list_i32_col(batch: &RecordBatch, idx: usize) -> Result<Vec<Vec<i32>>, GreeksError> {
    let array = batch
        .column(idx)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| ArrowError::SchemaError(format!("column {idx} not list")))
        .map_err(GreeksError::from)?;
    let values = array
        .values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| ArrowError::SchemaError("list values not i32".into()))
        .map_err(GreeksError::from)?;
    let offsets = array.value_offsets();
    let mut out = Vec::with_capacity(array.len());
    for row_idx in 0..array.len() {
        if array.is_null(row_idx) {
            out.push(Vec::new());
            continue;
        }
        let start = offsets[row_idx as usize] as usize;
        let end = offsets[row_idx as usize + 1] as usize;
        let mut row = Vec::with_capacity(end - start);
        for value_idx in start..end {
            row.push(values.value(value_idx));
        }
        out.push(row);
    }
    Ok(out)
}

fn slice_uid(bytes: &[u8]) -> Result<TradeUid, GreeksError> {
    if bytes.len() != 16 {
        return Err(ArrowError::SchemaError("invalid uid length".into()).into());
    }
    let mut uid = [0u8; 16];
    uid.copy_from_slice(bytes);
    Ok(uid)
}

#[derive(Debug, Error)]
pub enum GreeksError {
    #[error("controller error: {0}")]
    Controller(#[from] WindowSpaceError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("parquet error: {0}")]
    Parquet(#[from] ParquetError),
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    #[error("missing {kind} payload id {payload_id}")]
    MissingPayload { kind: &'static str, payload_id: u32 },
    #[error("missing treasury curve for window {window_idx}")]
    MissingTreasuryCurve { window_idx: WindowIndex },
}
