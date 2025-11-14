use std::{
    fs::{self, File, OpenOptions},
    io::{BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use aggregations::{AggregationInput, AggregationsEngine};
use arrow::{
    array::{
        Array, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array, ListArray, StringArray,
        UInt32Array, UInt64Array,
    },
    record_batch::RecordBatch,
};
use chrono::{DateTime, Utc};
use core_types::{
    config::AggregationsConfig,
    raw::{OptionTradeRecord, UnderlyingTradeRecord},
    types::{
        AggregationRow, AggressorSide, ClassMethod, EquityTrade, OptionTrade, Quality, Source,
    },
};
use crc32fast::Hasher as Crc32;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use thiserror::Error;
use window_space::{
    WindowSpaceController,
    error::WindowSpaceError,
    mapping::{AggregationPayload, PayloadStores},
    payload::{EnrichmentSlotKind, PayloadMeta, PayloadType, Slot, SlotKind, SlotStatus},
    window::{WindowIndex, WindowMeta, WindowSpace},
};

const AGG_WINDOW_SPEC: &str = "10m";
const AGG_WINDOW_MINUTES: usize = 10;
const REPORT_FILE: &str = "OUTPUT.txt";
const AGG_SCHEMA_VERSION: u8 = 1;

pub struct AggregationService {
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl AggregationService {
    pub fn start(controller: Arc<WindowSpaceController>, symbol: &str) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let symbol = symbol.to_string();
        let report_path = PathBuf::from(REPORT_FILE);
        let controller_clone = Arc::clone(&controller);
        let stop_flag = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            let mut runner = match AggregationRunner::new(controller_clone, symbol, report_path) {
                Ok(runner) => runner,
                Err(err) => {
                    eprintln!("aggregation: init failed: {err}");
                    return;
                }
            };
            runner.run(stop_flag);
        });
        Self {
            stop,
            handle: Some(handle),
        }
    }

    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

struct AggregationRunner {
    controller: Arc<WindowSpaceController>,
    symbol: String,
    state_dir: PathBuf,
    report_path: PathBuf,
    engine: AggregationsEngine,
    next_window_idx: WindowIndex,
    total_windows: WindowIndex,
    _ten_minute_space: WindowSpace,
}

impl AggregationRunner {
    fn new(
        controller: Arc<WindowSpaceController>,
        symbol: String,
        report_path: PathBuf,
    ) -> Result<Self, AggregationError> {
        let mut agg_cfg = AggregationsConfig::default();
        agg_cfg.windows = vec![AGG_WINDOW_SPEC.to_string()];
        agg_cfg.symbol = symbol.clone();
        let engine = AggregationsEngine::new(agg_cfg)?;
        let base_space = controller.window_space();
        let total_windows = base_space.len() as WindowIndex;
        let ten_minute_space = build_ten_minute_space(base_space.as_ref());
        let state_dir = controller.config().state_dir().to_path_buf();
        println!(
            "aggregation: spawned {} windows of {} for symbol {}",
            ten_minute_space.len(),
            AGG_WINDOW_SPEC,
            symbol
        );
        Ok(Self {
            controller,
            symbol,
            state_dir,
            report_path,
            engine,
            next_window_idx: 0,
            total_windows,
            _ten_minute_space: ten_minute_space,
        })
    }

    fn run(&mut self, stop: Arc<AtomicBool>) {
        while !stop.load(Ordering::SeqCst) {
            match self.process_ready_windows() {
                Ok(progress) => {
                    if progress == 0 {
                        thread::sleep(Duration::from_secs(2));
                    }
                }
                Err(err) => {
                    eprintln!("aggregation: processing error: {err}");
                    thread::sleep(Duration::from_secs(5));
                }
            }
            if self.next_window_idx >= self.total_windows {
                break;
            }
        }
        if let Err(err) = self.flush_remaining() {
            eprintln!("aggregation: flush error: {err}");
        }
    }

    fn process_ready_windows(&mut self) -> Result<usize, AggregationError> {
        let mut persisted = 0usize;
        while self.next_window_idx < self.total_windows {
            let window_idx = self.next_window_idx;
            let trade_row = self
                .controller
                .get_trade_row(&self.symbol, window_idx)
                .map_err(AggregationError::Controller)?;
            if !window_ready(&trade_row.option_trade_ref) {
                break;
            }
            let events = self.collect_events(window_idx, &trade_row)?;
            self.next_window_idx = self.next_window_idx.saturating_add(1);
            if events.is_empty() {
                continue;
            }
            let mut events = events;
            events.sort_by_key(|event| event_timestamp(event));
            for event in events {
                let rows = self.engine.ingest(event);
                persisted += self.persist_rows(rows)?;
            }
        }
        Ok(persisted)
    }

    fn flush_remaining(&mut self) -> Result<(), AggregationError> {
        let rows = self.engine.drain();
        self.persist_rows(rows)?;
        Ok(())
    }

    fn collect_events(
        &self,
        _window_idx: WindowIndex,
        trade_row: &window_space::ledger::TradeWindowRow,
    ) -> Result<Vec<AggregationInput>, AggregationError> {
        let mut events = Vec::new();
        if let Some(payload_id) = payload_id(&trade_row.option_trade_ref) {
            let records = self.read_option_payload(payload_id)?;
            for record in records {
                let trade = option_trade_from_record(&record);
                events.push(AggregationInput::OptionTrade {
                    trade,
                    underlying_price: None,
                });
            }
        }
        if let Some(payload_id) = payload_id(&trade_row.underlying_trade_ref) {
            let records = self.read_underlying_payload(payload_id)?;
            for record in records {
                let trade = equity_trade_from_record(&record);
                events.push(AggregationInput::UnderlyingTrade(trade));
            }
        }
        Ok(events)
    }

    fn read_option_payload(
        &self,
        payload_id: u32,
    ) -> Result<Vec<OptionTradeRecord>, AggregationError> {
        let payload = self.lookup_trade_payload(payload_id)?;
        let path = self.state_dir.join(&payload.artifact_uri);
        read_option_trade_records(path.as_path())
    }

    fn read_underlying_payload(
        &self,
        payload_id: u32,
    ) -> Result<Vec<UnderlyingTradeRecord>, AggregationError> {
        let payload = self.lookup_trade_payload(payload_id)?;
        let path = self.state_dir.join(&payload.artifact_uri);
        read_underlying_trade_records(path.as_path())
    }

    fn lookup_trade_payload(
        &self,
        payload_id: u32,
    ) -> Result<window_space::mapping::TradeBatchPayload, AggregationError> {
        let stores = self.controller.payload_stores();
        stores
            .trades
            .get(payload_id)
            .cloned()
            .ok_or(AggregationError::MissingPayload { payload_id })
    }

    fn persist_rows(&self, rows: Vec<AggregationRow>) -> Result<usize, AggregationError> {
        let mut written = 0usize;
        for row in rows {
            if row.symbol != self.symbol {
                continue;
            }
            if self.persist_row(&row)? {
                written += 1;
            }
        }
        Ok(written)
    }

    fn persist_row(&self, row: &AggregationRow) -> Result<bool, AggregationError> {
        let start_ns = row.window_start_ns;
        let start_sec = start_ns / 1_000_000_000;
        let window_idx = self
            .controller
            .window_idx_for_timestamp(start_sec)
            .ok_or(AggregationError::MissingWindowIndex { start_ns })?;
        let enrichment_row = self
            .controller
            .get_enrichment_row(&self.symbol, window_idx)
            .map_err(AggregationError::Controller)?;
        if matches!(
            enrichment_row.aggregation.status,
            SlotStatus::Filled | SlotStatus::Pending | SlotStatus::Retire | SlotStatus::Retired
        ) {
            return Ok(false);
        }
        self.controller
            .mark_pending(
                &self.symbol,
                window_idx,
                SlotKind::Enrichment(EnrichmentSlotKind::Aggregation),
            )
            .map_err(AggregationError::Controller)?;
        let (artifact_uri, checksum, manifest_line) = self.write_artifact(row)?;
        let payload_id = {
            let mut stores = self.controller.payload_stores();
            append_aggregation_payload(
                &mut *stores,
                AggregationPayload {
                    schema_version: AGG_SCHEMA_VERSION,
                    symbol: row.symbol.clone(),
                    window: row.window.clone(),
                    window_start_ns: row.window_start_ns,
                    window_end_ns: row.window_end_ns,
                    artifact_uri: artifact_uri.clone(),
                    checksum,
                },
            )?
        };
        let meta = PayloadMeta::new(PayloadType::Aggregation, payload_id, 1, checksum);
        if let Err(err) = self
            .controller
            .set_aggregation_ref(&self.symbol, window_idx, meta, None)
        {
            let _ = self.controller.clear_slot(
                &self.symbol,
                window_idx,
                SlotKind::Enrichment(EnrichmentSlotKind::Aggregation),
            );
            return Err(AggregationError::Controller(err));
        }
        self.append_report(&manifest_line)?;
        Ok(true)
    }

    fn write_artifact(
        &self,
        row: &AggregationRow,
    ) -> Result<(String, u32, String), AggregationError> {
        let dt = DateTime::<Utc>::from_timestamp(
            row.window_start_ns / 1_000_000_000,
            (row.window_start_ns % 1_000_000_000) as u32,
        )
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap());
        let date = dt.date_naive();
        let rel_dir = format!(
            "aggregations/manual/symbol={}/window={}/dt={}",
            row.symbol, row.window, date
        );
        let rel_path = format!("{}/{}.txt", rel_dir, row.window_start_ns);
        let abs_path = self.state_dir.join(&rel_path);
        if let Some(parent) = abs_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let summary = render_row(row);
        let mut writer = BufWriter::new(File::create(&abs_path)?);
        writer.write_all(summary.as_bytes())?;
        writer.flush()?;
        let checksum = file_checksum(&abs_path)?;
        Ok((rel_path, checksum, summary))
    }

    fn append_report(&self, line: &str) -> Result<(), AggregationError> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.report_path)?;
        file.write_all(line.as_bytes())?;
        if !line.ends_with('\n') {
            file.write_all(b"\n")?;
        }
        file.flush()?;
        Ok(())
    }
}

fn window_ready(slot: &Slot) -> bool {
    matches!(slot.status, SlotStatus::Filled)
}

fn payload_id(slot: &Slot) -> Option<u32> {
    if slot.payload_id == 0 {
        None
    } else {
        Some(slot.payload_id)
    }
}

fn build_ten_minute_space(base: &WindowSpace) -> WindowSpace {
    let mut windows = Vec::new();
    let mut chunk = Vec::new();
    let mut next_idx: WindowIndex = 0;
    for meta in base.iter() {
        chunk.push(*meta);
        if chunk.len() == AGG_WINDOW_MINUTES {
            let start = chunk.first().copied().unwrap();
            let duration = chunk.len() as u32 * start.duration_secs;
            let aggregated =
                WindowMeta::new(next_idx, start.start_ts, duration, start.schema_version);
            windows.push(aggregated);
            chunk.clear();
            next_idx = next_idx.saturating_add(1);
        }
    }
    WindowSpace::new(windows)
}

fn append_aggregation_payload(
    stores: &mut PayloadStores,
    payload: AggregationPayload,
) -> Result<u32, AggregationError> {
    stores.aggregations.append(payload).map_err(Into::into)
}

fn render_row(row: &AggregationRow) -> String {
    let start = format_ts(row.window_start_ns);
    let end = format_ts(row.window_end_ns);
    let mut output = String::new();
    output.push_str(&format!("[{}] {} → {}\n", row.symbol, start, end));
    output.push_str(&format!("  window: {}\n", row.window));
    output.push_str(&format!(
        "  underlying O/H/L/C: {}/{}/{}/{}\n",
        fmt_opt(row.underlying_price_open),
        fmt_opt(row.underlying_price_high),
        fmt_opt(row.underlying_price_low),
        fmt_opt(row.underlying_price_close),
    ));
    output.push_str(&format!(
        "  underlying dv total: {}\n",
        fmt_opt(row.underlying_dollar_value_total)
    ));
    output.push_str(&format!(
        "    min={} max={} mean={} stddev={} skew={} kurt={}\n",
        fmt_opt(row.underlying_dollar_value_minimum),
        fmt_opt(row.underlying_dollar_value_maximum),
        fmt_opt(row.underlying_dollar_value_mean),
        fmt_opt(row.underlying_dollar_value_stddev),
        fmt_opt(row.underlying_dollar_value_skew),
        fmt_opt(row.underlying_dollar_value_kurtosis),
    ));
    output.push_str(&format!(
        "    iqr={} mad={} cv={} mode={} bc={} dip_p={} kde_peaks={}\n",
        fmt_opt(row.underlying_dollar_value_iqr),
        fmt_opt(row.underlying_dollar_value_mad),
        fmt_opt(row.underlying_dollar_value_cv),
        fmt_opt(row.underlying_dollar_value_mode),
        fmt_opt(row.underlying_dollar_value_bc),
        fmt_opt(row.underlying_dollar_value_dip_pval),
        fmt_opt(row.underlying_dollar_value_kde_peaks),
    ));
    output.push_str(&format!(
        "  puts dADVv total: {} (signed: {})\n",
        fmt_opt(row.puts_dadvv_total),
        fmt_opt(row.puts_signed_dadvv_total)
    ));
    output.push_str(&format!(
        "    min={} max={} mean={} stddev={} skew={} kurt={}\n",
        fmt_opt(row.puts_dadvv_minimum),
        fmt_opt(row.puts_dadvv_maximum),
        fmt_opt(row.puts_dadvv_mean),
        fmt_opt(row.puts_dadvv_stddev),
        fmt_opt(row.puts_dadvv_skew),
        fmt_opt(row.puts_dadvv_kurtosis),
    ));
    output.push_str(&format!(
        "    iqr={} mad={} cv={} mode={} bc={} dip_p={} kde_peaks={}\n",
        fmt_opt(row.puts_dadvv_iqr),
        fmt_opt(row.puts_dadvv_mad),
        fmt_opt(row.puts_dadvv_cv),
        fmt_opt(row.puts_dadvv_mode),
        fmt_opt(row.puts_dadvv_bc),
        fmt_opt(row.puts_dadvv_dip_pval),
        fmt_opt(row.puts_dadvv_kde_peaks),
    ));
    output.push_str(&format!(
        "  puts signed dADVv mean/stddev: {} / {}   skew={} kurt={}\n",
        fmt_opt(row.puts_signed_dadvv_mean),
        fmt_opt(row.puts_signed_dadvv_stddev),
        fmt_opt(row.puts_signed_dadvv_skew),
        fmt_opt(row.puts_signed_dadvv_kurtosis),
    ));
    output.push_str(&format!(
        "  puts GADVv total: {} (signed: {})   min={} max={} mean={} stddev={}\n",
        fmt_opt(row.puts_gadvv_total),
        fmt_opt(row.puts_signed_gadvv_total),
        fmt_opt(row.puts_gadvv_minimum),
        fmt_opt(row.puts_gadvv_maximum),
        fmt_opt(row.puts_gadvv_mean),
        fmt_opt(row.puts_gadvv_stddev),
    ));
    output.push_str(&format!(
        "    skew={} kurt={} iqr={} mad={} cv={} mode={} bc={} dip_p={} kde_peaks={}\n",
        fmt_opt(row.puts_gadvv_skew),
        fmt_opt(row.puts_gadvv_kurtosis),
        fmt_opt(row.puts_gadvv_iqr),
        fmt_opt(row.puts_gadvv_mad),
        fmt_opt(row.puts_gadvv_cv),
        fmt_opt(row.puts_gadvv_mode),
        fmt_opt(row.puts_gadvv_bc),
        fmt_opt(row.puts_gadvv_dip_pval),
        fmt_opt(row.puts_gadvv_kde_peaks),
    ));
    output.push_str(&format!(
        "  puts classifier mix: touch={} at_or_beyond={} tick_rule={} unknown={}\n",
        fmt_pct(row.puts_classifier_touch_pct),
        fmt_pct(row.puts_classifier_at_or_beyond_pct),
        fmt_pct(row.puts_classifier_tick_rule_pct),
        fmt_pct(row.puts_classifier_unknown_pct),
    ));
    output.push_str(&format!(
        "  calls dADVv total: {} (signed: {})\n",
        fmt_opt(row.calls_dadvv_total),
        fmt_opt(row.calls_signed_dadvv_total)
    ));
    output.push_str(&format!(
        "    min={} max={} mean={} stddev={} skew={} kurt={}\n",
        fmt_opt(row.calls_dadvv_minimum),
        fmt_opt(row.calls_dadvv_maximum),
        fmt_opt(row.calls_dadvv_mean),
        fmt_opt(row.calls_dadvv_stddev),
        fmt_opt(row.calls_dadvv_skew),
        fmt_opt(row.calls_dadvv_kurtosis),
    ));
    output.push_str(&format!(
        "    iqr={} mad={} cv={} mode={} bc={} dip_p={} kde_peaks={}\n",
        fmt_opt(row.calls_dadvv_iqr),
        fmt_opt(row.calls_dadvv_mad),
        fmt_opt(row.calls_dadvv_cv),
        fmt_opt(row.calls_dadvv_mode),
        fmt_opt(row.calls_dadvv_bc),
        fmt_opt(row.calls_dadvv_dip_pval),
        fmt_opt(row.calls_dadvv_kde_peaks),
    ));
    output.push_str(&format!(
        "  calls signed dADVv mean/stddev: {} / {}   skew={} kurt={}\n",
        fmt_opt(row.calls_signed_dadvv_mean),
        fmt_opt(row.calls_signed_dadvv_stddev),
        fmt_opt(row.calls_signed_dadvv_skew),
        fmt_opt(row.calls_signed_dadvv_kurtosis),
    ));
    output.push_str(&format!(
        "  calls GADVv quartiles: q1={} q2={} q3={} (min={} max={})\n",
        fmt_opt(row.calls_gadvv_q1),
        fmt_opt(row.calls_gadvv_q2),
        fmt_opt(row.calls_gadvv_q3),
        fmt_opt(row.calls_gadvv_minimum),
        fmt_opt(row.calls_gadvv_maximum),
    ));
    output.push_str(&format!(
        "  calls classifier mix: touch={} at_or_beyond={} tick_rule={} unknown={}\n",
        fmt_pct(row.calls_classifier_touch_pct),
        fmt_pct(row.calls_classifier_at_or_beyond_pct),
        fmt_pct(row.calls_classifier_tick_rule_pct),
        fmt_pct(row.calls_classifier_unknown_pct),
    ));
    output.push_str(&format!(
        "  puts below intrinsic: {}  above intrinsic: {}\n",
        fmt_pct(row.puts_below_intrinsic_pct),
        fmt_pct(row.puts_above_intrinsic_pct),
    ));
    output.push_str(&format!(
        "  calls above intrinsic: {}  aggressor unknown: {}\n",
        fmt_pct(row.calls_above_intrinsic_pct),
        fmt_pct(row.calls_aggressor_unknown_pct),
    ));
    output.push('\n');
    output
}

fn event_timestamp(event: &AggregationInput) -> i64 {
    match event {
        AggregationInput::UnderlyingTrade(trade) => trade.trade_ts_ns,
        AggregationInput::OptionTrade { trade, .. } => trade.trade_ts_ns,
    }
}

fn format_ts(ts_ns: i64) -> String {
    DateTime::<Utc>::from_timestamp(ts_ns / 1_000_000_000, (ts_ns % 1_000_000_000) as u32)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|| "unavailable".to_string())
}

fn file_checksum(path: &Path) -> Result<u32, AggregationError> {
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

fn fmt_opt(value: Option<f64>) -> String {
    match value {
        Some(v) if v.is_finite() => format!("{:.4}", v),
        Some(_) => "n/a".to_string(),
        None => "n/a".to_string(),
    }
}

fn fmt_pct(value: Option<f64>) -> String {
    match value {
        Some(v) if v.is_finite() => format!("{:.1}%", v),
        _ => "n/a".to_string(),
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
        aggressor_side: AggressorSide::Unknown,
        class_method: ClassMethod::Unknown,
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
        aggressor_side: AggressorSide::Unknown,
        class_method: ClassMethod::Unknown,
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

fn read_option_trade_records(path: &Path) -> Result<Vec<OptionTradeRecord>, AggregationError> {
    read_record_batch(path, parse_option_batch)
}

fn read_underlying_trade_records(
    path: &Path,
) -> Result<Vec<UnderlyingTradeRecord>, AggregationError> {
    read_record_batch(path, parse_underlying_batch)
}

fn read_record_batch<T, F>(path: &Path, mut parser: F) -> Result<Vec<T>, AggregationError>
where
    F: FnMut(&RecordBatch) -> Result<Vec<T>, AggregationError>,
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

fn parse_option_batch(batch: &RecordBatch) -> Result<Vec<OptionTradeRecord>, AggregationError> {
    let contract = string_col(batch, 0)?;
    let trade_uid = fixed_binary_col(batch, 1)?;
    let direction = string_col(batch, 2)?;
    let strike = float_col(batch, 3)?;
    let underlying = string_col(batch, 4)?;
    let trade_ts = int64_col(batch, 5)?;
    let participant_ts = int64_col(batch, 6)?;
    let price = float_col(batch, 7)?;
    let size = uint32_col(batch, 8)?;
    let conditions = list_i32_col(batch, 9)?;
    let exchange = int32_col(batch, 10)?;
    let expiry = int64_col(batch, 11)?;
    let source = string_col(batch, 12)?;
    let quality = string_col(batch, 13)?;
    let watermark = int64_col(batch, 14)?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for idx in 0..batch.num_rows() {
        let mut uid = [0u8; 16];
        uid.copy_from_slice(trade_uid.value(idx));
        let record = OptionTradeRecord {
            contract: contract.value(idx).to_string(),
            trade_uid: uid,
            contract_direction: direction.value(idx).chars().next().unwrap_or('C'),
            strike_price: strike.value(idx),
            underlying: underlying.value(idx).to_string(),
            trade_ts_ns: trade_ts.value(idx),
            participant_ts_ns: if participant_ts.is_null(idx) {
                None
            } else {
                Some(participant_ts.value(idx))
            },
            price: price.value(idx),
            size: size.value(idx),
            conditions: conditions[idx].clone(),
            exchange: exchange.value(idx),
            expiry_ts_ns: expiry.value(idx),
            source: parse_source(source.value(idx)),
            quality: parse_quality(quality.value(idx)),
            watermark_ts_ns: watermark.value(idx),
        };
        rows.push(record);
    }
    Ok(rows)
}

fn parse_underlying_batch(
    batch: &RecordBatch,
) -> Result<Vec<UnderlyingTradeRecord>, AggregationError> {
    let symbol = string_col(batch, 0)?;
    let trade_uid = fixed_binary_col(batch, 1)?;
    let trade_ts = int64_col(batch, 2)?;
    let participant_ts = int64_col(batch, 3)?;
    let price = float_col(batch, 4)?;
    let size = uint32_col(batch, 5)?;
    let conditions = list_i32_col(batch, 6)?;
    let exchange = int32_col(batch, 7)?;
    let trade_id = string_col(batch, 8)?;
    let seq = uint64_col(batch, 9)?;
    let tape = string_col(batch, 10)?;
    let correction = int32_col(batch, 11)?;
    let trf_id = string_col(batch, 12)?;
    let trf_ts = int64_col(batch, 13)?;
    let source = string_col(batch, 14)?;
    let quality = string_col(batch, 15)?;
    let watermark = int64_col(batch, 16)?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for idx in 0..batch.num_rows() {
        let mut uid = [0u8; 16];
        uid.copy_from_slice(trade_uid.value(idx));
        let record = UnderlyingTradeRecord {
            symbol: symbol.value(idx).to_string(),
            trade_uid: uid,
            trade_ts_ns: trade_ts.value(idx),
            participant_ts_ns: if participant_ts.is_null(idx) {
                None
            } else {
                Some(participant_ts.value(idx))
            },
            price: price.value(idx),
            size: size.value(idx),
            conditions: conditions[idx].clone(),
            exchange: exchange.value(idx),
            trade_id: if trade_id.is_null(idx) {
                None
            } else {
                Some(trade_id.value(idx).to_string())
            },
            seq: if seq.is_null(idx) {
                None
            } else {
                Some(seq.value(idx))
            },
            tape: if tape.is_null(idx) {
                None
            } else {
                Some(tape.value(idx).to_string())
            },
            correction: if correction.is_null(idx) {
                None
            } else {
                Some(correction.value(idx))
            },
            trf_id: if trf_id.is_null(idx) {
                None
            } else {
                Some(trf_id.value(idx).to_string())
            },
            trf_ts_ns: if trf_ts.is_null(idx) {
                None
            } else {
                Some(trf_ts.value(idx))
            },
            source: parse_source(source.value(idx)),
            quality: parse_quality(quality.value(idx)),
            watermark_ts_ns: watermark.value(idx),
        };
        rows.push(record);
    }
    Ok(rows)
}

fn string_col<'a>(batch: &'a RecordBatch, idx: usize) -> Result<&'a StringArray, AggregationError> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| AggregationError::Schema(format!("column {idx} is not Utf8")))
}

fn fixed_binary_col<'a>(
    batch: &'a RecordBatch,
    idx: usize,
) -> Result<&'a FixedSizeBinaryArray, AggregationError> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| AggregationError::Schema(format!("column {idx} is not FixedSizeBinary")))
}

fn float_col<'a>(batch: &'a RecordBatch, idx: usize) -> Result<&'a Float64Array, AggregationError> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| AggregationError::Schema(format!("column {idx} is not Float64")))
}

fn int32_col<'a>(batch: &'a RecordBatch, idx: usize) -> Result<&'a Int32Array, AggregationError> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| AggregationError::Schema(format!("column {idx} is not Int32")))
}

fn int64_col<'a>(batch: &'a RecordBatch, idx: usize) -> Result<&'a Int64Array, AggregationError> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| AggregationError::Schema(format!("column {idx} is not Int64")))
}

fn uint32_col<'a>(batch: &'a RecordBatch, idx: usize) -> Result<&'a UInt32Array, AggregationError> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| AggregationError::Schema(format!("column {idx} is not UInt32")))
}

fn uint64_col<'a>(batch: &'a RecordBatch, idx: usize) -> Result<&'a UInt64Array, AggregationError> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| AggregationError::Schema(format!("column {idx} is not UInt64")))
}

fn list_i32_col(batch: &RecordBatch, idx: usize) -> Result<Vec<Vec<i32>>, AggregationError> {
    let list = batch
        .column(idx)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| AggregationError::Schema(format!("column {idx} is not List<Int32>")))?;
    let values = list.values();
    let values = values
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| AggregationError::Schema("list values are not Int32".to_string()))?;
    let offsets = list.value_offsets();
    let mut rows = Vec::with_capacity(list.len());
    for i in 0..list.len() {
        if list.is_null(i) {
            rows.push(Vec::new());
            continue;
        }
        let start = offsets[i as usize] as usize;
        let end = offsets[i as usize + 1] as usize;
        let mut row = Vec::with_capacity(end - start);
        for idx in start..end {
            row.push(values.value(idx));
        }
        rows.push(row);
    }
    Ok(rows)
}

fn parse_source(value: &str) -> Source {
    match value {
        "Flatfile" => Source::Flatfile,
        "Rest" => Source::Rest,
        _ => Source::Ws,
    }
}

fn parse_quality(value: &str) -> Quality {
    match value {
        "Final" => Quality::Final,
        "Enriched" => Quality::Enriched,
        _ => Quality::Prelim,
    }
}

#[derive(Debug, Error)]
pub enum AggregationError {
    #[error(transparent)]
    Engine(#[from] aggregations::AggregationError),
    #[error(transparent)]
    Controller(#[from] WindowSpaceError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("missing payload {payload_id}")]
    MissingPayload { payload_id: u32 },
    #[error("missing window index for start {start_ns}")]
    MissingWindowIndex { start_ns: i64 },
    #[error("{0}")]
    Schema(String),
}
