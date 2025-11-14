mod artifacts;
mod config;
mod download_metrics;
mod errors;
mod window_bucket;

pub use config::FlatfileRuntimeConfig;
pub use download_metrics::{DownloadMetrics, DownloadSnapshot};

use std::{
    cmp::Ordering,
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering as AtomicOrdering},
    },
    time::{Duration, Instant},
};

use arrow::{
    array::{
        ArrayRef, FixedSizeBinaryArray, Float64Array, Int32Array, Int32Builder, Int64Array,
        ListBuilder, StringArray, UInt32Array, UInt64Array,
    },
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use artifacts::{artifact_path, cleanup_partial_artifacts, sanitized_symbol, write_record_batch};
use async_compression::tokio::bufread::GzipDecoder;
use aws_sdk_s3::{
    Client,
    config::{BehaviorVersion, Credentials, Region},
    primitives::ByteStream,
};
use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use classifier::Classifier;
use core_types::{
    opra::parse_opra_contract,
    raw::{AggressorRecord, OptionTradeRecord, QuoteRecord, UnderlyingTradeRecord},
    schema::{
        aggressor_record_schema, nbbo_schema, option_trade_record_schema,
        underlying_trade_record_schema,
    },
    types::{ClassParams, Nbbo, NbboState, OptionTrade, Quality, Source},
    uid::{equity_trade_uid, option_trade_uid, quote_uid},
};
use csv_async::{AsyncReader, AsyncReaderBuilder, ErrorKind as CsvErrorKind, StringRecord, Trim};
use engine_api::{
    Engine, EngineError, EngineHealth, EngineResult, HealthStatus, PriorityHookDescription,
};
use errors::FlatfileError;
use futures::StreamExt;
use log::{error, info, warn};
use nbbo_cache::NbboStore;
use parking_lot::Mutex;
use tempfile::{NamedTempFile, TempPath};
use tokio::{
    fs::File as TokioFile,
    io::{AsyncRead, AsyncWriteExt, BufReader, BufWriter},
    runtime::Runtime,
    task::JoinHandle,
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use trading_calendar::{Market, TradingCalendar};
use window_bucket::{HasTimestamp, WindowBucket};
use window_space::error::WindowSpaceError;
use window_space::ledger::{EnrichmentWindowRow, TradeWindowRow};
use window_space::payload::EnrichmentSlotKind;
use window_space::{
    WindowIndex, WindowSpaceController,
    mapping::{AggressorPayload, QuoteBatchPayload, TradeBatchPayload},
    payload::{PayloadMeta, PayloadType, SlotKind, SlotStatus, TradeSlotKind},
    window::WindowSpace,
};

const OPTION_TRADE_SCHEMA_VERSION: u8 = 1;
const QUOTE_SCHEMA_VERSION: u8 = 1;
const UNDERLYING_TRADE_SCHEMA_VERSION: u8 = 1;
const AGGRESSOR_PAYLOAD_SCHEMA_VERSION: u8 = 1;
const DEFAULT_CLASSIFIER_EPSILON: f64 = 1e-4;
const DEFAULT_CLASSIFIER_LATENESS_MS: u32 = 1_000;

pub struct OptionQuoteFlatfileEngine {
    inner: Arc<OptionQuoteInner>,
}

pub struct UnderlyingTradeFlatfileEngine {
    inner: Arc<UnderlyingTradeInner>,
}

pub struct UnderlyingQuoteFlatfileEngine {
    inner: Arc<UnderlyingQuoteInner>,
}

struct OptionQuoteInner {
    config: FlatfileRuntimeConfig,
    ledger: Arc<WindowSpaceController>,
    client: Client,
    state: Mutex<EngineRuntimeState>,
    batch_seq: AtomicU32,
    calendar: TradingCalendar,
    day_index: Arc<DayWindowIndex>,
    classifier: Classifier,
    class_params: ClassParams,
}

struct UnderlyingTradeInner {
    config: FlatfileRuntimeConfig,
    ledger: Arc<WindowSpaceController>,
    client: Client,
    state: Mutex<EngineRuntimeState>,
    batch_seq: AtomicU32,
    calendar: TradingCalendar,
    day_index: Arc<DayWindowIndex>,
}

struct UnderlyingQuoteInner {
    config: FlatfileRuntimeConfig,
    ledger: Arc<WindowSpaceController>,
    client: Client,
    state: Mutex<EngineRuntimeState>,
    batch_seq: AtomicU32,
    calendar: TradingCalendar,
    day_index: Arc<DayWindowIndex>,
}

impl OptionQuoteInner {
    fn new(config: FlatfileRuntimeConfig, ledger: Arc<WindowSpaceController>) -> Self {
        let window_space = ledger.window_space();
        let day_index = Arc::new(DayWindowIndex::new(window_space.as_ref()));
        Self {
            client: make_s3_client(&config),
            ledger,
            state: Mutex::new(EngineRuntimeState::Stopped),
            batch_seq: AtomicU32::new(0),
            calendar: TradingCalendar::new(Market::NYSE).expect("init calendar"),
            config,
            day_index,
            classifier: Classifier::new(),
            class_params: default_class_params(),
        }
    }

    async fn run_quotes(self: Arc<Self>, cancel: CancellationToken) {
        let mut dates = planned_dates(&self.config, &self.calendar);
        dates.sort();
        dates.dedup();
        loop {
            if cancel.is_cancelled() {
                break;
            }
            let now = Utc::now();
            let mut made_progress = false;
            let mut has_future_work = false;
            let mut next_ready_at: Option<DateTime<Utc>> = None;
            for &date in &dates {
                if cancel.is_cancelled() {
                    break;
                }
                if let Some(ready_at) = ready_instant(&self.config, &self.calendar, date) {
                    if ready_at > now {
                        has_future_work = true;
                        update_next_ready(&mut next_ready_at, ready_at);
                        continue;
                    }
                }
                if !self.day_requires_option_ingest(date) {
                    continue;
                }
                has_future_work = true;
                if let Err(err) = self.process_option_quotes_day(date, cancel.clone()).await {
                    error!("option quote day {} failed: {err}", date);
                }
                made_progress = true;
            }
            if !has_future_work {
                break;
            }
            if !made_progress {
                let cancelled = if let Some(ready_at) = next_ready_at {
                    wait_until_ready(ready_at, cancel.clone()).await
                } else {
                    sleep_with_cancel(Duration::from_secs(60), cancel.clone()).await
                };
                if cancelled {
                    break;
                }
            }
        }
        info!(
            "[{}] option quote flatfile engine exiting",
            self.config.label
        );
    }

    fn day_requires_option_ingest(&self, date: NaiveDate) -> bool {
        let ledger = self.ledger.as_ref();
        let day_index = self.day_index.as_ref();
        day_requires_ingest(ledger, day_index, date, TradeSlotKind::OptionTrade)
            || day_requires_ingest(ledger, day_index, date, TradeSlotKind::OptionAggressor)
            || day_requires_ingest(ledger, day_index, date, TradeSlotKind::OptionQuote)
    }

    async fn process_option_quotes_day(
        &self,
        date: NaiveDate,
        cancel: CancellationToken,
    ) -> Result<(), FlatfileError> {
        let day_range = self.prime_day_slots(date)?;
        let quote_key = format!(
            "us_options_opra/quotes_v1/{}/{:02}/{}-{:02}-{:02}.csv.gz",
            date.year(),
            date.month(),
            date.year(),
            date.month(),
            date.day()
        );
        let trade_key = format!(
            "us_options_opra/trades_v1/{}/{:02}/{}-{:02}-{:02}.csv.gz",
            date.year(),
            date.month(),
            date.year(),
            date.month(),
            date.day()
        );
        info!(
            "[{}] ingesting option quotes from {} and trades from {}",
            self.config.label, quote_key, trade_key
        );
        let (quote_stream, quote_token) = fetch_stream(
            &self.client,
            &self.config,
            &quote_key,
            TradeSlotKind::OptionQuote,
        )
        .await?;
        let (trade_stream, trade_token) = fetch_stream(
            &self.client,
            &self.config,
            &trade_key,
            TradeSlotKind::OptionTrade,
        )
        .await?;
        let mut nbbo_store = NbboStore::new();
        let result = self
            .consume_option_quotes(
                date,
                quote_stream,
                trade_stream,
                cancel.clone(),
                &mut nbbo_store,
            )
            .await;
        drop(quote_token);
        drop(trade_token);
        result?;
        self.retire_quote_slots(day_range)?;
        if cancel.is_cancelled() {
            info!(
                "[{}] option quote ingestion cancelled after quotes for {}",
                self.config.label, date
            );
            return Ok(());
        }
        self.retire_empty_option_slots(day_range)?;
        Ok(())
    }

    async fn consume_option_quotes<RQ, RT>(
        &self,
        date: NaiveDate,
        quote_reader: RQ,
        trade_reader: RT,
        cancel: CancellationToken,
        nbbo_store: &mut NbboStore,
    ) -> Result<(), FlatfileError>
    where
        RQ: AsyncRead + Unpin + Send,
        RT: AsyncRead + Unpin + Send,
    {
        let mut quote_csv = AsyncReaderBuilder::new()
            .trim(Trim::All)
            .create_reader(quote_reader);
        let mut quotes = quote_csv.records();
        let mut trade_cursor = TradeCursor::new(trade_reader);
        let mut trade_tracker = SymbolTracker::new();
        let mut trade_overlays: HashMap<WindowKey, Vec<AggressorRecord>> = HashMap::new();
        let mut last_contract: Option<String> = None;
        let mut processed_rows = 0usize;
        let mut last_progress = Instant::now();
        let interval = Duration::from_millis(self.config.progress_update_ms.max(10));

        while let Some(record) = quotes.next().await {
            if cancel.is_cancelled() {
                info!(
                    "[{}] option quote ingestion cancelled while streaming {}",
                    self.config.label, date
                );
                return Ok(());
            }
            let record = match record {
                Ok(record) => record,
                Err(err) => {
                    log_streaming_error(self.config.label, "option quotes", date, &err);
                    return Err(err.into());
                }
            };
            if record.len() < 12 {
                continue;
            }
            let Some((contract, quote, underlying)) = parse_quote_row(&record, QuoteSource::Option)
            else {
                continue;
            };
            let Some(base) = underlying else {
                continue;
            };
            if !self.config.allows_underlying(&base) {
                continue;
            }
            if last_contract.as_deref() != Some(contract.as_str()) {
                if let Some(prev) = last_contract.take() {
                    self.drain_trades_for_contract(
                        date,
                        &prev,
                        i64::MAX,
                        &mut trade_cursor,
                        &mut trade_tracker,
                        &mut trade_overlays,
                        nbbo_store,
                    )
                    .await?;
                }
                last_contract = Some(contract.clone());
            }
            nbbo_store.put(&quote_to_nbbo(&quote));
            self.drain_trades_for_contract(
                date,
                &contract,
                quote.quote_ts_ns,
                &mut trade_cursor,
                &mut trade_tracker,
                &mut trade_overlays,
                nbbo_store,
            )
            .await?;
            processed_rows += 1;
            if self.config.progress_logging && last_progress.elapsed() >= interval {
                info!(
                    "[{}] streamed {} option quotes for {}",
                    self.config.label, processed_rows, date
                );
                last_progress = Instant::now();
            }
        }

        if let Some(prev) = last_contract.take() {
            self.drain_trades_for_contract(
                date,
                &prev,
                i64::MAX,
                &mut trade_cursor,
                &mut trade_tracker,
                &mut trade_overlays,
                nbbo_store,
            )
            .await?;
        }

        while trade_cursor.has_pending().await? {
            if let Some(next_contract) = trade_cursor.peek_contract().await? {
                self.drain_trades_for_contract(
                    date,
                    &next_contract,
                    i64::MAX,
                    &mut trade_cursor,
                    &mut trade_tracker,
                    &mut trade_overlays,
                    nbbo_store,
                )
                .await?;
            } else {
                break;
            }
        }

        let mut overlays_map = trade_overlays;
        let overlays_ref = &mut overlays_map;
        trade_tracker.finish(|state| {
            let key = WindowKey {
                symbol: state.symbol.clone(),
                window_idx: state.window_idx,
            };
            let overlays = overlays_ref.remove(&key).unwrap_or_default();
            self.finalize_option_trade_state(date, state, overlays)
        })?;

        Ok(())
    }

    async fn drain_trades_for_contract<TR>(
        &self,
        date: NaiveDate,
        contract: &str,
        max_ts: i64,
        trade_cursor: &mut TradeCursor<TR>,
        trade_tracker: &mut SymbolTracker<OptionTradeRecord>,
        trade_overlays: &mut HashMap<WindowKey, Vec<AggressorRecord>>,
        nbbo_store: &NbboStore,
    ) -> Result<(), FlatfileError>
    where
        TR: AsyncRead + Unpin + Send,
    {
        loop {
            trade_cursor.ensure_buffered().await?;
            let Some(ordering) = trade_cursor.compare_contract(contract) else {
                break;
            };
            match ordering {
                Ordering::Greater => break,
                Ordering::Less => {
                    let trade = trade_cursor.consume().await?;
                    self.handle_trade(date, trade, trade_tracker, trade_overlays, nbbo_store)?;
                }
                Ordering::Equal => {
                    if trade_cursor.buffered_ts().unwrap_or(i64::MAX) <= max_ts {
                        let trade = trade_cursor.consume().await?;
                        self.handle_trade(date, trade, trade_tracker, trade_overlays, nbbo_store)?;
                    } else {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    fn handle_trade(
        &self,
        date: NaiveDate,
        trade: OptionTradeRecord,
        trade_tracker: &mut SymbolTracker<OptionTradeRecord>,
        trade_overlays: &mut HashMap<WindowKey, Vec<AggressorRecord>>,
        nbbo_store: &NbboStore,
    ) -> Result<(), FlatfileError> {
        let trade_record = trade;
        let symbol = trade_record.underlying.clone();
        if symbol.is_empty() || !self.config.allows_underlying(&symbol) {
            return Ok(());
        }
        let ts = trade_record.timestamp_ns();
        let Some(window_idx) = self.window_idx_for_timestamp(ts) else {
            warn!("option trade timestamp {} fell outside window", ts);
            return Ok(());
        };
        let window_ts = window_start_ns(&self.ledger, window_idx)?;
        let overlay = self.classify_option_trade(&trade_record, nbbo_store, window_ts);
        let key = WindowKey {
            symbol: symbol.clone(),
            window_idx,
        };
        let mut wrote_trade = false;
        trade_tracker.with_bucket(
            symbol.as_str(),
            window_idx,
            |sym, idx| self.open_option_trade_window(date, sym, idx),
            |state| {
                let key = WindowKey {
                    symbol: state.symbol.clone(),
                    window_idx: state.window_idx,
                };
                let overlays = trade_overlays.remove(&key).unwrap_or_default();
                self.finalize_option_trade_state(date, state, overlays)
            },
            |bucket_opt| {
                if let Some(bucket) = bucket_opt {
                    bucket.observe(trade_record.clone());
                    wrote_trade = true;
                }
                Ok(())
            },
        )?;
        if wrote_trade {
            trade_overlays.entry(key).or_default().push(overlay);
        }
        Ok(())
    }

    fn persist_option_trade_bucket(
        &self,
        date: NaiveDate,
        key: WindowKey,
        bucket: WindowBucket<OptionTradeRecord>,
    ) -> Result<Option<u32>, FlatfileError> {
        let row = self.ledger.get_trade_row(&key.symbol, key.window_idx)?;
        if row.option_trade_ref.status == SlotStatus::Filled {
            return if row.option_trade_ref.payload_id == 0 {
                Ok(None)
            } else {
                Ok(Some(row.option_trade_ref.payload_id))
            };
        }
        self.ledger.mark_pending(
            &key.symbol,
            key.window_idx,
            SlotKind::Trade(TradeSlotKind::OptionTrade),
        )?;
        let batch = option_trade_batch(&bucket.records)?;
        let relative_path = artifact_path(
            &self.config,
            "options/trades",
            date,
            &key.symbol,
            key.window_idx,
            "parquet",
        );
        let artifact = write_record_batch(&self.config, &relative_path, &batch)?;
        let payload = TradeBatchPayload {
            schema_version: OPTION_TRADE_SCHEMA_VERSION,
            window_ts: window_start_ns(&self.ledger, key.window_idx)?,
            batch_id: self.batch_seq.fetch_add(1, AtomicOrdering::Relaxed) + 1,
            first_trade_ts: bucket.first_ts,
            last_trade_ts: bucket.last_ts,
            record_count: bucket.records.len() as u32,
            artifact_uri: artifact.relative_path.clone(),
            checksum: artifact.checksum,
        };
        let payload_id = {
            let mut stores = self.ledger.payload_stores();
            stores.trades.append(payload)?
        };
        let meta = PayloadMeta::new(PayloadType::Trade, payload_id, 1, artifact.checksum);
        if let Err(err) = self
            .ledger
            .set_option_trade_ref(&key.symbol, key.window_idx, meta, None)
        {
            self.ledger.clear_slot(
                &key.symbol,
                key.window_idx,
                SlotKind::Trade(TradeSlotKind::OptionTrade),
            )?;
            return Err(err.into());
        }
        Ok(Some(payload_id))
    }

    fn finalize_option_trade_state(
        &self,
        date: NaiveDate,
        state: SymbolWindow<OptionTradeRecord>,
        overlays: Vec<AggressorRecord>,
    ) -> Result<(), FlatfileError> {
        if let Some(bucket) = state.bucket {
            let key = WindowKey {
                symbol: state.symbol,
                window_idx: state.window_idx,
            };
            let trade_payload_id = self.persist_option_trade_bucket(date, key.clone(), bucket)?;
            self.persist_option_aggressor_records(date, key, overlays, trade_payload_id)?;
        }
        Ok(())
    }

    fn persist_option_aggressor_records(
        &self,
        _date: NaiveDate,
        key: WindowKey,
        overlays: Vec<AggressorRecord>,
        trade_payload_id: Option<u32>,
    ) -> Result<(), FlatfileError> {
        if overlays.is_empty() {
            return Ok(());
        }
        let window_ts = overlays
            .first()
            .map(|record| record.window_start_ts_ns)
            .unwrap_or_else(|| window_start_ns(&self.ledger, key.window_idx).unwrap_or(0));
        let trade_payload_id = if let Some(id) = trade_payload_id {
            id
        } else {
            let row = self.ledger.get_trade_row(&key.symbol, key.window_idx)?;
            if row.option_trade_ref.payload_id == 0 {
                warn!(
                    "[{}] missing trade payload while writing option aggressor for {}:{}",
                    self.config.label, key.symbol, key.window_idx
                );
                return Ok(());
            }
            row.option_trade_ref.payload_id
        };
        let batch = aggressor_batch(&overlays)?;
        let relative_path = self.aggressor_artifact_path(&key.symbol, key.window_idx, window_ts)?;
        let artifact = write_record_batch(&self.config, &relative_path, &batch)?;
        let payload = AggressorPayload {
            schema_version: AGGRESSOR_PAYLOAD_SCHEMA_VERSION,
            window_ts,
            trade_payload_id,
            quote_payload_id: 0,
            observation_count: overlays.len() as u32,
            artifact_uri: artifact.relative_path.clone(),
            checksum: artifact.checksum,
        };
        let payload_id = {
            let mut stores = self.ledger.payload_stores();
            stores.aggressor.append(payload)?
        };
        let meta = PayloadMeta::new(PayloadType::Aggressor, payload_id, 1, artifact.checksum);
        if let Err(err) =
            self.ledger
                .set_option_aggressor_ref(&key.symbol, key.window_idx, meta, None)
        {
            self.ledger.clear_slot(
                &key.symbol,
                key.window_idx,
                SlotKind::Trade(TradeSlotKind::OptionAggressor),
            )?;
            return Err(err.into());
        }
        Ok(())
    }

    fn aggressor_artifact_path(
        &self,
        symbol: &str,
        window_idx: WindowIndex,
        window_start_ts_ns: i64,
    ) -> Result<String, FlatfileError> {
        let naive = DateTime::<Utc>::from_timestamp(window_start_ts_ns / 1_000_000_000, 0)
            .ok_or(FlatfileError::MissingWindowMeta { window_idx })?
            .date_naive();
        Ok(format!(
            "aggressor/artifacts/options/{}/{:04}/{:02}/{:02}/{:06}/{}.parquet",
            self.config.label,
            naive.year(),
            naive.month(),
            naive.day(),
            window_idx,
            sanitized_symbol(symbol)
        ))
    }

    fn open_option_trade_window(
        &self,
        date: NaiveDate,
        underlying: &str,
        window_idx: WindowIndex,
    ) -> Result<SymbolWindow<OptionTradeRecord>, FlatfileError> {
        let key = WindowKey {
            symbol: underlying.to_string(),
            window_idx,
        };
        let status = ensure_slot_pending(
            &self.ledger,
            &key,
            SlotKind::Trade(TradeSlotKind::OptionTrade),
        )?;
        if status == SlotStatus::Filled {
            return Ok(SymbolWindow::skipped(underlying, window_idx));
        }
        if status == SlotStatus::Pending {
            let relative_path = artifact_path(
                &self.config,
                "options/trades",
                date,
                underlying,
                window_idx,
                "parquet",
            );
            cleanup_partial_artifacts(&self.config, &relative_path)?;
        }
        Ok(SymbolWindow::writable(underlying, window_idx))
    }

    fn classify_option_trade(
        &self,
        record: &OptionTradeRecord,
        nbbo_store: &NbboStore,
        window_start_ts_ns: i64,
    ) -> AggressorRecord {
        let mut trade = option_trade_from_record(record);
        self.classifier
            .classify_trade(&mut trade, nbbo_store, &self.class_params);
        AggressorRecord {
            instrument_id: trade.contract.clone(),
            underlying_symbol: if trade.underlying.is_empty() {
                None
            } else {
                Some(trade.underlying.clone())
            },
            trade_uid: trade.trade_uid,
            trade_ts_ns: trade.trade_ts_ns,
            window_start_ts_ns,
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
        }
    }

    fn prime_day_slots(
        &self,
        date: NaiveDate,
    ) -> Result<Option<(WindowIndex, WindowIndex)>, FlatfileError> {
        let Some((mut start_idx, mut end_idx)) = self.day_index.range_for(&date) else {
            return Ok(None);
        };
        if start_idx > end_idx {
            std::mem::swap(&mut start_idx, &mut end_idx);
        }
        for (_, symbol) in self.ledger.symbols() {
            if !self.config.allows_symbol(&symbol) {
                continue;
            }
            for window_idx in start_idx..=end_idx {
                let key = WindowKey {
                    symbol: symbol.clone(),
                    window_idx,
                };
                for slot in [
                    SlotKind::Trade(TradeSlotKind::OptionTrade),
                    SlotKind::Trade(TradeSlotKind::OptionAggressor),
                    SlotKind::Trade(TradeSlotKind::OptionQuote),
                ] {
                    ensure_slot_pending(&self.ledger, &key, slot)?;
                }
            }
        }
        Ok(Some((start_idx, end_idx)))
    }

    fn retire_quote_slots(
        &self,
        range: Option<(WindowIndex, WindowIndex)>,
    ) -> Result<(), FlatfileError> {
        let Some((mut start_idx, mut end_idx)) = range else {
            return Ok(());
        };
        if start_idx > end_idx {
            std::mem::swap(&mut start_idx, &mut end_idx);
        }
        for (_, symbol) in self.ledger.symbols() {
            if !self.config.allows_symbol(&symbol) {
                continue;
            }
            for window_idx in start_idx..=end_idx {
                self.ledger
                    .retire_slot(
                        &symbol,
                        window_idx,
                        SlotKind::Trade(TradeSlotKind::OptionQuote),
                    )
                    .map_err(FlatfileError::from)?;
            }
        }
        Ok(())
    }

    fn retire_empty_option_slots(
        &self,
        range: Option<(WindowIndex, WindowIndex)>,
    ) -> Result<(), FlatfileError> {
        let Some((mut start_idx, mut end_idx)) = range else {
            return Ok(());
        };
        if start_idx > end_idx {
            std::mem::swap(&mut start_idx, &mut end_idx);
        }
        for (_, symbol) in self.ledger.symbols() {
            if !self.config.allows_symbol(&symbol) {
                continue;
            }
            for window_idx in start_idx..=end_idx {
                let row = self.ledger.get_trade_row(&symbol, window_idx)?;
                if matches!(row.option_trade_ref.status, SlotStatus::Pending) {
                    self.ledger
                        .retire_slot(
                            &symbol,
                            window_idx,
                            SlotKind::Trade(TradeSlotKind::OptionTrade),
                        )
                        .map_err(FlatfileError::from)?;
                }
                if matches!(row.option_aggressor_ref.status, SlotStatus::Pending) {
                    self.ledger
                        .retire_slot(
                            &symbol,
                            window_idx,
                            SlotKind::Trade(TradeSlotKind::OptionAggressor),
                        )
                        .map_err(FlatfileError::from)?;
                }
            }
        }
        Ok(())
    }

    fn window_idx_for_timestamp(&self, ts_ns: i64) -> Option<WindowIndex> {
        self.ledger.window_idx_for_timestamp(ts_ns / 1_000_000_000)
    }

    fn stop(&self, label: &str) -> EngineResult<()> {
        stop_runtime(label, self.config.label, &self.state)
    }

    fn health(&self) -> EngineHealth {
        runtime_health(&self.state)
    }
}

impl UnderlyingTradeInner {
    fn new(config: FlatfileRuntimeConfig, ledger: Arc<WindowSpaceController>) -> Self {
        let window_space = ledger.window_space();
        let day_index = Arc::new(DayWindowIndex::new(window_space.as_ref()));
        Self {
            client: make_s3_client(&config),
            ledger,
            state: Mutex::new(EngineRuntimeState::Stopped),
            batch_seq: AtomicU32::new(0),
            calendar: TradingCalendar::new(Market::NYSE).expect("init calendar"),
            config,
            day_index,
        }
    }

    async fn run_trades(self: Arc<Self>, cancel: CancellationToken) {
        let mut dates = planned_dates(&self.config, &self.calendar);
        dates.sort();
        dates.dedup();
        loop {
            if cancel.is_cancelled() {
                break;
            }
            let now = Utc::now();
            let mut made_progress = false;
            let mut has_future_work = false;
            let mut next_ready_at: Option<DateTime<Utc>> = None;
            for &date in &dates {
                if cancel.is_cancelled() {
                    break;
                }
                if let Some(ready_at) = ready_instant(&self.config, &self.calendar, date) {
                    if ready_at > now {
                        has_future_work = true;
                        update_next_ready(&mut next_ready_at, ready_at);
                        continue;
                    }
                }
                if !day_requires_ingest(
                    self.ledger.as_ref(),
                    self.day_index.as_ref(),
                    date,
                    TradeSlotKind::UnderlyingTrade,
                ) {
                    continue;
                }
                has_future_work = true;
                if let Err(err) = self.process_equity_trades_day(date, cancel.clone()).await {
                    error!("underlying trades day {} failed: {err}", date);
                }
                made_progress = true;
            }
            if !has_future_work {
                break;
            }
            if !made_progress {
                let cancelled = if let Some(ready_at) = next_ready_at {
                    wait_until_ready(ready_at, cancel.clone()).await
                } else {
                    sleep_with_cancel(Duration::from_secs(60), cancel.clone()).await
                };
                if cancelled {
                    break;
                }
            }
        }
        info!(
            "[{}] underlying trade flatfile engine exiting",
            self.config.label
        );
    }

    async fn process_equity_trades_day(
        &self,
        date: NaiveDate,
        cancel: CancellationToken,
    ) -> Result<(), FlatfileError> {
        let key = format!(
            "us_stocks_sip/trades_v1/{}/{:02}/{}-{:02}-{:02}.csv.gz",
            date.year(),
            date.month(),
            date.year(),
            date.month(),
            date.day()
        );
        info!(
            "[{}] ingesting underlying trades from {}",
            self.config.label, key
        );
        let (stream, token) = fetch_stream(
            &self.client,
            &self.config,
            &key,
            TradeSlotKind::UnderlyingTrade,
        )
        .await?;
        let result = self.consume_underlying_trades(date, stream, cancel).await;
        drop(token);
        result
    }

    async fn consume_underlying_trades<R>(
        &self,
        date: NaiveDate,
        reader: R,
        cancel: CancellationToken,
    ) -> Result<(), FlatfileError>
    where
        R: AsyncRead + Unpin + Send,
    {
        let mut csv_reader = AsyncReaderBuilder::new()
            .trim(Trim::All)
            .create_reader(reader);
        let mut records = csv_reader.records();
        let mut tracker = SymbolTracker::new();
        let mut processed_rows = 0usize;
        let mut last_progress = Instant::now();
        let interval = Duration::from_millis(self.config.progress_update_ms.max(10));
        while let Some(record) = records.next().await {
            if cancel.is_cancelled() {
                info!(
                    "[{}] underlying trade ingestion cancelled while streaming {}",
                    self.config.label, date
                );
                return Ok(());
            }
            let record = match record {
                Ok(record) => record,
                Err(err) => {
                    log_streaming_error(self.config.label, "underlying trades", date, &err);
                    return Err(err.into());
                }
            };
            if record.len() < 12 {
                continue;
            }
            if let Some((symbol, trade)) = parse_underlying_trade_row(&record) {
                let ts = trade.timestamp_ns();
                let window_idx = match self.window_idx_for_timestamp(ts) {
                    Some(idx) => idx,
                    None => continue,
                };
                tracker.with_bucket(
                    &symbol,
                    window_idx,
                    |sym, idx| self.open_underlying_trade_window(date, sym, idx),
                    |state| self.finalize_underlying_trade_state(date, state),
                    |bucket_opt| {
                        if let Some(bucket) = bucket_opt {
                            if let Some(last_ts) = bucket.last_timestamp() {
                                if ts < last_ts {
                                    return Err(FlatfileError::TimestampRegression {
                                        symbol: symbol.clone(),
                                        window_idx,
                                        last_ts,
                                        ts,
                                    });
                                }
                            }
                            bucket.observe(trade);
                        }
                        Ok(())
                    },
                )?;
                processed_rows += 1;
                if self.config.progress_logging && last_progress.elapsed() >= interval {
                    info!(
                        "[{}] streamed {} underlying trades for {}",
                        self.config.label, processed_rows, date
                    );
                    last_progress = Instant::now();
                }
            }
        }
        if cancel.is_cancelled() {
            info!(
                "[{}] underlying trade ingestion cancelled before persisting {}",
                self.config.label, date
            );
            return Ok(());
        }
        tracker.finish(|state| self.finalize_underlying_trade_state(date, state))?;
        Ok(())
    }

    fn persist_underlying_trade_bucket(
        &self,
        date: NaiveDate,
        key: WindowKey,
        bucket: WindowBucket<UnderlyingTradeRecord>,
    ) -> Result<(), FlatfileError> {
        let row = self.ledger.get_trade_row(&key.symbol, key.window_idx)?;
        if row.underlying_trade_ref.status == SlotStatus::Filled {
            return Ok(());
        }
        self.ledger.mark_pending(
            &key.symbol,
            key.window_idx,
            SlotKind::Trade(TradeSlotKind::UnderlyingTrade),
        )?;
        let batch = underlying_trade_batch(&bucket.records)?;
        let relative_path = artifact_path(
            &self.config,
            "underlying/trades",
            date,
            &key.symbol,
            key.window_idx,
            "parquet",
        );
        let artifact = write_record_batch(&self.config, &relative_path, &batch)?;
        let payload = TradeBatchPayload {
            schema_version: UNDERLYING_TRADE_SCHEMA_VERSION,
            window_ts: window_start_ns(&self.ledger, key.window_idx)?,
            batch_id: self.batch_seq.fetch_add(1, AtomicOrdering::Relaxed) + 1,
            first_trade_ts: bucket.first_ts,
            last_trade_ts: bucket.last_ts,
            record_count: bucket.records.len() as u32,
            artifact_uri: artifact.relative_path.clone(),
            checksum: artifact.checksum,
        };
        let payload_id = {
            let mut stores = self.ledger.payload_stores();
            stores.trades.append(payload)?
        };
        let meta = PayloadMeta::new(PayloadType::Trade, payload_id, 1, artifact.checksum);
        if let Err(err) =
            self.ledger
                .set_underlying_trade_ref(&key.symbol, key.window_idx, meta, None)
        {
            self.ledger.clear_slot(
                &key.symbol,
                key.window_idx,
                SlotKind::Trade(TradeSlotKind::UnderlyingTrade),
            )?;
            return Err(err.into());
        }
        Ok(())
    }

    fn finalize_underlying_trade_state(
        &self,
        date: NaiveDate,
        state: SymbolWindow<UnderlyingTradeRecord>,
    ) -> Result<(), FlatfileError> {
        if let Some(bucket) = state.bucket {
            let key = WindowKey {
                symbol: state.symbol,
                window_idx: state.window_idx,
            };
            self.persist_underlying_trade_bucket(date, key, bucket)?;
        }
        Ok(())
    }

    fn open_underlying_trade_window(
        &self,
        date: NaiveDate,
        symbol: &str,
        window_idx: WindowIndex,
    ) -> Result<SymbolWindow<UnderlyingTradeRecord>, FlatfileError> {
        if !self.config.allows_symbol(symbol) {
            return Ok(SymbolWindow::skipped(symbol, window_idx));
        }
        let key = WindowKey {
            symbol: symbol.to_string(),
            window_idx,
        };
        let status = ensure_slot_pending(
            &self.ledger,
            &key,
            SlotKind::Trade(TradeSlotKind::UnderlyingTrade),
        )?;
        if status == SlotStatus::Filled {
            return Ok(SymbolWindow::skipped(symbol, window_idx));
        }
        if status == SlotStatus::Pending {
            let relative_path = artifact_path(
                &self.config,
                "underlying/trades",
                date,
                symbol,
                window_idx,
                "parquet",
            );
            cleanup_partial_artifacts(&self.config, &relative_path)?;
        }
        Ok(SymbolWindow::writable(symbol, window_idx))
    }

    fn window_idx_for_timestamp(&self, ts_ns: i64) -> Option<WindowIndex> {
        self.ledger.window_idx_for_timestamp(ts_ns / 1_000_000_000)
    }

    fn stop(&self, label: &str) -> EngineResult<()> {
        stop_runtime(label, self.config.label, &self.state)
    }

    fn health(&self) -> EngineHealth {
        runtime_health(&self.state)
    }
}

impl UnderlyingQuoteInner {
    fn new(config: FlatfileRuntimeConfig, ledger: Arc<WindowSpaceController>) -> Self {
        let window_space = ledger.window_space();
        let day_index = Arc::new(DayWindowIndex::new(window_space.as_ref()));
        Self {
            client: make_s3_client(&config),
            ledger,
            state: Mutex::new(EngineRuntimeState::Stopped),
            batch_seq: AtomicU32::new(0),
            calendar: TradingCalendar::new(Market::NYSE).expect("init calendar"),
            config,
            day_index,
        }
    }

    async fn run_quotes(self: Arc<Self>, cancel: CancellationToken) {
        let mut dates = planned_dates(&self.config, &self.calendar);
        dates.sort();
        dates.dedup();
        loop {
            if cancel.is_cancelled() {
                break;
            }
            let now = Utc::now();
            let mut made_progress = false;
            let mut has_future_work = false;
            let mut next_ready_at: Option<DateTime<Utc>> = None;
            for &date in &dates {
                if cancel.is_cancelled() {
                    break;
                }
                if let Some(ready_at) = ready_instant(&self.config, &self.calendar, date) {
                    if ready_at > now {
                        has_future_work = true;
                        update_next_ready(&mut next_ready_at, ready_at);
                        continue;
                    }
                }
                if !day_requires_ingest(
                    self.ledger.as_ref(),
                    self.day_index.as_ref(),
                    date,
                    TradeSlotKind::UnderlyingQuote,
                ) {
                    continue;
                }
                has_future_work = true;
                if let Err(err) = self.process_equity_quotes_day(date, cancel.clone()).await {
                    error!("underlying quotes day {} failed: {err}", date);
                }
                made_progress = true;
            }
            if !has_future_work {
                break;
            }
            if !made_progress {
                let cancelled = if let Some(ready_at) = next_ready_at {
                    wait_until_ready(ready_at, cancel.clone()).await
                } else {
                    sleep_with_cancel(Duration::from_secs(60), cancel.clone()).await
                };
                if cancelled {
                    break;
                }
            }
        }
        info!(
            "[{}] underlying quote flatfile engine exiting",
            self.config.label
        );
    }

    async fn process_equity_quotes_day(
        &self,
        date: NaiveDate,
        cancel: CancellationToken,
    ) -> Result<(), FlatfileError> {
        let key = format!(
            "us_stocks_sip/quotes_v1/{}/{:02}/{}-{:02}-{:02}.csv.gz",
            date.year(),
            date.month(),
            date.year(),
            date.month(),
            date.day()
        );
        info!(
            "[{}] ingesting underlying quotes from {}",
            self.config.label, key
        );
        let (stream, token) = fetch_stream(
            &self.client,
            &self.config,
            &key,
            TradeSlotKind::UnderlyingQuote,
        )
        .await?;
        let result = self.consume_underlying_quotes(date, stream, cancel).await;
        drop(token);
        result
    }

    async fn consume_underlying_quotes<R>(
        &self,
        date: NaiveDate,
        reader: R,
        cancel: CancellationToken,
    ) -> Result<(), FlatfileError>
    where
        R: AsyncRead + Unpin + Send,
    {
        let mut csv_reader = AsyncReaderBuilder::new()
            .trim(Trim::All)
            .create_reader(reader);
        let mut records = csv_reader.records();
        let mut tracker = SymbolTracker::new();
        let mut processed_rows = 0usize;
        let mut last_progress = Instant::now();
        let interval = Duration::from_millis(self.config.progress_update_ms.max(10));
        while let Some(record) = records.next().await {
            if cancel.is_cancelled() {
                info!(
                    "[{}] underlying quote ingestion cancelled while streaming {}",
                    self.config.label, date
                );
                return Ok(());
            }
            let record = match record {
                Ok(record) => record,
                Err(err) => {
                    log_streaming_error(self.config.label, "underlying quotes", date, &err);
                    return Err(err.into());
                }
            };
            if record.len() < 12 {
                continue;
            }
            if let Some((symbol, quote, _)) = parse_quote_row(&record, QuoteSource::Underlying) {
                let ts = quote.timestamp_ns();
                let window_idx = match self.window_idx_for_timestamp(ts) {
                    Some(idx) => idx,
                    None => continue,
                };
                tracker.with_bucket(
                    &symbol,
                    window_idx,
                    |sym, idx| self.open_underlying_quote_window(date, sym, idx),
                    |state| self.finalize_underlying_quote_state(date, state),
                    |bucket_opt| {
                        if let Some(bucket) = bucket_opt {
                            if let Some(last_ts) = bucket.last_timestamp() {
                                if ts < last_ts {
                                    return Err(FlatfileError::TimestampRegression {
                                        symbol: symbol.clone(),
                                        window_idx,
                                        last_ts,
                                        ts,
                                    });
                                }
                            }
                            bucket.observe(quote);
                        }
                        Ok(())
                    },
                )?;
                processed_rows += 1;
                if self.config.progress_logging && last_progress.elapsed() >= interval {
                    info!(
                        "[{}] streamed {} underlying quotes for {}",
                        self.config.label, processed_rows, date
                    );
                    last_progress = Instant::now();
                }
            }
        }
        if cancel.is_cancelled() {
            info!(
                "[{}] underlying quote ingestion cancelled before persisting {}",
                self.config.label, date
            );
            return Ok(());
        }
        tracker.finish(|state| self.finalize_underlying_quote_state(date, state))?;
        Ok(())
    }

    fn persist_underlying_quote_bucket(
        &self,
        date: NaiveDate,
        key: WindowKey,
        bucket: WindowBucket<QuoteRecord>,
    ) -> Result<(), FlatfileError> {
        let row = self.ledger.get_trade_row(&key.symbol, key.window_idx)?;
        if row.underlying_quote_ref.status == SlotStatus::Filled {
            return Ok(());
        }
        self.ledger.mark_pending(
            &key.symbol,
            key.window_idx,
            SlotKind::Trade(TradeSlotKind::UnderlyingQuote),
        )?;
        let batch = quote_batch(&bucket.records)?;
        let relative_path = artifact_path(
            &self.config,
            "underlying/quotes",
            date,
            &key.symbol,
            key.window_idx,
            "parquet",
        );
        let artifact = write_record_batch(&self.config, &relative_path, &batch)?;
        let payload = QuoteBatchPayload {
            schema_version: QUOTE_SCHEMA_VERSION,
            window_ts: window_start_ns(&self.ledger, key.window_idx)?,
            batch_id: self.batch_seq.fetch_add(1, AtomicOrdering::Relaxed) + 1,
            first_quote_ts: bucket.first_ts,
            last_quote_ts: bucket.last_ts,
            nbbo_sample_count: bucket.records.len() as u32,
            artifact_uri: artifact.relative_path.clone(),
            checksum: artifact.checksum,
        };
        let payload_id = {
            let mut stores = self.ledger.payload_stores();
            stores.quotes.append(payload)?
        };
        let meta = PayloadMeta::new(PayloadType::Quote, payload_id, 1, artifact.checksum);
        if let Err(err) =
            self.ledger
                .set_underlying_quote_ref(&key.symbol, key.window_idx, meta, None)
        {
            self.ledger.clear_slot(
                &key.symbol,
                key.window_idx,
                SlotKind::Trade(TradeSlotKind::UnderlyingQuote),
            )?;
            return Err(err.into());
        }
        Ok(())
    }

    fn finalize_underlying_quote_state(
        &self,
        date: NaiveDate,
        state: SymbolWindow<QuoteRecord>,
    ) -> Result<(), FlatfileError> {
        if let Some(bucket) = state.bucket {
            let key = WindowKey {
                symbol: state.symbol,
                window_idx: state.window_idx,
            };
            self.persist_underlying_quote_bucket(date, key, bucket)?;
        }
        Ok(())
    }

    fn open_underlying_quote_window(
        &self,
        date: NaiveDate,
        symbol: &str,
        window_idx: WindowIndex,
    ) -> Result<SymbolWindow<QuoteRecord>, FlatfileError> {
        if !self.config.allows_symbol(symbol) {
            return Ok(SymbolWindow::skipped(symbol, window_idx));
        }
        let key = WindowKey {
            symbol: symbol.to_string(),
            window_idx,
        };
        let status = ensure_slot_pending(
            &self.ledger,
            &key,
            SlotKind::Trade(TradeSlotKind::UnderlyingQuote),
        )?;
        if status == SlotStatus::Filled {
            return Ok(SymbolWindow::skipped(symbol, window_idx));
        }
        if status == SlotStatus::Pending {
            let relative_path = artifact_path(
                &self.config,
                "underlying/quotes",
                date,
                symbol,
                window_idx,
                "parquet",
            );
            cleanup_partial_artifacts(&self.config, &relative_path)?;
        }
        Ok(SymbolWindow::writable(symbol, window_idx))
    }

    fn window_idx_for_timestamp(&self, ts_ns: i64) -> Option<WindowIndex> {
        self.ledger.window_idx_for_timestamp(ts_ns / 1_000_000_000)
    }

    fn stop(&self, label: &str) -> EngineResult<()> {
        stop_runtime(label, self.config.label, &self.state)
    }

    fn health(&self) -> EngineHealth {
        runtime_health(&self.state)
    }
}

fn make_s3_client(config: &FlatfileRuntimeConfig) -> Client {
    let credentials = Credentials::new(
        config.access_key_id.clone(),
        config.secret_access_key.clone(),
        None,
        None,
        "trade-flatfile",
    );
    let s3_cfg = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .force_path_style(true)
        .endpoint_url(config.endpoint.clone())
        .region(Region::new(config.region.clone()))
        .credentials_provider(credentials)
        .build();
    Client::from_conf(s3_cfg)
}

fn planned_dates(config: &FlatfileRuntimeConfig, calendar: &TradingCalendar) -> Vec<NaiveDate> {
    let mut days = Vec::new();
    for range in &config.date_ranges {
        let Ok(start_ns) = range.start_ts_ns() else {
            continue;
        };
        let start = ts_ns_to_date(start_ns);
        if start > config.window_end {
            continue;
        }
        let mut end = range
            .end_ts_ns()
            .ok()
            .flatten()
            .map(ts_ns_to_date)
            .unwrap_or(config.window_end);
        if end > config.window_end {
            end = config.window_end;
        }
        if start > end {
            continue;
        }
        let mut current = start;
        while current <= end {
            if calendar.is_trading_day(current).unwrap_or(false) {
                days.push(current);
            }
            current = current.succ_opt().unwrap();
        }
    }
    days
}

fn ready_instant(
    config: &FlatfileRuntimeConfig,
    calendar: &TradingCalendar,
    date: NaiveDate,
) -> Option<DateTime<Utc>> {
    let next_day = date.succ_opt()?;
    let next_is_trading = calendar.is_trading_day(next_day).unwrap_or(false);
    let ready_time = if next_is_trading {
        config.next_day_ready_time
    } else {
        config.non_trading_ready_time
    };
    let naive = next_day.and_time(ready_time);
    Some(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
}

fn update_next_ready(slot: &mut Option<DateTime<Utc>>, candidate: DateTime<Utc>) {
    match slot {
        Some(current) if candidate < *current => *current = candidate,
        Some(_) => {}
        None => *slot = Some(candidate),
    }
}

async fn wait_until_ready(target: DateTime<Utc>, cancel: CancellationToken) -> bool {
    let now = Utc::now();
    let wait = target.signed_duration_since(now);
    let duration = wait.to_std().unwrap_or_default();
    let duration = if duration.is_zero() {
        Duration::from_secs(5)
    } else {
        duration
    };
    sleep_with_cancel(duration, cancel).await
}

async fn sleep_with_cancel(duration: Duration, cancel: CancellationToken) -> bool {
    tokio::select! {
        _ = sleep(duration) => false,
        _ = cancel.cancelled() => true,
    }
}

fn log_streaming_error(engine_label: &str, dataset: &str, date: NaiveDate, err: &csv_async::Error) {
    match err.kind() {
        CsvErrorKind::Io(io_err) => error!(
            "[{}] {} day {} streaming error: kind={:?} message={}",
            engine_label,
            dataset,
            date,
            io_err.kind(),
            io_err
        ),
        _ => error!(
            "[{}] {} day {} csv error: {}",
            engine_label, dataset, date, err
        ),
    }
}

fn ts_ns_to_date(ns: i64) -> NaiveDate {
    DateTime::<Utc>::from_timestamp(ns / 1_000_000_000, (ns % 1_000_000_000) as u32)
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).unwrap())
        .date_naive()
}

async fn fetch_stream(
    client: &Client,
    config: &FlatfileRuntimeConfig,
    key: &str,
    slot: TradeSlotKind,
) -> Result<(Box<dyn AsyncRead + Unpin + Send>, DownloadToken), FlatfileError> {
    let temp_file = NamedTempFile::new()?;
    let mut writer = BufWriter::new(TokioFile::from_std(temp_file.reopen()?));
    let resp = client
        .get_object()
        .bucket(config.bucket.clone())
        .key(key)
        .send()
        .await
        .map_err(|err| FlatfileError::Sdk(err.to_string()))?;
    let total_bytes = resp.content_length().map(|val| val as u64);
    config.download_metrics.reset(slot, total_bytes);
    let mut body: ByteStream = resp.body;
    while let Some(bytes) = body.next().await {
        let bytes = bytes.map_err(|err| FlatfileError::Sdk(err.to_string()))?;
        writer.write_all(&bytes).await?;
        config
            .download_metrics
            .add_streamed(slot, bytes.len() as u64);
    }
    writer.flush().await?;
    drop(writer);
    let temp_path = temp_file.into_temp_path();
    let file = TokioFile::open(temp_path.as_ref()).await?;
    let buf = BufReader::new(file);
    let decoder = GzipDecoder::new(buf);
    let token =
        DownloadToken::with_temp_path(Arc::clone(&config.download_metrics), slot, temp_path);
    Ok((Box::new(decoder), token))
}

struct DownloadToken {
    metrics: Arc<DownloadMetrics>,
    slot: TradeSlotKind,
    temp_path: Option<TempPath>,
}

impl DownloadToken {
    fn new(metrics: Arc<DownloadMetrics>, slot: TradeSlotKind) -> Self {
        Self {
            metrics,
            slot,
            temp_path: None,
        }
    }

    fn with_temp_path(
        metrics: Arc<DownloadMetrics>,
        slot: TradeSlotKind,
        temp_path: TempPath,
    ) -> Self {
        Self {
            metrics,
            slot,
            temp_path: Some(temp_path),
        }
    }
}

impl Drop for DownloadToken {
    fn drop(&mut self) {
        self.metrics.complete(self.slot);
        if let Some(path) = self.temp_path.take() {
            let _ = path.close();
        }
    }
}

fn parse_option_trade_row(record: &csv_async::StringRecord) -> Option<(String, OptionTradeRecord)> {
    if record.len() < 7 {
        return None;
    }
    let contract = record.get(0)?.to_string();
    let trade_ts_ns = record.get(5)?.parse().unwrap_or(0);
    let (contract_direction, strike_price, expiry_ts_ns, underlying) =
        parse_opra_contract(&contract, Some(trade_ts_ns));
    let conditions = parse_conditions_field(record.get(1).unwrap_or_default());
    let exchange = record.get(3)?.parse().unwrap_or(0);
    let price = record.get(4)?.parse().unwrap_or(0.0);
    let size = record.get(6)?.parse().unwrap_or(0);
    let trade_uid = option_trade_uid(
        &contract,
        trade_ts_ns,
        None,
        price,
        size,
        exchange,
        &conditions,
    );
    let trade = OptionTradeRecord {
        contract: contract.clone(),
        trade_uid,
        contract_direction,
        strike_price,
        underlying,
        trade_ts_ns,
        participant_ts_ns: None,
        price,
        size,
        conditions,
        exchange,
        expiry_ts_ns,
        source: Source::Flatfile,
        quality: Quality::Prelim,
        watermark_ts_ns: trade_ts_ns,
    };
    Some((contract, trade))
}

fn parse_underlying_trade_row(
    record: &csv_async::StringRecord,
) -> Option<(String, UnderlyingTradeRecord)> {
    let symbol = record.get(0)?.to_string();
    let conditions = parse_conditions_field(record.get(1).unwrap_or_default());
    let correction_val = record.get(2).and_then(|s| s.parse::<i32>().ok());
    let exchange = record.get(3)?.parse().unwrap_or(0);
    let trade_id = record
        .get(4)
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty());
    let trade_ts_ns = record.get(5)?.parse().unwrap_or(0);
    let price = record.get(6)?.parse().unwrap_or(0.0);
    let seq = record.get(7).and_then(|s| s.parse::<u64>().ok());
    let participant_ts_ns = record.get(8).and_then(|s| s.parse::<i64>().ok());
    let size = record.get(9)?.parse().unwrap_or(0);
    let tape = record
        .get(10)
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty());
    let trf_id = record
        .get(11)
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty());
    let trf_ts_ns = record.get(12).and_then(|s| s.parse::<i64>().ok());
    let trade_uid = equity_trade_uid(
        &symbol,
        trade_ts_ns,
        participant_ts_ns,
        seq,
        price,
        size,
        exchange,
        trade_id.as_deref(),
        correction_val,
        &conditions,
    );
    let trade = UnderlyingTradeRecord {
        symbol: symbol.clone(),
        trade_uid,
        trade_ts_ns,
        participant_ts_ns,
        price,
        size,
        conditions,
        exchange,
        trade_id,
        seq,
        tape,
        correction: correction_val,
        trf_id,
        trf_ts_ns,
        source: Source::Flatfile,
        quality: Quality::Prelim,
        watermark_ts_ns: trade_ts_ns,
    };
    Some((symbol, trade))
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

enum QuoteSource {
    Option,
    Underlying,
}

fn parse_quote_row(
    record: &csv_async::StringRecord,
    source: QuoteSource,
) -> Option<(String, QuoteRecord, Option<String>)> {
    let instrument_id = record.get(0)?.to_string();
    let ask_exchange = record.get(1)?.parse().unwrap_or(0);
    let ask_price = record.get(2)?.parse().unwrap_or(0.0);
    let ask_size = record.get(3)?.parse().unwrap_or(0);
    let bid_exchange = record.get(4)?.parse().unwrap_or(0);
    let bid_price = record.get(5)?.parse().unwrap_or(0.0);
    let bid_size = record.get(6)?.parse().unwrap_or(0);
    let (condition_idx, seq_idx, ts_idx) = if record.len() >= 12 {
        (Some(7), Some(10), 11)
    } else if record.len() >= 9 {
        (None, Some(7), 8)
    } else {
        return None;
    };
    let condition = condition_idx.and_then(|idx| {
        record.get(idx).and_then(|s| {
            if s.is_empty() {
                None
            } else {
                s.parse::<i32>().ok()
            }
        })
    });
    let sequence_number = seq_idx
        .and_then(|idx| record.get(idx))
        .and_then(|s| s.parse::<u64>().ok());
    let quote_ts_ns = record.get(ts_idx)?.parse().unwrap_or(0);
    if quote_ts_ns == 0 {
        return None;
    }
    let state = infer_nbbo_state(bid_price, ask_price);
    let quote_uid = quote_uid(
        &instrument_id,
        quote_ts_ns,
        sequence_number,
        bid_price,
        ask_price,
        bid_size,
        ask_size,
        Some(bid_exchange),
        Some(ask_exchange),
        condition,
    );
    let quote = QuoteRecord {
        instrument_id: instrument_id.clone(),
        quote_uid,
        quote_ts_ns,
        bid: bid_price,
        ask: ask_price,
        bid_sz: bid_size,
        ask_sz: ask_size,
        state,
        condition,
        best_bid_venue: Some(bid_exchange),
        best_ask_venue: Some(ask_exchange),
        source: Source::Flatfile,
        quality: Quality::Prelim,
        watermark_ts_ns: quote_ts_ns,
    };
    let underlying = match source {
        QuoteSource::Option => {
            let (_, _, _, base) = parse_opra_contract(&instrument_id, Some(quote_ts_ns));
            if base.is_empty() { None } else { Some(base) }
        }
        QuoteSource::Underlying => None,
    };
    Some((instrument_id, quote, underlying))
}

fn quote_to_nbbo(record: &QuoteRecord) -> Nbbo {
    Nbbo {
        instrument_id: record.instrument_id.clone(),
        quote_uid: record.quote_uid,
        quote_ts_ns: record.quote_ts_ns,
        bid: record.bid,
        ask: record.ask,
        bid_sz: record.bid_sz,
        ask_sz: record.ask_sz,
        state: record.state.clone(),
        condition: record.condition,
        best_bid_venue: record.best_bid_venue,
        best_ask_venue: record.best_ask_venue,
        source: record.source.clone(),
        quality: record.quality.clone(),
        watermark_ts_ns: record.watermark_ts_ns,
    }
}

fn parse_conditions_field(field: &str) -> Vec<i32> {
    field
        .split(',')
        .filter_map(|s| {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                None
            } else {
                trimmed.parse::<i32>().ok()
            }
        })
        .collect()
}

fn infer_nbbo_state(bid: f64, ask: f64) -> NbboState {
    if ask > 0.0 && bid > ask {
        NbboState::Crossed
    } else if ask > 0.0 && (bid - ask).abs() < f64::EPSILON {
        NbboState::Locked
    } else {
        NbboState::Normal
    }
}

fn option_trade_batch(records: &[OptionTradeRecord]) -> Result<RecordBatch, FlatfileError> {
    let schema: SchemaRef = Arc::new(option_trade_record_schema());
    let contract = Arc::new(StringArray::from(
        records
            .iter()
            .map(|r| r.contract.clone())
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let trade_uid = Arc::new(FixedSizeBinaryArray::try_from_iter(
        records.iter().map(|r| r.trade_uid.as_ref()),
    )?) as ArrayRef;
    let contract_direction = Arc::new(StringArray::from(
        records
            .iter()
            .map(|r| r.contract_direction.to_string())
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let strike_price = Arc::new(Float64Array::from(
        records.iter().map(|r| r.strike_price).collect::<Vec<_>>(),
    )) as ArrayRef;
    let underlying = Arc::new(StringArray::from(
        records
            .iter()
            .map(|r| r.underlying.clone())
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let trade_ts_ns = Arc::new(Int64Array::from(
        records.iter().map(|r| r.trade_ts_ns).collect::<Vec<_>>(),
    )) as ArrayRef;
    let participant_ts_ns = Arc::new(Int64Array::from(
        records
            .iter()
            .map(|r| r.participant_ts_ns)
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let price = Arc::new(Float64Array::from(
        records.iter().map(|r| r.price).collect::<Vec<_>>(),
    )) as ArrayRef;
    let size = Arc::new(UInt32Array::from(
        records.iter().map(|r| r.size).collect::<Vec<_>>(),
    )) as ArrayRef;
    let mut list_builder = ListBuilder::new(Int32Builder::new());
    for record in records {
        let values = list_builder.values();
        for cond in &record.conditions {
            values.append_value(*cond);
        }
        list_builder.append(true);
    }
    let conditions = Arc::new(list_builder.finish()) as ArrayRef;
    let exchange = Arc::new(Int32Array::from(
        records.iter().map(|r| r.exchange).collect::<Vec<_>>(),
    )) as ArrayRef;
    let expiry = Arc::new(Int64Array::from(
        records.iter().map(|r| r.expiry_ts_ns).collect::<Vec<_>>(),
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
        contract,
        trade_uid,
        contract_direction,
        strike_price,
        underlying,
        trade_ts_ns,
        participant_ts_ns,
        price,
        size,
        conditions,
        exchange,
        expiry,
        source,
        quality,
        watermark,
    ];
    RecordBatch::try_new(schema, arrays).map_err(FlatfileError::from)
}

fn aggressor_batch(records: &[AggressorRecord]) -> Result<RecordBatch, FlatfileError> {
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
    )?) as ArrayRef;
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

    RecordBatch::try_new(schema, arrays).map_err(FlatfileError::from)
}

fn underlying_trade_batch(records: &[UnderlyingTradeRecord]) -> Result<RecordBatch, FlatfileError> {
    let schema: SchemaRef = Arc::new(underlying_trade_record_schema());
    let symbol = Arc::new(StringArray::from(
        records.iter().map(|r| r.symbol.clone()).collect::<Vec<_>>(),
    )) as ArrayRef;
    let trade_uid = Arc::new(FixedSizeBinaryArray::try_from_iter(
        records.iter().map(|r| r.trade_uid.as_ref()),
    )?) as ArrayRef;
    let trade_ts_ns = Arc::new(Int64Array::from(
        records.iter().map(|r| r.trade_ts_ns).collect::<Vec<_>>(),
    )) as ArrayRef;
    let participant_ts_ns = Arc::new(Int64Array::from(
        records
            .iter()
            .map(|r| r.participant_ts_ns)
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let price = Arc::new(Float64Array::from(
        records.iter().map(|r| r.price).collect::<Vec<_>>(),
    )) as ArrayRef;
    let size = Arc::new(UInt32Array::from(
        records.iter().map(|r| r.size).collect::<Vec<_>>(),
    )) as ArrayRef;
    let mut list_builder = ListBuilder::new(Int32Builder::new());
    for record in records {
        let values = list_builder.values();
        for cond in &record.conditions {
            values.append_value(*cond);
        }
        list_builder.append(true);
    }
    let conditions = Arc::new(list_builder.finish()) as ArrayRef;
    let exchange = Arc::new(Int32Array::from(
        records.iter().map(|r| r.exchange).collect::<Vec<_>>(),
    )) as ArrayRef;
    let trade_id = Arc::new(StringArray::from(
        records
            .iter()
            .map(|r| r.trade_id.clone())
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let seq = Arc::new(UInt64Array::from(
        records.iter().map(|r| r.seq).collect::<Vec<_>>(),
    )) as ArrayRef;
    let tape = Arc::new(StringArray::from(
        records.iter().map(|r| r.tape.clone()).collect::<Vec<_>>(),
    )) as ArrayRef;
    let correction = Arc::new(Int32Array::from(
        records.iter().map(|r| r.correction).collect::<Vec<_>>(),
    )) as ArrayRef;
    let trf_id = Arc::new(StringArray::from(
        records.iter().map(|r| r.trf_id.clone()).collect::<Vec<_>>(),
    )) as ArrayRef;
    let trf_ts_ns = Arc::new(Int64Array::from(
        records.iter().map(|r| r.trf_ts_ns).collect::<Vec<_>>(),
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
        symbol,
        trade_uid,
        trade_ts_ns,
        participant_ts_ns,
        price,
        size,
        conditions,
        exchange,
        trade_id,
        seq,
        tape,
        correction,
        trf_id,
        trf_ts_ns,
        source,
        quality,
        watermark,
    ];
    RecordBatch::try_new(schema, arrays).map_err(FlatfileError::from)
}

fn quote_batch(records: &[QuoteRecord]) -> Result<RecordBatch, FlatfileError> {
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
    let best_bid = Arc::new(Int32Array::from(
        records.iter().map(|r| r.best_bid_venue).collect::<Vec<_>>(),
    )) as ArrayRef;
    let best_ask = Arc::new(Int32Array::from(
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
        best_bid,
        best_ask,
        source,
        quality,
        watermark,
    ];
    RecordBatch::try_new(schema, arrays).map_err(FlatfileError::from)
}

fn stop_runtime(
    label: &str,
    config_label: &str,
    state: &Mutex<EngineRuntimeState>,
) -> EngineResult<()> {
    let mut guard = state.lock();
    let bundle = match std::mem::replace(&mut *guard, EngineRuntimeState::Stopped) {
        EngineRuntimeState::Running(bundle) => bundle,
        EngineRuntimeState::Stopped => return Err(EngineError::NotRunning),
    };
    let RuntimeBundle {
        runtime,
        handle,
        cancel,
    } = bundle;
    cancel.cancel();
    handle.abort();
    if let Err(err) = runtime.block_on(async { handle.await }) {
        if !err.is_cancelled() {
            error!("{label} engine join error: {err}");
        }
    }
    info!("[{}] {} engine stopped", config_label, label);
    Ok(())
}

fn runtime_health(state: &Mutex<EngineRuntimeState>) -> EngineHealth {
    let guard = state.lock();
    match &*guard {
        EngineRuntimeState::Running(_) => EngineHealth::new(HealthStatus::Ready, None),
        EngineRuntimeState::Stopped => EngineHealth::new(HealthStatus::Stopped, None),
    }
}

impl HasTimestamp for OptionTradeRecord {
    fn timestamp_ns(&self) -> i64 {
        self.trade_ts_ns
    }
}

impl HasTimestamp for UnderlyingTradeRecord {
    fn timestamp_ns(&self) -> i64 {
        self.participant_ts_ns.unwrap_or(self.trade_ts_ns)
    }
}

impl HasTimestamp for QuoteRecord {
    fn timestamp_ns(&self) -> i64 {
        self.quote_ts_ns
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::config::DateRange;
    use futures::StreamExt;
    use tokio::fs::File;

    async fn open_fixture(path: &str) -> GzipDecoder<BufReader<File>> {
        let file = File::open(path)
            .await
            .unwrap_or_else(|err| panic!("failed to open fixture {}: {err}", path));
        GzipDecoder::new(BufReader::new(file))
    }

    #[tokio::test]
    async fn option_trades_fixture_parses() {
        let reader =
            open_fixture("../flatfile-source/fixtures/options_trades_example.csv.gz").await;
        let mut csv = AsyncReaderBuilder::new()
            .trim(Trim::All)
            .create_reader(reader);
        let mut records = csv.records();
        let mut rows = Vec::new();
        while let Some(record) = records.next().await {
            let record = record.expect("record");
            if let Some((_symbol, trade)) = parse_option_trade_row(&record) {
                rows.push(trade);
            }
        }
        assert!(!rows.is_empty(), "option trades fixture parsed rows");
        let first = &rows[0];
        assert_eq!(first.contract, "O:SPY251112C00675000");
        assert_eq!(first.price, 9.87);
        assert_eq!(first.size, 1);
        assert_eq!(first.exchange, 312);
        assert_eq!(first.contract_direction, 'C');
        assert_eq!(first.underlying, "SPY");
        assert!(first.participant_ts_ns.is_none());
    }

    #[tokio::test]
    async fn equity_trades_fixture_parses() {
        let reader = open_fixture("../flatfile-source/fixtures/stock_trades_sample.csv.gz").await;
        let mut csv = AsyncReaderBuilder::new()
            .trim(Trim::All)
            .create_reader(reader);
        let mut records = csv.records();
        let mut rows = Vec::new();
        while let Some(record) = records.next().await {
            let record = record.expect("record");
            if let Some((_symbol, trade)) = parse_underlying_trade_row(&record) {
                rows.push(trade);
            }
        }
        assert!(!rows.is_empty(), "underlying trades fixture parsed rows");
        let first = &rows[0];
        assert_eq!(first.symbol, "MSFT");
        assert_eq!(first.price, 276.16);
        assert_eq!(first.size, 55);
        assert_eq!(first.conditions, vec![12, 37]);
        assert_eq!(first.exchange, 11);
    }

    #[tokio::test]
    async fn option_quotes_fixture_parses() {
        let reader =
            open_fixture("../flatfile-source/fixtures/options_quotes_example.csv.gz").await;
        let mut csv = AsyncReaderBuilder::new()
            .trim(Trim::All)
            .create_reader(reader);
        let mut records = csv.records();
        let mut rows = Vec::new();
        while let Some(record) = records.next().await {
            let record = record.expect("record");
            if let Some((_symbol, quote, underlying)) =
                parse_quote_row(&record, QuoteSource::Option)
            {
                assert!(underlying.is_some());
                rows.push(quote);
            }
        }
        assert!(!rows.is_empty(), "option quotes fixture parsed rows");
        let first = &rows[0];
        assert_eq!(first.instrument_id, "O:SPY241220P00720000");
        assert_eq!(first.ask, 326.77);
        assert_eq!(first.bid, 321.77);
        assert!(first.quote_ts_ns > 0);
    }

    #[tokio::test]
    async fn equity_quotes_fixture_parses() {
        let reader = open_fixture("../flatfile-source/fixtures/stock_quotes_sample.csv.gz").await;
        let mut csv = AsyncReaderBuilder::new()
            .trim(Trim::All)
            .create_reader(reader);
        let mut records = csv.records();
        let mut rows = Vec::new();
        while let Some(record) = records.next().await {
            let record = record.expect("record");
            if let Some((_symbol, quote, _)) = parse_quote_row(&record, QuoteSource::Underlying) {
                rows.push(quote);
            }
        }
        assert!(!rows.is_empty(), "equity quotes fixture parsed rows");
        let first = &rows[0];
        assert_eq!(first.instrument_id, "MSFT");
        assert!(first.quote_ts_ns > 0);
        assert!(first.bid >= 0.0);
    }
}

enum EngineRuntimeState {
    Stopped,
    Running(RuntimeBundle),
}

struct RuntimeBundle {
    runtime: Runtime,
    handle: JoinHandle<()>,
    cancel: CancellationToken,
}

#[derive(Clone, Eq, PartialEq, Hash)]
struct WindowKey {
    symbol: String,
    window_idx: WindowIndex,
}

struct SymbolWindow<T> {
    symbol: String,
    window_idx: WindowIndex,
    bucket: Option<WindowBucket<T>>,
}

impl<T> SymbolWindow<T> {
    fn writable(symbol: &str, window_idx: WindowIndex) -> Self {
        Self {
            symbol: symbol.to_string(),
            window_idx,
            bucket: Some(WindowBucket::new()),
        }
    }

    fn skipped(symbol: &str, window_idx: WindowIndex) -> Self {
        Self {
            symbol: symbol.to_string(),
            window_idx,
            bucket: None,
        }
    }
}

struct SymbolTracker<T> {
    active: Option<SymbolWindow<T>>,
}

impl<T> SymbolTracker<T> {
    fn new() -> Self {
        Self { active: None }
    }

    fn with_bucket<Init, Flush, F, R>(
        &mut self,
        symbol: &str,
        window_idx: WindowIndex,
        mut init: Init,
        mut flush: Flush,
        f: F,
    ) -> Result<R, FlatfileError>
    where
        Init: FnMut(&str, WindowIndex) -> Result<SymbolWindow<T>, FlatfileError>,
        Flush: FnMut(SymbolWindow<T>) -> Result<(), FlatfileError>,
        F: FnOnce(Option<&mut WindowBucket<T>>) -> Result<R, FlatfileError>,
    {
        loop {
            if self.active.is_none() {
                let new_state = init(symbol, window_idx)?;
                self.active = Some(new_state);
                continue;
            }

            let should_replace = {
                let active = self
                    .active
                    .as_mut()
                    .expect("active state set before reach replace logic");
                if active.symbol == symbol {
                    match window_idx.cmp(&active.window_idx) {
                        Ordering::Equal => {
                            let bucket = active.bucket.as_mut();
                            return f(bucket);
                        }
                        Ordering::Less => {
                            return Err(FlatfileError::OutOfOrderWindow {
                                current: active.window_idx,
                                encountered: window_idx,
                            });
                        }
                        Ordering::Greater => true,
                    }
                } else {
                    true
                }
            };

            if should_replace {
                if let Some(old) = self.active.take() {
                    flush(old)?;
                }
                continue;
            }
        }
    }

    fn finish<Flush>(&mut self, mut flush: Flush) -> Result<(), FlatfileError>
    where
        Flush: FnMut(SymbolWindow<T>) -> Result<(), FlatfileError>,
    {
        if let Some(state) = self.active.take() {
            flush(state)?;
        }
        Ok(())
    }
}

struct TradeCursor<R> {
    reader: AsyncReader<R>,
    scratch: StringRecord,
    buffered: Option<(String, OptionTradeRecord)>,
}

impl<R> TradeCursor<R>
where
    R: AsyncRead + Unpin + Send,
{
    fn new(reader: R) -> Self {
        Self {
            reader: AsyncReaderBuilder::new()
                .trim(Trim::All)
                .create_reader(reader),
            scratch: StringRecord::new(),
            buffered: None,
        }
    }

    async fn ensure_buffered(&mut self) -> Result<(), FlatfileError> {
        if self.buffered.is_some() {
            return Ok(());
        }
        while self.buffered.is_none() {
            self.scratch.clear();
            let has_record = self.reader.read_record(&mut self.scratch).await?;
            if !has_record {
                break;
            }
            if self.scratch.len() < 7 {
                continue;
            }
            if let Some((contract, trade)) = parse_option_trade_row(&self.scratch) {
                self.buffered = Some((contract, trade));
            }
        }
        Ok(())
    }

    async fn has_pending(&mut self) -> Result<bool, FlatfileError> {
        self.ensure_buffered().await?;
        Ok(self.buffered.is_some())
    }

    async fn peek_contract(&mut self) -> Result<Option<String>, FlatfileError> {
        self.ensure_buffered().await?;
        Ok(self.buffered.as_ref().map(|(c, _)| c.clone()))
    }

    fn compare_contract(&self, contract: &str) -> Option<Ordering> {
        self.buffered
            .as_ref()
            .map(|(c, _)| c.as_str().cmp(contract))
    }

    fn buffered_ts(&self) -> Option<i64> {
        self.buffered.as_ref().map(|(_, trade)| trade.trade_ts_ns)
    }

    async fn consume(&mut self) -> Result<OptionTradeRecord, FlatfileError> {
        self.ensure_buffered().await?;
        let (_, trade) = self
            .buffered
            .take()
            .ok_or(FlatfileError::MissingBufferedTrade)?;
        Ok(trade)
    }
}

struct DayWindowIndex {
    ranges: HashMap<NaiveDate, (WindowIndex, WindowIndex)>,
}

impl DayWindowIndex {
    fn new(window_space: &WindowSpace) -> Self {
        let mut ranges = HashMap::new();
        for meta in window_space.iter() {
            if let Some(dt) = DateTime::<Utc>::from_timestamp(meta.start_ts, 0) {
                let date = dt.date_naive();
                ranges
                    .entry(date)
                    .and_modify(|range: &mut (WindowIndex, WindowIndex)| {
                        range.0 = range.0.min(meta.window_idx);
                        range.1 = range.1.max(meta.window_idx);
                    })
                    .or_insert((meta.window_idx, meta.window_idx));
            }
        }
        Self { ranges }
    }

    fn range_for(&self, date: &NaiveDate) -> Option<(WindowIndex, WindowIndex)> {
        self.ranges.get(date).copied()
    }
}

fn day_requires_ingest(
    ledger: &WindowSpaceController,
    day_index: &DayWindowIndex,
    date: NaiveDate,
    slot: TradeSlotKind,
) -> bool {
    let Some((start_idx, end_idx)) = day_index.range_for(&date) else {
        return true;
    };
    match slot_needs_ingest(ledger, slot, start_idx, end_idx) {
        Ok(needs) => needs,
        Err(err) => {
            warn!(
                "failed to evaluate coverage for {}: {}; scheduling ingestion",
                date, err
            );
            true
        }
    }
}

fn slot_needs_ingest(
    ledger: &WindowSpaceController,
    slot: TradeSlotKind,
    start_idx: WindowIndex,
    end_idx: WindowIndex,
) -> Result<bool, WindowSpaceError> {
    let symbol_ids = ledger.trade_symbol_ids();
    if symbol_ids.is_empty() {
        return Ok(true);
    }
    let trade_space = ledger.trade_window_space();
    for symbol_id in symbol_ids {
        let missing = trade_space
            .with_symbol_rows(symbol_id, |rows| {
                has_incomplete_slot(rows, slot, start_idx, end_idx)
            })
            .map_err(WindowSpaceError::from)?;
        if missing {
            return Ok(true);
        }
    }
    Ok(false)
}

fn has_incomplete_slot(
    rows: &[TradeWindowRow],
    slot: TradeSlotKind,
    start_idx: WindowIndex,
    end_idx: WindowIndex,
) -> bool {
    if rows.is_empty() {
        return true;
    }
    let start = start_idx.min(end_idx) as usize;
    let end = end_idx.max(start_idx) as usize;
    if start >= rows.len() {
        return true;
    }
    let upper = end.min(rows.len() - 1);
    for idx in start..=upper {
        let status = trade_slot_status(&rows[idx], slot);
        if matches!(status, SlotStatus::Empty | SlotStatus::Pending) {
            return true;
        }
    }
    false
}

fn window_start_ns(
    ledger: &WindowSpaceController,
    window_idx: WindowIndex,
) -> Result<i64, FlatfileError> {
    ledger
        .window_meta(window_idx)
        .map(|meta| meta.start_ts)
        .ok_or(FlatfileError::MissingWindowMeta { window_idx })
}

fn trade_slot_status(row: &TradeWindowRow, kind: TradeSlotKind) -> SlotStatus {
    match kind {
        TradeSlotKind::RfRate => row.rf_rate.status,
        TradeSlotKind::OptionTrade => row.option_trade_ref.status,
        TradeSlotKind::OptionQuote => row.option_quote_ref.status,
        TradeSlotKind::UnderlyingTrade => row.underlying_trade_ref.status,
        TradeSlotKind::UnderlyingQuote => row.underlying_quote_ref.status,
        TradeSlotKind::OptionAggressor => row.option_aggressor_ref.status,
        TradeSlotKind::UnderlyingAggressor => row.underlying_aggressor_ref.status,
    }
}

fn enrichment_slot_status(row: &EnrichmentWindowRow, kind: EnrichmentSlotKind) -> SlotStatus {
    match kind {
        EnrichmentSlotKind::Greeks => row.greeks.status,
    }
}

fn ensure_slot_pending(
    ledger: &WindowSpaceController,
    key: &WindowKey,
    slot: SlotKind,
) -> Result<SlotStatus, FlatfileError> {
    let status = match slot {
        SlotKind::Trade(kind) => {
            let row = ledger.get_trade_row(&key.symbol, key.window_idx)?;
            trade_slot_status(&row, kind)
        }
        SlotKind::Enrichment(kind) => {
            let row = ledger.get_enrichment_row(&key.symbol, key.window_idx)?;
            enrichment_slot_status(&row, kind)
        }
    };
    if status != SlotStatus::Filled {
        ledger.mark_pending(&key.symbol, key.window_idx, slot)?;
    }
    Ok(status)
}

fn default_class_params() -> ClassParams {
    ClassParams {
        use_tick_rule_fallback: true,
        epsilon_price: DEFAULT_CLASSIFIER_EPSILON,
        allowed_lateness_ms: DEFAULT_CLASSIFIER_LATENESS_MS,
    }
}

impl OptionQuoteFlatfileEngine {
    pub fn new(config: FlatfileRuntimeConfig, ledger: Arc<WindowSpaceController>) -> Self {
        let inner = OptionQuoteInner::new(config, ledger);
        Self {
            inner: Arc::new(inner),
        }
    }
}
impl Engine for OptionQuoteFlatfileEngine {
    fn start(&self) -> EngineResult<()> {
        self.inner
            .config
            .ensure_dirs()
            .map_err(|err| EngineError::Failure { source: err.into() })?;
        let mut guard = self.inner.state.lock();
        if matches!(*guard, EngineRuntimeState::Running(_)) {
            return Err(EngineError::AlreadyRunning);
        }
        let runtime = Runtime::new().map_err(|err| EngineError::Failure { source: err.into() })?;
        let cancel = CancellationToken::new();
        let runner = Arc::clone(&self.inner);
        let cancel_clone = cancel.clone();
        let handle = runtime.spawn(async move {
            runner.run_quotes(cancel_clone).await;
        });
        *guard = EngineRuntimeState::Running(RuntimeBundle {
            runtime,
            handle,
            cancel,
        });
        info!(
            "[{}] option quote flatfile engine started",
            self.inner.config.label
        );
        Ok(())
    }

    fn stop(&self) -> EngineResult<()> {
        self.inner.stop("option quote flatfile")
    }

    fn health(&self) -> EngineHealth {
        self.inner.health()
    }

    fn describe_priority_hooks(&self) -> PriorityHookDescription {
        PriorityHookDescription {
            supports_priority_regions: false,
            notes: Some("Option quote flatfile ingestion".into()),
        }
    }
}

impl UnderlyingTradeFlatfileEngine {
    pub fn new(config: FlatfileRuntimeConfig, ledger: Arc<WindowSpaceController>) -> Self {
        let inner = UnderlyingTradeInner::new(config, ledger);
        Self {
            inner: Arc::new(inner),
        }
    }
}
impl Engine for UnderlyingTradeFlatfileEngine {
    fn start(&self) -> EngineResult<()> {
        self.inner
            .config
            .ensure_dirs()
            .map_err(|err| EngineError::Failure { source: err.into() })?;
        let mut guard = self.inner.state.lock();
        if matches!(*guard, EngineRuntimeState::Running(_)) {
            return Err(EngineError::AlreadyRunning);
        }
        let runtime = Runtime::new().map_err(|err| EngineError::Failure { source: err.into() })?;
        let cancel = CancellationToken::new();
        let runner = Arc::clone(&self.inner);
        let cancel_clone = cancel.clone();
        let handle = runtime.spawn(async move {
            runner.run_trades(cancel_clone).await;
        });
        *guard = EngineRuntimeState::Running(RuntimeBundle {
            runtime,
            handle,
            cancel,
        });
        info!(
            "[{}] underlying trade flatfile engine started",
            self.inner.config.label
        );
        Ok(())
    }

    fn stop(&self) -> EngineResult<()> {
        self.inner.stop("underlying trade flatfile")
    }

    fn health(&self) -> EngineHealth {
        self.inner.health()
    }

    fn describe_priority_hooks(&self) -> PriorityHookDescription {
        PriorityHookDescription {
            supports_priority_regions: false,
            notes: Some("Underlying trade flatfile ingestion".into()),
        }
    }
}

impl UnderlyingQuoteFlatfileEngine {
    pub fn new(config: FlatfileRuntimeConfig, ledger: Arc<WindowSpaceController>) -> Self {
        let inner = UnderlyingQuoteInner::new(config, ledger);
        Self {
            inner: Arc::new(inner),
        }
    }
}
impl Engine for UnderlyingQuoteFlatfileEngine {
    fn start(&self) -> EngineResult<()> {
        self.inner
            .config
            .ensure_dirs()
            .map_err(|err| EngineError::Failure { source: err.into() })?;
        let mut guard = self.inner.state.lock();
        if matches!(*guard, EngineRuntimeState::Running(_)) {
            return Err(EngineError::AlreadyRunning);
        }
        let runtime = Runtime::new().map_err(|err| EngineError::Failure { source: err.into() })?;
        let cancel = CancellationToken::new();
        let runner = Arc::clone(&self.inner);
        let cancel_clone = cancel.clone();
        let handle = runtime.spawn(async move {
            runner.run_quotes(cancel_clone).await;
        });
        *guard = EngineRuntimeState::Running(RuntimeBundle {
            runtime,
            handle,
            cancel,
        });
        info!(
            "[{}] underlying quote flatfile engine started",
            self.inner.config.label
        );
        Ok(())
    }

    fn stop(&self) -> EngineResult<()> {
        self.inner.stop("underlying quote flatfile")
    }

    fn health(&self) -> EngineHealth {
        self.inner.health()
    }

    fn describe_priority_hooks(&self) -> PriorityHookDescription {
        PriorityHookDescription {
            supports_priority_regions: false,
            notes: Some("Underlying quote flatfile ingestion".into()),
        }
    }
}
