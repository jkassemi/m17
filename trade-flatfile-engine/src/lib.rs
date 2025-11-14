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
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering as AtomicOrdering},
    },
    task::{Context, Poll},
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
use artifacts::{artifact_path, cleanup_partial_artifacts, write_record_batch};
use async_compression::tokio::bufread::GzipDecoder;
use aws_sdk_s3::{
    Client,
    config::{BehaviorVersion, Credentials, Region},
    primitives::ByteStream,
};
use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use core_types::{
    opra::parse_opra_contract,
    raw::{OptionTradeRecord, QuoteRecord, UnderlyingTradeRecord},
    schema::{nbbo_schema, option_trade_record_schema, underlying_trade_record_schema},
    types::{NbboState, Quality, Source},
    uid::{equity_trade_uid, option_trade_uid, quote_uid},
};
use csv_async::{AsyncReaderBuilder, ErrorKind as CsvErrorKind, Trim};
use engine_api::{
    Engine, EngineError, EngineHealth, EngineResult, HealthStatus, PriorityHookDescription,
};
use errors::FlatfileError;
use futures::StreamExt;
use log::{error, info, warn};
use parking_lot::Mutex;
use tokio::{
    io::{AsyncRead, BufReader, ReadBuf},
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
    mapping::{QuoteBatchPayload, TradeBatchPayload},
    payload::{PayloadMeta, PayloadType, SlotKind, SlotStatus, TradeSlotKind},
    window::WindowSpace,
};

const OPTION_TRADE_SCHEMA_VERSION: u8 = 1;
const QUOTE_SCHEMA_VERSION: u8 = 1;
const UNDERLYING_TRADE_SCHEMA_VERSION: u8 = 1;

pub struct OptionTradeFlatfileEngine {
    inner: Arc<OptionTradeInner>,
}

pub struct OptionQuoteFlatfileEngine {
    inner: Arc<OptionQuoteInner>,
}

pub struct UnderlyingTradeFlatfileEngine {
    inner: Arc<UnderlyingTradeInner>,
}

pub struct UnderlyingQuoteFlatfileEngine {
    inner: Arc<UnderlyingQuoteInner>,
}

struct OptionTradeInner {
    config: FlatfileRuntimeConfig,
    ledger: Arc<WindowSpaceController>,
    client: Client,
    state: Mutex<EngineRuntimeState>,
    batch_seq: AtomicU32,
    calendar: TradingCalendar,
    day_index: Arc<DayWindowIndex>,
}

struct OptionQuoteInner {
    config: FlatfileRuntimeConfig,
    ledger: Arc<WindowSpaceController>,
    client: Client,
    state: Mutex<EngineRuntimeState>,
    batch_seq: AtomicU32,
    calendar: TradingCalendar,
    day_index: Arc<DayWindowIndex>,
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

impl OptionTradeInner {
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
                    TradeSlotKind::OptionTrade,
                ) {
                    continue;
                }
                has_future_work = true;
                if let Err(err) = self.process_options_day(date, cancel.clone()).await {
                    error!("option trade day {} failed: {err}", date);
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
            "[{}] option trade flatfile engine exiting",
            self.config.label
        );
    }

    async fn process_options_day(
        &self,
        date: NaiveDate,
        cancel: CancellationToken,
    ) -> Result<(), FlatfileError> {
        let key = format!(
            "us_options_opra/trades_v1/{}/{:02}/{}-{:02}-{:02}.csv.gz",
            date.year(),
            date.month(),
            date.year(),
            date.month(),
            date.day()
        );
        info!(
            "[{}] ingesting option trades from {}",
            self.config.label, key
        );
        let (stream, token) =
            fetch_stream(&self.client, &self.config, &key, TradeSlotKind::OptionTrade).await?;
        let result = self.consume_option_trades(date, stream, cancel).await;
        drop(token);
        result
    }

    async fn consume_option_trades<R>(
        &self,
        date: NaiveDate,
        reader: R,
        cancel: CancellationToken,
    ) -> Result<(), FlatfileError>
    where
        R: AsyncRead + Unpin + Send,
    {
        let buf = BufReader::new(reader);
        let mut csv_reader = AsyncReaderBuilder::new().trim(Trim::All).create_reader(buf);
        let mut records = csv_reader.records();
        let mut tracker = SymbolTracker::new();
        let mut processed_rows = 0usize;
        let mut last_progress = Instant::now();
        let interval = Duration::from_millis(self.config.progress_update_ms.max(10));
        while let Some(record) = records.next().await {
            if cancel.is_cancelled() {
                info!(
                    "[{}] option trade ingestion cancelled while streaming {}",
                    self.config.label, date
                );
                return Ok(());
            }
            let record = match record {
                Ok(record) => record,
                Err(err) => {
                    log_streaming_error(self.config.label, "option trades", date, &err);
                    return Err(err.into());
                }
            };
            if record.len() < 8 {
                continue;
            }
            if let Some((_contract, trade)) = parse_option_trade_row(&record) {
                let underlying = trade.underlying.clone();
                if underlying.is_empty() || !self.config.allows_underlying(&underlying) {
                    continue;
                }
                let symbol = underlying;
                let ts = trade.timestamp_ns();
                let window_idx = match self.window_idx_for_timestamp(ts) {
                    Some(idx) => idx,
                    None => {
                        warn!("option trade timestamp {} fell outside window", ts);
                        continue;
                    }
                };
                tracker.with_bucket(
                    symbol.as_str(),
                    window_idx,
                    |sym, idx| self.open_option_trade_window(date, sym, idx),
                    |state| self.finalize_option_trade_state(date, state),
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
                        "[{}] streamed {} option trades for {}",
                        self.config.label, processed_rows, date
                    );
                    last_progress = Instant::now();
                }
            }
        }
        if cancel.is_cancelled() {
            info!(
                "[{}] option trade ingestion cancelled before persisting {}",
                self.config.label, date
            );
            return Ok(());
        }
        tracker.finish(|state| self.finalize_option_trade_state(date, state))?;
        Ok(())
    }

    fn persist_option_trade_bucket(
        &self,
        date: NaiveDate,
        key: WindowKey,
        bucket: WindowBucket<OptionTradeRecord>,
    ) -> Result<(), FlatfileError> {
        let row = self.ledger.get_trade_row(&key.symbol, key.window_idx)?;
        if row.option_trade_ref.status == SlotStatus::Filled {
            return Ok(());
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
        Ok(())
    }

    fn finalize_option_trade_state(
        &self,
        date: NaiveDate,
        state: SymbolWindow<OptionTradeRecord>,
    ) -> Result<(), FlatfileError> {
        if let Some(bucket) = state.bucket {
            let key = WindowKey {
                symbol: state.symbol,
                window_idx: state.window_idx,
            };
            self.persist_option_trade_bucket(date, key, bucket)?;
        }
        Ok(())
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
                    TradeSlotKind::OptionQuote,
                ) {
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

    async fn process_option_quotes_day(
        &self,
        date: NaiveDate,
        cancel: CancellationToken,
    ) -> Result<(), FlatfileError> {
        let key = format!(
            "us_options_opra/quotes_v1/{}/{:02}/{}-{:02}-{:02}.csv.gz",
            date.year(),
            date.month(),
            date.year(),
            date.month(),
            date.day()
        );
        info!(
            "[{}] ingesting option quotes from {}",
            self.config.label, key
        );
        let (stream, token) =
            fetch_stream(&self.client, &self.config, &key, TradeSlotKind::OptionQuote).await?;
        let result = self.consume_option_quotes(date, stream, cancel).await;
        drop(token);
        result
    }

    async fn consume_option_quotes<R>(
        &self,
        date: NaiveDate,
        reader: R,
        cancel: CancellationToken,
    ) -> Result<(), FlatfileError>
    where
        R: AsyncRead + Unpin + Send,
    {
        let buf = BufReader::new(reader);
        let mut csv_reader = AsyncReaderBuilder::new().trim(Trim::All).create_reader(buf);
        let mut records = csv_reader.records();
        let mut tracker = SymbolTracker::new();
        let mut processed_rows = 0usize;
        let mut last_progress = Instant::now();
        let interval = Duration::from_millis(self.config.progress_update_ms.max(10));
        while let Some(record) = records.next().await {
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
            if let Some((_contract, quote, underlying)) =
                parse_quote_row(&record, QuoteSource::Option)
            {
                let Some(base) = underlying else {
                    continue;
                };
                if !self.config.allows_underlying(&base) {
                    continue;
                }
                let ts = quote.timestamp_ns();
                let window_idx = match self.window_idx_for_timestamp(ts) {
                    Some(idx) => idx,
                    None => continue,
                };
                tracker.with_bucket(
                    &base,
                    window_idx,
                    |sym, idx| self.open_option_quote_window(date, sym, idx),
                    |state| self.finalize_option_quote_state(date, state),
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
                        "[{}] streamed {} option quotes for {}",
                        self.config.label, processed_rows, date
                    );
                    last_progress = Instant::now();
                }
            }
        }
        if cancel.is_cancelled() {
            info!(
                "[{}] option quote ingestion cancelled before persisting {}",
                self.config.label, date
            );
            return Ok(());
        }
        tracker.finish(|state| self.finalize_option_quote_state(date, state))?;
        Ok(())
    }

    fn persist_option_quote_bucket(
        &self,
        date: NaiveDate,
        key: WindowKey,
        bucket: WindowBucket<QuoteRecord>,
    ) -> Result<(), FlatfileError> {
        let row = self.ledger.get_trade_row(&key.symbol, key.window_idx)?;
        if row.option_quote_ref.status == SlotStatus::Filled {
            return Ok(());
        }
        self.ledger.mark_pending(
            &key.symbol,
            key.window_idx,
            SlotKind::Trade(TradeSlotKind::OptionQuote),
        )?;
        let batch = quote_batch(&bucket.records)?;
        let relative_path = artifact_path(
            &self.config,
            "options/quotes",
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
        if let Err(err) = self
            .ledger
            .set_option_quote_ref(&key.symbol, key.window_idx, meta, None)
        {
            self.ledger.clear_slot(
                &key.symbol,
                key.window_idx,
                SlotKind::Trade(TradeSlotKind::OptionQuote),
            )?;
            return Err(err.into());
        }
        Ok(())
    }

    fn finalize_option_quote_state(
        &self,
        date: NaiveDate,
        state: SymbolWindow<QuoteRecord>,
    ) -> Result<(), FlatfileError> {
        if let Some(bucket) = state.bucket {
            let key = WindowKey {
                symbol: state.symbol,
                window_idx: state.window_idx,
            };
            self.persist_option_quote_bucket(date, key, bucket)?;
        }
        Ok(())
    }

    fn open_option_quote_window(
        &self,
        date: NaiveDate,
        underlying: &str,
        window_idx: WindowIndex,
    ) -> Result<SymbolWindow<QuoteRecord>, FlatfileError> {
        let key = WindowKey {
            symbol: underlying.to_string(),
            window_idx,
        };
        let status = ensure_slot_pending(
            &self.ledger,
            &key,
            SlotKind::Trade(TradeSlotKind::OptionQuote),
        )?;
        if status == SlotStatus::Filled {
            return Ok(SymbolWindow::skipped(underlying, window_idx));
        }
        if status == SlotStatus::Pending {
            let relative_path = artifact_path(
                &self.config,
                "options/quotes",
                date,
                underlying,
                window_idx,
                "parquet",
            );
            cleanup_partial_artifacts(&self.config, &relative_path)?;
        }
        Ok(SymbolWindow::writable(underlying, window_idx))
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
        let buf = BufReader::new(reader);
        let mut csv_reader = AsyncReaderBuilder::new().trim(Trim::All).create_reader(buf);
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
        let buf = BufReader::new(reader);
        let mut csv_reader = AsyncReaderBuilder::new().trim(Trim::All).create_reader(buf);
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
    let resp = client
        .get_object()
        .bucket(config.bucket.clone())
        .key(key)
        .send()
        .await
        .map_err(|err| FlatfileError::Sdk(err.to_string()))?;
    let total_bytes = resp.content_length().map(|val| val as u64);
    config.download_metrics.reset(slot, total_bytes);
    let body: ByteStream = resp.body;
    let reader = body.into_async_read();
    let counting_reader = CountingReader::new(reader, Arc::clone(&config.download_metrics), slot);
    let buf = BufReader::new(counting_reader);
    let decoder = GzipDecoder::new(buf);
    let token = DownloadToken::new(Arc::clone(&config.download_metrics), slot);
    Ok((Box::new(decoder), token))
}

struct CountingReader<R> {
    inner: R,
    metrics: Arc<DownloadMetrics>,
    slot: TradeSlotKind,
}

impl<R> CountingReader<R> {
    fn new(inner: R, metrics: Arc<DownloadMetrics>, slot: TradeSlotKind) -> Self {
        Self {
            inner,
            metrics,
            slot,
        }
    }
}

impl<R> AsyncRead for CountingReader<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &poll {
            let after = buf.filled().len();
            let read = after.saturating_sub(before);
            if read > 0 {
                self.metrics.add_streamed(self.slot, read as u64);
            }
        }
        poll
    }
}

struct DownloadToken {
    metrics: Arc<DownloadMetrics>,
    slot: TradeSlotKind,
}

impl DownloadToken {
    fn new(metrics: Arc<DownloadMetrics>, slot: TradeSlotKind) -> Self {
        Self { metrics, slot }
    }
}

impl Drop for DownloadToken {
    fn drop(&mut self) {
        self.metrics.complete(self.slot);
    }
}

fn parse_option_trade_row(record: &csv_async::StringRecord) -> Option<(String, OptionTradeRecord)> {
    let contract = record.get(0)?.to_string();
    let (contract_direction, strike_price, expiry_ts_ns, underlying) =
        parse_opra_contract(&contract, record.get(6).and_then(|s| s.parse::<i64>().ok()));
    let conditions = parse_conditions_field(record.get(1).unwrap_or_default());
    let exchange = record.get(3)?.parse().unwrap_or(0);
    let participant_ts_ns = record.get(4).and_then(|s| s.parse::<i64>().ok());
    let price = record.get(5)?.parse().unwrap_or(0.0);
    let trade_ts_ns = record.get(6)?.parse().unwrap_or(0);
    let size = record.get(7)?.parse().unwrap_or(0);
    let trade_uid = option_trade_uid(
        &contract,
        trade_ts_ns,
        participant_ts_ns,
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
        participant_ts_ns,
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
        assert_eq!(first.contract, "O:SPY230327P00390000");
        assert_eq!(first.price, 11.82);
        assert_eq!(first.size, 1);
        assert_eq!(first.exchange, 312);
        assert_eq!(first.contract_direction, 'P');
        assert_eq!(first.underlying, "SPY");
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

#[derive(Hash, Eq, PartialEq, Clone)]
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
    ledger.mark_pending(&key.symbol, key.window_idx, slot)?;
    Ok(status)
}

impl OptionTradeFlatfileEngine {
    pub fn new(config: FlatfileRuntimeConfig, ledger: Arc<WindowSpaceController>) -> Self {
        let inner = OptionTradeInner::new(config, ledger);
        Self {
            inner: Arc::new(inner),
        }
    }
}
impl Engine for OptionTradeFlatfileEngine {
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
            "[{}] option trade flatfile engine started",
            self.inner.config.label
        );
        Ok(())
    }

    fn stop(&self) -> EngineResult<()> {
        self.inner.stop("option trade flatfile")
    }

    fn health(&self) -> EngineHealth {
        self.inner.health()
    }

    fn describe_priority_hooks(&self) -> PriorityHookDescription {
        PriorityHookDescription {
            supports_priority_regions: false,
            notes: Some("Option trade flatfile ingestion".into()),
        }
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
