// Copyright (c) James Kassemi, SC, US. All rights reserved.
use async_compression::tokio::bufread::GzipDecoder;
use async_trait::async_trait;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use bytes::Bytes;
use chrono::{DateTime, Datelike, NaiveDate, TimeDelta, Utc};
use core_types::config::FlatfileConfig;
use core_types::opra::parse_opra_contract;
use core_types::retry::RetryPolicy;
use core_types::types::{
    AggressorSide, ClassMethod, Completeness, DataBatch, DataBatchMeta, EquityTrade, Nbbo,
    OptionTrade, Quality, QueryScope, Source, Watermark,
};
use csv_async::AsyncReaderBuilder;
use futures::{Stream, StreamExt};
use log::info;
use metrics::Metrics;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader, ReadBuf};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration, Instant};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::{ReaderStream, StreamReader};
use trading_calendar::{Market, TradingCalendar};

/// Combined source trait merging ObjectStore, UpdateLoop, and Queryable.
#[async_trait]
pub trait SourceTrait: Send + Sync + 'static {
    async fn get_stream(
        &self,
        path: &str,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
        Box<dyn std::error::Error + Send + Sync>,
    >;

    async fn object_len(&self, path: &str) -> Option<u64>;

    async fn run(&self);

    fn status(&self) -> String;

    async fn get_option_trades(
        &self,
        scope: QueryScope,
    ) -> Pin<Box<dyn Stream<Item = DataBatch<OptionTrade>> + Send>>;

    async fn get_equity_trades(
        &self,
        scope: QueryScope,
    ) -> Pin<Box<dyn Stream<Item = DataBatch<EquityTrade>> + Send>>;

    async fn get_nbbo(
        &self,
        scope: QueryScope,
    ) -> Pin<Box<dyn Stream<Item = DataBatch<Nbbo>> + Send>>;

    async fn get_option_nbbo(
        &self,
        scope: QueryScope,
    ) -> Pin<Box<dyn Stream<Item = DataBatch<Nbbo>> + Send>>;
}

#[derive(Clone)]
pub struct FlatfileSource {
    status: Arc<Mutex<String>>,
    config: Arc<FlatfileConfig>,
    client: Client,
    metrics: Option<Arc<Metrics>>,
    ingest_batch_size: usize,
    progress_update_ms: u64,
    retry: RetryPolicy,
}

impl FlatfileSource {
    pub async fn new(
        config: Arc<FlatfileConfig>,
        metrics: Option<Arc<Metrics>>,
        ingest_batch_size: usize,
        progress_update_ms: u64,
    ) -> Self {
        let credentials = Credentials::new(
            config.massive_access_key_id.clone(),
            config.massive_secret_access_key.clone(),
            None,
            None,
            "flatfile",
        );

        let s3_config = aws_sdk_s3::Config::builder()
            .endpoint_url(config.massive_flatfiles_endpoint.clone())
            .region(Region::new("custom"))
            .credentials_provider(credentials)
            .behavior_version(BehaviorVersion::latest())
            .force_path_style(true)
            .build();

        let client = Client::from_conf(s3_config);

        Self {
            status: Arc::new(Mutex::new("Initializing".to_string())),
            config,
            client,
            metrics,
            ingest_batch_size,
            progress_update_ms,
            retry: RetryPolicy::default_network(),
        }
    }

    fn nbbo_stream_for_dataset(
        &self,
        scope: QueryScope,
        dataset: &'static str,
        label: &'static str,
    ) -> Pin<Box<dyn Stream<Item = DataBatch<Nbbo>> + Send>> {
        let (tx, rx) = mpsc::channel(100);
        let self_clone = self.clone();
        let scope_clone = scope.clone();
        tokio::spawn(async move {
            let calendar = TradingCalendar::new(Market::NYSE).unwrap();
            let start_ts = scope_clone.time_range.0;
            let end_ts = scope_clone.time_range.1;
            let start_dt: DateTime<Utc> = DateTime::from_timestamp(
                start_ts / 1_000_000_000,
                (start_ts % 1_000_000_000) as u32,
            )
            .unwrap_or(Utc::now());
            let end_dt: DateTime<Utc> =
                DateTime::from_timestamp(end_ts / 1_000_000_000, (end_ts % 1_000_000_000) as u32)
                    .unwrap_or(Utc::now());
            let mut current_date = start_dt.date_naive();
            let end_date = end_dt.date_naive();
            while current_date <= end_date {
                if calendar.is_trading_day(current_date).unwrap_or(false) {
                    let path = build_quote_path(dataset, current_date);
                    info!("Checking path for {} quotes: {}", label, path);
                    process_nbbo_stream(
                        &self_clone,
                        &path,
                        &scope_clone,
                        tx.clone(),
                        self_clone.ingest_batch_size,
                        self_clone.progress_update_ms,
                        self_clone.metrics.clone(),
                    )
                    .await;
                }
                current_date += TimeDelta::try_days(1).unwrap();
            }
        });
        Box::pin(ReceiverStream::new(rx))
    }

    async fn download_file(
        &self,
        path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let local_dir = std::path::PathBuf::from("data");
        let local_path = local_dir.join(path);
        fs::create_dir_all(local_path.parent().unwrap()).await?;
        let resp = self
            .retry
            .retry_async(|_| async {
                self.client
                    .get_object()
                    .bucket(self.config.massive_flatfiles_bucket.clone())
                    .key(path)
                    .send()
                    .await
            })
            .await?;
        let data = resp.body.collect().await?;
        fs::write(&local_path, data.into_bytes()).await?;
        Ok(())
    }

    async fn load_processed(
        &self,
    ) -> Result<HashSet<String>, Box<dyn std::error::Error + Send + Sync>> {
        let state_path = std::path::PathBuf::from("flatfile_state.txt");
        if !fs::try_exists(&state_path).await? {
            fs::write(&state_path, b"").await?;
        }
        let file = File::open(&state_path).await?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut set = HashSet::new();
        while let Some(line) = lines.next_line().await? {
            set.insert(line);
        }
        Ok(set)
    }

    async fn append_processed(
        &self,
        path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let state_path = std::path::PathBuf::from("flatfile_state.txt");
        let mut file = OpenOptions::new().append(true).open(&state_path).await?;
        file.write_all((path.to_string() + "\n").as_bytes()).await?;
        Ok(())
    }
}

#[async_trait]
impl SourceTrait for FlatfileSource {
    async fn get_stream(
        &self,
        path: &str,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let local_dir = std::path::PathBuf::from("data");
        let local_path = local_dir.join(path);
        if fs::try_exists(&local_path).await? {
            let file = File::open(&local_path).await?;
            let reader = BufReader::new(file);
            let stream: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>;
            if path.ends_with(".gz") {
                let decoder = GzipDecoder::new(reader);
                stream = Box::pin(ReaderStream::new(decoder));
            } else {
                stream = Box::pin(ReaderStream::new(reader));
            }
            Ok(stream)
        } else {
            let resp = self
                .retry
                .retry_async(|_| async {
                    self.client
                        .get_object()
                        .bucket(self.config.massive_flatfiles_bucket.clone())
                        .key(path)
                        .send()
                        .await
                })
                .await?;
            let body: ByteStream = resp.body;
            let reader = body.into_async_read();
            let stream: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>;
            if path.ends_with(".gz") {
                let buf_reader = BufReader::new(reader);
                let decoder = GzipDecoder::new(buf_reader);
                stream = Box::pin(ReaderStream::new(decoder));
            } else {
                let buf_reader = BufReader::new(reader);
                stream = Box::pin(ReaderStream::new(buf_reader));
            }
            Ok(stream)
        }
    }

    async fn object_len(&self, path: &str) -> Option<u64> {
        let local_dir = std::path::PathBuf::from("data");
        let local_path = local_dir.join(path);
        if fs::try_exists(&local_path).await.ok()? {
            if let Ok(meta) = fs::metadata(&local_path).await {
                return Some(meta.len());
            }
            return None;
        }
        match self
            .client
            .head_object()
            .bucket(self.config.massive_flatfiles_bucket.clone())
            .key(path)
            .send()
            .await
        {
            Ok(resp) => resp.content_length().map(|v| v as u64),
            Err(_) => None,
        }
    }

    async fn run(&self) {
        loop {
            let mut status_parts = Vec::new();
            let calendar = TradingCalendar::new(Market::NYSE).unwrap();
            let mut processed = self.load_processed().await.unwrap_or_default();
            for range in self.config.date_ranges.iter() {
                let start_ts = range.start_ts_ns().unwrap_or(0);
                let end_ts_opt = range
                    .end_ts_ns()
                    .unwrap_or_else(|_| Some(Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX)));
                let end_ts =
                    end_ts_opt.unwrap_or(Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX));
                let start_dt: DateTime<Utc> = DateTime::from_timestamp(
                    start_ts / 1_000_000_000,
                    (start_ts % 1_000_000_000) as u32,
                )
                .unwrap_or(Utc::now());
                let end_dt: DateTime<Utc> = DateTime::from_timestamp(
                    end_ts / 1_000_000_000,
                    (end_ts % 1_000_000_000) as u32,
                )
                .unwrap_or(Utc::now());
                let mut current_date = start_dt.date_naive();
                let end_date = end_dt.date_naive();
                let now_date = Utc::now().date_naive();
                while current_date <= end_date {
                    if calendar.is_trading_day(current_date).unwrap_or(false) {
                        let year = current_date.year();
                        let month = current_date.month();
                        let day = current_date.day();
                        let path = format!(
                            "us_stocks_sip/trades_v1/{}/{:02}/{}-{:02}-{:02}.csv.gz",
                            year, month, year, month, day
                        );
                        let exists = self
                            .client
                            .head_object()
                            .bucket(self.config.massive_flatfiles_bucket.clone())
                            .key(&path)
                            .send()
                            .await
                            .is_ok();
                        if exists {
                            if !processed.contains(&path) {
                                if let Ok(_) = self.download_file(&path).await {
                                    processed.insert(path.clone());
                                    let _ = self.append_processed(&path).await;
                                }
                            }
                        } else if current_date < now_date {
                            status_parts.push(format!("Missing: {}", current_date));
                        }
                    }
                    current_date += TimeDelta::try_days(1).unwrap();
                }
            }
            let status = if status_parts.is_empty() {
                "All files present and processed".to_string()
            } else {
                format!("Missing files: {}", status_parts.join(", "))
            };
            *self.status.lock().unwrap() = status;

            sleep(Duration::from_secs(300)).await; // Check every 5 minutes
        }
    }

    fn status(&self) -> String {
        self.status.lock().unwrap().clone()
    }

    async fn get_option_trades(
        &self,
        scope: QueryScope,
    ) -> Pin<Box<dyn Stream<Item = DataBatch<OptionTrade>> + Send>> {
        let (tx, rx) = mpsc::channel(100);
        let self_clone = self.clone();
        let scope_clone = scope.clone();
        tokio::spawn(async move {
            let calendar = TradingCalendar::new(Market::NYSE).unwrap();
            let start_ts = scope_clone.time_range.0;
            let end_ts = scope_clone.time_range.1;
            let start_dt: DateTime<Utc> = DateTime::from_timestamp(
                start_ts / 1_000_000_000,
                (start_ts % 1_000_000_000) as u32,
            )
            .unwrap_or(Utc::now());
            let end_dt: DateTime<Utc> =
                DateTime::from_timestamp(end_ts / 1_000_000_000, (end_ts % 1_000_000_000) as u32)
                    .unwrap_or(Utc::now());
            let mut current_date = start_dt.date_naive();
            let end_date = end_dt.date_naive();
            while current_date <= end_date {
                if calendar.is_trading_day(current_date).unwrap_or(false) {
                    let year = current_date.year();
                    let month = current_date.month();
                    let day = current_date.day();
                    // OPRA options trades layout
                    let path = format!(
                        "us_options_opra/trades_v1/{}/{:02}/{}-{:02}-{:02}.csv.gz",
                        year, month, year, month, day
                    );
                    info!("Checking path for option trades: {}", path);
                    process_option_trades_stream(
                        &self_clone,
                        &path,
                        &scope_clone,
                        tx.clone(),
                        self_clone.ingest_batch_size,
                        self_clone.progress_update_ms,
                        self_clone.metrics.clone(),
                    )
                    .await;
                }
                current_date += TimeDelta::try_days(1).unwrap();
            }
        });
        Box::pin(ReceiverStream::new(rx))
    }

    async fn get_equity_trades(
        &self,
        scope: QueryScope,
    ) -> Pin<Box<dyn Stream<Item = DataBatch<EquityTrade>> + Send>> {
        let (tx, rx) = mpsc::channel(100);
        let self_clone = self.clone();
        let scope_clone = scope.clone();
        tokio::spawn(async move {
            let calendar = TradingCalendar::new(Market::NYSE).unwrap();
            let start_ts = scope_clone.time_range.0;
            let end_ts = scope_clone.time_range.1;
            let start_dt: DateTime<Utc> = DateTime::from_timestamp(
                start_ts / 1_000_000_000,
                (start_ts % 1_000_000_000) as u32,
            )
            .unwrap_or(Utc::now());
            let end_dt: DateTime<Utc> =
                DateTime::from_timestamp(end_ts / 1_000_000_000, (end_ts % 1_000_000_000) as u32)
                    .unwrap_or(Utc::now());
            let mut current_date = start_dt.date_naive();
            let end_date = end_dt.date_naive();
            while current_date <= end_date {
                if calendar.is_trading_day(current_date).unwrap_or(false) {
                    let year = current_date.year();
                    let month = current_date.month();
                    let day = current_date.day();
                    let path = format!(
                        "us_stocks_sip/trades_v1/{}/{:02}/{}-{:02}-{:02}.csv.gz",
                        year, month, year, month, day
                    );
                    info!("Checking path for equity trades: {}", path);
                    process_equity_trades_stream(
                        &self_clone,
                        &path,
                        &scope_clone,
                        tx.clone(),
                        self_clone.ingest_batch_size,
                        self_clone.progress_update_ms,
                        self_clone.metrics.clone(),
                    )
                    .await;
                }
                current_date += TimeDelta::try_days(1).unwrap();
            }
        });
        Box::pin(ReceiverStream::new(rx))
    }

    async fn get_nbbo(
        &self,
        scope: QueryScope,
    ) -> Pin<Box<dyn Stream<Item = DataBatch<Nbbo>> + Send>> {
        self.nbbo_stream_for_dataset(scope, "us_stocks_sip/quotes_v1", "equity")
    }

    async fn get_option_nbbo(
        &self,
        scope: QueryScope,
    ) -> Pin<Box<dyn Stream<Item = DataBatch<Nbbo>> + Send>> {
        self.nbbo_stream_for_dataset(scope, "us_options_opra/quotes_v1", "options")
    }
}

struct CountingReader<R> {
    inner: R,
    counter: Arc<AtomicU64>,
}

impl<R> CountingReader<R> {
    fn new(inner: R, counter: Arc<AtomicU64>) -> Self {
        Self { inner, counter }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for CountingReader<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let pre = buf.filled().len();
        let poll = std::pin::Pin::new(&mut self.inner).poll_read(cx, buf);
        if let std::task::Poll::Ready(Ok(())) = &poll {
            let post = buf.filled().len();
            let read = (post - pre) as u64;
            if read > 0 {
                self.counter.fetch_add(read, Ordering::Relaxed);
            }
        }
        poll
    }
}

async fn process_equity_trades_stream<S: SourceTrait>(
    source: &S,
    path: &str,
    scope: &QueryScope,
    tx: mpsc::Sender<DataBatch<EquityTrade>>,
    batch_size: usize,
    progress_update_ms: u64,
    metrics: Option<Arc<Metrics>>,
) {
    info!("Attempting to get stream for path: {}", path);
    match source.get_stream(path).await {
        Ok(stream) => {
            let total_len = source.object_len(path).await;
            if let Some(m) = metrics.as_ref() {
                m.set_current_file(path.to_string(), total_len.unwrap_or(0));
            }
            let bytes_read = Arc::new(AtomicU64::new(0));
            let reader = StreamReader::new(stream);
            let counting = CountingReader::new(reader, bytes_read.clone());
            let buf = BufReader::new(counting);
            // Streams returned by `get_stream` are already appropriately decoded
            // based on file extension. Avoid double-decompression here.
            let mut csv_reader = AsyncReaderBuilder::new().create_reader(buf);
            let mut records = csv_reader.records();
            let instruments = scope.instruments.clone();
            let mut batch = Vec::with_capacity(batch_size);
            let mut record_count = 0usize;
            let mut batch_count = 0usize;
            let mut last_update = Instant::now();

            while let Some(rec) = records.next().await {
                if let Ok(record) = rec {
                    record_count += 1;
                    let symbol = record[0].to_string();
                    if !instruments.is_empty() && !instruments.contains(&symbol) {
                        // update progress periodically even if filtered out
                        if let Some(m) = metrics.as_ref() {
                            if last_update.elapsed() >= Duration::from_millis(progress_update_ms) {
                                m.set_current_file_read(bytes_read.load(Ordering::Relaxed));
                                last_update = Instant::now();
                            }
                        }
                        continue;
                    }
                    let trade = EquityTrade {
                        symbol,
                        trade_ts_ns: record[5].parse().unwrap_or(0),
                        price: record[6].parse().unwrap_or(0.0),
                        size: record[9].parse().unwrap_or(0),
                        conditions: record[1]
                            .split(',')
                            .filter_map(|s| s.trim().parse().ok())
                            .collect(),
                        exchange: record[3].parse().unwrap_or(0),
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
                        source: Source::Flatfile,
                        quality: Quality::Prelim,
                        watermark_ts_ns: 0,
                        trade_id: Some(record[4].to_string()),
                        seq: Some(record[7].parse().unwrap_or(0)),
                        participant_ts_ns: Some(record[8].parse().unwrap_or(0)),
                        tape: Some(record[10].to_string()),
                        correction: Some(record[2].parse().unwrap_or(0)),
                        trf_id: Some(record[11].to_string()),
                        trf_ts_ns: Some(record[12].parse().unwrap_or(0)),
                    };
                    batch.push(trade);
                    if batch.len() >= batch_size {
                        batch_count += 1;
                        let meta = DataBatchMeta {
                            source: Source::Flatfile,
                            quality: Quality::Prelim,
                            watermark: Watermark {
                                watermark_ts_ns: 0, // Placeholder
                                completeness: Completeness::Complete,
                                hints: None,
                            },
                            schema_version: 1,
                        };
                        let _ = tx.try_send(DataBatch {
                            rows: std::mem::take(&mut batch),
                            meta,
                        });
                    }
                }
                if let Some(m) = metrics.as_ref() {
                    if last_update.elapsed() >= Duration::from_millis(progress_update_ms) {
                        m.set_current_file_read(bytes_read.load(Ordering::Relaxed));
                        last_update = Instant::now();
                    }
                }
            }

            if !batch.is_empty() {
                batch_count += 1;
                let meta = DataBatchMeta {
                    source: Source::Flatfile,
                    quality: Quality::Prelim,
                    watermark: Watermark {
                        watermark_ts_ns: 0, // Placeholder
                        completeness: Completeness::Complete,
                        hints: None,
                    },
                    schema_version: 1,
                };
                let _ = tx.try_send(DataBatch { rows: batch, meta });
            }

            if let Some(m) = metrics.as_ref() {
                if let Some(total) = total_len {
                    let read = bytes_read.load(Ordering::Relaxed);
                    if read < total {
                        m.set_current_file_read(total);
                    } else {
                        m.set_current_file_read(read);
                    }
                } else {
                    m.set_current_file_read(bytes_read.load(Ordering::Relaxed));
                }
            }

            info!(
                "Processed {} records into {} batches for path: {}",
                record_count, batch_count, path
            );
        }
        Err(e) => {
            info!("Failed to get stream for path: {}: {:?}", path, e);
        }
    }
}

async fn process_nbbo_stream<S: SourceTrait>(
    source: &S,
    path: &str,
    scope: &QueryScope,
    tx: mpsc::Sender<DataBatch<Nbbo>>,
    batch_size: usize,
    progress_update_ms: u64,
    metrics: Option<Arc<Metrics>>,
) {
    info!("Attempting to get stream for path: {}", path);
    match source.get_stream(path).await {
        Ok(stream) => {
            let total_len = source.object_len(path).await;
            if let Some(m) = metrics.as_ref() {
                m.set_current_file(path.to_string(), total_len.unwrap_or(0));
            }
            let bytes_read = Arc::new(AtomicU64::new(0));
            let reader = StreamReader::new(stream);
            let counting = CountingReader::new(reader, bytes_read.clone());
            let buf = BufReader::new(counting);
            let mut csv_reader = AsyncReaderBuilder::new().create_reader(buf);
            let mut records = csv_reader.records();
            let instruments = scope.instruments.clone();
            let mut batch: Vec<Nbbo> = Vec::with_capacity(batch_size);
            let mut last_update = Instant::now();

            while let Some(rec) = records.next().await {
                if let Ok(record) = rec {
                    // Ticker,ask_exchange,ask_price,ask_size,bid_exchange,bid_price,bid_size,conditions,indicators,participant_timestamp,sequence_number,sip_timestamp,tape,trf_timestamp
                    let symbol = record[0].to_string();
                    if !instruments.is_empty() && !instruments.contains(&symbol) {
                        if let Some(m) = metrics.as_ref() {
                            if last_update.elapsed() >= Duration::from_millis(progress_update_ms) {
                                m.set_current_file_read(bytes_read.load(Ordering::Relaxed));
                                last_update = Instant::now();
                            }
                        }
                        continue;
                    }

                    let ask_ex: i32 = record[1].parse().unwrap_or(0);
                    let ask: f64 = record[2].parse().unwrap_or(0.0);
                    let ask_sz: u32 = record[3].parse().unwrap_or(0);
                    let bid_ex: i32 = record[4].parse().unwrap_or(0);
                    let bid: f64 = record[5].parse().unwrap_or(0.0);
                    let bid_sz: u32 = record[6].parse().unwrap_or(0);
                    let cond_opt: Option<i32> =
                        record
                            .get(7)
                            .and_then(|s| if s.is_empty() { None } else { s.parse().ok() });
                    let sip_ts: i64 = record[11].parse().unwrap_or(0);
                    // Infer state
                    let state = if ask > 0.0 {
                        if bid > ask {
                            core_types::types::NbboState::Crossed
                        } else if (bid - ask).abs() < f64::EPSILON {
                            core_types::types::NbboState::Locked
                        } else {
                            core_types::types::NbboState::Normal
                        }
                    } else {
                        core_types::types::NbboState::Normal
                    };

                    let nbbo = Nbbo {
                        instrument_id: symbol,
                        quote_ts_ns: sip_ts,
                        bid,
                        ask,
                        bid_sz,
                        ask_sz,
                        state,
                        condition: cond_opt,
                        best_bid_venue: Some(bid_ex),
                        best_ask_venue: Some(ask_ex),
                        source: Source::Flatfile,
                        quality: Quality::Prelim,
                        watermark_ts_ns: 0,
                    };
                    batch.push(nbbo);
                    if batch.len() >= batch_size {
                        let meta = DataBatchMeta {
                            source: Source::Flatfile,
                            quality: Quality::Prelim,
                            watermark: Watermark {
                                watermark_ts_ns: 0,
                                completeness: Completeness::Complete,
                                hints: None,
                            },
                            schema_version: 1,
                        };
                        let _ = tx.try_send(DataBatch {
                            rows: std::mem::take(&mut batch),
                            meta,
                        });
                    }
                }
                if let Some(m) = metrics.as_ref() {
                    if last_update.elapsed() >= Duration::from_millis(progress_update_ms) {
                        m.set_current_file_read(bytes_read.load(Ordering::Relaxed));
                        last_update = Instant::now();
                    }
                }
            }

            if !batch.is_empty() {
                let meta = DataBatchMeta {
                    source: Source::Flatfile,
                    quality: Quality::Prelim,
                    watermark: Watermark {
                        watermark_ts_ns: 0,
                        completeness: Completeness::Complete,
                        hints: None,
                    },
                    schema_version: 1,
                };
                let _ = tx.try_send(DataBatch { rows: batch, meta });
            }

            if let Some(m) = metrics.as_ref() {
                if let Some(total) = total_len {
                    let read = bytes_read.load(Ordering::Relaxed);
                    if read < total {
                        m.set_current_file_read(total);
                    } else {
                        m.set_current_file_read(read);
                    }
                } else {
                    m.set_current_file_read(bytes_read.load(Ordering::Relaxed));
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to get stream for {}: {}", path, e);
        }
    }
}

fn build_quote_path(dataset: &str, date: NaiveDate) -> String {
    let year = date.year();
    let month = date.month();
    let day = date.day();
    format!(
        "{}/{}/{:02}/{}-{:02}-{:02}.csv.gz",
        dataset, year, month, year, month, day
    )
}
async fn process_option_trades_stream<S: SourceTrait>(
    source: &S,
    path: &str,
    scope: &QueryScope,
    tx: mpsc::Sender<DataBatch<OptionTrade>>,
    batch_size: usize,
    progress_update_ms: u64,
    metrics: Option<Arc<Metrics>>,
) {
    info!("Attempting to get stream for path: {}", path);
    match source.get_stream(path).await {
        Ok(stream) => {
            let total_len = source.object_len(path).await;
            if let Some(m) = metrics.as_ref() {
                m.set_current_file(path.to_string(), total_len.unwrap_or(0));
            }
            let bytes_read = Arc::new(AtomicU64::new(0));
            let reader = StreamReader::new(stream);
            let counting = CountingReader::new(reader, bytes_read.clone());
            let buf = BufReader::new(counting);
            let mut csv_reader = AsyncReaderBuilder::new().create_reader(buf);
            let mut records = csv_reader.records();
            let instruments = scope.instruments.clone();
            let mut batch: Vec<OptionTrade> = Vec::with_capacity(batch_size);
            let mut last_update = Instant::now();

            while let Some(rec) = records.next().await {
                if let Ok(record) = rec {
                    // ticker,conditions,correction,exchange,participant_timestamp,price,sip_timestamp,size
                    let contract = record[0].to_string();
                    if !instruments.is_empty() && !instruments.contains(&contract) {
                        if let Some(m) = metrics.as_ref() {
                            if last_update.elapsed() >= Duration::from_millis(progress_update_ms) {
                                m.set_current_file_read(bytes_read.load(Ordering::Relaxed));
                                last_update = Instant::now();
                            }
                        }
                        continue;
                    }

                    let (contract_direction, strike_price, expiry_ts_ns, underlying) =
                        parse_opra_contract(
                            &contract,
                            record.get(6).and_then(|s| s.parse::<i64>().ok()),
                        );

                    let trade = OptionTrade {
                        contract: contract.clone(),
                        contract_direction,
                        strike_price,
                        underlying,
                        trade_ts_ns: record[6].parse().unwrap_or(0),
                        price: record[5].parse().unwrap_or(0.0),
                        size: record[7].parse().unwrap_or(0),
                        conditions: record[1]
                            .split(',')
                            .filter_map(|s| s.trim().parse().ok())
                            .collect(),
                        exchange: record[3].parse().unwrap_or(0),
                        expiry_ts_ns,
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
                        delta: None,
                        gamma: None,
                        vega: None,
                        theta: None,
                        iv: None,
                        greeks_flags: 0,
                        source: Source::Flatfile,
                        quality: Quality::Prelim,
                        watermark_ts_ns: 0,
                    };
                    batch.push(trade);
                    if batch.len() >= batch_size {
                        let meta = DataBatchMeta {
                            source: Source::Flatfile,
                            quality: Quality::Prelim,
                            watermark: Watermark {
                                watermark_ts_ns: 0,
                                completeness: Completeness::Complete,
                                hints: None,
                            },
                            schema_version: 1,
                        };
                        let _ = tx.try_send(DataBatch {
                            rows: std::mem::take(&mut batch),
                            meta,
                        });
                    }
                }
                if let Some(m) = metrics.as_ref() {
                    if last_update.elapsed() >= Duration::from_millis(progress_update_ms) {
                        m.set_current_file_read(bytes_read.load(Ordering::Relaxed));
                        last_update = Instant::now();
                    }
                }
            }

            if !batch.is_empty() {
                let meta = DataBatchMeta {
                    source: Source::Flatfile,
                    quality: Quality::Prelim,
                    watermark: Watermark {
                        watermark_ts_ns: 0,
                        completeness: Completeness::Complete,
                        hints: None,
                    },
                    schema_version: 1,
                };
                let _ = tx.try_send(DataBatch { rows: batch, meta });
            }

            if let Some(m) = metrics.as_ref() {
                if let Some(total) = total_len {
                    let read = bytes_read.load(Ordering::Relaxed);
                    if read < total {
                        m.set_current_file_read(total);
                    } else {
                        m.set_current_file_read(read);
                    }
                } else {
                    m.set_current_file_read(bytes_read.load(Ordering::Relaxed));
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to get stream for {}: {}", path, e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::path::PathBuf;

    #[derive(Clone)]
    pub struct LocalFileSource {
        base_path: PathBuf,
    }

    impl LocalFileSource {
        pub fn new(base_path: PathBuf) -> Self {
            Self { base_path }
        }
    }

    #[async_trait]
    impl SourceTrait for LocalFileSource {
        async fn get_stream(
            &self,
            path: &str,
        ) -> Result<
            Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
            Box<dyn std::error::Error + Send + Sync>,
        > {
            let full_path = self.base_path.join(path);
            let file = File::open(&full_path).await?;
            let reader = BufReader::new(file);

            let stream: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>;
            if path.ends_with(".gz") {
                let decoder = GzipDecoder::new(reader);
                stream = Box::pin(ReaderStream::new(decoder));
            } else {
                stream = Box::pin(ReaderStream::new(reader));
            }

            Ok(stream)
        }

        async fn object_len(&self, path: &str) -> Option<u64> {
            let full_path = self.base_path.join(path);
            match tokio::fs::metadata(&full_path).await {
                Ok(meta) => Some(meta.len()),
                Err(_) => None,
            }
        }

        async fn run(&self) {
            loop {
                sleep(Duration::from_secs(10)).await;
            }
        }

        fn status(&self) -> String {
            "OK".to_string()
        }

        async fn get_option_trades(
            &self,
            _scope: QueryScope,
        ) -> Pin<Box<dyn Stream<Item = DataBatch<OptionTrade>> + Send>> {
            Box::pin(futures::stream::empty())
        }

        async fn get_equity_trades(
            &self,
            scope: QueryScope,
        ) -> Pin<Box<dyn Stream<Item = DataBatch<EquityTrade>> + Send>> {
            let (tx, rx) = mpsc::channel(10);
            let self_clone = self.clone();
            let scope_clone = scope.clone();
            tokio::spawn(async move {
                let path = "stock_trades_sample.csv.gz";
                process_equity_trades_stream(&self_clone, path, &scope_clone, tx, 1000, 250, None)
                    .await;
            });
            Box::pin(ReceiverStream::new(rx))
        }

        async fn get_nbbo(
            &self,
            _scope: QueryScope,
        ) -> Pin<Box<dyn Stream<Item = DataBatch<Nbbo>> + Send>> {
            Box::pin(futures::stream::empty())
        }

        async fn get_option_nbbo(
            &self,
            _scope: QueryScope,
        ) -> Pin<Box<dyn Stream<Item = DataBatch<Nbbo>> + Send>> {
            Box::pin(futures::stream::empty())
        }
    }

    #[tokio::test]
    async fn test_local_file_store_stream_stock_quotes_sample() {
        let source = LocalFileSource::new(PathBuf::from("fixtures"));
        let mut stream = source
            .get_stream("stock_quotes_sample.csv.gz")
            .await
            .unwrap();
        let mut bytes_collected = Vec::new();
        while let Some(chunk) = futures::StreamExt::next(&mut stream).await {
            bytes_collected.push(chunk.unwrap());
        }
        assert!(
            !bytes_collected.is_empty(),
            "Stream should produce bytes for stock_quotes_sample.csv.gz"
        );
    }

    #[tokio::test]
    async fn test_local_file_store_stream_stock_trades_sample() {
        let source = LocalFileSource::new(PathBuf::from("fixtures"));
        let mut stream = source
            .get_stream("stock_trades_sample.csv.gz")
            .await
            .unwrap();
        let mut bytes_collected = Vec::new();
        while let Some(chunk) = futures::StreamExt::next(&mut stream).await {
            bytes_collected.push(chunk.unwrap());
        }
        assert!(
            !bytes_collected.is_empty(),
            "Stream should produce bytes for stock_trades_sample.csv.gz"
        );
    }

    #[tokio::test]
    async fn test_local_file_source_get_option_trades() {
        let source = LocalFileSource::new(PathBuf::from("fixtures"));
        let scope = QueryScope {
            instruments: vec![],
            time_range: (0, i64::MAX),
            mode: "Historical".to_string(),
            quality_target: Quality::Prelim,
        };
        // Reuse LocalFileSource get_stream; add a small runner for options fixture
        let (tx, rx) = mpsc::channel(10);
        process_option_trades_stream(
            &source,
            "options_trades_example.csv.gz",
            &scope,
            tx,
            1000,
            250,
            None,
        )
        .await;
        // Collect all rows
        let mut all_rows = Vec::new();
        let mut rx_stream = ReceiverStream::new(rx);
        while let Some(batch) = tokio_stream::StreamExt::next(&mut rx_stream).await {
            all_rows.extend(batch.rows);
        }
        assert!(!all_rows.is_empty(), "Should yield OptionTrade rows");
        // Validate a couple of fields from the fixture
        let first = &all_rows[0];
        assert_eq!(first.contract, "O:SPY230327P00390000");
        assert_eq!(first.price, 11.82);
        assert_eq!(first.size, 1);
        assert_eq!(first.exchange, 312);
        assert!(first.trade_ts_ns > 0);
        // OPRA parsing
        assert_eq!(first.contract_direction, 'P');
        assert!((first.strike_price - 390.0).abs() < 1e-6);
        assert_eq!(first.underlying, "SPY");
    }

    #[tokio::test]
    async fn test_local_file_source_get_equity_trades() {
        let source = LocalFileSource::new(PathBuf::from("fixtures"));
        let scope = QueryScope {
            instruments: vec![],
            time_range: (0, i64::MAX),
            mode: "Historical".to_string(),
            quality_target: Quality::Prelim,
        };
        let mut stream = source.get_equity_trades(scope).await;
        let mut all_trades = Vec::new();
        while let Some(batch) = stream.next().await {
            all_trades.extend(batch.rows);
        }
        assert!(!all_trades.is_empty(), "Should yield EquityTrade rows");
        // Check conditions parsed (assume sample has conditions like "1,2")
        for trade in &all_trades {
            assert!(
                trade.conditions.iter().all(|&c| c >= 0),
                "Conditions should be parsed as non-negative integers"
            );
        }
        // Check timestamps correct (assume positive ns)
        for trade in &all_trades {
            assert!(
                trade.trade_ts_ns > 0,
                "Timestamps should be positive nanoseconds"
            );
        }
        // Additional checks based on sample data
        assert_eq!(
            all_trades[0].symbol, "MSFT",
            "First trade symbol should be MSFT"
        );
        assert_eq!(
            all_trades[0].price, 276.16,
            "First trade price should be 276.16"
        );
        assert_eq!(all_trades[0].size, 55, "First trade size should be 55");
        assert_eq!(
            all_trades[0].conditions,
            vec![12, 37],
            "First trade conditions should be [12, 37]"
        );
        assert_eq!(
            all_trades[0].exchange, 11,
            "First trade exchange should be 11"
        );
        assert_eq!(
            all_trades[1].symbol, "MSFT",
            "Second trade symbol should be MSFT"
        );
        assert_eq!(
            all_trades[1].price, 276.75,
            "Second trade price should be 276.75"
        );
        assert_eq!(all_trades[1].size, 100, "Second trade size should be 100");
        assert_eq!(
            all_trades[1].conditions,
            vec![12],
            "Second trade conditions should be [12]"
        );
        assert_eq!(
            all_trades[1].exchange, 8,
            "Second trade exchange should be 8"
        );
    }
}
