use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, hash_map::Entry},
    fs::{self, File},
    io::{self, Read},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering as AtomicOrdering},
    },
    time::Duration,
};

use arrow::{
    array::{
        ArrayRef, FixedSizeBinaryArray, Float64Array, Int32Array, Int32Builder, Int64Array,
        Int64Builder, ListBuilder, StringArray, UInt32Array,
    },
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use chrono::{DateTime, Datelike, Utc};
use core_types::{raw::OptionTradeRecord, schema::option_trade_record_schema, types::OptionTrade};
use crc32fast::Hasher as Crc32;
use engine_api::{
    Engine, EngineError, EngineHealth, EngineResult, HealthStatus, PriorityHookDescription,
};
use futures::StreamExt;
use log::{error, info, warn};
use parking_lot::Mutex;
use parquet::arrow::ArrowWriter;
use reqwest::Url;
use serde::Deserialize;
use thiserror::Error;
use tokio::{
    runtime::Runtime,
    sync::{mpsc, watch},
    task::JoinHandle,
    time::interval,
};
use tokio_util::sync::CancellationToken;
use window_space::{
    WindowIndex, WindowSpaceController, WindowSpaceError,
    mapping::TradeBatchPayload,
    payload::{PayloadMeta, PayloadType, SlotKind, SlotStatus, TradeSlotKind},
    window::WindowMeta,
};
use ws_source::worker::{ResourceKind, SubscriptionSource, WsMessage, WsWorker};

const DEFAULT_FLUSH_INTERVAL_MS: u64 = 1_000;
const DEFAULT_WINDOW_GRACE_MS: u64 = 2_000;
const OPTION_TRADE_SCHEMA_VERSION: u8 = 1;
const CHANNEL_CAPACITY: usize = 16_384;

pub struct TradeWsEngine {
    inner: Arc<TradeWsInner>,
}

impl TradeWsEngine {
    pub fn new(config: TradeWsConfig, controller: Arc<WindowSpaceController>) -> Self {
        Self {
            inner: Arc::new(TradeWsInner::new(config, controller)),
        }
    }
}

impl Engine for TradeWsEngine {
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
            notes: Some("Realtime Massive options trades → option_trade_ref".to_string()),
        }
    }
}

struct TradeWsInner {
    config: TradeWsConfig,
    controller: Arc<WindowSpaceController>,
    state: Mutex<EngineRuntimeState>,
    health: Arc<Mutex<EngineHealth>>,
    batch_seq: Arc<AtomicU32>,
    underlyings: Vec<String>,
}

impl TradeWsInner {
    fn new(config: TradeWsConfig, controller: Arc<WindowSpaceController>) -> Self {
        let underlyings = Self::derive_underlyings(&config, &controller);
        Self {
            config,
            controller,
            state: Mutex::new(EngineRuntimeState::Stopped),
            health: Arc::new(Mutex::new(EngineHealth::new(
                HealthStatus::Stopped,
                Some("engine not started".to_string()),
            ))),
            batch_seq: Arc::new(AtomicU32::new(0)),
            underlyings,
        }
    }

    fn derive_underlyings(
        config: &TradeWsConfig,
        controller: &WindowSpaceController,
    ) -> Vec<String> {
        if let Some(filter) = &config.symbol_filter {
            let mut symbols: Vec<_> = filter.iter().cloned().collect();
            symbols.sort();
            symbols
        } else {
            controller
                .symbols()
                .into_iter()
                .map(|(_, sym)| sym)
                .collect()
        }
    }

    fn start(&self) -> EngineResult<()> {
        self.config
            .ensure_dirs()
            .map_err(|err| EngineError::Failure { source: err.into() })?;
        let mut guard = self.state.lock();
        if matches!(*guard, EngineRuntimeState::Running(_)) {
            return Err(EngineError::AlreadyRunning);
        }
        if self.underlyings.is_empty() {
            return Err(EngineError::Failure {
                source: Box::new(TradeWsError::NoSymbols),
            });
        }
        let runtime = Runtime::new().map_err(|err| EngineError::Failure { source: err.into() })?;
        let topics = runtime
            .block_on(resolve_subscription_topics(&self.config, &self.underlyings))
            .map_err(|err| EngineError::Failure {
                source: Box::new(err),
            })?;
        if topics.is_empty() {
            return Err(EngineError::Failure {
                source: Box::new(TradeWsError::NoContracts),
            });
        }
        let cancel = CancellationToken::new();
        let runner = Arc::clone(&self.controller);
        let cfg = self.config.clone();
        let batch_seq = Arc::clone(&self.batch_seq);
        let health = Arc::clone(&self.health);
        let (topic_tx, topic_rx) = watch::channel(topics.clone());
        let mut refresh_rx = topic_tx.subscribe();
        let refresh_cfg = self.config.clone();
        let refresh_underlyings = self.underlyings.clone();
        let refresh_cancel = cancel.clone();
        runtime.spawn(async move {
            refresh_contracts_loop(
                refresh_cfg,
                refresh_underlyings,
                topic_tx,
                &mut refresh_rx,
                refresh_cancel,
            )
            .await;
        });
        let cancel_clone = cancel.clone();
        let handle = runtime.spawn(async move {
            let mut ingestor = WsIngestor::new(cfg, runner, batch_seq, health, topic_rx);
            ingestor.run(cancel_clone).await;
        });
        *guard = EngineRuntimeState::Running(RuntimeBundle {
            runtime,
            handle,
            cancel,
        });
        self.set_health(HealthStatus::Ready, Some("streaming".to_string()));
        info!("[{}] trade-ws engine started", self.config.label);
        Ok(())
    }

    fn stop(&self) -> EngineResult<()> {
        let mut guard = self.state.lock();
        let bundle = match std::mem::replace(&mut *guard, EngineRuntimeState::Stopped) {
            EngineRuntimeState::Running(bundle) => bundle,
            EngineRuntimeState::Stopped => return Err(EngineError::NotRunning),
        };
        bundle.cancel.cancel();
        if let Err(err) = bundle.runtime.block_on(bundle.handle) {
            if !err.is_cancelled() {
                warn!("trade-ws engine task error: {err}");
            }
        }
        self.set_health(HealthStatus::Stopped, Some("engine stopped".to_string()));
        info!("[{}] trade-ws engine stopped", self.config.label);
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
}

struct RuntimeBundle {
    runtime: Runtime,
    handle: JoinHandle<()>,
    cancel: CancellationToken,
}

enum EngineRuntimeState {
    Stopped,
    Running(RuntimeBundle),
}

struct WsIngestor {
    config: TradeWsConfig,
    controller: Arc<WindowSpaceController>,
    batch_seq: Arc<AtomicU32>,
    health: Arc<Mutex<EngineHealth>>,
    topics_rx: watch::Receiver<Vec<String>>,
}

impl WsIngestor {
    fn new(
        config: TradeWsConfig,
        controller: Arc<WindowSpaceController>,
        batch_seq: Arc<AtomicU32>,
        health: Arc<Mutex<EngineHealth>>,
        topics_rx: watch::Receiver<Vec<String>>,
    ) -> Self {
        Self {
            config,
            controller,
            batch_seq,
            health,
            topics_rx,
        }
    }

    async fn run(&mut self, cancel: CancellationToken) {
        let (tx, mut rx) = mpsc::channel(CHANNEL_CAPACITY);
        let ws_task = {
            let cfg = self.config.clone();
            let cancel = cancel.clone();
            let topics = self.topics_rx.clone();
            tokio::spawn(async move {
                stream_options_trades(cfg, topics, tx, cancel).await;
            })
        };
        let mut buckets: HashMap<WindowKey, WindowEntry> = HashMap::new();
        let mut ticker = interval(self.config.flush_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    break;
                }
                Some(trade) = rx.recv() => {
                    if let Err(err) = self.handle_trade(trade, &mut buckets).await {
                        self.set_health(HealthStatus::Degraded, Some(err.to_string()));
                        error!("trade-ws ingestion error: {err}");
                    }
                }
                _ = ticker.tick() => {
                    if let Err(err) = self.flush_ready(&mut buckets).await {
                        self.set_health(HealthStatus::Degraded, Some(err.to_string()));
                        error!("trade-ws flush error: {err}");
                    }
                }
            }
        }
        if let Err(err) = self.flush_all(&mut buckets).await {
            error!("trade-ws shutdown flush error: {err}");
        }
        ws_task.abort();
        self.set_health(HealthStatus::Stopped, Some("ingestor stopped".to_string()));
    }

    async fn handle_trade(
        &self,
        trade: OptionTrade,
        buckets: &mut HashMap<WindowKey, WindowEntry>,
    ) -> Result<(), TradeWsError> {
        if trade.underlying.is_empty() {
            return Ok(());
        }
        if let Some(filter) = &self.config.symbol_filter {
            if !filter.contains(&trade.underlying) {
                return Ok(());
            }
        }
        let window_idx =
            window_idx_for_timestamp(&self.controller, trade.trade_ts_ns).ok_or_else(|| {
                TradeWsError::WindowOutOfRange {
                    ts_ns: trade.trade_ts_ns,
                }
            })?;
        let key = WindowKey {
            symbol: trade.underlying.clone(),
            window_idx,
        };
        let mut record = Some(option_record_from_trade(trade));
        match buckets.entry(key.clone()) {
            Entry::Occupied(mut occ) => {
                let entry = occ.get_mut();
                if !entry.claimed {
                    match self.ensure_slot(&key)? {
                        SlotDisposition::Skip => {
                            occ.remove();
                            return Ok(());
                        }
                        SlotDisposition::Claimed => {
                            entry.claimed = true;
                        }
                    }
                }
                if let Some(rec) = record.take() {
                    entry.bucket.observe(rec);
                }
                Ok(())
            }
            Entry::Vacant(vacant) => {
                let meta = self
                    .controller
                    .window_meta(window_idx)
                    .ok_or(TradeWsError::MissingWindowMeta { window_idx })?;
                match self.ensure_slot(&key)? {
                    SlotDisposition::Skip => Ok(()),
                    SlotDisposition::Claimed => {
                        let entry = vacant.insert(WindowEntry::new(meta));
                        entry.claimed = true;
                        if let Some(rec) = record.take() {
                            entry.bucket.observe(rec);
                        }
                        Ok(())
                    }
                }
            }
        }
    }

    async fn flush_ready(
        &self,
        buckets: &mut HashMap<WindowKey, WindowEntry>,
    ) -> Result<(), TradeWsError> {
        let now_ns = current_time_ns();
        let grace_ns = self.config.window_grace_ns();
        let ready: Vec<WindowKey> = buckets
            .iter()
            .filter_map(|(key, entry)| {
                if entry.ready_to_flush(now_ns, grace_ns) {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect();
        for key in ready {
            if let Some(entry) = buckets.remove(&key) {
                if entry.bucket.records.is_empty() {
                    continue;
                }
                self.persist_bucket(&key, entry).await?;
            }
        }
        Ok(())
    }

    async fn flush_all(
        &self,
        buckets: &mut HashMap<WindowKey, WindowEntry>,
    ) -> Result<(), TradeWsError> {
        let keys: Vec<WindowKey> = buckets.keys().cloned().collect();
        for key in keys {
            if let Some(entry) = buckets.remove(&key) {
                if entry.bucket.records.is_empty() {
                    continue;
                }
                self.persist_bucket(&key, entry).await?;
            }
        }
        Ok(())
    }

    fn ensure_slot(&self, key: &WindowKey) -> Result<SlotDisposition, TradeWsError> {
        let row = self.controller.get_trade_row(&key.symbol, key.window_idx)?;
        match row.option_trade_ref.status {
            SlotStatus::Filled => Ok(SlotDisposition::Skip),
            SlotStatus::Retire | SlotStatus::Retired => Ok(SlotDisposition::Skip),
            SlotStatus::Pending => Ok(SlotDisposition::Claimed),
            SlotStatus::Empty | SlotStatus::Cleared => {
                self.controller.mark_pending(
                    &key.symbol,
                    key.window_idx,
                    SlotKind::Trade(TradeSlotKind::OptionTrade),
                )?;
                Ok(SlotDisposition::Claimed)
            }
        }
    }

    async fn persist_bucket(
        &self,
        key: &WindowKey,
        entry: WindowEntry,
    ) -> Result<(), TradeWsError> {
        let batch = option_trade_batch(&entry.bucket.records)?;
        let relative = artifact_path(
            &self.config,
            &key.symbol,
            key.window_idx,
            entry.window_start_ts,
        )?;
        let artifact = write_record_batch(self.config.state_dir(), &relative, &batch)?;
        let payload = TradeBatchPayload {
            schema_version: OPTION_TRADE_SCHEMA_VERSION,
            window_ts: entry.window_start_ts,
            batch_id: self.batch_seq.fetch_add(1, AtomicOrdering::Relaxed) + 1,
            first_trade_ts: entry.bucket.first_ts,
            last_trade_ts: entry.bucket.last_ts,
            record_count: entry.bucket.records.len() as u32,
            artifact_uri: artifact.relative_path.clone(),
            checksum: artifact.checksum,
        };
        let payload_id = {
            let mut stores = self.controller.payload_stores();
            stores.trades.append(payload)?
        };
        let meta = PayloadMeta::new(PayloadType::Trade, payload_id, 1, artifact.checksum);
        if let Err(err) =
            self.controller
                .set_option_trade_ref(&key.symbol, key.window_idx, meta, None)
        {
            self.controller.clear_slot(
                &key.symbol,
                key.window_idx,
                SlotKind::Trade(TradeSlotKind::OptionTrade),
            )?;
            return Err(err.into());
        }
        Ok(())
    }

    fn set_health(&self, status: HealthStatus, detail: Option<String>) {
        let mut guard = self.health.lock();
        guard.status = status;
        guard.detail = detail;
    }
}

async fn stream_options_trades(
    config: TradeWsConfig,
    topics: watch::Receiver<Vec<String>>,
    tx: mpsc::Sender<OptionTrade>,
    cancel: CancellationToken,
) {
    let worker = WsWorker::new(
        &config.options_ws_url,
        ResourceKind::OptionsTrades,
        Some(config.api_key.clone()),
        SubscriptionSource::Dynamic(topics),
    );
    match worker.stream().await {
        Ok(mut stream) => {
            while let Some(msg) = stream.next().await {
                if cancel.is_cancelled() {
                    break;
                }
                if let WsMessage::OptionTrade(trade) = msg {
                    if tx.send(trade).await.is_err() {
                        break;
                    }
                }
            }
        }
        Err(err) => {
            error!(
                "[{}] websocket stream failed: {}",
                config.label,
                err.to_string()
            );
        }
    }
}

#[derive(Clone)]
pub struct TradeWsConfig {
    pub label: String,
    pub state_dir: PathBuf,
    pub options_ws_url: String,
    pub api_key: String,
    pub rest_base_url: String,
    pub contracts_per_underlying: usize,
    pub flush_interval: Duration,
    pub window_grace: Duration,
    pub contract_refresh_interval: Duration,
    pub symbol_filter: Option<HashSet<String>>,
}

impl TradeWsConfig {
    pub fn ensure_dirs(&self) -> io::Result<()> {
        if !self.state_dir.exists() {
            fs::create_dir_all(&self.state_dir)?;
        }
        Ok(())
    }

    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    pub fn window_grace_ns(&self) -> i64 {
        self.window_grace.as_nanos().try_into().unwrap_or(i64::MAX)
    }
}

impl Default for TradeWsConfig {
    fn default() -> Self {
        Self {
            label: "dev".to_string(),
            state_dir: PathBuf::from("ledger.state"),
            options_ws_url: "wss://socket.massive.com/options".to_string(),
            api_key: String::new(),
            rest_base_url: "https://api.massive.com".to_string(),
            contracts_per_underlying: 1000,
            flush_interval: Duration::from_millis(DEFAULT_FLUSH_INTERVAL_MS),
            window_grace: Duration::from_millis(DEFAULT_WINDOW_GRACE_MS),
            contract_refresh_interval: Duration::from_secs(300),
            symbol_filter: None,
        }
    }
}

#[derive(Clone, Hash, Eq, PartialEq)]
struct WindowKey {
    symbol: String,
    window_idx: WindowIndex,
}

struct WindowEntry {
    bucket: WindowBucket<OptionTradeRecord>,
    window_start_ts: i64,
    window_end_ns: i64,
    claimed: bool,
}

impl WindowEntry {
    fn new(meta: WindowMeta) -> Self {
        let window_start_ts = meta.start_ts;
        let window_end_ns =
            (meta.start_ts + meta.duration_secs as i64).saturating_mul(1_000_000_000);
        Self {
            bucket: WindowBucket::new(),
            window_start_ts,
            window_end_ns,
            claimed: false,
        }
    }

    fn ready_to_flush(&self, now_ns: i64, grace_ns: i64) -> bool {
        now_ns >= self.window_end_ns + grace_ns
    }
}

enum SlotDisposition {
    Skip,
    Claimed,
}

fn window_idx_for_timestamp(controller: &WindowSpaceController, ts_ns: i64) -> Option<WindowIndex> {
    controller.window_idx_for_timestamp(ts_ns / 1_000_000_000)
}

fn option_trade_batch(records: &[OptionTradeRecord]) -> Result<RecordBatch, TradeWsError> {
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
    let mut participant_builder = Int64Builder::new();
    for record in records {
        if let Some(ts) = record.participant_ts_ns {
            participant_builder.append_value(ts);
        } else {
            participant_builder.append_null();
        }
    }
    let participant_ts_ns = Arc::new(participant_builder.finish()) as ArrayRef;
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
    RecordBatch::try_new(schema, arrays).map_err(TradeWsError::from)
}

fn option_record_from_trade(trade: OptionTrade) -> OptionTradeRecord {
    OptionTradeRecord {
        contract: trade.contract,
        trade_uid: trade.trade_uid,
        contract_direction: trade.contract_direction,
        strike_price: trade.strike_price,
        underlying: trade.underlying,
        trade_ts_ns: trade.trade_ts_ns,
        participant_ts_ns: None,
        price: trade.price,
        size: trade.size,
        conditions: trade.conditions,
        exchange: trade.exchange,
        expiry_ts_ns: trade.expiry_ts_ns,
        source: trade.source,
        quality: trade.quality,
        watermark_ts_ns: trade.watermark_ts_ns,
    }
}

fn artifact_path(
    cfg: &TradeWsConfig,
    symbol: &str,
    window_idx: WindowIndex,
    window_start_ts: i64,
) -> Result<String, TradeWsError> {
    let dt = DateTime::<Utc>::from_timestamp(window_start_ts, 0)
        .ok_or(TradeWsError::MissingWindowMeta { window_idx })?;
    Ok(format!(
        "trade-ws/{}/{:04}/{:02}/{:02}/{:06}/{}.parquet",
        cfg.label,
        dt.year(),
        dt.month(),
        dt.day(),
        window_idx,
        sanitized_symbol(symbol)
    ))
}

fn write_record_batch(
    state_dir: &Path,
    relative_path: &str,
    batch: &RecordBatch,
) -> Result<ArtifactInfo, TradeWsError> {
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
    Ok(ArtifactInfo {
        relative_path: relative_path.to_string(),
        checksum,
    })
}

fn compute_checksum(path: &Path) -> Result<u32, TradeWsError> {
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

fn sanitized_symbol(symbol: &str) -> String {
    symbol
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

fn current_time_ns() -> i64 {
    let now = Utc::now();
    now.timestamp_nanos_opt()
        .unwrap_or_else(|| now.timestamp() * 1_000_000_000)
}

async fn resolve_subscription_topics(
    config: &TradeWsConfig,
    underlyings: &[String],
) -> Result<Vec<String>, TradeWsError> {
    let client = reqwest::Client::new();
    let mut topics = HashSet::new();
    for symbol in underlyings {
        let contracts = fetch_top_options_contracts(
            &client,
            &config.rest_base_url,
            &config.api_key,
            symbol,
            config.contracts_per_underlying,
        )
        .await?;
        if contracts.is_empty() {
            warn!(
                "[{}] No contracts returned for {}; skipping",
                config.label, symbol
            );
            continue;
        }
        info!(
            "[{}] Prepared {} contracts for {}",
            config.label,
            contracts.len(),
            symbol
        );
        for contract in contracts {
            topics.insert(contract);
        }
    }
    if topics.is_empty() {
        return Err(TradeWsError::NoContracts);
    }
    let mut ordered: Vec<String> = topics.into_iter().collect();
    ordered.sort();
    info!(
        "[{}] Subscribing to {} option contracts across {} underlyings",
        config.label,
        ordered.len(),
        underlyings.len()
    );
    Ok(ordered)
}

async fn fetch_top_options_contracts(
    client: &reqwest::Client,
    rest_base: &str,
    api_key: &str,
    underlying: &str,
    max: usize,
) -> Result<Vec<String>, TradeWsError> {
    let mut url = Url::parse(rest_base).map_err(|err| TradeWsError::RestUrl(err.to_string()))?;
    url.set_path(&format!("/v3/snapshot/options/{}", underlying));
    url.query_pairs_mut()
        .append_pair("limit", "250")
        .append_pair("apiKey", api_key);

    let mut collected: Vec<(String, f64)> = Vec::new();
    let mut next = Some(url);
    let mut pages = 0usize;

    while let Some(current) = next.clone() {
        pages += 1;
        info!(
            "[trade-ws:{}] fetching contracts page {} for {}",
            underlying, pages, underlying
        );
        let resp = client.get(current.clone()).send().await?;
        if !resp.status().is_success() {
            warn!(
                "[trade-ws:{}] snapshot status {}",
                underlying,
                resp.status()
            );
            break;
        }
        let parsed: SnapshotResponse = resp.json().await?;
        if let Some(results) = parsed.results {
            for item in results {
                if let Some(details) = item.details {
                    if let Some(ticker) = details.ticker {
                        let score = item
                            .open_interest
                            .unwrap_or(0.0)
                            .max(item.day.and_then(|d| d.volume).unwrap_or(0.0));
                        collected.push((ticker, score));
                    }
                }
            }
        }
        if let Some(next_url) = parsed.next_url {
            let mut nu = Url::parse(&next_url).unwrap_or_else(|_| {
                let mut base = Url::parse(rest_base).expect("valid rest_base");
                base.set_path(&next_url);
                base
            });
            if !nu.query_pairs().any(|(k, _)| k == "apiKey") {
                nu.query_pairs_mut().append_pair("apiKey", api_key);
            }
            next = Some(nu);
        } else {
            next = None;
        }
        if collected.len() >= max {
            break;
        }
    }

    collected.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
    let mut result = Vec::new();
    for (ticker, _) in collected.into_iter() {
        if result.len() >= max {
            break;
        }
        result.push(ticker);
    }
    if result.is_empty() {
        warn!(
            "[trade-ws:{}] snapshot returned zero contracts; reverting to underlying",
            underlying
        );
    }
    Ok(result)
}

async fn refresh_contracts_loop(
    config: TradeWsConfig,
    underlyings: Vec<String>,
    tx: watch::Sender<Vec<String>>,
    rx: &mut watch::Receiver<Vec<String>>,
    cancel: CancellationToken,
) {
    if config.contract_refresh_interval.is_zero() {
        return;
    }
    let mut ticker = interval(config.contract_refresh_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = ticker.tick() => {
                match resolve_subscription_topics(&config, &underlyings).await {
                    Ok(new_topics) => {
                        let current = rx.borrow().clone();
                        if !new_topics.is_empty() && new_topics != current {
                            if tx.send(new_topics).is_err() {
                                break;
                            }
                        }
                    }
                    Err(err) => warn!("[{}] contract refresh failed: {}", config.label, err),
                }
            }
        }
    }
}

pub struct ArtifactInfo {
    pub relative_path: String,
    pub checksum: u32,
}

#[derive(Debug, Error)]
pub enum TradeWsError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("ledger error: {0}")]
    Ledger(#[from] WindowSpaceError),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("invalid REST URL: {0}")]
    RestUrl(String),
    #[error("no underlying symbols available for websocket subscriptions")]
    NoSymbols,
    #[error("no option contracts resolved for websocket subscriptions")]
    NoContracts,
    #[error("window timestamp {ts_ns} outside configured window space")]
    WindowOutOfRange { ts_ns: i64 },
    #[error("missing window metadata for {window_idx}")]
    MissingWindowMeta { window_idx: WindowIndex },
}

#[derive(Debug, Deserialize)]
struct SnapshotResponse {
    next_url: Option<String>,
    results: Option<Vec<SnapshotOption>>,
}

#[derive(Debug, Deserialize)]
struct SnapshotOption {
    open_interest: Option<f64>,
    day: Option<SnapshotDay>,
    details: Option<SnapshotDetails>,
}

#[derive(Debug, Deserialize)]
struct SnapshotDay {
    volume: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct SnapshotDetails {
    ticker: Option<String>,
}

struct WindowBucket<T> {
    records: Vec<T>,
    first_ts: i64,
    last_ts: i64,
}

impl<T> WindowBucket<T> {
    fn new() -> Self {
        Self {
            records: Vec::new(),
            first_ts: i64::MAX,
            last_ts: i64::MIN,
        }
    }
}

impl WindowBucket<OptionTradeRecord> {
    fn observe(&mut self, record: OptionTradeRecord) {
        let ts = record.trade_ts_ns;
        self.first_ts = self.first_ts.min(ts);
        self.last_ts = self.last_ts.max(ts);
        self.records.push(record);
    }
}
