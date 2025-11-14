use std::{
    collections::{BTreeSet, HashSet},
    io,
    sync::Arc,
    time::Duration,
};

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use crc32fast::Hasher as Crc32;
use engine_api::{
    Engine, EngineError, EngineHealth, EngineResult, HealthStatus, PriorityHookDescription,
};
use log::{debug, error, info, warn};
use parking_lot::Mutex;
use reqwest::{Client, Url};
use serde::Deserialize;
use thiserror::Error;
use tokio::{runtime::Runtime, task::JoinHandle, time::sleep};
use tokio_util::sync::CancellationToken;
use window_space::{
    mapping::RfRatePayload,
    payload::{PayloadMeta, PayloadType},
    WindowIndex, WindowSpaceController, WindowSpaceError,
};

const TREASURY_SCHEMA_VERSION: u8 = 1;
const DEFAULT_POLL_SECS: u64 = 300;

#[derive(Clone)]
pub struct TreasuryEngineConfig {
    pub label: String,
    pub rest_base_url: String,
    pub api_key: String,
    pub poll_interval: Duration,
    pub prime_symbols: Vec<String>,
}

impl TreasuryEngineConfig {
    pub fn new(
        label: impl Into<String>,
        rest_base_url: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Self {
        Self {
            label: label.into(),
            rest_base_url: rest_base_url.into(),
            api_key: api_key.into(),
            poll_interval: Duration::from_secs(DEFAULT_POLL_SECS),
            prime_symbols: Vec::new(),
        }
    }

    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    pub fn with_prime_symbols<T>(mut self, symbols: T) -> Self
    where
        T: IntoIterator<Item = String>,
    {
        self.prime_symbols = symbols.into_iter().collect();
        self
    }
}

pub struct TreasuryEngine {
    inner: Arc<TreasuryInner>,
}

impl TreasuryEngine {
    pub fn new(config: TreasuryEngineConfig, controller: Arc<WindowSpaceController>) -> Self {
        Self {
            inner: TreasuryInner::new(config, controller),
        }
    }
}

impl Engine for TreasuryEngine {
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
            notes: Some("Treasury curve ingestion".into()),
        }
    }
}

struct TreasuryInner {
    config: TreasuryEngineConfig,
    controller: Arc<WindowSpaceController>,
    client: Client,
    state: Mutex<EngineRuntimeState>,
    health: Mutex<EngineHealth>,
    known_symbols: Mutex<HashSet<String>>,
    stored_curves: Mutex<Vec<PersistedCurve>>,
    last_date: Mutex<Option<NaiveDate>>,
}

impl TreasuryInner {
    fn new(config: TreasuryEngineConfig, controller: Arc<WindowSpaceController>) -> Arc<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .expect("reqwest client");
        Arc::new(Self {
            config,
            controller,
            client,
            state: Mutex::new(EngineRuntimeState::Stopped),
            health: Mutex::new(EngineHealth::new(HealthStatus::Stopped, None)),
            known_symbols: Mutex::new(HashSet::new()),
            stored_curves: Mutex::new(Vec::new()),
            last_date: Mutex::new(None),
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
        info!("[{}] treasury engine starting", self.config.label);
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
                "[{}] treasury runtime join failed: {:?}",
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
        if let Err(err) = self.seed_curves().await {
            error!("[{}] treasury bootstrap failed: {}", self.config.label, err);
            self.set_health(HealthStatus::Degraded, Some(err.to_string()));
        } else {
            self.set_health(HealthStatus::Ready, None);
        }
        while !cancel.is_cancelled() {
            match self.refresh_latest().await {
                Ok(updated) => {
                    if updated {
                        self.set_health(HealthStatus::Ready, None);
                    } else if let Err(err) = self.apply_latest_to_new_symbols() {
                        warn!(
                            "[{}] treasury symbol catch-up failed: {}",
                            self.config.label, err
                        );
                        self.set_health(HealthStatus::Degraded, Some(err.to_string()));
                    }
                }
                Err(err) => {
                    warn!("[{}] treasury refresh failed: {}", self.config.label, err);
                    self.set_health(HealthStatus::Degraded, Some(err.to_string()));
                }
            }

            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = sleep(self.config.poll_interval) => {}
            }
        }
        self.set_health(HealthStatus::Stopped, None);
        info!("[{}] treasury engine stopped", self.config.label);
    }

    async fn seed_curves(&self) -> Result<(), TreasuryError> {
        let (start_date, end_date) = self.window_date_range();
        if start_date > end_date {
            return Ok(());
        }
        let samples = self.fetch_range(start_date, end_date).await?;
        let mut last = None;
        for (idx, sample) in samples.iter().enumerate() {
            let next_ts = samples
                .get(idx + 1)
                .map(|s| naive_to_timestamp(s.date))
                .unwrap_or_else(|| sample.effective_ts() + 86_400);
            self.persist_curve(sample, next_ts)?;
            last = Some(sample.date);
        }
        *self.last_date.lock() = last;
        Ok(())
    }

    async fn refresh_latest(&self) -> Result<bool, TreasuryError> {
        let latest = match self.fetch_latest().await? {
            Some(sample) => sample,
            None => return Ok(false),
        };
        {
            let mut guard = self.last_date.lock();
            if guard.as_ref().is_some_and(|date| *date >= latest.date) {
                return Ok(false);
            }
            *guard = Some(latest.date);
        }
        let next_ts = latest.effective_ts() + 86_400;
        self.persist_curve(&latest, next_ts)?;
        Ok(true)
    }

    fn persist_curve(
        &self,
        sample: &CurveSample,
        next_effective_ts: i64,
    ) -> Result<(), TreasuryError> {
        let payload = sample.payload(next_effective_ts);
        let checksum = checksum(&payload);
        let payload_id = {
            let mut stores = self.controller.payload_stores();
            stores.rf_rate.append(payload.clone())?
        };
        let version = payload_id;
        let meta = PayloadMeta::new(PayloadType::RfRate, payload_id, version, checksum);
        let windows =
            self.window_indices_for_range(payload.effective_ts, payload.next_effective_ts);
        if windows.is_empty() {
            debug!(
                "[{}] treasury curve {} has no target windows",
                self.config.label, sample.date
            );
            return Ok(());
        }
        let symbols = self.symbol_targets();
        for symbol in &symbols {
            for window_idx in &windows {
                self.controller
                    .set_rf_rate_ref(symbol, *window_idx, meta, None)?;
            }
        }
        self.mark_symbols_known(&symbols);
        let mut stored = self.stored_curves.lock();
        stored.push(PersistedCurve { meta, windows });
        info!(
            "[{}] applied treasury curve {} to {} symbols",
            self.config.label,
            sample.date,
            symbols.len()
        );
        Ok(())
    }

    fn apply_latest_to_new_symbols(&self) -> Result<(), TreasuryError> {
        let new_symbols = self.detect_new_symbols();
        if new_symbols.is_empty() {
            return Ok(());
        }
        let stored = self.stored_curves.lock();
        if stored.is_empty() {
            return Ok(());
        }
        for symbol in &new_symbols {
            for curve in stored.iter() {
                for window_idx in &curve.windows {
                    self.controller
                        .set_rf_rate_ref(symbol, *window_idx, curve.meta, None)?;
                }
            }
        }
        info!(
            "[{}] applied cached treasury curves to {} new symbols",
            self.config.label,
            new_symbols.len()
        );
        Ok(())
    }

    fn window_indices_for_range(&self, start_ts: i64, end_ts: i64) -> Vec<WindowIndex> {
        let space = self.controller.window_space();
        space
            .iter()
            .filter(|meta| meta.start_ts >= start_ts && meta.start_ts < end_ts)
            .map(|meta| meta.window_idx)
            .collect()
    }

    fn symbol_targets(&self) -> Vec<String> {
        let mut set = BTreeSet::new();
        for (_, symbol) in self.controller.symbols() {
            set.insert(symbol);
        }
        for sym in &self.config.prime_symbols {
            set.insert(sym.clone());
        }
        set.into_iter().collect()
    }

    fn mark_symbols_known(&self, symbols: &[String]) {
        let mut known = self.known_symbols.lock();
        for symbol in symbols {
            known.insert(symbol.clone());
        }
    }

    fn detect_new_symbols(&self) -> Vec<String> {
        let mut known = self.known_symbols.lock();
        let mut new_symbols = Vec::new();
        for symbol in self.symbol_targets() {
            if known.insert(symbol.clone()) {
                new_symbols.push(symbol);
            }
        }
        new_symbols
    }

    fn window_date_range(&self) -> (NaiveDate, NaiveDate) {
        let space = self.controller.window_space();
        let first_ts = space
            .iter()
            .next()
            .map(|m| m.start_ts)
            .unwrap_or_else(|| Utc::now().timestamp());
        let last_ts = space
            .iter()
            .last()
            .map(|m| m.start_ts + m.duration_secs as i64)
            .unwrap_or(first_ts);
        let today = Utc::now().date_naive();
        let start = timestamp_to_date(first_ts).unwrap_or(today);
        let end = timestamp_to_date(last_ts).unwrap_or(today);
        (start, end.min(today))
    }

    async fn fetch_range(
        &self,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<CurveSample>, TreasuryError> {
        let mut url = self.base_url()?;
        url.query_pairs_mut()
            .append_pair("limit", "5000")
            .append_pair("sort", "date.asc")
            .append_pair("date_gte", &start_date.to_string())
            .append_pair("date_lte", &end_date.to_string());
        self.fetch_paginated(url).await
    }

    async fn fetch_latest(&self) -> Result<Option<CurveSample>, TreasuryError> {
        let mut url = self.base_url()?;
        url.query_pairs_mut()
            .append_pair("limit", "1")
            .append_pair("sort", "date.desc");
        let mut results = self.fetch_paginated(url).await?;
        Ok(results.pop())
    }

    async fn fetch_paginated(&self, mut url: Url) -> Result<Vec<CurveSample>, TreasuryError> {
        url.query_pairs_mut()
            .append_pair("apiKey", &self.config.api_key);
        let mut curves = Vec::new();
        let mut next = Some(url);
        while let Some(current) = next {
            let resp = self.client.get(current.clone()).send().await?;
            if !resp.status().is_success() {
                return Err(TreasuryError::Http(resp.status().as_u16()));
            }
            let parsed: TreasuryYieldsResponse = resp.json().await?;
            if let Some(results) = parsed.results {
                for record in results {
                    match CurveSample::try_from(record) {
                        Ok(sample) => curves.push(sample),
                        Err(err) => warn!(
                            "[{}] skipping invalid treasury record: {}",
                            self.config.label, err
                        ),
                    }
                }
            }
            next = parsed.next_url.and_then(|next_url| {
                Url::parse(&next_url)
                    .ok()
                    .or_else(|| {
                        let mut base = Url::parse(&self.config.rest_base_url).ok()?;
                        base.set_path(&next_url);
                        Some(base)
                    })
                    .map(|mut next_link| {
                        if !next_link.query_pairs().any(|(k, _)| k == "apiKey") {
                            next_link
                                .query_pairs_mut()
                                .append_pair("apiKey", &self.config.api_key);
                        }
                        next_link
                    })
            });
        }
        Ok(curves)
    }

    fn base_url(&self) -> Result<Url, TreasuryError> {
        let mut url = Url::parse(&self.config.rest_base_url)?;
        url.set_path("/fed/v1/treasury-yields");
        Ok(url)
    }
}

#[derive(Debug)]
struct PersistedCurve {
    meta: PayloadMeta,
    windows: Vec<WindowIndex>,
}

#[derive(Debug, Clone)]
struct CurveSample {
    date: NaiveDate,
    tenor_bps: [i32; 12],
}

impl CurveSample {
    fn effective_ts(&self) -> i64 {
        naive_to_timestamp(self.date)
    }

    fn payload(&self, next_effective_ts: i64) -> RfRatePayload {
        RfRatePayload {
            schema_version: TREASURY_SCHEMA_VERSION,
            curve_id: self.date.ordinal() as u16,
            effective_ts: self.effective_ts(),
            next_effective_ts,
            tenor_bps: self.tenor_bps,
        }
    }
}

impl TryFrom<TreasuryYieldRecord> for CurveSample {
    type Error = TreasuryError;

    fn try_from(value: TreasuryYieldRecord) -> Result<Self, Self::Error> {
        let date = value
            .date
            .and_then(|d| NaiveDate::parse_from_str(&d, "%Y-%m-%d").ok())
            .ok_or(TreasuryError::InvalidCurve)?;
        let mut tenor_bps = [0i32; 12];
        let values = [
            value.yield_1_month,
            value.yield_3_month,
            value.yield_6_month,
            value.yield_1_year,
            value.yield_2_year,
            value.yield_3_year,
            value.yield_5_year,
            value.yield_7_year,
            value.yield_10_year,
            value.yield_20_year,
            value.yield_30_year,
            None,
        ];
        for (idx, entry) in values.iter().enumerate() {
            tenor_bps[idx] = entry
                .map(|v| (v * 100.0).round() as i32)
                .unwrap_or_default();
        }
        Ok(Self { date, tenor_bps })
    }
}

#[derive(Debug, Deserialize)]
struct TreasuryYieldsResponse {
    next_url: Option<String>,
    results: Option<Vec<TreasuryYieldRecord>>,
}

#[derive(Debug, Deserialize)]
struct TreasuryYieldRecord {
    date: Option<String>,
    #[serde(rename = "yield_1_month")]
    yield_1_month: Option<f64>,
    #[serde(rename = "yield_3_month")]
    yield_3_month: Option<f64>,
    #[serde(rename = "yield_6_month")]
    yield_6_month: Option<f64>,
    #[serde(rename = "yield_1_year")]
    yield_1_year: Option<f64>,
    #[serde(rename = "yield_2_year")]
    yield_2_year: Option<f64>,
    #[serde(rename = "yield_3_year")]
    yield_3_year: Option<f64>,
    #[serde(rename = "yield_5_year")]
    yield_5_year: Option<f64>,
    #[serde(rename = "yield_7_year")]
    yield_7_year: Option<f64>,
    #[serde(rename = "yield_10_year")]
    yield_10_year: Option<f64>,
    #[serde(rename = "yield_20_year")]
    yield_20_year: Option<f64>,
    #[serde(rename = "yield_30_year")]
    yield_30_year: Option<f64>,
}

#[derive(Debug, Error)]
enum TreasuryError {
    #[error("url parse error: {0}")]
    Url(#[from] url::ParseError),
    #[error("http status {0}")]
    Http(u16),
    #[error("request error: {0}")]
    Request(#[from] reqwest::Error),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("window error: {0}")]
    Window(#[from] WindowSpaceError),
    #[error("invalid curve data")]
    InvalidCurve,
}

impl From<TreasuryError> for EngineError {
    fn from(value: TreasuryError) -> Self {
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

fn checksum(payload: &RfRatePayload) -> u32 {
    let mut hasher = Crc32::new();
    hasher.update(&[payload.schema_version]);
    hasher.update(&payload.curve_id.to_le_bytes());
    hasher.update(&payload.effective_ts.to_le_bytes());
    hasher.update(&payload.next_effective_ts.to_le_bytes());
    for value in &payload.tenor_bps {
        hasher.update(&value.to_le_bytes());
    }
    hasher.finalize()
}

fn naive_to_timestamp(date: NaiveDate) -> i64 {
    let dt = NaiveDateTime::new(date, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
    dt.and_utc().timestamp()
}

fn timestamp_to_date(ts: i64) -> Option<NaiveDate> {
    chrono::DateTime::from_timestamp(ts, 0).map(|dt| dt.date_naive())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;
    use window_space::{PayloadType, SlotStatus, WindowSpace, WindowSpaceConfig};

    fn bootstrap_controller() -> (Arc<WindowSpaceController>, TempDir) {
        let dir = tempfile::tempdir().expect("temp dir");
        let start_date = NaiveDate::from_ymd_opt(2025, 11, 10).unwrap();
        let window_space = WindowSpace::standard(naive_to_timestamp(start_date));
        let mut config = WindowSpaceConfig::new(dir.path().to_path_buf(), window_space);
        config.max_symbols = 32;
        let (controller, _) = WindowSpaceController::bootstrap(config).unwrap();
        (Arc::new(controller), dir)
    }

    fn sample_curve(date: NaiveDate) -> CurveSample {
        CurveSample {
            date,
            tenor_bps: [123; 12],
        }
    }

    #[test]
    fn persist_curve_marks_rf_slots_filled() {
        let (controller, _dir) = bootstrap_controller();
        let engine = TreasuryInner::new(
            TreasuryEngineConfig::new("test", "http://localhost", "key")
                .with_prime_symbols(vec!["AAPL".to_string()]),
            Arc::clone(&controller),
        );
        let sample = sample_curve(NaiveDate::from_ymd_opt(2025, 11, 10).unwrap());
        let next_ts = sample.effective_ts() + 86_400;
        engine
            .persist_curve(&sample, next_ts)
            .expect("persist curve");

        let row = controller.get_trade_row("AAPL", 0).expect("trade row");
        assert_eq!(row.rf_rate.status, SlotStatus::Filled);
        assert_eq!(row.rf_rate.payload_type, PayloadType::RfRate);

        let stores = controller.payload_stores();
        assert_eq!(stores.rf_rate.len(), 1);
    }

    #[test]
    fn cached_curves_are_reapplied_for_new_symbols() {
        let (controller, _dir) = bootstrap_controller();
        let engine = TreasuryInner::new(
            TreasuryEngineConfig::new("test", "http://localhost", "key")
                .with_prime_symbols(vec!["AAPL".to_string()]),
            Arc::clone(&controller),
        );
        let sample = sample_curve(NaiveDate::from_ymd_opt(2025, 11, 10).unwrap());
        let next_ts = sample.effective_ts() + 86_400;
        engine
            .persist_curve(&sample, next_ts)
            .expect("persist curve");

        controller.resolve_symbol("MSFT").expect("resolve symbol");
        let before = controller
            .get_trade_row("MSFT", 0)
            .expect("trade row before backfill");
        assert_eq!(before.rf_rate.status, SlotStatus::Empty);

        engine
            .apply_latest_to_new_symbols()
            .expect("apply cached curves");

        let after = controller
            .get_trade_row("MSFT", 0)
            .expect("trade row after backfill");
        assert_eq!(after.rf_rate.status, SlotStatus::Filled);
        assert_eq!(after.rf_rate.payload_type, PayloadType::RfRate);
    }
}
