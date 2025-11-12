// Copyright (c) James Kassemi, SC, US. All rights reserved.
//! Treasury ingestion service: fetches Massive treasury yields, caches per-day curves, and refreshes latest data.

use async_trait::async_trait;
use chrono::{NaiveDate, Utc};
use core_types::retry::RetryPolicy;
use core_types::status::{OverallStatus, ServiceStatusHandle, StatusGauge};
use greeks_engine::TreasuryCurve;
use log::{error, info};
use reqwest::{Client, Url};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::RwLock;

type CurveCache = Arc<RwLock<HashMap<NaiveDate, Arc<TreasuryCurve>>>>;
type LatestCurve = Arc<RwLock<Option<Arc<TreasuryCurve>>>>;

#[derive(Clone)]
pub struct CurveStateSelection {
    pub state: LatestCurve,
    pub source_date: Option<NaiveDate>,
}

#[async_trait]
pub trait TreasuryCurveFetcher: Send + Sync {
    async fn fetch_latest(
        &self,
        client: &Client,
        rest_base_url: &str,
        api_key: &str,
    ) -> Result<Option<(NaiveDate, TreasuryCurve)>, TreasuryServiceError>;

    async fn fetch_range(
        &self,
        client: &Client,
        rest_base_url: &str,
        api_key: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<(NaiveDate, TreasuryCurve)>, TreasuryServiceError>;
}

#[derive(Clone, Default)]
struct HttpTreasuryCurveFetcher;

#[async_trait]
impl TreasuryCurveFetcher for HttpTreasuryCurveFetcher {
    async fn fetch_latest(
        &self,
        client: &Client,
        rest_base_url: &str,
        api_key: &str,
    ) -> Result<Option<(NaiveDate, TreasuryCurve)>, TreasuryServiceError> {
        fetch_latest_treasury_curve(client, rest_base_url, api_key).await
    }

    async fn fetch_range(
        &self,
        client: &Client,
        rest_base_url: &str,
        api_key: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<(NaiveDate, TreasuryCurve)>, TreasuryServiceError> {
        fetch_treasury_curve_range(client, rest_base_url, api_key, start_date, end_date).await
    }
}

#[derive(Debug, Error)]
pub enum TreasuryServiceError {
    #[error("request error: {0}")]
    Request(#[from] reqwest::Error),
    #[error("url parse error: {0}")]
    Url(#[from] url::ParseError),
    #[error("invalid curve data")]
    InvalidCurve,
}

#[derive(Clone)]
pub struct TreasuryIngestionService {
    client: Client,
    rest_base_url: String,
    api_key: String,
    cache: CurveCache,
    latest: LatestCurve,
    latest_date: Arc<RwLock<Option<NaiveDate>>>,
    status: ServiceStatusHandle,
    retry: RetryPolicy,
    fetcher: Arc<dyn TreasuryCurveFetcher>,
}

#[derive(Clone)]
pub struct TreasuryServiceHandle {
    cache: CurveCache,
    latest: LatestCurve,
    latest_date: Arc<RwLock<Option<NaiveDate>>>,
    status: ServiceStatusHandle,
}

impl TreasuryIngestionService {
    pub fn new(
        client: Client,
        rest_base_url: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Self {
        Self::with_fetcher(
            client,
            rest_base_url,
            api_key,
            Arc::new(HttpTreasuryCurveFetcher::default()),
        )
    }

    pub fn with_fetcher(
        client: Client,
        rest_base_url: impl Into<String>,
        api_key: impl Into<String>,
        fetcher: Arc<dyn TreasuryCurveFetcher>,
    ) -> Self {
        let status = ServiceStatusHandle::new("treasury_ingestion");
        status.set_overall(OverallStatus::Crit);
        status.push_warning("waiting for Massive treasury curves");
        Self {
            client,
            rest_base_url: rest_base_url.into(),
            api_key: api_key.into(),
            cache: Arc::new(RwLock::new(HashMap::new())),
            latest: Arc::new(RwLock::new(None)),
            latest_date: Arc::new(RwLock::new(None)),
            status,
            retry: RetryPolicy::default_network(),
            fetcher,
        }
    }

    pub fn handle(&self) -> TreasuryServiceHandle {
        TreasuryServiceHandle {
            cache: Arc::clone(&self.cache),
            latest: Arc::clone(&self.latest),
            latest_date: Arc::clone(&self.latest_date),
            status: self.status.clone(),
        }
    }

    pub fn status_handle(&self) -> ServiceStatusHandle {
        self.status.clone()
    }

    pub async fn prefetch_range(
        &self,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<(), TreasuryServiceError> {
        let rest_base_url = self.rest_base_url.clone();
        let api_key = self.api_key.clone();
        let client = self.client.clone();
        let fetcher = Arc::clone(&self.fetcher);
        match self
            .retry
            .retry_async(move |_| {
                let client = client.clone();
                let rest_base_url = rest_base_url.clone();
                let api_key = api_key.clone();
                let fetcher = Arc::clone(&fetcher);
                async move {
                    fetcher
                        .fetch_range(&client, &rest_base_url, &api_key, start_date, end_date)
                        .await
                }
            })
            .await
        {
            Ok(curves) => {
                {
                    let mut cache = self.cache.write().await;
                    for (date, curve) in curves {
                        cache.insert(date, Arc::new(curve));
                    }
                }
                self.status.set_overall(OverallStatus::Ok);
                self.status.clear_errors_matching(|_| true);
                self.status
                    .clear_warnings_matching(|msg| msg.contains("waiting"));
                self.publish_gauges().await;
                Ok(())
            }
            Err(err) => {
                self.status.set_overall(OverallStatus::Crit);
                self.status
                    .push_error(format!("treasury prefetch failed: {}", err));
                Err(err)
            }
        }
    }

    pub async fn refresh_latest(&self) -> Result<Option<NaiveDate>, TreasuryServiceError> {
        let rest_base_url = self.rest_base_url.clone();
        let api_key = self.api_key.clone();
        let client = self.client.clone();
        let fetcher = Arc::clone(&self.fetcher);
        match self
            .retry
            .retry_async(move |_| {
                let client = client.clone();
                let rest_base_url = rest_base_url.clone();
                let api_key = api_key.clone();
                let fetcher = Arc::clone(&fetcher);
                async move {
                    fetcher
                        .fetch_latest(&client, &rest_base_url, &api_key)
                        .await
                }
            })
            .await?
        {
            Some((date, curve)) => {
                let arc_curve = Arc::new(curve);
                {
                    let mut cache = self.cache.write().await;
                    cache.insert(date, arc_curve.clone());
                }
                *self.latest.write().await = Some(arc_curve);
                *self.latest_date.write().await = Some(date);
                self.status.set_overall(OverallStatus::Ok);
                self.status.clear_errors_matching(|_| true);
                self.publish_gauges().await;
                Ok(Some(date))
            }
            None => {
                self.status.set_overall(OverallStatus::Crit);
                self.status.push_error("treasury endpoint returned no data");
                self.publish_gauges().await;
                Ok(None)
            }
        }
    }

    pub fn spawn_refresh_loop(&self, interval: Duration) -> tokio::task::JoinHandle<()> {
        let svc = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                match svc.refresh_latest().await {
                    Ok(Some(date)) => info!("treasury curves refreshed for {}", date),
                    Ok(None) => error!("treasury refresh returned no data"),
                    Err(err) => {
                        svc.status.set_overall(OverallStatus::Crit);
                        svc.status
                            .push_error(format!("treasury refresh failed: {}", err));
                        error!("treasury refresh failed: {}", err);
                    }
                }
            }
        })
    }

    async fn publish_gauges(&self) {
        let cache_len = self.cache.read().await.len() as f64;
        let latest_age = {
            let guard = self.latest_date.read().await;
            guard.map(|d| {
                let today = Utc::now().date_naive();
                (today - d).num_days().max(0) as f64
            })
        };
        let mut gauges = vec![StatusGauge {
            label: "cached_curve_days".to_string(),
            value: cache_len,
            max: None,
            unit: Some("days".to_string()),
            details: None,
        }];
        if let Some(age) = latest_age {
            gauges.push(StatusGauge {
                label: "latest_curve_age_days".to_string(),
                value: age,
                max: None,
                unit: Some("days".to_string()),
                details: None,
            });
        }
        self.status.set_gauges(gauges);
    }
}

impl TreasuryServiceHandle {
    pub fn status_handle(&self) -> ServiceStatusHandle {
        self.status.clone()
    }

    pub async fn curve_for_date(&self, date: NaiveDate) -> Option<Arc<TreasuryCurve>> {
        let cache = self.cache.read().await;
        lookup_curve_on_or_before(&cache, date).map(|(_, curve)| curve)
    }

    pub async fn curve_for_date_with_metadata(
        &self,
        date: NaiveDate,
    ) -> Option<(NaiveDate, Arc<TreasuryCurve>)> {
        let cache = self.cache.read().await;
        lookup_curve_on_or_before(&cache, date)
    }

    pub async fn latest_curve(&self) -> Option<Arc<TreasuryCurve>> {
        let latest = self.latest.read().await;
        latest.clone()
    }

    pub fn latest_curve_state(&self) -> LatestCurve {
        Arc::clone(&self.latest)
    }

    pub async fn curve_state_for_date_with_metadata(&self, date: NaiveDate) -> CurveStateSelection {
        let (initial_curve, source_date) = {
            let cache = self.cache.read().await;
            match lookup_curve_on_or_before(&cache, date) {
                Some((curve_date, curve)) => (Some(curve), Some(curve_date)),
                None => (None, None),
            }
        };
        CurveStateSelection {
            state: Arc::new(RwLock::new(initial_curve)),
            source_date,
        }
    }

    pub async fn curve_state_for_date(&self, date: NaiveDate) -> LatestCurve {
        self.curve_state_for_date_with_metadata(date).await.state
    }

    pub async fn latest_curve_date(&self) -> Option<NaiveDate> {
        *self.latest_date.read().await
    }
}

fn lookup_curve_on_or_before(
    cache: &HashMap<NaiveDate, Arc<TreasuryCurve>>,
    date: NaiveDate,
) -> Option<(NaiveDate, Arc<TreasuryCurve>)> {
    if let Some(curve) = cache.get(&date) {
        return Some((date, curve.clone()));
    }
    cache
        .iter()
        .filter(|(candidate, _)| **candidate <= date)
        .max_by_key(|(candidate, _)| *candidate)
        .map(|(candidate, curve)| (*candidate, curve.clone()))
}

async fn fetch_latest_treasury_curve(
    client: &Client,
    rest_base_url: &str,
    api_key: &str,
) -> Result<Option<(NaiveDate, TreasuryCurve)>, TreasuryServiceError> {
    let mut url = Url::parse(rest_base_url)?;
    url.set_path("/fed/v1/treasury-yields");
    url.query_pairs_mut()
        .append_pair("limit", "1")
        .append_pair("sort", "date.desc")
        .append_pair("apiKey", api_key);
    let resp = client.get(url).send().await?;
    if !resp.status().is_success() {
        return Err(TreasuryServiceError::InvalidCurve);
    }
    let parsed: TreasuryYieldsResponse = resp.json().await?;
    if let Some(mut results) = parsed.results {
        if let Some(record) = results.pop() {
            return Ok(build_curve_from_record(record));
        }
    }
    Ok(None)
}

async fn fetch_treasury_curve_range(
    client: &Client,
    rest_base_url: &str,
    api_key: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<Vec<(NaiveDate, TreasuryCurve)>, TreasuryServiceError> {
    let mut url = Url::parse(rest_base_url)?;
    url.set_path("/fed/v1/treasury-yields");
    url.query_pairs_mut()
        .append_pair("limit", "5000")
        .append_pair("sort", "date.asc")
        .append_pair("date_gte", &start_date.to_string())
        .append_pair("date_lte", &end_date.to_string())
        .append_pair("apiKey", api_key);
    let mut curves = Vec::new();
    let mut next = Some(url);
    while let Some(current) = next {
        let resp = client.get(current.clone()).send().await?;
        if !resp.status().is_success() {
            return Err(TreasuryServiceError::InvalidCurve);
        }
        let parsed: TreasuryYieldsResponse = resp.json().await?;
        if let Some(results) = parsed.results {
            for record in results {
                if let Some((date, curve)) = build_curve_from_record(record) {
                    curves.push((date, curve));
                }
            }
        }
        next = parsed.next_url.and_then(|next_url| {
            Url::parse(&next_url)
                .or_else(|_| {
                    let mut base = Url::parse(rest_base_url)?;
                    base.set_path(&next_url);
                    Ok::<Url, url::ParseError>(base)
                })
                .ok()
                .map(|mut url| {
                    if !url.query_pairs().any(|(k, _)| k == "apiKey") {
                        url.query_pairs_mut().append_pair("apiKey", api_key);
                    }
                    url
                })
        });
    }
    Ok(curves)
}

fn build_curve_from_record(record: TreasuryYieldRecord) -> Option<(NaiveDate, TreasuryCurve)> {
    let date = record
        .date
        .as_ref()
        .and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())?;
    let mut points = Vec::new();
    macro_rules! push_point {
        ($value:expr, $tenor:expr) => {
            if let Some(v) = $value {
                points.push(($tenor, v / 100.0));
            }
        };
    }
    push_point!(record.yield_1_month, 1.0 / 12.0);
    push_point!(record.yield_3_month, 0.25);
    push_point!(record.yield_6_month, 0.5);
    push_point!(record.yield_1_year, 1.0);
    push_point!(record.yield_2_year, 2.0);
    push_point!(record.yield_3_year, 3.0);
    push_point!(record.yield_5_year, 5.0);
    push_point!(record.yield_7_year, 7.0);
    push_point!(record.yield_10_year, 10.0);
    push_point!(record.yield_20_year, 20.0);
    push_point!(record.yield_30_year, 30.0);
    TreasuryCurve::from_pairs(points).map(|curve| (date, curve))
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

pub fn default_prefetch_range() -> (NaiveDate, NaiveDate) {
    let today = Utc::now().naive_utc().date();
    (today - chrono::Duration::days(30), today)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct StubFetcher {
        range_results:
            Mutex<VecDeque<Result<Vec<(NaiveDate, TreasuryCurve)>, TreasuryServiceError>>>,
        latest_results:
            Mutex<VecDeque<Result<Option<(NaiveDate, TreasuryCurve)>, TreasuryServiceError>>>,
    }

    impl StubFetcher {
        fn push_range_result(
            &self,
            result: Result<Vec<(NaiveDate, TreasuryCurve)>, TreasuryServiceError>,
        ) {
            self.range_results.lock().unwrap().push_back(result);
        }

        fn push_latest_result(
            &self,
            result: Result<Option<(NaiveDate, TreasuryCurve)>, TreasuryServiceError>,
        ) {
            self.latest_results.lock().unwrap().push_back(result);
        }
    }

    #[async_trait]
    impl TreasuryCurveFetcher for StubFetcher {
        async fn fetch_latest(
            &self,
            _client: &Client,
            _rest_base_url: &str,
            _api_key: &str,
        ) -> Result<Option<(NaiveDate, TreasuryCurve)>, TreasuryServiceError> {
            self.latest_results
                .lock()
                .unwrap()
                .pop_front()
                .expect("latest result not stubbed")
        }

        async fn fetch_range(
            &self,
            _client: &Client,
            _rest_base_url: &str,
            _api_key: &str,
            _start_date: NaiveDate,
            _end_date: NaiveDate,
        ) -> Result<Vec<(NaiveDate, TreasuryCurve)>, TreasuryServiceError> {
            self.range_results
                .lock()
                .unwrap()
                .pop_front()
                .expect("range result not stubbed")
        }
    }

    fn make_service_with_fetcher(fetcher: Arc<StubFetcher>) -> TreasuryIngestionService {
        let fetcher_trait: Arc<dyn TreasuryCurveFetcher> = fetcher;
        TreasuryIngestionService::with_fetcher(
            Client::new(),
            "https://example.com",
            "test-api-key",
            fetcher_trait,
        )
    }

    fn sample_curve(rate: f64) -> TreasuryCurve {
        TreasuryCurve::from_pairs(vec![(1.0, rate)]).expect("valid curve")
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prefetch_range_populates_cache_and_clears_warning() {
        let stub = Arc::new(StubFetcher::default());
        let start = NaiveDate::from_ymd_opt(2024, 1, 10).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 11).unwrap();
        stub.push_range_result(Ok(vec![
            (start, sample_curve(0.04)),
            (end, sample_curve(0.042)),
        ]));
        let svc = make_service_with_fetcher(stub);
        svc.prefetch_range(start, end)
            .await
            .expect("prefetch succeeds");
        let handle = svc.handle();
        assert!(handle.curve_for_date(start).await.is_some());
        assert!(handle.curve_for_date(end).await.is_some());

        let snapshot = svc.status_handle().snapshot();
        assert_eq!(snapshot.overall, OverallStatus::Ok);
        assert!(snapshot.warnings.iter().all(|w| !w.contains("waiting")));
        let cached = snapshot
            .gauges
            .iter()
            .find(|g| g.label == "cached_curve_days")
            .expect("cached gauge");
        assert_eq!(cached.value, 2.0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refresh_latest_updates_latest_curve_and_status() {
        let stub = Arc::new(StubFetcher::default());
        let today = NaiveDate::from_ymd_opt(2024, 2, 5).unwrap();
        stub.push_latest_result(Ok(Some((today, sample_curve(0.035)))));
        let svc = make_service_with_fetcher(stub);
        let returned = svc
            .refresh_latest()
            .await
            .expect("refresh succeeds")
            .expect("latest date");
        assert_eq!(returned, today);

        let handle = svc.handle();
        assert_eq!(handle.latest_curve_date().await, Some(today));
        let curve = handle.latest_curve().await.expect("latest curve present");
        assert!((curve.rate_for(1.0) - 0.035).abs() < 1e-9);

        let snapshot = svc.status_handle().snapshot();
        assert_eq!(snapshot.overall, OverallStatus::Ok);
        assert!(snapshot
            .gauges
            .iter()
            .any(|g| g.label == "latest_curve_age_days"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refresh_latest_sets_crit_when_no_data_returned() {
        let stub = Arc::new(StubFetcher::default());
        stub.push_latest_result(Ok(None));
        let svc = make_service_with_fetcher(stub);
        let result = svc.refresh_latest().await.expect("call succeeds");
        assert!(result.is_none());

        let snapshot = svc.status_handle().snapshot();
        assert_eq!(snapshot.overall, OverallStatus::Crit);
        assert!(snapshot
            .errors
            .iter()
            .any(|msg| msg.contains("treasury endpoint returned no data")));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prefetch_range_sets_crit_on_http_errors() {
        let stub = Arc::new(StubFetcher::default());
        stub.push_range_result(Err(TreasuryServiceError::InvalidCurve));
        let mut svc = make_service_with_fetcher(stub);
        svc.retry = RetryPolicy::new(1, 1, 1, 0.0);
        let start = NaiveDate::from_ymd_opt(2024, 1, 10).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 10).unwrap();
        let err = svc.prefetch_range(start, end).await.expect_err("fails");
        assert!(matches!(err, TreasuryServiceError::InvalidCurve));

        let snapshot = svc.status_handle().snapshot();
        assert_eq!(snapshot.overall, OverallStatus::Crit);
        assert!(snapshot
            .errors
            .iter()
            .any(|msg| msg.contains("treasury prefetch failed")));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn curve_for_date_backfills_with_latest_prior_curve() {
        let stub = Arc::new(StubFetcher::default());
        let friday = NaiveDate::from_ymd_opt(2025, 11, 7).unwrap();
        stub.push_range_result(Ok(vec![(friday, sample_curve(0.04))]));
        let svc = make_service_with_fetcher(stub);
        svc.prefetch_range(friday, friday)
            .await
            .expect("prefetch succeeds");
        let handle = svc.handle();

        let weekend = friday.succ_opt().unwrap();
        let curve = handle.curve_for_date(weekend).await.expect("curve present");
        assert!((curve.rate_for(1.0) - 0.04).abs() < 1e-9);
    }
}
