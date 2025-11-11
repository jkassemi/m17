//! Treasury ingestion service: fetches Massive treasury yields, caches per-day curves, and refreshes latest data.

use chrono::{NaiveDate, Utc};
use classifier::greeks::TreasuryCurve;
use core_types::retry::RetryPolicy;
use core_types::status::{OverallStatus, ServiceStatusHandle, StatusGauge};
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
        match self
            .retry
            .retry_async(|_| async {
                fetch_treasury_curve_range(
                    &self.client,
                    &self.rest_base_url,
                    &self.api_key,
                    start_date,
                    end_date,
                )
                .await
            })
            .await
        {
            Ok(curves) => {
                let mut cache = self.cache.write().await;
                for (date, curve) in curves {
                    cache.insert(date, Arc::new(curve));
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
        match self
            .retry
            .retry_async(|_| async {
                fetch_latest_treasury_curve(&self.client, &self.rest_base_url, &self.api_key).await
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
        cache.get(&date).cloned()
    }

    pub async fn latest_curve(&self) -> Option<Arc<TreasuryCurve>> {
        let latest = self.latest.read().await;
        latest.clone()
    }

    pub fn latest_curve_state(&self) -> LatestCurve {
        Arc::clone(&self.latest)
    }

    pub async fn curve_state_for_date(&self, date: NaiveDate) -> LatestCurve {
        let initial = self.curve_for_date(date).await;
        Arc::new(RwLock::new(initial))
    }

    pub async fn latest_curve_date(&self) -> Option<NaiveDate> {
        *self.latest_date.read().await
    }
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
