//! Options universe ingestion service: periodically fetches most-active contracts via Massive REST.

use log::error;
use reqwest::{Client, Url};
use serde::Deserialize;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::watch;
use tokio::time::sleep;

#[derive(Debug, Error)]
pub enum OptionsUniverseError {
    #[error("request error: {0}")]
    Request(#[from] reqwest::Error),
    #[error("url parse error: {0}")]
    Url(#[from] url::ParseError),
    #[error("unexpected response")]
    UnexpectedResponse,
}

#[derive(Clone)]
pub struct OptionsUniverseIngestionService {
    client: Client,
    rest_base_url: String,
    api_key: String,
    underlying: String,
    contract_limit: usize,
    refresh_interval: Duration,
}

impl OptionsUniverseIngestionService {
    pub fn new(
        client: Client,
        rest_base_url: impl Into<String>,
        api_key: impl Into<String>,
        underlying: impl Into<String>,
        contract_limit: usize,
        refresh_interval: Duration,
    ) -> Self {
        Self {
            client,
            rest_base_url: rest_base_url.into(),
            api_key: api_key.into(),
            underlying: underlying.into(),
            contract_limit: contract_limit.min(1_000),
            refresh_interval,
        }
    }

    pub fn spawn(self) -> watch::Receiver<Vec<String>> {
        let (tx, rx) = watch::channel(Vec::new());
        tokio::spawn(async move {
            let mut first = true;
            loop {
                if !first {
                    sleep(self.refresh_interval).await;
                } else {
                    first = false;
                }
                match self.fetch_top_contracts().await {
                    Ok(contracts) => {
                        if tx.send(contracts).is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        error!("options universe refresh failed: {}", err);
                    }
                }
            }
        });
        rx
    }

    async fn fetch_top_contracts(&self) -> Result<Vec<String>, OptionsUniverseError> {
        let mut collected: Vec<(String, f64)> = Vec::new();
        let mut next = Some(self.build_initial_url()?);
        while let Some(current) = next {
            let resp = self.client.get(current.clone()).send().await?;
            if !resp.status().is_success() {
                return Err(OptionsUniverseError::UnexpectedResponse);
            }
            let parsed: ChainResponse = resp.json().await?;
            if let Some(results) = parsed.results {
                for item in results {
                    if let Some(ticker) = item
                        .details
                        .as_ref()
                        .and_then(|d| d.ticker.as_ref())
                        .cloned()
                    {
                        let score = item
                            .open_interest
                            .unwrap_or(0.0)
                            .max(item.day.as_ref().and_then(|d| d.volume).unwrap_or(0.0));
                        collected.push((ticker, score));
                    }
                }
            }
            if collected.len() >= self.contract_limit {
                break;
            }
            next = parsed.next_url.and_then(|u| self.normalize_url(&u).ok());
        }
        collected.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        collected.truncate(self.contract_limit);
        Ok(collected.into_iter().map(|(ticker, _)| ticker).collect())
    }

    fn build_initial_url(&self) -> Result<Url, OptionsUniverseError> {
        let mut url = Url::parse(&self.rest_base_url)?;
        url.set_path(&format!("/v3/snapshot/options/{}", self.underlying));
        url.query_pairs_mut()
            .append_pair("limit", "250")
            .append_pair("apiKey", &self.api_key);
        Ok(url)
    }

    fn normalize_url(&self, next_url: &str) -> Result<Url, OptionsUniverseError> {
        let mut url = match Url::parse(next_url) {
            Ok(abs) => abs,
            Err(_) => {
                let base = Url::parse(&self.rest_base_url)?;
                base.join(next_url)?
            }
        };
        if !url.query_pairs().any(|(k, _)| k == "apiKey") {
            url.query_pairs_mut().append_pair("apiKey", &self.api_key);
        }
        Ok(url)
    }
}

#[derive(Debug, Deserialize)]
struct ChainResponse {
    next_url: Option<String>,
    results: Option<Vec<ChainItem>>,
}

#[derive(Debug, Deserialize)]
struct ChainItem {
    open_interest: Option<f64>,
    day: Option<DayItem>,
    details: Option<DetailsItem>,
}

#[derive(Debug, Deserialize)]
struct DayItem {
    volume: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct DetailsItem {
    ticker: Option<String>,
}
