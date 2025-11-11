// Copyright (c) James Kassemi, SC, US. All rights reserved.
use chrono::{DateTime, Utc};
use config::{Config, ConfigError};
use serde::{Deserialize, Serialize};

/// Config structure with key knobs from spec.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AppConfig {
    pub ws: WsConfig,
    #[serde(default)]
    pub staleness: StalenessConfig,
    pub classifier: ClassifierConfig,
    pub greeks: GreeksConfig,
    #[serde(default)]
    pub aggregations: AggregationsConfig,
    pub storage: StorageConfig,
    pub rest: RestConfig,
    pub scheduler: SchedulerConfig,
    pub flatfile: FlatfileConfig,
    #[serde(default)]
    pub ingest: IngestConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WsConfig {
    #[serde(default = "default_stocks_ws_url")]
    pub stocks_ws_url: String,
    #[serde(default = "default_options_ws_url")]
    pub options_ws_url: String,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default = "default_underlying_symbol")]
    pub underlying_symbol: String,
    #[serde(default = "default_rest_base_url")]
    pub rest_base_url: String,
    #[serde(default = "default_options_contract_limit")]
    pub options_contract_limit: usize,
    #[serde(default = "default_options_refresh_interval_s")]
    pub options_refresh_interval_s: u64,
    #[serde(default = "default_ws_batch_size")]
    pub batch_size: usize,
    #[serde(default)]
    pub shards: std::collections::HashMap<String, u32>, // e.g., "options_quotes" => 1
}

fn default_stocks_ws_url() -> String {
    "wss://socket.massive.com/stocks".to_string()
}

fn default_options_ws_url() -> String {
    "wss://socket.massive.com/options".to_string()
}

fn default_underlying_symbol() -> String {
    String::new()
}

fn default_rest_base_url() -> String {
    "https://api.massive.com".to_string()
}

fn default_options_contract_limit() -> usize {
    1000
}

fn default_options_refresh_interval_s() -> u64 {
    900
}

fn default_ws_batch_size() -> usize {
    1000
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AggregationsConfig {
    #[serde(default = "default_agg_windows")]
    pub windows: Vec<String>,
    #[serde(default = "default_agg_symbol")]
    pub symbol: String,
    #[serde(default = "default_contract_size")]
    pub contract_size: u32,
    #[serde(default = "default_agg_dividend")]
    pub dividend_yield: f64,
    #[serde(default = "default_diptest_draws")]
    pub diptest_bootstrap_draws: usize,
}

fn default_agg_windows() -> Vec<String> {
    vec![
        "1m".to_string(),
        "5m".to_string(),
        "15m".to_string(),
        "30m".to_string(),
    ]
}

fn default_agg_symbol() -> String {
    "SPY".to_string()
}

fn default_contract_size() -> u32 {
    100
}

fn default_agg_dividend() -> f64 {
    0.0
}

fn default_diptest_draws() -> usize {
    2000
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StalenessConfig {
    #[serde(default)]
    pub bounds: std::collections::HashMap<String, (u32, u32)>, // e.g., "options" => (10, 150)
    #[serde(default = "default_quantile")]
    pub quantile: String, // "p99"
}

fn default_quantile() -> String {
    "p99".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClassifierConfig {
    #[serde(default)]
    pub use_tick_rule_rt: bool,
    #[serde(default = "default_tick_rule_t1")]
    pub use_tick_rule_t1: bool,
}

fn default_tick_rule_t1() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GreeksConfig {
    #[serde(default = "default_moneyness_threshold")]
    pub moneyness_threshold: f64,
    #[serde(default = "default_notional_threshold")]
    pub notional_threshold: f64,
    #[serde(default = "default_pool_size")]
    pub pool_size: usize,
    #[serde(default = "default_dividend_yield")]
    pub dividend_yield: f64,
    #[serde(default = "default_flatfile_underlying_staleness_us")]
    pub flatfile_underlying_staleness_us: u32,
    #[serde(default = "default_realtime_underlying_staleness_us")]
    pub realtime_underlying_staleness_us: u32,
}

fn default_moneyness_threshold() -> f64 {
    0.1
}

fn default_notional_threshold() -> f64 {
    1000.0
}

fn default_pool_size() -> usize {
    4
}

fn default_dividend_yield() -> f64 {
    0.0
}

fn default_flatfile_underlying_staleness_us() -> u32 {
    1_000_000
}
fn default_realtime_underlying_staleness_us() -> u32 {
    1_000_000
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StorageConfig {
    #[serde(default)]
    pub paths: std::collections::HashMap<String, String>,
    #[serde(default = "default_row_group_target")]
    pub row_group_target: usize,
    #[serde(default = "default_retention_weeks")]
    pub retention_weeks: u32,
    #[serde(default = "default_file_size_mb_target")]
    pub file_size_mb_target: u64,
}

fn default_row_group_target() -> usize {
    128000
}

fn default_retention_weeks() -> u32 {
    4
}

fn default_file_size_mb_target() -> u64 {
    128
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RestConfig {
    #[serde(default)]
    pub rate_limits: std::collections::HashMap<String, u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SchedulerConfig {
    #[serde(default = "default_top_n")]
    pub top_n: usize,
    #[serde(default = "default_exploration_fraction")]
    pub exploration_fraction: f64,
    #[serde(default = "default_rebalance_interval_s")]
    pub rebalance_interval_s: u64,
    #[serde(default = "default_hysteresis")]
    pub hysteresis: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestConfig {
    #[serde(default = "default_ingest_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_ingest_concurrent_days")]
    pub concurrent_days: usize,
    #[serde(default = "default_progress_update_ms")]
    pub progress_update_ms: u64,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            batch_size: default_ingest_batch_size(),
            concurrent_days: default_ingest_concurrent_days(),
            progress_update_ms: default_progress_update_ms(),
        }
    }
}

fn default_ingest_batch_size() -> usize {
    20_000
}

fn default_ingest_concurrent_days() -> usize {
    4
}

fn default_progress_update_ms() -> u64 {
    250
}

fn default_top_n() -> usize {
    100
}

fn default_exploration_fraction() -> f64 {
    0.1
}

fn default_rebalance_interval_s() -> u64 {
    300
}

fn default_hysteresis() -> f64 {
    0.05
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FlatfileConfig {
    #[serde(default)]
    pub date_ranges: Vec<DateRange>,
    pub massive_key: String,
    pub massive_access_key_id: String,
    pub massive_secret_access_key: String,
    pub massive_flatfiles_endpoint: String,
    pub massive_flatfiles_region: String,
    pub massive_flatfiles_bucket: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DateRange {
    pub start_ts: String,
    pub end_ts: Option<String>,
}

impl DateRange {
    /// Parse start_ts to nanoseconds since epoch.
    pub fn start_ts_ns(&self) -> Result<i64, chrono::ParseError> {
        let dt: DateTime<Utc> = self.start_ts.parse()?;
        Ok(dt.timestamp_nanos_opt().unwrap())
    }

    /// Parse end_ts to nanoseconds since epoch, if present.
    pub fn end_ts_ns(&self) -> Result<Option<i64>, chrono::ParseError> {
        if let Some(ref end) = self.end_ts {
            let dt: DateTime<Utc> = end.parse()?;
            Ok(Some(dt.timestamp_nanos_opt().unwrap()))
        } else {
            Ok(None)
        }
    }
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        let settings = Config::builder()
            .add_source(config::File::with_name("config.toml").required(true))
            .build()?;
        let config: Self = settings.try_deserialize()?;
        Ok(config)
    }
}
