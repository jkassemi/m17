use chrono::{DateTime, Utc};
use config::{Config, ConfigError};
use serde::{Deserialize, Serialize};

/// Config structure with key knobs from spec.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AppConfig {
    pub polygonio_key: String,
    pub polygonio_access_key_id: String,
    pub polygonio_secret_access_key: String,
    pub ws: WsConfig,
    pub staleness: StalenessConfig,
    pub classifier: ClassifierConfig,
    pub greeks: GreeksConfig,
    pub storage: StorageConfig,
    pub rest: RestConfig,
    pub scheduler: SchedulerConfig,
    pub flatfile: FlatfileConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WsConfig {
    #[serde(default)]
    pub shards: std::collections::HashMap<String, u32>, // e.g., "options_quotes" => 1
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StorageConfig {
    #[serde(default)]
    pub paths: std::collections::HashMap<String, String>,
    #[serde(default = "default_row_group_target")]
    pub row_group_target: usize,
    #[serde(default = "default_retention_weeks")]
    pub retention_weeks: u32,
}

fn default_row_group_target() -> usize {
    128000
}

fn default_retention_weeks() -> u32 {
    4
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
            .add_source(config::File::with_name("config.toml").required(false))
            .add_source(config::Environment::with_prefix("APP"))
            .add_source(config::Environment::default())
            .build()?;
        let config: Self = settings.try_deserialize()?;
        if config.polygonio_key.is_empty() {
            return Err(ConfigError::Message("POLYGONIO_KEY is required".to_string()));
        }
        if config.polygonio_access_key_id.is_empty() {
            return Err(ConfigError::Message("POLYGONIO_ACCESS_KEY_ID is required".to_string()));
        }
        if config.polygonio_secret_access_key.is_empty() {
            return Err(ConfigError::Message("POLYGONIO_SECRET_ACCESS_KEY is required".to_string()));
        }
        Ok(config)
    }
}
