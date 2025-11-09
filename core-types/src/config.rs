// Copyright (c) James Kassemi, SC, US. All rights reserved.

use config::{Config, ConfigError};
use serde::{Deserialize, Serialize};

/// Config structure with key knobs from spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub ws: WsConfig,
    pub staleness: StalenessConfig,
    pub classifier: ClassifierConfig,
    pub greeks: GreeksConfig,
    pub storage: StorageConfig,
    pub rest: RestConfig,
    pub scheduler: SchedulerConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsConfig {
    pub shards: std::collections::HashMap<String, u32>, // e.g., "options_quotes" => 1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StalenessConfig {
    pub bounds: std::collections::HashMap<String, (u32, u32)>, // e.g., "options" => (10, 150)
    pub quantile: String, // "p99"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassifierConfig {
    pub use_tick_rule_rt: bool,
    pub use_tick_rule_t1: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GreeksConfig {
    pub moneyness_threshold: f64,
    pub notional_threshold: f64,
    pub pool_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub paths: std::collections::HashMap<String, String>,
    pub row_group_target: usize,
    pub retention_weeks: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestConfig {
    pub rate_limits: std::collections::HashMap<String, u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerConfig {
    pub top_n: usize,
    pub exploration_fraction: f64,
    pub rebalance_interval_s: u64,
    pub hysteresis: f64,
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        let settings = Config::builder()
            .add_source(config::File::with_name("config.toml").required(false))
            .add_source(config::Environment::with_prefix("APP"))
            .build()?;
        settings.try_deserialize()
    }
}
