use std::{collections::HashSet, env, net::SocketAddr, path::PathBuf, str::FromStr, sync::Arc};

use chrono::NaiveTime;

use core_types::config::DateRange;
use thiserror::Error;
use time::macros::date;
use window_space::{
    SymbolId, WindowRangeConfig, WindowSpace,
    config::{DEFAULT_MAX_SYMBOLS, WindowSpaceConfig},
};

const DEV_SYMBOLS: &[&str] = &["AAPL", "F", "TSLA", "NVDA", "GME", "SPY"];

/// Deployment target for the binary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Environment {
    Dev,
    Prod,
}

impl FromStr for Environment {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "dev" => Ok(Environment::Dev),
            "prod" => Ok(Environment::Prod),
            other => Err(ConfigError::UnknownEnvironment {
                value: other.to_string(),
            }),
        }
    }
}

/// Minimal configuration blob compiled into the binary.
#[derive(Clone)]
pub struct AppConfig {
    pub env: Environment,
    pub ledger: WindowSpaceConfig,
    pub metrics_addr: SocketAddr,
    pub rest_base_url: &'static str,
    pub stocks_ws_url: &'static str,
    pub options_ws_url: &'static str,
    pub secrets: Secrets,
    pub flatfile: FlatfileSettings,
    pub symbol_universe: Option<Arc<HashSet<String>>>,
}

impl AppConfig {
    pub fn load(env: Environment) -> Result<Self, ConfigError> {
        let symbol_universe = symbol_universe_for(env);
        let mut ledger = ledger_config_for(env);
        ledger.max_symbols = symbol_universe
            .as_ref()
            .map(|set| {
                let limit = set.len();
                assert!(
                    limit <= SymbolId::MAX as usize,
                    "symbol universe size exceeds SymbolId capacity"
                );
                limit as SymbolId
            })
            .unwrap_or(DEFAULT_MAX_SYMBOLS);
        Ok(Self {
            env,
            ledger,
            metrics_addr: metrics_addr_for(env),
            rest_base_url: "https://api.massive.com",
            stocks_ws_url: "wss://socket.massive.com/stocks",
            options_ws_url: "wss://socket.massive.com/options",
            secrets: Secrets::from_env()?,
            flatfile: FlatfileSettings::for_env(env),
            symbol_universe,
        })
    }

    pub fn env_label(&self) -> &'static str {
        match self.env {
            Environment::Dev => "dev",
            Environment::Prod => "prod",
        }
    }
}

#[derive(Clone)]
pub struct FlatfileSettings {
    pub bucket: &'static str,
    pub endpoint: &'static str,
    pub region: &'static str,
    pub date_ranges: Vec<DateRange>,
    pub batch_size: usize,
    pub progress_update_ms: u64,
    pub next_day_ready_time: NaiveTime,
    pub non_trading_ready_time: NaiveTime,
}

impl FlatfileSettings {
    fn for_env(_env: Environment) -> Self {
        Self {
            bucket: "flatfiles",
            endpoint: "https://files.massive.com",
            region: "custom",
            date_ranges: vec![DateRange {
                start_ts: "2025-11-10T00:00:00Z".to_string(),
                end_ts: None,
            }],
            batch_size: 2000,
            progress_update_ms: 250,
            next_day_ready_time: NaiveTime::from_hms_opt(11, 0, 0).unwrap(),
            non_trading_ready_time: NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        }
    }
}

fn ledger_config_for(env: Environment) -> WindowSpaceConfig {
    let ledger_state_dir = match env {
        Environment::Dev => PathBuf::from("/home/james/m17/ledger.state"),
        Environment::Prod => PathBuf::from("/home/james/ledger.state"),
    };

    WindowSpaceConfig::new(ledger_state_dir, session_windows(env))
}

fn metrics_addr_for(env: Environment) -> SocketAddr {
    match env {
        Environment::Dev => "127.0.0.1:9095".parse().expect("valid metrics addr"),
        Environment::Prod => "127.0.0.1:9095".parse().expect("valid metrics addr"),
    }
}

fn session_windows(env: Environment) -> WindowSpace {
    let base = WindowRangeConfig::default();
    match env {
        Environment::Dev => {
            let start = date!(2025 - 11 - 10);
            let end = date!(2025 - 11 - 20);
            WindowSpace::from_bounds(
                start,
                end,
                base.session_open,
                base.session_windows,
                base.window_duration_secs,
                base.schema_version,
            )
        }
        Environment::Prod => WindowSpace::from_range(&base),
    }
}

fn symbol_universe_for(env: Environment) -> Option<Arc<HashSet<String>>> {
    match env {
        Environment::Dev => Some(Arc::new(
            DEV_SYMBOLS.iter().map(|sym| sym.to_string()).collect(),
        )),
        Environment::Prod => None,
    }
}

/// Operator-provided credentials pulled from the shell environment.
#[derive(Clone, Debug)]
pub struct Secrets {
    pub massive_api_key: String,
    pub flatfile_access_key_id: String,
    pub flatfile_secret_access_key: String,
}

impl Secrets {
    fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            massive_api_key: require_env("POLYGONIO_KEY")?,
            flatfile_access_key_id: require_env("POLYGONIO_ACCESS_KEY_ID")?,
            flatfile_secret_access_key: require_env("POLYGONIO_SECRET_ACCESS_KEY")?,
        })
    }
}

fn require_env(key: &str) -> Result<String, ConfigError> {
    env::var(key).map_err(|_| ConfigError::MissingEnv {
        key: key.to_string(),
    })
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("unknown environment '{value}' (expected 'dev' or 'prod')")]
    UnknownEnvironment { value: String },
    #[error("missing environment variable {key}")]
    MissingEnv { key: String },
}
