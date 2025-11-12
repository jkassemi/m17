mod config;

use std::{env, process, str::FromStr, sync::Arc};

use config::{AppConfig, ConfigError, Environment};
use engine_api::{Engine, EngineError};
use ledger::{LedgerController, LedgerError};
use thiserror::Error;
use trade_flatfile_engine::{
    FlatfileRuntimeConfig, OptionTradeFlatfileEngine, UnderlyingFlatfileEngine,
};

fn main() {
    if let Err(err) = run() {
        eprintln!("m17 failed: {err}");
        process::exit(1);
    }
}

fn run() -> Result<(), AppError> {
    let config = {
        let env = parse_environment()?;
        AppConfig::load(env)?
    };

    config.ledger.ensure_dirs()?;
    let controller = Arc::new(LedgerController::bootstrap(config.ledger.clone())?);
    let flatfile_cfg = FlatfileRuntimeConfig {
        label: config.env_label(),
        state_dir: config.ledger.state_dir().to_path_buf(),
        bucket: config.flatfile.bucket.to_string(),
        endpoint: config.flatfile.endpoint.to_string(),
        region: config.flatfile.region.to_string(),
        access_key_id: config.secrets.flatfile_access_key_id.clone(),
        secret_access_key: config.secrets.flatfile_secret_access_key.clone(),
        date_ranges: config.flatfile.date_ranges.clone(),
        batch_size: config.flatfile.batch_size,
        progress_update_ms: config.flatfile.progress_update_ms,
    };
    let option_engine = OptionTradeFlatfileEngine::new(flatfile_cfg.clone(), controller.clone());
    let underlying_engine = UnderlyingFlatfileEngine::new(flatfile_cfg, controller.clone());

    println!(
        "m17 orchestrator booted in {:?} mode; ledger state at {:?}",
        config.env,
        config.ledger.state_dir()
    );
    println!(
        "Massive REST base: {}; stocks WS: {}; options WS: {}",
        config.rest_base_url, config.stocks_ws_url, config.options_ws_url
    );
    println!(
        "Loaded Massive API key (len={}), flatfile access key (len={}), secret (len={})",
        config.secrets.massive_api_key.len(),
        config.secrets.flatfile_access_key_id.len(),
        config.secrets.flatfile_secret_access_key.len()
    );
    println!(
        "Ledger currently tracks up to {} symbols",
        config.ledger.max_symbols
    );

    option_engine.start()?;
    underlying_engine.start()?;
    let option_health = option_engine.health();
    println!(
        "option-trade-flatfile status: {:?} ({:?})",
        option_health.status, option_health.detail
    );
    let underlying_health = underlying_engine.health();
    println!(
        "underlying-flatfile status: {:?} ({:?})",
        underlying_health.status, underlying_health.detail
    );
    underlying_engine.stop()?;
    option_engine.stop()?;

    // Keep controller alive for the lifetime of engines.
    drop(controller);
    Ok(())
}

fn parse_environment() -> Result<Environment, AppError> {
    let arg = env::args().nth(1).ok_or(AppError::Usage)?;
    Environment::from_str(&arg).map_err(AppError::from)
}

#[derive(Debug, Error)]
enum AppError {
    #[error("usage: m17 <dev|prod>")]
    Usage,
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Ledger(#[from] LedgerError),
    #[error(transparent)]
    Engine(#[from] EngineError),
}
