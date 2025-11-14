mod config;
mod metrics;

use std::{
    collections::HashSet,
    env, process,
    str::FromStr,
    sync::{Arc, Once, mpsc},
    time::Duration,
};

use chrono::{DateTime, Utc};
use config::{AppConfig, ConfigError, Environment};
use core_types::{config::DateRange, types::ClassParams};
use engine_api::{Engine, EngineError};
use gc_engine::{GcEngine, GcEngineConfig};
use log::{LevelFilter, Log, Metadata, Record};
use metrics::MetricsServer;
use nbbo_engine::{
    AggressorEngineConfig, AggressorMetrics, OptionAggressorEngine, UnderlyingAggressorEngine,
};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use trade_flatfile_engine::{
    DownloadMetrics, FlatfileRuntimeConfig, OptionQuoteFlatfileEngine,
    UnderlyingQuoteFlatfileEngine, UnderlyingTradeFlatfileEngine,
};
use trade_ws_engine::{TradeWsConfig, TradeWsEngine};
use treasury_engine::{TreasuryEngine, TreasuryEngineConfig};
use window_space::{
    WindowSpace, WindowSpaceController, WindowSpaceError, WindowSpaceStorageReport,
};

const DEFAULT_CLASSIFIER_EPSILON: f64 = 1e-4;
const DEFAULT_CLASSIFIER_LATENESS_MS: u32 = 1_000;

fn main() {
    init_logger();
    if let Err(err) = run() {
        eprintln!("m17 failed: {err}");
        process::exit(1);
    }
}

fn run() -> Result<(), AppError> {
    let args = parse_cli_args()?;
    let config = AppConfig::load(args.env)?;
    assert!(
        !config.secrets.massive_api_key.is_empty(),
        "POLYGONIO_KEY must be set"
    );

    config.ledger.ensure_dirs()?;
    let (controller_inner, storage_report) =
        WindowSpaceController::bootstrap(config.ledger.clone())?;
    let controller = Arc::new(controller_inner);
    let window_end = controller
        .window_space()
        .iter()
        .last()
        .and_then(|meta| DateTime::<Utc>::from_timestamp(meta.start_ts, 0))
        .map(|dt| dt.date_naive())
        .unwrap_or_else(|| Utc::now().date_naive());
    let download_metrics = Arc::new(DownloadMetrics::new());
    let aggressor_metrics = Arc::new(AggressorMetrics::new());
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
        progress_logging: args.progress_logging,
        download_metrics: Arc::clone(&download_metrics),
        symbol_universe: config.symbol_universe.clone(),
        next_day_ready_time: config.flatfile.next_day_ready_time,
        non_trading_ready_time: config.flatfile.non_trading_ready_time,
        window_end,
    };
    let option_quote_engine =
        OptionQuoteFlatfileEngine::new(flatfile_cfg.clone(), controller.clone());
    let underlying_trade_engine =
        UnderlyingTradeFlatfileEngine::new(flatfile_cfg.clone(), controller.clone());
    let underlying_quote_engine =
        UnderlyingQuoteFlatfileEngine::new(flatfile_cfg.clone(), controller.clone());
    let aggressor_cfg = AggressorEngineConfig::for_label(config.env_label());
    let option_aggressor_engine = OptionAggressorEngine::new(
        aggressor_cfg.clone(),
        controller.clone(),
        default_class_params(),
        Arc::clone(&aggressor_metrics),
    );
    let underlying_aggressor_engine = UnderlyingAggressorEngine::new(
        aggressor_cfg,
        controller.clone(),
        default_class_params(),
        Arc::clone(&aggressor_metrics),
    );
    let gc_engine = GcEngine::new(
        GcEngineConfig {
            label: config.env_label().to_string(),
            ..Default::default()
        },
        controller.clone(),
    );
    let prime_symbols = config
        .symbol_universe
        .as_ref()
        .map(|set| set.iter().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    let treasury_cfg = TreasuryEngineConfig::new(
        config.env_label(),
        config.rest_base_url,
        config.secrets.massive_api_key.clone(),
    )
    .with_prime_symbols(prime_symbols);
    let treasury_engine = TreasuryEngine::new(treasury_cfg, controller.clone());
    let trade_ws_cfg = TradeWsConfig {
        label: config.env_label().to_string(),
        state_dir: config.ledger.state_dir().to_path_buf(),
        options_ws_url: config.options_ws_url.to_string(),
        api_key: config.secrets.massive_api_key.clone(),
        rest_base_url: config.rest_base_url.to_string(),
        contracts_per_underlying: 1_000,
        flush_interval: Duration::from_millis(1_000),
        window_grace: Duration::from_millis(2_000),
        contract_refresh_interval: Duration::from_secs(300),
        symbol_filter: {
            let mut set = HashSet::new();
            set.insert(config.ws_target_symbol.to_string());
            Some(set)
        },
    };
    let trade_ws_engine = TradeWsEngine::new(trade_ws_cfg, controller.clone());

    println!(
        "m17 orchestrator booted in {:?} mode; window space state at {:?}",
        config.env,
        config.ledger.state_dir()
    );
    match ledger_window_summary(&config.ledger.window_space) {
        Some(summary) => println!(
            "Window space capacity: up to {} symbols across {} minutes ({} -> {})",
            config.ledger.max_symbols, summary.minutes, summary.start_label, summary.end_label
        ),
        None => println!(
            "Window space capacity: up to {} symbols with no configured window range",
            config.ledger.max_symbols
        ),
    }
    log_storage_summary(&storage_report);
    println!(
        "Massive REST base: {}; stocks WS: {}; options WS: {}",
        config.rest_base_url, config.stocks_ws_url, config.options_ws_url
    );
    println!(
        "Flatfile ingestion: bucket={}, endpoint={}, region={}, batch_size={}, progress_update={}ms",
        config.flatfile.bucket,
        config.flatfile.endpoint,
        config.flatfile.region,
        config.flatfile.batch_size,
        config.flatfile.progress_update_ms
    );
    println!(
        "Flatfile date ranges: {}",
        describe_flatfile_ranges(&config.flatfile.date_ranges)
    );

    treasury_engine.start()?;
    trade_ws_engine.start()?;
    option_quote_engine.start()?;
    underlying_trade_engine.start()?;
    underlying_quote_engine.start()?;
    option_aggressor_engine.start()?;
    underlying_aggressor_engine.start()?;
    gc_engine.start()?;
    log_engine_health("treasury", &treasury_engine);
    log_engine_health("options-ws-trade", &trade_ws_engine);
    log_engine_health("option-quote-flatfile", &option_quote_engine);
    log_engine_health("underlying-trade-flatfile", &underlying_trade_engine);
    log_engine_health("underlying-quote-flatfile", &underlying_quote_engine);
    log_engine_health("option-aggressor", &option_aggressor_engine);
    log_engine_health("underlying-aggressor", &underlying_aggressor_engine);
    log_engine_health("gc-engine", &gc_engine);
    println!("Flatfile engines are running; press Ctrl+C to shut down.");
    let metrics_server = MetricsServer::start(
        controller.slot_metrics(),
        download_metrics,
        Arc::clone(&controller),
        aggressor_metrics,
        config.metrics_addr,
    );
    wait_for_shutdown_signal()?;
    println!("Shutdown signal received; stopping engines...");
    underlying_aggressor_engine.stop()?;
    option_aggressor_engine.stop()?;
    gc_engine.stop()?;
    underlying_quote_engine.stop()?;
    underlying_trade_engine.stop()?;
    option_quote_engine.stop()?;
    treasury_engine.stop()?;
    trade_ws_engine.stop()?;
    metrics_server.shutdown();

    // Keep controller alive for the lifetime of engines.
    drop(controller);
    Ok(())
}

struct CliArgs {
    env: Environment,
    progress_logging: bool,
}

fn parse_cli_args() -> Result<CliArgs, AppError> {
    let mut args = env::args().skip(1);
    let env_arg = args.next().ok_or(AppError::Usage)?;
    let env = Environment::from_str(&env_arg).map_err(AppError::from)?;
    let mut progress_logging = false;
    for arg in args {
        match arg.as_str() {
            "--debug-progress" => progress_logging = true,
            flag => return Err(AppError::UnknownFlag(flag.to_string())),
        }
    }
    Ok(CliArgs {
        env,
        progress_logging,
    })
}

#[derive(Debug, Error)]
enum AppError {
    #[error("usage: m17 <dev|prod> [--debug-progress]")]
    Usage,
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Ledger(#[from] WindowSpaceError),
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error("failed to install signal handler: {0}")]
    Signal(#[from] ctrlc::Error),
    #[error("failed while waiting for shutdown signal: {0}")]
    ShutdownWait(#[from] mpsc::RecvError),
    #[error("unknown flag: {0}")]
    UnknownFlag(String),
}

struct WindowSummary {
    minutes: usize,
    start_label: String,
    end_label: String,
}

fn ledger_window_summary(window_space: &WindowSpace) -> Option<WindowSummary> {
    let mut iter = window_space.iter();
    let first = iter.next()?;
    let mut last = first;
    for meta in iter {
        last = meta;
    }
    let minutes = window_space.len();
    let start_label = format_timestamp(first.start_ts);
    let end_label = format_timestamp(last.start_ts + 60);
    Some(WindowSummary {
        minutes,
        start_label,
        end_label,
    })
}

fn format_timestamp(ts: i64) -> String {
    OffsetDateTime::from_unix_timestamp(ts)
        .ok()
        .and_then(|dt| dt.format(&Rfc3339).ok())
        .unwrap_or_else(|| format!("{ts}"))
}

fn describe_flatfile_ranges(ranges: &[DateRange]) -> String {
    if ranges.is_empty() {
        return "none configured".to_string();
    }

    ranges
        .iter()
        .enumerate()
        .map(|(idx, range)| match &range.end_ts {
            Some(end) => format!("#{}: {} -> {}", idx + 1, range.start_ts, end),
            None => format!("#{}: {} -> open-ended", idx + 1, range.start_ts),
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn log_storage_summary(report: &WindowSpaceStorageReport) {
    println!(
        "Trade ledger file: {} (size={} bytes, created_at={}, init_time={})",
        report.trade.path.display(),
        report.trade.file_size,
        format_timestamp(report.trade.created_at_s),
        format_creation_duration(report.trade.creation_duration)
    );
    println!(
        "Enrichment ledger file: {} (size={} bytes, created_at={}, init_time={})",
        report.enrichment.path.display(),
        report.enrichment.file_size,
        format_timestamp(report.enrichment.created_at_s),
        format_creation_duration(report.enrichment.creation_duration)
    );
}

fn format_creation_duration(duration: Option<Duration>) -> String {
    match duration {
        Some(dur) => format!("{:?}", dur),
        None => "existing".to_string(),
    }
}

fn default_class_params() -> ClassParams {
    ClassParams {
        use_tick_rule_fallback: true,
        epsilon_price: DEFAULT_CLASSIFIER_EPSILON,
        allowed_lateness_ms: DEFAULT_CLASSIFIER_LATENESS_MS,
    }
}

fn wait_for_shutdown_signal() -> Result<(), AppError> {
    let (tx, rx) = mpsc::channel();
    ctrlc::set_handler(move || {
        let _ = tx.send(());
    })?;
    rx.recv()?;
    Ok(())
}

fn log_engine_health(label: &str, engine: &dyn Engine) {
    let health = engine.health();
    println!("{label} status: {:?} ({:?})", health.status, health.detail);
}

static LOGGER: SimpleLogger = SimpleLogger;

fn init_logger() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        log::set_logger(&LOGGER).expect("install logger");
        let level = env::var("RUST_LOG")
            .ok()
            .and_then(|value| value.parse::<LevelFilter>().ok())
            .unwrap_or(LevelFilter::Info);
        log::set_max_level(level);
    });
}

struct SimpleLogger;

impl Log for SimpleLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!(
                "[{}] {}: {}",
                record.level(),
                record.target(),
                record.args()
            );
        }
    }

    fn flush(&self) {}
}
