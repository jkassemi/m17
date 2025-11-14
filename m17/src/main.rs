mod aggregation;
mod config;
mod greeks;
mod metrics;

use std::{
    collections::HashSet,
    env, process,
    str::FromStr,
    sync::{
        Arc, Once,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::Duration,
};

use aggregation::AggregationService;
use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, TimeZone, Utc};
use config::{AppConfig, ConfigError, Environment};
use core_types::{
    config::{DateRange, GreeksConfig},
    types::ClassParams,
};
use engine_api::{Engine, EngineError};
use gc_engine::{GcEngine, GcEngineConfig};
use greeks::GreeksMaterializationEngine;
use log::{LevelFilter, Log, Metadata, Record, info};
use metrics::{EngineStatusRegistry, MetricsServer};
use nbbo_engine::{
    AggressorEngineConfig, AggressorMetrics, OptionAggressorEngine, UnderlyingAggressorEngine,
};
use quote_backfill_engine::{QuoteBackfillConfig, QuoteBackfillEngine, QuoteBackfillMetrics};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use trade_flatfile_engine::{
    DownloadMetrics, FlatfileRuntimeConfig, OptionQuoteFlatfileEngine,
    UnderlyingQuoteFlatfileEngine, UnderlyingTradeFlatfileEngine,
};
use trade_ws_engine::{
    DEFAULT_CONTRACTS_PER_UNDERLYING, TradeWsConfig, TradeWsEngine, TradeWsMetrics,
};
use trading_calendar::{Market, TradingCalendar};
use treasury_engine::{TreasuryEngine, TreasuryEngineConfig};
use window_space::{
    WindowSpace, WindowSpaceController, WindowSpaceError, WindowSpaceStorageReport,
};

const DEFAULT_CLASSIFIER_EPSILON: f64 = 1e-4;
const DEFAULT_CLASSIFIER_LATENESS_MS: u32 = 1_000;

const ENGINE_TREASURY: &str = "treasury_engine";
const ENGINE_TRADE_WS: &str = "trade_ws_engine";
const ENGINE_OPTION_QUOTE: &str = "option_quote_flatfile";
const ENGINE_OPTION_QUOTE_BACKFILL: &str = "option_quote_rest_backfill";
const ENGINE_UNDERLYING_TRADE: &str = "underlying_trade_flatfile";
const ENGINE_UNDERLYING_QUOTE: &str = "underlying_quote_flatfile";
const ENGINE_OPTION_AGGRESSOR: &str = "option_aggressor_engine";
const ENGINE_UNDERLYING_AGGRESSOR: &str = "underlying_aggressor_engine";
const ENGINE_GC: &str = "gc_engine";
const ENGINE_GREEKS: &str = "greeks_engine";

const ENGINE_LABELS: &[&str] = &[
    ENGINE_TREASURY,
    ENGINE_TRADE_WS,
    ENGINE_OPTION_QUOTE,
    ENGINE_OPTION_QUOTE_BACKFILL,
    ENGINE_UNDERLYING_TRADE,
    ENGINE_UNDERLYING_QUOTE,
    ENGINE_OPTION_AGGRESSOR,
    ENGINE_UNDERLYING_AGGRESSOR,
    ENGINE_GC,
    ENGINE_GREEKS,
];

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
    let trade_ws_metrics = Arc::new(TradeWsMetrics::new());
    let quote_backfill_metrics = Arc::new(QuoteBackfillMetrics::new());
    let engine_status = Arc::new(EngineStatusRegistry::default());
    for label in ENGINE_LABELS {
        engine_status.mark_stopped(label);
    }
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
    let option_quote_engine = Arc::new(OptionQuoteFlatfileEngine::new(
        flatfile_cfg.clone(),
        controller.clone(),
    ));
    let underlying_trade_engine = Arc::new(UnderlyingTradeFlatfileEngine::new(
        flatfile_cfg.clone(),
        controller.clone(),
    ));
    let underlying_quote_engine = Arc::new(UnderlyingQuoteFlatfileEngine::new(
        flatfile_cfg.clone(),
        controller.clone(),
    ));
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
    let greeks_cfg = GreeksConfig::default();
    let greeks_engine = GreeksMaterializationEngine::new(
        Arc::clone(&controller),
        config.ws_target_symbol.to_string(),
        config.env_label().to_string(),
        greeks_cfg.clone(),
        greeks_cfg.flatfile_underlying_staleness_us,
    );
    let prime_symbols = config
        .symbol_universe
        .as_ref()
        .map(|set| set.iter().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    let treasury_cfg = TreasuryEngineConfig::new(
        config.env_label(),
        config.rest_base_url.clone(),
        config.secrets.massive_api_key.clone(),
    )
    .with_prime_symbols(prime_symbols);
    let treasury_engine = TreasuryEngine::new(treasury_cfg, controller.clone());
    // Allow brute-force testing by overriding via env without rebuilding.
    // Default stays at 499 contracts/underlying (see comment in
    // trade-ws-engine/src/lib.rs) to stay under Massive's quote cap.
    let contracts_per_underlying = env::var("M17_CONTRACTS_PER_UNDERLYING")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_CONTRACTS_PER_UNDERLYING);
    info!(
        "trade-ws: using {} contracts per underlying",
        contracts_per_underlying
    );
    let trade_ws_cfg = TradeWsConfig {
        label: config.env_label().to_string(),
        state_dir: config.ledger.state_dir().to_path_buf(),
        options_ws_url: config.options_ws_url.clone(),
        stocks_ws_url: config.stocks_ws_url.clone(),
        api_key: config.secrets.massive_api_key.clone(),
        rest_base_url: config.rest_base_url.clone(),
        contracts_per_underlying,
        flush_interval: Duration::from_millis(1_000),
        window_grace: Duration::from_millis(2_000),
        contract_refresh_interval: Duration::from_secs(300),
        symbol_filter: {
            let mut set = HashSet::new();
            set.insert(config.ws_target_symbol.to_string());
            Some(set)
        },
        subscribe_all_options: false,
    };
    let trade_ws_engine = TradeWsEngine::new(
        trade_ws_cfg,
        controller.clone(),
        Arc::clone(&trade_ws_metrics),
    );
    let quote_backfill_cfg = QuoteBackfillConfig::new(
        config.env_label(),
        config.ledger.state_dir().to_path_buf(),
        config.rest_base_url.clone(),
        config.secrets.massive_api_key.clone(),
    );
    let quote_backfill_engine = QuoteBackfillEngine::new(
        quote_backfill_cfg,
        controller.clone(),
        Arc::clone(&quote_backfill_metrics),
    );

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

    start_engine_if_stopped(&treasury_engine, ENGINE_TREASURY, engine_status.as_ref())?;
    start_engine_if_stopped(&trade_ws_engine, ENGINE_TRADE_WS, engine_status.as_ref())?;
    start_engine_if_stopped(
        &quote_backfill_engine,
        ENGINE_OPTION_QUOTE_BACKFILL,
        engine_status.as_ref(),
    )?;
    start_engine_if_stopped(
        &*option_quote_engine,
        ENGINE_OPTION_QUOTE,
        engine_status.as_ref(),
    )?;
    start_engine_if_stopped(
        &*underlying_trade_engine,
        ENGINE_UNDERLYING_TRADE,
        engine_status.as_ref(),
    )?;
    start_engine_if_stopped(
        &*underlying_quote_engine,
        ENGINE_UNDERLYING_QUOTE,
        engine_status.as_ref(),
    )?;
    start_engine_if_stopped(
        &option_aggressor_engine,
        ENGINE_OPTION_AGGRESSOR,
        engine_status.as_ref(),
    )?;
    start_engine_if_stopped(
        &underlying_aggressor_engine,
        ENGINE_UNDERLYING_AGGRESSOR,
        engine_status.as_ref(),
    )?;
    start_engine_if_stopped(&gc_engine, ENGINE_GC, engine_status.as_ref())?;
    start_engine_if_stopped(&greeks_engine, ENGINE_GREEKS, engine_status.as_ref())?;
    log_engine_health("treasury", &treasury_engine);
    log_engine_health("options-ws-trade", &trade_ws_engine);
    log_engine_health("option-quote-backfill", &quote_backfill_engine);
    log_engine_health("option-quote-flatfile", &*option_quote_engine);
    log_engine_health("underlying-trade-flatfile", &*underlying_trade_engine);
    log_engine_health("underlying-quote-flatfile", &*underlying_quote_engine);
    log_engine_health("option-aggressor", &option_aggressor_engine);
    log_engine_health("underlying-aggressor", &underlying_aggressor_engine);
    log_engine_health("gc-engine", &gc_engine);
    println!("Flatfile engines are running; press Ctrl+C to shut down.");
    let _flatfile_guard = spawn_flatfile_schedule_guard(
        Arc::clone(&option_quote_engine),
        Arc::clone(&underlying_trade_engine),
        Arc::clone(&underlying_quote_engine),
        Arc::clone(&engine_status),
    );
    let aggregation_service =
        AggregationService::start(Arc::clone(&controller), config.ws_target_symbol);
    let metrics_server = MetricsServer::start(
        controller.slot_metrics(),
        download_metrics,
        Arc::clone(&controller),
        aggressor_metrics,
        trade_ws_metrics,
        quote_backfill_metrics,
        Arc::clone(&engine_status),
        config.metrics_addr,
    );
    wait_for_shutdown_signal()?;
    println!("Shutdown signal received; stopping engines...");
    stop_engine_if_running(
        &underlying_aggressor_engine,
        ENGINE_UNDERLYING_AGGRESSOR,
        engine_status.as_ref(),
    )?;
    stop_engine_if_running(
        &option_aggressor_engine,
        ENGINE_OPTION_AGGRESSOR,
        engine_status.as_ref(),
    )?;
    stop_engine_if_running(&gc_engine, ENGINE_GC, engine_status.as_ref())?;
    stop_engine_if_running(&greeks_engine, ENGINE_GREEKS, engine_status.as_ref())?;
    stop_engine_if_running(
        &*underlying_quote_engine,
        ENGINE_UNDERLYING_QUOTE,
        engine_status.as_ref(),
    )?;
    stop_engine_if_running(
        &*underlying_trade_engine,
        ENGINE_UNDERLYING_TRADE,
        engine_status.as_ref(),
    )?;
    stop_engine_if_running(
        &*option_quote_engine,
        ENGINE_OPTION_QUOTE,
        engine_status.as_ref(),
    )?;
    stop_engine_if_running(
        &quote_backfill_engine,
        ENGINE_OPTION_QUOTE_BACKFILL,
        engine_status.as_ref(),
    )?;
    stop_engine_if_running(&treasury_engine, ENGINE_TREASURY, engine_status.as_ref())?;
    stop_engine_if_running(&trade_ws_engine, ENGINE_TRADE_WS, engine_status.as_ref())?;
    aggregation_service.shutdown();
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
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    let handler_tx = tx.clone();
    let handler_flag = Arc::clone(&shutdown_requested);
    ctrlc::set_handler(move || {
        if handler_flag.swap(true, Ordering::SeqCst) {
            eprintln!("Second interrupt received; forcing shutdown");
            process::exit(130);
        }
        let _ = handler_tx.send(());
    })?;
    drop(tx);
    rx.recv()?;
    Ok(())
}

fn log_engine_health(label: &str, engine: &dyn Engine) {
    let health = engine.health();
    println!("{label} status: {:?} ({:?})", health.status, health.detail);
}

fn spawn_flatfile_schedule_guard(
    option_engine: Arc<OptionQuoteFlatfileEngine>,
    underlying_trade_engine: Arc<UnderlyingTradeFlatfileEngine>,
    underlying_quote_engine: Arc<UnderlyingQuoteFlatfileEngine>,
    engine_status: Arc<EngineStatusRegistry>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let calendar =
            TradingCalendar::new(Market::NYSE).expect("init calendar for flatfile guard");
        let tracker = engine_status.as_ref();
        loop {
            let now = Utc::now();
            let Some((should_run, next_transition)) = desired_flatfile_state(&calendar, now) else {
                log::warn!("flatfile guard missing schedule; sleeping 5 minutes");
                std::thread::sleep(Duration::from_secs(300));
                continue;
            };
            if should_run {
                if let Err(err) =
                    start_engine_if_stopped(&*option_engine, ENGINE_OPTION_QUOTE, tracker)
                {
                    log::error!("option quote flatfile start failed: {err}");
                }
                if let Err(err) = start_engine_if_stopped(
                    &*underlying_trade_engine,
                    ENGINE_UNDERLYING_TRADE,
                    tracker,
                ) {
                    log::error!("underlying trade flatfile start failed: {err}");
                }
                if let Err(err) = start_engine_if_stopped(
                    &*underlying_quote_engine,
                    ENGINE_UNDERLYING_QUOTE,
                    tracker,
                ) {
                    log::error!("underlying quote flatfile start failed: {err}");
                }
            } else {
                if let Err(err) =
                    stop_engine_if_running(&*option_engine, ENGINE_OPTION_QUOTE, tracker)
                {
                    log::error!("option quote flatfile stop failed: {err}");
                }
                if let Err(err) = stop_engine_if_running(
                    &*underlying_trade_engine,
                    ENGINE_UNDERLYING_TRADE,
                    tracker,
                ) {
                    log::error!("underlying trade flatfile stop failed: {err}");
                }
                if let Err(err) = stop_engine_if_running(
                    &*underlying_quote_engine,
                    ENGINE_UNDERLYING_QUOTE,
                    tracker,
                ) {
                    log::error!("underlying quote flatfile stop failed: {err}");
                }
            }
            let wait = next_transition
                .signed_duration_since(Utc::now())
                .to_std()
                .unwrap_or_else(|_| Duration::from_secs(5));
            let capped = wait.min(Duration::from_secs(300));
            std::thread::sleep(capped);
        }
    })
}

fn desired_flatfile_state(
    calendar: &TradingCalendar,
    now: DateTime<Utc>,
) -> Option<(bool, DateTime<Utc>)> {
    let tz = calendar.timezone();
    let local_now = now.with_timezone(&tz);
    let today = local_now.date_naive();
    if let Some((cutoff, close)) = market_window(calendar, today) {
        if now < cutoff {
            return Some((true, cutoff));
        }
        if now < close {
            return Some((false, close));
        }
        let next_day = calendar.next_trading_day(today);
        return next_cutoff_after(calendar, next_day).map(|cutoff| (true, cutoff));
    }
    let start_day = if calendar.is_trading_day(today).unwrap_or(false) {
        calendar.next_trading_day(today)
    } else {
        today
    };
    next_cutoff_after(calendar, start_day).map(|cutoff| (true, cutoff))
}

fn stop_engine_if_running(
    engine: &dyn Engine,
    label: &'static str,
    status: &EngineStatusRegistry,
) -> Result<(), EngineError> {
    match engine.stop() {
        Ok(()) => {
            status.mark_stopped(label);
            Ok(())
        }
        Err(EngineError::NotRunning) => {
            status.mark_stopped(label);
            Ok(())
        }
        Err(err) => Err(err),
    }
}

fn start_engine_if_stopped(
    engine: &dyn Engine,
    label: &'static str,
    status: &EngineStatusRegistry,
) -> Result<(), EngineError> {
    match engine.start() {
        Ok(()) => {
            status.mark_started(label);
            Ok(())
        }
        Err(EngineError::AlreadyRunning) => Ok(()),
        Err(err) => Err(err),
    }
}

fn market_window(
    calendar: &TradingCalendar,
    date: NaiveDate,
) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    if !calendar.is_trading_day(date).unwrap_or(false) {
        return None;
    }
    let hours = calendar.trading_hours(date);
    let cutoff_time = hours.regular.start - ChronoDuration::minutes(15);
    let tz = calendar.timezone();
    let cutoff_local = date.and_time(cutoff_time);
    let close_local = date.and_time(hours.market_close());
    let cutoff_utc = tz
        .from_local_datetime(&cutoff_local)
        .earliest()?
        .with_timezone(&Utc);
    let close_utc = tz
        .from_local_datetime(&close_local)
        .earliest()?
        .with_timezone(&Utc);
    Some((cutoff_utc, close_utc))
}

fn next_cutoff_after(calendar: &TradingCalendar, mut date: NaiveDate) -> Option<DateTime<Utc>> {
    for _ in 0..(5 * 365) {
        if let Some((cutoff, _)) = market_window(calendar, date) {
            return Some(cutoff);
        }
        date = calendar.next_trading_day(date);
    }
    None
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
