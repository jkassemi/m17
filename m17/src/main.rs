mod config;

use std::{
    env, process,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use config::{AppConfig, ConfigError, Environment};
use core_types::config::DateRange;
use engine_api::{Engine, EngineError};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use trade_flatfile_engine::{
    FlatfileRuntimeConfig, OptionQuoteFlatfileEngine, OptionTradeFlatfileEngine,
    UnderlyingQuoteFlatfileEngine, UnderlyingTradeFlatfileEngine,
};
use window_space::{
    WindowSpace, WindowSpaceController, WindowSpaceError, WindowSpaceSlotStatusSnapshot,
    WindowSpaceStorageReport,
};

fn main() {
    if let Err(err) = run() {
        eprintln!("m17 failed: {err}");
        process::exit(1);
    }
}

const STATUS_LOG_INTERVAL_SECS: u64 = 30;

fn run() -> Result<(), AppError> {
    let config = {
        let env = parse_environment()?;
        AppConfig::load(env)?
    };

    config.ledger.ensure_dirs()?;
    let (controller_inner, storage_report) =
        WindowSpaceController::bootstrap(config.ledger.clone())?;
    let controller = Arc::new(controller_inner);
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
    let option_trade_engine =
        OptionTradeFlatfileEngine::new(flatfile_cfg.clone(), controller.clone());
    let option_quote_engine =
        OptionQuoteFlatfileEngine::new(flatfile_cfg.clone(), controller.clone());
    let underlying_trade_engine =
        UnderlyingTradeFlatfileEngine::new(flatfile_cfg.clone(), controller.clone());
    let underlying_quote_engine =
        UnderlyingQuoteFlatfileEngine::new(flatfile_cfg, controller.clone());

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

    option_trade_engine.start()?;
    option_quote_engine.start()?;
    underlying_trade_engine.start()?;
    underlying_quote_engine.start()?;
    log_engine_health("option-trade-flatfile", &option_trade_engine);
    log_engine_health("option-quote-flatfile", &option_quote_engine);
    log_engine_health("underlying-trade-flatfile", &underlying_trade_engine);
    log_engine_health("underlying-quote-flatfile", &underlying_quote_engine);
    println!("Flatfile engines are running; press Ctrl+C to shut down.");
    let status_logger = LedgerStatusLogger::spawn(
        controller.clone(),
        Duration::from_secs(STATUS_LOG_INTERVAL_SECS),
    );
    wait_for_shutdown_signal()?;
    println!("Shutdown signal received; stopping flatfile engines...");
    status_logger.shutdown();
    underlying_quote_engine.stop()?;
    underlying_trade_engine.stop()?;
    option_quote_engine.stop()?;
    option_trade_engine.stop()?;

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
    Ledger(#[from] WindowSpaceError),
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error("failed to install signal handler: {0}")]
    Signal(#[from] ctrlc::Error),
    #[error("failed while waiting for shutdown signal: {0}")]
    ShutdownWait(#[from] mpsc::RecvError),
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

struct LedgerStatusLogger {
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl LedgerStatusLogger {
    fn spawn(controller: Arc<WindowSpaceController>, interval: Duration) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            while !stop_clone.load(Ordering::Relaxed) {
                match controller.slot_status_snapshot() {
                    Ok(snapshot) => log_snapshot(&snapshot),
                    Err(err) => eprintln!("failed to snapshot window space slots: {err}"),
                }
                if stop_clone.load(Ordering::Relaxed) {
                    break;
                }
                sleep_with_stop(&stop_clone, interval);
            }
        });
        Self {
            stop,
            handle: Some(handle),
        }
    }

    fn shutdown(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for LedgerStatusLogger {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn log_snapshot(snapshot: &WindowSpaceSlotStatusSnapshot) {
    println!();
    println!("{snapshot}");
}

fn sleep_with_stop(stop: &AtomicBool, interval: Duration) {
    let mut remaining = interval;
    const STEP: Duration = Duration::from_millis(500);
    while remaining > Duration::ZERO {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let sleep_for = if remaining > STEP { STEP } else { remaining };
        thread::sleep(sleep_for);
        remaining = remaining.saturating_sub(sleep_for);
    }
}
