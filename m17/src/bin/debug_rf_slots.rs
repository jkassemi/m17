#[allow(dead_code)]
#[path = "../config.rs"]
mod config;

use config::{ConfigError, Environment, window_space_config_for_env};
use std::{env, process, str::FromStr};
use window_space::{
    SlotWriteError, WindowSpaceController, WindowSpaceError, ledger::TradeWindowRow,
    payload::SlotStatus,
};

fn main() {
    if let Err(err) = run() {
        eprintln!("debug_rf_slots failed: {err}");
        process::exit(1);
    }
}

fn run() -> Result<(), DebugError> {
    let env_arg = env::args().nth(1).unwrap_or_else(|| "dev".to_string());
    let env = Environment::from_str(&env_arg).map_err(DebugError::Env)?;
    let ledger = window_space_config_for_env(env);
    let (controller, _) = WindowSpaceController::bootstrap(ledger)?;
    let trade_space = controller.trade_window_space();
    let mut total_counts = SlotCounts::default();

    println!("Symbol rf_rate slot status counts (env={env_arg}):");
    for (symbol_id, symbol) in controller.symbols() {
        let rows = trade_space.iter_symbol(symbol_id)?;
        let counts = summarize_rf_slots(&rows);
        total_counts += counts;
        println!(
            "{symbol}: filled={}, pending={}, empty={}, other={}",
            counts.filled, counts.pending, counts.empty, counts.other
        );
    }

    println!(
        "TOTAL: filled={}, pending={}, empty={}, other={}",
        total_counts.filled, total_counts.pending, total_counts.empty, total_counts.other
    );

    Ok(())
}

fn summarize_rf_slots(rows: &[TradeWindowRow]) -> SlotCounts {
    let mut counts = SlotCounts::default();
    for row in rows {
        match row.rf_rate.status {
            SlotStatus::Filled => counts.filled += 1,
            SlotStatus::Pending => counts.pending += 1,
            SlotStatus::Empty => counts.empty += 1,
            _ => counts.other += 1,
        }
    }
    counts
}

#[derive(Default, Clone, Copy)]
struct SlotCounts {
    filled: u64,
    pending: u64,
    empty: u64,
    other: u64,
}

impl std::ops::AddAssign for SlotCounts {
    fn add_assign(&mut self, rhs: Self) {
        self.filled += rhs.filled;
        self.pending += rhs.pending;
        self.empty += rhs.empty;
        self.other += rhs.other;
    }
}

#[derive(thiserror::Error, Debug)]
enum DebugError {
    #[error(transparent)]
    Window(#[from] WindowSpaceError),
    #[error(transparent)]
    Slot(#[from] SlotWriteError),
    #[error(transparent)]
    Env(ConfigError),
}
