#[allow(dead_code)]
#[path = "../config.rs"]
mod config;

use config::{ConfigError, Environment, window_space_config_for_env};
use std::{env, process, str::FromStr};
use window_space::{
    SlotWriteError, WindowSpaceController, WindowSpaceError,
    ledger::WindowRow,
    payload::{EnrichmentSlotKind, Slot, SlotKind, SlotStatus, TradeSlotKind},
    window::WindowIndex,
};

fn main() {
    if let Err(err) = run() {
        eprintln!("windowspace_diag failed: {err}");
        process::exit(1);
    }
}

fn run() -> Result<(), DiagError> {
    let args = CliArgs::parse()?;
    let config = window_space_config_for_env(args.env);
    let (controller, _) = WindowSpaceController::bootstrap(config)?;
    let trade_space = controller.trade_window_space();
    let enrichment_space = controller.enrichment_window_space();

    let mut trade_summaries: Vec<SlotSummary> = TradeSlotKind::ALL
        .iter()
        .map(|kind| SlotSummary::new(SlotKind::Trade(*kind)))
        .collect();
    let mut enrichment_summaries: Vec<SlotSummary> = EnrichmentSlotKind::ALL
        .iter()
        .map(|kind| SlotSummary::new(SlotKind::Enrichment(*kind)))
        .collect();

    let mut matched_symbol = false;
    for (symbol_id, symbol) in controller.symbols() {
        if let Some(filter) = args.symbol.as_deref() {
            if filter != symbol {
                continue;
            }
        }
        matched_symbol = true;
        let trade_rows = trade_space.iter_symbol(symbol_id)?;
        for row in &trade_rows {
            let window_idx = row.header.window_idx;
            for kind in TradeSlotKind::ALL {
                let slot = row.slot(kind.index());
                trade_summaries[kind.index()].record(slot, &symbol, window_idx);
            }
        }
        let enrichment_rows = enrichment_space.iter_symbol(symbol_id)?;
        for row in &enrichment_rows {
            let window_idx = row.header.window_idx;
            for kind in EnrichmentSlotKind::ALL {
                let slot = row.slot(kind.index());
                enrichment_summaries[kind.index()].record(slot, &symbol, window_idx);
            }
        }
    }

    if args.symbol.is_some() && !matched_symbol {
        println!(
            "No symbol named '{}' found in the window space.",
            args.symbol.unwrap()
        );
        return Ok(());
    }

    println!("Trade slot diagnostics (env={:?}):", args.env);
    print_summaries(&trade_summaries);
    println!();
    println!("Enrichment slot diagnostics (env={:?}):", args.env);
    print_summaries(&enrichment_summaries);
    println!();
    println!(
        "Tip: any `sample` entry shows the first non-empty window (symbol@window_idx) for that slot."
    );

    Ok(())
}

fn print_summaries(summaries: &[SlotSummary]) {
    for summary in summaries {
        let total = summary.counts.total();
        if total == 0 {
            println!("  {:>22}: no windows allocated", summary.label());
            continue;
        }
        let counts = &summary.counts;
        println!(
            "  {:>22}: total={:<6} empty={:<6} pending={:<6} filled={:<6} cleared={:<6} retire={:<6} retired={:<6} sample={}",
            summary.label(),
            total,
            counts.empty,
            counts.pending,
            counts.filled,
            counts.cleared,
            counts.retire,
            counts.retired,
            summary.first_non_empty.as_deref().unwrap_or("-")
        );
    }
}

struct CliArgs {
    env: Environment,
    symbol: Option<String>,
}

impl CliArgs {
    fn parse() -> Result<Self, DiagError> {
        let mut args = env::args().skip(1);
        let env_arg = args.next().unwrap_or_else(|| "dev".to_string());
        let env = Environment::from_str(&env_arg).map_err(DiagError::Env)?;
        let mut symbol = None;
        while let Some(arg) = args.next() {
            if let Some(value) = arg.strip_prefix("--symbol=") {
                symbol = Some(value.to_string());
            } else {
                return Err(DiagError::UnknownArg(arg));
            }
        }
        Ok(Self { env, symbol })
    }
}

#[derive(Default, Clone, Copy)]
struct SlotCounts {
    empty: u64,
    pending: u64,
    filled: u64,
    cleared: u64,
    retire: u64,
    retired: u64,
}

impl SlotCounts {
    fn record(&mut self, status: SlotStatus) {
        match status {
            SlotStatus::Empty => self.empty += 1,
            SlotStatus::Pending => self.pending += 1,
            SlotStatus::Filled => self.filled += 1,
            SlotStatus::Cleared => self.cleared += 1,
            SlotStatus::Retire => self.retire += 1,
            SlotStatus::Retired => self.retired += 1,
        }
    }

    fn total(&self) -> u64 {
        self.empty + self.pending + self.filled + self.cleared + self.retire + self.retired
    }
}

struct SlotSummary {
    slot: SlotKind,
    counts: SlotCounts,
    first_non_empty: Option<String>,
}

impl SlotSummary {
    fn new(slot: SlotKind) -> Self {
        Self {
            slot,
            counts: SlotCounts::default(),
            first_non_empty: None,
        }
    }

    fn label(&self) -> &'static str {
        match self.slot {
            SlotKind::Trade(kind) => kind.label(),
            SlotKind::Enrichment(kind) => kind.label(),
        }
    }

    fn record(&mut self, slot: &Slot, symbol: &str, window_idx: WindowIndex) {
        self.counts.record(slot.status);
        if slot.status != SlotStatus::Empty && self.first_non_empty.is_none() {
            self.first_non_empty = Some(format!("{symbol}@{window_idx}"));
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum DiagError {
    #[error(transparent)]
    Window(#[from] WindowSpaceError),
    #[error(transparent)]
    Slot(#[from] SlotWriteError),
    #[error(transparent)]
    Env(#[from] ConfigError),
    #[error("unknown argument: {0}")]
    UnknownArg(String),
}
