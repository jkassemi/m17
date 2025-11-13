use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::{error::Result, symbol_map::SymbolId, window::WindowSpace};

pub const DEFAULT_MAX_SYMBOLS: SymbolId = 10_000;
pub const DEFAULT_STATE_DIR: &str = "ledger.state";

#[derive(Clone)]
pub struct LedgerConfig {
    pub state_dir: PathBuf,
    pub max_symbols: SymbolId,
    pub window_space: WindowSpace,
}

impl LedgerConfig {
    pub fn new(state_dir: PathBuf, window_space: WindowSpace) -> Self {
        Self {
            state_dir,
            max_symbols: DEFAULT_MAX_SYMBOLS,
            window_space,
        }
    }

    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    pub fn symbol_map_path(&self) -> PathBuf {
        self.state_dir.join("symbol-map.json")
    }

    pub fn trade_ledger_path(&self) -> PathBuf {
        self.state_dir.join("trade-ledger.dat")
    }

    pub fn enrichment_ledger_path(&self) -> PathBuf {
        self.state_dir.join("enrichment-ledger.dat")
    }

    pub fn ensure_dirs(&self) -> Result<()> {
        if !self.state_dir.exists() {
            fs::create_dir_all(&self.state_dir)?;
        }
        Ok(())
    }
}
