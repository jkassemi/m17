use std::{collections::HashSet, path::PathBuf, sync::Arc};

use crate::download_metrics::DownloadMetrics;
use core_types::config::DateRange;

/// Runtime wiring supplied by the orchestrator.
#[derive(Clone, Debug)]
pub struct FlatfileRuntimeConfig {
    pub label: &'static str,
    pub state_dir: PathBuf,
    pub bucket: String,
    pub endpoint: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub date_ranges: Vec<DateRange>,
    pub batch_size: usize,
    pub progress_update_ms: u64,
    pub progress_logging: bool,
    pub download_metrics: Arc<DownloadMetrics>,
    pub symbol_universe: Option<Arc<HashSet<String>>>,
}

impl FlatfileRuntimeConfig {
    pub fn ensure_dirs(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(self.state_dir.join("trade-flatfile"))
    }

    pub fn artifact_root(&self) -> PathBuf {
        self.state_dir.join("trade-flatfile").join("artifacts")
    }

    pub fn allows_symbol(&self, symbol: &str) -> bool {
        if let Some(filter) = &self.symbol_universe {
            filter.contains(symbol)
        } else {
            true
        }
    }
}
