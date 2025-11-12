use std::path::PathBuf;

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
}

impl FlatfileRuntimeConfig {
    pub fn ensure_dirs(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(self.state_dir.join("trade-flatfile"))
    }

    pub fn artifact_root(&self) -> PathBuf {
        self.state_dir.join("trade-flatfile").join("artifacts")
    }
}
