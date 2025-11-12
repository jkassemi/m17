use chrono::{NaiveDate, Utc};
use core_types::types::EnrichmentDataset;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EnrichmentRunState {
    Pending {
        run_id: String,
    },
    InFlight {
        run_id: String,
    },
    Blocked {
        reason: EnrichmentBlockedReason,
        run_id: Option<String>,
    },
    Published {
        run_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub enum EnrichmentBlockedReason {
    MissingTreasuryCurve,
    SourceFailure(Option<String>),
    WriterFailure(Option<String>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichmentCheckpoint {
    pub dataset: EnrichmentDataset,
    pub date: NaiveDate,
    pub state: EnrichmentRunState,
    pub last_updated_ns: i64,
}

#[derive(Debug, Error)]
pub enum EnrichmentStateError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

pub struct EnrichmentStateStore {
    root: PathBuf,
    lock: Mutex<()>,
}

impl EnrichmentStateStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            lock: Mutex::new(()),
        }
    }

    fn file_path(&self, dataset: EnrichmentDataset, date: NaiveDate) -> PathBuf {
        self.root
            .join(dataset.to_string())
            .join(format!("{}.json", date.format("%Y-%m-%d")))
    }

    fn ensure_parent(path: &Path) -> Result<(), std::io::Error> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn load(
        &self,
        dataset: EnrichmentDataset,
        date: NaiveDate,
    ) -> Result<Option<EnrichmentCheckpoint>, EnrichmentStateError> {
        let path = self.file_path(dataset, date);
        if !path.exists() {
            return Ok(None);
        }
        let _guard = self.lock.lock().unwrap();
        let data = fs::read_to_string(path)?;
        Ok(Some(serde_json::from_str(&data)?))
    }

    pub fn write_state(
        &self,
        dataset: EnrichmentDataset,
        date: NaiveDate,
        state: EnrichmentRunState,
    ) -> Result<EnrichmentCheckpoint, EnrichmentStateError> {
        let checkpoint = EnrichmentCheckpoint {
            dataset,
            date,
            state,
            last_updated_ns: current_time_ns(),
        };
        self.persist(&checkpoint)?;
        Ok(checkpoint)
    }

    fn persist(&self, checkpoint: &EnrichmentCheckpoint) -> Result<(), EnrichmentStateError> {
        let path = self.file_path(checkpoint.dataset, checkpoint.date);
        Self::ensure_parent(&path)?;
        let _guard = self.lock.lock().unwrap();
        let file = fs::File::create(path)?;
        serde_json::to_writer_pretty(file, checkpoint)?;
        Ok(())
    }
}

fn current_time_ns() -> i64 {
    Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn writes_and_loads_checkpoint() {
        let dir = tempdir().unwrap();
        let store = EnrichmentStateStore::new(dir.path());
        let date = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        store
            .write_state(
                EnrichmentDataset::Options,
                date,
                EnrichmentRunState::Pending {
                    run_id: "run-1".to_string(),
                },
            )
            .unwrap();
        let loaded = store
            .load(EnrichmentDataset::Options, date)
            .unwrap()
            .unwrap();
        match loaded.state {
            EnrichmentRunState::Pending { run_id } => assert_eq!(run_id, "run-1"),
            other => panic!("unexpected state {:?}", other),
        }
    }
}
