use crate::BoxError;
use chrono::NaiveDate;
use core_types::types::{EnrichmentDataset, GreeksOverlayRow};
use std::sync::{Arc, Mutex};
use storage::Storage;

pub trait GreeksOverlayWriter: Send + Sync + 'static {
    fn write_batch(
        &self,
        dataset: EnrichmentDataset,
        date: NaiveDate,
        run_id: &str,
        rows: &[GreeksOverlayRow],
    ) -> Result<(), BoxError>;

    fn finalize_run(
        &self,
        dataset: EnrichmentDataset,
        date: NaiveDate,
        run_id: &str,
    ) -> Result<(), BoxError>;
}

pub struct StorageGreeksWriter {
    storage: Arc<Mutex<Storage>>,
}

impl StorageGreeksWriter {
    pub fn new(storage: Arc<Mutex<Storage>>) -> Self {
        Self { storage }
    }
}

impl GreeksOverlayWriter for StorageGreeksWriter {
    fn write_batch(
        &self,
        dataset: EnrichmentDataset,
        date: NaiveDate,
        run_id: &str,
        rows: &[GreeksOverlayRow],
    ) -> Result<(), BoxError> {
        self.storage
            .lock()
            .unwrap()
            .write_greeks_overlays(dataset, date, run_id, rows)
            .map_err(|err| Box::new(err) as BoxError)
    }

    fn finalize_run(
        &self,
        dataset: EnrichmentDataset,
        date: NaiveDate,
        run_id: &str,
    ) -> Result<(), BoxError> {
        self.storage
            .lock()
            .unwrap()
            .finalize_manifest(dataset.dataset_name(), date, run_id)
            .map_err(|err| Box::new(err) as BoxError)
    }
}
