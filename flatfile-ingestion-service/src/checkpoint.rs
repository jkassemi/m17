// Copyright (c) James Kassemi, SC, US. All rights reserved.
use arrow::array::{Int16Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDate, Utc};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};

pub struct CheckpointManager {
    root: PathBuf,
    schema: Schema,
}

impl CheckpointManager {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let schema = Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("max_hour", DataType::Int16, false),
            Field::new("updated_ns", DataType::Int64, false),
        ]);
        Self {
            root: root.into(),
            schema,
        }
    }

    fn file_path(&self, dataset: &str, date: NaiveDate) -> PathBuf {
        self.root
            .join(dataset)
            .join(date.format("%Y-%m-%d").to_string() + ".parquet")
    }

    pub fn load(&self, dataset: &str, date: NaiveDate) -> HashMap<String, i16> {
        let path = self.file_path(dataset, date);
        if !path.exists() {
            return HashMap::new();
        }
        match self.read_file(&path) {
            Ok(map) => map,
            Err(err) => {
                log::error!("failed to read checkpoint {:?}: {}", path, err);
                HashMap::new()
            }
        }
    }

    fn read_file(&self, path: &Path) -> Result<HashMap<String, i16>, Box<dyn std::error::Error>> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let mut reader = builder.build()?;
        let mut map = HashMap::new();
        while let Some(batch) = reader.next() {
            let batch = batch?;
            let symbols = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let hours = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                map.insert(symbols.value(i).to_string(), hours.value(i));
            }
        }
        Ok(map)
    }

    pub fn save(&self, dataset: &str, date: NaiveDate, entries: &HashMap<String, i16>) {
        let path = self.file_path(dataset, date);
        if entries.is_empty() {
            if path.exists() {
                if let Err(err) = std::fs::remove_file(&path) {
                    log::error!("failed to remove empty checkpoint {:?}: {}", path, err);
                }
            }
            return;
        }
        if let Err(err) = self.write_file(&path, entries) {
            log::error!("failed to write checkpoint {:?}: {}", path, err);
        }
    }

    fn write_file(
        &self,
        path: &Path,
        entries: &HashMap<String, i16>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let symbols: Vec<String> = entries.keys().cloned().collect();
        let hours: Vec<i16> = entries.values().cloned().collect();
        let updated_ns = vec![Utc::now().timestamp_nanos_opt().unwrap_or_default(); entries.len()];
        let batch = RecordBatch::try_new(
            std::sync::Arc::new(self.schema.clone()),
            vec![
                std::sync::Arc::new(StringArray::from(symbols)),
                std::sync::Arc::new(Int16Array::from(hours)),
                std::sync::Arc::new(Int64Array::from(updated_ns)),
            ],
        )?;
        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }
}
