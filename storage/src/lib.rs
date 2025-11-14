// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Parquet writer/reader with partitioning, compaction, and deduplication.

use arrow::array::{
    ArrayRef, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array, ListArray, StringArray,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::{Int32Type, SchemaRef};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Datelike, NaiveDate};
use core_types::schema::{
    aggregation_schema, equity_trade_schema, greeks_overlay_schema, nbbo_schema,
    option_trade_schema,
};
use core_types::types::{
    AggregationRow, Completeness, DataBatch, DataBatchMeta, EnrichmentDataset, EquityTrade,
    GreeksOverlayRow, Nbbo, OptionTrade, Quality, Source, Watermark,
};
use lru::LruCache;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use uuid::Uuid;

const GREEKS_SCHEMA_VERSION: u16 = 1;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("Schema version mismatch")]
    SchemaVersionMismatch,
    #[error("Corrupt WAL entry: {0}")]
    WalCorrupt(String),
    #[error("Manifest error: {0}")]
    Manifest(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalEntry {
    id: String,
    dataset: String,
    partition: String,
    meta: WalMeta,
    payload: WalPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalMeta {
    source: Source,
    quality: Quality,
    watermark_ts_ns: i64,
    watermark_completeness: Completeness,
    watermark_hints: Option<String>,
    schema_version: u16,
    run_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum WalPayload {
    OptionTrades { trades: Vec<OptionTrade> },
    EquityTrades { trades: Vec<EquityTrade> },
    Nbbo { quotes: Vec<Nbbo> },
    Aggregations { rows: Vec<AggregationRow> },
}

fn normalize_prefix(raw: &str, len: usize) -> String {
    let mut prefix: String = raw
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(len.max(1))
        .collect();
    if prefix.is_empty() {
        prefix.push('X');
    }
    prefix.to_ascii_uppercase()
}

fn option_partition_prefix(contract: &str) -> String {
    if let Some(rest) = contract.strip_prefix("O:") {
        let root: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphabetic())
            .collect();
        if !root.is_empty() {
            return normalize_prefix(&root, 3);
        }
    }
    normalize_prefix(contract, 3)
}

fn equity_partition_prefix(symbol: &str) -> String {
    normalize_prefix(symbol, 2)
}

fn instrument_partition_prefix(instrument: &str) -> String {
    if instrument.starts_with("O:") {
        option_partition_prefix(instrument)
    } else {
        normalize_prefix(instrument, 2)
    }
}

fn greeks_partition_path(dataset: EnrichmentDataset, date: NaiveDate, run_id: &str) -> String {
    format!(
        "{}/dt={:04}/{:02}/{:02}/run_id={}/",
        dataset.partition_root(),
        date.year(),
        date.month(),
        date.day(),
        run_id
    )
}

impl From<&DataBatchMeta> for WalMeta {
    fn from(meta: &DataBatchMeta) -> Self {
        Self {
            source: meta.source.clone(),
            quality: meta.quality.clone(),
            watermark_ts_ns: meta.watermark.watermark_ts_ns,
            watermark_completeness: meta.watermark.completeness.clone(),
            watermark_hints: meta.watermark.hints.clone(),
            schema_version: meta.schema_version,
            run_id: meta.run_id.clone(),
        }
    }
}

impl From<WalMeta> for DataBatchMeta {
    fn from(meta: WalMeta) -> Self {
        DataBatchMeta {
            source: meta.source,
            quality: meta.quality,
            watermark: Watermark {
                watermark_ts_ns: meta.watermark_ts_ns,
                completeness: meta.watermark_completeness,
                hints: meta.watermark_hints,
            },
            schema_version: meta.schema_version,
            run_id: meta.run_id,
        }
    }
}

/// Storage handles Parquet I/O with partitioning and deduplication.
pub struct Storage {
    base_path: PathBuf,
    dedup_cache: HashMap<String, LruCache<String, ()>>,
    manifest_wip: Mutex<HashMap<String, ManifestAccumulator>>,
}

impl Storage {
    pub fn new(config: core_types::config::StorageConfig) -> Self {
        let mut storage = Self {
            base_path: PathBuf::from(&config.paths.get("base").unwrap_or(&"data".to_string())),
            dedup_cache: HashMap::new(),
            manifest_wip: Mutex::new(HashMap::new()),
        };
        if let Err(err) = storage.replay_wal() {
            panic!("failed to replay WAL: {}", err);
        }
        storage
    }

    pub fn flush_all(&self) -> Result<(), StorageError> {
        Ok(())
    }

    fn wal_root(&self) -> PathBuf {
        self.base_path.join("wal")
    }

    fn append_wal_entry(
        &self,
        dataset: &str,
        partition: &str,
        meta: &DataBatchMeta,
        payload: WalPayload,
    ) -> Result<PathBuf, StorageError> {
        let entry = WalEntry {
            id: Uuid::new_v4().to_string(),
            dataset: dataset.to_string(),
            partition: partition.to_string(),
            meta: WalMeta::from(meta),
            payload,
        };
        let wal_dir = self.wal_root().join(dataset);
        std::fs::create_dir_all(&wal_dir)?;
        let path = wal_dir.join(format!("{}.json", entry.id));
        let file = std::fs::File::create(&path)?;
        serde_json::to_writer(file, &entry).map_err(|e| StorageError::WalCorrupt(e.to_string()))?;
        Ok(path)
    }

    fn commit_wal_entry(&self, path: PathBuf) -> Result<(), StorageError> {
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        Ok(())
    }

    fn replay_wal(&mut self) -> Result<(), StorageError> {
        let wal_root = self.wal_root();
        if !wal_root.exists() {
            return Ok(());
        }
        let mut pending = Vec::new();
        for dataset_entry in std::fs::read_dir(&wal_root)? {
            let dataset_path = dataset_entry?.path();
            if !dataset_path.is_dir() {
                continue;
            }
            for file in std::fs::read_dir(&dataset_path)? {
                let path = file?.path();
                if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                    continue;
                }
                pending.push(path);
            }
        }
        pending.sort();
        for path in pending {
            let data = std::fs::read_to_string(&path)?;
            let entry: WalEntry =
                serde_json::from_str(&data).map_err(|e| StorageError::WalCorrupt(e.to_string()))?;
            self.apply_wal_entry(entry)?;
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }

    fn apply_wal_entry(&mut self, entry: WalEntry) -> Result<(), StorageError> {
        let meta: DataBatchMeta = entry.meta.into();
        match entry.payload {
            WalPayload::OptionTrades { trades } => {
                let manifest_date = extract_date_from_partition(&entry.partition);
                self.write_option_partition_from_rows(
                    &entry.partition,
                    trades,
                    &meta,
                    false,
                    manifest_date,
                )?;
            }
            WalPayload::EquityTrades { trades } => {
                let manifest_date = extract_date_from_partition(&entry.partition);
                self.write_equity_partition_from_rows(
                    &entry.partition,
                    trades,
                    &meta,
                    false,
                    manifest_date,
                )?;
            }
            WalPayload::Nbbo { quotes } => {
                self.write_nbbo_from_rows(&entry.partition, quotes, &meta, false)?;
            }
            WalPayload::Aggregations { rows } => {
                self.write_aggregation_partition_from_rows(&entry.partition, rows, &meta, false)?;
            }
        }
        Ok(())
    }

    fn partition_dir(&self, rel_partition: &str) -> PathBuf {
        if rel_partition.is_empty() {
            self.base_path.clone()
        } else {
            self.base_path.join(rel_partition)
        }
    }

    fn write_option_partition_from_rows(
        &mut self,
        rel_partition: &str,
        trades: Vec<OptionTrade>,
        meta: &DataBatchMeta,
        log_to_wal: bool,
        manifest_date: Option<NaiveDate>,
    ) -> Result<(), StorageError> {
        if trades.is_empty() {
            return Ok(());
        }
        let record_batch = self.option_trades_to_record_batch(&trades, meta)?;
        let wal_path = if log_to_wal {
            let payload = WalPayload::OptionTrades {
                trades: trades.clone(),
            };
            Some(self.append_wal_entry("options_trades", rel_partition, meta, payload)?)
        } else {
            None
        };
        let file_path = self.write_partitioned(rel_partition, &record_batch)?;
        if let (Some(run_id), Some(date)) = (meta.run_id.as_deref(), manifest_date) {
            let file =
                ManifestFile::from_option_trades(self.relative_data_path(&file_path), &trades);
            self.record_manifest_file("options_trades", date, run_id, meta.schema_version, file);
        }
        if let Some(path) = wal_path {
            self.commit_wal_entry(path)?;
        }
        Ok(())
    }

    fn write_equity_partition_from_rows(
        &mut self,
        rel_partition: &str,
        trades: Vec<EquityTrade>,
        meta: &DataBatchMeta,
        log_to_wal: bool,
        manifest_date: Option<NaiveDate>,
    ) -> Result<(), StorageError> {
        if trades.is_empty() {
            return Ok(());
        }
        let record_batch = self.equity_trades_to_record_batch(&trades, meta)?;
        let wal_path = if log_to_wal {
            let payload = WalPayload::EquityTrades {
                trades: trades.clone(),
            };
            Some(self.append_wal_entry("equity_trades", rel_partition, meta, payload)?)
        } else {
            None
        };
        let file_path = self.write_partitioned(rel_partition, &record_batch)?;
        if let (Some(run_id), Some(date)) = (meta.run_id.as_deref(), manifest_date) {
            let file =
                ManifestFile::from_equity_trades(self.relative_data_path(&file_path), &trades);
            self.record_manifest_file("equity_trades", date, run_id, meta.schema_version, file);
        }
        if let Some(path) = wal_path {
            self.commit_wal_entry(path)?;
        }
        Ok(())
    }

    fn write_aggregation_partition_from_rows(
        &mut self,
        rel_partition: &str,
        rows: Vec<AggregationRow>,
        meta: &DataBatchMeta,
        log_to_wal: bool,
    ) -> Result<(), StorageError> {
        if rows.is_empty() {
            return Ok(());
        }
        let record_batch = self.aggregations_to_record_batch(&rows, meta)?;
        let wal_path = if log_to_wal {
            let payload = WalPayload::Aggregations { rows: rows.clone() };
            Some(self.append_wal_entry("aggregations", rel_partition, meta, payload)?)
        } else {
            None
        };
        let _ = self.write_partitioned(rel_partition, &record_batch)?;
        if let Some(path) = wal_path {
            self.commit_wal_entry(path)?;
        }
        Ok(())
    }

    fn write_nbbo_from_rows(
        &self,
        rel_partition: &str,
        quotes: Vec<Nbbo>,
        meta: &DataBatchMeta,
        log_to_wal: bool,
    ) -> Result<(), StorageError> {
        if quotes.is_empty() {
            return Ok(());
        }
        let record_batch = self.nbbo_to_record_batch(&quotes, meta)?;
        let wal_path = if log_to_wal {
            let payload = WalPayload::Nbbo {
                quotes: quotes.clone(),
            };
            Some(self.append_wal_entry("nbbo", rel_partition, meta, payload)?)
        } else {
            None
        };
        let _ = self.write_partitioned(rel_partition, &record_batch)?;
        if let Some(path) = wal_path {
            self.commit_wal_entry(path)?;
        }
        Ok(())
    }

    fn write_greeks_partition(
        &mut self,
        rel_partition: &str,
        rows: &[GreeksOverlayRow],
        dataset: EnrichmentDataset,
        date: NaiveDate,
        run_id: &str,
    ) -> Result<(), StorageError> {
        if rows.is_empty() {
            return Ok(());
        }
        let record_batch = self.greeks_overlays_to_record_batch(rows)?;
        let file_path = self.write_partitioned(rel_partition, &record_batch)?;
        let file = ManifestFile::from_greeks_overlays(self.relative_data_path(&file_path), rows);
        self.record_manifest_file(
            dataset.dataset_name(),
            date,
            run_id,
            GREEKS_SCHEMA_VERSION,
            file,
        );
        Ok(())
    }

    /// Write derived Greeks overlays for a given dataset/day/run.
    pub fn write_greeks_overlays(
        &mut self,
        dataset: EnrichmentDataset,
        date: NaiveDate,
        run_id: &str,
        rows: &[GreeksOverlayRow],
    ) -> Result<(), StorageError> {
        if rows.is_empty() {
            return Ok(());
        }
        let rel_partition = greeks_partition_path(dataset, date, run_id);
        self.write_greeks_partition(&rel_partition, rows, dataset, date, run_id)
    }

    /// Write a batch of option trades to Parquet, with basic dedup and pooled writers.
    pub fn write_option_trades(
        &mut self,
        batch: &DataBatch<OptionTrade>,
    ) -> Result<(), StorageError> {
        // Group trades by date
        let mut trades_by_date: HashMap<NaiveDate, Vec<OptionTrade>> = HashMap::new();
        for trade in &batch.rows {
            let dt = DateTime::from_timestamp(
                trade.trade_ts_ns / 1_000_000_000,
                (trade.trade_ts_ns % 1_000_000_000) as u32,
            )
            .unwrap()
            .naive_utc()
            .date();
            trades_by_date
                .entry(dt)
                .or_insert_with(Vec::new)
                .push(trade.clone());
        }

        for (date, trades) in trades_by_date {
            // Dedup using in-memory cache keyed by contract
            let mut deduped_trades = Vec::new();
            for trade in trades {
                let contract = &trade.contract;
                let key = format!(
                    "{}:{}:{}:{}",
                    contract, trade.trade_ts_ns, trade.price, trade.size
                );
                let cache = self
                    .dedup_cache
                    .entry(contract.clone())
                    .or_insert_with(|| LruCache::new(std::num::NonZeroUsize::new(1000).unwrap()));
                if !cache.contains(&key) {
                    cache.put(key, ());
                    deduped_trades.push(trade);
                }
            }

            if deduped_trades.is_empty() {
                continue;
            }

            let dt_str = date.to_string();
            let mut per_prefix: HashMap<String, Vec<OptionTrade>> = HashMap::new();
            for trade in deduped_trades.into_iter() {
                let prefix = option_partition_prefix(&trade.contract);
                per_prefix.entry(prefix).or_default().push(trade);
            }
            for (prefix, trades) in per_prefix {
                let rel_partition = format!("options_trades/dt={}/root={}/", dt_str, prefix);
                self.write_option_partition_from_rows(
                    &rel_partition,
                    trades,
                    &batch.meta,
                    true,
                    Some(date),
                )?;
            }
        }
        Ok(())
    }

    /// Write a batch of equity trades to Parquet.
    pub fn write_equity_trades(
        &mut self,
        batch: &DataBatch<EquityTrade>,
    ) -> Result<(), StorageError> {
        // Group trades by date
        let mut trades_by_date: HashMap<NaiveDate, Vec<EquityTrade>> = HashMap::new();
        for trade in &batch.rows {
            let dt = DateTime::from_timestamp(
                trade.trade_ts_ns / 1_000_000_000,
                (trade.trade_ts_ns % 1_000_000_000) as u32,
            )
            .unwrap()
            .naive_utc()
            .date();
            trades_by_date
                .entry(dt)
                .or_insert_with(Vec::new)
                .push(trade.clone());
        }

        // For each date group, dedup and write using a pooled writer per partition
        for (date, trades) in trades_by_date {
            // Dedup using in-memory cache
            let mut deduped_trades = Vec::new();
            for trade in trades {
                let symbol = &trade.symbol;
                let key = if let Some(ref trade_id) = trade.trade_id {
                    format!("{}:{}", symbol, trade_id)
                } else {
                    format!(
                        "{}:{}:{}:{}",
                        symbol, trade.trade_ts_ns, trade.price, trade.size
                    )
                };
                let cache = self
                    .dedup_cache
                    .entry(symbol.clone())
                    .or_insert_with(|| LruCache::new(std::num::NonZeroUsize::new(1000).unwrap()));
                if !cache.contains(&key) {
                    cache.put(key, ());
                    deduped_trades.push(trade);
                }
            }

            if deduped_trades.is_empty() {
                continue;
            }

            let dt_str = date.to_string();
            let mut per_prefix: HashMap<String, Vec<EquityTrade>> = HashMap::new();
            for trade in deduped_trades.into_iter() {
                let prefix = equity_partition_prefix(&trade.symbol);
                per_prefix.entry(prefix).or_default().push(trade);
            }
            for (prefix, trades) in per_prefix {
                let rel_partition =
                    format!("equity_trades/dt={}/symbol_prefix={}/", dt_str, prefix);
                self.write_equity_partition_from_rows(
                    &rel_partition,
                    trades,
                    &batch.meta,
                    true,
                    Some(date),
                )?;
            }
        }
        Ok(())
    }

    /// Write a batch of NBBO to Parquet (deltas only if configured).
    pub fn write_nbbo(&self, batch: &DataBatch<Nbbo>) -> Result<(), StorageError> {
        let dt = DateTime::from_timestamp(
            batch.meta.watermark.watermark_ts_ns / 1_000_000_000,
            (batch.meta.watermark.watermark_ts_ns % 1_000_000_000) as u32,
        )
        .unwrap()
        .naive_utc()
        .date();
        let dt_str = dt.to_string();
        let mut per_prefix: HashMap<String, Vec<Nbbo>> = HashMap::new();
        for quote in batch.rows.clone().into_iter() {
            let prefix = instrument_partition_prefix(&quote.instrument_id);
            per_prefix.entry(prefix).or_default().push(quote);
        }
        for (prefix, quotes) in per_prefix {
            let rel_partition = format!("nbbo/dt={}/prefix={}/", dt_str, prefix);
            self.write_nbbo_from_rows(&rel_partition, quotes, &batch.meta, true)?;
        }
        Ok(())
    }

    /// Write aggregation rows.
    pub fn write_aggregations(
        &mut self,
        batch: &DataBatch<AggregationRow>,
    ) -> Result<(), StorageError> {
        if batch.rows.is_empty() {
            return Ok(());
        }
        let mut grouped: std::collections::HashMap<
            (String, String, NaiveDate),
            Vec<AggregationRow>,
        > = std::collections::HashMap::new();
        for row in &batch.rows {
            let dt = DateTime::from_timestamp(
                row.window_start_ns / 1_000_000_000,
                (row.window_start_ns % 1_000_000_000) as u32,
            )
            .unwrap()
            .naive_utc()
            .date();
            grouped
                .entry((row.symbol.clone(), row.window.clone(), dt))
                .or_default()
                .push(row.clone());
        }
        for ((symbol, window, date), rows) in grouped {
            let rel_partition = format!(
                "aggregations/symbol={}/window={}/dt={}/",
                symbol, window, date
            );
            self.write_aggregation_partition_from_rows(&rel_partition, rows, &batch.meta, true)?;
        }
        Ok(())
    }

    /// Finalize aggressor fields for option trades (column patch: read, update, rewrite).
    pub fn finalize_option_trades(&self, _instrument: &str, _dt: &str) -> Result<(), StorageError> {
        // Stub: Read existing file, update aggressor fields, write new version.
        // In practice, this would involve reading the Parquet, applying updates from a delta file or in-memory,
        // and rewriting with new quality=Final.
        // For now, no-op.
        Ok(())
    }

    /// Finalize aggressor fields for equity trades.
    pub fn finalize_equity_trades(&self, _instrument: &str, _dt: &str) -> Result<(), StorageError> {
        // Similar to above.
        Ok(())
    }

    /// Compact partitions (merge small files).
    pub fn compact(&self, _partition: &str) -> Result<(), StorageError> {
        // Stub: List files in partition, merge if below target size.
        Ok(())
    }

    /// Offload old data to S3 (stub).
    pub fn offload_to_s3(&self, _dt: &str) -> Result<(), StorageError> {
        // Stub: Move files older than retention_weeks to S3.
        Ok(())
    }

    fn write_partitioned(
        &self,
        rel_partition: &str,
        record_batch: &RecordBatch,
    ) -> Result<PathBuf, StorageError> {
        let partition_dir = self.partition_dir(rel_partition);
        std::fs::create_dir_all(&partition_dir)?;
        let file_path = partition_dir.join(format!("data_{}.parquet", Uuid::new_v4()));
        let file = std::fs::File::create(&file_path)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();
        let mut writer = ArrowWriter::try_new(file, record_batch.schema(), Some(props))?;
        writer.write(record_batch)?;
        writer.close()?;
        Ok(file_path)
    }

    fn record_manifest_file(
        &self,
        dataset: &str,
        date: NaiveDate,
        run_id: &str,
        schema_version: u16,
        file: ManifestFile,
    ) {
        let key = manifest_key(dataset, date, run_id);
        let mut guard = self.manifest_wip.lock().unwrap();
        let entry = guard
            .entry(key)
            .or_insert_with(|| ManifestAccumulator::new(dataset, date, run_id, schema_version));
        entry.files.push(file);
    }

    fn persist_manifest(&self, acc: ManifestAccumulator) -> Result<(), StorageError> {
        let manifest_dir = self
            .base_path
            .join("manifests")
            .join(&acc.dataset)
            .join(format!("{:04}", acc.date.year()))
            .join(format!("{:02}", acc.date.month()))
            .join(format!("{:02}", acc.date.day()));
        std::fs::create_dir_all(&manifest_dir)?;
        let manifest_path = manifest_dir.join(format!("manifest-{}.json", acc.run_id));
        let manifest_doc = DatasetManifest {
            dataset: acc.dataset,
            date: acc.date.format("%Y-%m-%d").to_string(),
            run_id: acc.run_id,
            schema_version: acc.schema_version,
            generated_ns: current_time_ns(),
            files: acc.files,
        };
        let file = std::fs::File::create(&manifest_path)?;
        serde_json::to_writer_pretty(file, &manifest_doc)
            .map_err(|err| StorageError::Manifest(err.to_string()))
    }

    fn relative_data_path(&self, path: &Path) -> String {
        path.strip_prefix(&self.base_path)
            .unwrap_or(path)
            .to_string_lossy()
            .to_string()
    }

    pub fn finalize_manifest(
        &self,
        dataset: &str,
        date: NaiveDate,
        run_id: &str,
    ) -> Result<(), StorageError> {
        let key = manifest_key(dataset, date, run_id);
        let accumulator = {
            let mut guard = self.manifest_wip.lock().unwrap();
            guard.remove(&key)
        };
        if let Some(acc) = accumulator {
            if acc.files.is_empty() {
                return Ok(());
            }
            self.persist_manifest(acc)?;
        }
        Ok(())
    }

    fn option_trades_to_record_batch(
        &self,
        trades: &[OptionTrade],
        meta: &DataBatchMeta,
    ) -> Result<RecordBatch, StorageError> {
        let mut contract = Vec::new();
        let mut trade_uid_bytes: Vec<[u8; 16]> = Vec::new();
        let mut contract_direction = Vec::new();
        let mut strike_price = Vec::new();
        let mut underlying = Vec::new();
        let mut trade_ts_ns = Vec::new();
        let mut price = Vec::new();
        let mut size = Vec::new();
        let mut conditions: Vec<Vec<i32>> = Vec::new();
        let mut exchange = Vec::new();
        let mut expiry_ts_ns = Vec::new();
        let mut aggressor_side = Vec::new();
        let mut class_method = Vec::new();
        let mut aggressor_offset_mid_bp = Vec::new();
        let mut aggressor_offset_touch_ticks = Vec::new();
        let mut aggressor_confidence: Vec<Option<f64>> = Vec::new();
        let mut nbbo_bid = Vec::new();
        let mut nbbo_ask = Vec::new();
        let mut nbbo_bid_sz: Vec<Option<u32>> = Vec::new();
        let mut nbbo_ask_sz: Vec<Option<u32>> = Vec::new();
        let mut nbbo_ts_ns = Vec::new();
        let mut nbbo_age_us: Vec<Option<u32>> = Vec::new();
        let mut nbbo_state = Vec::new();
        let mut underlying_nbbo_bid = Vec::new();
        let mut underlying_nbbo_ask = Vec::new();
        let mut underlying_nbbo_bid_sz: Vec<Option<u32>> = Vec::new();
        let mut underlying_nbbo_ask_sz: Vec<Option<u32>> = Vec::new();
        let mut underlying_nbbo_ts_ns = Vec::new();
        let mut underlying_nbbo_age_us: Vec<Option<u32>> = Vec::new();
        let mut underlying_nbbo_state = Vec::new();
        let mut tick_size_used = Vec::new();
        let mut source = Vec::new();
        let mut quality = Vec::new();
        let mut watermark_ts_ns = Vec::new();

        for trade in trades {
            contract.push(trade.contract.clone());
            trade_uid_bytes.push(trade.trade_uid);
            contract_direction.push(trade.contract_direction.to_string());
            strike_price.push(trade.strike_price);
            underlying.push(trade.underlying.clone());
            trade_ts_ns.push(trade.trade_ts_ns);
            price.push(trade.price);
            size.push(trade.size);
            conditions.push(trade.conditions.clone());
            exchange.push(trade.exchange);
            expiry_ts_ns.push(trade.expiry_ts_ns);
            aggressor_side.push(format!("{:?}", trade.aggressor_side));
            class_method.push(format!("{:?}", trade.class_method));
            aggressor_offset_mid_bp.push(trade.aggressor_offset_mid_bp);
            aggressor_offset_touch_ticks.push(trade.aggressor_offset_touch_ticks);
            aggressor_confidence.push(trade.aggressor_confidence);
            nbbo_bid.push(trade.nbbo_bid);
            nbbo_ask.push(trade.nbbo_ask);
            nbbo_bid_sz.push(trade.nbbo_bid_sz);
            nbbo_ask_sz.push(trade.nbbo_ask_sz);
            nbbo_ts_ns.push(trade.nbbo_ts_ns);
            nbbo_age_us.push(trade.nbbo_age_us);
            nbbo_state.push(format!("{:?}", trade.nbbo_state));
            underlying_nbbo_bid.push(trade.underlying_nbbo_bid);
            underlying_nbbo_ask.push(trade.underlying_nbbo_ask);
            underlying_nbbo_bid_sz.push(trade.underlying_nbbo_bid_sz);
            underlying_nbbo_ask_sz.push(trade.underlying_nbbo_ask_sz);
            underlying_nbbo_ts_ns.push(trade.underlying_nbbo_ts_ns);
            underlying_nbbo_age_us.push(trade.underlying_nbbo_age_us);
            underlying_nbbo_state.push(format!("{:?}", trade.underlying_nbbo_state));
            tick_size_used.push(trade.tick_size_used);
            source.push(format!("{:?}", meta.source));
            quality.push(format!("{:?}", meta.quality));
            watermark_ts_ns.push(meta.watermark.watermark_ts_ns);
        }

        let schema: SchemaRef = Arc::new(option_trade_schema());
        let trade_uid_array =
            FixedSizeBinaryArray::try_from_iter(trade_uid_bytes.iter().map(|uid| uid.as_ref()))?;
        let conditions_array = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
            conditions
                .into_iter()
                .map(|v| Some(v.into_iter().map(Some))),
        ));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(contract)),
            Arc::new(trade_uid_array),
            Arc::new(StringArray::from(contract_direction)),
            Arc::new(Float64Array::from(strike_price)),
            Arc::new(StringArray::from(underlying)),
            Arc::new(Int64Array::from(trade_ts_ns)),
            Arc::new(Float64Array::from(price)),
            Arc::new(UInt32Array::from(size)),
            conditions_array,
            Arc::new(Int32Array::from(exchange)),
            Arc::new(Int64Array::from(expiry_ts_ns)),
            Arc::new(StringArray::from(aggressor_side)),
            Arc::new(StringArray::from(class_method)),
            Arc::new(Int32Array::from(aggressor_offset_mid_bp)),
            Arc::new(Int32Array::from(aggressor_offset_touch_ticks)),
            Arc::new(Float64Array::from(aggressor_confidence)),
            Arc::new(Float64Array::from(nbbo_bid)),
            Arc::new(Float64Array::from(nbbo_ask)),
            Arc::new(UInt32Array::from(nbbo_bid_sz)),
            Arc::new(UInt32Array::from(nbbo_ask_sz)),
            Arc::new(Int64Array::from(nbbo_ts_ns)),
            Arc::new(UInt32Array::from(nbbo_age_us)),
            Arc::new(StringArray::from(nbbo_state)),
            Arc::new(Float64Array::from(underlying_nbbo_bid)),
            Arc::new(Float64Array::from(underlying_nbbo_ask)),
            Arc::new(UInt32Array::from(underlying_nbbo_bid_sz)),
            Arc::new(UInt32Array::from(underlying_nbbo_ask_sz)),
            Arc::new(Int64Array::from(underlying_nbbo_ts_ns)),
            Arc::new(UInt32Array::from(underlying_nbbo_age_us)),
            Arc::new(StringArray::from(underlying_nbbo_state)),
            Arc::new(Float64Array::from(tick_size_used)),
            Arc::new(StringArray::from(source)),
            Arc::new(StringArray::from(quality)),
            Arc::new(Int64Array::from(watermark_ts_ns)),
        ];

        Ok(RecordBatch::try_new(schema, arrays)?)
    }

    fn greeks_overlays_to_record_batch(
        &self,
        rows: &[GreeksOverlayRow],
    ) -> Result<RecordBatch, StorageError> {
        let len = rows.len();
        let mut trade_uids = Vec::with_capacity(len);
        let mut trade_ts_ns = Vec::with_capacity(len);
        let mut contract = Vec::with_capacity(len);
        let mut underlying = Vec::with_capacity(len);
        let mut delta = Vec::with_capacity(len);
        let mut gamma = Vec::with_capacity(len);
        let mut vega = Vec::with_capacity(len);
        let mut theta = Vec::with_capacity(len);
        let mut iv = Vec::with_capacity(len);
        let mut greeks_flags = Vec::with_capacity(len);
        let mut nbbo_bid = Vec::with_capacity(len);
        let mut nbbo_ask = Vec::with_capacity(len);
        let mut nbbo_bid_sz = Vec::with_capacity(len);
        let mut nbbo_ask_sz = Vec::with_capacity(len);
        let mut nbbo_ts_ns = Vec::with_capacity(len);
        let mut nbbo_age_us = Vec::with_capacity(len);
        let mut nbbo_state = Vec::with_capacity(len);
        let mut engine_version = Vec::with_capacity(len);
        let mut curve_date = Vec::with_capacity(len);
        let mut curve_source_date = Vec::with_capacity(len);
        let mut enriched_at_ns = Vec::with_capacity(len);
        let mut run_ids = Vec::with_capacity(len);

        for row in rows {
            trade_uids.push(row.trade_uid);
            trade_ts_ns.push(row.trade_ts_ns);
            contract.push(row.contract.clone());
            underlying.push(row.underlying.clone());
            delta.push(row.delta);
            gamma.push(row.gamma);
            vega.push(row.vega);
            theta.push(row.theta);
            iv.push(row.iv);
            greeks_flags.push(row.greeks_flags);
            nbbo_bid.push(row.nbbo_bid);
            nbbo_ask.push(row.nbbo_ask);
            nbbo_bid_sz.push(row.nbbo_bid_sz);
            nbbo_ask_sz.push(row.nbbo_ask_sz);
            nbbo_ts_ns.push(row.nbbo_ts_ns);
            nbbo_age_us.push(row.nbbo_age_us);
            nbbo_state.push(row.nbbo_state.as_ref().map(|state| format!("{:?}", state)));
            engine_version.push(row.engine_version.clone());
            curve_date.push(row.treasury_curve_date.clone());
            curve_source_date.push(row.treasury_curve_source_date.clone());
            enriched_at_ns.push(row.enriched_at_ns);
            run_ids.push(row.run_id.clone());
        }

        let schema: SchemaRef = Arc::new(greeks_overlay_schema());
        let trade_uid_array =
            FixedSizeBinaryArray::try_from_iter(trade_uids.iter().map(|uid| uid.as_ref()))?;
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(trade_uid_array),
            Arc::new(Int64Array::from(trade_ts_ns)),
            Arc::new(StringArray::from(contract)),
            Arc::new(StringArray::from(underlying)),
            Arc::new(Float64Array::from(delta)),
            Arc::new(Float64Array::from(gamma)),
            Arc::new(Float64Array::from(vega)),
            Arc::new(Float64Array::from(theta)),
            Arc::new(Float64Array::from(iv)),
            Arc::new(UInt32Array::from(greeks_flags)),
            Arc::new(Float64Array::from(nbbo_bid)),
            Arc::new(Float64Array::from(nbbo_ask)),
            Arc::new(UInt32Array::from(nbbo_bid_sz)),
            Arc::new(UInt32Array::from(nbbo_ask_sz)),
            Arc::new(Int64Array::from(nbbo_ts_ns)),
            Arc::new(UInt32Array::from(nbbo_age_us)),
            Arc::new(StringArray::from(nbbo_state)),
            Arc::new(StringArray::from(engine_version)),
            Arc::new(StringArray::from(curve_date)),
            Arc::new(StringArray::from(curve_source_date)),
            Arc::new(Int64Array::from(enriched_at_ns)),
            Arc::new(StringArray::from(run_ids)),
        ];

        Ok(RecordBatch::try_new(schema, arrays)?)
    }

    fn equity_trades_to_record_batch(
        &self,
        trades: &[EquityTrade],
        meta: &DataBatchMeta,
    ) -> Result<RecordBatch, StorageError> {
        let mut symbol = Vec::new();
        let mut trade_uid_bytes: Vec<[u8; 16]> = Vec::new();
        let mut trade_ts_ns = Vec::new();
        let mut price = Vec::new();
        let mut size = Vec::new();
        let mut conditions = Vec::new();
        let mut exchange = Vec::new();
        let mut aggressor_side = Vec::new();
        let mut class_method = Vec::new();
        let mut aggressor_offset_mid_bp = Vec::new();
        let mut aggressor_offset_touch_ticks = Vec::new();
        let mut aggressor_confidence: Vec<Option<f64>> = Vec::new();
        let mut nbbo_bid = Vec::new();
        let mut nbbo_ask = Vec::new();
        let mut nbbo_bid_sz = Vec::new();
        let mut nbbo_ask_sz = Vec::new();
        let mut nbbo_ts_ns = Vec::new();
        let mut nbbo_age_us = Vec::new();
        let mut nbbo_state = Vec::new();
        let mut tick_size_used = Vec::new();
        let mut source = Vec::new();
        let mut quality = Vec::new();
        let mut watermark_ts_ns = Vec::new();
        let mut trade_id = Vec::new();
        let mut seq = Vec::new();
        let mut participant_ts_ns = Vec::new();
        let mut tape = Vec::new();
        let mut correction = Vec::new();
        let mut trf_id = Vec::new();
        let mut trf_ts_ns = Vec::new();

        for trade in trades {
            symbol.push(trade.symbol.clone());
            trade_uid_bytes.push(trade.trade_uid);
            trade_ts_ns.push(trade.trade_ts_ns);
            price.push(trade.price);
            size.push(trade.size);
            conditions.push(trade.conditions.clone());
            exchange.push(trade.exchange);
            aggressor_side.push(format!("{:?}", trade.aggressor_side));
            class_method.push(format!("{:?}", trade.class_method));
            aggressor_offset_mid_bp.push(trade.aggressor_offset_mid_bp);
            aggressor_offset_touch_ticks.push(trade.aggressor_offset_touch_ticks);
            aggressor_confidence.push(trade.aggressor_confidence);
            nbbo_bid.push(trade.nbbo_bid);
            nbbo_ask.push(trade.nbbo_ask);
            nbbo_bid_sz.push(trade.nbbo_bid_sz);
            nbbo_ask_sz.push(trade.nbbo_ask_sz);
            nbbo_ts_ns.push(trade.nbbo_ts_ns);
            nbbo_age_us.push(trade.nbbo_age_us);
            nbbo_state.push(format!("{:?}", trade.nbbo_state));
            tick_size_used.push(trade.tick_size_used);
            source.push(format!("{:?}", meta.source));
            quality.push(format!("{:?}", meta.quality));
            watermark_ts_ns.push(meta.watermark.watermark_ts_ns);
            trade_id.push(trade.trade_id.clone());
            seq.push(trade.seq);
            participant_ts_ns.push(trade.participant_ts_ns);
            tape.push(trade.tape.clone());
            correction.push(trade.correction);
            trf_id.push(trade.trf_id.clone());
            trf_ts_ns.push(trade.trf_ts_ns);
        }

        let schema: SchemaRef = Arc::new(equity_trade_schema());
        let trade_uid_array =
            FixedSizeBinaryArray::try_from_iter(trade_uid_bytes.iter().map(|uid| uid.as_ref()))?;
        let conditions_array = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
            conditions
                .into_iter()
                .map(|v| Some(v.into_iter().map(Some))),
        ));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(symbol)),
            Arc::new(trade_uid_array),
            Arc::new(Int64Array::from(trade_ts_ns)),
            Arc::new(Float64Array::from(price)),
            Arc::new(UInt32Array::from(size)),
            conditions_array,
            Arc::new(Int32Array::from(exchange)),
            Arc::new(StringArray::from(aggressor_side)),
            Arc::new(StringArray::from(class_method)),
            Arc::new(Int32Array::from(aggressor_offset_mid_bp)),
            Arc::new(Int32Array::from(aggressor_offset_touch_ticks)),
            Arc::new(Float64Array::from(aggressor_confidence)),
            Arc::new(Float64Array::from(nbbo_bid)),
            Arc::new(Float64Array::from(nbbo_ask)),
            Arc::new(UInt32Array::from(nbbo_bid_sz)),
            Arc::new(UInt32Array::from(nbbo_ask_sz)),
            Arc::new(Int64Array::from(nbbo_ts_ns)),
            Arc::new(UInt32Array::from(nbbo_age_us)),
            Arc::new(StringArray::from(nbbo_state)),
            Arc::new(Float64Array::from(tick_size_used)),
            Arc::new(StringArray::from(source)),
            Arc::new(StringArray::from(quality)),
            Arc::new(Int64Array::from(watermark_ts_ns)),
            Arc::new(StringArray::from(trade_id)),
            Arc::new(UInt64Array::from(seq)),
            Arc::new(Int64Array::from(participant_ts_ns)),
            Arc::new(StringArray::from(tape)),
            Arc::new(Int32Array::from(correction)),
            Arc::new(StringArray::from(trf_id)),
            Arc::new(Int64Array::from(trf_ts_ns)),
        ];

        Ok(RecordBatch::try_new(schema, arrays)?)
    }

    fn nbbo_to_record_batch(
        &self,
        nbbos: &[Nbbo],
        _meta: &DataBatchMeta,
    ) -> Result<RecordBatch, StorageError> {
        let len = nbbos.len();
        let mut instrument_id = Vec::with_capacity(len);
        let mut quote_uids: Vec<[u8; 16]> = Vec::with_capacity(len);
        let mut quote_ts_ns = Vec::with_capacity(len);
        let mut bid = Vec::with_capacity(len);
        let mut ask = Vec::with_capacity(len);
        let mut bid_sz = Vec::with_capacity(len);
        let mut ask_sz = Vec::with_capacity(len);
        let mut state = Vec::with_capacity(len);
        let mut condition = Vec::with_capacity(len);
        let mut best_bid_venue = Vec::with_capacity(len);
        let mut best_ask_venue = Vec::with_capacity(len);
        let mut source = Vec::with_capacity(len);
        let mut quality = Vec::with_capacity(len);
        let mut watermark_ts_ns = Vec::with_capacity(len);

        for quote in nbbos {
            instrument_id.push(quote.instrument_id.clone());
            quote_uids.push(quote.quote_uid);
            quote_ts_ns.push(quote.quote_ts_ns);
            bid.push(quote.bid);
            ask.push(quote.ask);
            bid_sz.push(quote.bid_sz);
            ask_sz.push(quote.ask_sz);
            state.push(format!("{:?}", quote.state));
            condition.push(quote.condition);
            best_bid_venue.push(quote.best_bid_venue);
            best_ask_venue.push(quote.best_ask_venue);
            source.push(format!("{:?}", quote.source));
            quality.push(format!("{:?}", quote.quality));
            watermark_ts_ns.push(quote.watermark_ts_ns);
        }

        let schema: SchemaRef = Arc::new(nbbo_schema());
        let quote_uid_array =
            FixedSizeBinaryArray::try_from_iter(quote_uids.iter().map(|uid| uid.as_ref()))?;
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(instrument_id)),
            Arc::new(quote_uid_array),
            Arc::new(Int64Array::from(quote_ts_ns)),
            Arc::new(Float64Array::from(bid)),
            Arc::new(Float64Array::from(ask)),
            Arc::new(UInt32Array::from(bid_sz)),
            Arc::new(UInt32Array::from(ask_sz)),
            Arc::new(StringArray::from(state)),
            Arc::new(Int32Array::from(condition)),
            Arc::new(Int32Array::from(best_bid_venue)),
            Arc::new(Int32Array::from(best_ask_venue)),
            Arc::new(StringArray::from(source)),
            Arc::new(StringArray::from(quality)),
            Arc::new(Int64Array::from(watermark_ts_ns)),
        ];
        Ok(RecordBatch::try_new(schema, arrays)?)
    }

    fn aggregations_to_record_batch(
        &self,
        rows: &[AggregationRow],
        _meta: &DataBatchMeta,
    ) -> Result<RecordBatch, StorageError> {
        let len = rows.len();
        let mut symbols = Vec::with_capacity(len);
        let mut windows = Vec::with_capacity(len);
        let mut window_start_ns = Vec::with_capacity(len);
        let mut window_end_ns = Vec::with_capacity(len);
        macro_rules! init_float_cols {
            ($($name:ident),+) => {
                $(let mut $name: Vec<Option<f64>> = Vec::with_capacity(len);)+
            }
        }
        init_float_cols!(
            underlying_price_open,
            underlying_price_high,
            underlying_price_low,
            underlying_price_close,
            underlying_dollar_value_total,
            underlying_dollar_value_minimum,
            underlying_dollar_value_maximum,
            underlying_dollar_value_mean,
            underlying_dollar_value_stddev,
            underlying_dollar_value_skew,
            underlying_dollar_value_kurtosis,
            underlying_dollar_value_iqr,
            underlying_dollar_value_mad,
            underlying_dollar_value_cv,
            underlying_dollar_value_mode,
            underlying_dollar_value_bc,
            underlying_dollar_value_dip_pval,
            underlying_dollar_value_kde_peaks,
            puts_below_intrinsic_pct,
            puts_above_intrinsic_pct,
            puts_aggressor_unknown_pct,
            puts_classifier_touch_pct,
            puts_classifier_at_or_beyond_pct,
            puts_classifier_tick_rule_pct,
            puts_classifier_unknown_pct,
            puts_dadvv_total,
            puts_dadvv_minimum,
            puts_dadvv_maximum,
            puts_dadvv_mean,
            puts_dadvv_stddev,
            puts_dadvv_skew,
            puts_dadvv_kurtosis,
            puts_dadvv_iqr,
            puts_dadvv_mad,
            puts_dadvv_cv,
            puts_dadvv_mode,
            puts_dadvv_bc,
            puts_dadvv_dip_pval,
            puts_dadvv_kde_peaks,
            puts_signed_dadvv_total,
            puts_signed_dadvv_minimum,
            puts_signed_dadvv_maximum,
            puts_signed_dadvv_mean,
            puts_signed_dadvv_stddev,
            puts_signed_dadvv_skew,
            puts_signed_dadvv_kurtosis,
            puts_signed_dadvv_iqr,
            puts_signed_dadvv_mad,
            puts_signed_dadvv_cv,
            puts_signed_dadvv_mode,
            puts_signed_dadvv_bc,
            puts_signed_dadvv_dip_pval,
            puts_signed_dadvv_kde_peaks,
            puts_gadvv_total,
            puts_gadvv_minimum,
            puts_gadvv_maximum,
            puts_gadvv_mean,
            puts_gadvv_stddev,
            puts_gadvv_skew,
            puts_gadvv_kurtosis,
            puts_gadvv_iqr,
            puts_gadvv_mad,
            puts_gadvv_cv,
            puts_gadvv_mode,
            puts_gadvv_bc,
            puts_gadvv_dip_pval,
            puts_gadvv_kde_peaks,
            puts_signed_gadvv_total,
            puts_signed_gadvv_minimum,
            puts_signed_gadvv_maximum,
            puts_signed_gadvv_mean,
            puts_signed_gadvv_stddev,
            puts_signed_gadvv_skew,
            puts_signed_gadvv_kurtosis,
            puts_signed_gadvv_iqr,
            puts_signed_gadvv_mad,
            puts_signed_gadvv_cv,
            puts_signed_gadvv_mode,
            puts_signed_gadvv_bc,
            puts_signed_gadvv_dip_pval,
            puts_signed_gadvv_kde_peaks,
            calls_dollar_value,
            calls_above_intrinsic_pct,
            calls_aggressor_unknown_pct,
            calls_classifier_touch_pct,
            calls_classifier_at_or_beyond_pct,
            calls_classifier_tick_rule_pct,
            calls_classifier_unknown_pct,
            calls_dadvv_total,
            calls_dadvv_minimum,
            calls_dadvv_maximum,
            calls_dadvv_mean,
            calls_dadvv_stddev,
            calls_dadvv_skew,
            calls_dadvv_kurtosis,
            calls_dadvv_iqr,
            calls_dadvv_mad,
            calls_dadvv_cv,
            calls_dadvv_mode,
            calls_dadvv_bc,
            calls_dadvv_dip_pval,
            calls_dadvv_kde_peaks,
            calls_signed_dadvv_total,
            calls_signed_dadvv_minimum,
            calls_signed_dadvv_maximum,
            calls_signed_dadvv_mean,
            calls_signed_dadvv_stddev,
            calls_signed_dadvv_skew,
            calls_signed_dadvv_kurtosis,
            calls_signed_dadvv_iqr,
            calls_signed_dadvv_mad,
            calls_signed_dadvv_cv,
            calls_signed_dadvv_mode,
            calls_signed_dadvv_bc,
            calls_signed_dadvv_dip_pval,
            calls_signed_dadvv_kde_peaks,
            calls_gadvv_total,
            calls_gadvv_minimum,
            calls_gadvv_q1,
            calls_gadvv_q2,
            calls_gadvv_q3,
            calls_gadvv_maximum,
            calls_gadvv_mean,
            calls_gadvv_stddev,
            calls_gadvv_skew,
            calls_gadvv_kurtosis,
            calls_gadvv_iqr,
            calls_gadvv_mad,
            calls_gadvv_cv,
            calls_gadvv_mode,
            calls_gadvv_bc,
            calls_gadvv_dip_pval,
            calls_gadvv_kde_peaks,
            calls_signed_gadvv_total,
            calls_signed_gadvv_minimum,
            calls_signed_gadvv_q1,
            calls_signed_gadvv_q2,
            calls_signed_gadvv_q3,
            calls_signed_gadvv_maximum,
            calls_signed_gadvv_mean,
            calls_signed_gadvv_stddev,
            calls_signed_gadvv_skew,
            calls_signed_gadvv_kurtosis,
            calls_signed_gadvv_iqr,
            calls_signed_gadvv_mad,
            calls_signed_gadvv_cv,
            calls_signed_gadvv_mode,
            calls_signed_gadvv_bc,
            calls_signed_gadvv_dip_pval,
            calls_signed_gadvv_kde_peaks
        );
        for row in rows {
            symbols.push(row.symbol.clone());
            windows.push(row.window.clone());
            window_start_ns.push(row.window_start_ns);
            window_end_ns.push(row.window_end_ns);
            underlying_price_open.push(row.underlying_price_open);
            underlying_price_high.push(row.underlying_price_high);
            underlying_price_low.push(row.underlying_price_low);
            underlying_price_close.push(row.underlying_price_close);
            underlying_dollar_value_total.push(row.underlying_dollar_value_total);
            underlying_dollar_value_minimum.push(row.underlying_dollar_value_minimum);
            underlying_dollar_value_maximum.push(row.underlying_dollar_value_maximum);
            underlying_dollar_value_mean.push(row.underlying_dollar_value_mean);
            underlying_dollar_value_stddev.push(row.underlying_dollar_value_stddev);
            underlying_dollar_value_skew.push(row.underlying_dollar_value_skew);
            underlying_dollar_value_kurtosis.push(row.underlying_dollar_value_kurtosis);
            underlying_dollar_value_iqr.push(row.underlying_dollar_value_iqr);
            underlying_dollar_value_mad.push(row.underlying_dollar_value_mad);
            underlying_dollar_value_cv.push(row.underlying_dollar_value_cv);
            underlying_dollar_value_mode.push(row.underlying_dollar_value_mode);
            underlying_dollar_value_bc.push(row.underlying_dollar_value_bc);
            underlying_dollar_value_dip_pval.push(row.underlying_dollar_value_dip_pval);
            underlying_dollar_value_kde_peaks.push(row.underlying_dollar_value_kde_peaks);
            puts_below_intrinsic_pct.push(row.puts_below_intrinsic_pct);
            puts_above_intrinsic_pct.push(row.puts_above_intrinsic_pct);
            puts_aggressor_unknown_pct.push(row.puts_aggressor_unknown_pct);
            puts_classifier_touch_pct.push(row.puts_classifier_touch_pct);
            puts_classifier_at_or_beyond_pct.push(row.puts_classifier_at_or_beyond_pct);
            puts_classifier_tick_rule_pct.push(row.puts_classifier_tick_rule_pct);
            puts_classifier_unknown_pct.push(row.puts_classifier_unknown_pct);
            puts_dadvv_total.push(row.puts_dadvv_total);
            puts_dadvv_minimum.push(row.puts_dadvv_minimum);
            puts_dadvv_maximum.push(row.puts_dadvv_maximum);
            puts_dadvv_mean.push(row.puts_dadvv_mean);
            puts_dadvv_stddev.push(row.puts_dadvv_stddev);
            puts_dadvv_skew.push(row.puts_dadvv_skew);
            puts_dadvv_kurtosis.push(row.puts_dadvv_kurtosis);
            puts_dadvv_iqr.push(row.puts_dadvv_iqr);
            puts_dadvv_mad.push(row.puts_dadvv_mad);
            puts_dadvv_cv.push(row.puts_dadvv_cv);
            puts_dadvv_mode.push(row.puts_dadvv_mode);
            puts_dadvv_bc.push(row.puts_dadvv_bc);
            puts_dadvv_dip_pval.push(row.puts_dadvv_dip_pval);
            puts_dadvv_kde_peaks.push(row.puts_dadvv_kde_peaks);
            puts_signed_dadvv_total.push(row.puts_signed_dadvv_total);
            puts_signed_dadvv_minimum.push(row.puts_signed_dadvv_minimum);
            puts_signed_dadvv_maximum.push(row.puts_signed_dadvv_maximum);
            puts_signed_dadvv_mean.push(row.puts_signed_dadvv_mean);
            puts_signed_dadvv_stddev.push(row.puts_signed_dadvv_stddev);
            puts_signed_dadvv_skew.push(row.puts_signed_dadvv_skew);
            puts_signed_dadvv_kurtosis.push(row.puts_signed_dadvv_kurtosis);
            puts_signed_dadvv_iqr.push(row.puts_signed_dadvv_iqr);
            puts_signed_dadvv_mad.push(row.puts_signed_dadvv_mad);
            puts_signed_dadvv_cv.push(row.puts_signed_dadvv_cv);
            puts_signed_dadvv_mode.push(row.puts_signed_dadvv_mode);
            puts_signed_dadvv_bc.push(row.puts_signed_dadvv_bc);
            puts_signed_dadvv_dip_pval.push(row.puts_signed_dadvv_dip_pval);
            puts_signed_dadvv_kde_peaks.push(row.puts_signed_dadvv_kde_peaks);
            puts_gadvv_total.push(row.puts_gadvv_total);
            puts_gadvv_minimum.push(row.puts_gadvv_minimum);
            puts_gadvv_maximum.push(row.puts_gadvv_maximum);
            puts_gadvv_mean.push(row.puts_gadvv_mean);
            puts_gadvv_stddev.push(row.puts_gadvv_stddev);
            puts_gadvv_skew.push(row.puts_gadvv_skew);
            puts_gadvv_kurtosis.push(row.puts_gadvv_kurtosis);
            puts_gadvv_iqr.push(row.puts_gadvv_iqr);
            puts_gadvv_mad.push(row.puts_gadvv_mad);
            puts_gadvv_cv.push(row.puts_gadvv_cv);
            puts_gadvv_mode.push(row.puts_gadvv_mode);
            puts_gadvv_bc.push(row.puts_gadvv_bc);
            puts_gadvv_dip_pval.push(row.puts_gadvv_dip_pval);
            puts_gadvv_kde_peaks.push(row.puts_gadvv_kde_peaks);
            puts_signed_gadvv_total.push(row.puts_signed_gadvv_total);
            puts_signed_gadvv_minimum.push(row.puts_signed_gadvv_minimum);
            puts_signed_gadvv_maximum.push(row.puts_signed_gadvv_maximum);
            puts_signed_gadvv_mean.push(row.puts_signed_gadvv_mean);
            puts_signed_gadvv_stddev.push(row.puts_signed_gadvv_stddev);
            puts_signed_gadvv_skew.push(row.puts_signed_gadvv_skew);
            puts_signed_gadvv_kurtosis.push(row.puts_signed_gadvv_kurtosis);
            puts_signed_gadvv_iqr.push(row.puts_signed_gadvv_iqr);
            puts_signed_gadvv_mad.push(row.puts_signed_gadvv_mad);
            puts_signed_gadvv_cv.push(row.puts_signed_gadvv_cv);
            puts_signed_gadvv_mode.push(row.puts_signed_gadvv_mode);
            puts_signed_gadvv_bc.push(row.puts_signed_gadvv_bc);
            puts_signed_gadvv_dip_pval.push(row.puts_signed_gadvv_dip_pval);
            puts_signed_gadvv_kde_peaks.push(row.puts_signed_gadvv_kde_peaks);
            calls_dollar_value.push(row.calls_dollar_value);
            calls_above_intrinsic_pct.push(row.calls_above_intrinsic_pct);
            calls_aggressor_unknown_pct.push(row.calls_aggressor_unknown_pct);
            calls_classifier_touch_pct.push(row.calls_classifier_touch_pct);
            calls_classifier_at_or_beyond_pct.push(row.calls_classifier_at_or_beyond_pct);
            calls_classifier_tick_rule_pct.push(row.calls_classifier_tick_rule_pct);
            calls_classifier_unknown_pct.push(row.calls_classifier_unknown_pct);
            calls_dadvv_total.push(row.calls_dadvv_total);
            calls_dadvv_minimum.push(row.calls_dadvv_minimum);
            calls_dadvv_maximum.push(row.calls_dadvv_maximum);
            calls_dadvv_mean.push(row.calls_dadvv_mean);
            calls_dadvv_stddev.push(row.calls_dadvv_stddev);
            calls_dadvv_skew.push(row.calls_dadvv_skew);
            calls_dadvv_kurtosis.push(row.calls_dadvv_kurtosis);
            calls_dadvv_iqr.push(row.calls_dadvv_iqr);
            calls_dadvv_mad.push(row.calls_dadvv_mad);
            calls_dadvv_cv.push(row.calls_dadvv_cv);
            calls_dadvv_mode.push(row.calls_dadvv_mode);
            calls_dadvv_bc.push(row.calls_dadvv_bc);
            calls_dadvv_dip_pval.push(row.calls_dadvv_dip_pval);
            calls_dadvv_kde_peaks.push(row.calls_dadvv_kde_peaks);
            calls_signed_dadvv_total.push(row.calls_signed_dadvv_total);
            calls_signed_dadvv_minimum.push(row.calls_signed_dadvv_minimum);
            calls_signed_dadvv_maximum.push(row.calls_signed_dadvv_maximum);
            calls_signed_dadvv_mean.push(row.calls_signed_dadvv_mean);
            calls_signed_dadvv_stddev.push(row.calls_signed_dadvv_stddev);
            calls_signed_dadvv_skew.push(row.calls_signed_dadvv_skew);
            calls_signed_dadvv_kurtosis.push(row.calls_signed_dadvv_kurtosis);
            calls_signed_dadvv_iqr.push(row.calls_signed_dadvv_iqr);
            calls_signed_dadvv_mad.push(row.calls_signed_dadvv_mad);
            calls_signed_dadvv_cv.push(row.calls_signed_dadvv_cv);
            calls_signed_dadvv_mode.push(row.calls_signed_dadvv_mode);
            calls_signed_dadvv_bc.push(row.calls_signed_dadvv_bc);
            calls_signed_dadvv_dip_pval.push(row.calls_signed_dadvv_dip_pval);
            calls_signed_dadvv_kde_peaks.push(row.calls_signed_dadvv_kde_peaks);
            calls_gadvv_total.push(row.calls_gadvv_total);
            calls_gadvv_minimum.push(row.calls_gadvv_minimum);
            calls_gadvv_q1.push(row.calls_gadvv_q1);
            calls_gadvv_q2.push(row.calls_gadvv_q2);
            calls_gadvv_q3.push(row.calls_gadvv_q3);
            calls_gadvv_maximum.push(row.calls_gadvv_maximum);
            calls_gadvv_mean.push(row.calls_gadvv_mean);
            calls_gadvv_stddev.push(row.calls_gadvv_stddev);
            calls_gadvv_skew.push(row.calls_gadvv_skew);
            calls_gadvv_kurtosis.push(row.calls_gadvv_kurtosis);
            calls_gadvv_iqr.push(row.calls_gadvv_iqr);
            calls_gadvv_mad.push(row.calls_gadvv_mad);
            calls_gadvv_cv.push(row.calls_gadvv_cv);
            calls_gadvv_mode.push(row.calls_gadvv_mode);
            calls_gadvv_bc.push(row.calls_gadvv_bc);
            calls_gadvv_dip_pval.push(row.calls_gadvv_dip_pval);
            calls_gadvv_kde_peaks.push(row.calls_gadvv_kde_peaks);
            calls_signed_gadvv_total.push(row.calls_signed_gadvv_total);
            calls_signed_gadvv_minimum.push(row.calls_signed_gadvv_minimum);
            calls_signed_gadvv_q1.push(row.calls_signed_gadvv_q1);
            calls_signed_gadvv_q2.push(row.calls_signed_gadvv_q2);
            calls_signed_gadvv_q3.push(row.calls_signed_gadvv_q3);
            calls_signed_gadvv_maximum.push(row.calls_signed_gadvv_maximum);
            calls_signed_gadvv_mean.push(row.calls_signed_gadvv_mean);
            calls_signed_gadvv_stddev.push(row.calls_signed_gadvv_stddev);
            calls_signed_gadvv_skew.push(row.calls_signed_gadvv_skew);
            calls_signed_gadvv_kurtosis.push(row.calls_signed_gadvv_kurtosis);
            calls_signed_gadvv_iqr.push(row.calls_signed_gadvv_iqr);
            calls_signed_gadvv_mad.push(row.calls_signed_gadvv_mad);
            calls_signed_gadvv_cv.push(row.calls_signed_gadvv_cv);
            calls_signed_gadvv_mode.push(row.calls_signed_gadvv_mode);
            calls_signed_gadvv_bc.push(row.calls_signed_gadvv_bc);
            calls_signed_gadvv_dip_pval.push(row.calls_signed_gadvv_dip_pval);
            calls_signed_gadvv_kde_peaks.push(row.calls_signed_gadvv_kde_peaks);
        }
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(symbols)),
            Arc::new(StringArray::from(windows)),
            Arc::new(Int64Array::from(window_start_ns)),
            Arc::new(Int64Array::from(window_end_ns)),
            Arc::new(Float64Array::from(underlying_price_open)),
            Arc::new(Float64Array::from(underlying_price_high)),
            Arc::new(Float64Array::from(underlying_price_low)),
            Arc::new(Float64Array::from(underlying_price_close)),
            Arc::new(Float64Array::from(underlying_dollar_value_total)),
            Arc::new(Float64Array::from(underlying_dollar_value_minimum)),
            Arc::new(Float64Array::from(underlying_dollar_value_maximum)),
            Arc::new(Float64Array::from(underlying_dollar_value_mean)),
            Arc::new(Float64Array::from(underlying_dollar_value_stddev)),
            Arc::new(Float64Array::from(underlying_dollar_value_skew)),
            Arc::new(Float64Array::from(underlying_dollar_value_kurtosis)),
            Arc::new(Float64Array::from(underlying_dollar_value_iqr)),
            Arc::new(Float64Array::from(underlying_dollar_value_mad)),
            Arc::new(Float64Array::from(underlying_dollar_value_cv)),
            Arc::new(Float64Array::from(underlying_dollar_value_mode)),
            Arc::new(Float64Array::from(underlying_dollar_value_bc)),
            Arc::new(Float64Array::from(underlying_dollar_value_dip_pval)),
            Arc::new(Float64Array::from(underlying_dollar_value_kde_peaks)),
            Arc::new(Float64Array::from(puts_below_intrinsic_pct)),
            Arc::new(Float64Array::from(puts_above_intrinsic_pct)),
            Arc::new(Float64Array::from(puts_aggressor_unknown_pct)),
            Arc::new(Float64Array::from(puts_classifier_touch_pct)),
            Arc::new(Float64Array::from(puts_classifier_at_or_beyond_pct)),
            Arc::new(Float64Array::from(puts_classifier_tick_rule_pct)),
            Arc::new(Float64Array::from(puts_classifier_unknown_pct)),
            Arc::new(Float64Array::from(puts_dadvv_total)),
            Arc::new(Float64Array::from(puts_dadvv_minimum)),
            Arc::new(Float64Array::from(puts_dadvv_maximum)),
            Arc::new(Float64Array::from(puts_dadvv_mean)),
            Arc::new(Float64Array::from(puts_dadvv_stddev)),
            Arc::new(Float64Array::from(puts_dadvv_skew)),
            Arc::new(Float64Array::from(puts_dadvv_kurtosis)),
            Arc::new(Float64Array::from(puts_dadvv_iqr)),
            Arc::new(Float64Array::from(puts_dadvv_mad)),
            Arc::new(Float64Array::from(puts_dadvv_cv)),
            Arc::new(Float64Array::from(puts_dadvv_mode)),
            Arc::new(Float64Array::from(puts_dadvv_bc)),
            Arc::new(Float64Array::from(puts_dadvv_dip_pval)),
            Arc::new(Float64Array::from(puts_dadvv_kde_peaks)),
            Arc::new(Float64Array::from(puts_signed_dadvv_total)),
            Arc::new(Float64Array::from(puts_signed_dadvv_minimum)),
            Arc::new(Float64Array::from(puts_signed_dadvv_maximum)),
            Arc::new(Float64Array::from(puts_signed_dadvv_mean)),
            Arc::new(Float64Array::from(puts_signed_dadvv_stddev)),
            Arc::new(Float64Array::from(puts_signed_dadvv_skew)),
            Arc::new(Float64Array::from(puts_signed_dadvv_kurtosis)),
            Arc::new(Float64Array::from(puts_signed_dadvv_iqr)),
            Arc::new(Float64Array::from(puts_signed_dadvv_mad)),
            Arc::new(Float64Array::from(puts_signed_dadvv_cv)),
            Arc::new(Float64Array::from(puts_signed_dadvv_mode)),
            Arc::new(Float64Array::from(puts_signed_dadvv_bc)),
            Arc::new(Float64Array::from(puts_signed_dadvv_dip_pval)),
            Arc::new(Float64Array::from(puts_signed_dadvv_kde_peaks)),
            Arc::new(Float64Array::from(puts_gadvv_total)),
            Arc::new(Float64Array::from(puts_gadvv_minimum)),
            Arc::new(Float64Array::from(puts_gadvv_maximum)),
            Arc::new(Float64Array::from(puts_gadvv_mean)),
            Arc::new(Float64Array::from(puts_gadvv_stddev)),
            Arc::new(Float64Array::from(puts_gadvv_skew)),
            Arc::new(Float64Array::from(puts_gadvv_kurtosis)),
            Arc::new(Float64Array::from(puts_gadvv_iqr)),
            Arc::new(Float64Array::from(puts_gadvv_mad)),
            Arc::new(Float64Array::from(puts_gadvv_cv)),
            Arc::new(Float64Array::from(puts_gadvv_mode)),
            Arc::new(Float64Array::from(puts_gadvv_bc)),
            Arc::new(Float64Array::from(puts_gadvv_dip_pval)),
            Arc::new(Float64Array::from(puts_gadvv_kde_peaks)),
            Arc::new(Float64Array::from(puts_signed_gadvv_total)),
            Arc::new(Float64Array::from(puts_signed_gadvv_minimum)),
            Arc::new(Float64Array::from(puts_signed_gadvv_maximum)),
            Arc::new(Float64Array::from(puts_signed_gadvv_mean)),
            Arc::new(Float64Array::from(puts_signed_gadvv_stddev)),
            Arc::new(Float64Array::from(puts_signed_gadvv_skew)),
            Arc::new(Float64Array::from(puts_signed_gadvv_kurtosis)),
            Arc::new(Float64Array::from(puts_signed_gadvv_iqr)),
            Arc::new(Float64Array::from(puts_signed_gadvv_mad)),
            Arc::new(Float64Array::from(puts_signed_gadvv_cv)),
            Arc::new(Float64Array::from(puts_signed_gadvv_mode)),
            Arc::new(Float64Array::from(puts_signed_gadvv_bc)),
            Arc::new(Float64Array::from(puts_signed_gadvv_dip_pval)),
            Arc::new(Float64Array::from(puts_signed_gadvv_kde_peaks)),
            Arc::new(Float64Array::from(calls_dollar_value)),
            Arc::new(Float64Array::from(calls_above_intrinsic_pct)),
            Arc::new(Float64Array::from(calls_aggressor_unknown_pct)),
            Arc::new(Float64Array::from(calls_classifier_touch_pct)),
            Arc::new(Float64Array::from(calls_classifier_at_or_beyond_pct)),
            Arc::new(Float64Array::from(calls_classifier_tick_rule_pct)),
            Arc::new(Float64Array::from(calls_classifier_unknown_pct)),
            Arc::new(Float64Array::from(calls_dadvv_total)),
            Arc::new(Float64Array::from(calls_dadvv_minimum)),
            Arc::new(Float64Array::from(calls_dadvv_maximum)),
            Arc::new(Float64Array::from(calls_dadvv_mean)),
            Arc::new(Float64Array::from(calls_dadvv_stddev)),
            Arc::new(Float64Array::from(calls_dadvv_skew)),
            Arc::new(Float64Array::from(calls_dadvv_kurtosis)),
            Arc::new(Float64Array::from(calls_dadvv_iqr)),
            Arc::new(Float64Array::from(calls_dadvv_mad)),
            Arc::new(Float64Array::from(calls_dadvv_cv)),
            Arc::new(Float64Array::from(calls_dadvv_mode)),
            Arc::new(Float64Array::from(calls_dadvv_bc)),
            Arc::new(Float64Array::from(calls_dadvv_dip_pval)),
            Arc::new(Float64Array::from(calls_dadvv_kde_peaks)),
            Arc::new(Float64Array::from(calls_signed_dadvv_total)),
            Arc::new(Float64Array::from(calls_signed_dadvv_minimum)),
            Arc::new(Float64Array::from(calls_signed_dadvv_maximum)),
            Arc::new(Float64Array::from(calls_signed_dadvv_mean)),
            Arc::new(Float64Array::from(calls_signed_dadvv_stddev)),
            Arc::new(Float64Array::from(calls_signed_dadvv_skew)),
            Arc::new(Float64Array::from(calls_signed_dadvv_kurtosis)),
            Arc::new(Float64Array::from(calls_signed_dadvv_iqr)),
            Arc::new(Float64Array::from(calls_signed_dadvv_mad)),
            Arc::new(Float64Array::from(calls_signed_dadvv_cv)),
            Arc::new(Float64Array::from(calls_signed_dadvv_mode)),
            Arc::new(Float64Array::from(calls_signed_dadvv_bc)),
            Arc::new(Float64Array::from(calls_signed_dadvv_dip_pval)),
            Arc::new(Float64Array::from(calls_signed_dadvv_kde_peaks)),
            Arc::new(Float64Array::from(calls_gadvv_total)),
            Arc::new(Float64Array::from(calls_gadvv_minimum)),
            Arc::new(Float64Array::from(calls_gadvv_q1)),
            Arc::new(Float64Array::from(calls_gadvv_q2)),
            Arc::new(Float64Array::from(calls_gadvv_q3)),
            Arc::new(Float64Array::from(calls_gadvv_maximum)),
            Arc::new(Float64Array::from(calls_gadvv_mean)),
            Arc::new(Float64Array::from(calls_gadvv_stddev)),
            Arc::new(Float64Array::from(calls_gadvv_skew)),
            Arc::new(Float64Array::from(calls_gadvv_kurtosis)),
            Arc::new(Float64Array::from(calls_gadvv_iqr)),
            Arc::new(Float64Array::from(calls_gadvv_mad)),
            Arc::new(Float64Array::from(calls_gadvv_cv)),
            Arc::new(Float64Array::from(calls_gadvv_mode)),
            Arc::new(Float64Array::from(calls_gadvv_bc)),
            Arc::new(Float64Array::from(calls_gadvv_dip_pval)),
            Arc::new(Float64Array::from(calls_gadvv_kde_peaks)),
            Arc::new(Float64Array::from(calls_signed_gadvv_total)),
            Arc::new(Float64Array::from(calls_signed_gadvv_minimum)),
            Arc::new(Float64Array::from(calls_signed_gadvv_q1)),
            Arc::new(Float64Array::from(calls_signed_gadvv_q2)),
            Arc::new(Float64Array::from(calls_signed_gadvv_q3)),
            Arc::new(Float64Array::from(calls_signed_gadvv_maximum)),
            Arc::new(Float64Array::from(calls_signed_gadvv_mean)),
            Arc::new(Float64Array::from(calls_signed_gadvv_stddev)),
            Arc::new(Float64Array::from(calls_signed_gadvv_skew)),
            Arc::new(Float64Array::from(calls_signed_gadvv_kurtosis)),
            Arc::new(Float64Array::from(calls_signed_gadvv_iqr)),
            Arc::new(Float64Array::from(calls_signed_gadvv_mad)),
            Arc::new(Float64Array::from(calls_signed_gadvv_cv)),
            Arc::new(Float64Array::from(calls_signed_gadvv_mode)),
            Arc::new(Float64Array::from(calls_signed_gadvv_bc)),
            Arc::new(Float64Array::from(calls_signed_gadvv_dip_pval)),
            Arc::new(Float64Array::from(calls_signed_gadvv_kde_peaks)),
        ];
        RecordBatch::try_new(Arc::new(aggregation_schema()), arrays).map_err(StorageError::from)
    }
}

#[derive(Clone, Serialize)]
struct ManifestFile {
    relative_path: String,
    rows: usize,
    min_ts_ns: i64,
    max_ts_ns: i64,
    min_trade_uid: Option<String>,
    max_trade_uid: Option<String>,
}

impl ManifestFile {
    fn new(
        relative_path: String,
        rows: usize,
        min_ts_ns: i64,
        max_ts_ns: i64,
        min_uid: Option<[u8; 16]>,
        max_uid: Option<[u8; 16]>,
    ) -> Self {
        Self {
            relative_path,
            rows,
            min_ts_ns,
            max_ts_ns,
            min_trade_uid: min_uid.as_ref().map(encode_uid),
            max_trade_uid: max_uid.as_ref().map(encode_uid),
        }
    }

    fn from_option_trades(relative_path: String, trades: &[OptionTrade]) -> Self {
        let (min_ts, max_ts) = ts_bounds(trades.iter().map(|t| t.trade_ts_ns));
        let (min_uid, max_uid) = uid_bounds(trades.iter().map(|t| t.trade_uid));
        Self::new(
            relative_path,
            trades.len(),
            min_ts,
            max_ts,
            min_uid,
            max_uid,
        )
    }

    fn from_equity_trades(relative_path: String, trades: &[EquityTrade]) -> Self {
        let (min_ts, max_ts) = ts_bounds(trades.iter().map(|t| t.trade_ts_ns));
        let (min_uid, max_uid) = uid_bounds(trades.iter().map(|t| t.trade_uid));
        Self::new(
            relative_path,
            trades.len(),
            min_ts,
            max_ts,
            min_uid,
            max_uid,
        )
    }

    fn from_greeks_overlays(relative_path: String, rows: &[GreeksOverlayRow]) -> Self {
        let (min_ts, max_ts) = ts_bounds(rows.iter().map(|row| row.trade_ts_ns));
        let (min_uid, max_uid) = uid_bounds(rows.iter().map(|row| row.trade_uid));
        Self::new(relative_path, rows.len(), min_ts, max_ts, min_uid, max_uid)
    }
}

#[derive(Serialize)]
struct DatasetManifest {
    dataset: String,
    date: String,
    run_id: String,
    schema_version: u16,
    generated_ns: i64,
    files: Vec<ManifestFile>,
}

struct ManifestAccumulator {
    dataset: String,
    date: NaiveDate,
    run_id: String,
    schema_version: u16,
    files: Vec<ManifestFile>,
}

impl ManifestAccumulator {
    fn new(dataset: &str, date: NaiveDate, run_id: &str, schema_version: u16) -> Self {
        Self {
            dataset: dataset.to_string(),
            date,
            run_id: run_id.to_string(),
            schema_version,
            files: Vec::new(),
        }
    }
}

fn manifest_key(dataset: &str, date: NaiveDate, run_id: &str) -> String {
    format!("{}:{}:{}", dataset, date.format("%Y-%m-%d"), run_id)
}

fn encode_uid(uid: &[u8; 16]) -> String {
    use std::fmt::Write as _;
    let mut buf = String::with_capacity(32);
    for byte in uid {
        let _ = write!(&mut buf, "{:02x}", byte);
    }
    buf
}

fn current_time_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

fn extract_date_from_partition(partition: &str) -> Option<NaiveDate> {
    for segment in partition.split('/') {
        if let Some(rest) = segment.strip_prefix("dt=") {
            if let Ok(date) = NaiveDate::parse_from_str(rest, "%Y-%m-%d") {
                return Some(date);
            }
        }
    }
    None
}

fn ts_bounds<I>(iter: I) -> (i64, i64)
where
    I: Iterator<Item = i64>,
{
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for ts in iter {
        if ts < min {
            min = ts;
        }
        if ts > max {
            max = ts;
        }
    }
    if min == i64::MAX {
        (0, 0)
    } else {
        (min, max)
    }
}

fn uid_bounds<I>(iter: I) -> (Option<[u8; 16]>, Option<[u8; 16]>)
where
    I: Iterator<Item = [u8; 16]>,
{
    let mut min_val: Option<(u128, [u8; 16])> = None;
    let mut max_val: Option<(u128, [u8; 16])> = None;
    for uid in iter {
        let val = u128::from_le_bytes(uid);
        if min_val
            .as_ref()
            .map(|(existing, _)| val < *existing)
            .unwrap_or(true)
        {
            min_val = Some((val, uid));
        }
        if max_val
            .as_ref()
            .map(|(existing, _)| val > *existing)
            .unwrap_or(true)
        {
            max_val = Some((val, uid));
        }
    }
    (min_val.map(|(_, uid)| uid), max_val.map(|(_, uid)| uid))
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::types::{
        AggressorSide, ClassMethod, Completeness, DataBatchMeta, EquityTrade, NbboState, Quality,
        Source, Watermark,
    };
    use core_types::uid::equity_trade_uid;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_serialize_equity_trade() {
        let trade_uid = equity_trade_uid(
            "AAPL",
            1640995200000000000,
            Some(1640995200000000000),
            Some(123456),
            150.0,
            100,
            1,
            Some("12345"),
            Some(0),
            &[1, 2],
        );
        let trade = EquityTrade {
            symbol: "AAPL".to_string(),
            trade_uid,
            trade_ts_ns: 1640995200000000000,
            price: 150.0,
            size: 100,
            conditions: vec![1, 2],
            exchange: 1,
            aggressor_side: AggressorSide::Buyer,
            class_method: ClassMethod::NbboTouch,
            aggressor_offset_mid_bp: Some(10),
            aggressor_offset_touch_ticks: Some(5),
            aggressor_confidence: Some(1.0),
            nbbo_bid: Some(149.0),
            nbbo_ask: Some(151.0),
            nbbo_bid_sz: Some(200),
            nbbo_ask_sz: Some(300),
            nbbo_ts_ns: Some(1640995200000000000),
            nbbo_age_us: Some(1000),
            nbbo_state: Some(NbboState::Normal),
            tick_size_used: Some(0.01),
            source: Source::Ws,
            quality: Quality::Prelim,
            watermark_ts_ns: 1640995200000000000,
            trade_id: Some("12345".to_string()),
            seq: Some(123456),
            participant_ts_ns: Some(1640995200000000000),
            tape: Some("A".to_string()),
            correction: Some(0),
            trf_id: Some("trf123".to_string()),
            trf_ts_ns: Some(1640995200000000000),
        };
        let meta = DataBatchMeta {
            source: Source::Ws,
            quality: Quality::Prelim,
            watermark: Watermark {
                watermark_ts_ns: 1640995200000000000,
                completeness: Completeness::Complete,
                hints: None,
            },
            schema_version: 2,
            run_id: None,
        };
        let batch = DataBatch {
            rows: vec![trade],
            meta,
        };
        let storage = Storage::new(core_types::config::StorageConfig::default());
        let record_batch = storage
            .equity_trades_to_record_batch(&batch.rows, &batch.meta)
            .unwrap();
        assert_eq!(record_batch.num_rows(), 1);
        assert_eq!(record_batch.num_columns(), 30);
    }

    #[test]
    fn test_dedup_equity_trades() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = core_types::config::StorageConfig::default();
        config.paths.insert(
            "base".to_string(),
            temp_dir.path().to_string_lossy().to_string(),
        );
        let mut storage = Storage::new(config);

        let trade_uid = equity_trade_uid(
            "AAPL",
            1640995200000000000,
            Some(1640995200000000000),
            Some(123456),
            150.0,
            100,
            1,
            Some("12345"),
            Some(0),
            &[1],
        );
        let trade1 = EquityTrade {
            symbol: "AAPL".to_string(),
            trade_uid,
            trade_ts_ns: 1640995200000000000, // Same day
            price: 150.0,
            size: 100,
            conditions: vec![1],
            exchange: 1,
            aggressor_side: AggressorSide::Buyer,
            class_method: ClassMethod::NbboTouch,
            aggressor_offset_mid_bp: Some(10),
            aggressor_offset_touch_ticks: Some(5),
            aggressor_confidence: Some(1.0),
            nbbo_bid: Some(149.0),
            nbbo_ask: Some(151.0),
            nbbo_bid_sz: Some(200),
            nbbo_ask_sz: Some(300),
            nbbo_ts_ns: Some(1640995200000000000),
            nbbo_age_us: Some(1000),
            nbbo_state: Some(NbboState::Normal),
            tick_size_used: Some(0.01),
            source: Source::Ws,
            quality: Quality::Prelim,
            watermark_ts_ns: 1640995200000000000,
            trade_id: Some("12345".to_string()),
            seq: Some(123456),
            participant_ts_ns: Some(1640995200000000000),
            tape: Some("A".to_string()),
            correction: Some(0),
            trf_id: Some("trf123".to_string()),
            trf_ts_ns: Some(1640995200000000000),
        };
        let trade2 = trade1.clone(); // Duplicate

        let meta = DataBatchMeta {
            source: Source::Ws,
            quality: Quality::Prelim,
            watermark: Watermark {
                watermark_ts_ns: 1640995200000000000,
                completeness: Completeness::Complete,
                hints: None,
            },
            schema_version: 2,
            run_id: None,
        };

        // First batch
        let batch1 = DataBatch {
            rows: vec![trade1.clone()],
            meta: meta.clone(),
        };
        storage.write_equity_trades(&batch1).unwrap();

        // Second batch with duplicate
        let batch2 = DataBatch {
            rows: vec![trade2],
            meta,
        };
        storage.write_equity_trades(&batch2).unwrap();

        storage.flush_all().unwrap();

        // Check total rows in files
        let mut total_rows = 0;
        for entry in fs::read_dir(temp_dir.path().join("equity_trades")).unwrap() {
            let entry = entry.unwrap();
            if entry.path().is_dir() {
                for sub_entry in fs::read_dir(entry.path()).unwrap() {
                    let sub_entry = sub_entry.unwrap();
                    if sub_entry.path().is_dir() {
                        for file_entry in fs::read_dir(sub_entry.path()).unwrap() {
                            let file_entry = file_entry.unwrap();
                            if file_entry.path().extension().unwrap_or_default() == "parquet" {
                                let file = std::fs::File::open(&file_entry.path()).unwrap();
                                let builder =
                                    ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
                                let mut reader = builder.build().unwrap();
                                if let Some(record_batch) = reader.next() {
                                    total_rows += record_batch.unwrap().num_rows();
                                }
                            }
                        }
                    }
                }
            }
        }
        // Since dedup, should have only 1 row total
        assert_eq!(total_rows, 1);
    }
}
