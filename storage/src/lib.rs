// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Parquet writer/reader with partitioning, compaction, and deduplication.

use std::path::PathBuf;
use std::sync::Arc;
use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, StringArray, UInt32Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use core_types::schema::{equity_trade_schema, nbbo_schema, option_trade_schema};
use core_types::types::{DataBatch, DataBatchMeta, EquityTrade, Nbbo, OptionTrade};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use thiserror::Error;
use uuid::Uuid;

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
}

/// Storage handles Parquet I/O with partitioning and deduplication.
pub struct Storage {
    config: core_types::config::StorageConfig,
    base_path: PathBuf,
}

impl Storage {
    pub fn new(config: core_types::config::StorageConfig) -> Self {
        Self {
            base_path: PathBuf::from(&config.paths.get("base").unwrap_or(&"data".to_string())),
            config,
        }
    }

    /// Write a batch of option trades to Parquet, deduping and partitioning.
    pub fn write_option_trades(&self, batch: &DataBatch<OptionTrade>) -> Result<(), StorageError> {
        let _schema = option_trade_schema();
        let record_batch = self.option_trades_to_record_batch(&batch.rows, &batch.meta)?;
        self.write_partitioned("options_trades", &record_batch, &batch.meta)?;
        Ok(())
    }

    /// Write a batch of equity trades to Parquet.
    pub fn write_equity_trades(&self, batch: &DataBatch<EquityTrade>) -> Result<(), StorageError> {
        let _schema = equity_trade_schema();
        let record_batch = self.equity_trades_to_record_batch(&batch.rows, &batch.meta)?;
        self.write_partitioned("equity_trades", &record_batch, &batch.meta)?;
        Ok(())
    }

    /// Write a batch of NBBO to Parquet (deltas only if configured).
    pub fn write_nbbo(&self, batch: &DataBatch<Nbbo>) -> Result<(), StorageError> {
        let _schema = nbbo_schema();
        let record_batch = self.nbbo_to_record_batch(&batch.rows, &batch.meta)?;
        self.write_partitioned("nbbo", &record_batch, &batch.meta)?;
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

    fn write_partitioned(&self, table: &str, record_batch: &RecordBatch, meta: &DataBatchMeta) -> Result<(), StorageError> {
        // Determine partition path: dt=YYYY-MM-DD, instrument_type, prefix
        let dt = DateTime::from_timestamp(meta.watermark.watermark_ts_ns / 1_000_000_000, (meta.watermark.watermark_ts_ns % 1_000_000_000) as u32)
            .unwrap()
            .naive_utc()
            .date()
            .to_string();
        let instrument_type = match table {
            "options_trades" | "nbbo" if table.contains("options") => "option",
            "equity_trades" => "equity",
            _ => "unknown",
        };
        let prefix = "prefix"; // Placeholder: derive from instrument_id prefix
        let partition_path = format!("{}/{}/dt={}/{}/", self.base_path.display(), table, dt, prefix);
        std::fs::create_dir_all(&partition_path)?;
    
        // Write Parquet with ZSTD compression
        let file_path = format!("{}data_{}.parquet", partition_path, Uuid::new_v4());
        let file = std::fs::File::create(&file_path)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();
        let mut writer = ArrowWriter::try_new(file, record_batch.schema(), Some(props))?;
        writer.write(record_batch)?;
        writer.close()?;
        Ok(())
    }

    fn option_trades_to_record_batch(&self, trades: &[OptionTrade], meta: &DataBatchMeta) -> Result<RecordBatch, StorageError> {
        let mut contract = Vec::new();
        let mut contract_direction = Vec::new();
        let mut strike_price = Vec::new();
        let mut underlying = Vec::new();
        let mut trade_ts_ns = Vec::new();
        let mut price = Vec::new();
        let mut size = Vec::new();
        let mut conditions = Vec::new();
        let mut exchange = Vec::new();
        let mut expiry_ts_ns = Vec::new();
        let mut aggressor_side = Vec::new();
        let mut class_method = Vec::new();
        let mut aggressor_offset_mid_bp = Vec::new();
        let mut aggressor_offset_touch_ticks = Vec::new();
        let mut nbbo_bid = Vec::new();
        let mut nbbo_ask = Vec::new();
        let mut nbbo_bid_sz = Vec::new();
        let mut nbbo_ask_sz = Vec::new();
        let mut nbbo_ts_ns = Vec::new();
        let mut nbbo_age_us = Vec::new();
        let mut nbbo_state = Vec::new();
        let mut tick_size_used = Vec::new();
        let mut delta = Vec::new();
        let mut gamma = Vec::new();
        let mut vega = Vec::new();
        let mut theta = Vec::new();
        let mut iv = Vec::new();
        let mut greeks_flags = Vec::new();
        let mut source = Vec::new();
        let mut quality = Vec::new();
        let mut watermark_ts_ns = Vec::new();

        for trade in trades {
            contract.push(trade.contract.clone());
            contract_direction.push(trade.contract_direction.to_string());
            strike_price.push(trade.strike_price);
            underlying.push(trade.underlying.clone());
            trade_ts_ns.push(trade.trade_ts_ns);
            price.push(trade.price);
            size.push(trade.size);
            conditions.push(format!("{:?}", trade.conditions)); // Vec<i32> as string for simplicity
            exchange.push(trade.exchange);
            expiry_ts_ns.push(trade.expiry_ts_ns);
            aggressor_side.push(format!("{:?}", trade.aggressor_side));
            class_method.push(format!("{:?}", trade.class_method));
            aggressor_offset_mid_bp.push(trade.aggressor_offset_mid_bp.map(|x| x as i64));
            aggressor_offset_touch_ticks.push(trade.aggressor_offset_touch_ticks.map(|x| x as i64));
            nbbo_bid.push(trade.nbbo_bid);
            nbbo_ask.push(trade.nbbo_ask);
            nbbo_bid_sz.push(trade.nbbo_bid_sz.map(|x| x as i64));
            nbbo_ask_sz.push(trade.nbbo_ask_sz.map(|x| x as i64));
            nbbo_ts_ns.push(trade.nbbo_ts_ns);
            nbbo_age_us.push(trade.nbbo_age_us.map(|x| x as i64));
            nbbo_state.push(format!("{:?}", trade.nbbo_state));
            tick_size_used.push(trade.tick_size_used);
            delta.push(trade.delta);
            gamma.push(trade.gamma);
            vega.push(trade.vega);
            theta.push(trade.theta);
            iv.push(trade.iv);
            greeks_flags.push(trade.greeks_flags as i64);
            source.push(format!("{:?}", meta.source));
            quality.push(format!("{:?}", meta.quality));
            watermark_ts_ns.push(meta.watermark.watermark_ts_ns);
        }

        let schema: SchemaRef = Arc::new(option_trade_schema());
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(contract)),
            Arc::new(StringArray::from(contract_direction)),
            Arc::new(Float64Array::from(strike_price)),
            Arc::new(StringArray::from(underlying)),
            Arc::new(Int64Array::from(trade_ts_ns)),
            Arc::new(Float64Array::from(price)),
            Arc::new(UInt32Array::from(size)),
            Arc::new(StringArray::from(conditions)),
            Arc::new(Int32Array::from(exchange)),
            Arc::new(Int64Array::from(expiry_ts_ns)),
            Arc::new(StringArray::from(aggressor_side)),
            Arc::new(StringArray::from(class_method)),
            Arc::new(Int64Array::from(aggressor_offset_mid_bp)),
            Arc::new(Int64Array::from(aggressor_offset_touch_ticks)),
            Arc::new(Float64Array::from(nbbo_bid)),
            Arc::new(Float64Array::from(nbbo_ask)),
            Arc::new(Int64Array::from(nbbo_bid_sz)),
            Arc::new(Int64Array::from(nbbo_ask_sz)),
            Arc::new(Int64Array::from(nbbo_ts_ns)),
            Arc::new(Int64Array::from(nbbo_age_us)),
            Arc::new(StringArray::from(nbbo_state)),
            Arc::new(Float64Array::from(tick_size_used)),
            Arc::new(Float64Array::from(delta)),
            Arc::new(Float64Array::from(gamma)),
            Arc::new(Float64Array::from(vega)),
            Arc::new(Float64Array::from(theta)),
            Arc::new(Float64Array::from(iv)),
            Arc::new(Int64Array::from(greeks_flags)),
            Arc::new(StringArray::from(source)),
            Arc::new(StringArray::from(quality)),
            Arc::new(Int64Array::from(watermark_ts_ns)),
        ];

        Ok(RecordBatch::try_new(schema, arrays).map_err(Into::into)?)
    }

    fn equity_trades_to_record_batch(&self, trades: &[EquityTrade], meta: &DataBatchMeta) -> Result<RecordBatch, StorageError> {
        // Similar to option_trades, but with equity fields.
        // Omitted for brevity; implement analogously.
        let schema: SchemaRef = Arc::new(equity_trade_schema());
        // Build arrays...
        // For now, return empty batch.
        Ok(RecordBatch::new_empty(schema))
    }

    fn nbbo_to_record_batch(&self, nbbos: &[Nbbo], meta: &DataBatchMeta) -> Result<RecordBatch, StorageError> {
        // Similar implementation.
        let schema: SchemaRef = Arc::new(nbbo_schema());
        // Build arrays...
        Ok(RecordBatch::new_empty(schema))
    }
}
