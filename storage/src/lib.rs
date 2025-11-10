// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Parquet writer/reader with partitioning, compaction, and deduplication.

use arrow::array::{
    ArrayRef, Float64Array, Int32Array, Int64Array, ListArray, StringArray, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{Int32Type, SchemaRef};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, Utc};
use core_types::schema::{
    aggregation_schema, equity_trade_schema, nbbo_schema, option_trade_schema,
};
use core_types::types::{AggregationRow, DataBatch, DataBatchMeta, EquityTrade, Nbbo, OptionTrade};
use lru::LruCache;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
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
    dedup_cache: HashMap<String, LruCache<String, ()>>,
    writers: Mutex<HashMap<String, PartitionWriter>>,
}

impl Storage {
    pub fn new(config: core_types::config::StorageConfig) -> Self {
        Self {
            base_path: PathBuf::from(&config.paths.get("base").unwrap_or(&"data".to_string())),
            config,
            dedup_cache: HashMap::new(),
            writers: Mutex::new(HashMap::new()),
        }
    }

    pub fn flush_all(&self) -> Result<(), StorageError> {
        let mut writers = self.writers.lock().unwrap();
        for writer in writers.values_mut() {
            writer.close()?;
        }
        Ok(())
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

            let record_batch = self.option_trades_to_record_batch(&deduped_trades, &batch.meta)?;
            let dt_str = date.to_string();
            let prefix = "prefix"; // TODO: derive from contract prefix
            let partition_dir = format!(
                "{}/{}/dt={}/{}/",
                self.base_path.display(),
                "options_trades",
                dt_str,
                prefix
            );
            std::fs::create_dir_all(&partition_dir)?;

            // Use pooled writer for this partition
            let target_bytes = self.config.file_size_mb_target.saturating_mul(1024 * 1024);
            {
                let mut writers = self.writers.lock().unwrap();
                let pw = writers.entry(partition_dir.clone()).or_insert_with(|| {
                    PartitionWriter::new(&partition_dir, record_batch.schema())
                        .expect("failed to create partition writer")
                });
                pw.write(&record_batch)?;
                rotate_if_needed(pw, target_bytes)?;
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

            let record_batch = self.equity_trades_to_record_batch(&deduped_trades, &batch.meta)?;
            let dt_str = date.to_string();
            let prefix = "prefix"; // TODO: derive from instrument_id prefix
            let partition_dir = format!(
                "{}/{}/dt={}/{}/",
                self.base_path.display(),
                "equity_trades",
                dt_str,
                prefix
            );
            std::fs::create_dir_all(&partition_dir)?;
            // Get a writer for this partition
            let target_bytes = self.config.file_size_mb_target.saturating_mul(1024 * 1024);
            {
                let mut writers = self.writers.lock().unwrap();
                let pw = writers.entry(partition_dir.clone()).or_insert_with(|| {
                    PartitionWriter::new(&partition_dir, record_batch.schema())
                        .expect("failed to create partition writer")
                });
                pw.write(&record_batch)?;
                rotate_if_needed(pw, target_bytes)?;
            }
        }
        Ok(())
    }

    /// Write a batch of NBBO to Parquet (deltas only if configured).
    pub fn write_nbbo(&self, batch: &DataBatch<Nbbo>) -> Result<(), StorageError> {
        let _schema = nbbo_schema();
        let record_batch = self.nbbo_to_record_batch(&batch.rows, &batch.meta)?;
        let dt = DateTime::from_timestamp(
            batch.meta.watermark.watermark_ts_ns / 1_000_000_000,
            (batch.meta.watermark.watermark_ts_ns % 1_000_000_000) as u32,
        )
        .unwrap()
        .naive_utc()
        .date();
        self.write_partitioned("nbbo", &record_batch, &batch.meta, dt)?;
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
            let record_batch = self.aggregations_to_record_batch(&rows, &batch.meta)?;
            let partition_dir = format!(
                "{}/{}/symbol={}/window={}/dt={}/",
                self.base_path.display(),
                "aggregations",
                symbol,
                window,
                date
            );
            std::fs::create_dir_all(&partition_dir)?;
            let target_bytes = self.config.file_size_mb_target.saturating_mul(1024 * 1024);
            {
                let mut writers = self.writers.lock().unwrap();
                let pw = writers.entry(partition_dir.clone()).or_insert_with(|| {
                    PartitionWriter::new(&partition_dir, record_batch.schema())
                        .expect("failed to create aggregation writer")
                });
                pw.write(&record_batch)?;
                rotate_if_needed(pw, target_bytes)?;
            }
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
        table: &str,
        record_batch: &RecordBatch,
        meta: &DataBatchMeta,
        dt: NaiveDate,
    ) -> Result<(), StorageError> {
        // Determine partition path: dt=YYYY-MM-DD, instrument_type, prefix
        let dt_str = dt.to_string();
        let _instrument_type = match table {
            "options_trades" | "nbbo" if table.contains("options") => "option",
            "equity_trades" => "equity",
            _ => "unknown",
        };
        let prefix = "prefix"; // Placeholder: derive from instrument_id prefix
        let partition_path = format!(
            "{}/{}/dt={}/{}/",
            self.base_path.display(),
            table,
            dt_str,
            prefix
        );
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

    fn option_trades_to_record_batch(
        &self,
        trades: &[OptionTrade],
        meta: &DataBatchMeta,
    ) -> Result<RecordBatch, StorageError> {
        let mut contract = Vec::new();
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
        let mut nbbo_bid = Vec::new();
        let mut nbbo_ask = Vec::new();
        let mut nbbo_bid_sz: Vec<Option<u32>> = Vec::new();
        let mut nbbo_ask_sz: Vec<Option<u32>> = Vec::new();
        let mut nbbo_ts_ns = Vec::new();
        let mut nbbo_age_us: Vec<Option<u32>> = Vec::new();
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
            conditions.push(trade.conditions.clone());
            exchange.push(trade.exchange);
            expiry_ts_ns.push(trade.expiry_ts_ns);
            aggressor_side.push(format!("{:?}", trade.aggressor_side));
            class_method.push(format!("{:?}", trade.class_method));
            aggressor_offset_mid_bp.push(trade.aggressor_offset_mid_bp);
            aggressor_offset_touch_ticks.push(trade.aggressor_offset_touch_ticks);
            nbbo_bid.push(trade.nbbo_bid);
            nbbo_ask.push(trade.nbbo_ask);
            nbbo_bid_sz.push(trade.nbbo_bid_sz);
            nbbo_ask_sz.push(trade.nbbo_ask_sz);
            nbbo_ts_ns.push(trade.nbbo_ts_ns);
            nbbo_age_us.push(trade.nbbo_age_us);
            nbbo_state.push(format!("{:?}", trade.nbbo_state));
            tick_size_used.push(trade.tick_size_used);
            delta.push(trade.delta);
            gamma.push(trade.gamma);
            vega.push(trade.vega);
            theta.push(trade.theta);
            iv.push(trade.iv);
            greeks_flags.push(trade.greeks_flags);
            source.push(format!("{:?}", meta.source));
            quality.push(format!("{:?}", meta.quality));
            watermark_ts_ns.push(meta.watermark.watermark_ts_ns);
        }

        let schema: SchemaRef = Arc::new(option_trade_schema());
        let conditions_array = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
            conditions
                .into_iter()
                .map(|v| Some(v.into_iter().map(Some))),
        ));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(contract)),
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
            Arc::new(Float64Array::from(nbbo_bid)),
            Arc::new(Float64Array::from(nbbo_ask)),
            Arc::new(UInt32Array::from(nbbo_bid_sz)),
            Arc::new(UInt32Array::from(nbbo_ask_sz)),
            Arc::new(Int64Array::from(nbbo_ts_ns)),
            Arc::new(UInt32Array::from(nbbo_age_us)),
            Arc::new(StringArray::from(nbbo_state)),
            Arc::new(Float64Array::from(tick_size_used)),
            Arc::new(Float64Array::from(delta)),
            Arc::new(Float64Array::from(gamma)),
            Arc::new(Float64Array::from(vega)),
            Arc::new(Float64Array::from(theta)),
            Arc::new(Float64Array::from(iv)),
            Arc::new(UInt32Array::from(greeks_flags)),
            Arc::new(StringArray::from(source)),
            Arc::new(StringArray::from(quality)),
            Arc::new(Int64Array::from(watermark_ts_ns)),
        ];

        Ok(RecordBatch::try_new(schema, arrays)?)
    }

    fn equity_trades_to_record_batch(
        &self,
        trades: &[EquityTrade],
        meta: &DataBatchMeta,
    ) -> Result<RecordBatch, StorageError> {
        let mut symbol = Vec::new();
        let mut trade_ts_ns = Vec::new();
        let mut price = Vec::new();
        let mut size = Vec::new();
        let mut conditions = Vec::new();
        let mut exchange = Vec::new();
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
            trade_ts_ns.push(trade.trade_ts_ns);
            price.push(trade.price);
            size.push(trade.size);
            conditions.push(trade.conditions.clone());
            exchange.push(trade.exchange);
            aggressor_side.push(format!("{:?}", trade.aggressor_side));
            class_method.push(format!("{:?}", trade.class_method));
            aggressor_offset_mid_bp.push(trade.aggressor_offset_mid_bp);
            aggressor_offset_touch_ticks.push(trade.aggressor_offset_touch_ticks);
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
        let conditions_array = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
            conditions
                .into_iter()
                .map(|v| Some(v.into_iter().map(Some))),
        ));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(symbol)),
            Arc::new(Int64Array::from(trade_ts_ns)),
            Arc::new(Float64Array::from(price)),
            Arc::new(UInt32Array::from(size)),
            conditions_array,
            Arc::new(Int32Array::from(exchange)),
            Arc::new(StringArray::from(aggressor_side)),
            Arc::new(StringArray::from(class_method)),
            Arc::new(Int32Array::from(aggressor_offset_mid_bp)),
            Arc::new(Int32Array::from(aggressor_offset_touch_ticks)),
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
        _nbbos: &[Nbbo],
        _meta: &DataBatchMeta,
    ) -> Result<RecordBatch, StorageError> {
        let schema: SchemaRef = Arc::new(nbbo_schema());
        Ok(RecordBatch::new_empty(schema))
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
            calls_dollar_value,
            calls_above_intrinsic_pct,
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
            calls_gadvv_kde_peaks
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
            calls_dollar_value.push(row.calls_dollar_value);
            calls_above_intrinsic_pct.push(row.calls_above_intrinsic_pct);
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
            Arc::new(Float64Array::from(calls_dollar_value)),
            Arc::new(Float64Array::from(calls_above_intrinsic_pct)),
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
        ];
        RecordBatch::try_new(Arc::new(aggregation_schema()), arrays).map_err(StorageError::from)
    }
}

fn rotate_if_needed(pw: &mut PartitionWriter, target_bytes: u64) -> Result<(), StorageError> {
    if target_bytes > 0 && pw.current_size_bytes()? >= target_bytes as u64 {
        pw.rotate()?;
    }
    Ok(())
}

struct PartitionWriter {
    partition_dir: String,
    file_path: PathBuf,
    writer: Option<ArrowWriter<std::fs::File>>,
    schema: SchemaRef,
}

impl PartitionWriter {
    fn new(partition_dir: &str, schema: SchemaRef) -> Result<Self, StorageError> {
        let dir = partition_dir.to_string();
        let file_path = PathBuf::from(format!("{}data_{}.parquet", partition_dir, Uuid::new_v4()));
        let file = std::fs::File::create(&file_path)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
        Ok(Self {
            partition_dir: dir,
            file_path,
            writer: Some(writer),
            schema,
        })
    }

    fn ensure_open(&mut self) -> Result<(), StorageError> {
        if self.writer.is_none() {
            let new_path = PathBuf::from(format!(
                "{}data_{}.parquet",
                self.partition_dir,
                Uuid::new_v4()
            ));
            let file = std::fs::File::create(&new_path)?;
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(Default::default()))
                .build();
            let writer = ArrowWriter::try_new(file, self.schema.clone(), Some(props))?;
            self.file_path = new_path;
            self.writer = Some(writer);
        }
        Ok(())
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<(), StorageError> {
        self.ensure_open()?;
        if let Some(writer) = &mut self.writer {
            writer.write(batch)?;
        }
        Ok(())
    }

    fn current_size_bytes(&self) -> Result<u64, StorageError> {
        let meta = std::fs::metadata(&self.file_path)?;
        Ok(meta.len())
    }

    fn rotate(&mut self) -> Result<(), StorageError> {
        self.close()?;
        // Open a new file for continued writes
        self.ensure_open()?;
        Ok(())
    }

    fn close(&mut self) -> Result<(), StorageError> {
        if let Some(mut writer) = self.writer.take() {
            writer.close()?; // ensures footer is written
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::types::{
        AggressorSide, ClassMethod, Completeness, DataBatchMeta, EquityTrade, NbboState, Quality,
        Source, Watermark,
    };
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_serialize_equity_trade() {
        let trade = EquityTrade {
            symbol: "AAPL".to_string(),
            trade_ts_ns: 1640995200000000000,
            price: 150.0,
            size: 100,
            conditions: vec![1, 2],
            exchange: 1,
            aggressor_side: AggressorSide::Buyer,
            class_method: ClassMethod::NbboTouch,
            aggressor_offset_mid_bp: Some(10),
            aggressor_offset_touch_ticks: Some(5),
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
        assert_eq!(record_batch.num_columns(), 28);
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

        let trade1 = EquityTrade {
            symbol: "AAPL".to_string(),
            trade_ts_ns: 1640995200000000000, // Same day
            price: 150.0,
            size: 100,
            conditions: vec![1],
            exchange: 1,
            aggressor_side: AggressorSide::Buyer,
            class_method: ClassMethod::NbboTouch,
            aggressor_offset_mid_bp: Some(10),
            aggressor_offset_touch_ticks: Some(5),
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
