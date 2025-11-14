use crate::BoxError;
use arrow::array::{
    Array, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array, ListArray, StringArray,
    UInt32Array,
};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{Datelike, NaiveDate};
use core_types::types::{
    AggressorSide, ClassMethod, Completeness, DataBatch, DataBatchMeta, EnrichmentDataset,
    NbboState, OptionTrade, Quality, Source, Watermark,
};
use futures::stream::BoxStream;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Deserialize;
use std::fs::File;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub type BatchStream = BoxStream<'static, Result<DataBatch<OptionTrade>, BoxError>>;

#[async_trait]
pub trait GreeksEnrichmentSource: Send + Sync + 'static {
    async fn stream_batches(
        &self,
        dataset: EnrichmentDataset,
        date: NaiveDate,
        run_id: &str,
    ) -> Result<BatchStream, BoxError>;
}

#[derive(Clone)]
pub struct ManifestParquetSource {
    base_path: PathBuf,
}

impl ManifestParquetSource {
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        Self {
            base_path: base_path.into(),
        }
    }

    fn manifest_path(&self, dataset: EnrichmentDataset, date: NaiveDate, run_id: &str) -> PathBuf {
        self.base_path
            .join("manifests")
            .join(dataset.raw_dataset_name())
            .join(format!("{:04}", date.year()))
            .join(format!("{:02}", date.month()))
            .join(format!("{:02}", date.day()))
            .join(format!("manifest-{}.json", run_id))
    }
}

#[async_trait]
impl GreeksEnrichmentSource for ManifestParquetSource {
    async fn stream_batches(
        &self,
        dataset: EnrichmentDataset,
        date: NaiveDate,
        run_id: &str,
    ) -> Result<BatchStream, BoxError> {
        let manifest_path = self.manifest_path(dataset, date, run_id);
        let base_path = self.base_path.clone();
        let run_id = run_id.to_string();
        let (tx, rx) = mpsc::channel(4);
        let tx_for_worker = tx.clone();
        tokio::task::spawn_blocking(move || {
            if let Err(err) = load_batches_blocking(
                base_path,
                manifest_path.as_path(),
                &run_id,
                tx_for_worker.clone(),
            ) {
                log::error!("failed to load manifest batches: {}", err);
                let _ = tx_for_worker.blocking_send(Err(err));
            }
        });
        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

fn load_batches_blocking(
    base_path: PathBuf,
    manifest_path: &Path,
    run_id: &str,
    tx: mpsc::Sender<Result<DataBatch<OptionTrade>, BoxError>>,
) -> Result<(), BoxError> {
    let manifest = read_manifest(manifest_path)?;
    for file in manifest.files {
        let abs_path = base_path.join(file.relative_path);
        let batches = read_file_batches(&abs_path, run_id, manifest.schema_version)?;
        for batch in batches {
            tx.blocking_send(Ok(batch))
                .map_err(|e| -> BoxError { format!("failed to send batch: {}", e).into() })?;
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct ManifestDoc {
    files: Vec<ManifestFile>,
    schema_version: u16,
}

#[derive(Deserialize)]
struct ManifestFile {
    relative_path: String,
}

fn read_manifest(path: &Path) -> Result<ManifestDoc, BoxError> {
    let file = File::open(path)?;
    Ok(serde_json::from_reader(file)?)
}

fn read_file_batches(
    path: &Path,
    run_id: &str,
    schema_version: u16,
) -> Result<Vec<DataBatch<OptionTrade>>, BoxError> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.build()?;
    let mut batches = Vec::new();
    while let Some(batch) = reader.next() {
        let batch = batch?;
        let rows = record_batch_to_trades(&batch)?;
        if rows.is_empty() {
            continue;
        }
        let watermark_ts_ns = rows
            .iter()
            .map(|t| t.watermark_ts_ns)
            .max()
            .unwrap_or_default();
        let meta = DataBatchMeta {
            source: Source::Flatfile,
            quality: Quality::Prelim,
            watermark: Watermark {
                watermark_ts_ns,
                completeness: Completeness::Complete,
                hints: None,
            },
            schema_version,
            run_id: Some(run_id.to_string()),
        };
        batches.push(DataBatch { rows, meta });
    }
    Ok(batches)
}

fn record_batch_to_trades(batch: &RecordBatch) -> Result<Vec<OptionTrade>, BoxError> {
    let len = batch.num_rows();
    let contract = as_string_array(batch, 0)?;
    let trade_uids = batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or("trade_uid column missing")?;
    let contract_direction = as_string_array(batch, 2)?;
    let strike_price = as_f64_array(batch, 3)?;
    let underlying = as_string_array(batch, 4)?;
    let trade_ts_ns = as_i64_array(batch, 5)?;
    let price = as_f64_array(batch, 6)?;
    let size = as_u32_array(batch, 7)?;
    let conditions = batch
        .column(8)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or("conditions column missing")?;
    let exchange = as_i32_array(batch, 9)?;
    let expiry_ts_ns = as_i64_array(batch, 10)?;
    let aggressor_side = as_string_array(batch, 11)?;
    let class_method = as_string_array(batch, 12)?;
    let aggressor_offset_mid_bp = as_i32_array(batch, 13)?;
    let aggressor_offset_touch_ticks = as_i32_array(batch, 14)?;
    let aggressor_confidence = as_f64_array(batch, 15)?;
    let nbbo_bid = as_f64_array(batch, 16)?;
    let nbbo_ask = as_f64_array(batch, 17)?;
    let nbbo_bid_sz = as_u32_array(batch, 18)?;
    let nbbo_ask_sz = as_u32_array(batch, 19)?;
    let nbbo_ts_ns = as_i64_array(batch, 20)?;
    let nbbo_age_us = as_u32_array(batch, 21)?;
    let nbbo_state = as_string_array(batch, 22)?;
    let tick_size_used = as_f64_array(batch, 23)?;
    let source = as_string_array(batch, 24)?;
    let quality = as_string_array(batch, 25)?;
    let watermark_ts_ns = as_i64_array(batch, 26)?;
    let underlying_nbbo_bid = as_f64_array(batch, 27)?;
    let underlying_nbbo_ask = as_f64_array(batch, 28)?;
    let underlying_nbbo_bid_sz = as_u32_array(batch, 29)?;
    let underlying_nbbo_ask_sz = as_u32_array(batch, 30)?;
    let underlying_nbbo_ts_ns = as_i64_array(batch, 31)?;
    let underlying_nbbo_age_us = as_u32_array(batch, 32)?;
    let underlying_nbbo_state = as_string_array(batch, 33)?;

    let mut trades = Vec::with_capacity(len);
    for row in 0..len {
        trades.push(OptionTrade {
            contract: contract.value(row).to_string(),
            trade_uid: {
                let bytes = trade_uids.value(row);
                let mut uid = [0u8; 16];
                uid.copy_from_slice(bytes);
                uid
            },
            contract_direction: contract_direction.value(row).chars().next().unwrap_or('C'),
            strike_price: strike_price.value(row),
            underlying: underlying.value(row).to_string(),
            trade_ts_ns: trade_ts_ns.value(row),
            price: price.value(row),
            size: size.value(row),
            conditions: list_to_vec_i32(conditions, row)?,
            exchange: exchange.value(row),
            expiry_ts_ns: expiry_ts_ns.value(row),
            aggressor_side: parse_aggressor_side(aggressor_side.value(row)),
            class_method: parse_class_method(class_method.value(row)),
            aggressor_offset_mid_bp: take_opt_i32(aggressor_offset_mid_bp, row),
            aggressor_offset_touch_ticks: take_opt_i32(aggressor_offset_touch_ticks, row),
            aggressor_confidence: take_opt_f64(aggressor_confidence, row),
            nbbo_bid: take_opt_f64(nbbo_bid, row),
            nbbo_ask: take_opt_f64(nbbo_ask, row),
            nbbo_bid_sz: take_opt_u32(nbbo_bid_sz, row),
            nbbo_ask_sz: take_opt_u32(nbbo_ask_sz, row),
            nbbo_ts_ns: take_opt_i64(nbbo_ts_ns, row),
            nbbo_age_us: take_opt_u32(nbbo_age_us, row),
            nbbo_state: parse_nbbo_state(nbbo_state.value(row)),
            underlying_nbbo_bid: take_opt_f64(underlying_nbbo_bid, row),
            underlying_nbbo_ask: take_opt_f64(underlying_nbbo_ask, row),
            underlying_nbbo_bid_sz: take_opt_u32(underlying_nbbo_bid_sz, row),
            underlying_nbbo_ask_sz: take_opt_u32(underlying_nbbo_ask_sz, row),
            underlying_nbbo_ts_ns: take_opt_i64(underlying_nbbo_ts_ns, row),
            underlying_nbbo_age_us: take_opt_u32(underlying_nbbo_age_us, row),
            underlying_nbbo_state: parse_nbbo_state(underlying_nbbo_state.value(row)),
            tick_size_used: take_opt_f64(tick_size_used, row),
            delta: None,
            gamma: None,
            vega: None,
            theta: None,
            iv: None,
            greeks_flags: 0,
            source: parse_source(source.value(row)),
            quality: parse_quality(quality.value(row)),
            watermark_ts_ns: watermark_ts_ns.value(row),
        });
    }
    Ok(trades)
}

fn as_string_array<'a>(batch: &'a RecordBatch, idx: usize) -> Result<&'a StringArray, BoxError> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| format!("column {} not Utf8", idx).into())
}

fn as_f64_array<'a>(batch: &'a RecordBatch, idx: usize) -> Result<&'a Float64Array, BoxError> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| format!("column {} not f64", idx).into())
}

fn as_i32_array<'a>(batch: &'a RecordBatch, idx: usize) -> Result<&'a Int32Array, BoxError> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| format!("column {} not i32", idx).into())
}

fn as_i64_array<'a>(batch: &'a RecordBatch, idx: usize) -> Result<&'a Int64Array, BoxError> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| format!("column {} not i64", idx).into())
}

fn as_u32_array<'a>(batch: &'a RecordBatch, idx: usize) -> Result<&'a UInt32Array, BoxError> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| format!("column {} not u32", idx).into())
}

fn list_to_vec_i32(list: &ListArray, row: usize) -> Result<Vec<i32>, BoxError> {
    let values = list.value(row);
    let ints = values
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or("conditions inner array missing")?;
    Ok((0..ints.len()).map(|i| ints.value(i)).collect())
}

fn take_opt_f64(array: &Float64Array, idx: usize) -> Option<f64> {
    if array.is_null(idx) {
        None
    } else {
        Some(array.value(idx))
    }
}

fn take_opt_i32(array: &Int32Array, idx: usize) -> Option<i32> {
    if array.is_null(idx) {
        None
    } else {
        Some(array.value(idx))
    }
}

fn take_opt_i64(array: &Int64Array, idx: usize) -> Option<i64> {
    if array.is_null(idx) {
        None
    } else {
        Some(array.value(idx))
    }
}

fn take_opt_u32(array: &UInt32Array, idx: usize) -> Option<u32> {
    if array.is_null(idx) {
        None
    } else {
        Some(array.value(idx))
    }
}

fn parse_aggressor_side(value: &str) -> AggressorSide {
    match value {
        "Buyer" => AggressorSide::Buyer,
        "Seller" => AggressorSide::Seller,
        _ => AggressorSide::Unknown,
    }
}

fn parse_class_method(value: &str) -> ClassMethod {
    match value {
        "NbboTouch" => ClassMethod::NbboTouch,
        "NbboAtOrBeyond" => ClassMethod::NbboAtOrBeyond,
        "TickRule" => ClassMethod::TickRule,
        _ => ClassMethod::Unknown,
    }
}

fn parse_nbbo_state(value: &str) -> Option<NbboState> {
    match value {
        "Some(Normal)" => Some(NbboState::Normal),
        "Some(Locked)" => Some(NbboState::Locked),
        "Some(Crossed)" => Some(NbboState::Crossed),
        _ => None,
    }
}

fn parse_source(value: &str) -> Source {
    match value {
        "Rest" => Source::Rest,
        "Ws" => Source::Ws,
        _ => Source::Flatfile,
    }
}

fn parse_quality(value: &str) -> Quality {
    match value {
        "Enriched" => Quality::Enriched,
        "Final" => Quality::Final,
        _ => Quality::Prelim,
    }
}
