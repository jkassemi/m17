// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Flatfile reader (stub).

use async_compression::tokio::bufread::GzipDecoder;
use async_trait::async_trait;
use bytes::Bytes;
use core_types::config::FlatfileConfig;
use core_types::types::{
    AggressorSide, ClassMethod, Completeness, DataBatch, DataBatchMeta, EquityTrade, Nbbo,
    OptionTrade, Quality, QueryScope, Source, Watermark,
};
use csv::Reader;
use flate2::read::GzDecoder;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use std::io::{Cursor, Read};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::fs::File;
use tokio::io::BufReader;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::ReaderStream;

/// Object store trait for accessing files.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Check if a file exists (head operation).
    async fn head(&self, path: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// List files with a given prefix.
    async fn list(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>>;

    /// Get a stream of bytes for a file.
    async fn get_stream(
        &self,
        path: &str,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<Bytes, Box<dyn std::error::Error + Send + Sync>>> + Send>>,
        Box<dyn std::error::Error + Send + Sync>,
    >;
}

/// Local file system implementation for development.
pub struct LocalFileStore {
    base_path: PathBuf,
}

impl LocalFileStore {
    pub fn new(base_path: PathBuf) -> Self {
        Self { base_path }
    }
}

#[async_trait]
impl ObjectStore for LocalFileStore {
    async fn head(&self, path: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let full_path = self.base_path.join(path);
        if full_path.exists() {
            Ok(())
        } else {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "File not found",
            )))
        }
    }

    async fn list(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let mut files = Vec::new();
        let prefix_path = self.base_path.join(prefix);
        if let Ok(entries) = std::fs::read_dir(&prefix_path) {
            for entry in entries {
                if let Ok(entry) = entry {
                    if let Some(file_name) = entry.file_name().to_str() {
                        files.push(file_name.to_string());
                    }
                }
            }
        }
        Ok(files)
    }

    async fn get_stream(
        &self,
        path: &str,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<Bytes, Box<dyn std::error::Error + Send + Sync>>> + Send>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let full_path = self.base_path.join(path);
        let file = File::open(&full_path).await?;
        let reader = BufReader::new(file);

        // Check if gzipped
        let stream: Pin<
            Box<dyn Stream<Item = Result<Bytes, Box<dyn std::error::Error + Send + Sync>>> + Send>,
        >;
        if path.ends_with(".gz") {
            let decoder = GzipDecoder::new(reader);
            stream = Box::pin(
                ReaderStream::new(decoder)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
            );
        } else {
            stream = Box::pin(
                ReaderStream::new(reader)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
            );
        }

        Ok(stream)
    }
}

/// S3 implementation (stub: signature and config only).
pub struct S3Store {
    bucket: String,
    // Add config fields as needed
}

impl S3Store {
    pub fn new(bucket: String) -> Self {
        Self { bucket }
    }
}

#[async_trait]
impl ObjectStore for S3Store {
    async fn head(&self, _path: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Stub: Always return error
        Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "S3 not implemented",
        )))
    }

    async fn list(
        &self,
        _prefix: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        // Stub: Return empty list
        Ok(Vec::new())
    }

    async fn get_stream(
        &self,
        _path: &str,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<Bytes, Box<dyn std::error::Error + Send + Sync>>> + Send>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        // Stub: Return empty stream
        Ok(Box::pin(futures::stream::empty()))
    }
}

/// Stub flatfile source.
pub struct FlatfileSource {
    status: Arc<Mutex<String>>,
    store: Arc<dyn ObjectStore>,
}

impl FlatfileSource {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            status: Arc::new(Mutex::new("Initializing".to_string())),
            store,
        }
    }

    /// Run the flatfile source loop to maintain data for configured ranges.
    pub async fn run(&self, config: FlatfileConfig) {
        loop {
            // Stub: Check and update data for each range
            let mut status_parts = Vec::new();
            for range in &config.date_ranges {
                let start = range.start_ts_ns().unwrap_or(0);
                let end = range.end_ts_ns().unwrap_or(None).unwrap_or(0);
                status_parts.push(format!("Range {} - {}: Checking...", start, end));
            }
            let status = format!("Flatfile: {}", status_parts.join(", "));
            *self.status.lock().unwrap() = status;

            // Stub: Simulate work
            sleep(Duration::from_secs(10)).await;
        }
    }

    /// Get current status.
    pub fn status(&self) -> String {
        self.status.lock().unwrap().clone()
    }

    /// Stub: Return empty stream.
    pub async fn get_nbbo(
        &self,
        _scope: QueryScope,
    ) -> Pin<Box<dyn Stream<Item = DataBatch<Nbbo>> + Send>> {
        Box::pin(futures::stream::empty())
    }

    /// Stream parse CSV gz to DataBatch<EquityTrade> with bounded buffers, spawn_blocking for decode, batch every 1000 rows, set DataBatchMeta.
    pub async fn get_equity_trades(
        &self,
        _scope: QueryScope,
    ) -> Pin<Box<dyn Stream<Item = DataBatch<EquityTrade>> + Send>> {
        let (tx, rx) = mpsc::channel(10);
        let store = self.store.clone();
        tokio::spawn(async move {
            // Hardcode path for stub; in real impl, derive from scope
            let path = "stock_trades_sample.csv.gz";
            let stream_result = store.get_stream(path).await;
            if let Ok(mut stream) = stream_result {
                let mut bytes = Vec::new();
                while let Some(chunk) = stream.next().await {
                    if let Ok(chunk) = chunk {
                        bytes.extend_from_slice(&chunk);
                    }
                }
                // Spawn blocking for CPU-intensive decode and parse
                let tx_clone = tx.clone();
                tokio::task::spawn_blocking(move || {
                    let csv_data = bytes;
                    let cursor = Cursor::new(csv_data);
                    let mut rdr = Reader::from_reader(cursor);
                    let mut batch = Vec::new();
                    const BATCH_SIZE: usize = 1000;
                    for result in rdr.records() {
                        if let Ok(record) = result {
                            let trade = EquityTrade {
                                symbol: record[0].to_string(),
                                trade_ts_ns: record[5].parse().unwrap_or(0),
                                price: record[6].parse().unwrap_or(0.0),
                                size: record[9].parse().unwrap_or(0),
                                conditions: record[1]
                                    .split(',')
                                    .filter_map(|s| s.trim().parse().ok())
                                    .collect(),
                                exchange: record[3].parse().unwrap_or(0),
                                aggressor_side: AggressorSide::Unknown,
                                class_method: ClassMethod::Unknown,
                                aggressor_offset_mid_bp: None,
                                aggressor_offset_touch_ticks: None,
                                nbbo_bid: None,
                                nbbo_ask: None,
                                nbbo_bid_sz: None,
                                nbbo_ask_sz: None,
                                nbbo_ts_ns: None,
                                nbbo_age_us: None,
                                nbbo_state: None,
                                tick_size_used: None,
                                source: Source::Flatfile,
                                quality: Quality::Prelim,
                                watermark_ts_ns: 0,
                                trade_id: Some(record[4].to_string()),
                                seq: Some(record[7].parse().unwrap_or(0)),
                                participant_ts_ns: Some(record[8].parse().unwrap_or(0)),
                                tape: Some(record[10].to_string()),
                                correction: Some(record[2].parse().unwrap_or(0)),
                                trf_id: Some(record[11].to_string()),
                                trf_ts_ns: Some(record[12].parse().unwrap_or(0)),
                            };
                            batch.push(trade);
                            if batch.len() == BATCH_SIZE {
                                let meta = DataBatchMeta {
                                    source: Source::Flatfile,
                                    quality: Quality::Prelim,
                                    watermark: Watermark {
                                        watermark_ts_ns: 0, // Placeholder
                                        completeness: Completeness::Complete,
                                        hints: None,
                                    },
                                    schema_version: 1,
                                };
                                let _ = tx_clone.try_send(DataBatch { rows: batch, meta });
                                batch = Vec::new();
                            }
                        }
                    }
                    if !batch.is_empty() {
                        let meta = DataBatchMeta {
                            source: Source::Flatfile,
                            quality: Quality::Prelim,
                            watermark: Watermark {
                                watermark_ts_ns: 0, // Placeholder
                                completeness: Completeness::Complete,
                                hints: None,
                            },
                            schema_version: 1,
                        };
                        let _ = tx_clone.try_send(DataBatch { rows: batch, meta });
                    }
                })
                .await
                .unwrap();
            }
        });
        Box::pin(ReceiverStream::new(rx))
    }
}

fn parse_aggressor_side(s: &str) -> AggressorSide {
    match s.to_lowercase().as_str() {
        "buy" => AggressorSide::Buyer,
        "sell" => AggressorSide::Seller,
        _ => AggressorSide::Unknown,
    }
}

fn parse_class_method(s: &str) -> ClassMethod {
    match s.to_lowercase().as_str() {
        "tick_rule" => ClassMethod::TickRule,
        _ => ClassMethod::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_local_file_store_stream_stock_quotes_sample() {
        let store = LocalFileStore::new(PathBuf::from("fixtures"));
        let mut stream = store
            .get_stream("stock_quotes_sample.csv.gz")
            .await
            .unwrap();
        let mut bytes_collected = Vec::new();
        while let Some(chunk) = stream.next().await {
            bytes_collected.push(chunk.unwrap());
        }
        assert!(
            !bytes_collected.is_empty(),
            "Stream should produce bytes for stock_quotes_sample.csv.gz"
        );
    }

    #[tokio::test]
    async fn test_local_file_store_stream_stock_trades_sample() {
        let store = LocalFileStore::new(PathBuf::from("fixtures"));
        let mut stream = store
            .get_stream("stock_trades_sample.csv.gz")
            .await
            .unwrap();
        let mut bytes_collected = Vec::new();
        while let Some(chunk) = stream.next().await {
            bytes_collected.push(chunk.unwrap());
        }
        assert!(
            !bytes_collected.is_empty(),
            "Stream should produce bytes for stock_trades_sample.csv.gz"
        );
    }

    #[tokio::test]
    async fn test_flatfile_source_get_equity_trades() {
        let store = Arc::new(LocalFileStore::new(PathBuf::from("fixtures")));
        let source = FlatfileSource::new(store);
        let scope = QueryScope {
            instruments: vec![],
            time_range: (0, i64::MAX),
            mode: "Historical".to_string(),
            quality_target: Quality::Prelim,
        };
        let mut stream = source.get_equity_trades(scope).await;
        let mut all_trades = Vec::new();
        while let Some(batch) = stream.next().await {
            all_trades.extend(batch.rows);
        }
        assert!(!all_trades.is_empty(), "Should yield EquityTrade rows");
        // Check conditions parsed (assume sample has conditions like "1,2")
        for trade in &all_trades {
            assert!(
                trade.conditions.iter().all(|&c| c >= 0),
                "Conditions should be parsed as non-negative integers"
            );
        }
        // Check timestamps correct (assume positive ns)
        for trade in &all_trades {
            assert!(
                trade.trade_ts_ns > 0,
                "Timestamps should be positive nanoseconds"
            );
        }
        // Additional checks based on sample data
        assert_eq!(
            all_trades[0].symbol, "MSFT",
            "First trade symbol should be MSFT"
        );
        assert_eq!(
            all_trades[0].price, 276.16,
            "First trade price should be 276.16"
        );
        assert_eq!(all_trades[0].size, 55, "First trade size should be 55");
        assert_eq!(
            all_trades[0].conditions,
            vec![12, 37],
            "First trade conditions should be [12, 37]"
        );
        assert_eq!(
            all_trades[0].exchange, 11,
            "First trade exchange should be 11"
        );
        assert_eq!(
            all_trades[1].symbol, "MSFT",
            "Second trade symbol should be MSFT"
        );
        assert_eq!(
            all_trades[1].price, 276.75,
            "Second trade price should be 276.75"
        );
        assert_eq!(all_trades[1].size, 100, "Second trade size should be 100");
        assert_eq!(
            all_trades[1].conditions,
            vec![12],
            "Second trade conditions should be [12]"
        );
        assert_eq!(
            all_trades[1].exchange, 8,
            "Second trade exchange should be 8"
        );
    }
}
