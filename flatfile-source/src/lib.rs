// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Flatfile reader (stub).

use async_compression::tokio::bufread::GzipDecoder;
use async_trait::async_trait;
use bytes::Bytes;
use core_types::config::FlatfileConfig;
use core_types::types::{DataBatch, EquityTrade, Nbbo, OptionTrade, QueryScope};
use futures::Stream;
use futures::TryStreamExt;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::fs::File;
use tokio::io::BufReader;
use tokio::time::{sleep, Duration};
use tokio_util::io::ReaderStream;

/// Object store trait for accessing files.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Check if a file exists (head operation).
    async fn head(&self, path: &str) -> Result<(), Box<dyn std::error::Error>>;

    /// List files with a given prefix.
    async fn list(&self, prefix: &str) -> Result<Vec<String>, Box<dyn std::error::Error>>;

    /// Get a stream of bytes for a file.
    async fn get_stream(
        &self,
        path: &str,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<Bytes, Box<dyn std::error::Error + Send + Sync>>> + Send>>,
        Box<dyn std::error::Error>,
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
    async fn head(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
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

    async fn list(&self, prefix: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
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
        Box<dyn std::error::Error>,
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
    async fn head(&self, _path: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Stub: Always return error
        Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "S3 not implemented",
        )))
    }

    async fn list(&self, _prefix: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        // Stub: Return empty list
        Ok(Vec::new())
    }

    async fn get_stream(
        &self,
        _path: &str,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<Bytes, Box<dyn std::error::Error + Send + Sync>>> + Send>>,
        Box<dyn std::error::Error>,
    > {
        // Stub: Return empty stream
        Ok(Box::pin(futures::stream::empty()))
    }
}

/// Stub flatfile source.
pub struct FlatfileSource {
    status: Arc<Mutex<String>>,
    store: Box<dyn ObjectStore>,
}

impl FlatfileSource {
    pub fn new(store: Box<dyn ObjectStore>) -> Self {
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
}
