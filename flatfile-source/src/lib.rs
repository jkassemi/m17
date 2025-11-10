// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Flatfile reader (stub).

use core_types::config::FlatfileConfig;
use core_types::types::{DataBatch, EquityTrade, Nbbo, OptionTrade, QueryScope};
use futures::Stream;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};

/// Stub flatfile source.
pub struct FlatfileSource {
    status: Arc<Mutex<String>>,
}

impl FlatfileSource {
    pub fn new() -> Self {
        Self {
            status: Arc::new(Mutex::new("Initializing".to_string())),
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
