// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Stub WS worker for equities trades/NBBO.

use core_types::types::{DataBatch, EquityTrade};
use futures::Stream;
use std::pin::Pin;
use tokio_tungstenite::connect_async;
use url::Url;

/// Stub WS worker.
pub struct WsWorker {
    url: Url,
}

impl WsWorker {
    pub fn new(url: &str) -> Self {
        Self {
            url: Url::parse(url).unwrap(),
        }
    }

    /// Stub: Connect and emit empty batches.
    pub async fn run(&self) -> Pin<Box<dyn Stream<Item = DataBatch<EquityTrade>> + Send>> {
        // Placeholder connection (doesn't connect to real endpoint yet)
        let (_ws_stream, _) = connect_async(self.url.as_str()).await.unwrap();
        // Stub: Return empty stream
        Box::pin(futures::stream::empty())
    }
}
