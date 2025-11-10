// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Stub WS worker for equities trades/NBBO.

use core_types::types::{DataBatch, EquityTrade};
use futures::Stream;
use std::pin::Pin;
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

    /// Stub: Return empty stream (no real connection yet)
    pub async fn run(&self) -> Pin<Box<dyn Stream<Item = DataBatch<EquityTrade>> + Send>> {
        // Stub: Return empty stream
        Box::pin(futures::stream::empty())
    }
}
