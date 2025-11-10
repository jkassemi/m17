// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! REST client (stub).

use core_types::types::{DataBatch, EquityTrade, Nbbo, OptionTrade, QueryScope};
use futures::Stream;
use std::pin::Pin;

/// Stub REST source.
pub struct RestSource;

impl RestSource {
    pub fn new() -> Self {
        Self
    }

    /// Stub: Return empty stream.
    pub async fn get_option_trades(
        &self,
        _scope: QueryScope,
    ) -> Pin<Box<dyn Stream<Item = DataBatch<OptionTrade>> + Send>> {
        Box::pin(futures::stream::empty())
    }
}
