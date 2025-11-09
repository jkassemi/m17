// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Flatfile reader (stub).

use core_types::{DataBatch, EquityTrade, Nbbo, OptionTrade, QueryScope};
use futures::Stream;
use std::pin::Pin;

/// Stub flatfile source.
pub struct FlatfileSource;

impl FlatfileSource {
    pub fn new() -> Self {
        Self
    }

    /// Stub: Return empty stream.
    pub async fn get_nbbo(
        &self,
        _scope: QueryScope,
    ) -> Pin<Box<dyn Stream<Item = DataBatch<Nbbo>> + Send>> {
        Box::pin(futures::stream::empty())
    }
}
