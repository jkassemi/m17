// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! DataClient trait and router (stub).

use core_types::{DataBatch, EquityTrade, Nbbo, OptionTrade, QueryScope};
use futures::Stream;
use std::pin::Pin;
use tokio_stream::StreamExt;

/// DataClient trait as per spec.
#[async_trait::async_trait]
pub trait DataClient {
    async fn get_option_trades(
        &self,
        scope: QueryScope,
    ) -> Pin<Box<dyn Stream<Item = DataBatch<OptionTrade>> + Send>>;
    async fn get_equity_trades(
        &self,
        scope: QueryScope,
    ) -> Pin<Box<dyn Stream<Item = DataBatch<EquityTrade>> + Send>>;
    async fn get_nbbo(
        &self,
        scope: QueryScope,
    ) -> Pin<Box<dyn Stream<Item = DataBatch<Nbbo>> + Send>>;
}

/// Router implementation (stub).
pub struct DataClientRouter;

impl DataClientRouter {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl DataClient for DataClientRouter {
    async fn get_option_trades(
        &self,
        _scope: QueryScope,
    ) -> Pin<Box<dyn Stream<Item = DataBatch<OptionTrade>> + Send>> {
        // Stub: Return empty stream
        Box::pin(tokio_stream::empty())
    }

    async fn get_equity_trades(
        &self,
        _scope: QueryScope,
    ) -> Pin<Box<dyn Stream<Item = DataBatch<EquityTrade>> + Send>> {
        // Stub: Return empty stream
        Box::pin(tokio_stream::empty())
    }

    async fn get_nbbo(
        &self,
        _scope: QueryScope,
    ) -> Pin<Box<dyn Stream<Item = DataBatch<Nbbo>> + Send>> {
        // Stub: Return empty stream
        Box::pin(tokio_stream::empty())
    }
}
