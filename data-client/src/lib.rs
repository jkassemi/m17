// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! DataClient trait and router (stub).

use core_types::data_client::DataClient;
use core_types::types::{DataBatch, EquityTrade, Nbbo, OptionTrade, QueryScope};
use futures::Stream;
use std::pin::Pin;

/// Router implementation (stub).
pub struct DataClientRouter {
    flatfile_source: flatfile_source::FlatfileSource,
}

impl DataClientRouter {
    pub fn new(flatfile_source: flatfile_source::FlatfileSource) -> Self {
        Self { flatfile_source }
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
        scope: QueryScope,
    ) -> Pin<Box<dyn Stream<Item = DataBatch<EquityTrade>> + Send>> {
        self.flatfile_source.get_equity_trades(scope).await
    }

    async fn get_nbbo(
        &self,
        _scope: QueryScope,
    ) -> Pin<Box<dyn Stream<Item = DataBatch<Nbbo>> + Send>> {
        // Stub: Return empty stream
        Box::pin(tokio_stream::empty())
    }
}
