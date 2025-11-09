// Copyright (c) James Kassemi, SC, US. All rights reserved.

use core_types::*;
use tokio_stream::Stream;
use std::pin::Pin;

// QueryScope and DataClient trait from spec
#[derive(Debug)]
pub struct QueryScope {
    pub instruments: Vec<String>,  // Stub
    pub time: (i64, i64),  // Stub
    pub mode: String,  // "Realtime", etc.
    pub quality_target: Quality,
}

#[async_trait::async_trait]
pub trait DataClient {
    async fn get_option_trades(&self, scope: QueryScope) -> Pin<Box<dyn Stream<Item = DataBatch<OptionTrade>> + Send>>;
    async fn get_equity_trades(&self, scope: QueryScope) -> Pin<Box<dyn Stream<Item = DataBatch<EquityTrade>> + Send>>;
    async fn get_nbbo(&self, scope: QueryScope) -> Pin<Box<dyn Stream<Item = DataBatch<Nbbo>> + Send>>;
}

// Router stub
pub struct DataClientRouter;

#[async_trait::async_trait]
impl DataClient for DataClientRouter {
    // Stub implementations returning empty streams
    async fn get_option_trades(&self, _scope: QueryScope) -> Pin<Box<dyn Stream<Item = DataBatch<OptionTrade>> + Send>> {
        Box::pin(tokio_stream::empty())
    }
    // ... similar for others
}
