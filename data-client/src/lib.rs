// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! DataClient trait.

use core_types::types::{DataBatch, EquityTrade, Nbbo, OptionTrade, QueryScope};
use futures::Stream;
use std::pin::Pin;

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
