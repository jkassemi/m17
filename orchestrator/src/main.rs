// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Main runtime with Tokio.

use core_types::AppConfig;
use data_client::DataClientRouter;
use metrics::Metrics;
use nbbo_cache::NbboStore;
use storage::Storage;
use tokio::net::TcpListener;
use ws_source::worker::WsWorker;

#[tokio::main]
async fn main() {
    let config = AppConfig::load().unwrap_or_default();
    let data_client = DataClientRouter::new();
    let nbbo_store = NbboStore::new();
    let classifier = classifier::