// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Main runtime with Tokio.

use classifier::Classifier;
use core_types::config::AppConfig;
use data_client::DataClientRouter;
use metrics::Metrics;
use nbbo_cache::NbboStore;
use storage::Storage;
use tokio::net::TcpListener;
use ws_source::worker::WsWorker;

#[tokio::main]
async fn main() {
    let config = AppConfig::load().unwrap();
    let data_client = DataClientRouter::new();
    let nbbo_store = NbboStore::new();
    let classifier = Classifier::new();
    let storage = Storage::new(config.storage);
    let metrics = std::sync::Arc::new(Metrics::new());  // Wrap in Arc to match the serve method signature

    // Stub WS worker
    let ws_worker = WsWorker::new("ws://example.com"); // Placeholder URL
    let _stream = ws_worker.run().await;

    // Stub metrics server
    let listener = TcpListener::bind("127.0.0.1:9090").await.unwrap();
    let metrics_clone = metrics.clone();  // Clone the Arc for the spawned task
    tokio::spawn(async move {
        metrics_clone.serve(listener).await;  // Use the cloned Arc
    });

    // Stub: Run forever
    tokio::signal::ctrl_c().await.unwrap();
}
