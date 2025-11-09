// Copyright (c) James Kassemi, SC, US. All rights reserved.

use core_types::config::AppConfig;
use ws_source::worker::WsWorker;
use metrics::encode_metrics;
use warp::Filter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::init();

    let config = AppConfig::load()?;
    tracing::info!("Loaded config: {:?}", config);

    // Start stub WS worker
    let ws_worker = WsWorker::new("wss://echo.websocket.org".to_string());  // Placeholder
    tokio::spawn(async move {
        if let Err(e) = ws_worker.run().await {
            tracing::error!("WS worker error: {}", e);
        }
    });

    // Metrics endpoint
    let metrics_route = warp::path("metrics").map(|| {
        warp::reply::with_header(encode_metrics(), "content-type", "text/plain")
    });

    warp::serve(metrics_route).run(([127, 0, 0, 1], 9090)).await;
    Ok(())
}
