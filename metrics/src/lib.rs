// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Prometheus metrics. hyper v1.+

use hyper::body::{Body, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use prometheus::{Encoder, Histogram, HistogramOpts, IntCounter, TextEncoder};
use std::convert::Infallible;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::task;

pub struct Metrics {
    classify_unknown_rate: IntCounter,
    nbbo_age_us: Histogram,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            classify_unknown_rate: IntCounter::new("classify_unknown_rate", "Unknown rate")
                .unwrap(),
            nbbo_age_us: Histogram::with_opts(HistogramOpts::new("nbbo_age_us", "NBBO age"))
                .unwrap(),
        }
    }

    async fn handle_metrics(&self, _req: Request<Incoming>) -> Result<Response<Body>, Infallible> {
        let encoder = TextEncoder::new();
        let metric_families = prometheus::gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        Ok(Response::new(Body::from(buffer)))
    }

    pub async fn serve(self: &std::sync::Arc<Self>, listener: TcpListener) -> hyper::Result<()> {
        loop {
            let (stream, _addr) = listener.accept().await?;
            let metrics = self.clone();

            // Spawn per-connection task
            task::spawn(async move {
                if let Err(err) = http1::Builder::new()
                    .serve_connection(
                        stream,
                        service_fn(move |req| {
                            let metrics = metrics.clone();
                            async move { metrics.handle_metrics(req).await }
                        }),
                    )
                    .await
                {
                    eprintln!("metrics server connection error: {err}");
                }
            });
        }
    }
}

/*
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> hyper::Result<()> {
    let addr: SocketAddr = "127.0.0.1:9090".parse().unwrap();
    let listener = TcpListener::bind(addr).await.unwrap();

    let metrics = Arc::new(Metrics::new());
    metrics.serve(listener).await
}
*/
