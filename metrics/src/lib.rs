// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Prometheus metrics.

use hyper::{Body, Request, Response, Server};
use prometheus::{Encoder, Gauge, Histogram, HistogramOpts, IntCounter, TextEncoder};
use std::net::SocketAddr;
use tokio::net::TcpListener;

pub struct Metrics {
    classify_unknown_rate: IntCounter,
    nbbo_age_us: Histogram,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            classify_unknown_rate: IntCounter::new("classify_unknown_rate", "Unknown rate").unwrap(),
            nbbo_age_us: Histogram::with_opts(HistogramOpts::new("nbbo_age_us", "NBBO age")).unwrap(),
        }
    }

    pub async fn serve(&self, listener: TcpListener) {
        // Stub: Basic HTTP server for /metrics
        let service = hyper::service::make_service_fn(|_| {
            let encoder = TextEncoder::new();
            async move {
                Ok::<_, hyper::Error>(hyper::service::service_fn(move |_req| async move {
                    let mut buffer = Vec::new();
                    encoder.encode(&prometheus::gather(), &mut buffer).unwrap();
                    Ok(Response::new(Body::from(buffer)))
                }))
            }
        });
        Server::builder(listener).serve(service).await.unwrap();
    }
}
