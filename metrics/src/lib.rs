// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Prometheus metrics.

use hyper::server::Server;
use hyper::{Request, Response};
use hyper::body::{Body, Incoming};
use prometheus::{Encoder, Histogram, HistogramOpts, IntCounter, TextEncoder};
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
        let encoder = TextEncoder::new();
        let service = hyper::service::service_fn(move |_req: Request<Incoming>| async move {
            let mut buffer = Vec::new();
            encoder.encode(&prometheus::gather(), &mut buffer).unwrap();
            Ok::<_, hyper::Error>(Response::new(Body::from(buffer)))
        });
        Server::builder(listener).serve(service).await.unwrap();
    }
}
