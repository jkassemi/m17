// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Prometheus metrics. hyper v1.+

use hyper::{Body, Request, Response};
use hyper::body::Incoming;
use hyper::server::Server;
use hyper::service::service_fn;
use prometheus::{Encoder, Histogram, HistogramOpts, IntCounter, TextEncoder};
use std::convert::Infallible;
use tokio::net::TcpListener;

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
        let service = service_fn(move |req| {
            let metrics = self.clone();
            async move { metrics.handle_metrics(req).await }
        });
        Server::builder(listener).serve(service).await
    }
}
