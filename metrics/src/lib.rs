// Copyright (c) James Kassemi,// Copyright (c) James Kassemi, SC, US. All rights reserved.
//! Prometheus metrics. hyper v1.+
use http_body_util::Full;
use hyper::body::{Bytes, Incoming}; // Removed 'Body' from import
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::Request;
use hyper::Response;
use hyper_util::rt::TokioIo;
use prometheus::{Encoder, Histogram, HistogramOpts, IntCounter, TextEncoder};
use std::error::Error;
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

    async fn handle_metrics(
        &self,
        _req: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
        let encoder = TextEncoder::new();
        let metric_families = prometheus::gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        Ok(Response::new(Full::new(Bytes::from(buffer))))
    }

    pub async fn serve(
        self: &std::sync::Arc<Self>,
        listener: TcpListener,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        loop {
            let (socket, _) = listener.accept().await?;
            let io = TokioIo::new(socket);
            let metrics = self.clone();
            let service = service_fn(move |req| {
                let metrics = metrics.clone();
                async move { metrics.handle_metrics(req).await }
            });
            tokio::spawn(async move {
                if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                    eprintln!("Error serving connection: {:?}", err);
                }
            });
        }
    }
}
