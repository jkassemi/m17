// Copyright (c) James Kassemi, SC, US. All rights reserved.
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;

pub struct Metrics {
    classify_unknown_rate: IntCounter,
    nbbo_age_us: Histogram,
    last_request_ts_ns: Arc<Mutex<Option<i64>>>,
    flatfile_status: Arc<Mutex<String>>,
    last_config_reload_ts_ns: Arc<Mutex<Option<i64>>>,
    planned_days: AtomicU64,
    completed_days: AtomicU64,
    ingested_batches: AtomicU64,
    ingested_rows: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            classify_unknown_rate: IntCounter::new("classify_unknown_rate", "Unknown rate")
                .unwrap(),
            nbbo_age_us: Histogram::with_opts(HistogramOpts::new("nbbo_age_us", "NBBO age"))
                .unwrap(),
            last_request_ts_ns: Arc::new(Mutex::new(None)),
            flatfile_status: Arc::new(Mutex::new("Not started".to_string())),
            last_config_reload_ts_ns: Arc::new(Mutex::new(None)),
            planned_days: AtomicU64::new(0),
            completed_days: AtomicU64::new(0),
            ingested_batches: AtomicU64::new(0),
            ingested_rows: AtomicU64::new(0),
        }
    }

    pub fn last_request_ts_ns(&self) -> Option<i64> {
        *self.last_request_ts_ns.lock().unwrap()
    }

    pub fn flatfile_status(&self) -> String {
        self.flatfile_status.lock().unwrap().clone()
    }

    pub fn set_flatfile_status(&self, status: String) {
        *self.flatfile_status.lock().unwrap() = status;
    }

    pub fn last_config_reload_ts_ns(&self) -> Option<i64> {
        *self.last_config_reload_ts_ns.lock().unwrap()
    }

    pub fn set_last_config_reload_ts_ns(&self, ts: i64) {
        *self.last_config_reload_ts_ns.lock().unwrap() = Some(ts);
    }

    // planned/completed days
    pub fn add_planned_days(&self, n: u64) {
        self.planned_days.fetch_add(n, Ordering::Relaxed);
    }
    pub fn inc_completed_day(&self) {
        self.completed_days.fetch_add(1, Ordering::Relaxed);
    }
    pub fn planned_days(&self) -> u64 {
        self.planned_days.load(Ordering::Relaxed)
    }
    pub fn completed_days(&self) -> u64 {
        self.completed_days.load(Ordering::Relaxed)
    }

    // ingestion progress
    pub fn inc_batches(&self, n: u64) {
        self.ingested_batches.fetch_add(n, Ordering::Relaxed);
    }
    pub fn inc_rows(&self, n: u64) {
        self.ingested_rows.fetch_add(n, Ordering::Relaxed);
    }
    pub fn ingested_batches(&self) -> u64 {
        self.ingested_batches.load(Ordering::Relaxed)
    }
    pub fn ingested_rows(&self) -> u64 {
        self.ingested_rows.load(Ordering::Relaxed)
    }

    async fn handle_metrics(
        &self,
        _req: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
        // Update last request timestamp
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        *self.last_request_ts_ns.lock().unwrap() = Some(now);

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
