// Copyright (c) James Kassemi, SC, US. All rights reserved.
//! Prometheus metrics. hyper v1.+
use core_types::status::{
    MetricSample, ServiceMetricsReporter, ServiceStatusHandle, ServiceStatusSnapshot,
};
use http_body_util::Full;
use hyper::body::{Bytes, Incoming}; // Removed 'Body' from import
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::Request;
use hyper::Response;
use hyper_util::rt::TokioIo;
use prometheus::{register_gauge_vec, Encoder, GaugeVec, TextEncoder};
use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::time::{self, Duration};

pub struct Metrics {
    last_request_ts_ns: Arc<Mutex<Option<i64>>>,
    flatfile_status: Arc<Mutex<String>>,
    last_config_reload_ts_ns: Arc<Mutex<Option<i64>>>,
    planned_days: AtomicU64,
    completed_days: AtomicU64,
    ingested_batches: AtomicU64,
    ingested_rows: AtomicU64,
    // Current file progress
    current_file_name: Arc<Mutex<Option<String>>>,
    current_file_total: AtomicU64,
    current_file_read: AtomicU64,
    current_file_started_ts_ns: AtomicU64,
    service_statuses: Arc<Mutex<Vec<ServiceStatusHandle>>>,
    service_metrics: Arc<RwLock<Vec<Arc<dyn ServiceMetricsReporter>>>>,
    service_gauges: GaugeVec,
}

impl Metrics {
    pub fn new() -> Self {
        let service_gauges = register_gauge_vec!(
            "service_gauge",
            "Service supplied gauges exposed by orchestrator-managed components",
            &["service", "metric"]
        )
        .unwrap();
        Self {
            last_request_ts_ns: Arc::new(Mutex::new(None)),
            flatfile_status: Arc::new(Mutex::new("Not started".to_string())),
            last_config_reload_ts_ns: Arc::new(Mutex::new(None)),
            planned_days: AtomicU64::new(0),
            completed_days: AtomicU64::new(0),
            ingested_batches: AtomicU64::new(0),
            ingested_rows: AtomicU64::new(0),
            current_file_name: Arc::new(Mutex::new(None)),
            current_file_total: AtomicU64::new(0),
            current_file_read: AtomicU64::new(0),
            current_file_started_ts_ns: AtomicU64::new(0),
            service_statuses: Arc::new(Mutex::new(Vec::new())),
            service_metrics: Arc::new(RwLock::new(Vec::new())),
            service_gauges,
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

    // current file progress
    pub fn set_current_file(&self, name: String, total: u64) {
        *self.current_file_name.lock().unwrap() = Some(name);
        self.current_file_total.store(total, Ordering::Relaxed);
        self.current_file_read.store(0, Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        self.current_file_started_ts_ns
            .store(now as u64, Ordering::Relaxed);
    }

    pub fn set_current_file_read(&self, read: u64) {
        self.current_file_read.store(read, Ordering::Relaxed);
    }

    pub fn current_file_name(&self) -> Option<String> {
        self.current_file_name.lock().unwrap().clone()
    }

    pub fn current_file_progress(&self) -> Option<(u64, u64, i64)> {
        if self.current_file_name.lock().unwrap().is_none() {
            return None;
        }
        let read = self.current_file_read.load(Ordering::Relaxed);
        let total = self.current_file_total.load(Ordering::Relaxed);
        let started = self.current_file_started_ts_ns.load(Ordering::Relaxed) as i64;
        Some((read, total, started))
    }

    pub fn register_service_status(&self, handle: ServiceStatusHandle) {
        self.service_statuses.lock().unwrap().push(handle.clone());
        let reporter: Arc<dyn ServiceMetricsReporter> = Arc::new(handle);
        self.register_service_metrics(reporter);
    }

    pub fn service_status_snapshots(&self) -> Vec<ServiceStatusSnapshot> {
        self.service_statuses
            .lock()
            .unwrap()
            .iter()
            .map(|handle| handle.snapshot())
            .collect()
    }

    pub fn register_service_metrics(&self, reporter: Arc<dyn ServiceMetricsReporter>) {
        self.service_metrics.write().unwrap().push(reporter);
    }

    pub fn spawn_service_metric_task(
        self: &Arc<Self>,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let metrics = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = time::interval(interval);
            loop {
                ticker.tick().await;
                metrics.collect_service_metrics();
            }
        })
    }

    fn collect_service_metrics(&self) {
        let reporters = {
            let guard = self.service_metrics.read().unwrap();
            guard.clone()
        };
        for reporter in reporters {
            let samples = reporter.collect_metrics();
            let service = reporter.service_name().to_string();
            for sample in samples {
                self.record_metric(&service, &sample);
            }
        }
    }

    fn record_metric(&self, service: &str, sample: &MetricSample) {
        let gauge = self
            .service_gauges
            .with_label_values(&[service, sample.metric.as_str()]);
        gauge.set(sample.value);
        // Additional labels are not exposed yet; keep metric cardinality bounded.
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
