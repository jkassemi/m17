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
use prometheus::{
    register_gauge_vec, register_histogram, register_int_counter, register_int_gauge,
    register_int_gauge_vec, Encoder, GaugeVec, Histogram, IntCounter, IntGauge, IntGaugeVec,
    TextEncoder,
};
use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
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
    current_files: Arc<Mutex<HashMap<u64, CurrentFileState>>>,
    current_file_seq: AtomicU64,
    service_statuses: Arc<Mutex<Vec<ServiceStatusHandle>>>,
    service_metrics: Arc<RwLock<Vec<Arc<dyn ServiceMetricsReporter>>>>,
    service_gauges: GaugeVec,
    planned_days_gauge: IntGauge,
    completed_days_gauge: IntGauge,
    ingested_batches_counter: IntCounter,
    ingested_rows_counter: IntCounter,
    active_files_gauge: IntGauge,
    queue_depth_gauges: IntGaugeVec,
    enrichment_row_counter: IntCounter,
    enrichment_batch_histogram: Histogram,
    metrics_port: AtomicU16,
    uptime_gauge: IntGauge,
    start_time: Instant,
    download_bytes_counter: IntCounter,
}

#[derive(Clone)]
pub struct CurrentFileSnapshot {
    pub id: u64,
    pub name: String,
    pub total: u64,
    pub read: u64,
    pub started_ns: i64,
}

pub struct FileProgressGuard {
    metrics: Arc<Metrics>,
    id: u64,
}

struct CurrentFileState {
    name: String,
    total: u64,
    read: u64,
    started_ns: i64,
}

impl Metrics {
    pub fn new() -> Self {
        let service_gauges = register_gauge_vec!(
            "service_gauge",
            "Service supplied gauges exposed by orchestrator-managed components",
            &["service", "metric"]
        )
        .unwrap();
        let planned_days_gauge = register_int_gauge!(
            "ingest_planned_days",
            "Total number of ingestion days scheduled across all ranges"
        )
        .unwrap();
        let completed_days_gauge = register_int_gauge!(
            "ingest_completed_days",
            "Number of ingestion days successfully completed"
        )
        .unwrap();
        let ingested_batches_counter = register_int_counter!(
            "ingest_batches_total",
            "Total number of data batches processed by flatfile ingestion"
        )
        .unwrap();
        let ingested_rows_counter = register_int_counter!(
            "ingest_rows_total",
            "Total number of rows processed by flatfile ingestion"
        )
        .unwrap();
        let active_files_gauge = register_int_gauge!(
            "ingest_active_files",
            "Number of flatfile downloads currently in flight"
        )
        .unwrap();
        let queue_depth_gauges = register_int_gauge_vec!(
            "ingest_queue_depth",
            "Buffered items waiting in internal ingestion queues",
            &["queue"]
        )
        .unwrap();
        let enrichment_row_counter = register_int_counter!(
            "ingest_enriched_rows_total",
            "Total option rows enriched via GreeksEngine during flatfile replay"
        )
        .unwrap();
        let enrichment_batch_histogram = register_histogram!(
            "ingest_enrichment_batch_seconds",
            "Wall-clock seconds spent enriching each flatfile batch",
            vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0]
        )
        .unwrap();
        let download_bytes_counter = register_int_counter!(
            "ingest_flatfile_download_bytes_total",
            "Total bytes downloaded by the flatfile prefetcher"
        )
        .unwrap();
        let uptime_gauge = register_int_gauge!(
            "process_uptime_seconds",
            "Wall-clock seconds since the orchestrator process started"
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
            current_files: Arc::new(Mutex::new(HashMap::new())),
            current_file_seq: AtomicU64::new(0),
            service_statuses: Arc::new(Mutex::new(Vec::new())),
            service_metrics: Arc::new(RwLock::new(Vec::new())),
            service_gauges,
            planned_days_gauge,
            completed_days_gauge,
            ingested_batches_counter,
            ingested_rows_counter,
            active_files_gauge,
            queue_depth_gauges,
            enrichment_row_counter,
            enrichment_batch_histogram,
            metrics_port: AtomicU16::new(8080),
            uptime_gauge,
            start_time: Instant::now(),
            download_bytes_counter,
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
        let total = self.planned_days.fetch_add(n, Ordering::Relaxed) + n;
        self.planned_days_gauge.set(total as i64);
    }
    pub fn inc_completed_day(&self) {
        let total = self.completed_days.fetch_add(1, Ordering::Relaxed) + 1;
        self.completed_days_gauge.set(total as i64);
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
        self.ingested_batches_counter.inc_by(n);
    }
    pub fn inc_rows(&self, n: u64) {
        self.ingested_rows.fetch_add(n, Ordering::Relaxed);
        self.ingested_rows_counter.inc_by(n);
    }
    pub fn ingested_batches(&self) -> u64 {
        self.ingested_batches.load(Ordering::Relaxed)
    }
    pub fn ingested_rows(&self) -> u64 {
        self.ingested_rows.load(Ordering::Relaxed)
    }

    pub fn current_files(&self) -> Vec<CurrentFileSnapshot> {
        let files = self.current_files.lock().unwrap();
        let mut snapshots: Vec<_> = files
            .iter()
            .map(|(id, state)| CurrentFileSnapshot {
                id: *id,
                name: state.name.clone(),
                total: state.total,
                read: state.read,
                started_ns: state.started_ns,
            })
            .collect();
        snapshots.sort_by_key(|snapshot| snapshot.started_ns);
        snapshots
    }

    pub fn track_current_file(self: &Arc<Self>, name: String, total: u64) -> FileProgressGuard {
        let id = self.current_file_seq.fetch_add(1, Ordering::Relaxed) + 1;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        let mut files = self.current_files.lock().unwrap();
        files.insert(
            id,
            CurrentFileState {
                name,
                total,
                read: 0,
                started_ns: now,
            },
        );
        self.active_files_gauge.inc();
        FileProgressGuard {
            metrics: Arc::clone(self),
            id,
        }
    }

    fn update_current_file_read(&self, id: u64, read: u64) {
        if let Some(entry) = self.current_files.lock().unwrap().get_mut(&id) {
            entry.read = read;
        }
    }

    fn finish_current_file(&self, id: u64) {
        if self.current_files.lock().unwrap().remove(&id).is_some() {
            self.active_files_gauge.dec();
        }
    }

    pub fn set_queue_depth(&self, queue: &str, depth: usize) {
        self.queue_depth_gauges
            .with_label_values(&[queue])
            .set(depth as i64);
    }

    pub fn observe_enrichment(&self, rows: usize, duration: std::time::Duration) {
        if rows > 0 {
            self.enrichment_row_counter.inc_by(rows as u64);
        }
        self.enrichment_batch_histogram
            .observe(duration.as_secs_f64());
    }

    pub fn add_downloaded_bytes(&self, bytes: u64) {
        if bytes > 0 {
            self.download_bytes_counter.inc_by(bytes);
        }
    }

    pub fn register_service_status(&self, handle: ServiceStatusHandle) {
        self.service_statuses.lock().unwrap().push(handle.clone());
        let reporter: Arc<dyn ServiceMetricsReporter> = Arc::new(handle);
        self.register_service_metrics(reporter);
    }

    pub fn set_metrics_port(&self, port: u16) {
        self.metrics_port.store(port, Ordering::Relaxed);
    }

    pub fn metrics_port(&self) -> u16 {
        self.metrics_port.load(Ordering::Relaxed)
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
        let uptime_secs = self.start_time.elapsed().as_secs() as i64;
        self.uptime_gauge.set(uptime_secs);

        let encoder = TextEncoder::new();
        let metric_families = prometheus::gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        let response = Response::builder()
            .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            .body(Full::new(Bytes::from(buffer)))
            .unwrap();
        Ok(response)
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

impl FileProgressGuard {
    pub fn update_read(&self, read: u64) {
        self.metrics.update_current_file_read(self.id, read);
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}

impl Drop for FileProgressGuard {
    fn drop(&mut self) {
        self.metrics.finish_current_file(self.id);
    }
}
