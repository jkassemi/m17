use std::{
    net::SocketAddr,
    sync::{Arc, OnceLock},
    thread,
};

use http_body_util::Full;
use hyper::{
    Request, Response,
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use nbbo_engine::{AggressorMetrics, AggressorMetricsSnapshot};
use prometheus::{Encoder, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder};
use tokio::{net::TcpListener, sync::oneshot};
use trade_flatfile_engine::{DownloadMetrics, DownloadSnapshot};
use window_space::{
    WindowSpaceController,
    payload::SlotKind,
    slot_metrics::{SlotCountsSnapshot, SlotMetrics, SlotMetricsSnapshot},
};

pub struct MetricsServer {
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl MetricsServer {
    pub fn start(
        slot_metrics: Arc<SlotMetrics>,
        download_metrics: Arc<DownloadMetrics>,
        controller: Arc<WindowSpaceController>,
        aggressor_metrics: Arc<nbbo_engine::AggressorMetrics>,
        addr: SocketAddr,
    ) -> Self {
        static LOG_ONCE: OnceLock<()> = OnceLock::new();
        LOG_ONCE.get_or_init(|| {
            println!("Metrics server listening on {}", addr);
        });
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let handle = thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().expect("start metrics runtime");
            runtime.block_on(run_http(
                slot_metrics,
                download_metrics,
                controller,
                aggressor_metrics,
                addr,
                shutdown_rx,
            ));
        });
        Self {
            shutdown: Some(shutdown_tx),
            handle: Some(handle),
        }
    }

    pub fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

async fn run_http(
    slot_metrics: Arc<SlotMetrics>,
    download_metrics: Arc<DownloadMetrics>,
    controller: Arc<WindowSpaceController>,
    aggressor_metrics: Arc<AggressorMetrics>,
    addr: SocketAddr,
    mut shutdown: oneshot::Receiver<()>,
) {
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(err) => {
            eprintln!("metrics: failed to bind {}: {}", addr, err);
            return;
        }
    };
    let exporter = Arc::new(MetricsExporter::new());
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, _)) => {
                        let exporter = exporter.clone();
                        let slot_metrics = Arc::clone(&slot_metrics);
                        let download_metrics = Arc::clone(&download_metrics);
                        let controller = Arc::clone(&controller);
                        let aggressor_metrics = Arc::clone(&aggressor_metrics);
                        tokio::spawn(async move {
                            if let Err(err) = serve_connection(
                                stream,
                                exporter,
                                slot_metrics,
                                download_metrics,
                                controller,
                                aggressor_metrics,
                            )
                            .await
                            {
                                eprintln!("metrics connection error: {err}");
                            }
                        });
                    }
                    Err(err) => {
                        eprintln!("metrics accept error: {err}");
                    }
                }
            }
        }
    }
}

async fn serve_connection(
    stream: tokio::net::TcpStream,
    exporter: Arc<MetricsExporter>,
    slot_metrics: Arc<SlotMetrics>,
    download_metrics: Arc<DownloadMetrics>,
    controller: Arc<WindowSpaceController>,
    aggressor_metrics: Arc<AggressorMetrics>,
) -> Result<(), hyper::Error> {
    let io = TokioIo::new(stream);
    let service = service_fn(move |req: Request<Incoming>| {
        let exporter = exporter.clone();
        let slot_metrics = Arc::clone(&slot_metrics);
        let download_metrics = Arc::clone(&download_metrics);
        let controller = Arc::clone(&controller);
        let aggressor_metrics = Arc::clone(&aggressor_metrics);
        async move {
            let response = handle_request(
                req,
                exporter,
                slot_metrics,
                download_metrics,
                controller,
                aggressor_metrics,
            );
            Ok::<_, hyper::Error>(response)
        }
    });
    http1::Builder::new().serve_connection(io, service).await?;
    Ok(())
}

fn handle_request(
    req: Request<Incoming>,
    exporter: Arc<MetricsExporter>,
    slot_metrics: Arc<SlotMetrics>,
    download_metrics: Arc<DownloadMetrics>,
    controller: Arc<WindowSpaceController>,
    aggressor_metrics: Arc<AggressorMetrics>,
) -> Response<Full<Bytes>> {
    if req.uri().path() != "/metrics" {
        return Response::builder()
            .status(404)
            .body(Full::new(Bytes::from_static(b"not found")))
            .unwrap_or_else(|_| Response::new(Full::new(Bytes::from_static(b"not found"))));
    }
    let snapshot = slot_metrics.snapshot();
    let downloads = download_metrics.snapshot();
    let symbol_count = controller.symbol_count();
    let classification = aggressor_metrics.snapshot();
    let body = exporter
        .render(snapshot, &downloads, symbol_count, classification)
        .unwrap_or_else(|_| b"metrics_unavailable".to_vec());
    Response::builder()
        .status(200)
        .header("content-type", "text/plain; version=0.0.4")
        .body(Full::new(Bytes::from(body)))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::from_static(b"bad response"))))
}

struct MetricsExporter {
    registry: Registry,
    slot_gauge: IntGaugeVec,
    window_gauge: IntGaugeVec,
    download_streamed: IntGaugeVec,
    download_remaining: IntGaugeVec,
    symbol_count: IntGauge,
    classification_total: IntGaugeVec,
}

impl MetricsExporter {
    fn new() -> Self {
        let registry = Registry::new();
        let slot_gauge = IntGaugeVec::new(
            Opts::new(
                "windowspace_slots",
                "Window space slot state counts per slot kind",
            ),
            &["kind", "slot", "state"],
        )
        .expect("slot gauge");
        let window_gauge = IntGaugeVec::new(
            Opts::new(
                "windowspace_windows_total",
                "Total tracked windows per store",
            ),
            &["kind"],
        )
        .expect("window gauge");
        registry
            .register(Box::new(slot_gauge.clone()))
            .expect("register slot gauge");
        registry
            .register(Box::new(window_gauge.clone()))
            .expect("register window gauge");
        let download_streamed = IntGaugeVec::new(
            Opts::new(
                "flatfile_slot_bytes_streamed",
                "Compressed bytes streamed for current flatfile downloads",
            ),
            &["slot"],
        )
        .expect("download streamed gauge");
        registry
            .register(Box::new(download_streamed.clone()))
            .expect("register download streamed");
        let download_remaining = IntGaugeVec::new(
            Opts::new(
                "flatfile_slot_bytes_remaining",
                "Compressed bytes remaining for current flatfile downloads",
            ),
            &["slot"],
        )
        .expect("download remaining gauge");
        registry
            .register(Box::new(download_remaining.clone()))
            .expect("register download remaining");
        let symbol_count = IntGauge::with_opts(Opts::new(
            "windowspace_symbol_count",
            "Current number of symbols tracked in the window space",
        ))
        .expect("symbol count gauge");
        registry
            .register(Box::new(symbol_count.clone()))
            .expect("register symbol count gauge");
        let classification_total = IntGaugeVec::new(
            Opts::new(
                "aggressor_classifications_total",
                "Cumulative aggressor classifications by stream",
            ),
            &["kind"],
        )
        .expect("classification gauge");
        registry
            .register(Box::new(classification_total.clone()))
            .expect("register classification gauge");
        Self {
            registry,
            slot_gauge,
            window_gauge,
            download_streamed,
            download_remaining,
            symbol_count,
            classification_total,
        }
    }

    fn render(
        &self,
        snapshot: SlotMetricsSnapshot,
        downloads: &[DownloadSnapshot],
        symbols: usize,
        classifications: AggressorMetricsSnapshot,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        self.window_gauge
            .with_label_values(&["trade"])
            .set(snapshot.trade_windows as i64);
        self.window_gauge
            .with_label_values(&["enrichment"])
            .set(snapshot.enrichment_windows as i64);
        self.record_slots(&snapshot.trade_slots);
        self.record_slots(&snapshot.enrichment_slots);
        self.record_downloads(downloads);
        self.symbol_count.set(symbols as i64);
        self.record_classifications(classifications);
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        TextEncoder::new().encode(&metric_families, &mut buffer)?;
        Ok(buffer)
    }

    fn record_slots(&self, slots: &[SlotCountsSnapshot]) {
        for slot in slots {
            let (kind_label, slot_label) = match slot.slot {
                SlotKind::Trade(kind) => ("trade", kind.label()),
                SlotKind::Enrichment(kind) => ("enrichment", kind.label()),
            };
            self.slot_gauge
                .with_label_values(&[kind_label, slot_label, "empty"])
                .set(slot.counts.empty as i64);
            self.slot_gauge
                .with_label_values(&[kind_label, slot_label, "pending"])
                .set(slot.counts.pending as i64);
            self.slot_gauge
                .with_label_values(&[kind_label, slot_label, "filled"])
                .set(slot.counts.filled as i64);
            self.slot_gauge
                .with_label_values(&[kind_label, slot_label, "cleared"])
                .set(slot.counts.cleared as i64);
            self.slot_gauge
                .with_label_values(&[kind_label, slot_label, "retired"])
                .set(slot.counts.retired as i64);
        }
    }

    fn record_downloads(&self, downloads: &[DownloadSnapshot]) {
        for snapshot in downloads {
            let label = snapshot.slot.label();
            self.download_streamed
                .with_label_values(&[label])
                .set(snapshot.streamed_bytes as i64);
            let remaining = snapshot
                .total_bytes
                .and_then(|total| total.checked_sub(snapshot.streamed_bytes))
                .unwrap_or(0);
            self.download_remaining
                .with_label_values(&[label])
                .set(remaining as i64);
        }
    }

    fn record_classifications(&self, snapshot: AggressorMetricsSnapshot) {
        self.classification_total
            .with_label_values(&["option"])
            .set(snapshot.option_total as i64);
        self.classification_total
            .with_label_values(&["underlying"])
            .set(snapshot.underlying_total as i64);
        self.classification_total
            .with_label_values(&["combined"])
            .set((snapshot.option_total + snapshot.underlying_total) as i64);
    }
}
