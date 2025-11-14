use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex, OnceLock},
    thread,
    time::SystemTime,
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
use quote_backfill_engine::{QuoteBackfillMetrics, QuoteBackfillMetricsSnapshot};
use tokio::{net::TcpListener, sync::oneshot};
use trade_flatfile_engine::{DownloadMetrics, DownloadSnapshot};
use trade_ws_engine::{TradeWsMetrics, TradeWsMetricsSnapshot};
use window_space::{
    PayloadStoreCounts, SetCounterSnapshot, WindowSpaceController,
    payload::{EnrichmentSlotKind, SlotKind},
    slot_metrics::{SlotCountsSnapshot, SlotMetrics, SlotMetricsSnapshot},
};

pub struct EngineStatusSample {
    pub name: String,
    pub seconds_since_start: i64,
}

#[derive(Default)]
pub struct EngineStatusRegistry {
    inner: Mutex<HashMap<&'static str, EngineRuntimeState>>,
}

#[derive(Clone, Copy)]
struct EngineRuntimeState {
    running: bool,
    started_at: Option<SystemTime>,
}

impl EngineStatusRegistry {
    pub fn mark_started(&self, name: &'static str) {
        let mut guard = self.inner.lock().unwrap();
        guard.insert(
            name,
            EngineRuntimeState {
                running: true,
                started_at: Some(SystemTime::now()),
            },
        );
    }

    pub fn mark_stopped(&self, name: &'static str) {
        let mut guard = self.inner.lock().unwrap();
        guard
            .entry(name)
            .and_modify(|state| {
                state.running = false;
            })
            .or_insert(EngineRuntimeState {
                running: false,
                started_at: None,
            });
    }

    pub fn snapshot(&self) -> Vec<EngineStatusSample> {
        let guard = self.inner.lock().unwrap();
        guard
            .iter()
            .map(|(name, state)| {
                let seconds = if state.running {
                    state
                        .started_at
                        .and_then(|ts| SystemTime::now().duration_since(ts).ok())
                        .map(|dur| dur.as_secs() as i64)
                        .unwrap_or(0)
                } else {
                    0
                };
                EngineStatusSample {
                    name: (*name).to_string(),
                    seconds_since_start: seconds,
                }
            })
            .collect()
    }
}

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
        trade_ws_metrics: Arc<TradeWsMetrics>,
        quote_backfill_metrics: Arc<QuoteBackfillMetrics>,
        engine_status: Arc<EngineStatusRegistry>,
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
                trade_ws_metrics,
                quote_backfill_metrics,
                engine_status,
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
    trade_ws_metrics: Arc<TradeWsMetrics>,
    quote_backfill_metrics: Arc<QuoteBackfillMetrics>,
    engine_status: Arc<EngineStatusRegistry>,
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
                        let trade_ws_metrics = Arc::clone(&trade_ws_metrics);
                        let quote_backfill_metrics = Arc::clone(&quote_backfill_metrics);
                        let engine_status = Arc::clone(&engine_status);
                        tokio::spawn(async move {
                            if let Err(err) = serve_connection(
                                stream,
                                exporter,
                                slot_metrics,
                                download_metrics,
                                controller,
                                aggressor_metrics,
                                trade_ws_metrics,
                                quote_backfill_metrics,
                                engine_status,
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
    trade_ws_metrics: Arc<TradeWsMetrics>,
    quote_backfill_metrics: Arc<QuoteBackfillMetrics>,
    engine_status: Arc<EngineStatusRegistry>,
) -> Result<(), hyper::Error> {
    let io = TokioIo::new(stream);
    let service = service_fn(move |req: Request<Incoming>| {
        let exporter = exporter.clone();
        let slot_metrics = Arc::clone(&slot_metrics);
        let download_metrics = Arc::clone(&download_metrics);
        let controller = Arc::clone(&controller);
        let aggressor_metrics = Arc::clone(&aggressor_metrics);
        let trade_ws_metrics = Arc::clone(&trade_ws_metrics);
        let quote_backfill_metrics = Arc::clone(&quote_backfill_metrics);
        let engine_status = Arc::clone(&engine_status);
        async move {
            let response = handle_request(
                req,
                exporter,
                slot_metrics,
                download_metrics,
                controller,
                aggressor_metrics,
                trade_ws_metrics,
                quote_backfill_metrics,
                engine_status,
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
    trade_ws_metrics: Arc<TradeWsMetrics>,
    quote_backfill_metrics: Arc<QuoteBackfillMetrics>,
    engine_status: Arc<EngineStatusRegistry>,
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
    let payload_counts = controller.payload_store_counts();
    let classification = aggressor_metrics.snapshot();
    let set_counts = controller.set_counter_snapshot();
    let trade_ws = trade_ws_metrics.snapshot();
    let quote_backfill = quote_backfill_metrics.snapshot();
    let engines = engine_status.snapshot();
    let body = exporter
        .render(
            snapshot,
            &downloads,
            symbol_count,
            payload_counts,
            set_counts,
            classification,
            trade_ws,
            quote_backfill,
            &engines,
        )
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
    aggregation_slot_gauge: IntGaugeVec,
    download_streamed: IntGaugeVec,
    download_remaining: IntGaugeVec,
    symbol_count: IntGauge,
    payload_entries: IntGaugeVec,
    set_calls: IntGaugeVec,
    classification_total: IntGaugeVec,
    trade_ws_events: IntGauge,
    trade_ws_quote_events: IntGauge,
    trade_ws_option_trade_events: IntGauge,
    trade_ws_option_quote_events: IntGauge,
    trade_ws_underlying_trade_events: IntGauge,
    trade_ws_underlying_quote_events: IntGauge,
    trade_ws_cancel_events: IntGauge,
    trade_ws_errors: IntGauge,
    trade_ws_skipped: IntGauge,
    trade_ws_subscriptions: IntGauge,
    trade_ws_refresh_age: IntGauge,
    quote_backfill_rest_requests: IntGauge,
    quote_backfill_rest_success: IntGauge,
    quote_backfill_rest_client_errors: IntGauge,
    quote_backfill_rest_server_errors: IntGauge,
    quote_backfill_rest_network_errors: IntGauge,
    quote_backfill_rest_inflight: IntGauge,
    quote_backfill_rest_latency_ms_avg: IntGauge,
    quote_backfill_rest_latency_ms_max: IntGauge,
    quote_backfill_windows_examined: IntGauge,
    quote_backfill_windows_filled: IntGauge,
    quote_backfill_windows_skipped: IntGauge,
    quote_backfill_windows_failed: IntGauge,
    quote_backfill_quotes_written: IntGauge,
    quote_backfill_backlog: IntGauge,
    engine_runtime: IntGaugeVec,
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
        let aggregation_slot_gauge = IntGaugeVec::new(
            Opts::new(
                "aggregation_windowspace_slots",
                "Aggregation slot state counts across the enrichment ledger",
            ),
            &["slot", "state"],
        )
        .expect("aggregation slot gauge");
        registry
            .register(Box::new(aggregation_slot_gauge.clone()))
            .expect("register aggregation slot gauge");
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
        let payload_entries = IntGaugeVec::new(
            Opts::new(
                "windowspace_payload_entries",
                "Counts of payload mapping entries per store",
            ),
            &["store"],
        )
        .expect("payload entry gauge");
        registry
            .register(Box::new(payload_entries.clone()))
            .expect("register payload entry gauge");
        let set_calls = IntGaugeVec::new(
            Opts::new(
                "windowspace_set_calls",
                "Total successful slot setter calls per slot kind",
            ),
            &["slot"],
        )
        .expect("set calls gauge");
        registry
            .register(Box::new(set_calls.clone()))
            .expect("register set calls gauge");
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
        let trade_ws_events = IntGauge::with_opts(Opts::new(
            "trade_ws_events_total",
            "Total websocket trade events observed",
        ))
        .expect("trade ws events");
        registry
            .register(Box::new(trade_ws_events.clone()))
            .expect("register trade ws events");
        let trade_ws_quote_events = IntGauge::with_opts(Opts::new(
            "trade_ws_quote_events_total",
            "Total websocket quote events observed",
        ))
        .expect("trade ws quote events");
        registry
            .register(Box::new(trade_ws_quote_events.clone()))
            .expect("register trade ws quote events");
        let trade_ws_option_trade_events = IntGauge::with_opts(Opts::new(
            "trade_ws_option_trade_events_total",
            "Total option trade websocket events observed",
        ))
        .expect("trade ws option trade events");
        registry
            .register(Box::new(trade_ws_option_trade_events.clone()))
            .expect("register trade ws option trade events");
        let trade_ws_option_quote_events = IntGauge::with_opts(Opts::new(
            "trade_ws_option_quote_events_total",
            "Total option quote websocket events observed",
        ))
        .expect("trade ws option quote events");
        registry
            .register(Box::new(trade_ws_option_quote_events.clone()))
            .expect("register trade ws option quote events");
        let trade_ws_underlying_trade_events = IntGauge::with_opts(Opts::new(
            "trade_ws_underlying_trade_events_total",
            "Total underlying trade websocket events observed",
        ))
        .expect("trade ws underlying trade events");
        registry
            .register(Box::new(trade_ws_underlying_trade_events.clone()))
            .expect("register trade ws underlying trade events");
        let trade_ws_underlying_quote_events = IntGauge::with_opts(Opts::new(
            "trade_ws_underlying_quote_events_total",
            "Total underlying quote websocket events observed",
        ))
        .expect("trade ws underlying quote events");
        registry
            .register(Box::new(trade_ws_underlying_quote_events.clone()))
            .expect("register trade ws underlying quote events");
        let trade_ws_cancel_events = IntGauge::with_opts(Opts::new(
            "trade_ws_cancellations_total",
            "Total websocket cancellations observed",
        ))
        .expect("trade ws cancel events");
        registry
            .register(Box::new(trade_ws_cancel_events.clone()))
            .expect("register trade ws cancel events");
        let trade_ws_errors = IntGauge::with_opts(Opts::new(
            "trade_ws_errors_total",
            "Total websocket ingestion errors observed",
        ))
        .expect("trade ws errors");
        registry
            .register(Box::new(trade_ws_errors.clone()))
            .expect("register trade ws errors");
        let trade_ws_skipped = IntGauge::with_opts(Opts::new(
            "trade_ws_skipped_events_total",
            "Total websocket events skipped before ingestion",
        ))
        .expect("trade ws skipped");
        registry
            .register(Box::new(trade_ws_skipped.clone()))
            .expect("register trade ws skipped");
        let trade_ws_subscriptions = IntGauge::with_opts(Opts::new(
            "trade_ws_subscribed_contracts",
            "Number of option contracts currently subscribed via websocket",
        ))
        .expect("trade ws subscriptions");
        registry
            .register(Box::new(trade_ws_subscriptions.clone()))
            .expect("register trade ws subscribed");
        let trade_ws_refresh_age = IntGauge::with_opts(Opts::new(
            "trade_ws_seconds_since_contract_refresh",
            "Seconds since the last most-active contract snapshot pull",
        ))
        .expect("trade ws refresh age");
        registry
            .register(Box::new(trade_ws_refresh_age.clone()))
            .expect("register trade ws refresh age");
        let quote_backfill_rest_requests = IntGauge::with_opts(Opts::new(
            "quote_backfill_rest_requests_total",
            "Total REST requests issued by the quote backfill engine",
        ))
        .expect("quote backfill rest requests");
        registry
            .register(Box::new(quote_backfill_rest_requests.clone()))
            .expect("register quote backfill rest requests");
        let quote_backfill_rest_success = IntGauge::with_opts(Opts::new(
            "quote_backfill_rest_success_total",
            "Successful REST responses",
        ))
        .expect("quote backfill rest success");
        registry
            .register(Box::new(quote_backfill_rest_success.clone()))
            .expect("register quote backfill rest success");
        let quote_backfill_rest_client_errors = IntGauge::with_opts(Opts::new(
            "quote_backfill_rest_client_errors_total",
            "Client-side REST failures (4xx)",
        ))
        .expect("quote backfill client errors");
        registry
            .register(Box::new(quote_backfill_rest_client_errors.clone()))
            .expect("register quote backfill client errors");
        let quote_backfill_rest_server_errors = IntGauge::with_opts(Opts::new(
            "quote_backfill_rest_server_errors_total",
            "Server-side REST failures (5xx)",
        ))
        .expect("quote backfill server errors");
        registry
            .register(Box::new(quote_backfill_rest_server_errors.clone()))
            .expect("register quote backfill server errors");
        let quote_backfill_rest_network_errors = IntGauge::with_opts(Opts::new(
            "quote_backfill_rest_network_errors_total",
            "Errors before a REST response completed (network/json)",
        ))
        .expect("quote backfill network errors");
        registry
            .register(Box::new(quote_backfill_rest_network_errors.clone()))
            .expect("register quote backfill network errors");
        let quote_backfill_rest_inflight = IntGauge::with_opts(Opts::new(
            "quote_backfill_rest_inflight",
            "Currently in-flight REST requests",
        ))
        .expect("quote backfill inflight");
        registry
            .register(Box::new(quote_backfill_rest_inflight.clone()))
            .expect("register quote backfill inflight");
        let quote_backfill_rest_latency_ms_avg = IntGauge::with_opts(Opts::new(
            "quote_backfill_rest_latency_ms_avg",
            "Average REST latency (ms)",
        ))
        .expect("quote backfill latency avg");
        registry
            .register(Box::new(quote_backfill_rest_latency_ms_avg.clone()))
            .expect("register quote backfill latency avg");
        let quote_backfill_rest_latency_ms_max = IntGauge::with_opts(Opts::new(
            "quote_backfill_rest_latency_ms_max",
            "Maximum observed REST latency (ms)",
        ))
        .expect("quote backfill latency max");
        registry
            .register(Box::new(quote_backfill_rest_latency_ms_max.clone()))
            .expect("register quote backfill latency max");
        let quote_backfill_windows_examined = IntGauge::with_opts(Opts::new(
            "quote_backfill_windows_examined_total",
            "Windows the backfill engine attempted this session",
        ))
        .expect("quote backfill windows examined");
        registry
            .register(Box::new(quote_backfill_windows_examined.clone()))
            .expect("register quote backfill windows examined");
        let quote_backfill_windows_filled = IntGauge::with_opts(Opts::new(
            "quote_backfill_windows_filled_total",
            "Windows successfully backfilled",
        ))
        .expect("quote backfill windows filled");
        registry
            .register(Box::new(quote_backfill_windows_filled.clone()))
            .expect("register quote backfill windows filled");
        let quote_backfill_windows_skipped = IntGauge::with_opts(Opts::new(
            "quote_backfill_windows_skipped_total",
            "Windows with no REST quotes available",
        ))
        .expect("quote backfill windows skipped");
        registry
            .register(Box::new(quote_backfill_windows_skipped.clone()))
            .expect("register quote backfill windows skipped");
        let quote_backfill_windows_failed = IntGauge::with_opts(Opts::new(
            "quote_backfill_windows_failed_total",
            "Windows that failed backfill due to errors",
        ))
        .expect("quote backfill windows failed");
        registry
            .register(Box::new(quote_backfill_windows_failed.clone()))
            .expect("register quote backfill windows failed");
        let quote_backfill_quotes_written = IntGauge::with_opts(Opts::new(
            "quote_backfill_quotes_written_total",
            "Quotes written via REST backfill",
        ))
        .expect("quote backfill quotes written");
        registry
            .register(Box::new(quote_backfill_quotes_written.clone()))
            .expect("register quote backfill quotes written");
        let quote_backfill_backlog = IntGauge::with_opts(Opts::new(
            "quote_backfill_backlog_windows",
            "Backlog windows in the latest poll cycle",
        ))
        .expect("quote backfill backlog");
        registry
            .register(Box::new(quote_backfill_backlog.clone()))
            .expect("register quote backfill backlog");
        let engine_runtime = IntGaugeVec::new(
            Opts::new(
                "engine_runtime_seconds",
                "Seconds since each engine last successfully started (0 if stopped)",
            ),
            &["engine"],
        )
        .expect("engine runtime gauge");
        registry
            .register(Box::new(engine_runtime.clone()))
            .expect("register engine runtime");
        Self {
            registry,
            slot_gauge,
            window_gauge,
            aggregation_slot_gauge,
            download_streamed,
            download_remaining,
            symbol_count,
            payload_entries,
            set_calls,
            classification_total,
            trade_ws_events,
            trade_ws_quote_events,
            trade_ws_option_trade_events,
            trade_ws_option_quote_events,
            trade_ws_underlying_trade_events,
            trade_ws_underlying_quote_events,
            trade_ws_cancel_events,
            trade_ws_errors,
            trade_ws_skipped,
            trade_ws_subscriptions,
            trade_ws_refresh_age,
            quote_backfill_rest_requests,
            quote_backfill_rest_success,
            quote_backfill_rest_client_errors,
            quote_backfill_rest_server_errors,
            quote_backfill_rest_network_errors,
            quote_backfill_rest_inflight,
            quote_backfill_rest_latency_ms_avg,
            quote_backfill_rest_latency_ms_max,
            quote_backfill_windows_examined,
            quote_backfill_windows_filled,
            quote_backfill_windows_skipped,
            quote_backfill_windows_failed,
            quote_backfill_quotes_written,
            quote_backfill_backlog,
            engine_runtime,
        }
    }

    fn render(
        &self,
        snapshot: SlotMetricsSnapshot,
        downloads: &[DownloadSnapshot],
        symbols: usize,
        payload_counts: PayloadStoreCounts,
        set_counts: SetCounterSnapshot,
        classifications: AggressorMetricsSnapshot,
        trade_ws: TradeWsMetricsSnapshot,
        quote_backfill: QuoteBackfillMetricsSnapshot,
        engine_status: &[EngineStatusSample],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        self.window_gauge
            .with_label_values(&["trade"])
            .set(snapshot.trade_windows as i64);
        self.window_gauge
            .with_label_values(&["enrichment"])
            .set(snapshot.enrichment_windows as i64);
        self.record_slots(&snapshot.trade_slots);
        self.record_slots(&snapshot.enrichment_slots);
        self.record_aggregation_slots(&snapshot.enrichment_slots);
        self.record_downloads(downloads);
        self.symbol_count.set(symbols as i64);
        self.record_payload_counts(payload_counts);
        self.record_set_counts(set_counts);
        self.record_classifications(classifications);
        self.record_trade_ws(trade_ws);
        self.record_quote_backfill(quote_backfill);
        self.record_engine_status(engine_status);
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
                .with_label_values(&[kind_label, slot_label, "retire"])
                .set(slot.counts.retire as i64);
            self.slot_gauge
                .with_label_values(&[kind_label, slot_label, "retired"])
                .set(slot.counts.retired as i64);
        }
    }

    fn record_aggregation_slots(&self, slots: &[SlotCountsSnapshot]) {
        for slot in slots {
            if let SlotKind::Enrichment(EnrichmentSlotKind::Aggregation) = slot.slot {
                self.aggregation_slot_gauge
                    .with_label_values(&["aggregation", "empty"])
                    .set(slot.counts.empty as i64);
                self.aggregation_slot_gauge
                    .with_label_values(&["aggregation", "pending"])
                    .set(slot.counts.pending as i64);
                self.aggregation_slot_gauge
                    .with_label_values(&["aggregation", "filled"])
                    .set(slot.counts.filled as i64);
                self.aggregation_slot_gauge
                    .with_label_values(&["aggregation", "cleared"])
                    .set(slot.counts.cleared as i64);
                self.aggregation_slot_gauge
                    .with_label_values(&["aggregation", "retire"])
                    .set(slot.counts.retire as i64);
                self.aggregation_slot_gauge
                    .with_label_values(&["aggregation", "retired"])
                    .set(slot.counts.retired as i64);
            }
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

    fn record_trade_ws(&self, snapshot: TradeWsMetricsSnapshot) {
        self.trade_ws_events.set(snapshot.trade_events as i64);
        self.trade_ws_quote_events.set(snapshot.quote_events as i64);
        self.trade_ws_option_trade_events
            .set(snapshot.option_trade_events as i64);
        self.trade_ws_option_quote_events
            .set(snapshot.option_quote_events as i64);
        self.trade_ws_underlying_trade_events
            .set(snapshot.underlying_trade_events as i64);
        self.trade_ws_underlying_quote_events
            .set(snapshot.underlying_quote_events as i64);
        self.trade_ws_cancel_events
            .set(snapshot.cancel_events as i64);
        self.trade_ws_errors.set(snapshot.error_events as i64);
        self.trade_ws_skipped.set(snapshot.skipped_events as i64);
        self.trade_ws_subscriptions
            .set(snapshot.subscribed_contracts as i64);
        let age = snapshot
            .seconds_since_last_download
            .map(|secs| secs as i64)
            .unwrap_or(-1);
        self.trade_ws_refresh_age.set(age);
    }

    fn record_quote_backfill(&self, snapshot: QuoteBackfillMetricsSnapshot) {
        self.quote_backfill_rest_requests
            .set(snapshot.rest_requests as i64);
        self.quote_backfill_rest_success
            .set(snapshot.rest_success as i64);
        self.quote_backfill_rest_client_errors
            .set(snapshot.rest_client_error as i64);
        self.quote_backfill_rest_server_errors
            .set(snapshot.rest_server_error as i64);
        self.quote_backfill_rest_network_errors
            .set(snapshot.rest_network_error as i64);
        self.quote_backfill_rest_inflight
            .set(snapshot.rest_inflight);
        self.quote_backfill_rest_latency_ms_avg
            .set(snapshot.rest_latency_ms_avg.round() as i64);
        self.quote_backfill_rest_latency_ms_max
            .set(snapshot.rest_latency_ms_max.round() as i64);
        self.quote_backfill_windows_examined
            .set(snapshot.windows_examined as i64);
        self.quote_backfill_windows_filled
            .set(snapshot.windows_filled as i64);
        self.quote_backfill_windows_skipped
            .set(snapshot.windows_skipped as i64);
        self.quote_backfill_windows_failed
            .set(snapshot.windows_failed as i64);
        self.quote_backfill_quotes_written
            .set(snapshot.quotes_written as i64);
        self.quote_backfill_backlog
            .set(snapshot.backlog_windows as i64);
    }

    fn record_engine_status(&self, statuses: &[EngineStatusSample]) {
        self.engine_runtime.reset();
        for sample in statuses {
            self.engine_runtime
                .with_label_values(&[sample.name.as_str()])
                .set(sample.seconds_since_start);
        }
    }

    fn record_payload_counts(&self, counts: PayloadStoreCounts) {
        self.payload_entries
            .with_label_values(&["rf_rate"])
            .set(counts.rf_rate as i64);
        self.payload_entries
            .with_label_values(&["trades"])
            .set(counts.trades as i64);
        self.payload_entries
            .with_label_values(&["quotes"])
            .set(counts.quotes as i64);
        self.payload_entries
            .with_label_values(&["aggressor"])
            .set(counts.aggressor as i64);
        self.payload_entries
            .with_label_values(&["greeks"])
            .set(counts.greeks as i64);
        self.payload_entries
            .with_label_values(&["aggregations"])
            .set(counts.aggregations as i64);
    }

    fn record_set_counts(&self, counts: SetCounterSnapshot) {
        self.set_calls
            .with_label_values(&["rf_rate"])
            .set(counts.rf_rate as i64);
        self.set_calls
            .with_label_values(&["option_trade"])
            .set(counts.option_trade as i64);
        self.set_calls
            .with_label_values(&["option_quote"])
            .set(counts.option_quote as i64);
        self.set_calls
            .with_label_values(&["underlying_trade"])
            .set(counts.underlying_trade as i64);
        self.set_calls
            .with_label_values(&["underlying_quote"])
            .set(counts.underlying_quote as i64);
        self.set_calls
            .with_label_values(&["option_aggressor"])
            .set(counts.option_aggressor as i64);
        self.set_calls
            .with_label_values(&["underlying_aggressor"])
            .set(counts.underlying_aggressor as i64);
        self.set_calls
            .with_label_values(&["greeks"])
            .set(counts.greeks as i64);
        self.set_calls
            .with_label_values(&["aggregation"])
            .set(counts.aggregation as i64);
    }
}
