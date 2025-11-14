use std::{
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

#[derive(Default)]
struct QuoteBackfillMetricsInner {
    backlog_windows: AtomicU64,
    windows_examined: AtomicU64,
    windows_filled: AtomicU64,
    windows_skipped: AtomicU64,
    windows_failed: AtomicU64,
    quotes_written: AtomicU64,
    rest_requests: AtomicU64,
    rest_success: AtomicU64,
    rest_client_error: AtomicU64,
    rest_server_error: AtomicU64,
    rest_network_error: AtomicU64,
    rest_latency_total_ns: AtomicU64,
    rest_latency_samples: AtomicU64,
    rest_latency_max_ns: AtomicU64,
    rest_inflight: AtomicI64,
}

#[derive(Clone, Default)]
pub struct QuoteBackfillMetrics {
    inner: Arc<QuoteBackfillMetricsInner>,
}

pub struct QuoteBackfillMetricsSnapshot {
    pub backlog_windows: u64,
    pub windows_examined: u64,
    pub windows_filled: u64,
    pub windows_skipped: u64,
    pub windows_failed: u64,
    pub quotes_written: u64,
    pub rest_requests: u64,
    pub rest_success: u64,
    pub rest_client_error: u64,
    pub rest_server_error: u64,
    pub rest_network_error: u64,
    pub rest_inflight: i64,
    pub rest_latency_ms_avg: f64,
    pub rest_latency_ms_max: f64,
}

impl QuoteBackfillMetrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(QuoteBackfillMetricsInner::default()),
        }
    }

    pub fn record_backlog(&self, backlog: usize) {
        self.inner
            .backlog_windows
            .store(backlog as u64, Ordering::Relaxed);
    }

    pub fn record_windows_examined(&self, count: usize) {
        if count > 0 {
            self.inner
                .windows_examined
                .fetch_add(count as u64, Ordering::Relaxed);
        }
    }

    pub fn record_window_filled(&self, quotes_written: usize) {
        self.inner.windows_filled.fetch_add(1, Ordering::Relaxed);
        if quotes_written > 0 {
            self.inner
                .quotes_written
                .fetch_add(quotes_written as u64, Ordering::Relaxed);
        }
    }

    pub fn record_window_skipped(&self) {
        self.inner.windows_skipped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_window_failed(&self) {
        self.inner.windows_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_rest_attempt(&self) {
        self.inner.rest_requests.fetch_add(1, Ordering::Relaxed);
        self.inner.rest_inflight.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_rest_success(&self, _status: u16, elapsed: Duration) {
        self.inner.rest_success.fetch_add(1, Ordering::Relaxed);
        self.observe_latency(elapsed);
        self.finish_inflight();
    }

    pub fn record_rest_client_error(&self, _status: u16, elapsed: Duration) {
        self.inner.rest_client_error.fetch_add(1, Ordering::Relaxed);
        self.observe_latency(elapsed);
        self.finish_inflight();
    }

    pub fn record_rest_server_error(&self, _status: u16, elapsed: Duration) {
        self.inner.rest_server_error.fetch_add(1, Ordering::Relaxed);
        self.observe_latency(elapsed);
        self.finish_inflight();
    }

    pub fn record_rest_network_error(&self, elapsed: Duration) {
        self.inner
            .rest_network_error
            .fetch_add(1, Ordering::Relaxed);
        self.observe_latency(elapsed);
        self.finish_inflight();
    }

    pub fn snapshot(&self) -> QuoteBackfillMetricsSnapshot {
        let total_ns = self.inner.rest_latency_total_ns.load(Ordering::Relaxed);
        let samples = self.inner.rest_latency_samples.load(Ordering::Relaxed);
        let max_ns = self.inner.rest_latency_max_ns.load(Ordering::Relaxed);
        let avg_ms = if samples > 0 {
            (total_ns as f64 / samples as f64) / 1_000_000.0
        } else {
            0.0
        };
        let max_ms = if max_ns > 0 {
            max_ns as f64 / 1_000_000.0
        } else {
            0.0
        };
        QuoteBackfillMetricsSnapshot {
            backlog_windows: self.inner.backlog_windows.load(Ordering::Relaxed),
            windows_examined: self.inner.windows_examined.load(Ordering::Relaxed),
            windows_filled: self.inner.windows_filled.load(Ordering::Relaxed),
            windows_skipped: self.inner.windows_skipped.load(Ordering::Relaxed),
            windows_failed: self.inner.windows_failed.load(Ordering::Relaxed),
            quotes_written: self.inner.quotes_written.load(Ordering::Relaxed),
            rest_requests: self.inner.rest_requests.load(Ordering::Relaxed),
            rest_success: self.inner.rest_success.load(Ordering::Relaxed),
            rest_client_error: self.inner.rest_client_error.load(Ordering::Relaxed),
            rest_server_error: self.inner.rest_server_error.load(Ordering::Relaxed),
            rest_network_error: self.inner.rest_network_error.load(Ordering::Relaxed),
            rest_inflight: self.inner.rest_inflight.load(Ordering::Relaxed),
            rest_latency_ms_avg: avg_ms,
            rest_latency_ms_max: max_ms,
        }
    }

    fn observe_latency(&self, elapsed: Duration) {
        let nanos = elapsed.as_nanos().min(u64::MAX as u128) as u64;
        self.inner
            .rest_latency_total_ns
            .fetch_add(nanos, Ordering::Relaxed);
        self.inner
            .rest_latency_samples
            .fetch_add(1, Ordering::Relaxed);
        let mut current = self.inner.rest_latency_max_ns.load(Ordering::Relaxed);
        while nanos > current {
            match self.inner.rest_latency_max_ns.compare_exchange(
                current,
                nanos,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }

    fn finish_inflight(&self) {
        self.inner.rest_inflight.fetch_sub(1, Ordering::Relaxed);
    }
}
