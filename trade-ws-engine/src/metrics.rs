use std::{
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Default)]
struct TradeWsMetricsInner {
    trade_events: AtomicU64,
    quote_events: AtomicU64,
    skipped_events: AtomicU64,
    error_events: AtomicU64,
    subscribed_contracts: AtomicU64,
    last_contract_refresh: AtomicI64,
}

#[derive(Clone, Default)]
pub struct TradeWsMetrics {
    inner: Arc<TradeWsMetricsInner>,
}

pub struct TradeWsMetricsSnapshot {
    pub trade_events: u64,
    pub quote_events: u64,
    pub skipped_events: u64,
    pub error_events: u64,
    pub subscribed_contracts: u64,
    pub seconds_since_last_download: Option<u64>,
}

impl TradeWsMetrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(TradeWsMetricsInner {
                trade_events: AtomicU64::new(0),
                quote_events: AtomicU64::new(0),
                skipped_events: AtomicU64::new(0),
                error_events: AtomicU64::new(0),
                subscribed_contracts: AtomicU64::new(0),
                last_contract_refresh: AtomicI64::new(-1),
            }),
        }
    }

    pub fn inc_trade_events(&self, delta: u64) {
        if delta > 0 {
            self.inner.trade_events.fetch_add(delta, Ordering::Relaxed);
        }
    }

    pub fn inc_quote_events(&self, delta: u64) {
        if delta > 0 {
            self.inner.quote_events.fetch_add(delta, Ordering::Relaxed);
        }
    }

    pub fn inc_skipped(&self) {
        self.inner.skipped_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_errors(&self) {
        self.inner.error_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_subscribed_contracts(&self, count: usize) {
        self.inner
            .subscribed_contracts
            .store(count as u64, Ordering::Relaxed);
    }

    pub fn mark_contract_refresh(&self) {
        if let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) {
            self.inner
                .last_contract_refresh
                .store(duration.as_secs() as i64, Ordering::Relaxed);
        }
    }

    pub fn snapshot(&self) -> TradeWsMetricsSnapshot {
        let trade_events = self.inner.trade_events.load(Ordering::Relaxed);
        let quote_events = self.inner.quote_events.load(Ordering::Relaxed);
        let skipped_events = self.inner.skipped_events.load(Ordering::Relaxed);
        let error_events = self.inner.error_events.load(Ordering::Relaxed);
        let subscribed_contracts = self.inner.subscribed_contracts.load(Ordering::Relaxed);
        let last = self.inner.last_contract_refresh.load(Ordering::Relaxed);
        let seconds_since_last_download = if last <= 0 {
            None
        } else if let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) {
            let now = duration.as_secs() as i64;
            if now >= last {
                Some((now - last) as u64)
            } else {
                None
            }
        } else {
            None
        };
        TradeWsMetricsSnapshot {
            trade_events,
            quote_events,
            skipped_events,
            error_events,
            subscribed_contracts,
            seconds_since_last_download,
        }
    }
}
