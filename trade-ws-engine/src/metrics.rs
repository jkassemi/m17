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
    option_trade_events: AtomicU64,
    option_quote_events: AtomicU64,
    underlying_trade_events: AtomicU64,
    underlying_quote_events: AtomicU64,
    skipped_events: AtomicU64,
    error_events: AtomicU64,
    cancel_events: AtomicU64,
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
    pub option_trade_events: u64,
    pub option_quote_events: u64,
    pub underlying_trade_events: u64,
    pub underlying_quote_events: u64,
    pub skipped_events: u64,
    pub error_events: u64,
    pub cancel_events: u64,
    pub subscribed_contracts: u64,
    pub seconds_since_last_download: Option<u64>,
}

impl TradeWsMetrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(TradeWsMetricsInner {
                trade_events: AtomicU64::new(0),
                quote_events: AtomicU64::new(0),
                option_trade_events: AtomicU64::new(0),
                option_quote_events: AtomicU64::new(0),
                underlying_trade_events: AtomicU64::new(0),
                underlying_quote_events: AtomicU64::new(0),
                skipped_events: AtomicU64::new(0),
                error_events: AtomicU64::new(0),
                cancel_events: AtomicU64::new(0),
                subscribed_contracts: AtomicU64::new(0),
                last_contract_refresh: AtomicI64::new(-1),
            }),
        }
    }

    pub fn inc_trade_events(&self, delta: u64) {
        self.add_trade_events(delta);
    }

    pub fn inc_quote_events(&self, delta: u64) {
        self.add_quote_events(delta);
    }

    pub fn inc_option_trade_events(&self, delta: u64) {
        if delta > 0 {
            self.inner
                .option_trade_events
                .fetch_add(delta, Ordering::Relaxed);
            self.add_trade_events(delta);
        }
    }

    pub fn inc_option_quote_events(&self, delta: u64) {
        if delta > 0 {
            self.inner
                .option_quote_events
                .fetch_add(delta, Ordering::Relaxed);
            self.add_quote_events(delta);
        }
    }

    pub fn inc_underlying_trade_events(&self, delta: u64) {
        if delta > 0 {
            self.inner
                .underlying_trade_events
                .fetch_add(delta, Ordering::Relaxed);
            self.add_trade_events(delta);
        }
    }

    pub fn inc_underlying_quote_events(&self, delta: u64) {
        if delta > 0 {
            self.inner
                .underlying_quote_events
                .fetch_add(delta, Ordering::Relaxed);
            self.add_quote_events(delta);
        }
    }

    pub fn inc_skipped(&self) {
        self.inner.skipped_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_errors(&self) {
        self.inner.error_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_cancellations(&self) {
        self.inner.cancel_events.fetch_add(1, Ordering::Relaxed);
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
        let option_trade_events = self.inner.option_trade_events.load(Ordering::Relaxed);
        let option_quote_events = self.inner.option_quote_events.load(Ordering::Relaxed);
        let underlying_trade_events = self.inner.underlying_trade_events.load(Ordering::Relaxed);
        let underlying_quote_events = self.inner.underlying_quote_events.load(Ordering::Relaxed);
        let skipped_events = self.inner.skipped_events.load(Ordering::Relaxed);
        let error_events = self.inner.error_events.load(Ordering::Relaxed);
        let cancel_events = self.inner.cancel_events.load(Ordering::Relaxed);
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
            option_trade_events,
            option_quote_events,
            underlying_trade_events,
            underlying_quote_events,
            skipped_events,
            error_events,
            cancel_events,
            subscribed_contracts,
            seconds_since_last_download,
        }
    }

    fn add_trade_events(&self, delta: u64) {
        if delta > 0 {
            self.inner.trade_events.fetch_add(delta, Ordering::Relaxed);
        }
    }

    fn add_quote_events(&self, delta: u64) {
        if delta > 0 {
            self.inner.quote_events.fetch_add(delta, Ordering::Relaxed);
        }
    }
}
