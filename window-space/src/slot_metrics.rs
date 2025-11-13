use std::{
    array,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::payload::{EnrichmentSlotKind, SlotKind, SlotStatus, TradeSlotKind};

#[derive(Clone, Copy, Debug)]
pub enum StoreKind {
    Trade,
    Enrichment,
}

pub struct SlotMetrics {
    window_count: usize,
    trade_windows: AtomicU64,
    enrichment_windows: AtomicU64,
    trade_slots: [SlotStateCounter; TradeSlotKind::ALL.len()],
    enrichment_slots: [SlotStateCounter; EnrichmentSlotKind::ALL.len()],
}

impl SlotMetrics {
    pub fn new(window_count: usize) -> Arc<Self> {
        Arc::new(Self {
            window_count,
            trade_windows: AtomicU64::new(0),
            enrichment_windows: AtomicU64::new(0),
            trade_slots: array::from_fn(|_| SlotStateCounter::new()),
            enrichment_slots: array::from_fn(|_| SlotStateCounter::new()),
        })
    }

    pub fn window_count(&self) -> usize {
        self.window_count
    }

    pub fn record_symbol_bootstrap(&self, store: StoreKind, slots: &[SlotKind]) {
        let windows = self.window_count as u64;
        match store {
            StoreKind::Trade => {
                self.trade_windows.fetch_add(windows, Ordering::Relaxed);
            }
            StoreKind::Enrichment => {
                self.enrichment_windows
                    .fetch_add(windows, Ordering::Relaxed);
            }
        }
        for slot in slots {
            self.increment(*slot, SlotStatus::Empty, windows);
        }
    }

    pub fn record_transition(&self, slot: SlotKind, from: SlotStatus, to: SlotStatus) {
        if from == to {
            return;
        }
        self.decrement(slot, from, 1);
        self.increment(slot, to, 1);
    }

    pub fn snapshot(&self) -> SlotMetricsSnapshot {
        let trade_slots = TradeSlotKind::ALL
            .iter()
            .enumerate()
            .map(|(idx, kind)| {
                let counts = self.trade_slots[idx].snapshot();
                SlotCountsSnapshot {
                    slot: SlotKind::Trade(*kind),
                    counts,
                }
            })
            .collect();
        let enrichment_slots = EnrichmentSlotKind::ALL
            .iter()
            .enumerate()
            .map(|(idx, kind)| {
                let counts = self.enrichment_slots[idx].snapshot();
                SlotCountsSnapshot {
                    slot: SlotKind::Enrichment(*kind),
                    counts,
                }
            })
            .collect();
        SlotMetricsSnapshot {
            trade_windows: self.trade_windows.load(Ordering::Relaxed),
            enrichment_windows: self.enrichment_windows.load(Ordering::Relaxed),
            trade_slots,
            enrichment_slots,
        }
    }

    pub fn replace_counts(
        &self,
        trade_windows: u64,
        enrichment_windows: u64,
        trade_counts: &[SlotStateSnapshot],
        enrichment_counts: &[SlotStateSnapshot],
    ) {
        self.trade_windows.store(trade_windows, Ordering::Relaxed);
        self.enrichment_windows
            .store(enrichment_windows, Ordering::Relaxed);
        for (counter, snapshot) in self.trade_slots.iter().zip(trade_counts.iter()) {
            counter.set(snapshot);
        }
        for (counter, snapshot) in self.enrichment_slots.iter().zip(enrichment_counts.iter()) {
            counter.set(snapshot);
        }
    }

    fn increment(&self, slot: SlotKind, status: SlotStatus, delta: u64) {
        self.counters(slot).increment(status, delta);
    }

    fn decrement(&self, slot: SlotKind, status: SlotStatus, delta: u64) {
        self.counters(slot).decrement(status, delta);
    }

    fn counters(&self, slot: SlotKind) -> &SlotStateCounter {
        match slot {
            SlotKind::Trade(kind) => {
                let idx = kind.index();
                &self.trade_slots[idx]
            }
            SlotKind::Enrichment(kind) => {
                let idx = kind.index();
                &self.enrichment_slots[idx]
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct SlotMetricsSnapshot {
    pub trade_windows: u64,
    pub enrichment_windows: u64,
    pub trade_slots: Vec<SlotCountsSnapshot>,
    pub enrichment_slots: Vec<SlotCountsSnapshot>,
}

#[derive(Clone, Debug)]
pub struct SlotCountsSnapshot {
    pub slot: SlotKind,
    pub counts: SlotStateSnapshot,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SlotStateSnapshot {
    pub empty: u64,
    pub pending: u64,
    pub filled: u64,
    pub cleared: u64,
    pub retired: u64,
}

impl SlotStateSnapshot {
    pub fn increment(&mut self, status: SlotStatus, delta: u64) {
        match status {
            SlotStatus::Empty => self.empty += delta,
            SlotStatus::Pending => self.pending += delta,
            SlotStatus::Filled => self.filled += delta,
            SlotStatus::Cleared => self.cleared += delta,
            SlotStatus::Retired => self.retired += delta,
        }
    }
}

struct SlotStateCounter {
    empty: AtomicU64,
    pending: AtomicU64,
    filled: AtomicU64,
    cleared: AtomicU64,
    retired: AtomicU64,
}

impl SlotStateCounter {
    const fn new() -> Self {
        Self {
            empty: AtomicU64::new(0),
            pending: AtomicU64::new(0),
            filled: AtomicU64::new(0),
            cleared: AtomicU64::new(0),
            retired: AtomicU64::new(0),
        }
    }

    fn increment(&self, status: SlotStatus, delta: u64) {
        match status {
            SlotStatus::Empty => {
                self.empty.fetch_add(delta, Ordering::Relaxed);
            }
            SlotStatus::Pending => {
                self.pending.fetch_add(delta, Ordering::Relaxed);
            }
            SlotStatus::Filled => {
                self.filled.fetch_add(delta, Ordering::Relaxed);
            }
            SlotStatus::Cleared => {
                self.cleared.fetch_add(delta, Ordering::Relaxed);
            }
            SlotStatus::Retired => {
                self.retired.fetch_add(delta, Ordering::Relaxed);
            }
        }
    }

    fn decrement(&self, status: SlotStatus, delta: u64) {
        match status {
            SlotStatus::Empty => {
                self.empty.fetch_sub(delta, Ordering::Relaxed);
            }
            SlotStatus::Pending => {
                self.pending.fetch_sub(delta, Ordering::Relaxed);
            }
            SlotStatus::Filled => {
                self.filled.fetch_sub(delta, Ordering::Relaxed);
            }
            SlotStatus::Cleared => {
                self.cleared.fetch_sub(delta, Ordering::Relaxed);
            }
            SlotStatus::Retired => {
                self.retired.fetch_sub(delta, Ordering::Relaxed);
            }
        }
    }

    fn snapshot(&self) -> SlotStateSnapshot {
        SlotStateSnapshot {
            empty: self.empty.load(Ordering::Relaxed),
            pending: self.pending.load(Ordering::Relaxed),
            filled: self.filled.load(Ordering::Relaxed),
            cleared: self.cleared.load(Ordering::Relaxed),
            retired: self.retired.load(Ordering::Relaxed),
        }
    }

    fn set(&self, snapshot: &SlotStateSnapshot) {
        self.empty.store(snapshot.empty, Ordering::Relaxed);
        self.pending.store(snapshot.pending, Ordering::Relaxed);
        self.filled.store(snapshot.filled, Ordering::Relaxed);
        self.cleared.store(snapshot.cleared, Ordering::Relaxed);
        self.retired.store(snapshot.retired, Ordering::Relaxed);
    }
}
