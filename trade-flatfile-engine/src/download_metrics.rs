use std::{
    array,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use window_space::payload::TradeSlotKind;

#[derive(Clone, Debug)]
pub struct DownloadMetrics {
    inner: Arc<DownloadMetricsInner>,
}

#[derive(Debug)]
struct DownloadMetricsInner {
    entries: [DownloadEntry; TradeSlotKind::ALL.len()],
}

#[derive(Default, Debug)]
struct DownloadEntry {
    total_bytes: AtomicU64,
    streamed_bytes: AtomicU64,
    total_known: AtomicBool,
}

#[derive(Clone, Copy, Debug)]
pub struct DownloadSnapshot {
    pub slot: TradeSlotKind,
    pub total_bytes: Option<u64>,
    pub streamed_bytes: u64,
}

impl DownloadMetrics {
    pub fn new() -> Self {
        let inner = DownloadMetricsInner {
            entries: array::from_fn(|_| DownloadEntry::default()),
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn reset(&self, slot: TradeSlotKind, total_bytes: Option<u64>) {
        let entry = self.entry(slot);
        match total_bytes {
            Some(value) => {
                entry.total_bytes.store(value, Ordering::Relaxed);
                entry.total_known.store(true, Ordering::Relaxed);
            }
            None => {
                entry.total_bytes.store(0, Ordering::Relaxed);
                entry.total_known.store(false, Ordering::Relaxed);
            }
        }
        entry.streamed_bytes.store(0, Ordering::Relaxed);
    }

    pub fn add_streamed(&self, slot: TradeSlotKind, delta: u64) {
        if delta == 0 {
            return;
        }
        let entry = self.entry(slot);
        entry.streamed_bytes.fetch_add(delta, Ordering::Relaxed);
    }

    pub fn complete(&self, slot: TradeSlotKind) {
        let entry = self.entry(slot);
        entry.total_bytes.store(0, Ordering::Relaxed);
        entry.streamed_bytes.store(0, Ordering::Relaxed);
        entry.total_known.store(false, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> Vec<DownloadSnapshot> {
        TradeSlotKind::ALL
            .iter()
            .map(|kind| {
                let entry = self.entry(*kind);
                let streamed = entry.streamed_bytes.load(Ordering::Relaxed);
                let total = if entry.total_known.load(Ordering::Relaxed) {
                    Some(entry.total_bytes.load(Ordering::Relaxed))
                } else {
                    None
                };
                DownloadSnapshot {
                    slot: *kind,
                    total_bytes: total,
                    streamed_bytes: streamed,
                }
            })
            .collect()
    }

    fn entry(&self, slot: TradeSlotKind) -> &DownloadEntry {
        &self.inner.entries[slot.index()]
    }
}
