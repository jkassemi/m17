// Copyright (c) James Kassemi, SC, US. All rights reserved.

use core_types::types::{Nbbo, NbboState, StalenessParams};
use std::collections::HashMap;

fn age_us(trade_ts_ns: i64, quote_ts_ns: i64) -> u32 {
    let diff = trade_ts_ns.saturating_sub(quote_ts_ns);
    (diff / 1_000) as u32
}

/// Stub for in-memory NBBO store with per-instrument ring buffers.
/// Tracks quote states (Normal/Locked/Crossed) and adaptive staleness.
pub struct NbboStore {
    // Per-instrument sorted quotes by quote_ts_ns
    pub data: HashMap<String, Vec<Nbbo>>,
}

impl NbboStore {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Put a new NBBO quote into the store. Assumes quotes arrive roughly sorted by time.
    pub fn put(&mut self, quote: &Nbbo) {
        let buf = self
            .data
            .entry(quote.instrument_id.clone())
            .or_insert_with(Vec::new);
        if buf
            .last()
            .map(|q| q.quote_ts_ns <= quote.quote_ts_ns)
            .unwrap_or(true)
        {
            buf.push(quote.clone());
        } else {
            // Fallback: insert keeping order (rare for out-of-order)
            let pos = buf
                .binary_search_by_key(&quote.quote_ts_ns, |q| q.quote_ts_ns)
                .unwrap_or_else(|e| e);
            buf.insert(pos, quote.clone());
        }
    }

    /// Get the NBBO before or at ts_ns, if within staleness in microseconds.
    pub fn get_best_before(&self, id: &str, ts_ns: i64, max_staleness_us: u32) -> Option<Nbbo> {
        let buf = self.data.get(id)?;
        // binary search last index with quote_ts_ns <= ts_ns
        let mut lo = 0i64;
        let mut hi = (buf.len() as i64) - 1;
        if hi < 0 {
            return None;
        }
        if buf[hi as usize].quote_ts_ns <= ts_ns {
            let q = &buf[hi as usize];
            let a = age_us(ts_ns, q.quote_ts_ns);
            return if a <= max_staleness_us {
                Some(q.clone())
            } else {
                None
            };
        }
        let mut res: Option<usize> = None;
        while lo <= hi {
            let mid = (lo + hi) / 2;
            let qts = buf[mid as usize].quote_ts_ns;
            if qts <= ts_ns {
                res = Some(mid as usize);
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        res.and_then(|idx| {
            let q = &buf[idx];
            let a = age_us(ts_ns, q.quote_ts_ns);
            if a <= max_staleness_us {
                Some(q.clone())
            } else {
                None
            }
        })
    }

    /// Get the NBBO state before the given timestamp.
    pub fn get_state_before(&self, id: &str, ts_ns: i64) -> Option<NbboState> {
        self.get_best_before(id, ts_ns, u32::MAX).map(|q| q.state)
    }

    /// Get adaptive staleness parameters for an instrument.
    pub fn adaptive_params(&self, _id: &str) -> StalenessParams {
        // Stub: Return default params (no adaptive logic yet)
        StalenessParams {
            max_staleness_us: 100_000, // 100ms default
        }
    }

    /// Drop quotes strictly before cutoff for all instruments.
    pub fn prune_before(&mut self, cutoff_ts_ns: i64) {
        for (_sym, buf) in self.data.iter_mut() {
            if buf.is_empty() {
                continue;
            }
            // find first index with ts >= cutoff
            let idx = match buf.binary_search_by_key(&cutoff_ts_ns, |q| q.quote_ts_ns) {
                Ok(i) => i,                  // exact match; keep from i
                Err(insert_pt) => insert_pt, // insert point of cutoff
            };
            if idx > 0 {
                buf.drain(0..idx);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::uid::quote_uid;

    fn mk_nbbo(sym: &str, ts_ns: i64, bid: f64, ask: f64) -> Nbbo {
        let quote_uid = quote_uid(sym, ts_ns, None, bid, ask, 1, 1, Some(12), Some(11), None);
        Nbbo {
            instrument_id: sym.to_string(),
            quote_uid,
            quote_ts_ns: ts_ns,
            bid,
            ask,
            bid_sz: 1,
            ask_sz: 1,
            state: NbboState::Normal,
            condition: None,
            best_bid_venue: Some(12),
            best_ask_venue: Some(11),
            source: core_types::types::Source::Flatfile,
            quality: core_types::types::Quality::Prelim,
            watermark_ts_ns: 0,
        }
    }

    #[test]
    fn test_get_best_before_within_staleness() {
        let mut store = NbboStore::new();
        let t0 = 1_000_000_000i64; // 1.0s
        let t1 = 1_400_000_000i64; // 1.4s
        store.put(&mk_nbbo("MSFT", t0, 275.0, 276.0));
        store.put(&mk_nbbo("MSFT", t1, 275.3, 276.1));
        let trade_ts = 1_500_000_000i64; // 1.5s
                                         // staleness 1s => 1_000_000 us; age is 100ms (100_000us), within window
        let q = store.get_best_before("MSFT", trade_ts, 1_000_000).unwrap();
        assert_eq!(q.quote_ts_ns, t1);
        assert!((q.bid - 275.3).abs() < 1e-9);
    }

    #[test]
    fn test_get_best_before_stale_returns_none() {
        let mut store = NbboStore::new();
        let t0 = 1_000_000_000i64; // 1.0s
        store.put(&mk_nbbo("MSFT", t0, 275.0, 276.0));
        let trade_ts = 3_500_000_000i64; // 3.5s
                                         // staleness 1s -> age 2.5s is too old
        assert!(store.get_best_before("MSFT", trade_ts, 1_000_000).is_none());
    }

    #[test]
    fn test_prune_before_drops_old() {
        let mut store = NbboStore::new();
        for i in 0..5 {
            store.put(&mk_nbbo(
                "MSFT",
                1_000_000_000 + i * 100_000_000,
                270.0 + i as f64,
                271.0,
            ));
        }
        // cutoff at 1.2s -> drop entries < 1.2s (first two)
        store.prune_before(1_200_000_000);
        let buf = store.data.get("MSFT").unwrap();
        assert_eq!(buf.len(), 3);
        assert_eq!(buf.first().unwrap().quote_ts_ns, 1_200_000_000);
    }
}
