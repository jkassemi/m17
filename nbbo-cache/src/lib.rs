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
        if buf.last().map(|q| q.quote_ts_ns <= quote.quote_ts_ns).unwrap_or(true) {
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
        if hi < 0 { return None; }
        if buf[hi as usize].quote_ts_ns <= ts_ns {
            let q = &buf[hi as usize];
            let a = age_us(ts_ns, q.quote_ts_ns);
            return if a <= max_staleness_us { Some(q.clone()) } else { None };
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
            if a <= max_staleness_us { Some(q.clone()) } else { None }
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
}
