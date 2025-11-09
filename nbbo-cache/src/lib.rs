// Copyright (c) James Kassemi, SC, US. All rights reserved.

use std::collections::HashMap;
use core_types::{Nbbo, NbboState, StalenessParams};

/// Stub for in-memory NBBO store with per-instrument ring buffers.
/// Tracks quote states (Normal/Locked/Crossed) and adaptive staleness.
pub struct NbboStore {
    // Placeholder: per-instrument storage (e.g., ring buffers for quotes)
    pub data: HashMap<String, Vec<Nbbo>>,
}

impl NbboStore {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Put a new NBBO quote into the store.
    pub fn put(&mut self, quote: &Nbbo) {
        // Stub: Insert into per-instrument buffer (no-op for now)
        self.data.entry(quote.instrument_id.clone()).or_insert_with(Vec::new).push(quote.clone());
    }

    /// Get the best NBBO before the given timestamp within max staleness.
    pub fn get_best_before(&self, id: &str, ts_ns: i64, max_staleness_us: u32) -> Option<Nbbo> {
        // Stub: Return the most recent quote if within staleness (no-op for now)
        self.data.get(id)?.last().cloned()
    }

    /// Get the NBBO state before the given timestamp.
    pub fn get_state_before(&self, id: &str, ts_ns: i64) -> Option<NbboState> {
        // Stub: Return state from the most recent quote (no-op for now)
        self.data.get(id)?.last().map(|q| q.state)
    }

    /// Get adaptive staleness parameters for an instrument.
    pub fn adaptive_params(&self, id: &str) -> StalenessParams {
        // Stub: Return default params (no adaptive logic yet)
        StalenessParams {
            max_staleness_us: 100_000, // 100ms default
        }
    }
}
