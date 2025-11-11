// Copyright (c) James Kassemi, SC, US. All rights reserved.

use core_types::types::{ClassParams, TradeLike};
use nbbo_cache::NbboStore;

/// Stub for stateless aggressor classifier.
/// Handles deterministic classification per trade, locked/crossed policies, tick-size, and finalizer.
pub struct Classifier;

impl Classifier {
    pub fn new() -> Self {
        Self
    }

    /// Classify aggressor for a trade using NBBO store and params.
    pub fn classify_trade(
        &self,
        trade: &mut dyn TradeLike,
        nbbo: &NbboStore,
        params: &ClassParams,
    ) {
        let _ = (trade, nbbo, params);
        // Stub: No-op for now
    }
}
