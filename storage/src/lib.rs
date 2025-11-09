// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Parquet writer/reader (stub).

use core_types::{EquityTrade, Nbbo, OptionTrade};

/// Stub storage.
pub struct Storage;

impl Storage {
    pub fn new() -> Self {
        Self
    }

    /// Stub: Write batch.
    pub async fn write_option_trades(&self, _batch: Vec<OptionTrade>) {
        // No-op
    }
}
