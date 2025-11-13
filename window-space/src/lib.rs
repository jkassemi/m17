//! Window space controller library for the greenfield rewrite.
//!
//! WindowSpace defines the temporal grid; [`Window`] handles expose per-window context while
//! storage remains pluggable (memory-mapped files today).
//!
//! The crate exposes:
//! - [`WindowSpace`]: helper for precomputing trading windows and iterating over [`Window`] handles.
//! - [`WindowSpaceController`]: high-level API orchestrating symbol resolution and slot mutations.
//! - [`TradeWindowSpace`] / [`EnrichmentWindowSpace`]: dense per-window stores that back each
//!   window.

pub mod config;
pub mod controller;
pub mod error;
pub mod ledger;
pub mod mapping;
pub mod payload;
pub mod slot_metrics;
mod storage;
pub mod symbol_map;
pub mod window;

#[allow(deprecated)]
pub use config::LedgerConfig;
pub use config::WindowSpaceConfig;
#[allow(deprecated)]
pub use controller::{LedgerController, LedgerSlotStatusSnapshot, LedgerStorageReport};
pub use controller::{
    PendingHandle, SlotStatusCounts, StorageSummary, WindowSpaceController,
    WindowSpaceSlotStatusSnapshot, WindowSpaceStorageReport,
};
#[allow(deprecated)]
pub use error::LedgerError;
pub use error::{ControllerError, SlotWriteError, WindowSpaceError};
#[allow(deprecated)]
pub use ledger::{EnrichmentLedger, TradeLedger};
pub use ledger::{EnrichmentWindowSpace, TradeWindowSpace};
pub use payload::{PayloadMeta, PayloadType, SlotKind, SlotStatus};
pub use symbol_map::{SymbolId, SymbolMap};
pub use window::{
    DEFAULT_WINDOW_DURATION_SECS, WINDOWS_PER_SESSION, Window, WindowIndex, WindowMeta,
    WindowRangeConfig, WindowSpace, WindowSpaceBuilder,
};
