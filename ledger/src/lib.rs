//! Ledger/controller library for the greenfield rewrite.
//!
//! The crate exposes:
//! - [`LedgerController`]: high-level API orchestrating symbol resolution and slot mutations.
//! - [`TradeLedger`] / [`EnrichmentLedger`]: dense per-minute window stores.
//! - [`WindowSpace`]: helper for precomputing minute windows.

pub mod config;
pub mod controller;
pub mod error;
pub mod ledger;
pub mod mapping;
pub mod payload;
pub mod symbol_map;
pub mod window;

pub use config::LedgerConfig;
pub use controller::{LedgerController, LedgerSlotStatusSnapshot, PendingHandle, SlotStatusCounts};
pub use error::{ControllerError, LedgerError, SlotWriteError};
pub use ledger::{EnrichmentLedger, TradeLedger};
pub use payload::{PayloadMeta, PayloadType, SlotKind, SlotStatus};
pub use symbol_map::{SymbolId, SymbolMap};
pub use window::{MINUTES_PER_SESSION, MinuteIndex, WindowMeta, WindowSpace, WindowSpaceBuilder};
