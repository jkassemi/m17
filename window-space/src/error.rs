use std::path::PathBuf;

use thiserror::Error;

use crate::{
    payload::{PayloadType, SlotKind},
    symbol_map::{SymbolId, SymbolMapError},
    window::WindowIndex,
};

pub type Result<T> = std::result::Result<T, WindowSpaceError>;

#[derive(Debug, Error)]
pub enum SlotWriteError {
    #[error("window_idx {window_idx} exceeds max {max}")]
    InvalidWindow {
        window_idx: WindowIndex,
        max: WindowIndex,
    },
    #[error("symbol {symbol_id} not allocated in ledger")]
    MissingSymbol { symbol_id: SymbolId },
    #[error("payload type mismatch for slot {slot:?}: expected {expected:?}, got {actual:?}")]
    PayloadTypeMismatch {
        slot: SlotKind,
        expected: PayloadType,
        actual: PayloadType,
    },
    #[error("version conflict for slot {slot:?}: expected {expected:?}, actual {actual}")]
    VersionConflict {
        slot: SlotKind,
        expected: Option<u32>,
        actual: u32,
    },
}

#[derive(Debug, Error)]
pub enum WindowSpaceError {
    #[error("state directory missing: {path}")]
    MissingStateDir { path: PathBuf },
    #[error("symbol map error: {0}")]
    SymbolMap(#[from] SymbolMapError),
    #[error("slot write error: {0}")]
    SlotWrite(#[from] SlotWriteError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Error)]
pub enum ControllerError {
    #[error("window space error: {0}")]
    Ledger(#[from] WindowSpaceError),
}

impl From<SlotWriteError> for ControllerError {
    fn from(value: SlotWriteError) -> Self {
        WindowSpaceError::from(value).into()
    }
}

#[deprecated(
    since = "0.1.0",
    note = "LedgerError has been renamed to WindowSpaceError; update imports to window_space::WindowSpaceError"
)]
pub type LedgerError = WindowSpaceError;
