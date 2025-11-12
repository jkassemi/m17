use std::path::PathBuf;

use thiserror::Error;

use crate::{
    payload::{PayloadType, SlotKind},
    symbol_map::{SymbolId, SymbolMapError},
    window::MinuteIndex,
};

pub type Result<T> = std::result::Result<T, LedgerError>;

#[derive(Debug, Error)]
pub enum SlotWriteError {
    #[error("minute_idx {minute_idx} exceeds max {max}")]
    InvalidMinute {
        minute_idx: MinuteIndex,
        max: MinuteIndex,
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
pub enum LedgerError {
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
    #[error("ledger error: {0}")]
    Ledger(#[from] LedgerError),
}

impl From<SlotWriteError> for ControllerError {
    fn from(value: SlotWriteError) -> Self {
        LedgerError::from(value).into()
    }
}
