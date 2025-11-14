use std::{marker::PhantomData, mem, path::Path, ptr, slice, sync::Arc};

use parking_lot::RwLock;

use crate::{
    error::{SlotWriteError, WindowSpaceError},
    payload::{
        EnrichmentSlotKind, PayloadMeta, PayloadType, Slot, SlotKind, SlotStatus, TradeSlotKind,
    },
    slot_metrics::{SlotMetrics, StoreKind},
    storage::{WindowSpaceFile, WindowSpaceFileOptions, WindowSpaceFileStats},
    symbol_map::SymbolId,
    window::{WindowIndex, WindowMeta, WindowSpace},
};

const TRADE_ROW_SCHEMA_VERSION: u32 = 1;
const ENRICHMENT_ROW_SCHEMA_VERSION: u32 = 1;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct WindowRowHeader {
    pub symbol_id: SymbolId,
    pub window_idx: WindowIndex,
    pub start_ts: i64,
    pub schema_version: u32,
}

impl WindowRowHeader {
    pub fn from_meta(symbol_id: SymbolId, meta: &WindowMeta) -> Self {
        Self {
            symbol_id,
            window_idx: meta.window_idx,
            start_ts: meta.start_ts,
            schema_version: meta.schema_version,
        }
    }
}

pub trait WindowRow: Clone + Copy + Send + Sync + 'static {
    fn new(symbol_id: SymbolId, meta: &WindowMeta) -> Self;
    fn slot(&self, index: usize) -> &Slot;
    fn slot_mut(&mut self, index: usize) -> &mut Slot;
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TradeWindowRow {
    pub header: WindowRowHeader,
    pub rf_rate: Slot,
    pub option_trade_ref: Slot,
    pub option_quote_ref: Slot,
    pub underlying_trade_ref: Slot,
    pub underlying_quote_ref: Slot,
    pub option_aggressor_ref: Slot,
    pub underlying_aggressor_ref: Slot,
}

impl Default for TradeWindowRow {
    fn default() -> Self {
        Self {
            header: WindowRowHeader::default(),
            rf_rate: Slot::default(),
            option_trade_ref: Slot::default(),
            option_quote_ref: Slot::default(),
            underlying_trade_ref: Slot::default(),
            underlying_quote_ref: Slot::default(),
            option_aggressor_ref: Slot::default(),
            underlying_aggressor_ref: Slot::default(),
        }
    }
}

impl WindowRow for TradeWindowRow {
    fn new(symbol_id: SymbolId, meta: &WindowMeta) -> Self {
        Self {
            header: WindowRowHeader::from_meta(symbol_id, meta),
            ..Default::default()
        }
    }

    fn slot(&self, index: usize) -> &Slot {
        match index {
            0 => &self.rf_rate,
            1 => &self.option_trade_ref,
            2 => &self.option_quote_ref,
            3 => &self.underlying_trade_ref,
            4 => &self.underlying_quote_ref,
            5 => &self.option_aggressor_ref,
            6 => &self.underlying_aggressor_ref,
            _ => panic!("invalid trade slot index {index}"),
        }
    }

    fn slot_mut(&mut self, index: usize) -> &mut Slot {
        match index {
            0 => &mut self.rf_rate,
            1 => &mut self.option_trade_ref,
            2 => &mut self.option_quote_ref,
            3 => &mut self.underlying_trade_ref,
            4 => &mut self.underlying_quote_ref,
            5 => &mut self.option_aggressor_ref,
            6 => &mut self.underlying_aggressor_ref,
            _ => panic!("invalid trade slot index {index}"),
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct EnrichmentWindowRow {
    pub header: WindowRowHeader,
    pub greeks: Slot,
}

impl Default for EnrichmentWindowRow {
    fn default() -> Self {
        Self {
            header: WindowRowHeader::default(),
            greeks: Slot::default(),
        }
    }
}

impl WindowRow for EnrichmentWindowRow {
    fn new(symbol_id: SymbolId, meta: &WindowMeta) -> Self {
        Self {
            header: WindowRowHeader::from_meta(symbol_id, meta),
            ..Default::default()
        }
    }

    fn slot(&self, index: usize) -> &Slot {
        match index {
            0 => &self.greeks,
            _ => panic!("invalid enrichment slot index {index}"),
        }
    }

    fn slot_mut(&mut self, index: usize) -> &mut Slot {
        match index {
            0 => &mut self.greeks,
            _ => panic!("invalid enrichment slot index {index}"),
        }
    }
}

pub struct TradeWindowSpace {
    core: WindowSpaceCore<TradeWindowRow>,
}

impl TradeWindowSpace {
    pub fn bootstrap(
        path: &Path,
        max_symbols: SymbolId,
        window_space: Arc<WindowSpace>,
        slot_metrics: Option<Arc<SlotMetrics>>,
    ) -> Result<(Self, WindowSpaceFileStats), WindowSpaceError> {
        let window_count = window_space.len();
        let (storage, stats) =
            RowStorage::new(path, window_count, max_symbols, TRADE_ROW_SCHEMA_VERSION)?;
        Ok((
            Self {
                core: WindowSpaceCore::new(
                    storage,
                    window_space,
                    slot_metrics,
                    StoreKind::Trade,
                    trade_slot_kinds(),
                ),
            },
            stats,
        ))
    }

    pub fn ensure_symbol(&self, symbol_id: SymbolId) -> Result<(), SlotWriteError> {
        self.core.ensure_symbol(symbol_id)
    }

    pub fn mark_symbol_loaded(&self, symbol_id: SymbolId) -> Result<(), SlotWriteError> {
        self.core.mark_symbol_ready(symbol_id)
    }

    pub fn mark_pending(
        &self,
        symbol_id: SymbolId,
        window_idx: WindowIndex,
        kind: TradeSlotKind,
    ) -> Result<Slot, SlotWriteError> {
        self.core
            .mutate_slot(symbol_id, window_idx, kind.index(), |slot| {
                slot.mark_pending();
                Ok(())
            })
    }

    pub fn clear_slot(
        &self,
        symbol_id: SymbolId,
        window_idx: WindowIndex,
        kind: TradeSlotKind,
    ) -> Result<Slot, SlotWriteError> {
        self.core
            .mutate_slot(symbol_id, window_idx, kind.index(), |slot| {
                slot.clear();
                Ok(())
            })
    }

    pub fn mark_prune(
        &self,
        symbol_id: SymbolId,
        window_idx: WindowIndex,
        kind: TradeSlotKind,
    ) -> Result<Slot, SlotWriteError> {
        self.core
            .mutate_slot(symbol_id, window_idx, kind.index(), |slot| {
                slot.mark_prune();
                Ok(())
            })
    }

    pub fn mark_pruned(
        &self,
        symbol_id: SymbolId,
        window_idx: WindowIndex,
        kind: TradeSlotKind,
    ) -> Result<Slot, SlotWriteError> {
        self.core
            .mutate_slot(symbol_id, window_idx, kind.index(), |slot| {
                slot.mark_pruned();
                Ok(())
            })
    }

    pub fn write_slot(
        &self,
        symbol_id: SymbolId,
        window_idx: WindowIndex,
        kind: TradeSlotKind,
        meta: PayloadMeta,
        expected_version: Option<u32>,
    ) -> Result<Slot, SlotWriteError> {
        self.core
            .mutate_slot(symbol_id, window_idx, kind.index(), |slot| {
                validate_payload(kind.payload_type(), &meta, SlotKind::Trade(kind))?;
                if let Some(expected) = expected_version {
                    if slot.version != expected {
                        return Err(SlotWriteError::VersionConflict {
                            slot: SlotKind::Trade(kind),
                            expected: Some(expected),
                            actual: slot.version,
                        });
                    }
                }
                slot.overwrite(&meta);
                Ok(())
            })
    }

    pub fn mark_retired(
        &self,
        symbol_id: SymbolId,
        window_idx: WindowIndex,
        kind: TradeSlotKind,
    ) -> Result<Slot, SlotWriteError> {
        self.core
            .mutate_slot(symbol_id, window_idx, kind.index(), |slot| {
                slot.retire();
                Ok(())
            })
    }

    pub fn get_row(
        &self,
        symbol_id: SymbolId,
        window_idx: WindowIndex,
    ) -> Result<TradeWindowRow, SlotWriteError> {
        self.core.read_row(symbol_id, window_idx)
    }

    pub fn iter_symbol(&self, symbol_id: SymbolId) -> Result<Vec<TradeWindowRow>, SlotWriteError> {
        self.core.iter_symbol(symbol_id)
    }

    pub fn next_unfilled_window(
        &self,
        symbol_id: SymbolId,
        start_idx: WindowIndex,
        kind: TradeSlotKind,
    ) -> Result<Option<WindowIndex>, SlotWriteError> {
        self.core.next_unfilled(symbol_id, start_idx, kind.index())
    }

    pub fn with_symbol_rows<F, R>(&self, symbol_id: SymbolId, f: F) -> Result<R, SlotWriteError>
    where
        F: FnOnce(&[TradeWindowRow]) -> R,
    {
        self.core.with_symbol_rows(symbol_id, f)
    }
}

pub struct EnrichmentWindowSpace {
    core: WindowSpaceCore<EnrichmentWindowRow>,
}

impl EnrichmentWindowSpace {
    pub fn bootstrap(
        path: &Path,
        max_symbols: SymbolId,
        window_space: Arc<WindowSpace>,
        slot_metrics: Option<Arc<SlotMetrics>>,
    ) -> Result<(Self, WindowSpaceFileStats), WindowSpaceError> {
        let window_count = window_space.len();
        let (storage, stats) = RowStorage::new(
            path,
            window_count,
            max_symbols,
            ENRICHMENT_ROW_SCHEMA_VERSION,
        )?;
        Ok((
            Self {
                core: WindowSpaceCore::new(
                    storage,
                    window_space,
                    slot_metrics,
                    StoreKind::Enrichment,
                    enrichment_slot_kinds(),
                ),
            },
            stats,
        ))
    }

    pub fn ensure_symbol(&self, symbol_id: SymbolId) -> Result<(), SlotWriteError> {
        self.core.ensure_symbol(symbol_id)
    }

    pub fn mark_symbol_loaded(&self, symbol_id: SymbolId) -> Result<(), SlotWriteError> {
        self.core.mark_symbol_ready(symbol_id)
    }

    pub fn mark_pending(
        &self,
        symbol_id: SymbolId,
        window_idx: WindowIndex,
        kind: EnrichmentSlotKind,
    ) -> Result<Slot, SlotWriteError> {
        self.core
            .mutate_slot(symbol_id, window_idx, kind.index(), |slot| {
                slot.mark_pending();
                Ok(())
            })
    }

    pub fn clear_slot(
        &self,
        symbol_id: SymbolId,
        window_idx: WindowIndex,
        kind: EnrichmentSlotKind,
    ) -> Result<Slot, SlotWriteError> {
        self.core
            .mutate_slot(symbol_id, window_idx, kind.index(), |slot| {
                slot.clear();
                Ok(())
            })
    }

    pub fn mark_prune(
        &self,
        symbol_id: SymbolId,
        window_idx: WindowIndex,
        kind: EnrichmentSlotKind,
    ) -> Result<Slot, SlotWriteError> {
        self.core
            .mutate_slot(symbol_id, window_idx, kind.index(), |slot| {
                slot.mark_prune();
                Ok(())
            })
    }

    pub fn mark_pruned(
        &self,
        symbol_id: SymbolId,
        window_idx: WindowIndex,
        kind: EnrichmentSlotKind,
    ) -> Result<Slot, SlotWriteError> {
        self.core
            .mutate_slot(symbol_id, window_idx, kind.index(), |slot| {
                slot.mark_pruned();
                Ok(())
            })
    }

    pub fn write_slot(
        &self,
        symbol_id: SymbolId,
        window_idx: WindowIndex,
        kind: EnrichmentSlotKind,
        meta: PayloadMeta,
        expected_version: Option<u32>,
    ) -> Result<Slot, SlotWriteError> {
        self.core
            .mutate_slot(symbol_id, window_idx, kind.index(), |slot| {
                validate_payload(kind.payload_type(), &meta, SlotKind::Enrichment(kind))?;
                if let Some(expected) = expected_version {
                    if slot.version != expected {
                        return Err(SlotWriteError::VersionConflict {
                            slot: SlotKind::Enrichment(kind),
                            expected: Some(expected),
                            actual: slot.version,
                        });
                    }
                }
                slot.overwrite(&meta);
                Ok(())
            })
    }

    pub fn mark_retired(
        &self,
        symbol_id: SymbolId,
        window_idx: WindowIndex,
        kind: EnrichmentSlotKind,
    ) -> Result<Slot, SlotWriteError> {
        self.core
            .mutate_slot(symbol_id, window_idx, kind.index(), |slot| {
                slot.retire();
                Ok(())
            })
    }

    pub fn get_row(
        &self,
        symbol_id: SymbolId,
        window_idx: WindowIndex,
    ) -> Result<EnrichmentWindowRow, SlotWriteError> {
        self.core.read_row(symbol_id, window_idx)
    }

    pub fn iter_symbol(
        &self,
        symbol_id: SymbolId,
    ) -> Result<Vec<EnrichmentWindowRow>, SlotWriteError> {
        self.core.iter_symbol(symbol_id)
    }

    pub fn next_unfilled_window(
        &self,
        symbol_id: SymbolId,
        start_idx: WindowIndex,
        kind: EnrichmentSlotKind,
    ) -> Result<Option<WindowIndex>, SlotWriteError> {
        self.core.next_unfilled(symbol_id, start_idx, kind.index())
    }

    pub fn with_symbol_rows<F, R>(&self, symbol_id: SymbolId, f: F) -> Result<R, SlotWriteError>
    where
        F: FnOnce(&[EnrichmentWindowRow]) -> R,
    {
        self.core.with_symbol_rows(symbol_id, f)
    }
}

fn trade_slot_kinds() -> Vec<SlotKind> {
    TradeSlotKind::ALL
        .iter()
        .copied()
        .map(SlotKind::Trade)
        .collect()
}

fn enrichment_slot_kinds() -> Vec<SlotKind> {
    EnrichmentSlotKind::ALL
        .iter()
        .copied()
        .map(SlotKind::Enrichment)
        .collect()
}

#[deprecated(
    since = "0.1.0",
    note = "TradeLedger has been renamed to TradeWindowSpace; update imports to window_space::TradeWindowSpace"
)]
pub type TradeLedger = TradeWindowSpace;

#[deprecated(
    since = "0.1.0",
    note = "EnrichmentLedger has been renamed to EnrichmentWindowSpace; update imports to window_space::EnrichmentWindowSpace"
)]
pub type EnrichmentLedger = EnrichmentWindowSpace;

#[deprecated(
    since = "0.1.0",
    note = "LedgerRow has been renamed to WindowRow; update trait bounds to window_space::WindowRow"
)]
pub trait LedgerRow: WindowRow {}

#[allow(deprecated)]
impl<T: WindowRow> LedgerRow for T {}

struct WindowSpaceCore<Row: WindowRow> {
    storage: RowStorage<Row>,
    allocations: RwLock<Vec<bool>>,
    window_space: Arc<WindowSpace>,
    slot_metrics: Option<Arc<SlotMetrics>>,
    store_kind: StoreKind,
    slot_kinds: Vec<SlotKind>,
}

impl<Row: WindowRow> WindowSpaceCore<Row> {
    fn new(
        storage: RowStorage<Row>,
        window_space: Arc<WindowSpace>,
        slot_metrics: Option<Arc<SlotMetrics>>,
        store_kind: StoreKind,
        slot_kinds: Vec<SlotKind>,
    ) -> Self {
        let allocations = vec![false; storage.max_symbols()];
        Self {
            storage,
            allocations: RwLock::new(allocations),
            window_space,
            slot_metrics,
            store_kind,
            slot_kinds,
        }
    }

    fn ensure_symbol(&self, symbol_id: SymbolId) -> Result<(), SlotWriteError> {
        let mut guard = self.allocations.write();
        self.ensure_symbol_locked(&mut guard, symbol_id)
    }

    fn mark_symbol_ready(&self, symbol_id: SymbolId) -> Result<(), SlotWriteError> {
        let mut guard = self.allocations.write();
        let idx = symbol_id as usize;
        if idx >= guard.len() {
            return Err(SlotWriteError::MissingSymbol { symbol_id });
        }
        guard[idx] = true;
        Ok(())
    }

    fn ensure_symbol_locked(
        &self,
        allocations: &mut [bool],
        symbol_id: SymbolId,
    ) -> Result<(), SlotWriteError> {
        let idx = symbol_id as usize;
        if idx >= allocations.len() {
            return Err(SlotWriteError::MissingSymbol { symbol_id });
        }
        if !allocations[idx] {
            self.storage
                .initialize_symbol(symbol_id, &self.window_space);
            allocations[idx] = true;
            if let Some(metrics) = &self.slot_metrics {
                metrics.record_symbol_bootstrap(self.store_kind, &self.slot_kinds);
            }
        }
        Ok(())
    }

    fn mutate_slot<F>(
        &self,
        symbol_id: SymbolId,
        window_idx: WindowIndex,
        slot_index: usize,
        mutator: F,
    ) -> Result<Slot, SlotWriteError>
    where
        F: FnOnce(&mut Slot) -> Result<(), SlotWriteError>,
    {
        let window_idx_usize = window_idx as usize;
        if window_idx_usize >= self.storage.window_count() {
            return Err(SlotWriteError::InvalidWindow {
                window_idx,
                max: self.max_window_idx(),
            });
        }
        let mut guard = self.allocations.write();
        self.ensure_symbol_locked(&mut guard, symbol_id)?;
        drop(guard);
        unsafe {
            let row_ptr = self.storage.row_ptr(symbol_id, window_idx_usize);
            let row = &mut *row_ptr;
            let slot = row.slot_mut(slot_index);
            let previous = *slot;
            mutator(slot)?;
            let updated = *slot;
            if let Some(metrics) = &self.slot_metrics {
                if let Some(slot_kind) = self.slot_kinds.get(slot_index).copied() {
                    metrics.record_transition(slot_kind, previous.status, updated.status);
                }
            }
            Ok(updated)
        }
    }

    fn read_row(
        &self,
        symbol_id: SymbolId,
        window_idx: WindowIndex,
    ) -> Result<Row, SlotWriteError> {
        let window_idx_usize = window_idx as usize;
        if window_idx_usize >= self.storage.window_count() {
            return Err(SlotWriteError::InvalidWindow {
                window_idx,
                max: self.max_window_idx(),
            });
        }
        let guard = self.allocations.read();
        if !self.symbol_ready(&guard, symbol_id)? {
            return Err(SlotWriteError::MissingSymbol { symbol_id });
        }
        drop(guard);
        Ok(self.storage.read_row(symbol_id, window_idx_usize))
    }

    fn iter_symbol(&self, symbol_id: SymbolId) -> Result<Vec<Row>, SlotWriteError> {
        let guard = self.allocations.read();
        if !self.symbol_ready(&guard, symbol_id)? {
            return Err(SlotWriteError::MissingSymbol { symbol_id });
        }
        drop(guard);
        Ok(self.storage.rows_slice(symbol_id).to_vec())
    }

    fn next_unfilled(
        &self,
        symbol_id: SymbolId,
        start_idx: WindowIndex,
        slot_index: usize,
    ) -> Result<Option<WindowIndex>, SlotWriteError> {
        let start = start_idx as usize;
        if start >= self.storage.window_count() {
            return Ok(None);
        }
        let guard = self.allocations.read();
        if !self.symbol_ready(&guard, symbol_id)? {
            return Err(SlotWriteError::MissingSymbol { symbol_id });
        }
        drop(guard);
        for idx in start..self.storage.window_count() {
            let row = self.storage.read_row(symbol_id, idx);
            let status = row.slot(slot_index).status;
            if !matches!(
                status,
                SlotStatus::Filled | SlotStatus::Retired | SlotStatus::Prune | SlotStatus::Pruned
            ) {
                return Ok(Some(idx as WindowIndex));
            }
        }
        Ok(None)
    }

    fn with_symbol_rows<F, R>(&self, symbol_id: SymbolId, f: F) -> Result<R, SlotWriteError>
    where
        F: FnOnce(&[Row]) -> R,
    {
        let guard = self.allocations.read();
        if !self.symbol_ready(&guard, symbol_id)? {
            return Err(SlotWriteError::MissingSymbol { symbol_id });
        }
        drop(guard);
        Ok(f(self.storage.rows_slice(symbol_id)))
    }

    fn max_window_idx(&self) -> WindowIndex {
        let len = self.storage.window_count();
        if len == 0 {
            0
        } else {
            (len - 1) as WindowIndex
        }
    }

    fn symbol_ready(
        &self,
        allocations: &[bool],
        symbol_id: SymbolId,
    ) -> Result<bool, SlotWriteError> {
        let idx = symbol_id as usize;
        if idx >= allocations.len() {
            return Err(SlotWriteError::MissingSymbol { symbol_id });
        }
        Ok(allocations[idx])
    }
}

struct RowStorage<Row: WindowRow> {
    file: WindowSpaceFile,
    window_count: usize,
    row_size: usize,
    max_symbols: usize,
    _marker: PhantomData<Row>,
}

impl<Row: WindowRow> RowStorage<Row> {
    fn new(
        path: &Path,
        window_count: usize,
        max_symbols: SymbolId,
        schema_version: u32,
    ) -> Result<(Self, WindowSpaceFileStats), WindowSpaceError> {
        let row_size = mem::size_of::<Row>();
        let options = WindowSpaceFileOptions {
            path: path.to_path_buf(),
            window_count,
            max_symbols: max_symbols as usize,
            row_size,
            schema_version,
        };
        let (file, stats) = WindowSpaceFile::open(options).map_err(WindowSpaceError::from)?;
        Ok((
            Self {
                file,
                window_count,
                row_size,
                max_symbols: max_symbols as usize,
                _marker: PhantomData,
            },
            stats,
        ))
    }

    fn window_count(&self) -> usize {
        self.window_count
    }

    fn max_symbols(&self) -> usize {
        self.max_symbols
    }

    unsafe fn row_ptr(&self, symbol_id: SymbolId, window_idx: usize) -> *mut Row {
        let offset = (symbol_id as usize * self.window_count + window_idx) * self.row_size;
        unsafe { self.file.data_ptr().add(offset) as *mut Row }
    }

    fn rows_slice(&self, symbol_id: SymbolId) -> &[Row] {
        let offset = symbol_id as usize * self.window_count * self.row_size;
        unsafe {
            let ptr = self.file.data_ptr().add(offset) as *const Row;
            slice::from_raw_parts(ptr, self.window_count)
        }
    }

    fn initialize_symbol(&self, symbol_id: SymbolId, window_space: &WindowSpace) {
        for (idx, meta) in window_space.iter().enumerate() {
            unsafe {
                let row_ptr = self.row_ptr(symbol_id, idx);
                ptr::write(row_ptr, Row::new(symbol_id, meta));
            }
        }
    }

    fn read_row(&self, symbol_id: SymbolId, window_idx: usize) -> Row {
        unsafe { *self.row_ptr(symbol_id, window_idx) }
    }
}

fn validate_payload(
    expected_type: PayloadType,
    meta: &PayloadMeta,
    slot: SlotKind,
) -> Result<(), SlotWriteError> {
    if meta.payload_type != expected_type {
        return Err(SlotWriteError::PayloadTypeMismatch {
            slot,
            expected: expected_type,
            actual: meta.payload_type,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload::{PayloadMeta, PayloadType};
    use tempfile::tempdir;

    fn window() -> Arc<WindowSpace> {
        Arc::new(WindowSpace::standard(1_600_000_000))
    }

    #[test]
    fn trade_slot_write_and_conflict() {
        let dir = tempdir().unwrap();
        let trade_path = dir.path().join("trade-ledger.dat");
        let (ledger, _stats) =
            TradeWindowSpace::bootstrap(trade_path.as_path(), 4, window(), None).unwrap();

        ledger.ensure_symbol(0).unwrap();
        let meta = PayloadMeta::new(PayloadType::Trade, 42, 1, 777);
        let slot = ledger
            .write_slot(0, 0, TradeSlotKind::OptionTrade, meta, None)
            .unwrap();
        assert_eq!(slot.payload_id, 42);

        let err = ledger
            .write_slot(0, 0, TradeSlotKind::OptionTrade, meta, Some(0))
            .unwrap_err();
        assert!(matches!(err, SlotWriteError::VersionConflict { .. }));
    }

    #[test]
    fn enrichment_slot_roundtrip() {
        let dir = tempdir().unwrap();
        let enrich_path = dir.path().join("enrich-ledger.dat");
        let (ledger, _stats) =
            EnrichmentWindowSpace::bootstrap(enrich_path.as_path(), 4, window(), None).unwrap();

        ledger.ensure_symbol(1).unwrap();
        let meta = PayloadMeta::new(PayloadType::Greeks, 5, 2, 111);
        ledger
            .write_slot(1, 0, EnrichmentSlotKind::Greeks, meta, None)
            .unwrap();
        let row = ledger.get_row(1, 0).unwrap();
        assert_eq!(row.greeks.payload_id, 5);
    }
}
