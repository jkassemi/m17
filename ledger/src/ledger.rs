use std::sync::Arc;

use parking_lot::RwLock;

use crate::{
    error::SlotWriteError,
    payload::{EnrichmentSlotKind, PayloadMeta, PayloadType, Slot, SlotKind, TradeSlotKind},
    symbol_map::SymbolId,
    window::{MinuteIndex, WindowMeta, WindowSpace},
};

pub type MinuteCount = usize;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct WindowRowHeader {
    pub symbol_id: SymbolId,
    pub minute_idx: MinuteIndex,
    pub start_ts: i64,
    pub schema_version: u32,
}

impl WindowRowHeader {
    pub fn from_meta(symbol_id: SymbolId, meta: &WindowMeta) -> Self {
        Self {
            symbol_id,
            minute_idx: meta.minute_idx,
            start_ts: meta.start_ts,
            schema_version: meta.schema_version,
        }
    }
}

pub trait LedgerRow: Clone + Send + Sync + 'static {
    fn new(symbol_id: SymbolId, meta: &WindowMeta) -> Self;
    fn slot(&self, index: usize) -> &Slot;
    fn slot_mut(&mut self, index: usize) -> &mut Slot;
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TradeWindowRow {
    pub header: WindowRowHeader,
    pub rf_rate: Slot,
    pub trade_ref: Slot,
    pub quote_ref: Slot,
    pub aggressor_ref: Slot,
}

impl Default for TradeWindowRow {
    fn default() -> Self {
        Self {
            header: WindowRowHeader::default(),
            rf_rate: Slot::default(),
            trade_ref: Slot::default(),
            quote_ref: Slot::default(),
            aggressor_ref: Slot::default(),
        }
    }
}

impl LedgerRow for TradeWindowRow {
    fn new(symbol_id: SymbolId, meta: &WindowMeta) -> Self {
        Self {
            header: WindowRowHeader::from_meta(symbol_id, meta),
            ..Default::default()
        }
    }

    fn slot(&self, index: usize) -> &Slot {
        match index {
            0 => &self.rf_rate,
            1 => &self.trade_ref,
            2 => &self.quote_ref,
            3 => &self.aggressor_ref,
            _ => panic!("invalid trade slot index {index}"),
        }
    }

    fn slot_mut(&mut self, index: usize) -> &mut Slot {
        match index {
            0 => &mut self.rf_rate,
            1 => &mut self.trade_ref,
            2 => &mut self.quote_ref,
            3 => &mut self.aggressor_ref,
            _ => panic!("invalid trade slot index {index}"),
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct EnrichmentWindowRow {
    pub header: WindowRowHeader,
    pub greeks: Slot,
    pub aggs_1m: Slot,
    pub aggs_5m: Slot,
    pub aggs_10m: Slot,
    pub aggs_30m: Slot,
    pub aggs_1h: Slot,
    pub aggs_4h: Slot,
}

impl Default for EnrichmentWindowRow {
    fn default() -> Self {
        Self {
            header: WindowRowHeader::default(),
            greeks: Slot::default(),
            aggs_1m: Slot::default(),
            aggs_5m: Slot::default(),
            aggs_10m: Slot::default(),
            aggs_30m: Slot::default(),
            aggs_1h: Slot::default(),
            aggs_4h: Slot::default(),
        }
    }
}

impl LedgerRow for EnrichmentWindowRow {
    fn new(symbol_id: SymbolId, meta: &WindowMeta) -> Self {
        Self {
            header: WindowRowHeader::from_meta(symbol_id, meta),
            ..Default::default()
        }
    }

    fn slot(&self, index: usize) -> &Slot {
        match index {
            0 => &self.greeks,
            1 => &self.aggs_1m,
            2 => &self.aggs_5m,
            3 => &self.aggs_10m,
            4 => &self.aggs_30m,
            5 => &self.aggs_1h,
            6 => &self.aggs_4h,
            _ => panic!("invalid enrichment slot index {index}"),
        }
    }

    fn slot_mut(&mut self, index: usize) -> &mut Slot {
        match index {
            0 => &mut self.greeks,
            1 => &mut self.aggs_1m,
            2 => &mut self.aggs_5m,
            3 => &mut self.aggs_10m,
            4 => &mut self.aggs_30m,
            5 => &mut self.aggs_1h,
            6 => &mut self.aggs_4h,
            _ => panic!("invalid enrichment slot index {index}"),
        }
    }
}

struct LedgerCore<Row: LedgerRow> {
    rows: RwLock<Vec<Option<Vec<Row>>>>,
    window_space: Arc<WindowSpace>,
}

impl<Row: LedgerRow> LedgerCore<Row> {
    fn new(max_symbols: SymbolId, window_space: Arc<WindowSpace>) -> Self {
        let mut rows = Vec::with_capacity(max_symbols as usize);
        rows.resize_with(max_symbols as usize, || None);
        Self {
            rows: RwLock::new(rows),
            window_space,
        }
    }

    fn ensure_symbol(&self, symbol_id: SymbolId) -> Result<(), SlotWriteError> {
        let mut guard = self.rows.write();
        self.ensure_symbol_locked(&mut guard, symbol_id)
    }

    fn ensure_symbol_locked(
        &self,
        rows: &mut Vec<Option<Vec<Row>>>,
        symbol_id: SymbolId,
    ) -> Result<(), SlotWriteError> {
        let idx = symbol_id as usize;
        if idx >= rows.len() {
            return Err(SlotWriteError::MissingSymbol { symbol_id });
        }
        if rows[idx].is_none() {
            let symbol_rows = self.build_symbol_rows(symbol_id);
            rows[idx] = Some(symbol_rows);
        }
        Ok(())
    }

    fn build_symbol_rows(&self, symbol_id: SymbolId) -> Vec<Row> {
        self.window_space
            .iter()
            .map(|meta| Row::new(symbol_id, meta))
            .collect()
    }

    fn mutate_slot<F>(
        &self,
        symbol_id: SymbolId,
        minute_idx: MinuteIndex,
        slot_index: usize,
        mutator: F,
    ) -> Result<Slot, SlotWriteError>
    where
        F: FnOnce(&mut Slot) -> Result<(), SlotWriteError>,
    {
        if minute_idx as usize >= self.window_space.len() {
            return Err(SlotWriteError::InvalidMinute {
                minute_idx,
                max: (self.window_space.len() - 1) as MinuteIndex,
            });
        }

        let mut guard = self.rows.write();
        self.ensure_symbol_locked(&mut guard, symbol_id)?;

        let rows = guard
            .get_mut(symbol_id as usize)
            .and_then(|opt| opt.as_mut())
            .ok_or(SlotWriteError::MissingSymbol { symbol_id })?;
        let slot = rows
            .get_mut(minute_idx as usize)
            .map(|row| row.slot_mut(slot_index))
            .ok_or(SlotWriteError::InvalidMinute {
                minute_idx,
                max: (self.window_space.len() - 1) as MinuteIndex,
            })?;

        mutator(slot)?;
        Ok(*slot)
    }

    fn read_row(
        &self,
        symbol_id: SymbolId,
        minute_idx: MinuteIndex,
    ) -> Result<Row, SlotWriteError> {
        if minute_idx as usize >= self.window_space.len() {
            return Err(SlotWriteError::InvalidMinute {
                minute_idx,
                max: (self.window_space.len() - 1) as MinuteIndex,
            });
        }
        let guard = self.rows.read();
        let rows = guard
            .get(symbol_id as usize)
            .and_then(|opt| opt.as_ref())
            .ok_or(SlotWriteError::MissingSymbol { symbol_id })?;
        Ok(rows[minute_idx as usize].clone())
    }

    fn iter_symbol(&self, symbol_id: SymbolId) -> Result<Vec<Row>, SlotWriteError> {
        let guard = self.rows.read();
        let rows = guard
            .get(symbol_id as usize)
            .and_then(|opt| opt.as_ref())
            .ok_or(SlotWriteError::MissingSymbol { symbol_id })?;
        Ok(rows.clone())
    }
}

pub struct TradeLedger {
    core: LedgerCore<TradeWindowRow>,
}

impl TradeLedger {
    pub fn new(max_symbols: SymbolId, window_space: Arc<WindowSpace>) -> Self {
        Self {
            core: LedgerCore::new(max_symbols, window_space),
        }
    }

    pub fn ensure_symbol(&self, symbol_id: SymbolId) -> Result<(), SlotWriteError> {
        self.core.ensure_symbol(symbol_id)
    }

    pub fn mark_pending(
        &self,
        symbol_id: SymbolId,
        minute_idx: MinuteIndex,
        kind: TradeSlotKind,
    ) -> Result<Slot, SlotWriteError> {
        self.core
            .mutate_slot(symbol_id, minute_idx, kind.index(), |slot| {
                slot.mark_pending();
                Ok(())
            })
    }

    pub fn clear_slot(
        &self,
        symbol_id: SymbolId,
        minute_idx: MinuteIndex,
        kind: TradeSlotKind,
    ) -> Result<Slot, SlotWriteError> {
        self.core
            .mutate_slot(symbol_id, minute_idx, kind.index(), |slot| {
                slot.clear();
                Ok(())
            })
    }

    pub fn write_slot(
        &self,
        symbol_id: SymbolId,
        minute_idx: MinuteIndex,
        kind: TradeSlotKind,
        meta: PayloadMeta,
        expected_version: Option<u32>,
    ) -> Result<Slot, SlotWriteError> {
        self.core
            .mutate_slot(symbol_id, minute_idx, kind.index(), |slot| {
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

    pub fn get_row(
        &self,
        symbol_id: SymbolId,
        minute_idx: MinuteIndex,
    ) -> Result<TradeWindowRow, SlotWriteError> {
        self.core.read_row(symbol_id, minute_idx)
    }

    pub fn iter_symbol(&self, symbol_id: SymbolId) -> Result<Vec<TradeWindowRow>, SlotWriteError> {
        self.core.iter_symbol(symbol_id)
    }
}

pub struct EnrichmentLedger {
    core: LedgerCore<EnrichmentWindowRow>,
}

impl EnrichmentLedger {
    pub fn new(max_symbols: SymbolId, window_space: Arc<WindowSpace>) -> Self {
        Self {
            core: LedgerCore::new(max_symbols, window_space),
        }
    }

    pub fn ensure_symbol(&self, symbol_id: SymbolId) -> Result<(), SlotWriteError> {
        self.core.ensure_symbol(symbol_id)
    }

    pub fn mark_pending(
        &self,
        symbol_id: SymbolId,
        minute_idx: MinuteIndex,
        kind: EnrichmentSlotKind,
    ) -> Result<Slot, SlotWriteError> {
        self.core
            .mutate_slot(symbol_id, minute_idx, kind.index(), |slot| {
                slot.mark_pending();
                Ok(())
            })
    }

    pub fn clear_slot(
        &self,
        symbol_id: SymbolId,
        minute_idx: MinuteIndex,
        kind: EnrichmentSlotKind,
    ) -> Result<Slot, SlotWriteError> {
        self.core
            .mutate_slot(symbol_id, minute_idx, kind.index(), |slot| {
                slot.clear();
                Ok(())
            })
    }

    pub fn write_slot(
        &self,
        symbol_id: SymbolId,
        minute_idx: MinuteIndex,
        kind: EnrichmentSlotKind,
        meta: PayloadMeta,
        expected_version: Option<u32>,
    ) -> Result<Slot, SlotWriteError> {
        self.core
            .mutate_slot(symbol_id, minute_idx, kind.index(), |slot| {
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

    pub fn get_row(
        &self,
        symbol_id: SymbolId,
        minute_idx: MinuteIndex,
    ) -> Result<EnrichmentWindowRow, SlotWriteError> {
        self.core.read_row(symbol_id, minute_idx)
    }

    pub fn iter_symbol(
        &self,
        symbol_id: SymbolId,
    ) -> Result<Vec<EnrichmentWindowRow>, SlotWriteError> {
        self.core.iter_symbol(symbol_id)
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

    fn window() -> Arc<WindowSpace> {
        Arc::new(WindowSpace::standard(1_600_000_000))
    }

    #[test]
    fn trade_slot_write_and_conflict() {
        let ledger = TradeLedger::new(4, window());
        ledger.ensure_symbol(0).unwrap();

        let meta = PayloadMeta::new(PayloadType::Trade, 42, 1, 777);
        let slot = ledger
            .write_slot(0, 0, TradeSlotKind::Trade, meta, None)
            .unwrap();
        assert_eq!(slot.payload_id, 42);
        assert_eq!(slot.version, 1);

        let err = ledger
            .write_slot(0, 0, TradeSlotKind::Trade, meta, Some(0))
            .unwrap_err();
        assert!(matches!(err, SlotWriteError::VersionConflict { .. }));
    }

    #[test]
    fn enrichment_slot_rejects_wrong_payload() {
        let ledger = EnrichmentLedger::new(4, window());
        ledger.ensure_symbol(0).unwrap();
        let meta = PayloadMeta::new(PayloadType::Trade, 1, 1, 1);
        let err = ledger
            .write_slot(0, 0, EnrichmentSlotKind::Greeks, meta, None)
            .unwrap_err();
        assert!(matches!(err, SlotWriteError::PayloadTypeMismatch { .. }));
    }
}
