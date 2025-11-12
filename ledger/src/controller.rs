use std::{collections::BTreeMap, fmt, sync::Arc};

use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::{
    config::LedgerConfig,
    error::{LedgerError, Result},
    ledger::{EnrichmentLedger, LedgerRow, TradeLedger},
    mapping::PayloadStores,
    payload::{
        AggregateWindowKind, EnrichmentSlotKind, PayloadMeta, Slot, SlotKind, SlotStatus,
        TradeSlotKind,
    },
    symbol_map::{SymbolId, SymbolMap},
    window::{MinuteIndex, WindowSpace},
};

pub struct LedgerController {
    config: LedgerConfig,
    trade_ledger: Arc<TradeLedger>,
    enrichment_ledger: Arc<EnrichmentLedger>,
    symbol_map: RwLock<SymbolMap>,
    payload_stores: Mutex<PayloadStores>,
    window_space: Arc<WindowSpace>,
}

impl LedgerController {
    pub fn bootstrap(config: LedgerConfig) -> Result<Self> {
        config.ensure_dirs()?;
        let symbol_map_path = config.symbol_map_path();
        let symbol_map = SymbolMap::load_or_init(&symbol_map_path, config.max_symbols)?;
        let window_space = Arc::new(config.window_space.clone());

        let trade_ledger = Arc::new(TradeLedger::new(config.max_symbols, window_space.clone()));
        let enrichment_ledger = Arc::new(EnrichmentLedger::new(
            config.max_symbols,
            window_space.clone(),
        ));

        for (symbol_id, _) in symbol_map.iter() {
            trade_ledger
                .ensure_symbol(symbol_id)
                .expect("seed trade ledger");
            enrichment_ledger
                .ensure_symbol(symbol_id)
                .expect("seed enrichment ledger");
        }

        let payload_stores = PayloadStores::load(config.state_dir())?;

        Ok(Self {
            config,
            trade_ledger,
            enrichment_ledger,
            symbol_map: RwLock::new(symbol_map),
            payload_stores: Mutex::new(payload_stores),
            window_space,
        })
    }

    pub fn trade_ledger(&self) -> Arc<TradeLedger> {
        Arc::clone(&self.trade_ledger)
    }

    pub fn enrichment_ledger(&self) -> Arc<EnrichmentLedger> {
        Arc::clone(&self.enrichment_ledger)
    }

    pub fn resolve_symbol(&self, symbol: &str) -> Result<SymbolId> {
        let mut map = self.symbol_map.write();
        let symbol_id = map.resolve_or_insert(symbol)?;
        map.persist()?;
        self.trade_ledger
            .ensure_symbol(symbol_id)
            .map_err(LedgerError::from)?;
        self.enrichment_ledger
            .ensure_symbol(symbol_id)
            .map_err(LedgerError::from)?;
        Ok(symbol_id)
    }

    pub fn set_rf_rate_ref(
        &self,
        symbol: &str,
        minute_idx: MinuteIndex,
        meta: PayloadMeta,
        expected_version: Option<u32>,
    ) -> Result<Slot> {
        let symbol_id = self.resolve_symbol(symbol)?;
        Ok(self.trade_ledger.write_slot(
            symbol_id,
            minute_idx,
            TradeSlotKind::RfRate,
            meta,
            expected_version,
        )?)
    }

    pub fn set_option_trade_ref(
        &self,
        symbol: &str,
        minute_idx: MinuteIndex,
        meta: PayloadMeta,
        expected_version: Option<u32>,
    ) -> Result<Slot> {
        let symbol_id = self.resolve_symbol(symbol)?;
        Ok(self.trade_ledger.write_slot(
            symbol_id,
            minute_idx,
            TradeSlotKind::OptionTrade,
            meta,
            expected_version,
        )?)
    }

    pub fn set_option_quote_ref(
        &self,
        symbol: &str,
        minute_idx: MinuteIndex,
        meta: PayloadMeta,
        expected_version: Option<u32>,
    ) -> Result<Slot> {
        let symbol_id = self.resolve_symbol(symbol)?;
        Ok(self.trade_ledger.write_slot(
            symbol_id,
            minute_idx,
            TradeSlotKind::OptionQuote,
            meta,
            expected_version,
        )?)
    }

    pub fn set_underlying_trade_ref(
        &self,
        symbol: &str,
        minute_idx: MinuteIndex,
        meta: PayloadMeta,
        expected_version: Option<u32>,
    ) -> Result<Slot> {
        let symbol_id = self.resolve_symbol(symbol)?;
        Ok(self.trade_ledger.write_slot(
            symbol_id,
            minute_idx,
            TradeSlotKind::UnderlyingTrade,
            meta,
            expected_version,
        )?)
    }

    pub fn set_underlying_quote_ref(
        &self,
        symbol: &str,
        minute_idx: MinuteIndex,
        meta: PayloadMeta,
        expected_version: Option<u32>,
    ) -> Result<Slot> {
        let symbol_id = self.resolve_symbol(symbol)?;
        Ok(self.trade_ledger.write_slot(
            symbol_id,
            minute_idx,
            TradeSlotKind::UnderlyingQuote,
            meta,
            expected_version,
        )?)
    }

    pub fn set_option_aggressor_ref(
        &self,
        symbol: &str,
        minute_idx: MinuteIndex,
        meta: PayloadMeta,
        expected_version: Option<u32>,
    ) -> Result<Slot> {
        let symbol_id = self.resolve_symbol(symbol)?;
        Ok(self.trade_ledger.write_slot(
            symbol_id,
            minute_idx,
            TradeSlotKind::OptionAggressor,
            meta,
            expected_version,
        )?)
    }

    pub fn set_underlying_aggressor_ref(
        &self,
        symbol: &str,
        minute_idx: MinuteIndex,
        meta: PayloadMeta,
        expected_version: Option<u32>,
    ) -> Result<Slot> {
        let symbol_id = self.resolve_symbol(symbol)?;
        Ok(self.trade_ledger.write_slot(
            symbol_id,
            minute_idx,
            TradeSlotKind::UnderlyingAggressor,
            meta,
            expected_version,
        )?)
    }

    pub fn set_greeks_ref(
        &self,
        symbol: &str,
        minute_idx: MinuteIndex,
        meta: PayloadMeta,
        expected_version: Option<u32>,
    ) -> Result<Slot> {
        let symbol_id = self.resolve_symbol(symbol)?;
        Ok(self.enrichment_ledger.write_slot(
            symbol_id,
            minute_idx,
            EnrichmentSlotKind::Greeks,
            meta,
            expected_version,
        )?)
    }

    pub fn set_aggregate_ref(
        &self,
        symbol: &str,
        minute_idx: MinuteIndex,
        window: AggregateWindowKind,
        meta: PayloadMeta,
        expected_version: Option<u32>,
    ) -> Result<Slot> {
        let symbol_id = self.resolve_symbol(symbol)?;
        Ok(self.enrichment_ledger.write_slot(
            symbol_id,
            minute_idx,
            EnrichmentSlotKind::Aggregate(window),
            meta,
            expected_version,
        )?)
    }

    pub fn mark_pending(
        &self,
        symbol: &str,
        minute_idx: MinuteIndex,
        slot: SlotKind,
    ) -> Result<PendingHandle> {
        let symbol_id = self.resolve_symbol(symbol)?;
        match slot {
            SlotKind::Trade(kind) => {
                self.trade_ledger
                    .mark_pending(symbol_id, minute_idx, kind)?;
            }
            SlotKind::Enrichment(kind) => {
                self.enrichment_ledger
                    .mark_pending(symbol_id, minute_idx, kind)?;
            }
        }
        Ok(PendingHandle {
            symbol_id,
            minute_idx,
            slot,
        })
    }

    pub fn clear_slot(
        &self,
        symbol: &str,
        minute_idx: MinuteIndex,
        slot: SlotKind,
    ) -> Result<Slot> {
        let symbol_id = self.resolve_symbol(symbol)?;
        let slot_meta = match slot {
            SlotKind::Trade(kind) => self.trade_ledger.clear_slot(symbol_id, minute_idx, kind)?,
            SlotKind::Enrichment(kind) => self
                .enrichment_ledger
                .clear_slot(symbol_id, minute_idx, kind)?,
        };
        Ok(slot_meta)
    }

    pub fn get_trade_row(
        &self,
        symbol: &str,
        minute_idx: MinuteIndex,
    ) -> Result<crate::ledger::TradeWindowRow> {
        let symbol_id = self.resolve_symbol(symbol)?;
        self.trade_ledger
            .get_row(symbol_id, minute_idx)
            .map_err(LedgerError::from)
    }

    pub fn get_enrichment_row(
        &self,
        symbol: &str,
        minute_idx: MinuteIndex,
    ) -> Result<crate::ledger::EnrichmentWindowRow> {
        let symbol_id = self.resolve_symbol(symbol)?;
        self.enrichment_ledger
            .get_row(symbol_id, minute_idx)
            .map_err(LedgerError::from)
    }

    pub fn payload_stores(&self) -> MutexGuard<'_, PayloadStores> {
        self.payload_stores.lock()
    }

    pub fn slot_status_snapshot(&self) -> Result<LedgerSlotStatusSnapshot> {
        let symbol_ids: Vec<SymbolId> = {
            let map = self.symbol_map.read();
            map.iter().map(|(id, _)| id).collect()
        };
        let mut snapshot = LedgerSlotStatusSnapshot::default();
        for symbol_id in symbol_ids {
            self.trade_ledger
                .with_symbol_rows(symbol_id, |rows| {
                    for row in rows {
                        for kind in TradeSlotKind::ALL {
                            let slot = row.slot(kind.index());
                            snapshot.record_trade(kind, slot.status);
                        }
                    }
                })
                .map_err(LedgerError::from)?;
            self.enrichment_ledger
                .with_symbol_rows(symbol_id, |rows| {
                    for row in rows {
                        for kind in EnrichmentSlotKind::ALL {
                            let slot = row.slot(kind.index());
                            snapshot.record_enrichment(kind, slot.status);
                        }
                    }
                })
                .map_err(LedgerError::from)?;
        }
        Ok(snapshot)
    }

    pub fn config(&self) -> &LedgerConfig {
        &self.config
    }

    pub fn minute_idx_for_timestamp(&self, timestamp: i64) -> Option<MinuteIndex> {
        self.window_space.minute_idx_for_timestamp(timestamp)
    }

    pub fn next_unfilled_window(
        &self,
        symbol: &str,
        start_idx: MinuteIndex,
        slot: SlotKind,
    ) -> Result<Option<MinuteIndex>> {
        let symbol_id = self.resolve_symbol(symbol)?;
        let next = match slot {
            SlotKind::Trade(kind) => self
                .trade_ledger
                .next_unfilled_window(symbol_id, start_idx, kind)?,
            SlotKind::Enrichment(kind) => self
                .enrichment_ledger
                .next_unfilled_window(symbol_id, start_idx, kind)?,
        };
        Ok(next)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SlotStatusCounts {
    pub empty: usize,
    pub pending: usize,
    pub filled: usize,
    pub cleared: usize,
}

impl SlotStatusCounts {
    fn increment(&mut self, status: SlotStatus) {
        match status {
            SlotStatus::Empty => self.empty += 1,
            SlotStatus::Pending => self.pending += 1,
            SlotStatus::Filled => self.filled += 1,
            SlotStatus::Cleared => self.cleared += 1,
        }
    }

    pub fn total(&self) -> usize {
        self.empty + self.pending + self.filled + self.cleared
    }

    pub fn is_zero(&self) -> bool {
        self.total() == 0
    }
}

impl fmt::Display for SlotStatusCounts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "empty={}, pending={}, filled={}, cleared={}",
            self.empty, self.pending, self.filled, self.cleared
        )
    }
}

#[derive(Clone, Debug, Default)]
pub struct LedgerSlotStatusSnapshot {
    pub trade: BTreeMap<TradeSlotKind, SlotStatusCounts>,
    pub enrichment: BTreeMap<EnrichmentSlotKind, SlotStatusCounts>,
}

impl LedgerSlotStatusSnapshot {
    fn record_trade(&mut self, kind: TradeSlotKind, status: SlotStatus) {
        self.trade.entry(kind).or_default().increment(status);
    }

    fn record_enrichment(&mut self, kind: EnrichmentSlotKind, status: SlotStatus) {
        self.enrichment.entry(kind).or_default().increment(status);
    }

    pub fn is_empty(&self) -> bool {
        self.trade.values().all(SlotStatusCounts::is_zero)
            && self.enrichment.values().all(SlotStatusCounts::is_zero)
    }

    pub fn total_slots(&self) -> usize {
        self.trade
            .values()
            .map(SlotStatusCounts::total)
            .sum::<usize>()
            + self
                .enrichment
                .values()
                .map(SlotStatusCounts::total)
                .sum::<usize>()
    }
}

impl fmt::Display for LedgerSlotStatusSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            return write!(f, "ledger slot statuses: no symbols loaded");
        }
        writeln!(f, "ledger slot statuses:")?;
        if !self.trade.is_empty() {
            writeln!(f, "  trade refs:")?;
            for (kind, counts) in &self.trade {
                writeln!(f, "    {:>18}: {}", kind.label(), counts)?;
            }
        }
        if !self.enrichment.is_empty() {
            writeln!(f, "  enrichment refs:")?;
            for (kind, counts) in &self.enrichment {
                writeln!(f, "    {:>18}: {}", kind.label(), counts)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PendingHandle {
    pub symbol_id: SymbolId,
    pub minute_idx: MinuteIndex,
    pub slot: SlotKind,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        payload::{PayloadMeta, PayloadType, SlotKind, TradeSlotKind},
        window::WindowSpace,
    };
    use tempfile::tempdir;

    #[test]
    fn controller_sets_and_reads_slots() {
        let dir = tempdir().unwrap();
        let window = WindowSpace::standard(1_600_000_000);
        let mut config = LedgerConfig::new(dir.path().to_path_buf(), window);
        config.max_symbols = 32;

        let controller = LedgerController::bootstrap(config).unwrap();

        let meta = PayloadMeta::new(PayloadType::Trade, 1, 5, 123);
        controller
            .set_option_trade_ref("AAPL", 0, meta, None)
            .expect("write slot");

        let row = controller.get_trade_row("AAPL", 0).expect("fetch row");
        assert_eq!(row.option_trade_ref.payload_id, 1);

        controller
            .mark_pending("AAPL", 1, SlotKind::Trade(TradeSlotKind::OptionQuote))
            .expect("mark pending");
    }
}
