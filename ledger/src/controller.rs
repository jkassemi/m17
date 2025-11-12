use std::sync::Arc;

use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::{
    config::LedgerConfig,
    error::{LedgerError, Result},
    ledger::{EnrichmentLedger, TradeLedger},
    mapping::PayloadStores,
    payload::{
        AggregateWindowKind, EnrichmentSlotKind, PayloadMeta, Slot, SlotKind, TradeSlotKind,
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

    pub fn set_trade_ref(
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
            TradeSlotKind::Trade,
            meta,
            expected_version,
        )?)
    }

    pub fn set_quote_ref(
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
            TradeSlotKind::Quote,
            meta,
            expected_version,
        )?)
    }

    pub fn set_aggressor_ref(
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
            TradeSlotKind::Aggressor,
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

    pub fn config(&self) -> &LedgerConfig {
        &self.config
    }

    pub fn minute_idx_for_timestamp(&self, timestamp: i64) -> Option<MinuteIndex> {
        self.window_space.minute_idx_for_timestamp(timestamp)
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
            .set_trade_ref("AAPL", 0, meta, None)
            .expect("write slot");

        let row = controller.get_trade_row("AAPL", 0).expect("fetch row");
        assert_eq!(row.trade_ref.payload_id, 1);

        controller
            .mark_pending("AAPL", 1, SlotKind::Trade(TradeSlotKind::Quote))
            .expect("mark pending");
    }
}
