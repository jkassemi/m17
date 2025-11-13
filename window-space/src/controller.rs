use std::{collections::BTreeMap, fmt, path::PathBuf, sync::Arc, time::Duration};

use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::{
    config::WindowSpaceConfig,
    error::{Result, WindowSpaceError},
    ledger::{EnrichmentWindowSpace, TradeWindowSpace, WindowRow},
    mapping::PayloadStores,
    payload::{EnrichmentSlotKind, PayloadMeta, Slot, SlotKind, SlotStatus, TradeSlotKind},
    storage::WindowSpaceFileStats,
    symbol_map::{SymbolId, SymbolMap},
    window::{WindowIndex, WindowMeta, WindowSpace},
};

pub struct WindowSpaceController {
    config: WindowSpaceConfig,
    trade_window_space: Arc<TradeWindowSpace>,
    enrichment_window_space: Arc<EnrichmentWindowSpace>,
    symbol_map: RwLock<SymbolMap>,
    payload_stores: Mutex<PayloadStores>,
    window_space: Arc<WindowSpace>,
}

impl WindowSpaceController {
    pub fn bootstrap(config: WindowSpaceConfig) -> Result<(Self, WindowSpaceStorageReport)> {
        config.ensure_dirs()?;
        let symbol_map_path = config.symbol_map_path();
        let symbol_map = SymbolMap::load_or_init(&symbol_map_path, config.max_symbols)?;
        let window_space = Arc::new(config.window_space.clone());

        let trade_path = config.trade_ledger_path();
        let (trade_window_space_inner, trade_stats) = TradeWindowSpace::bootstrap(
            trade_path.as_path(),
            config.max_symbols,
            window_space.clone(),
        )?;
        let trade_window_space = Arc::new(trade_window_space_inner);
        let enrichment_path = config.enrichment_ledger_path();
        let (enrichment_window_space_inner, enrichment_stats) = EnrichmentWindowSpace::bootstrap(
            enrichment_path.as_path(),
            config.max_symbols,
            window_space.clone(),
        )?;
        let enrichment_window_space = Arc::new(enrichment_window_space_inner);

        for (symbol_id, _) in symbol_map.iter() {
            if trade_stats.created {
                trade_window_space
                    .ensure_symbol(symbol_id)
                    .expect("seed trade window space");
            } else {
                trade_window_space
                    .mark_symbol_loaded(symbol_id)
                    .expect("mark trade window space symbol");
            }
            if enrichment_stats.created {
                enrichment_window_space
                    .ensure_symbol(symbol_id)
                    .expect("seed enrichment window space");
            } else {
                enrichment_window_space
                    .mark_symbol_loaded(symbol_id)
                    .expect("mark enrichment window space symbol");
            }
        }

        let payload_stores = PayloadStores::load(config.state_dir())?;

        let report = WindowSpaceStorageReport::from_stats(trade_stats, enrichment_stats);

        Ok((
            Self {
                config,
                trade_window_space,
                enrichment_window_space,
                symbol_map: RwLock::new(symbol_map),
                payload_stores: Mutex::new(payload_stores),
                window_space,
            },
            report,
        ))
    }

    pub fn trade_window_space(&self) -> Arc<TradeWindowSpace> {
        Arc::clone(&self.trade_window_space)
    }

    pub fn enrichment_window_space(&self) -> Arc<EnrichmentWindowSpace> {
        Arc::clone(&self.enrichment_window_space)
    }

    #[deprecated(
        since = "0.1.0",
        note = "trade_ledger() has been renamed to trade_window_space(); update call sites to window_space::WindowSpaceController::trade_window_space"
    )]
    pub fn trade_ledger(&self) -> Arc<TradeWindowSpace> {
        self.trade_window_space()
    }

    #[deprecated(
        since = "0.1.0",
        note = "enrichment_ledger() has been renamed to enrichment_window_space(); update call sites to window_space::WindowSpaceController::enrichment_window_space"
    )]
    pub fn enrichment_ledger(&self) -> Arc<EnrichmentWindowSpace> {
        self.enrichment_window_space()
    }

    pub fn resolve_symbol(&self, symbol: &str) -> Result<SymbolId> {
        let mut map = self.symbol_map.write();
        let symbol_id = map.resolve_or_insert(symbol)?;
        map.persist()?;
        self.trade_window_space
            .ensure_symbol(symbol_id)
            .map_err(WindowSpaceError::from)?;
        self.enrichment_window_space
            .ensure_symbol(symbol_id)
            .map_err(WindowSpaceError::from)?;
        Ok(symbol_id)
    }

    pub fn set_rf_rate_ref(
        &self,
        symbol: &str,
        window_idx: WindowIndex,
        meta: PayloadMeta,
        expected_version: Option<u32>,
    ) -> Result<Slot> {
        let symbol_id = self.resolve_symbol(symbol)?;
        Ok(self.trade_window_space.write_slot(
            symbol_id,
            window_idx,
            TradeSlotKind::RfRate,
            meta,
            expected_version,
        )?)
    }

    pub fn set_option_trade_ref(
        &self,
        symbol: &str,
        window_idx: WindowIndex,
        meta: PayloadMeta,
        expected_version: Option<u32>,
    ) -> Result<Slot> {
        let symbol_id = self.resolve_symbol(symbol)?;
        Ok(self.trade_window_space.write_slot(
            symbol_id,
            window_idx,
            TradeSlotKind::OptionTrade,
            meta,
            expected_version,
        )?)
    }

    pub fn set_option_quote_ref(
        &self,
        symbol: &str,
        window_idx: WindowIndex,
        meta: PayloadMeta,
        expected_version: Option<u32>,
    ) -> Result<Slot> {
        let symbol_id = self.resolve_symbol(symbol)?;
        Ok(self.trade_window_space.write_slot(
            symbol_id,
            window_idx,
            TradeSlotKind::OptionQuote,
            meta,
            expected_version,
        )?)
    }

    pub fn set_underlying_trade_ref(
        &self,
        symbol: &str,
        window_idx: WindowIndex,
        meta: PayloadMeta,
        expected_version: Option<u32>,
    ) -> Result<Slot> {
        let symbol_id = self.resolve_symbol(symbol)?;
        Ok(self.trade_window_space.write_slot(
            symbol_id,
            window_idx,
            TradeSlotKind::UnderlyingTrade,
            meta,
            expected_version,
        )?)
    }

    pub fn set_underlying_quote_ref(
        &self,
        symbol: &str,
        window_idx: WindowIndex,
        meta: PayloadMeta,
        expected_version: Option<u32>,
    ) -> Result<Slot> {
        let symbol_id = self.resolve_symbol(symbol)?;
        Ok(self.trade_window_space.write_slot(
            symbol_id,
            window_idx,
            TradeSlotKind::UnderlyingQuote,
            meta,
            expected_version,
        )?)
    }

    pub fn set_option_aggressor_ref(
        &self,
        symbol: &str,
        window_idx: WindowIndex,
        meta: PayloadMeta,
        expected_version: Option<u32>,
    ) -> Result<Slot> {
        let symbol_id = self.resolve_symbol(symbol)?;
        Ok(self.trade_window_space.write_slot(
            symbol_id,
            window_idx,
            TradeSlotKind::OptionAggressor,
            meta,
            expected_version,
        )?)
    }

    pub fn set_underlying_aggressor_ref(
        &self,
        symbol: &str,
        window_idx: WindowIndex,
        meta: PayloadMeta,
        expected_version: Option<u32>,
    ) -> Result<Slot> {
        let symbol_id = self.resolve_symbol(symbol)?;
        Ok(self.trade_window_space.write_slot(
            symbol_id,
            window_idx,
            TradeSlotKind::UnderlyingAggressor,
            meta,
            expected_version,
        )?)
    }

    pub fn set_greeks_ref(
        &self,
        symbol: &str,
        window_idx: WindowIndex,
        meta: PayloadMeta,
        expected_version: Option<u32>,
    ) -> Result<Slot> {
        let symbol_id = self.resolve_symbol(symbol)?;
        Ok(self.enrichment_window_space.write_slot(
            symbol_id,
            window_idx,
            EnrichmentSlotKind::Greeks,
            meta,
            expected_version,
        )?)
    }

    pub fn mark_pending(
        &self,
        symbol: &str,
        window_idx: WindowIndex,
        slot: SlotKind,
    ) -> Result<PendingHandle> {
        let symbol_id = self.resolve_symbol(symbol)?;
        match slot {
            SlotKind::Trade(kind) => {
                self.trade_window_space
                    .mark_pending(symbol_id, window_idx, kind)?;
            }
            SlotKind::Enrichment(kind) => {
                self.enrichment_window_space
                    .mark_pending(symbol_id, window_idx, kind)?;
            }
        }
        Ok(PendingHandle {
            symbol_id,
            window_idx,
            slot,
        })
    }

    pub fn clear_slot(
        &self,
        symbol: &str,
        window_idx: WindowIndex,
        slot: SlotKind,
    ) -> Result<Slot> {
        let symbol_id = self.resolve_symbol(symbol)?;
        let slot_meta = match slot {
            SlotKind::Trade(kind) => self
                .trade_window_space
                .clear_slot(symbol_id, window_idx, kind)?,
            SlotKind::Enrichment(kind) => self
                .enrichment_window_space
                .clear_slot(symbol_id, window_idx, kind)?,
        };
        Ok(slot_meta)
    }

    pub fn get_trade_row(
        &self,
        symbol: &str,
        window_idx: WindowIndex,
    ) -> Result<crate::ledger::TradeWindowRow> {
        let symbol_id = self.resolve_symbol(symbol)?;
        self.trade_window_space
            .get_row(symbol_id, window_idx)
            .map_err(WindowSpaceError::from)
    }

    pub fn get_enrichment_row(
        &self,
        symbol: &str,
        window_idx: WindowIndex,
    ) -> Result<crate::ledger::EnrichmentWindowRow> {
        let symbol_id = self.resolve_symbol(symbol)?;
        self.enrichment_window_space
            .get_row(symbol_id, window_idx)
            .map_err(WindowSpaceError::from)
    }

    pub fn payload_stores(&self) -> MutexGuard<'_, PayloadStores> {
        self.payload_stores.lock()
    }

    pub fn slot_status_snapshot(&self) -> Result<WindowSpaceSlotStatusSnapshot> {
        let symbol_ids: Vec<SymbolId> = {
            let map = self.symbol_map.read();
            map.iter().map(|(id, _)| id).collect()
        };
        let mut snapshot = WindowSpaceSlotStatusSnapshot::default();
        for symbol_id in symbol_ids {
            self.trade_window_space
                .with_symbol_rows(symbol_id, |rows| {
                    for row in rows {
                        for kind in TradeSlotKind::ALL {
                            let slot = row.slot(kind.index());
                            snapshot.record_trade(kind, slot.status);
                        }
                    }
                })
                .map_err(WindowSpaceError::from)?;
            self.enrichment_window_space
                .with_symbol_rows(symbol_id, |rows| {
                    for row in rows {
                        for kind in EnrichmentSlotKind::ALL {
                            let slot = row.slot(kind.index());
                            snapshot.record_enrichment(kind, slot.status);
                        }
                    }
                })
                .map_err(WindowSpaceError::from)?;
        }
        Ok(snapshot)
    }

    pub fn config(&self) -> &WindowSpaceConfig {
        &self.config
    }

    pub fn window_idx_for_timestamp(&self, timestamp: i64) -> Option<WindowIndex> {
        self.window_space.window_idx_for_timestamp(timestamp)
    }

    pub fn window_meta(&self, window_idx: WindowIndex) -> Option<WindowMeta> {
        self.window_space.window(window_idx).copied()
    }

    #[deprecated(since = "0.1.0", note = "Use window_idx_for_timestamp() instead")]
    pub fn minute_idx_for_timestamp(&self, timestamp: i64) -> Option<WindowIndex> {
        self.window_idx_for_timestamp(timestamp)
    }

    pub fn next_unfilled_window(
        &self,
        symbol: &str,
        start_idx: WindowIndex,
        slot: SlotKind,
    ) -> Result<Option<WindowIndex>> {
        let symbol_id = self.resolve_symbol(symbol)?;
        let next = match slot {
            SlotKind::Trade(kind) => self
                .trade_window_space
                .next_unfilled_window(symbol_id, start_idx, kind)?,
            SlotKind::Enrichment(kind) => self
                .enrichment_window_space
                .next_unfilled_window(symbol_id, start_idx, kind)?,
        };
        Ok(next)
    }
}

#[derive(Clone, Debug)]
pub struct WindowSpaceStorageReport {
    pub trade: StorageSummary,
    pub enrichment: StorageSummary,
}

impl WindowSpaceStorageReport {
    fn from_stats(trade: WindowSpaceFileStats, enrichment: WindowSpaceFileStats) -> Self {
        Self {
            trade: StorageSummary::from(trade),
            enrichment: StorageSummary::from(enrichment),
        }
    }
}

#[derive(Clone, Debug)]
pub struct StorageSummary {
    pub path: PathBuf,
    pub file_size: u64,
    pub created_at_s: i64,
    pub created: bool,
    pub creation_duration: Option<Duration>,
}

impl From<WindowSpaceFileStats> for StorageSummary {
    fn from(value: WindowSpaceFileStats) -> Self {
        Self {
            path: value.path,
            file_size: value.file_size,
            created_at_s: value.created_at_s,
            created: value.created,
            creation_duration: value.creation_duration,
        }
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
pub struct WindowSpaceSlotStatusSnapshot {
    pub trade: BTreeMap<TradeSlotKind, SlotStatusCounts>,
    pub enrichment: BTreeMap<EnrichmentSlotKind, SlotStatusCounts>,
}

impl WindowSpaceSlotStatusSnapshot {
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

impl fmt::Display for WindowSpaceSlotStatusSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            return write!(f, "window space slot statuses: no symbols loaded");
        }
        writeln!(f, "window space slot statuses:")?;
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
    pub window_idx: WindowIndex,
    pub slot: SlotKind,
}

#[deprecated(
    since = "0.1.0",
    note = "LedgerController has been renamed to WindowSpaceController; update imports to window_space::WindowSpaceController"
)]
pub type LedgerController = WindowSpaceController;

#[deprecated(
    since = "0.1.0",
    note = "LedgerStorageReport has been renamed to WindowSpaceStorageReport; update imports to window_space::WindowSpaceStorageReport"
)]
pub type LedgerStorageReport = WindowSpaceStorageReport;

#[deprecated(
    since = "0.1.0",
    note = "LedgerSlotStatusSnapshot has been renamed to WindowSpaceSlotStatusSnapshot; update imports to window_space::WindowSpaceSlotStatusSnapshot"
)]
pub type LedgerSlotStatusSnapshot = WindowSpaceSlotStatusSnapshot;

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
        let mut config = WindowSpaceConfig::new(dir.path().to_path_buf(), window);
        config.max_symbols = 32;

        let (controller, _) = WindowSpaceController::bootstrap(config).unwrap();

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
