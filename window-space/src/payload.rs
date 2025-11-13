use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Identifies which mapping table owns a payload.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum PayloadType {
    #[default]
    Unknown = 0,
    RfRate = 1,
    Trade = 2,
    Quote = 3,
    Aggressor = 4,
    Greeks = 5,
}

/// Logical status of a slot inside a window row.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SlotStatus {
    #[default]
    Empty = 0,
    Pending = 1,
    Filled = 2,
    Cleared = 3,
}

/// Metadata recorded for each slot column.
#[repr(C)]
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Slot {
    pub payload_type: PayloadType,
    pub status: SlotStatus,
    pub payload_id: u32,
    pub version: u32,
    pub checksum: u32,
    pub last_updated_ns: i64,
}

impl Default for Slot {
    fn default() -> Self {
        Self {
            payload_type: PayloadType::Unknown,
            status: SlotStatus::Empty,
            payload_id: 0,
            version: 0,
            checksum: 0,
            last_updated_ns: 0,
        }
    }
}

impl Slot {
    /// Bumps the version while clearing metadata.
    pub fn clear(&mut self) {
        self.payload_type = PayloadType::Unknown;
        self.status = SlotStatus::Cleared;
        self.payload_id = 0;
        self.checksum = 0;
        self.version = self.version.wrapping_add(1);
        self.last_updated_ns = current_time_ns();
    }

    pub fn mark_pending(&mut self) {
        self.status = SlotStatus::Pending;
        self.last_updated_ns = current_time_ns();
    }

    pub fn overwrite(&mut self, meta: &PayloadMeta) {
        self.payload_type = meta.payload_type;
        self.payload_id = meta.payload_id;
        self.version = meta.version;
        self.checksum = meta.checksum;
        self.status = SlotStatus::Filled;
        self.last_updated_ns = meta.last_updated_ns.unwrap_or_else(current_time_ns);
    }
}

/// Metadata that writers supply to ledger setters.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct PayloadMeta {
    pub payload_type: PayloadType,
    pub payload_id: u32,
    pub version: u32,
    pub checksum: u32,
    pub last_updated_ns: Option<i64>,
}

impl PayloadMeta {
    pub fn new(payload_type: PayloadType, payload_id: u32, version: u32, checksum: u32) -> Self {
        Self {
            payload_type,
            payload_id,
            version,
            checksum,
            last_updated_ns: None,
        }
    }
}

/// Slot kinds for the trade ledger.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TradeSlotKind {
    RfRate,
    OptionTrade,
    OptionQuote,
    UnderlyingTrade,
    UnderlyingQuote,
    OptionAggressor,
    UnderlyingAggressor,
}

impl TradeSlotKind {
    pub const ALL: [Self; 7] = [
        TradeSlotKind::RfRate,
        TradeSlotKind::OptionTrade,
        TradeSlotKind::OptionQuote,
        TradeSlotKind::UnderlyingTrade,
        TradeSlotKind::UnderlyingQuote,
        TradeSlotKind::OptionAggressor,
        TradeSlotKind::UnderlyingAggressor,
    ];

    pub fn index(&self) -> usize {
        match self {
            TradeSlotKind::RfRate => 0,
            TradeSlotKind::OptionTrade => 1,
            TradeSlotKind::OptionQuote => 2,
            TradeSlotKind::UnderlyingTrade => 3,
            TradeSlotKind::UnderlyingQuote => 4,
            TradeSlotKind::OptionAggressor => 5,
            TradeSlotKind::UnderlyingAggressor => 6,
        }
    }

    pub fn payload_type(&self) -> PayloadType {
        match self {
            TradeSlotKind::RfRate => PayloadType::RfRate,
            TradeSlotKind::OptionTrade => PayloadType::Trade,
            TradeSlotKind::OptionQuote => PayloadType::Quote,
            TradeSlotKind::UnderlyingTrade => PayloadType::Trade,
            TradeSlotKind::UnderlyingQuote => PayloadType::Quote,
            TradeSlotKind::OptionAggressor => PayloadType::Aggressor,
            TradeSlotKind::UnderlyingAggressor => PayloadType::Aggressor,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            TradeSlotKind::RfRate => "rf_rate",
            TradeSlotKind::OptionTrade => "option_trade",
            TradeSlotKind::OptionQuote => "option_quote",
            TradeSlotKind::UnderlyingTrade => "underlying_trade",
            TradeSlotKind::UnderlyingQuote => "underlying_quote",
            TradeSlotKind::OptionAggressor => "option_aggressor",
            TradeSlotKind::UnderlyingAggressor => "underlying_aggressor",
        }
    }
}

/// Slot kinds for the enrichment ledger.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EnrichmentSlotKind {
    Greeks,
}

impl EnrichmentSlotKind {
    pub const ALL: [Self; 1] = [EnrichmentSlotKind::Greeks];

    pub fn index(&self) -> usize {
        match self {
            EnrichmentSlotKind::Greeks => 0,
        }
    }

    pub fn payload_type(&self) -> PayloadType {
        match self {
            EnrichmentSlotKind::Greeks => PayloadType::Greeks,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            EnrichmentSlotKind::Greeks => "greeks",
        }
    }
}

/// Unified representation when APIs need to accept both trade and enrichment slot names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotKind {
    Trade(TradeSlotKind),
    Enrichment(EnrichmentSlotKind),
}

fn current_time_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or_default()
}
