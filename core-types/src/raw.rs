// Copyright (c) James Kassemi, SC, US. All rights reserved.

use serde::{Deserialize, Serialize};

use crate::{
    types::{NbboState, Quality, Source},
    uid::{QuoteUid, TradeUid},
};

/// Minimal option trade record persisted in ledger payload artifacts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionTradeRecord {
    pub contract: String,
    pub trade_uid: TradeUid,
    pub contract_direction: char,
    pub strike_price: f64,
    pub underlying: String,
    pub trade_ts_ns: i64,
    pub participant_ts_ns: Option<i64>,
    pub price: f64,
    pub size: u32,
    pub conditions: Vec<i32>,
    pub exchange: i32,
    pub expiry_ts_ns: i64,
    pub source: Source,
    pub quality: Quality,
    pub watermark_ts_ns: i64,
}

/// Minimal underlying trade (equity) record for ledger payload artifacts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnderlyingTradeRecord {
    pub symbol: String,
    pub trade_uid: TradeUid,
    pub trade_ts_ns: i64,
    pub participant_ts_ns: Option<i64>,
    pub price: f64,
    pub size: u32,
    pub conditions: Vec<i32>,
    pub exchange: i32,
    pub trade_id: Option<String>,
    pub seq: Option<u64>,
    pub tape: Option<String>,
    pub correction: Option<i32>,
    pub trf_id: Option<String>,
    pub trf_ts_ns: Option<i64>,
    pub source: Source,
    pub quality: Quality,
    pub watermark_ts_ns: i64,
}

/// Minimal quote record persisted in ledger payload artifacts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuoteRecord {
    pub instrument_id: String,
    pub quote_uid: QuoteUid,
    pub quote_ts_ns: i64,
    pub bid: f64,
    pub ask: f64,
    pub bid_sz: u32,
    pub ask_sz: u32,
    pub state: NbboState,
    pub condition: Option<i32>,
    pub best_bid_venue: Option<i32>,
    pub best_ask_venue: Option<i32>,
    pub source: Source,
    pub quality: Quality,
    pub watermark_ts_ns: i64,
}
