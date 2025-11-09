// Copyright (c) James Kassemi, SC, US. All rights reserved.

use serde::{Deserialize, Serialize};

/// Common enums as per spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Source {
    Flatfile,
    Rest,
    Ws,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Quality {
    Prelim,
    Enriched,
    Final,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Completeness {
    Complete,
    Partial,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InstrumentType {
    Equity,
    Option,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggressorSide {
    Buyer,
    Seller,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClassMethod {
    NbboTouch,
    NbboAtOrBeyond,
    TickRule,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NbboState {
    Normal,
    Locked,
    Crossed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TickSizeMethod {
    FromRules,
    InferredFromQuotes,
    DefaultFallback,
}

/// Shared metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Watermark {
    pub watermark_ts_ns: i64,
    pub completeness: Completeness,
    pub hints: Option<String>,
}

/// Option trade row (stub).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionTrade {
    pub contract: String,
    pub contract_direction: char,
    pub strike_price: f64,
    pub underlying: String,
    pub trade_ts_ns: i64,
    pub price: f64,
    pub size: u32,
    pub conditions: Vec<i32>,
    pub exchange: i32,
    pub expiry_ts_ns: i64,
    pub aggressor_side: AggressorSide,
    pub class_method: ClassMethod,
    pub aggressor_offset_mid_bp: Option<i32>,
    pub aggressor_offset_touch_ticks: Option<i32>,
    pub nbbo_bid: Option<f64>,
    pub nbbo_ask: Option<f64>,
    pub nbbo_bid_sz: Option<u32>,
    pub nbbo_ask_sz: Option<u32>,
    pub nbbo_ts_ns: Option<i64>,
    pub nbbo_age_us: Option<u32>,
    pub nbbo_state: Option<NbboState>,
    pub tick_size_used: Option<f64>,
    pub delta: Option<f64>,
    pub gamma: Option<f64>,
    pub vega: Option<f64>,
    pub theta: Option<f64>,
    pub iv: Option<f64>,
    pub greeks_flags: u32,
    pub source: Source,
    pub quality: Quality,
    pub watermark_ts_ns: i64,
}

/// Equity trade row (stub).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityTrade {
    pub symbol: String,
    pub trade_ts_ns: i64,
    pub price: f64,
    pub size: u32,
    pub conditions: Vec<i32>,
    pub exchange: i32,
    pub aggressor_side: AggressorSide,
    pub class_method: ClassMethod,
    pub aggressor_offset_mid_bp: Option<i32>,
    pub aggressor_offset_touch_ticks: Option<i32>,
    pub nbbo_bid: Option<f64>,
    pub nbbo_ask: Option<f64>,
    pub nbbo_bid_sz: Option<u32>,
    pub nbbo_ask_sz: Option<u32>,
    pub nbbo_ts_ns: Option<i64>,
    pub nbbo_age_us: Option<u32>,
    pub nbbo_state: Option<NbboState>,
    pub tick_size_used: Option<f64>,
    pub source: Source,
    pub quality: Quality,
    pub watermark_ts_ns: i64,
}

/// NBBO row (stub).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Nbbo {
    pub instrument_id: String,
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

/// QueryScope (stub).
#[derive(Debug, Clone)]
pub struct QueryScope {
    pub instruments: Vec<String>, // Placeholder for InstrumentSet
    pub time_range: (i64, i64),   // Placeholder for TimeRange
    pub mode: String,             // Realtime, T1, Historical
    pub quality_target: Quality,
}

/// DataBatch (stub).
#[derive(Debug, Clone)]
pub struct DataBatch<T> {
    pub rows: Vec<T>,
    pub meta: DataBatchMeta,
}

#[derive(Debug, Clone)]
pub struct DataBatchMeta {
    pub source: Source,
    pub quality: Quality,
    pub watermark: Watermark,
    pub schema_version: u16,
}

/// ClassParams (stub).
#[derive(Debug, Clone)]
pub struct ClassParams {
    pub use_tick_rule_fallback: bool,
    pub epsilon_price: f64,
    pub allowed_lateness_ms: u32,
}

/// StalenessParams (stub).
#[derive(Debug, Clone)]
pub struct StalenessParams {
    pub max_staleness_us: u32,
}
