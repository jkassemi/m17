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
    pub trade_id: Option<String>,
    pub seq: Option<u64>,
    pub participant_ts_ns: Option<i64>,
    pub tape: Option<String>,
    pub correction: Option<i32>,
    pub trf_id: Option<String>,
    pub trf_ts_ns: Option<i64>,
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

/// Aggregated feature row per window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationRow {
    pub symbol: String,
    pub window: String,
    pub window_start_ns: i64,
    pub window_end_ns: i64,
    pub underlying_price_open: Option<f64>,
    pub underlying_price_high: Option<f64>,
    pub underlying_price_low: Option<f64>,
    pub underlying_price_close: Option<f64>,
    pub underlying_dollar_value_total: Option<f64>,
    pub underlying_dollar_value_minimum: Option<f64>,
    pub underlying_dollar_value_maximum: Option<f64>,
    pub underlying_dollar_value_mean: Option<f64>,
    pub underlying_dollar_value_stddev: Option<f64>,
    pub underlying_dollar_value_skew: Option<f64>,
    pub underlying_dollar_value_kurtosis: Option<f64>,
    pub underlying_dollar_value_iqr: Option<f64>,
    pub underlying_dollar_value_mad: Option<f64>,
    pub underlying_dollar_value_cv: Option<f64>,
    pub underlying_dollar_value_mode: Option<f64>,
    pub underlying_dollar_value_bc: Option<f64>,
    pub underlying_dollar_value_dip_pval: Option<f64>,
    pub underlying_dollar_value_kde_peaks: Option<f64>,
    pub puts_below_intrinsic_pct: Option<f64>,
    pub puts_above_intrinsic_pct: Option<f64>,
    pub puts_dadvv_total: Option<f64>,
    pub puts_dadvv_minimum: Option<f64>,
    pub puts_dadvv_maximum: Option<f64>,
    pub puts_dadvv_mean: Option<f64>,
    pub puts_dadvv_stddev: Option<f64>,
    pub puts_dadvv_skew: Option<f64>,
    pub puts_dadvv_kurtosis: Option<f64>,
    pub puts_dadvv_iqr: Option<f64>,
    pub puts_dadvv_mad: Option<f64>,
    pub puts_dadvv_cv: Option<f64>,
    pub puts_dadvv_mode: Option<f64>,
    pub puts_dadvv_bc: Option<f64>,
    pub puts_dadvv_dip_pval: Option<f64>,
    pub puts_dadvv_kde_peaks: Option<f64>,
    pub puts_gadvv_total: Option<f64>,
    pub puts_gadvv_minimum: Option<f64>,
    pub puts_gadvv_maximum: Option<f64>,
    pub puts_gadvv_mean: Option<f64>,
    pub puts_gadvv_stddev: Option<f64>,
    pub puts_gadvv_skew: Option<f64>,
    pub puts_gadvv_kurtosis: Option<f64>,
    pub puts_gadvv_iqr: Option<f64>,
    pub puts_gadvv_mad: Option<f64>,
    pub puts_gadvv_cv: Option<f64>,
    pub puts_gadvv_mode: Option<f64>,
    pub puts_gadvv_bc: Option<f64>,
    pub puts_gadvv_dip_pval: Option<f64>,
    pub puts_gadvv_kde_peaks: Option<f64>,
    pub calls_dollar_value: Option<f64>,
    pub calls_above_intrinsic_pct: Option<f64>,
    pub calls_dadvv_total: Option<f64>,
    pub calls_dadvv_minimum: Option<f64>,
    pub calls_dadvv_maximum: Option<f64>,
    pub calls_dadvv_mean: Option<f64>,
    pub calls_dadvv_stddev: Option<f64>,
    pub calls_dadvv_skew: Option<f64>,
    pub calls_dadvv_kurtosis: Option<f64>,
    pub calls_dadvv_iqr: Option<f64>,
    pub calls_dadvv_mad: Option<f64>,
    pub calls_dadvv_cv: Option<f64>,
    pub calls_dadvv_mode: Option<f64>,
    pub calls_dadvv_bc: Option<f64>,
    pub calls_dadvv_dip_pval: Option<f64>,
    pub calls_dadvv_kde_peaks: Option<f64>,
    pub calls_gadvv_total: Option<f64>,
    pub calls_gadvv_minimum: Option<f64>,
    pub calls_gadvv_q1: Option<f64>,
    pub calls_gadvv_q2: Option<f64>,
    pub calls_gadvv_q3: Option<f64>,
    pub calls_gadvv_maximum: Option<f64>,
    pub calls_gadvv_mean: Option<f64>,
    pub calls_gadvv_stddev: Option<f64>,
    pub calls_gadvv_skew: Option<f64>,
    pub calls_gadvv_kurtosis: Option<f64>,
    pub calls_gadvv_iqr: Option<f64>,
    pub calls_gadvv_mad: Option<f64>,
    pub calls_gadvv_cv: Option<f64>,
    pub calls_gadvv_mode: Option<f64>,
    pub calls_gadvv_bc: Option<f64>,
    pub calls_gadvv_dip_pval: Option<f64>,
    pub calls_gadvv_kde_peaks: Option<f64>,
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

/// Trait for trade-like objects that can be classified.
pub trait TradeLike {
    fn instrument_id(&self) -> &str;
    fn trade_ts_ns(&self) -> i64;
    fn price(&self) -> f64;
    fn set_aggressor_side(&mut self, side: AggressorSide);
    fn set_class_method(&mut self, method: ClassMethod);
    fn set_aggressor_offset_mid_bp(&mut self, offset: Option<i32>);
    fn set_nbbo_snapshot(
        &mut self,
        bid: Option<f64>,
        ask: Option<f64>,
        bid_sz: Option<u32>,
        ask_sz: Option<u32>,
        ts_ns: Option<i64>,
        age_us: Option<u32>,
        state: Option<NbboState>,
    );
    fn set_tick_size_used(&mut self, size: Option<f64>);
}

impl TradeLike for OptionTrade {
    fn instrument_id(&self) -> &str {
        &self.contract
    }
    fn trade_ts_ns(&self) -> i64 {
        self.trade_ts_ns
    }
    fn price(&self) -> f64 {
        self.price
    }
    fn set_aggressor_side(&mut self, side: AggressorSide) {
        self.aggressor_side = side;
    }
    fn set_class_method(&mut self, method: ClassMethod) {
        self.class_method = method;
    }
    fn set_aggressor_offset_mid_bp(&mut self, offset: Option<i32>) {
        self.aggressor_offset_mid_bp = offset;
    }
    fn set_nbbo_snapshot(
        &mut self,
        bid: Option<f64>,
        ask: Option<f64>,
        bid_sz: Option<u32>,
        ask_sz: Option<u32>,
        ts_ns: Option<i64>,
        age_us: Option<u32>,
        state: Option<NbboState>,
    ) {
        self.nbbo_bid = bid;
        self.nbbo_ask = ask;
        self.nbbo_bid_sz = bid_sz;
        self.nbbo_ask_sz = ask_sz;
        self.nbbo_ts_ns = ts_ns;
        self.nbbo_age_us = age_us;
        self.nbbo_state = state;
    }
    fn set_tick_size_used(&mut self, size: Option<f64>) {
        self.tick_size_used = size;
    }
}

impl TradeLike for EquityTrade {
    fn instrument_id(&self) -> &str {
        &self.symbol
    }
    fn trade_ts_ns(&self) -> i64 {
        self.trade_ts_ns
    }
    fn price(&self) -> f64 {
        self.price
    }
    fn set_aggressor_side(&mut self, side: AggressorSide) {
        self.aggressor_side = side;
    }
    fn set_class_method(&mut self, method: ClassMethod) {
        self.class_method = method;
    }
    fn set_aggressor_offset_mid_bp(&mut self, offset: Option<i32>) {
        self.aggressor_offset_mid_bp = offset;
    }
    fn set_nbbo_snapshot(
        &mut self,
        bid: Option<f64>,
        ask: Option<f64>,
        bid_sz: Option<u32>,
        ask_sz: Option<u32>,
        ts_ns: Option<i64>,
        age_us: Option<u32>,
        state: Option<NbboState>,
    ) {
        self.nbbo_bid = bid;
        self.nbbo_ask = ask;
        self.nbbo_bid_sz = bid_sz;
        self.nbbo_ask_sz = ask_sz;
        self.nbbo_ts_ns = ts_ns;
        self.nbbo_age_us = age_us;
        self.nbbo_state = state;
    }
    fn set_tick_size_used(&mut self, size: Option<f64>) {
        self.tick_size_used = size;
    }
}
