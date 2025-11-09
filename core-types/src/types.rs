// Copyright (c) James Kassemi, SC, US. All rights reserved.

use serde::{Deserialize, Serialize};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

// Common enums from spec
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Source { Flatfile, Rest, Ws }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Quality { Prelim, Enriched, Final }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Completeness { Complete, Partial, Unknown }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InstrumentType { Equity, Option }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggressorSide { Buyer, Seller, Unknown }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClassMethod { NbboTouch, NbboAtOrBeyond, TickRule, Unknown }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NbboState { Normal, Locked, Crossed }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TickSizeMethod { FromRules, InferredFromQuotes, DefaultFallback }

// Shared metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Watermark {
    pub watermark_ts_ns: i64,
    pub completeness: Completeness,
    pub hints: Option<String>,
}

// Schemas (abbreviated; full Arrow schemas would be built here)
pub fn option_trade_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("contract", DataType::Utf8, false),
        // ... add all fields from spec
    ]))
}

// Structs (abbreviated; implement full from spec)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionTrade {
    pub contract: String,
    // ... other fields
    pub source: Source,
    pub quality: Quality,
    pub watermark_ts_ns: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityTrade { /* similar */ }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Nbbo { /* similar */ }

// Traits
pub trait TradeLike {
    fn trade_ts_ns(&self) -> i64;
    // ... other methods
}

// DataBatch and related
#[derive(Debug)]
pub struct DataBatch<T> {
    pub rows: Vec<T>,
    pub meta: Arc<DataBatchMeta>,
}

#[derive(Debug)]
pub struct DataBatchMeta {
    pub source: Source,
    pub quality: Quality,
    pub watermark: Watermark,
    pub schema_version: u16,
}
