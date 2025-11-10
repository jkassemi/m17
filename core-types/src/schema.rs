// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Arrow/Parquet schemas (stub).

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Placeholder for OptionTrade schema.
pub fn option_trade_schema() -> Schema {
    Schema::new(vec![
        Field::new("contract", DataType::Utf8, false),
        // Add other fields as needed...
    ])
}

/// Placeholder for EquityTrade schema.
pub fn equity_trade_schema() -> Schema {
    Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("trade_ts_ns", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("size", DataType::UInt32, false),
        Field::new("conditions", DataType::List(Arc::new(Field::new("item", DataType::Int32, true))), true),
        Field::new("exchange", DataType::Int32, false),
        Field::new("aggressor_side", DataType::Utf8, false),
        Field::new("class_method", DataType::Utf8, false),
        Field::new("aggressor_offset_mid_bp", DataType::Int32, true),
        Field::new("aggressor_offset_touch_ticks", DataType::Int32, true),
        Field::new("nbbo_bid", DataType::Float64, true),
        Field::new("nbbo_ask", DataType::Float64, true),
        Field::new("nbbo_bid_sz", DataType::UInt32, true),
        Field::new("nbbo_ask_sz", DataType::UInt32, true),
        Field::new("nbbo_ts_ns", DataType::Int64, true),
        Field::new("nbbo_age_us", DataType::UInt32, true),
        Field::new("nbbo_state", DataType::Utf8, true),
        Field::new("tick_size_used", DataType::Float64, true),
        Field::new("source", DataType::Utf8, false),
        Field::new("quality", DataType::Utf8, false),
        Field::new("watermark_ts_ns", DataType::Int64, false),
        Field::new("trade_id", DataType::Utf8, true),
        Field::new("seq", DataType::UInt64, true),
        Field::new("participant_ts_ns", DataType::Int64, true),
        Field::new("tape", DataType::Utf8, true),
        Field::new("correction", DataType::Int32, true),
        Field::new("trf_id", DataType::Utf8, true),
        Field::new("trf_ts_ns", DataType::Int64, true),
    ])
}

/// Placeholder for Nbbo schema.
pub fn nbbo_schema() -> Schema {
    Schema::new(vec![
        Field::new("instrument_id", DataType::Utf8, false),
        // Add other fields as needed...
    ])
}
