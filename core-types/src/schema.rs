// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Arrow/Parquet schemas (stub).

use arrow::datatypes::{DataType, Field, Schema};

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
        // Add other fields as needed...
    ])
}

/// Placeholder for Nbbo schema.
pub fn nbbo_schema() -> Schema {
    Schema::new(vec![
        Field::new("instrument_id", DataType::Utf8, false),
        // Add other fields as needed...
    ])
}
