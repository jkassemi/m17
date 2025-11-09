// Copyright (c) James Kassemi, SC, US. All rights reserved.

use prometheus::{Encoder, Histogram, IntCounter, TextEncoder};
use lazy_static::lazy_static;

lazy_static! {
    pub static ref NBBO_AGE_HISTOGRAM: Histogram = register_histogram!(
        "nbbo_age_us",
        "NBBO age in microseconds"
    ).unwrap();

    pub static ref CLASSIFY_UNKNOWN_COUNTER: IntCounter = register_int_counter!(
        "classify_unknown_total",
        "Total unknown classifications"
    ).unwrap();
    // ... add more from spec
}

pub fn encode_metrics() -> Vec<u8> {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &buffer).unwrap();
    buffer
}
