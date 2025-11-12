// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Deterministic 128-bit identifiers for trades and quotes.

use blake3::Hasher;

pub const UID_LEN: usize = 16;
pub type TradeUid = [u8; UID_LEN];
pub type QuoteUid = [u8; UID_LEN];

struct UidBuilder {
    hasher: Hasher,
}

impl UidBuilder {
    fn new(domain: &[u8]) -> Self {
        let mut hasher = Hasher::new();
        hasher.update(&(domain.len() as u32).to_le_bytes());
        hasher.update(domain);
        Self { hasher }
    }

    fn write_len_prefixed(&mut self, bytes: &[u8]) {
        self.hasher.update(&(bytes.len() as u32).to_le_bytes());
        self.hasher.update(bytes);
    }

    fn write_str(&mut self, value: &str) -> &mut Self {
        self.write_len_prefixed(value.as_bytes());
        self
    }

    fn write_i64(&mut self, value: i64) -> &mut Self {
        self.hasher.update(&value.to_le_bytes());
        self
    }

    fn write_u64(&mut self, value: u64) -> &mut Self {
        self.hasher.update(&value.to_le_bytes());
        self
    }

    fn write_i32(&mut self, value: i32) -> &mut Self {
        self.hasher.update(&value.to_le_bytes());
        self
    }

    fn write_u32(&mut self, value: u32) -> &mut Self {
        self.hasher.update(&value.to_le_bytes());
        self
    }

    fn write_f64(&mut self, value: f64) -> &mut Self {
        self.hasher.update(&value.to_le_bytes());
        self
    }

    fn write_i32_list(&mut self, values: &[i32]) -> &mut Self {
        self.hasher.update(&(values.len() as u32).to_le_bytes());
        for v in values {
            self.hasher.update(&v.to_le_bytes());
        }
        self
    }

    fn write_option<T>(
        &mut self,
        value: Option<T>,
        f: impl FnOnce(&mut Self, T) -> &mut Self,
    ) -> &mut Self {
        match value {
            Some(v) => {
                self.hasher.update(&[1]);
                f(self, v)
            }
            None => {
                self.hasher.update(&[0]);
                self
            }
        }
    }

    fn finish(self) -> [u8; UID_LEN] {
        let hash = self.hasher.finalize();
        let mut bytes = [0u8; UID_LEN];
        bytes.copy_from_slice(&hash.as_bytes()[..UID_LEN]);
        bytes
    }
}

/// Build a UID for option trades.
pub fn option_trade_uid(
    contract: &str,
    trade_ts_ns: i64,
    participant_ts_ns: Option<i64>,
    price: f64,
    size: u32,
    exchange: i32,
    conditions: &[i32],
) -> TradeUid {
    let mut builder = UidBuilder::new(b"option_trade_uid.v1");
    builder
        .write_str(contract)
        .write_i64(trade_ts_ns)
        .write_option(participant_ts_ns, UidBuilder::write_i64)
        .write_f64(price)
        .write_u32(size)
        .write_i32(exchange)
        .write_i32_list(conditions);
    builder.finish()
}

/// Build a UID for equity trades.
pub fn equity_trade_uid(
    symbol: &str,
    trade_ts_ns: i64,
    participant_ts_ns: Option<i64>,
    seq: Option<u64>,
    price: f64,
    size: u32,
    exchange: i32,
    trade_id: Option<&str>,
    correction: Option<i32>,
    conditions: &[i32],
) -> TradeUid {
    let mut builder = UidBuilder::new(b"equity_trade_uid.v1");
    builder
        .write_str(symbol)
        .write_i64(trade_ts_ns)
        .write_option(participant_ts_ns, UidBuilder::write_i64)
        .write_option(seq, UidBuilder::write_u64)
        .write_f64(price)
        .write_u32(size)
        .write_i32(exchange)
        .write_option(trade_id, UidBuilder::write_str)
        .write_option(correction, UidBuilder::write_i32)
        .write_i32_list(conditions);
    builder.finish()
}

/// Build a UID for NBBO quotes (options + equities share schema).
pub fn quote_uid(
    instrument_id: &str,
    quote_ts_ns: i64,
    sequence_number: Option<u64>,
    bid: f64,
    ask: f64,
    bid_sz: u32,
    ask_sz: u32,
    best_bid_venue: Option<i32>,
    best_ask_venue: Option<i32>,
    condition: Option<i32>,
) -> QuoteUid {
    let mut builder = UidBuilder::new(b"quote_uid.v1");
    builder
        .write_str(instrument_id)
        .write_i64(quote_ts_ns)
        .write_option(sequence_number, UidBuilder::write_u64)
        .write_f64(bid)
        .write_f64(ask)
        .write_u32(bid_sz)
        .write_u32(ask_sz)
        .write_option(best_bid_venue, UidBuilder::write_i32)
        .write_option(best_ask_venue, UidBuilder::write_i32)
        .write_option(condition, UidBuilder::write_i32);
    builder.finish()
}
