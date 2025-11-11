// Copyright (c) James Kassemi, SC, US. All rights reserved.

use core_types::types::{AggressorSide, ClassMethod, ClassParams, NbboState, TradeLike};
use nbbo_cache::NbboStore;
use std::collections::HashMap;
use std::sync::Mutex;

/// Aggressor classifier that prefers NBBO touch/through logic with tick-rule fallback.
pub struct Classifier {
    last_prices: Mutex<HashMap<String, f64>>,
    last_underlying_mid: Mutex<HashMap<String, f64>>,
}

impl Default for Classifier {
    fn default() -> Self {
        Self {
            last_prices: Mutex::new(HashMap::new()),
            last_underlying_mid: Mutex::new(HashMap::new()),
        }
    }
}

impl Classifier {
    pub fn new() -> Self {
        Self::default()
    }

    /// Classify aggressor for a trade using NBBO store and params.
    pub fn classify_trade(
        &self,
        trade: &mut dyn TradeLike,
        nbbo: &NbboStore,
        params: &ClassParams,
    ) {
        let instrument = trade.instrument_id().to_string();
        let epsilon = params.epsilon_price.max(1e-6);
        let staleness_us = params.allowed_lateness_ms.saturating_mul(1_000);
        let prev_price = self.swap_last_price(&instrument, trade.price());

        let quote = nbbo.get_best_before(&instrument, trade.trade_ts_ns(), staleness_us);
        if let Some(ref qte) = quote {
            let age_ns = trade.trade_ts_ns().saturating_sub(qte.quote_ts_ns);
            let age_us = (age_ns / 1_000).max(0);
            trade.set_nbbo_snapshot(
                Some(qte.bid),
                Some(qte.ask),
                Some(qte.bid_sz),
                Some(qte.ask_sz),
                Some(qte.quote_ts_ns),
                Some(age_us.min(u32::MAX as i64) as u32),
                Some(qte.state.clone()),
            );
        } else {
            trade.set_nbbo_snapshot(None, None, None, None, None, None, None);
        }

        let mut side = AggressorSide::Unknown;
        let mut method = ClassMethod::Unknown;
        let mut offset_mid_bp: Option<i32> = None;
        let mut confidence = Some(0.0);

        if let Some(ref qte) = quote {
            if actionable_quote(qte) {
                let mid = mid_from_quote(qte);
                if let Some(m) = mid {
                    offset_mid_bp = round_to_i32(((trade.price() - m) / m) * 10_000.0);
                }
                let price = trade.price();
                if price >= qte.ask - epsilon {
                    side = AggressorSide::Buyer;
                    method = ClassMethod::NbboTouch;
                    confidence = Some(1.0);
                } else if price <= qte.bid + epsilon {
                    side = AggressorSide::Seller;
                    method = ClassMethod::NbboTouch;
                    confidence = Some(1.0);
                } else if let Some(mid) = mid_from_quote(qte) {
                    method = ClassMethod::NbboAtOrBeyond;
                    confidence = Some(0.7);
                    let dist_to_bid = (price - qte.bid).abs();
                    let dist_to_ask = (qte.ask - price).abs();
                    side = if dist_to_ask < dist_to_bid && price >= mid {
                        AggressorSide::Buyer
                    } else if dist_to_bid < dist_to_ask && price <= mid {
                        AggressorSide::Seller
                    } else if price >= mid {
                        AggressorSide::Buyer
                    } else {
                        AggressorSide::Seller
                    };
                }
            }
        }

        if matches!(side, AggressorSide::Unknown) && params.use_tick_rule_fallback {
            let tick = tick_rule_side(prev_price, trade.price(), epsilon);
            side = tick.side;
            method = match side {
                AggressorSide::Unknown => ClassMethod::Unknown,
                _ => ClassMethod::TickRule,
            };
            confidence = match side {
                AggressorSide::Unknown => Some(0.0),
                _ => Some(0.5),
            };
            if trade.is_option() && !matches!(side, AggressorSide::Unknown) {
                if let Some(adj) =
                    self.apply_delta_alignment(trade, tick.delta_price, nbbo, staleness_us)
                {
                    confidence = Some(adj);
                }
            }
            if offset_mid_bp.is_none() {
                offset_mid_bp = quote
                    .as_ref()
                    .and_then(mid_from_quote)
                    .and_then(|mid| round_to_i32(((trade.price() - mid) / mid) * 10_000.0));
            }
        }

        trade.set_tick_size_used(Some(epsilon));
        trade.set_aggressor_side(side);
        trade.set_class_method(method);
        trade.set_aggressor_offset_mid_bp(offset_mid_bp);
        trade.set_aggressor_confidence(confidence);
    }

    fn swap_last_price(&self, instrument: &str, price: f64) -> Option<f64> {
        let mut guard = self.last_prices.lock().unwrap();
        guard.insert(instrument.to_string(), price)
    }

    fn store_underlying_mid(&self, instrument: &str, mid: f64) -> Option<f64> {
        let mut guard = self.last_underlying_mid.lock().unwrap();
        guard.insert(instrument.to_string(), mid)
    }

    fn apply_delta_alignment(
        &self,
        trade: &dyn TradeLike,
        price_delta: Option<f64>,
        nbbo: &NbboStore,
        staleness_us: u32,
    ) -> Option<f64> {
        if !trade.is_option() {
            return None;
        }
        let option_delta = trade.option_delta()?;
        if option_delta.abs() <= f64::EPSILON {
            return None;
        }
        let price_delta = price_delta?;
        if price_delta.abs() <= f64::EPSILON {
            return None;
        }
        let underlying = trade.underlying_symbol()?;
        let underlying_quote =
            nbbo.get_best_before(underlying, trade.trade_ts_ns(), staleness_us)?;
        let current_mid = mid_from_quote(&underlying_quote)?;
        let previous_mid = self.store_underlying_mid(underlying, current_mid)?;
        let d_s = current_mid - previous_mid;
        if d_s.abs() <= f64::EPSILON {
            return None;
        }
        let alignment_sign = (option_delta.signum() * d_s.signum()).signum();
        let trade_sign = price_delta.signum();
        if alignment_sign == 0.0 || trade_sign == 0.0 {
            return None;
        }
        if alignment_sign == trade_sign {
            Some(0.7)
        } else {
            Some(0.0)
        }
    }
}

struct TickOutcome {
    side: AggressorSide,
    delta_price: Option<f64>,
}

fn tick_rule_side(previous_price: Option<f64>, price: f64, epsilon: f64) -> TickOutcome {
    let delta = previous_price.map(|prev| price - prev);
    let side = match delta {
        Some(diff) if diff > epsilon => AggressorSide::Buyer,
        Some(diff) if diff < -epsilon => AggressorSide::Seller,
        _ => AggressorSide::Unknown,
    };
    TickOutcome {
        side,
        delta_price: delta,
    }
}

fn actionable_quote(q: &core_types::types::Nbbo) -> bool {
    matches!(q.state, NbboState::Normal) && q.ask.is_finite() && q.bid.is_finite() && q.ask > q.bid
}

fn mid_from_quote(q: &core_types::types::Nbbo) -> Option<f64> {
    if q.ask.is_finite() && q.ask > 0.0 {
        Some(0.5 * (q.bid + q.ask))
    } else if q.bid.is_finite() && q.bid > 0.0 {
        Some(q.bid)
    } else {
        None
    }
}

fn round_to_i32(value: f64) -> Option<i32> {
    if value.is_finite() {
        let rounded = value.round();
        let clamped = rounded.clamp(i32::MIN as f64, i32::MAX as f64);
        Some(clamped as i32)
    } else {
        None
    }
}
