// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! Streaming windowed aggregations for options + underlying trades.

use core_types::config::AggregationsConfig;
use core_types::types::{AggregationRow, AggressorSide, ClassMethod, EquityTrade, OptionTrade};
use diptest::{
    diptest_with_options, BootstrapConfig, DipStatOptions, DipTestOptions, PValueMethod,
};
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::f64::consts::PI;
use thiserror::Error;

const NANOS_PER_SECOND: i64 = 1_000_000_000;
const BOX_COX_EPSILON: f64 = 1e-6;
const BOX_COX_LAMBDAS: [f64; 17] = [
    -2.0, -1.75, -1.5, -1.25, -1.0, -0.75, -0.5, -0.25, 0.0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75,
    2.0,
];

/// Input event for streaming aggregation.
#[derive(Debug, Clone)]
pub enum AggregationInput {
    UnderlyingTrade(EquityTrade),
    OptionTrade {
        trade: OptionTrade,
        underlying_price: Option<f64>,
    },
}

#[derive(Debug, Error)]
pub enum AggregationError {
    #[error("invalid window string: {0}")]
    InvalidWindow(String),
}

/// Streaming engine producing per-window aggregation rows.
pub struct AggregationsEngine {
    symbol: String,
    contract_size: f64,
    windows: Vec<WindowAggregator>,
    last_underlying_price: Option<f64>,
    // retain recent underlying prices for backfilling option trades when needed
    recent_underlyings: VecDeque<(i64, f64)>,
    max_window_ns: i64,
    diptest_draws: usize,
}

impl AggregationsEngine {
    pub fn new(cfg: AggregationsConfig) -> Result<Self, AggregationError> {
        let mut windows = Vec::new();
        let mut max_window_ns = 0i64;
        for win in &cfg.windows {
            let (duration_ns, label) = parse_window(win)?;
            max_window_ns = max_window_ns.max(duration_ns);
            windows.push(WindowAggregator::new(
                label,
                duration_ns,
                cfg.symbol.clone(),
            ));
        }
        windows.sort_by(|a, b| a.duration_ns.cmp(&b.duration_ns));
        Ok(Self {
            symbol: cfg.symbol,
            contract_size: cfg.contract_size as f64,
            windows,
            last_underlying_price: None,
            recent_underlyings: VecDeque::new(),
            max_window_ns,
            diptest_draws: cfg.diptest_bootstrap_draws,
        })
    }

    /// Ingest an event and return any closed window rows.
    pub fn ingest(&mut self, event: AggregationInput) -> Vec<AggregationRow> {
        match &event {
            AggregationInput::UnderlyingTrade(trade) => {
                if trade.symbol != self.symbol {
                    return Vec::new();
                }
                self.last_underlying_price = Some(trade.price);
                self.recent_underlyings
                    .push_back((trade.trade_ts_ns, trade.price));
            }
            AggregationInput::OptionTrade { trade, .. } => {
                if trade.underlying != self.symbol {
                    return Vec::new();
                }
            }
        }

        drop_stale_prices(
            &mut self.recent_underlyings,
            event.timestamp_ns(),
            self.max_window_ns,
        );

        let mut output = Vec::new();
        for window in self.windows.iter_mut() {
            output.extend(window.process_event(
                event.clone(),
                self.contract_size,
                self.last_underlying_price,
                &self.recent_underlyings,
                self.diptest_draws,
            ));
        }
        output
    }
}

fn drop_stale_prices(buf: &mut VecDeque<(i64, f64)>, ts_ns: i64, horizon_ns: i64) {
    while let Some((old_ts, _)) = buf.front() {
        if ts_ns - *old_ts > horizon_ns * 2 {
            buf.pop_front();
        } else {
            break;
        }
    }
}

fn parse_window(spec: &str) -> Result<(i64, String), AggregationError> {
    if spec.is_empty() {
        return Err(AggregationError::InvalidWindow(spec.to_string()));
    }
    let unit = spec.chars().last().unwrap();
    let value: i64 = spec[..spec.len() - 1]
        .parse()
        .map_err(|_| AggregationError::InvalidWindow(spec.to_string()))?;
    let seconds = match unit {
        's' | 'S' => value,
        'm' | 'M' => value * 60,
        'h' | 'H' => value * 3600,
        _ => return Err(AggregationError::InvalidWindow(spec.to_string())),
    };
    Ok((seconds * NANOS_PER_SECOND, spec.to_string()))
}

#[derive(Clone)]
struct WindowAggregator {
    label: String,
    duration_ns: i64,
    symbol: String,
    state: Option<WindowState>,
}

impl WindowAggregator {
    fn new(label: String, duration_ns: i64, symbol: String) -> Self {
        Self {
            label,
            duration_ns,
            symbol,
            state: None,
        }
    }

    fn process_event(
        &mut self,
        event: AggregationInput,
        contract_size: f64,
        last_under_price: Option<f64>,
        price_history: &VecDeque<(i64, f64)>,
        diptest_draws: usize,
    ) -> Vec<AggregationRow> {
        let ts_ns = event.timestamp_ns();
        let mut rows = Vec::new();
        if self.state.is_none() {
            let aligned = align_ts(ts_ns, self.duration_ns);
            self.state = Some(WindowState::new(
                aligned,
                aligned + self.duration_ns,
                self.label.clone(),
                self.symbol.clone(),
                diptest_draws,
            ));
        }
        while self
            .state
            .as_ref()
            .map_or(false, |state| ts_ns >= state.end_ns)
        {
            let next_start = self.state.as_ref().map(|s| s.end_ns).unwrap();
            if let Some(finished) = self.state.take().unwrap().finalize() {
                rows.push(finished);
            }
            self.state = Some(WindowState::new(
                next_start,
                next_start + self.duration_ns,
                self.label.clone(),
                self.symbol.clone(),
                diptest_draws,
            ));
        }
        if let Some(state) = self.state.as_mut() {
            state.ingest(event, contract_size, last_under_price, price_history);
        }
        rows
    }
}

fn align_ts(ts_ns: i64, duration_ns: i64) -> i64 {
    (ts_ns / duration_ns) * duration_ns
}

#[derive(Clone)]
struct WindowState {
    symbol: String,
    window_label: String,
    start_ns: i64,
    end_ns: i64,
    underlying_open: Option<f64>,
    underlying_high: Option<f64>,
    underlying_low: Option<f64>,
    underlying_close: Option<f64>,
    underlying_dv: Vec<f64>,
    puts_below_intrinsic: u64,
    puts_above_intrinsic: u64,
    puts_total: u64,
    calls_above_intrinsic: u64,
    calls_total: u64,
    calls_notional: f64,
    puts_unknown: u64,
    calls_unknown: u64,
    puts_method_mix: MethodMix,
    calls_method_mix: MethodMix,
    puts_dadvv: Vec<f64>,
    puts_gadvv: Vec<f64>,
    puts_signed_dadvv: Vec<f64>,
    puts_signed_gadvv: Vec<f64>,
    calls_dadvv: Vec<f64>,
    calls_gadvv: Vec<f64>,
    calls_signed_dadvv: Vec<f64>,
    calls_signed_gadvv: Vec<f64>,
    diptest_draws: usize,
}

#[derive(Default, Clone)]
struct MethodMix {
    touch: u64,
    at_or_beyond: u64,
    tick: u64,
    unknown: u64,
}

impl MethodMix {
    fn record(&mut self, method: &ClassMethod) {
        match method {
            ClassMethod::NbboTouch => self.touch += 1,
            ClassMethod::NbboAtOrBeyond => self.at_or_beyond += 1,
            ClassMethod::TickRule => self.tick += 1,
            ClassMethod::Unknown => self.unknown += 1,
        }
    }

    fn touch_pct(&self, total: u64) -> Option<f64> {
        pct(self.touch, total)
    }

    fn at_or_beyond_pct(&self, total: u64) -> Option<f64> {
        pct(self.at_or_beyond, total)
    }

    fn tick_pct(&self, total: u64) -> Option<f64> {
        pct(self.tick, total)
    }

    fn unknown_pct(&self, total: u64) -> Option<f64> {
        pct(self.unknown, total)
    }
}

impl WindowState {
    fn new(
        start_ns: i64,
        end_ns: i64,
        label: String,
        symbol: String,
        diptest_draws: usize,
    ) -> Self {
        Self {
            symbol,
            window_label: label,
            start_ns,
            end_ns,
            underlying_open: None,
            underlying_high: None,
            underlying_low: None,
            underlying_close: None,
            underlying_dv: Vec::new(),
            puts_below_intrinsic: 0,
            puts_above_intrinsic: 0,
            puts_total: 0,
            calls_above_intrinsic: 0,
            calls_total: 0,
            calls_notional: 0.0,
            puts_unknown: 0,
            calls_unknown: 0,
            puts_method_mix: MethodMix::default(),
            calls_method_mix: MethodMix::default(),
            puts_dadvv: Vec::new(),
            puts_gadvv: Vec::new(),
            puts_signed_dadvv: Vec::new(),
            puts_signed_gadvv: Vec::new(),
            calls_dadvv: Vec::new(),
            calls_gadvv: Vec::new(),
            calls_signed_dadvv: Vec::new(),
            calls_signed_gadvv: Vec::new(),
            diptest_draws,
        }
    }

    fn ingest(
        &mut self,
        event: AggregationInput,
        contract_size: f64,
        last_under_price: Option<f64>,
        price_history: &VecDeque<(i64, f64)>,
    ) {
        match event {
            AggregationInput::UnderlyingTrade(trade) => {
                let price = trade.price;
                if self.underlying_open.is_none() {
                    self.underlying_open = Some(price);
                }
                self.underlying_close = Some(price);
                self.underlying_high = Some(self.underlying_high.map_or(price, |h| h.max(price)));
                self.underlying_low = Some(self.underlying_low.map_or(price, |l| l.min(price)));
                let dv = (trade.price * trade.size as f64).abs();
                self.underlying_dv.push(dv);
            }
            AggregationInput::OptionTrade {
                trade,
                underlying_price,
            } => {
                let s = underlying_price
                    .or(last_under_price)
                    .or_else(|| lookup_price(price_history, trade.trade_ts_ns));
                let s = match s {
                    Some(val) if val.is_finite() && val > 0.0 => val,
                    _ => return,
                };
                let notional = trade.price * trade.size as f64 * contract_size;
                let intrinsic = intrinsic_value(&trade, s);
                let above_intrinsic = trade.price > intrinsic;
                let is_call = matches!(trade.contract_direction, 'C' | 'c');
                if is_call {
                    self.calls_total += 1;
                    self.calls_method_mix.record(&trade.class_method);
                    if matches!(trade.aggressor_side, AggressorSide::Unknown) {
                        self.calls_unknown += 1;
                    }
                    if above_intrinsic {
                        self.calls_above_intrinsic += 1;
                    }
                    self.calls_notional += notional;
                } else {
                    self.puts_total += 1;
                    self.puts_method_mix.record(&trade.class_method);
                    if matches!(trade.aggressor_side, AggressorSide::Unknown) {
                        self.puts_unknown += 1;
                    }
                    if trade.price < intrinsic {
                        self.puts_below_intrinsic += 1;
                    }
                    if trade.price > intrinsic {
                        self.puts_above_intrinsic += 1;
                    }
                }
                let raw_delta = trade.delta.unwrap_or(0.0);
                let raw_gamma = trade.gamma.unwrap_or(0.0);
                let unsigned_delta = raw_delta.abs();
                let unsigned_gamma = raw_gamma.abs();
                let p_buy = buy_probability(&trade);
                let signed_factor = 2.0 * p_buy - 1.0;
                let contract_qty = trade.size as f64 * contract_size;
                let dadvv = unsigned_delta * s * contract_qty;
                let signed_dadvv = signed_factor * dadvv;
                let gadvv = unsigned_gamma * s * s * contract_qty;
                let signed_gadvv = signed_factor * gadvv;
                if is_call {
                    if dadvv.is_finite() {
                        self.calls_dadvv.push(dadvv);
                    }
                    if signed_dadvv.is_finite() {
                        self.calls_signed_dadvv.push(signed_dadvv);
                    }
                    if gadvv.is_finite() {
                        self.calls_gadvv.push(gadvv);
                    }
                    if signed_gadvv.is_finite() {
                        self.calls_signed_gadvv.push(signed_gadvv);
                    }
                } else {
                    if dadvv.is_finite() {
                        self.puts_dadvv.push(dadvv);
                    }
                    if signed_dadvv.is_finite() {
                        self.puts_signed_dadvv.push(signed_dadvv);
                    }
                    if gadvv.is_finite() {
                        self.puts_gadvv.push(gadvv);
                    }
                    if signed_gadvv.is_finite() {
                        self.puts_signed_gadvv.push(signed_gadvv);
                    }
                }
            }
        }
    }

    fn finalize(self) -> Option<AggregationRow> {
        if self.underlying_dv.is_empty()
            && self.puts_dadvv.is_empty()
            && self.calls_dadvv.is_empty()
            && self.calls_notional == 0.0
        {
            return None;
        }
        let underline_stats = compute_stats(&self.underlying_dv, self.diptest_draws);
        let puts_dadvv_stats = compute_stats(&self.puts_dadvv, self.diptest_draws);
        let puts_signed_dadvv_stats = compute_stats(&self.puts_signed_dadvv, self.diptest_draws);
        let puts_gadvv_stats = compute_stats(&self.puts_gadvv, self.diptest_draws);
        let puts_signed_gadvv_stats = compute_stats(&self.puts_signed_gadvv, self.diptest_draws);
        let calls_dadvv_stats = compute_stats(&self.calls_dadvv, self.diptest_draws);
        let calls_signed_dadvv_stats = compute_stats(&self.calls_signed_dadvv, self.diptest_draws);
        let calls_gadvv_stats = compute_stats(&self.calls_gadvv, self.diptest_draws);
        let calls_signed_gadvv_stats = compute_stats(&self.calls_signed_gadvv, self.diptest_draws);

        Some(AggregationRow {
            symbol: self.symbol.clone(),
            window: self.window_label.clone(),
            window_start_ns: self.start_ns,
            window_end_ns: self.end_ns,
            underlying_price_open: self.underlying_open,
            underlying_price_high: self.underlying_high,
            underlying_price_low: self.underlying_low,
            underlying_price_close: self.underlying_close,
            underlying_dollar_value_total: underline_stats.total,
            underlying_dollar_value_minimum: underline_stats.min,
            underlying_dollar_value_maximum: underline_stats.max,
            underlying_dollar_value_mean: underline_stats.mean,
            underlying_dollar_value_stddev: underline_stats.stddev,
            underlying_dollar_value_skew: underline_stats.skew,
            underlying_dollar_value_kurtosis: underline_stats.kurtosis,
            underlying_dollar_value_iqr: underline_stats.iqr,
            underlying_dollar_value_mad: underline_stats.mad,
            underlying_dollar_value_cv: underline_stats.cv,
            underlying_dollar_value_mode: underline_stats.mode,
            underlying_dollar_value_bc: underline_stats.bc,
            underlying_dollar_value_dip_pval: underline_stats.dip_pval,
            underlying_dollar_value_kde_peaks: underline_stats.kde_peaks,
            puts_below_intrinsic_pct: pct(self.puts_below_intrinsic, self.puts_total),
            puts_above_intrinsic_pct: pct(self.puts_above_intrinsic, self.puts_total),
            puts_aggressor_unknown_pct: pct(self.puts_unknown, self.puts_total),
            puts_classifier_touch_pct: self.puts_method_mix.touch_pct(self.puts_total),
            puts_classifier_at_or_beyond_pct: self
                .puts_method_mix
                .at_or_beyond_pct(self.puts_total),
            puts_classifier_tick_rule_pct: self.puts_method_mix.tick_pct(self.puts_total),
            puts_classifier_unknown_pct: self.puts_method_mix.unknown_pct(self.puts_total),
            puts_dadvv_total: puts_dadvv_stats.total,
            puts_dadvv_minimum: puts_dadvv_stats.min,
            puts_dadvv_maximum: puts_dadvv_stats.max,
            puts_dadvv_mean: puts_dadvv_stats.mean,
            puts_dadvv_stddev: puts_dadvv_stats.stddev,
            puts_dadvv_skew: puts_dadvv_stats.skew,
            puts_dadvv_kurtosis: puts_dadvv_stats.kurtosis,
            puts_dadvv_iqr: puts_dadvv_stats.iqr,
            puts_dadvv_mad: puts_dadvv_stats.mad,
            puts_dadvv_cv: puts_dadvv_stats.cv,
            puts_dadvv_mode: puts_dadvv_stats.mode,
            puts_dadvv_bc: puts_dadvv_stats.bc,
            puts_dadvv_dip_pval: puts_dadvv_stats.dip_pval,
            puts_dadvv_kde_peaks: puts_dadvv_stats.kde_peaks,
            puts_signed_dadvv_total: puts_signed_dadvv_stats.total,
            puts_signed_dadvv_minimum: puts_signed_dadvv_stats.min,
            puts_signed_dadvv_maximum: puts_signed_dadvv_stats.max,
            puts_signed_dadvv_mean: puts_signed_dadvv_stats.mean,
            puts_signed_dadvv_stddev: puts_signed_dadvv_stats.stddev,
            puts_signed_dadvv_skew: puts_signed_dadvv_stats.skew,
            puts_signed_dadvv_kurtosis: puts_signed_dadvv_stats.kurtosis,
            puts_signed_dadvv_iqr: puts_signed_dadvv_stats.iqr,
            puts_signed_dadvv_mad: puts_signed_dadvv_stats.mad,
            puts_signed_dadvv_cv: puts_signed_dadvv_stats.cv,
            puts_signed_dadvv_mode: puts_signed_dadvv_stats.mode,
            puts_signed_dadvv_bc: puts_signed_dadvv_stats.bc,
            puts_signed_dadvv_dip_pval: puts_signed_dadvv_stats.dip_pval,
            puts_signed_dadvv_kde_peaks: puts_signed_dadvv_stats.kde_peaks,
            puts_gadvv_total: puts_gadvv_stats.total,
            puts_gadvv_minimum: puts_gadvv_stats.min,
            puts_gadvv_maximum: puts_gadvv_stats.max,
            puts_gadvv_mean: puts_gadvv_stats.mean,
            puts_gadvv_stddev: puts_gadvv_stats.stddev,
            puts_gadvv_skew: puts_gadvv_stats.skew,
            puts_gadvv_kurtosis: puts_gadvv_stats.kurtosis,
            puts_gadvv_iqr: puts_gadvv_stats.iqr,
            puts_gadvv_mad: puts_gadvv_stats.mad,
            puts_gadvv_cv: puts_gadvv_stats.cv,
            puts_gadvv_mode: puts_gadvv_stats.mode,
            puts_gadvv_bc: puts_gadvv_stats.bc,
            puts_gadvv_dip_pval: puts_gadvv_stats.dip_pval,
            puts_gadvv_kde_peaks: puts_gadvv_stats.kde_peaks,
            puts_signed_gadvv_total: puts_signed_gadvv_stats.total,
            puts_signed_gadvv_minimum: puts_signed_gadvv_stats.min,
            puts_signed_gadvv_maximum: puts_signed_gadvv_stats.max,
            puts_signed_gadvv_mean: puts_signed_gadvv_stats.mean,
            puts_signed_gadvv_stddev: puts_signed_gadvv_stats.stddev,
            puts_signed_gadvv_skew: puts_signed_gadvv_stats.skew,
            puts_signed_gadvv_kurtosis: puts_signed_gadvv_stats.kurtosis,
            puts_signed_gadvv_iqr: puts_signed_gadvv_stats.iqr,
            puts_signed_gadvv_mad: puts_signed_gadvv_stats.mad,
            puts_signed_gadvv_cv: puts_signed_gadvv_stats.cv,
            puts_signed_gadvv_mode: puts_signed_gadvv_stats.mode,
            puts_signed_gadvv_bc: puts_signed_gadvv_stats.bc,
            puts_signed_gadvv_dip_pval: puts_signed_gadvv_stats.dip_pval,
            puts_signed_gadvv_kde_peaks: puts_signed_gadvv_stats.kde_peaks,
            calls_dollar_value: Some(self.calls_notional).filter(|v| *v > 0.0),
            calls_above_intrinsic_pct: pct(self.calls_above_intrinsic, self.calls_total),
            calls_aggressor_unknown_pct: pct(self.calls_unknown, self.calls_total),
            calls_classifier_touch_pct: self.calls_method_mix.touch_pct(self.calls_total),
            calls_classifier_at_or_beyond_pct: self
                .calls_method_mix
                .at_or_beyond_pct(self.calls_total),
            calls_classifier_tick_rule_pct: self.calls_method_mix.tick_pct(self.calls_total),
            calls_classifier_unknown_pct: self.calls_method_mix.unknown_pct(self.calls_total),
            calls_dadvv_total: calls_dadvv_stats.total,
            calls_dadvv_minimum: calls_dadvv_stats.min,
            calls_dadvv_maximum: calls_dadvv_stats.max,
            calls_dadvv_mean: calls_dadvv_stats.mean,
            calls_dadvv_stddev: calls_dadvv_stats.stddev,
            calls_dadvv_skew: calls_dadvv_stats.skew,
            calls_dadvv_kurtosis: calls_dadvv_stats.kurtosis,
            calls_dadvv_iqr: calls_dadvv_stats.iqr,
            calls_dadvv_mad: calls_dadvv_stats.mad,
            calls_dadvv_cv: calls_dadvv_stats.cv,
            calls_dadvv_mode: calls_dadvv_stats.mode,
            calls_dadvv_bc: calls_dadvv_stats.bc,
            calls_dadvv_dip_pval: calls_dadvv_stats.dip_pval,
            calls_dadvv_kde_peaks: calls_dadvv_stats.kde_peaks,
            calls_signed_dadvv_total: calls_signed_dadvv_stats.total,
            calls_signed_dadvv_minimum: calls_signed_dadvv_stats.min,
            calls_signed_dadvv_maximum: calls_signed_dadvv_stats.max,
            calls_signed_dadvv_mean: calls_signed_dadvv_stats.mean,
            calls_signed_dadvv_stddev: calls_signed_dadvv_stats.stddev,
            calls_signed_dadvv_skew: calls_signed_dadvv_stats.skew,
            calls_signed_dadvv_kurtosis: calls_signed_dadvv_stats.kurtosis,
            calls_signed_dadvv_iqr: calls_signed_dadvv_stats.iqr,
            calls_signed_dadvv_mad: calls_signed_dadvv_stats.mad,
            calls_signed_dadvv_cv: calls_signed_dadvv_stats.cv,
            calls_signed_dadvv_mode: calls_signed_dadvv_stats.mode,
            calls_signed_dadvv_bc: calls_signed_dadvv_stats.bc,
            calls_signed_dadvv_dip_pval: calls_signed_dadvv_stats.dip_pval,
            calls_signed_dadvv_kde_peaks: calls_signed_dadvv_stats.kde_peaks,
            calls_gadvv_total: calls_gadvv_stats.total,
            calls_gadvv_minimum: calls_gadvv_stats.min,
            calls_gadvv_q1: calls_gadvv_stats.q1,
            calls_gadvv_q2: calls_gadvv_stats.median,
            calls_gadvv_q3: calls_gadvv_stats.q3,
            calls_gadvv_maximum: calls_gadvv_stats.max,
            calls_gadvv_mean: calls_gadvv_stats.mean,
            calls_gadvv_stddev: calls_gadvv_stats.stddev,
            calls_gadvv_skew: calls_gadvv_stats.skew,
            calls_gadvv_kurtosis: calls_gadvv_stats.kurtosis,
            calls_gadvv_iqr: calls_gadvv_stats.iqr,
            calls_gadvv_mad: calls_gadvv_stats.mad,
            calls_gadvv_cv: calls_gadvv_stats.cv,
            calls_gadvv_mode: calls_gadvv_stats.mode,
            calls_gadvv_bc: calls_gadvv_stats.bc,
            calls_gadvv_dip_pval: calls_gadvv_stats.dip_pval,
            calls_gadvv_kde_peaks: calls_gadvv_stats.kde_peaks,
            calls_signed_gadvv_total: calls_signed_gadvv_stats.total,
            calls_signed_gadvv_minimum: calls_signed_gadvv_stats.min,
            calls_signed_gadvv_q1: calls_signed_gadvv_stats.q1,
            calls_signed_gadvv_q2: calls_signed_gadvv_stats.median,
            calls_signed_gadvv_q3: calls_signed_gadvv_stats.q3,
            calls_signed_gadvv_maximum: calls_signed_gadvv_stats.max,
            calls_signed_gadvv_mean: calls_signed_gadvv_stats.mean,
            calls_signed_gadvv_stddev: calls_signed_gadvv_stats.stddev,
            calls_signed_gadvv_skew: calls_signed_gadvv_stats.skew,
            calls_signed_gadvv_kurtosis: calls_signed_gadvv_stats.kurtosis,
            calls_signed_gadvv_iqr: calls_signed_gadvv_stats.iqr,
            calls_signed_gadvv_mad: calls_signed_gadvv_stats.mad,
            calls_signed_gadvv_cv: calls_signed_gadvv_stats.cv,
            calls_signed_gadvv_mode: calls_signed_gadvv_stats.mode,
            calls_signed_gadvv_bc: calls_signed_gadvv_stats.bc,
            calls_signed_gadvv_dip_pval: calls_signed_gadvv_stats.dip_pval,
            calls_signed_gadvv_kde_peaks: calls_signed_gadvv_stats.kde_peaks,
        })
    }
}

fn pct(part: u64, total: u64) -> Option<f64> {
    if total == 0 {
        None
    } else {
        Some(part as f64 / total as f64)
    }
}

fn lookup_price(history: &VecDeque<(i64, f64)>, ts_ns: i64) -> Option<f64> {
    history
        .iter()
        .rev()
        .find(|(ts, _)| *ts <= ts_ns)
        .map(|(_, price)| *price)
}

fn intrinsic_value(trade: &OptionTrade, s: f64) -> f64 {
    match trade.contract_direction {
        'C' | 'c' => (s - trade.strike_price).max(0.0),
        _ => (trade.strike_price - s).max(0.0),
    }
}

fn buy_probability(trade: &OptionTrade) -> f64 {
    let confidence = trade.aggressor_confidence.unwrap_or(1.0).clamp(0.0, 1.0);
    match trade.aggressor_side {
        AggressorSide::Buyer => confidence,
        AggressorSide::Seller => 1.0 - confidence,
        AggressorSide::Unknown => 0.5,
    }
}

#[derive(Default, Clone, Debug)]
struct StatsSummary {
    total: Option<f64>,
    min: Option<f64>,
    max: Option<f64>,
    mean: Option<f64>,
    stddev: Option<f64>,
    skew: Option<f64>,
    kurtosis: Option<f64>,
    q1: Option<f64>,
    median: Option<f64>,
    q3: Option<f64>,
    iqr: Option<f64>,
    mad: Option<f64>,
    cv: Option<f64>,
    mode: Option<f64>,
    bc: Option<f64>,
    dip_pval: Option<f64>,
    kde_peaks: Option<f64>,
}

fn compute_stats(values: &[f64], diptest_draws: usize) -> StatsSummary {
    if values.is_empty() {
        return StatsSummary::default();
    }
    let mut summary = StatsSummary::default();
    let mut sum = 0.0;
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    let mut m1 = 0.0;
    let mut m2 = 0.0;
    let mut m3 = 0.0;
    let mut m4 = 0.0;
    for (i, &x) in values.iter().enumerate() {
        sum += x;
        if x < min {
            min = x;
        }
        if x > max {
            max = x;
        }
        let delta = x - m1;
        let n = (i + 1) as f64;
        let delta_n = delta / n;
        let term1 = delta * delta_n * (n - 1.0);
        m4 += term1 * delta_n * delta_n * (n * n - 3.0 * n + 3.0) + 6.0 * delta_n * delta_n * m2
            - 4.0 * delta_n * m3;
        m3 += term1 * delta_n * (n - 2.0) - 3.0 * delta_n * m2;
        m2 += term1;
        m1 += delta_n;
    }
    let n = values.len() as f64;
    summary.total = Some(sum);
    summary.min = Some(min);
    summary.max = Some(max);
    summary.mean = Some(m1);
    if n > 1.0 {
        let variance = m2 / (n - 1.0);
        let stddev = variance.sqrt();
        summary.stddev = Some(stddev);
        if stddev > 0.0 {
            summary.cv = Some(stddev / m1.abs().max(1e-9));
        }
        if variance > 0.0 {
            summary.skew = Some((n.sqrt() * m3) / (m2.powf(1.5)));
            summary.kurtosis = Some((n * m4) / (m2 * m2));
        }
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    summary.q1 = Some(quantile(&sorted, 0.25));
    summary.median = Some(quantile(&sorted, 0.5));
    summary.q3 = Some(quantile(&sorted, 0.75));
    summary.iqr = match (summary.q3, summary.q1) {
        (Some(q3), Some(q1)) => Some(q3 - q1),
        _ => None,
    };
    if let Some(median) = summary.median {
        let mut deviations: Vec<f64> = sorted.iter().map(|v| (v - median).abs()).collect();
        deviations.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        summary.mad = Some(quantile(&deviations, 0.5));
    }
    summary.bc = estimate_box_cox_lambda(values);
    summary.mode = estimate_mode(&sorted, summary.iqr, summary.stddev);
    summary.kde_peaks = estimate_kde_peaks(&sorted, summary.stddev);
    if sorted.len() >= 3 && diptest_draws > 0 {
        let options = DipTestOptions {
            stat: DipStatOptions {
                sort_data: false,
                allow_zero: true,
                full_output: false,
            },
            p_value_method: PValueMethod::Bootstrap(BootstrapConfig {
                draws: diptest_draws,
                seed: None,
                threads: None,
            }),
        };
        if let Ok(res) = diptest_with_options(&sorted, options) {
            summary.dip_pval = Some(res.p_value);
        }
    }
    summary
}

fn estimate_box_cox_lambda(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    if values.iter().any(|v| !v.is_finite() || *v <= 0.0) {
        return None;
    }
    let log_values: Vec<f64> = values.iter().map(|v| v.ln()).collect();
    let sum_logs: f64 = log_values.iter().sum();
    let n = values.len() as f64;
    let mut best_lambda = None;
    let mut best_ll = f64::NEG_INFINITY;
    'grid: for &lambda in BOX_COX_LAMBDAS.iter() {
        let mut mean = 0.0;
        let mut m2 = 0.0;
        let mut count = 0.0;
        for &log_x in &log_values {
            let y = if lambda.abs() < BOX_COX_EPSILON {
                log_x
            } else {
                let exp_term = (lambda * log_x).exp();
                if !exp_term.is_finite() {
                    continue 'grid;
                }
                (exp_term - 1.0) / lambda
            };
            count += 1.0;
            let delta = y - mean;
            mean += delta / count;
            m2 += delta * (y - mean);
        }
        if count < 2.0 {
            continue;
        }
        let variance = m2 / count;
        if variance <= 0.0 || !variance.is_finite() {
            continue;
        }
        let ll = (lambda - 1.0) * sum_logs - 0.5 * n * variance.ln();
        if ll > best_ll {
            best_ll = ll;
            best_lambda = Some(lambda);
        }
    }
    best_lambda
}

fn quantile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let pos = q * ((sorted.len() - 1) as f64);
    let lower = pos.floor() as usize;
    let upper = pos.ceil() as usize;
    if lower == upper {
        sorted[lower]
    } else {
        let weight = pos - lower as f64;
        sorted[lower] * (1.0 - weight) + sorted[upper] * weight
    }
}

fn estimate_mode(sorted: &[f64], iqr: Option<f64>, stddev: Option<f64>) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    if sorted.len() <= 16 {
        let mut best = sorted[0];
        let mut best_count = 1;
        let mut current = sorted[0];
        let mut count = 1;
        for &val in &sorted[1..] {
            if (val - current).abs() < f64::EPSILON {
                count += 1;
            } else {
                if count > best_count {
                    best = current;
                    best_count = count;
                }
                current = val;
                count = 1;
            }
        }
        if count > best_count {
            best = current;
        }
        return Some(best);
    }
    let width = match (iqr, stddev) {
        (Some(iqr), _) if iqr > 0.0 => 2.0 * iqr / (sorted.len() as f64).powf(1.0 / 3.0),
        (_, Some(stddev)) if stddev > 0.0 => 3.5 * stddev / (sorted.len() as f64).powf(1.0 / 3.0),
        _ => return Some(sorted[sorted.len() / 2]),
    };
    if width <= 0.0 {
        return Some(sorted[sorted.len() / 2]);
    }
    let min = *sorted.first().unwrap();
    let max = *sorted.last().unwrap();
    let bins = ((max - min) / width).ceil().max(1.0) as usize;
    let mut counts = vec![0usize; bins];
    for &v in sorted {
        let idx = ((v - min) / width).floor() as usize;
        let idx = idx.min(bins - 1);
        counts[idx] += 1;
    }
    let (max_idx, _) = counts
        .iter()
        .enumerate()
        .max_by_key(|(_, count)| *count)
        .unwrap();
    Some(min + width * max_idx as f64 + width / 2.0)
}

fn estimate_kde_peaks(sorted: &[f64], stddev: Option<f64>) -> Option<f64> {
    if sorted.len() < 3 {
        return Some(sorted.len() as f64);
    }
    let stddev = stddev?;
    if stddev == 0.0 {
        return Some(1.0);
    }
    let n = sorted.len() as f64;
    let bandwidth = 1.06 * stddev * n.powf(-0.2);
    if bandwidth <= 0.0 {
        return Some(1.0);
    }
    let min = *sorted.first().unwrap();
    let max = *sorted.last().unwrap();
    let steps = 128usize;
    let step = (max - min) / steps as f64;
    if step <= 0.0 {
        return Some(1.0);
    }
    let mut peaks = 0usize;
    let mut prev_val = None;
    let mut prev_derivative = None;
    for i in 0..=steps {
        let x = min + step * i as f64;
        let density = gaussian_kde(sorted, x, bandwidth);
        if let Some(prev) = prev_val {
            let derivative = density - prev;
            if let Some(prev_d) = prev_derivative {
                if prev_d > 0.0 && derivative < 0.0 {
                    peaks += 1;
                }
            }
            prev_derivative = Some(derivative);
        }
        prev_val = Some(density);
    }
    Some(peaks.max(1) as f64)
}

fn gaussian_kde(data: &[f64], x: f64, h: f64) -> f64 {
    let norm = 1.0 / (data.len() as f64 * h * (2.0 * PI).sqrt());
    data.iter()
        .map(|&val| {
            let u = (x - val) / h;
            (-0.5 * u * u).exp()
        })
        .sum::<f64>()
        * norm
}

impl AggregationInput {
    fn timestamp_ns(&self) -> i64 {
        match self {
            AggregationInput::UnderlyingTrade(t) => t.trade_ts_ns,
            AggregationInput::OptionTrade { trade, .. } => trade.trade_ts_ns,
        }
    }
}

pub use self::AggregationInput as AggregationEvent;
