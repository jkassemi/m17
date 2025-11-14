// Copyright (c) James Kassemi, SC, US. All rights reserved.
use core_types::config::GreeksConfig;
use core_types::types::{Nbbo, NbboState, OptionTrade};
use futures::{stream, StreamExt};
use libm::erf;
use nbbo_cache::NbboStore;
use std::f64::consts::SQRT_2;
use std::sync::Arc;
use tokio::sync::RwLock;

const NANOS_PER_SECOND: f64 = 1_000_000_000.0;
const SECONDS_PER_YEAR: f64 = 31_536_000.0;
const MIN_TAU_YEARS: f64 = 1.0 / SECONDS_PER_YEAR;
const MID_RATIO_MIN: f64 = 1e-6;
const MID_RATIO_MAX: f64 = 1e6;
const LOCK_TICK_TOL: f64 = 1e-6;
const MIN_VOL: f64 = 1e-4;
const MAX_VOL: f64 = 5.0;
const IV_TOLERANCE: f64 = 1e-4;
const IV_REL_TOLERANCE: f64 = 1e-6;
const IV_MAX_ITERS: usize = 50;
const BISECT_MAX_ITERS: usize = 150;
const VEGA_EPS: f64 = 1e-8;
const NBBO_AGE_EPS_US: u64 = 1;
const INV_SQRT_TWO_PI: f64 = 0.3989422804014327;

// Flags bitfield
pub const FLAG_NO_UNDERLYING: u32 = 0b0001;
pub const FLAG_NO_IV: u32 = 0b0010;
pub const FLAG_TIME_EXPIRED: u32 = 0b0100;
pub const FLAG_NO_TREASURY: u32 = 0b1000;
pub const FLAG_CROSSED_MARKET: u32 = 0b1_0000;
pub const FLAG_LOCKED_MARKET: u32 = 0b10_0000;
pub const FLAG_BAD_RATIO: u32 = 0b100_0000;
pub const FLAG_MID_NONPOS: u32 = 0b1_0000_000;
pub const FLAG_NBBO_STALE: u32 = 0b10_0000_000;
pub const FLAG_IV_NEWTON_FAIL: u32 = 0b100_0000_000;
pub const FLAG_IV_BISECT_FAIL: u32 = 0b1_0000_0000;
pub const FLAG_VEGA_TINY: u32 = 0b10_0000_0000;
pub const FLAG_PRICE_BELOW_INTRINSIC: u32 = 0b100_0000_0000;
pub const FLAG_RATE_EXTRAP: u32 = 0b1_0000_0000_000;
pub const FLAG_TAU_CLAMPED: u32 = 0b10_0000_0000_000;

#[derive(Clone)]
struct QuoteSnapshot {
    bid: f64,
    ask: f64,
    bid_sz: u32,
    ask_sz: u32,
    quote_ts_ns: i64,
    state: NbboState,
}

impl From<Nbbo> for QuoteSnapshot {
    fn from(nbbo: Nbbo) -> Self {
        Self {
            bid: nbbo.bid,
            ask: nbbo.ask,
            bid_sz: nbbo.bid_sz,
            ask_sz: nbbo.ask_sz,
            quote_ts_ns: nbbo.quote_ts_ns,
            state: nbbo.state,
        }
    }
}

fn resolve_pool_permits(configured: usize, detected_cores: usize) -> usize {
    let detected = detected_cores.max(1);
    match configured {
        0 => detected,
        n => n.max(1).min(detected),
    }
}

fn runtime_pool_permits(configured: usize) -> usize {
    resolve_pool_permits(configured, num_cpus::get())
}

#[derive(Clone)]
pub struct GreeksEngine {
    cfg: GreeksConfig,
    max_concurrency: usize,
    nbbo: Arc<RwLock<NbboStore>>,
    staleness_us: u32,
    treasury_curve: Arc<RwLock<Option<Arc<TreasuryCurve>>>>,
}

impl GreeksEngine {
    pub fn new(
        cfg: GreeksConfig,
        nbbo: Arc<RwLock<NbboStore>>,
        staleness_us: u32,
        treasury_curve: Arc<RwLock<Option<Arc<TreasuryCurve>>>>,
    ) -> Self {
        let permits = runtime_pool_permits(cfg.pool_size);
        Self {
            cfg,
            max_concurrency: permits.max(1),
            nbbo,
            staleness_us,
            treasury_curve,
        }
    }

    pub async fn enrich_batch(&self, trades: &mut [OptionTrade]) {
        if trades.is_empty() {
            return;
        }
        let curve = {
            let guard = self.treasury_curve.read().await;
            guard.clone()
        };
        if curve.is_none() {
            for trade in trades.iter_mut() {
                trade.greeks_flags |= FLAG_NO_TREASURY;
            }
            return;
        }
        let curve = curve.unwrap();
        let cfg = Arc::new(self.cfg.clone());
        let nbbo = self.nbbo.clone();
        let staleness_us = self.staleness_us;
        let max_concurrency = self.max_concurrency;
        stream::iter(trades.iter_mut())
            .for_each_concurrent(Some(max_concurrency), |trade| {
                let cfg = Arc::clone(&cfg);
                let nbbo = nbbo.clone();
                let curve = curve.clone();
                async move {
                    enrich_single_trade(trade, cfg.as_ref(), nbbo, curve.as_ref(), staleness_us)
                        .await;
                }
            })
            .await;
    }
}

fn reset_trade_outputs(trade: &mut OptionTrade) {
    trade.delta = None;
    trade.gamma = None;
    trade.vega = None;
    trade.theta = None;
}

fn stored_underlying_quote(trade: &OptionTrade) -> Option<QuoteSnapshot> {
    let bid = trade.underlying_nbbo_bid?;
    let ask = trade.underlying_nbbo_ask?;
    let quote_ts_ns = trade.underlying_nbbo_ts_ns?;
    let state = trade
        .underlying_nbbo_state
        .clone()
        .unwrap_or(NbboState::Normal);
    Some(QuoteSnapshot {
        bid,
        ask,
        bid_sz: trade.underlying_nbbo_bid_sz.unwrap_or(0),
        ask_sz: trade.underlying_nbbo_ask_sz.unwrap_or(0),
        quote_ts_ns,
        state,
    })
}

async fn enrich_single_trade(
    trade: &mut OptionTrade,
    cfg: &GreeksConfig,
    nbbo: Arc<RwLock<NbboStore>>,
    curve: &TreasuryCurve,
    staleness_us: u32,
) {
    reset_trade_outputs(trade);
    let quote = match stored_underlying_quote(trade) {
        Some(snapshot) => Some(snapshot),
        None => {
            let store = nbbo.read().await;
            store
                .get_best_before(&trade.underlying, trade.trade_ts_ns, u32::MAX)
                .map(QuoteSnapshot::from)
        }
    };
    let Some(qte) = quote else {
        trade.greeks_flags |= FLAG_NO_UNDERLYING;
        return;
    };

    trade.nbbo_bid = Some(qte.bid);
    trade.nbbo_ask = Some(qte.ask);
    trade.nbbo_bid_sz = Some(qte.bid_sz);
    trade.nbbo_ask_sz = Some(qte.ask_sz);
    trade.nbbo_ts_ns = Some(qte.quote_ts_ns);
    let age_ns = trade.trade_ts_ns.saturating_sub(qte.quote_ts_ns);
    let age_us = (age_ns / 1_000).max(0) as u64;
    trade.nbbo_age_us = Some(age_us.min(u32::MAX as u64) as u32);
    trade.nbbo_state = Some(qte.state.clone());

    let stale_limit = staleness_us as u64;
    let limit_with_epsilon = if stale_limit == 0 {
        0
    } else {
        stale_limit.saturating_add(NBBO_AGE_EPS_US)
    };
    if age_us > limit_with_epsilon {
        trade.greeks_flags |= FLAG_NBBO_STALE | FLAG_NO_UNDERLYING;
        return;
    }

    let bid_valid = is_valid_quote_side(qte.bid);
    let ask_valid = is_valid_quote_side(qte.ask);
    if bid_valid && ask_valid {
        if qte.ask <= qte.bid {
            let spread = (qte.ask - qte.bid).abs();
            if spread <= LOCK_TICK_TOL {
                if !nbbo_allows_lock(&qte.state) {
                    trade.greeks_flags |= FLAG_LOCKED_MARKET | FLAG_NO_UNDERLYING;
                    return;
                }
            } else {
                trade.greeks_flags |= FLAG_CROSSED_MARKET | FLAG_NO_UNDERLYING;
                return;
            }
        }
    }
    let mid = match (bid_valid, ask_valid) {
        (true, true) => 0.5 * (qte.bid + qte.ask),
        (true, false) => qte.bid,
        (false, true) => qte.ask,
        _ => {
            trade.greeks_flags |= FLAG_MID_NONPOS | FLAG_NO_UNDERLYING;
            return;
        }
    };
    if !(mid.is_finite() && mid > 0.0) {
        trade.greeks_flags |= FLAG_MID_NONPOS | FLAG_NO_UNDERLYING;
        return;
    }

    let strike = trade.strike_price;
    if !strike.is_finite() || strike <= 0.0 {
        trade.greeks_flags |= FLAG_BAD_RATIO;
        return;
    }
    let ratio = mid / strike;
    if !ratio.is_finite() || ratio < MID_RATIO_MIN || ratio > MID_RATIO_MAX {
        trade.greeks_flags |= FLAG_BAD_RATIO;
        return;
    }

    let time_to_expiry_ns = trade.expiry_ts_ns.saturating_sub(trade.trade_ts_ns);
    if time_to_expiry_ns <= 0 {
        trade.greeks_flags |= FLAG_TIME_EXPIRED;
        return;
    }
    let tau_years_raw = (time_to_expiry_ns as f64) / NANOS_PER_SECOND / SECONDS_PER_YEAR;
    let tau_years = tau_years_raw.max(MIN_TAU_YEARS);
    if (tau_years_raw - tau_years).abs() > f64::EPSILON {
        trade.greeks_flags |= FLAG_TAU_CLAMPED;
    }

    let (rate, rate_extrapolated) = curve.rate_for_with_meta(tau_years);
    if rate_extrapolated {
        trade.greeks_flags |= FLAG_RATE_EXTRAP;
    }

    let dividend_yield = cfg.dividend_yield;
    let is_call = matches!(trade.contract_direction, 'C' | 'c');
    let target_price = trade.price;
    if !target_price.is_finite() || target_price <= 0.0 {
        trade.greeks_flags |= FLAG_NO_IV;
        return;
    }
    let intrinsic = intrinsic_value(is_call, mid, strike);
    if target_price < intrinsic - IV_TOLERANCE {
        trade.greeks_flags |= FLAG_PRICE_BELOW_INTRINSIC | FLAG_NO_IV;
        return;
    }

    let implied = trade
        .iv
        .filter(|iv| iv.is_finite() && *iv >= MIN_VOL && *iv <= MAX_VOL)
        .map(|vol| IvSolve {
            vol,
            newton_failed: false,
        })
        .or_else(|| {
            match solve_implied_vol(
                target_price,
                is_call,
                mid,
                strike,
                rate,
                dividend_yield,
                tau_years,
            ) {
                Ok(sol) => Some(sol),
                Err(IvError::PriceBelowIntrinsic) => {
                    trade.greeks_flags |= FLAG_PRICE_BELOW_INTRINSIC | FLAG_NO_IV;
                    None
                }
                Err(IvError::VegaTiny) => {
                    trade.greeks_flags |= FLAG_VEGA_TINY | FLAG_IV_NEWTON_FAIL | FLAG_NO_IV;
                    None
                }
                Err(IvError::BisectionFailed) => {
                    trade.greeks_flags |= FLAG_IV_BISECT_FAIL | FLAG_IV_NEWTON_FAIL | FLAG_NO_IV;
                    None
                }
                Err(IvError::InvalidInputs) => {
                    trade.greeks_flags |= FLAG_NO_IV;
                    None
                }
            }
        });

    let Some(solution) = implied else {
        return;
    };
    if solution.newton_failed {
        trade.greeks_flags |= FLAG_IV_NEWTON_FAIL;
    }

    let Some(greeks) = bs_price_and_greeks(
        is_call,
        mid,
        strike,
        rate,
        dividend_yield,
        solution.vol,
        tau_years,
    ) else {
        trade.greeks_flags |= FLAG_NO_IV;
        return;
    };
    trade.iv = Some(solution.vol);
    trade.delta = Some(greeks.delta);
    trade.gamma = Some(greeks.gamma);
    trade.vega = Some(greeks.vega);
    trade.theta = Some(greeks.theta);
}

fn is_valid_quote_side(value: f64) -> bool {
    value.is_finite() && value > 0.0
}

fn nbbo_allows_lock(state: &NbboState) -> bool {
    matches!(state, NbboState::Locked)
}

fn intrinsic_value(is_call: bool, spot: f64, strike: f64) -> f64 {
    if is_call {
        (spot - strike).max(0.0)
    } else {
        (strike - spot).max(0.0)
    }
}

struct GreeksResult {
    price: f64,
    delta: f64,
    gamma: f64,
    vega: f64,
    theta: f64,
}

#[derive(Clone, Copy, Debug)]
struct IvSolve {
    vol: f64,
    newton_failed: bool,
}

#[derive(Debug, PartialEq)]
enum IvError {
    InvalidInputs,
    PriceBelowIntrinsic,
    VegaTiny,
    BisectionFailed,
}

fn bs_price_and_greeks(
    is_call: bool,
    spot: f64,
    strike: f64,
    rate: f64,
    dividend_yield: f64,
    vol: f64,
    tau: f64,
) -> Option<GreeksResult> {
    if !(spot > 0.0 && strike > 0.0 && vol > 0.0 && tau > 0.0) {
        return None;
    }
    let sqrt_tau = tau.sqrt();
    if !sqrt_tau.is_finite() || sqrt_tau == 0.0 {
        return None;
    }
    let denom = vol * sqrt_tau;
    if denom <= 0.0 {
        return None;
    }
    let log_term = (spot / strike).ln();
    if !log_term.is_finite() {
        return None;
    }
    let drift = rate - dividend_yield + 0.5 * vol * vol;
    let d1 = (log_term + drift * tau) / denom;
    let d2 = d1 - denom;
    let disc_r = (-rate * tau).exp();
    let disc_q = (-dividend_yield * tau).exp();
    let pdf_d1 = norm_pdf(d1);
    let nd1 = norm_cdf(d1);
    let nd2 = norm_cdf(d2);
    let nneg_d1 = norm_cdf(-d1);
    let nneg_d2 = norm_cdf(-d2);
    let gamma = disc_q * pdf_d1 / (spot * denom);
    let vega = spot * disc_q * pdf_d1 * sqrt_tau;
    if !gamma.is_finite() || !vega.is_finite() {
        return None;
    }
    // Theta remains annualized (per-year); downstream callers may convert to per-day if desired.
    let (price, delta, theta) = if is_call {
        let price = spot * disc_q * nd1 - strike * disc_r * nd2;
        let delta = disc_q * nd1;
        let theta = -spot * disc_q * pdf_d1 * vol / (2.0 * sqrt_tau)
            + dividend_yield * spot * disc_q * nd1
            - rate * strike * disc_r * nd2;
        (price, delta, theta)
    } else {
        let price = strike * disc_r * nneg_d2 - spot * disc_q * nneg_d1;
        let delta = disc_q * (nd1 - 1.0);
        let theta = -spot * disc_q * pdf_d1 * vol / (2.0 * sqrt_tau)
            - dividend_yield * spot * disc_q * nneg_d1
            + rate * strike * disc_r * nneg_d2;
        (price, delta, theta)
    };
    Some(GreeksResult {
        price,
        delta,
        gamma,
        vega,
        theta,
    })
}

fn solve_implied_vol(
    target: f64,
    is_call: bool,
    spot: f64,
    strike: f64,
    rate: f64,
    dividend_yield: f64,
    tau: f64,
) -> Result<IvSolve, IvError> {
    if !(target.is_finite() && target > 0.0 && spot > 0.0 && strike > 0.0 && tau > 0.0) {
        return Err(IvError::InvalidInputs);
    }
    let intrinsic = intrinsic_value(is_call, spot, strike);
    if target < intrinsic - IV_TOLERANCE {
        return Err(IvError::PriceBelowIntrinsic);
    }
    let mut sigma = 0.3f64;
    for _ in 0..IV_MAX_ITERS {
        let Some(res) =
            bs_price_and_greeks(is_call, spot, strike, rate, dividend_yield, sigma, tau)
        else {
            break;
        };
        let diff = res.price - target;
        if diff.abs() < IV_TOLERANCE || (diff / target).abs() < IV_REL_TOLERANCE {
            return Ok(IvSolve {
                vol: sigma,
                newton_failed: false,
            });
        }
        if res.vega.abs() < VEGA_EPS {
            return Err(IvError::VegaTiny);
        }
        sigma -= diff / res.vega;
        if !sigma.is_finite() {
            break;
        }
        sigma = sigma.clamp(MIN_VOL, MAX_VOL);
    }
    let Some(vol) =
        solve_implied_vol_bisection(target, is_call, spot, strike, rate, dividend_yield, tau)
    else {
        return Err(IvError::BisectionFailed);
    };
    Ok(IvSolve {
        vol,
        newton_failed: true,
    })
}

fn solve_implied_vol_bisection(
    target: f64,
    is_call: bool,
    spot: f64,
    strike: f64,
    rate: f64,
    dividend_yield: f64,
    tau: f64,
) -> Option<f64> {
    if !(target.is_finite() && target > 0.0) {
        return None;
    }
    let mut low = MIN_VOL;
    let mut high = MAX_VOL;
    let low_price =
        bs_price_and_greeks(is_call, spot, strike, rate, dividend_yield, low, tau)?.price;
    let high_price =
        bs_price_and_greeks(is_call, spot, strike, rate, dividend_yield, high, tau)?.price;
    if target <= low_price {
        return Some(low);
    }
    if target > high_price {
        return None;
    }
    for _ in 0..BISECT_MAX_ITERS {
        let mid = 0.5 * (low + high);
        let res = bs_price_and_greeks(is_call, spot, strike, rate, dividend_yield, mid, tau)?;
        let diff = res.price - target;
        if diff.abs() < IV_TOLERANCE || (diff / target).abs() < IV_REL_TOLERANCE {
            return Some(mid);
        }
        if diff > 0.0 {
            high = mid;
        } else {
            low = mid;
        }
    }
    None
}

fn norm_pdf(x: f64) -> f64 {
    INV_SQRT_TWO_PI * (-0.5 * x * x).exp()
}

fn norm_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf(x / SQRT_2))
}

#[derive(Clone, Debug)]
pub struct TreasuryCurve {
    points: Vec<(f64, f64)>,
}

impl TreasuryCurve {
    /// Builds a curve from tenor/rate pairs (rates expressed in decimal form, e.g. 0.03 for 3%).
    pub fn from_pairs<I>(pairs: I) -> Option<Self>
    where
        I: IntoIterator<Item = (f64, f64)>,
    {
        let mut points: Vec<(f64, f64)> = pairs
            .into_iter()
            .filter(|(tenor, rate)| *tenor >= 0.0 && rate.is_finite())
            .collect();
        if points.is_empty() {
            return None;
        }
        points.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        points.dedup_by(|a, b| (a.0 - b.0).abs() < f64::EPSILON);
        Some(Self { points })
    }

    /// Returns an interpolated rate for the requested tenor in years.
    pub fn rate_for(&self, target_years: f64) -> f64 {
        self.rate_for_with_meta(target_years).0
    }

    /// Returns (rate, extrapolated_flag) for optional FLAG_RATE_EXTRAP tagging.
    pub fn rate_for_with_meta(&self, target_years: f64) -> (f64, bool) {
        if self.points.is_empty() {
            return (0.0, false);
        }
        let target = target_years.max(0.0);
        if target <= self.points[0].0 {
            return (self.points[0].1, false);
        }
        for window in self.points.windows(2) {
            let (t0, r0) = window[0];
            let (t1, r1) = window[1];
            if target <= t1 {
                let span = (t1 - t0).max(f64::EPSILON);
                let w = (target - t0) / span;
                let log_d0 = -r0 * t0;
                let log_d1 = -r1 * t1;
                let log_dt = log_d0 + (log_d1 - log_d0) * w;
                let denom = target.max(f64::EPSILON);
                let rate = -(log_dt) / denom;
                return (rate, false);
            }
        }
        (
            self.points.last().map(|(_, rate)| *rate).unwrap_or(0.0),
            true,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::types::{Nbbo, NbboState, Quality, Source};
    use core_types::uid::{option_trade_uid, quote_uid};
    use serde::{de::DeserializeOwned, Serialize};
    use std::{
        fs::File,
        io::{self, BufReader, BufWriter},
        path::Path,
    };
    use tempfile::TempDir;
    use window_space::{
        mapping::{QuoteBatchPayload, TradeBatchPayload},
        payload::{PayloadMeta, PayloadType, SlotStatus, TradeSlotKind},
        window::WindowIndex,
        WindowSpace, WindowSpaceConfig, WindowSpaceController,
    };

    const ONE_YEAR_NS: i64 = 31_536_000_000_000_000;
    const BASE_TRADE_TS_NS: i64 = 1_000_100_000;

    fn mk_nbbo(sym: &str, ts_ns: i64, bid: f64, ask: f64) -> Nbbo {
        let quote_uid = quote_uid(sym, ts_ns, None, bid, ask, 1, 1, Some(12), Some(11), None);
        Nbbo {
            instrument_id: sym.to_string(),
            quote_uid,
            quote_ts_ns: ts_ns,
            bid,
            ask,
            bid_sz: 1,
            ask_sz: 1,
            state: NbboState::Normal,
            condition: None,
            best_bid_venue: Some(12),
            best_ask_venue: Some(11),
            source: Source::Flatfile,
            quality: Quality::Prelim,
            watermark_ts_ns: 0,
        }
    }

    fn mk_nbbo_with_state(sym: &str, ts_ns: i64, bid: f64, ask: f64, state: NbboState) -> Nbbo {
        let mut nbbo = mk_nbbo(sym, ts_ns, bid, ask);
        nbbo.state = state;
        nbbo
    }

    fn base_cfg() -> GreeksConfig {
        GreeksConfig {
            pool_size: 2,
            dividend_yield: 0.0,
            flatfile_underlying_staleness_us: 1_000_000,
            realtime_underlying_staleness_us: 1_000_000,
            ..Default::default()
        }
    }

    fn sample_trade() -> OptionTrade {
        let trade_uid = option_trade_uid(
            "O:SPY250101C00400000",
            BASE_TRADE_TS_NS,
            None,
            1.0,
            1,
            11,
            &[],
        );
        OptionTrade {
            contract: "O:SPY250101C00400000".to_string(),
            trade_uid,
            contract_direction: 'C',
            strike_price: 400.0,
            underlying: "SPY".to_string(),
            trade_ts_ns: BASE_TRADE_TS_NS,
            price: 1.0,
            size: 1,
            conditions: vec![],
            exchange: 11,
            expiry_ts_ns: BASE_TRADE_TS_NS + ONE_YEAR_NS,
            aggressor_side: core_types::types::AggressorSide::Unknown,
            class_method: core_types::types::ClassMethod::Unknown,
            aggressor_offset_mid_bp: None,
            aggressor_offset_touch_ticks: None,
            aggressor_confidence: None,
            nbbo_bid: None,
            nbbo_ask: None,
            nbbo_bid_sz: None,
            nbbo_ask_sz: None,
            nbbo_ts_ns: None,
            nbbo_age_us: None,
            nbbo_state: None,
            underlying_nbbo_bid: None,
            underlying_nbbo_ask: None,
            underlying_nbbo_bid_sz: None,
            underlying_nbbo_ask_sz: None,
            underlying_nbbo_ts_ns: None,
            underlying_nbbo_age_us: None,
            underlying_nbbo_state: None,
            tick_size_used: None,
            delta: None,
            gamma: None,
            vega: None,
            theta: None,
            iv: Some(0.3),
            greeks_flags: 0,
            source: Source::Flatfile,
            quality: Quality::Prelim,
            watermark_ts_ns: 0,
        }
    }

    fn make_curve_state(points: &[(f64, f64)]) -> Arc<RwLock<Option<Arc<TreasuryCurve>>>> {
        let curve = TreasuryCurve::from_pairs(points.to_vec()).expect("valid curve");
        Arc::new(RwLock::new(Some(Arc::new(curve))))
    }

    fn bootstrap_test_controller() -> (Arc<WindowSpaceController>, TempDir) {
        let dir = tempfile::tempdir().expect("temp dir");
        let window_space = WindowSpace::standard(1_600_000_000);
        let mut config = WindowSpaceConfig::new(dir.path().to_path_buf(), window_space);
        config.max_symbols = 8;
        let (controller, _) = WindowSpaceController::bootstrap(config).expect("bootstrap");
        (Arc::new(controller), dir)
    }

    fn persist_payload<T: Serialize + ?Sized>(
        state_dir: &Path,
        file_name: &str,
        value: &T,
    ) -> io::Result<()> {
        let path = state_dir.join(file_name);
        let file = File::create(path)?;
        serde_json::to_writer(BufWriter::new(file), value).map_err(json_err)
    }

    fn load_payload<T: DeserializeOwned>(state_dir: &Path, file_name: &str) -> io::Result<Vec<T>> {
        let path = state_dir.join(file_name);
        let file = File::open(path)?;
        serde_json::from_reader(BufReader::new(file)).map_err(json_err)
    }

    fn stage_option_trade_payload(
        controller: &WindowSpaceController,
        symbol: &str,
        window_idx: WindowIndex,
        trades: &[OptionTrade],
        file_name: &str,
    ) -> io::Result<()> {
        persist_payload(controller.config().state_dir(), file_name, trades)?;
        let window_space = controller.window_space();
        let window_meta = window_space.window(window_idx).expect("window meta");
        let payload = TradeBatchPayload {
            schema_version: 1,
            window_ts: window_meta.start_ts,
            batch_id: 1,
            first_trade_ts: trades.first().map(|t| t.trade_ts_ns).unwrap_or_default(),
            last_trade_ts: trades.last().map(|t| t.trade_ts_ns).unwrap_or_default(),
            record_count: trades.len() as u32,
            artifact_uri: file_name.to_string(),
            checksum: 0,
        };
        let payload_id = {
            let mut stores = controller.payload_stores();
            stores.trades.append(payload)?
        };
        let meta = PayloadMeta::new(PayloadType::Trade, payload_id, 1, 0);
        controller
            .set_option_trade_ref(symbol, window_idx, meta, None)
            .expect("option trade slot");
        Ok(())
    }

    fn stage_quote_payload(
        controller: &WindowSpaceController,
        symbol: &str,
        window_idx: WindowIndex,
        quotes: &[Nbbo],
        slot: TradeSlotKind,
        file_name: &str,
    ) -> io::Result<()> {
        assert!(matches!(
            slot,
            TradeSlotKind::OptionQuote | TradeSlotKind::UnderlyingQuote
        ));
        persist_payload(controller.config().state_dir(), file_name, quotes)?;
        let window_space = controller.window_space();
        let window_meta = window_space.window(window_idx).expect("window meta");
        let payload = QuoteBatchPayload {
            schema_version: 1,
            window_ts: window_meta.start_ts,
            batch_id: 1,
            first_quote_ts: quotes.first().map(|q| q.quote_ts_ns).unwrap_or_default(),
            last_quote_ts: quotes.last().map(|q| q.quote_ts_ns).unwrap_or_default(),
            nbbo_sample_count: quotes.len() as u32,
            artifact_uri: file_name.to_string(),
            checksum: 0,
        };
        let payload_id = {
            let mut stores = controller.payload_stores();
            stores.quotes.append(payload)?
        };
        let meta = PayloadMeta::new(PayloadType::Quote, payload_id, 1, 0);
        match slot {
            TradeSlotKind::OptionQuote => {
                controller
                    .set_option_quote_ref(symbol, window_idx, meta, None)
                    .expect("option quote slot");
            }
            TradeSlotKind::UnderlyingQuote => {
                controller
                    .set_underlying_quote_ref(symbol, window_idx, meta, None)
                    .expect("underlying quote slot");
            }
            _ => unreachable!("unsupported slot for quote payload"),
        }
        Ok(())
    }

    fn json_err(err: serde_json::Error) -> io::Error {
        io::Error::new(io::ErrorKind::Other, err)
    }

    fn nbbo_age_us(trade_ts_ns: i64, quote_ts_ns: i64) -> u32 {
        let age_ns = trade_ts_ns.saturating_sub(quote_ts_ns);
        let age_us = (age_ns / 1_000).max(0) as u64;
        age_us.min(u32::MAX as u64) as u32
    }

    async fn attach_option_nbbo_snapshot(
        nbbo: Arc<RwLock<NbboStore>>,
        trade: &mut OptionTrade,
        staleness_us: u32,
    ) {
        let guard = nbbo.read().await;
        if let Some(q) = guard.get_best_before(&trade.contract, trade.trade_ts_ns, staleness_us) {
            let age_us = nbbo_age_us(trade.trade_ts_ns, q.quote_ts_ns);
            trade.nbbo_bid = Some(q.bid);
            trade.nbbo_ask = Some(q.ask);
            trade.nbbo_bid_sz = Some(q.bid_sz);
            trade.nbbo_ask_sz = Some(q.ask_sz);
            trade.nbbo_ts_ns = Some(q.quote_ts_ns);
            trade.nbbo_age_us = Some(age_us);
            trade.nbbo_state = Some(q.state.clone());
        } else {
            trade.nbbo_bid = None;
            trade.nbbo_ask = None;
            trade.nbbo_bid_sz = None;
            trade.nbbo_ask_sz = None;
            trade.nbbo_ts_ns = None;
            trade.nbbo_age_us = None;
            trade.nbbo_state = None;
        }
    }

    async fn attach_underlying_nbbo_snapshot(
        nbbo: Arc<RwLock<NbboStore>>,
        trade: &mut OptionTrade,
        staleness_us: u32,
    ) {
        let guard = nbbo.read().await;
        if let Some(q) = guard.get_best_before(&trade.underlying, trade.trade_ts_ns, staleness_us) {
            let age_us = nbbo_age_us(trade.trade_ts_ns, q.quote_ts_ns);
            trade.set_underlying_nbbo_snapshot(
                Some(q.bid),
                Some(q.ask),
                Some(q.bid_sz),
                Some(q.ask_sz),
                Some(q.quote_ts_ns),
                Some(age_us),
                Some(q.state.clone()),
            );
        } else {
            trade.set_underlying_nbbo_snapshot(None, None, None, None, None, None, None);
        }
    }

    #[tokio::test]
    async fn controller_pipeline_matches_quotes_and_computes_greeks() {
        let (controller, _dir) = bootstrap_test_controller();
        let state_dir = controller.config().state_dir().to_path_buf();
        let staleness_us = 250_000;
        let trade_ts_ns = 1_600_000_000i64 * 1_000_000_000 + 50_000;

        let mut base_trade = sample_trade();
        base_trade.trade_ts_ns = trade_ts_ns;
        base_trade.expiry_ts_ns = trade_ts_ns + ONE_YEAR_NS;
        base_trade.price = 5.0;
        base_trade.size = 10;
        base_trade.trade_uid = option_trade_uid(
            &base_trade.contract,
            base_trade.trade_ts_ns,
            None,
            base_trade.price,
            base_trade.size,
            base_trade.exchange,
            &base_trade.conditions,
        );

        let symbol = base_trade.underlying.clone();
        let option_quote = mk_nbbo(&base_trade.contract, trade_ts_ns - 5_000, 4.9, 5.05);
        let underlying_quote = mk_nbbo(&symbol, trade_ts_ns - 2_000, 400.0, 400.2);

        let window_idx = controller
            .window_idx_for_timestamp(trade_ts_ns / 1_000_000_000)
            .expect("window index");

        stage_option_trade_payload(
            &controller,
            &symbol,
            window_idx,
            &[base_trade.clone()],
            "option_trades.json",
        )
        .expect("stage option trades");
        stage_quote_payload(
            &controller,
            &symbol,
            window_idx,
            &[option_quote.clone()],
            TradeSlotKind::OptionQuote,
            "option_quotes.json",
        )
        .expect("stage option quotes");
        stage_quote_payload(
            &controller,
            &symbol,
            window_idx,
            &[underlying_quote.clone()],
            TradeSlotKind::UnderlyingQuote,
            "underlying_quotes.json",
        )
        .expect("stage underlying quotes");

        let trade_row = controller
            .get_trade_row(&symbol, window_idx)
            .expect("trade row");
        assert_eq!(trade_row.option_trade_ref.status, SlotStatus::Filled);
        assert_eq!(trade_row.option_quote_ref.status, SlotStatus::Filled);
        assert_eq!(trade_row.underlying_quote_ref.status, SlotStatus::Filled);

        let (trade_payload, option_quotes_payload, underlying_quotes_payload) = {
            let stores = controller.payload_stores();
            (
                stores
                    .trades
                    .get(trade_row.option_trade_ref.payload_id)
                    .cloned()
                    .expect("trade payload"),
                stores
                    .quotes
                    .get(trade_row.option_quote_ref.payload_id)
                    .cloned()
                    .expect("option quote payload"),
                stores
                    .quotes
                    .get(trade_row.underlying_quote_ref.payload_id)
                    .cloned()
                    .expect("underlying quote payload"),
            )
        };

        let mut trades: Vec<OptionTrade> =
            load_payload(state_dir.as_path(), &trade_payload.artifact_uri)
                .expect("option trades from payload");
        let option_quotes: Vec<Nbbo> =
            load_payload(state_dir.as_path(), &option_quotes_payload.artifact_uri)
                .expect("option quotes from payload");
        let underlying_quotes: Vec<Nbbo> =
            load_payload(state_dir.as_path(), &underlying_quotes_payload.artifact_uri)
                .expect("underlying quotes from payload");
        assert_eq!(trades.len(), 1);

        let mut trade = trades.remove(0);
        let nbbo_store = Arc::new(RwLock::new(NbboStore::new()));
        {
            let mut guard = nbbo_store.write().await;
            for q in option_quotes.iter().chain(underlying_quotes.iter()) {
                guard.put(q);
            }
        }

        attach_option_nbbo_snapshot(nbbo_store.clone(), &mut trade, staleness_us).await;
        assert_eq!(trade.nbbo_bid, Some(option_quote.bid));
        assert_eq!(trade.nbbo_ask, Some(option_quote.ask));

        attach_underlying_nbbo_snapshot(nbbo_store.clone(), &mut trade, staleness_us).await;
        assert_eq!(trade.underlying_nbbo_bid, Some(underlying_quote.bid));
        assert_eq!(trade.underlying_nbbo_ask, Some(underlying_quote.ask));

        let curve_state = make_curve_state(&[(0.5, 0.01), (1.0, 0.02)]);
        let engine = GreeksEngine::new(base_cfg(), nbbo_store.clone(), staleness_us, curve_state);
        let mut trades = vec![trade];
        engine.enrich_batch(&mut trades).await;

        let enriched = &trades[0];
        assert_eq!(
            enriched.greeks_flags, 0,
            "unexpected greeks flags: {}",
            enriched.greeks_flags
        );
        assert!(enriched.delta.is_some());
        assert!(enriched.gamma.is_some());
        assert!(enriched.vega.is_some());
        assert!(enriched.theta.is_some());
        assert_eq!(enriched.nbbo_bid, Some(underlying_quote.bid));
        assert_eq!(enriched.nbbo_ask, Some(underlying_quote.ask));
    }

    #[tokio::test]
    async fn test_enrich_batch_populates_greeks_and_snapshots() {
        let cfg = base_cfg();
        let store = Arc::new(RwLock::new(NbboStore::new()));
        {
            let mut w = store.write().await;
            w.put(&mk_nbbo("SPY", 1_000_000_000, 400.0, 400.1));
        }
        let curve_state = make_curve_state(&[(0.5, 0.01), (1.0, 0.02)]);
        let engine = GreeksEngine::new(cfg.clone(), store.clone(), 1_000_000, curve_state);
        let mut trades = vec![sample_trade()];
        engine.enrich_batch(&mut trades).await;
        let t = &trades[0];
        assert!(t.nbbo_bid.is_some() && t.nbbo_ask.is_some());
        assert!(t.nbbo_age_us.unwrap() <= 1_000_000);
        assert!(t.delta.is_some() && t.gamma.is_some() && t.vega.is_some() && t.theta.is_some());
        assert_eq!(t.greeks_flags, 0);
    }

    #[tokio::test]
    async fn crossed_market_sets_flag() {
        let cfg = base_cfg();
        let store = Arc::new(RwLock::new(NbboStore::new()));
        {
            let mut w = store.write().await;
            w.put(&mk_nbbo_with_state(
                "SPY",
                1_000_000_000,
                401.0,
                400.5,
                NbboState::Crossed,
            ));
        }
        let curve_state = make_curve_state(&[(0.5, 0.01), (1.0, 0.02)]);
        let engine = GreeksEngine::new(cfg, store.clone(), 1_000, curve_state);
        let mut trades = vec![sample_trade()];
        engine.enrich_batch(&mut trades).await;
        let flags = trades[0].greeks_flags;
        assert_ne!(flags & FLAG_CROSSED_MARKET, 0);
        assert_ne!(flags & FLAG_NO_UNDERLYING, 0);
        assert!(trades[0].delta.is_none());
    }

    #[tokio::test]
    async fn locked_market_without_state_sets_flag() {
        let cfg = base_cfg();
        let store = Arc::new(RwLock::new(NbboStore::new()));
        {
            let mut w = store.write().await;
            w.put(&mk_nbbo_with_state(
                "SPY",
                1_000_000_000,
                400.0,
                400.0,
                NbboState::Normal,
            ));
        }
        let curve_state = make_curve_state(&[(0.5, 0.01), (1.0, 0.02)]);
        let engine = GreeksEngine::new(cfg, store.clone(), 1_000, curve_state);
        let mut trades = vec![sample_trade()];
        engine.enrich_batch(&mut trades).await;
        let flags = trades[0].greeks_flags;
        assert_ne!(flags & FLAG_LOCKED_MARKET, 0);
        assert_ne!(flags & FLAG_NO_UNDERLYING, 0);
    }

    #[tokio::test]
    async fn extreme_ratio_sets_flag() {
        let cfg = base_cfg();
        let store = Arc::new(RwLock::new(NbboStore::new()));
        {
            let mut w = store.write().await;
            w.put(&mk_nbbo("SPY", 1_000_000_000, 400.0, 400.5));
        }
        let curve_state = make_curve_state(&[(0.5, 0.01), (1.0, 0.02)]);
        let engine = GreeksEngine::new(cfg, store.clone(), 1_000_000, curve_state);
        let mut trade = sample_trade();
        trade.strike_price = 1e-9;
        let mut trades = vec![trade];
        engine.enrich_batch(&mut trades).await;
        assert_ne!(trades[0].greeks_flags & FLAG_BAD_RATIO, 0);
    }

    #[tokio::test]
    async fn expired_option_sets_flag() {
        let cfg = base_cfg();
        let store = Arc::new(RwLock::new(NbboStore::new()));
        {
            let mut w = store.write().await;
            w.put(&mk_nbbo("SPY", 1_000_000_000, 400.0, 400.5));
        }
        let curve_state = make_curve_state(&[(0.5, 0.01), (1.0, 0.02)]);
        let engine = GreeksEngine::new(cfg, store.clone(), 1_000_000, curve_state);
        let mut trade = sample_trade();
        trade.expiry_ts_ns = trade.trade_ts_ns - 1;
        let mut trades = vec![trade];
        engine.enrich_batch(&mut trades).await;
        assert_ne!(trades[0].greeks_flags & FLAG_TIME_EXPIRED, 0);
    }

    #[tokio::test]
    async fn nbbo_stale_sets_flag() {
        let cfg = base_cfg();
        let store = Arc::new(RwLock::new(NbboStore::new()));
        {
            let mut w = store.write().await;
            w.put(&mk_nbbo("SPY", BASE_TRADE_TS_NS - 5_000_000, 400.0, 400.5));
        }
        let curve_state = make_curve_state(&[(0.5, 0.01), (1.0, 0.02)]);
        let engine = GreeksEngine::new(cfg, store.clone(), 1_000, curve_state);
        let mut trades = vec![sample_trade()];
        engine.enrich_batch(&mut trades).await;
        let flags = trades[0].greeks_flags;
        assert_ne!(flags & FLAG_NBBO_STALE, 0);
        assert_ne!(flags & FLAG_NO_UNDERLYING, 0);
    }

    #[tokio::test]
    async fn tau_clamped_sets_flag() {
        let cfg = base_cfg();
        let store = Arc::new(RwLock::new(NbboStore::new()));
        {
            let mut w = store.write().await;
            w.put(&mk_nbbo("SPY", 1_000_000_000, 400.0, 400.5));
        }
        let curve_state = make_curve_state(&[(0.5, 0.01), (1.0, 0.02)]);
        let engine = GreeksEngine::new(cfg, store.clone(), 1_000_000, curve_state);
        let mut trade = sample_trade();
        trade.expiry_ts_ns = trade.trade_ts_ns + 1_000;
        let mut trades = vec![trade];
        engine.enrich_batch(&mut trades).await;
        assert_ne!(trades[0].greeks_flags & FLAG_TAU_CLAMPED, 0);
        assert!(trades[0].delta.is_some());
    }

    #[tokio::test]
    async fn rate_extrapolation_sets_flag() {
        let cfg = base_cfg();
        let store = Arc::new(RwLock::new(NbboStore::new()));
        {
            let mut w = store.write().await;
            w.put(&mk_nbbo("SPY", 1_000_000_000, 400.0, 400.5));
        }
        let curve_state = make_curve_state(&[(0.5, 0.01), (1.0, 0.02)]);
        let engine = GreeksEngine::new(cfg, store.clone(), 1_000_000, curve_state);
        let mut trade = sample_trade();
        trade.expiry_ts_ns = trade.trade_ts_ns + ONE_YEAR_NS * 10;
        let mut trades = vec![trade];
        engine.enrich_batch(&mut trades).await;
        assert_ne!(trades[0].greeks_flags & FLAG_RATE_EXTRAP, 0);
    }

    #[tokio::test]
    async fn iv_price_below_intrinsic_sets_flag() {
        let cfg = base_cfg();
        let store = Arc::new(RwLock::new(NbboStore::new()));
        {
            let mut w = store.write().await;
            w.put(&mk_nbbo("SPY", 1_000_000_000, 420.0, 421.0));
        }
        let curve_state = make_curve_state(&[(0.5, 0.01), (1.0, 0.02)]);
        let engine = GreeksEngine::new(cfg, store.clone(), 1_000_000, curve_state);
        let mut trade = sample_trade();
        trade.iv = None;
        trade.price = 0.5;
        let mut trades = vec![trade];
        engine.enrich_batch(&mut trades).await;
        let flags = trades[0].greeks_flags;
        assert_ne!(flags & FLAG_PRICE_BELOW_INTRINSIC, 0);
        assert_ne!(flags & FLAG_NO_IV, 0);
    }

    #[tokio::test]
    async fn iv_bisection_failure_sets_flags() {
        let cfg = base_cfg();
        let store = Arc::new(RwLock::new(NbboStore::new()));
        {
            let mut w = store.write().await;
            w.put(&mk_nbbo("SPY", 1_000_000_000, 400.0, 400.5));
        }
        let curve_state = make_curve_state(&[(0.5, 0.01), (1.0, 0.02)]);
        let engine = GreeksEngine::new(cfg, store.clone(), 1_000_000, curve_state);
        let mut trade = sample_trade();
        trade.iv = None;
        trade.price = 1_000.0;
        let mut trades = vec![trade];
        engine.enrich_batch(&mut trades).await;
        let flags = trades[0].greeks_flags;
        assert_ne!(flags & FLAG_IV_BISECT_FAIL, 0);
        assert_ne!(flags & FLAG_IV_NEWTON_FAIL, 0);
        assert_ne!(flags & FLAG_NO_IV, 0);
    }

    #[test]
    fn test_treasury_curve_interpolates_log_linear() {
        let curve = TreasuryCurve::from_pairs(vec![(0.5, 0.01), (1.0, 0.02), (2.0, 0.03)]).unwrap();
        assert!((curve.rate_for(0.25) - 0.01).abs() < 1e-9);
        let (rate, extrap) = curve.rate_for_with_meta(0.75);
        assert!((rate - 0.016_666_666_666_666_666).abs() < 1e-9);
        assert!(!extrap);
    }

    #[test]
    fn test_treasury_curve_flags_extrapolation() {
        let curve = TreasuryCurve::from_pairs(vec![(0.5, 0.01), (1.0, 0.02), (2.0, 0.03)]).unwrap();
        let (rate, extrap) = curve.rate_for_with_meta(5.0);
        assert!(extrap);
        assert!((rate - 0.03).abs() < 1e-9);
    }

    #[test]
    fn test_treasury_curve_handles_empty_pairs() {
        assert!(TreasuryCurve::from_pairs(Vec::<(f64, f64)>::new()).is_none());
    }

    #[test]
    fn solve_iv_detects_price_below_intrinsic() {
        let err =
            solve_implied_vol(5.0, true, 110.0, 100.0, 0.0, 0.0, 0.5).expect_err("should error");
        assert_eq!(err, IvError::PriceBelowIntrinsic);
    }

    #[test]
    fn solve_iv_returns_vega_tiny_error() {
        let err = solve_implied_vol(0.5, true, 1.0, 1.0, 0.0, 0.0, 1e-16).expect_err("tiny tau");
        assert_eq!(err, IvError::VegaTiny);
    }

    #[test]
    fn pool_resolution_respects_cpu_cap() {
        assert_eq!(super::resolve_pool_permits(8, 2), 2);
    }

    #[test]
    fn pool_resolution_falls_back_to_cpu_count() {
        assert_eq!(super::resolve_pool_permits(0, 6), 6);
    }
}
