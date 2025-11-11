// Copyright (c) James Kassemi, SC, US. All rights reserved.
use core_types::config::GreeksConfig;
use core_types::types::OptionTrade;
use futures::{stream, StreamExt};
use libm::erf;
use nbbo_cache::NbboStore;
use num_cpus;
use std::cmp::Ordering;
use std::f64::consts::SQRT_2;
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};

const NANOS_PER_SECOND: f64 = 1_000_000_000.0;
const SECONDS_PER_YEAR: f64 = 31_536_000.0;
const MIN_TAU_YEARS: f64 = 1.0 / SECONDS_PER_YEAR;
const MIN_VOL: f64 = 1e-4;
const MAX_VOL: f64 = 5.0;
const IV_TOLERANCE: f64 = 1e-4;
const IV_MAX_ITERS: usize = 50;
const INV_SQRT_TWO_PI: f64 = 0.3989422804014327;

// Flags bitfield
pub const FLAG_NO_UNDERLYING: u32 = 0b0001;
pub const FLAG_NO_IV: u32 = 0b0010;
pub const FLAG_TIME_EXPIRED: u32 = 0b0100;
pub const FLAG_NO_TREASURY: u32 = 0b1000;

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
    pool: Arc<Semaphore>,
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
        let pool = Arc::new(Semaphore::new(permits));
        Self {
            cfg,
            pool,
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
        let pool = self.pool.clone();
        let staleness_us = self.staleness_us;

        stream::iter(trades.iter_mut())
            .for_each_concurrent(None, |trade| {
                let cfg = Arc::clone(&cfg);
                let nbbo = nbbo.clone();
                let curve = curve.clone();
                let pool = pool.clone();
                async move {
                    if let Ok(_permit) = pool.acquire_owned().await {
                        enrich_single_trade(trade, cfg.as_ref(), nbbo, curve, staleness_us).await;
                    } else {
                        enrich_single_trade(trade, cfg.as_ref(), nbbo, curve, staleness_us).await;
                    }
                }
            })
            .await;
    }
}

async fn enrich_single_trade(
    trade: &mut OptionTrade,
    cfg: &GreeksConfig,
    nbbo: Arc<RwLock<NbboStore>>,
    curve: Arc<TreasuryCurve>,
    staleness_us: u32,
) {
    trade.delta = None;
    trade.gamma = None;
    trade.vega = None;
    trade.theta = None;
    let quote = {
        let store = nbbo.read().await;
        store.get_best_before(&trade.underlying, trade.trade_ts_ns, staleness_us)
    };
    let Some(qte) = quote else {
        trade.greeks_flags |= FLAG_NO_UNDERLYING;
        return;
    };
    let mid = if qte.ask.is_finite() && qte.ask > 0.0 {
        0.5 * (qte.bid + qte.ask)
    } else {
        qte.bid
    };
    if !mid.is_finite() || mid <= 0.0 {
        trade.greeks_flags |= FLAG_NO_UNDERLYING;
        return;
    }
    let age_ns = trade.trade_ts_ns.saturating_sub(qte.quote_ts_ns);
    let age_us = (age_ns / 1_000).max(0) as u64;
    trade.nbbo_bid = Some(qte.bid);
    trade.nbbo_ask = Some(qte.ask);
    trade.nbbo_bid_sz = Some(qte.bid_sz);
    trade.nbbo_ask_sz = Some(qte.ask_sz);
    trade.nbbo_ts_ns = Some(qte.quote_ts_ns);
    trade.nbbo_age_us = Some(age_us.min(u32::MAX as u64) as u32);
    trade.nbbo_state = Some(qte.state.clone());

    let time_to_expiry_ns = trade.expiry_ts_ns.saturating_sub(trade.trade_ts_ns);
    if time_to_expiry_ns <= 0 {
        trade.greeks_flags |= FLAG_TIME_EXPIRED;
        return;
    }
    let tau_years =
        ((time_to_expiry_ns as f64) / NANOS_PER_SECOND / SECONDS_PER_YEAR).max(MIN_TAU_YEARS);
    let rate = curve.rate_for(tau_years);
    let dividend_yield = cfg.dividend_yield;
    let is_call = matches!(trade.contract_direction, 'C' | 'c');
    let mut sigma = trade
        .iv
        .filter(|iv| iv.is_finite() && *iv >= MIN_VOL && *iv <= MAX_VOL);
    if sigma.is_none() {
        sigma = solve_implied_vol(
            trade.price,
            is_call,
            mid,
            trade.strike_price,
            rate,
            dividend_yield,
            tau_years,
        );
    }
    let Some(vol) = sigma else {
        trade.greeks_flags |= FLAG_NO_IV;
        return;
    };
    let Some(greeks) = bs_price_and_greeks(
        is_call,
        mid,
        trade.strike_price,
        rate,
        dividend_yield,
        vol,
        tau_years,
    ) else {
        trade.greeks_flags |= FLAG_NO_IV;
        return;
    };
    trade.iv = Some(vol);
    trade.delta = Some(greeks.delta);
    trade.gamma = Some(greeks.gamma);
    trade.vega = Some(greeks.vega);
    trade.theta = Some(greeks.theta);
}

struct GreeksResult {
    price: f64,
    delta: f64,
    gamma: f64,
    vega: f64,
    theta: f64,
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
) -> Option<f64> {
    if !target.is_finite() || target <= 0.0 || spot <= 0.0 || strike <= 0.0 {
        return None;
    }
    let intrinsic = if is_call {
        (spot - strike).max(0.0)
    } else {
        (strike - spot).max(0.0)
    };
    if target < intrinsic - 1e-6 {
        return None;
    }
    let mut sigma = 0.3f64;
    for _ in 0..IV_MAX_ITERS {
        let Some(res) =
            bs_price_and_greeks(is_call, spot, strike, rate, dividend_yield, sigma, tau)
        else {
            break;
        };
        let diff = res.price - target;
        if diff.abs() < IV_TOLERANCE {
            return Some(sigma);
        }
        if res.vega.abs() < 1e-8 {
            break;
        }
        sigma -= diff / res.vega;
        if !sigma.is_finite() {
            break;
        }
        sigma = sigma.clamp(MIN_VOL, MAX_VOL);
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
        points.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
        points.dedup_by(|a, b| (a.0 - b.0).abs() < f64::EPSILON);
        if points.len() < 1 {
            return None;
        }
        for window in points.windows(2) {
            if window[1].0 <= window[0].0 {
                return None;
            }
        }
        Some(Self { points })
    }

    /// Returns an interpolated rate for the requested tenor in years.
    pub fn rate_for(&self, target_years: f64) -> f64 {
        if self.points.is_empty() {
            return 0.0;
        }
        let target = target_years.max(0.0);
        if target <= self.points[0].0 {
            return self.points[0].1;
        }
        for window in self.points.windows(2) {
            let (t0, r0) = window[0];
            let (t1, r1) = window[1];
            if target <= t1 {
                let span = (t1 - t0).max(f64::EPSILON);
                let w = (target - t0) / span;
                return r0 + (r1 - r0) * w;
            }
        }
        self.points.last().map(|(_, rate)| *rate).unwrap_or(0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_types::types::{Nbbo, NbboState, Quality, Source};

    fn mk_nbbo(sym: &str, ts_ns: i64, bid: f64, ask: f64) -> Nbbo {
        Nbbo {
            instrument_id: sym.to_string(),
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

    #[tokio::test]
    async fn test_enrich_batch_populates_greeks_and_snapshots() {
        let cfg = GreeksConfig {
            pool_size: 2,
            dividend_yield: 0.0,
            flatfile_underlying_staleness_us: 1_000_000,
            realtime_underlying_staleness_us: 1_000_000,
            ..Default::default()
        };
        let store = Arc::new(RwLock::new(NbboStore::new()));
        {
            let mut w = store.write().await;
            w.put(&mk_nbbo("SPY", 1_000_000_000, 400.0, 400.1));
        }
        let curve_state = Arc::new(RwLock::new(None));
        {
            let mut guard = curve_state.write().await;
            *guard = TreasuryCurve::from_pairs(vec![(0.5, 0.01), (1.0, 0.02)]).map(Arc::new);
        }
        let engine = GreeksEngine::new(cfg.clone(), store.clone(), 1_000_000, curve_state);
        let mut trades = vec![OptionTrade {
            contract: "O:SPY250101C00400000".to_string(),
            contract_direction: 'C',
            strike_price: 400.0,
            underlying: "SPY".to_string(),
            trade_ts_ns: 1_000_100_000,
            price: 1.0,
            size: 1,
            conditions: vec![],
            exchange: 11,
            expiry_ts_ns: 1_000_000_000 + 31_536_000_000,
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
        }];
        engine.enrich_batch(&mut trades).await;
        let t = &trades[0];
        assert!(t.nbbo_bid.is_some() && t.nbbo_ask.is_some());
        assert!(t.nbbo_age_us.unwrap() <= 1_000_000);
        assert!(t.delta.is_some() && t.gamma.is_some() && t.vega.is_some() && t.theta.is_some());
        assert_eq!(t.greeks_flags, 0);
    }

    #[test]
    fn test_treasury_curve_interpolates() {
        let curve = TreasuryCurve::from_pairs(vec![(0.5, 0.01), (1.0, 0.02), (2.0, 0.03)]).unwrap();
        assert!((curve.rate_for(0.25) - 0.01).abs() < 1e-9);
        assert!((curve.rate_for(0.75) - 0.015).abs() < 1e-9);
        assert!((curve.rate_for(5.0) - 0.03).abs() < 1e-9);
    }

    #[test]
    fn test_treasury_curve_handles_empty_pairs() {
        assert!(TreasuryCurve::from_pairs(Vec::<(f64, f64)>::new()).is_none());
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
