// Copyright (c) James Kassemi, SC, US. All rights reserved.
use black_scholes::*;
use core_types::config::GreeksConfig;
use core_types::types::OptionTrade;
use nbbo_cache::NbboStore;
use std::cmp::Ordering;
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};

// Flags bitfield
pub const FLAG_NO_UNDERLYING: u32 = 0b0001;
pub const FLAG_NO_IV: u32 = 0b0010;
pub const FLAG_TIME_EXPIRED: u32 = 0b0100;
pub const FLAG_NO_TREASURY: u32 = 0b1000;

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
        let pool = Arc::new(Semaphore::new(std::cmp::max(1, cfg.pool_size)));
        Self {
            cfg,
            pool,
            nbbo,
            staleness_us,
            treasury_curve,
        }
    }

    pub async fn enrich_batch(&self, trades: &mut [OptionTrade]) {
        let _permits = self.pool.clone();
        let _q = self.cfg.dividend_yield;
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
        // simple sequential for now; hook pool for heavy work like IV root-finding in future
        for t in trades.iter_mut() {
            let mut flags = 0u32;
            // Underlying S from NBBO bid
            let und = &t.underlying;
            let quote = {
                let store = self.nbbo.read().await;
                store.get_best_before(und, t.trade_ts_ns, self.staleness_us)
            };
            if quote.is_none() || quote.as_ref().unwrap().bid <= 0.0 {
                flags |= FLAG_NO_UNDERLYING;
            }
            let s = quote.as_ref().map(|q| q.bid).unwrap_or(0.0);
            if let Some(qte) = quote.as_ref() {
                t.nbbo_bid = Some(qte.bid);
                t.nbbo_ask = Some(qte.ask);
                t.nbbo_bid_sz = Some(qte.bid_sz);
                t.nbbo_ask_sz = Some(qte.ask_sz);
                t.nbbo_ts_ns = Some(qte.quote_ts_ns);
                t.nbbo_age_us = Some(((t.trade_ts_ns - qte.quote_ts_ns) / 1_000) as u32);
                t.nbbo_state = Some(qte.state.clone());
            }
            // Inputs
            let k = t.strike_price;
            let t_years =
                ((t.expiry_ts_ns - t.trade_ts_ns) as f64 / 1_000_000_000f64 / 31_536_000f64)
                    .max(0.0);
            if t_years <= 0.0 {
                flags |= FLAG_TIME_EXPIRED;
            }
            let r = curve.rate_for(t_years);

            // Sigma (IV) optional; if None, skip for now
            let sigma = t.iv;
            if quote.is_none() || sigma.is_none() || t_years <= 0.0 {
                t.greeks_flags |= flags | if sigma.is_none() { FLAG_NO_IV } else { 0 };
                continue;
            }
            let sigma = sigma.unwrap();

            // Compute greeks via black_scholes crate
            let is_call = t.contract_direction == 'C';
            let delta = if is_call {
                call_delta(s, k, r, sigma, t_years)
            } else {
                put_delta(s, k, r, sigma, t_years)
            };
            let gamma = if is_call {
                call_gamma(s, k, r, sigma, t_years)
            } else {
                put_gamma(s, k, r, sigma, t_years)
            };
            let vega = if is_call {
                call_vega(s, k, r, sigma, t_years)
            } else {
                put_vega(s, k, r, sigma, t_years)
            };
            let theta = if is_call {
                call_theta(s, k, r, sigma, t_years)
            } else {
                put_theta(s, k, r, sigma, t_years)
            };
            t.delta = Some(delta);
            t.gamma = Some(gamma);
            t.vega = Some(vega);
            t.theta = Some(theta);
            t.greeks_flags |= flags;
        }
    }
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
            trade_ts_ns: 1_000_100_000, // 100us later
            price: 1.0,
            size: 1,
            conditions: vec![],
            exchange: 11,
            expiry_ts_ns: 1_000_000_000 + 31_536_000_000, // +1 year
            aggressor_side: core_types::types::AggressorSide::Unknown,
            class_method: core_types::types::ClassMethod::Unknown,
            aggressor_offset_mid_bp: None,
            aggressor_offset_touch_ticks: None,
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
}
