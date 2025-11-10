use core_types::config::GreeksConfig;
use core_types::types::{OptionTrade, TradeLike};
use nbbo_cache::NbboStore;
use std::sync::Arc;
use tokio::sync::{Semaphore, RwLock};
use black_scholes::*;

// Flags bitfield
pub const FLAG_NO_UNDERLYING: u32 = 0b0001;
pub const FLAG_NO_IV: u32 = 0b0010;
pub const FLAG_TIME_EXPIRED: u32 = 0b0100;

#[derive(Clone)]
pub struct GreeksEngine {
    cfg: GreeksConfig,
    pool: Arc<Semaphore>,
    nbbo: Arc<RwLock<NbboStore>>,
    staleness_us: u32,
}

impl GreeksEngine {
    pub fn new(cfg: GreeksConfig, nbbo: Arc<RwLock<NbboStore>>, staleness_us: u32) -> Self {
        let pool = Arc::new(Semaphore::new(std::cmp::max(1, cfg.pool_size)));
        Self { cfg, pool, nbbo, staleness_us }
    }

    pub async fn enrich_batch(&self, trades: &mut [OptionTrade]) {
        let permits = self.pool.clone();
        let r = self.cfg.risk_free_rate;
        let q = self.cfg.dividend_yield;
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
            let t_years = ((t.expiry_ts_ns - t.trade_ts_ns) as f64 / 1_000_000_000f64 / 31_536_000f64).max(0.0);
            if t_years <= 0.0 { flags |= FLAG_TIME_EXPIRED; }

            // Sigma (IV) optional; if None, skip for now
            let sigma = t.iv;
            if quote.is_none() || sigma.is_none() || t_years <= 0.0 {
                t.greeks_flags |= flags | if sigma.is_none() { FLAG_NO_IV } else { 0 };
                continue;
            }
            let sigma = sigma.unwrap();

            // Compute greeks via black_scholes crate
            let is_call = t.contract_direction == 'C';
            let delta = if is_call { call_delta(s, k, r, sigma, t_years) } else { put_delta(s, k, r, sigma, t_years) };
            let gamma = if is_call { call_gamma(s, k, r, sigma, t_years) } else { put_gamma(s, k, r, sigma, t_years) };
            let vega = if is_call { call_vega(s, k, r, sigma, t_years) } else { put_vega(s, k, r, sigma, t_years) };
            let theta = if is_call { call_theta(s, k, r, sigma, t_years) } else { put_theta(s, k, r, sigma, t_years) };
            t.delta = Some(delta);
            t.gamma = Some(gamma);
            t.vega = Some(vega);
            t.theta = Some(theta);
            t.greeks_flags |= flags;
        }
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
            risk_free_rate: 0.02,
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
        let engine = GreeksEngine::new(cfg.clone(), store.clone(), 1_000_000);
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
}
