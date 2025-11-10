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
