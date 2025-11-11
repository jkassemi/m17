// Copyright (c) James Kassemi, SC, US. All rights reserved.
use aggregations::AggregationEvent;
use classifier::greeks::GreeksEngine;
use core_types::config::WsConfig;
use core_types::status::{OverallStatus, ServiceStatusHandle, StatusGauge};
use core_types::types::{
    Completeness, DataBatch, DataBatchMeta, OptionTrade, Quality, Source, Watermark,
};
use futures::StreamExt;
use log::error;
use metrics::Metrics;
use nbbo_cache::NbboStore;
use options_universe_ingestion_service::OptionsUniverseIngestionService;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use storage::Storage;
use tokio::sync::{mpsc, RwLock as TokioRwLock};
use tokio::time::sleep;
use treasury_ingestion_service::TreasuryServiceHandle;
use ws_source::worker::{ResourceKind, SubscriptionSource, WsMessage, WsWorker};

pub struct RealtimeWsIngestionService {
    ws_cfg: WsConfig,
    nbbo: Arc<TokioRwLock<NbboStore>>,
    storage: Arc<Mutex<Storage>>,
    metrics: Arc<Metrics>,
    greeks_cfg: core_types::config::GreeksConfig,
    staleness_us: u32,
    treasury: TreasuryServiceHandle,
    agg_sender: Option<mpsc::Sender<AggregationEvent>>,
    status: ServiceStatusHandle,
}

impl RealtimeWsIngestionService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ws_cfg: WsConfig,
        nbbo: Arc<TokioRwLock<NbboStore>>,
        storage: Arc<Mutex<Storage>>,
        metrics: Arc<Metrics>,
        greeks_cfg: core_types::config::GreeksConfig,
        staleness_us: u32,
        treasury: TreasuryServiceHandle,
        agg_sender: Option<mpsc::Sender<AggregationEvent>>,
    ) -> Self {
        let status = ServiceStatusHandle::new("realtime_ws");
        status.set_overall(OverallStatus::Warn);
        status.push_warning("realtime websocket ingestion not started");
        Self {
            ws_cfg,
            nbbo,
            storage,
            metrics,
            greeks_cfg,
            staleness_us,
            treasury,
            agg_sender,
            status,
        }
    }

    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run().await })
    }

    pub fn status_handle(&self) -> ServiceStatusHandle {
        self.status.clone()
    }

    async fn run(self) {
        let underlying = self.ws_cfg.underlying_symbol.trim().to_string();
        if underlying.is_empty() {
            error!("ws underlying_symbol is empty; skipping realtime feeds");
            self.status.set_overall(OverallStatus::Crit);
            self.status
                .push_error("ws underlying_symbol is empty; cannot start realtime feeds");
            return;
        }
        self.status.clear_warnings_matching(|_| true);
        self.status.set_overall(OverallStatus::Ok);
        self.spawn_quote_worker(&underlying);
        self.spawn_equity_trade_worker(&underlying);
        self.spawn_options_worker(&underlying).await;
    }

    fn spawn_quote_worker(&self, underlying: &str) {
        let quote_topic = format!("Q.{}", underlying);
        let worker = WsWorker::new(
            &self.ws_cfg.stocks_ws_url,
            ResourceKind::EquityQuotes,
            self.ws_cfg.api_key.clone(),
            SubscriptionSource::Static(vec![quote_topic]),
        );
        let nbbo_rt = self.nbbo.clone();
        let status = self.status.clone();
        tokio::spawn(async move {
            match worker.stream().await {
                Ok(mut stream) => {
                    status.clear_errors_matching(|m| m.contains("quotes stream"));
                    while let Some(msg) = stream.next().await {
                        if let WsMessage::Nbbo(nbbo) = msg {
                            let mut guard = nbbo_rt.write().await;
                            guard.put(&nbbo);
                            guard.prune_before(nbbo.quote_ts_ns.saturating_sub(2_000_000_000));
                        }
                    }
                }
                Err(err) => {
                    status.set_overall(OverallStatus::Crit);
                    status.push_error(format!("ws quotes stream error: {}", err));
                    error!("ws quotes stream error: {}", err)
                }
            }
        });
    }

    fn spawn_equity_trade_worker(&self, underlying: &str) {
        if self.agg_sender.is_none() {
            return;
        }
        let trades_worker = WsWorker::new(
            &self.ws_cfg.stocks_ws_url,
            ResourceKind::EquityTrades,
            self.ws_cfg.api_key.clone(),
            SubscriptionSource::Static(vec![format!("T.{}", underlying)]),
        );
        let sender = self.agg_sender.clone().unwrap();
        let status = self.status.clone();
        tokio::spawn(async move {
            match trades_worker.stream().await {
                Ok(mut stream) => {
                    status.clear_errors_matching(|m| m.contains("equities trade stream"));
                    while let Some(msg) = stream.next().await {
                        if let WsMessage::EquityTrade(trade) = msg {
                            let _ = sender.send(AggregationEvent::UnderlyingTrade(trade)).await;
                        }
                    }
                }
                Err(err) => {
                    status.set_overall(OverallStatus::Crit);
                    status.push_error(format!("ws equities trade stream error: {}", err));
                    error!("ws equities trade stream error: {}", err);
                }
            }
        });
    }

    async fn spawn_options_worker(&self, underlying: &str) {
        let Some(api_key) = self.ws_cfg.api_key.clone() else {
            error!("ws api_key missing; cannot start options trades");
            self.status
                .push_error("ws api_key missing; cannot start options trades");
            self.status.set_overall(OverallStatus::Crit);
            return;
        };
        let service = OptionsUniverseIngestionService::new(
            reqwest::Client::new(),
            self.ws_cfg.rest_base_url.clone(),
            api_key.clone(),
            underlying.to_string(),
            self.ws_cfg.options_contract_limit,
            Duration::from_secs(self.ws_cfg.options_refresh_interval_s.max(60)),
        );
        let options_rx = service.spawn();
        let options_worker = WsWorker::new(
            &self.ws_cfg.options_ws_url,
            ResourceKind::OptionsTrades,
            self.ws_cfg.api_key.clone(),
            SubscriptionSource::Dynamic(options_rx),
        );
        let storage = self.storage.clone();
        let metrics = self.metrics.clone();
        let nbbo_lookup = self.nbbo.clone();
        let agg_sender = self.agg_sender.clone();
        let staleness_us = self.staleness_us;
        let batch_cap = self.ws_cfg.batch_size.max(1);
        let greeks_engine = GreeksEngine::new(
            self.greeks_cfg.clone(),
            self.nbbo.clone(),
            self.staleness_us,
            self.treasury.latest_curve_state(),
        );
        let treasury = self.treasury.clone();
        let status = self.status.clone();
        tokio::spawn(async move {
            // Block until treasury has at least one curve before streaming trades.
            loop {
                if treasury.latest_curve().await.is_some() {
                    status.clear_warnings_matching(|m| m.contains("treasury data"));
                    break;
                }
                status.set_overall(OverallStatus::Crit);
                status.push_warning("waiting for treasury data before pricing realtime options");
                sleep(Duration::from_secs(1)).await;
            }
            match options_worker.stream().await {
                Ok(mut stream) => {
                    status.clear_errors_matching(|m| m.contains("options stream"));
                    let mut pending: Vec<OptionTrade> = Vec::with_capacity(batch_cap);
                    while let Some(msg) = stream.next().await {
                        if let WsMessage::OptionTrade(trade) = msg {
                            if treasury.latest_curve().await.is_none() {
                                status
                                    .clear_errors_matching(|m| m.contains("treasury unavailable"));
                                status.set_overall(OverallStatus::Crit);
                                status.push_error(
                                    "treasury unavailable; pausing realtime option pricing",
                                );
                                sleep(Duration::from_millis(250)).await;
                                continue;
                            } else {
                                status
                                    .clear_errors_matching(|m| m.contains("treasury unavailable"));
                                status.set_overall(OverallStatus::Ok);
                            }
                            if let Some(sender) = &agg_sender {
                                let underlying_price = {
                                    let store = nbbo_lookup.read().await;
                                    store
                                        .get_best_before(
                                            &trade.underlying,
                                            trade.trade_ts_ns,
                                            staleness_us,
                                        )
                                        .map(|q| q.bid)
                                };
                                let _ = sender
                                    .send(AggregationEvent::OptionTrade {
                                        trade: trade.clone(),
                                        underlying_price,
                                    })
                                    .await;
                            }
                            pending.push(trade);
                            status.set_gauges(vec![StatusGauge {
                                label: "pending_ws_batch".to_string(),
                                value: pending.len() as f64,
                                max: Some(batch_cap as f64),
                                unit: Some("trades".to_string()),
                                details: None,
                            }]);
                            if pending.len() >= batch_cap {
                                persist_realtime_options(
                                    &mut pending,
                                    &greeks_engine,
                                    &storage,
                                    &metrics,
                                )
                                .await;
                                status.set_gauges(vec![StatusGauge {
                                    label: "pending_ws_batch".to_string(),
                                    value: 0.0,
                                    max: Some(batch_cap as f64),
                                    unit: Some("trades".to_string()),
                                    details: None,
                                }]);
                            }
                        }
                    }
                    if !pending.is_empty() {
                        persist_realtime_options(&mut pending, &greeks_engine, &storage, &metrics)
                            .await;
                        status.set_gauges(vec![StatusGauge {
                            label: "pending_ws_batch".to_string(),
                            value: 0.0,
                            max: Some(batch_cap as f64),
                            unit: Some("trades".to_string()),
                            details: None,
                        }]);
                    }
                }
                Err(err) => {
                    status.set_overall(OverallStatus::Crit);
                    status.push_error(format!("ws options stream error: {}", err));
                    error!("ws options stream error: {}", err);
                }
            }
        });
    }
}

async fn persist_realtime_options(
    rows: &mut Vec<OptionTrade>,
    greeks: &GreeksEngine,
    storage: &Arc<Mutex<Storage>>,
    metrics: &Arc<Metrics>,
) {
    if rows.is_empty() {
        return;
    }
    greeks.enrich_batch(rows).await;
    let watermark = rows.last().map(|t| t.trade_ts_ns).unwrap_or(0);
    let meta = DataBatchMeta {
        source: Source::Ws,
        quality: Quality::Prelim,
        watermark: Watermark {
            watermark_ts_ns: watermark,
            completeness: Completeness::Unknown,
            hints: None,
        },
        schema_version: 1,
    };
    let batch = DataBatch {
        rows: std::mem::take(rows),
        meta,
    };
    metrics.inc_batches(1);
    metrics.inc_rows(batch.rows.len() as u64);
    if let Err(e) = storage.lock().unwrap().write_option_trades(&batch) {
        error!("Failed to write realtime option trades: {}", e);
    }
}
