// Copyright (c) James Kassemi, SC, US. All rights reserved.

//! WebSocket ingestion utilities for realtime equities quotes and options trades.

use crate::install_rustls_provider;
use core_types::opra::parse_opra_contract;
use core_types::types::{
    AggressorSide, ClassMethod, EquityTrade, Nbbo, NbboState, OptionTrade, Quality, Source,
};
use core_types::uid::{equity_trade_uid, option_trade_uid, quote_uid};
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, Stream, StreamExt,
};
use log::{error, info, warn};
use serde::Deserialize;
use serde_json::Value;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message as WsTextMessage, WebSocketStream,
};
use url::Url;

type Socket = WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
type SocketSink = SplitSink<Socket, WsTextMessage>;

/// Output emitted by a [`WsWorker`].
#[derive(Debug)]
pub enum WsMessage {
    Nbbo(Nbbo),
    OptionTrade(OptionTrade),
    EquityTrade(EquityTrade),
}

pub type WsStream = Pin<Box<dyn Stream<Item = WsMessage> + Send>>;

#[derive(Debug, Clone, Copy)]
pub enum ResourceKind {
    EquityQuotes,
    EquityTrades,
    OptionsQuotes,
    OptionsTrades,
}

#[derive(Clone)]
pub enum SubscriptionSource {
    Static(Vec<String>),
    Dynamic(watch::Receiver<Vec<String>>),
}

impl SubscriptionSource {
    fn snapshot(&self) -> Vec<String> {
        match self {
            SubscriptionSource::Static(v) => v.clone(),
            SubscriptionSource::Dynamic(rx) => rx.borrow().clone(),
        }
    }

    fn dynamic_receiver(&self) -> Option<watch::Receiver<Vec<String>>> {
        match self {
            SubscriptionSource::Static(_) => None,
            SubscriptionSource::Dynamic(rx) => Some(rx.clone()),
        }
    }
}

/// Ingest websocket data.
pub struct WsWorker {
    url: Url,
    resource: ResourceKind,
    api_key: Option<String>,
    subscriptions: SubscriptionSource,
}

impl WsWorker {
    pub fn new(
        url: &str,
        resource: ResourceKind,
        api_key: Option<String>,
        subscriptions: SubscriptionSource,
    ) -> Self {
        Self {
            url: Url::parse(url).expect("invalid websocket url"),
            resource,
            api_key,
            subscriptions,
        }
    }

    pub async fn stream(self) -> Result<WsStream, WsError> {
        install_rustls_provider();
        self.stream_live().await
    }

    async fn stream_live(&self) -> Result<WsStream, WsError> {
        let (tx, rx) = mpsc::channel(1024);
        let url = self.url.to_string();
        let api_key = self.api_key.clone();
        let resource = self.resource;
        let subscriptions = self.subscriptions.clone();
        tokio::spawn(async move {
            let mut attempt: u32 = 0;
            loop {
                match connect_async(url.as_str()).await {
                    Ok((ws, _)) => {
                        attempt = 0;
                        info!("ws connected: {}", url);
                        let (write, read) = ws.split();
                        if let Err(e) = handle_connection(
                            write,
                            read,
                            api_key.clone(),
                            resource,
                            subscriptions.clone(),
                            tx.clone(),
                        )
                        .await
                        {
                            warn!("ws connection error: {}", e);
                        }
                    }
                    Err(err) => {
                        error!("ws connect error ({}): {}", url, err);
                    }
                }
                attempt += 1;
                let backoff = std::cmp::min(1 << attempt.min(5), 30);
                sleep(Duration::from_secs(backoff as u64)).await;
            }
        });
        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

async fn handle_connection(
    write: SocketSink,
    mut read: SplitStream<Socket>,
    api_key: Option<String>,
    resource: ResourceKind,
    subscriptions: SubscriptionSource,
    tx: mpsc::Sender<WsMessage>,
) -> Result<(), WsError> {
    let writer = Arc::new(Mutex::new(write));
    if let Some(key) = api_key.clone() {
        send_text(
            &writer,
            serde_json::json!({"action":"auth","params": key}).to_string(),
        )
        .await?;
    }

    let current = subscriptions.snapshot();
    let formatted = format_symbols(resource, &current);
    send_subscriptions(&writer, &formatted, "subscribe").await?;

    let mut sub_task = None;
    if let Some(mut rx) = subscriptions.dynamic_receiver() {
        let writer_clone = writer.clone();
        let initial = current.clone();
        sub_task = Some(tokio::spawn(async move {
            let mut active = initial;
            loop {
                if rx.changed().await.is_err() {
                    break;
                }
                let latest = rx.borrow().clone();
                let (adds, removes) = diff_lists(&active, &latest);
                if !adds.is_empty() {
                    let adds = format_symbols(resource, &adds);
                    if let Err(e) = send_subscriptions(&writer_clone, &adds, "subscribe").await {
                        warn!("ws subscribe update failed: {}", e);
                        break;
                    }
                }
                if !removes.is_empty() {
                    let removes = format_symbols(resource, &removes);
                    if let Err(e) = send_subscriptions(&writer_clone, &removes, "unsubscribe").await
                    {
                        warn!("ws unsubscribe update failed: {}", e);
                        break;
                    }
                }
                active = latest;
            }
        }));
    }

    while let Some(msg) = read.next().await {
        match msg {
            Ok(WsTextMessage::Text(text)) => {
                if handle_incoming_text(&text, resource, &tx).await.is_err() {
                    break;
                }
            }
            Ok(WsTextMessage::Binary(bin)) => {
                if let Ok(text) = String::from_utf8(bin.to_vec()) {
                    if handle_incoming_text(&text, resource, &tx).await.is_err() {
                        break;
                    }
                }
            }
            Ok(WsTextMessage::Close(frame)) => {
                warn!("ws closed: {:?}", frame);
                break;
            }
            Ok(_) => {}
            Err(err) => {
                warn!("ws read error: {}", err);
                break;
            }
        }
    }

    if let Some(task) = sub_task {
        task.abort();
    }
    Ok(())
}

fn diff_lists(old: &[String], new: &[String]) -> (Vec<String>, Vec<String>) {
    use std::collections::HashSet;

    let old_set: HashSet<&String> = old.iter().collect();
    let new_set: HashSet<&String> = new.iter().collect();
    let adds = new
        .iter()
        .filter(|s| !old_set.contains(*s))
        .cloned()
        .collect();
    let removes = old
        .iter()
        .filter(|s| !new_set.contains(*s))
        .cloned()
        .collect();
    (adds, removes)
}

fn format_symbols(resource: ResourceKind, symbols: &[String]) -> Vec<String> {
    match resource {
        ResourceKind::OptionsTrades => prefix_symbols("T.", symbols),
        ResourceKind::OptionsQuotes => prefix_symbols("Q.", symbols),
        _ => symbols.to_vec(),
    }
}

fn prefix_symbols(prefix: &str, symbols: &[String]) -> Vec<String> {
    symbols
        .iter()
        .map(|sym| {
            if sym.starts_with(prefix) {
                sym.clone()
            } else {
                format!("{}{}", prefix, sym)
            }
        })
        .collect()
}

async fn send_text(writer: &Arc<Mutex<SocketSink>>, msg: String) -> Result<(), WsError> {
    let mut guard = writer.lock().await;
    guard
        .send(WsTextMessage::Text(msg.into()))
        .await
        .map_err(WsError::from)
}

async fn send_subscriptions(
    writer: &Arc<Mutex<SocketSink>>,
    symbols: &[String],
    action: &str,
) -> Result<(), WsError> {
    if symbols.is_empty() {
        return Ok(());
    }
    const SUB_BATCH: usize = 200;
    for chunk in symbols.chunks(SUB_BATCH) {
        let params = chunk.join(",");
        let msg = serde_json::json!({ "action": action, "params": params }).to_string();
        send_text(writer, msg).await?;
        sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

async fn handle_incoming_text(
    text: &str,
    resource: ResourceKind,
    tx: &mpsc::Sender<WsMessage>,
) -> Result<(), ()> {
    let val: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(err) => {
            warn!("ws invalid json: {}", err);
            return Ok(());
        }
    };
    match val {
        Value::Array(arr) => {
            for item in arr {
                if let Some(msg) = decode_value(&item, resource) {
                    if tx.send(msg).await.is_err() {
                        return Err(());
                    }
                }
            }
        }
        Value::Object(_) => {
            if let Some(msg) = decode_value(&val, resource) {
                if tx.send(msg).await.is_err() {
                    return Err(());
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn decode_value(value: &Value, resource: ResourceKind) -> Option<WsMessage> {
    let ev = value.get("ev").and_then(Value::as_str)?;
    match resource {
        ResourceKind::EquityQuotes | ResourceKind::OptionsQuotes if ev == "Q" => {
            serde_json::from_value::<RawQuote>(value.clone())
                .ok()
                .and_then(raw_quote_to_nbbo)
                .map(WsMessage::Nbbo)
        }
        ResourceKind::EquityTrades if ev == "T" => {
            serde_json::from_value::<RawEquityTrade>(value.clone())
                .ok()
                .and_then(raw_equity_trade_to_row)
                .map(WsMessage::EquityTrade)
        }
        ResourceKind::OptionsTrades if ev == "T" => {
            serde_json::from_value::<RawOptionTrade>(value.clone())
                .ok()
                .and_then(raw_option_trade_to_row)
                .map(WsMessage::OptionTrade)
        }
        _ => None,
    }
}

fn raw_quote_to_nbbo(raw: RawQuote) -> Option<Nbbo> {
    let instrument = raw.sym?;
    let quote_ts_ns = millis_to_ns(raw.ts_ms);
    let bid = raw.bid_price;
    let ask = raw.ask_price;
    let bid_sz = raw.bid_size.unwrap_or(0);
    let ask_sz = raw.ask_size.unwrap_or(0);
    let state = if bid > ask {
        NbboState::Crossed
    } else if (bid - ask).abs() < f64::EPSILON {
        NbboState::Locked
    } else {
        NbboState::Normal
    };
    let quote_uid = quote_uid(
        &instrument,
        quote_ts_ns,
        None,
        bid,
        ask,
        bid_sz,
        ask_sz,
        raw.bid_exchange,
        raw.ask_exchange,
        raw.condition,
    );
    Some(Nbbo {
        instrument_id: instrument,
        quote_uid,
        quote_ts_ns,
        bid,
        ask,
        bid_sz,
        ask_sz,
        state,
        condition: raw.condition,
        best_bid_venue: raw.bid_exchange,
        best_ask_venue: raw.ask_exchange,
        source: Source::Ws,
        quality: Quality::Prelim,
        watermark_ts_ns: quote_ts_ns,
    })
}

fn raw_option_trade_to_row(raw: RawOptionTrade) -> Option<OptionTrade> {
    let contract = raw.sym?;
    if !contract.starts_with("O:") {
        return None;
    }
    let trade_ts_ns = millis_to_ns(raw.ts_ms);
    let size = raw.size.unwrap_or(0);
    let exchange = raw.exchange.unwrap_or(0);
    let conditions = raw.conditions.unwrap_or_default();
    let (dir, strike, expiry_ts_ns, underlying) = parse_opra_contract(&contract, Some(trade_ts_ns));
    let trade_uid = option_trade_uid(
        &contract,
        trade_ts_ns,
        None,
        raw.price,
        size,
        exchange,
        &conditions,
    );
    Some(OptionTrade {
        contract,
        trade_uid,
        contract_direction: dir,
        strike_price: strike,
        underlying,
        trade_ts_ns,
        price: raw.price,
        size,
        conditions,
        exchange,
        expiry_ts_ns,
        aggressor_side: AggressorSide::Unknown,
        class_method: ClassMethod::Unknown,
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
        iv: None,
        greeks_flags: 0,
        source: Source::Ws,
        quality: Quality::Prelim,
        watermark_ts_ns: trade_ts_ns,
    })
}

fn millis_to_ns(ms: i64) -> i64 {
    ms.saturating_mul(1_000_000)
}

fn raw_equity_trade_to_row(raw: RawEquityTrade) -> Option<EquityTrade> {
    let symbol = raw.sym?;
    let trade_ts_ns = millis_to_ns(raw.ts_ms);
    let size = raw.size.unwrap_or(0);
    let exchange = raw.exchange.unwrap_or(0);
    let conditions = raw.conditions.unwrap_or_default();
    let trade_id = raw.trade_id;
    let trade_uid = equity_trade_uid(
        &symbol,
        trade_ts_ns,
        None,
        None,
        raw.price,
        size,
        exchange,
        trade_id.as_deref(),
        None,
        &conditions,
    );
    Some(EquityTrade {
        symbol,
        trade_uid,
        trade_ts_ns,
        price: raw.price,
        size,
        conditions,
        exchange,
        aggressor_side: AggressorSide::Unknown,
        class_method: ClassMethod::Unknown,
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
        source: Source::Ws,
        quality: Quality::Prelim,
        watermark_ts_ns: trade_ts_ns,
        trade_id,
        seq: None,
        participant_ts_ns: None,
        tape: None,
        correction: None,
        trf_id: None,
        trf_ts_ns: None,
    })
}

#[derive(Debug, Deserialize)]
struct RawQuote {
    #[serde(rename = "sym")]
    sym: Option<String>,
    #[serde(rename = "ap")]
    ask_price: f64,
    #[serde(rename = "bp")]
    bid_price: f64,
    #[serde(rename = "as")]
    ask_size: Option<u32>,
    #[serde(rename = "bs")]
    bid_size: Option<u32>,
    #[serde(rename = "ax")]
    ask_exchange: Option<i32>,
    #[serde(rename = "bx")]
    bid_exchange: Option<i32>,
    #[serde(rename = "c")]
    condition: Option<i32>,
    #[serde(rename = "t")]
    ts_ms: i64,
}

#[derive(Debug, Deserialize)]
struct RawOptionTrade {
    #[serde(rename = "sym")]
    sym: Option<String>,
    #[serde(rename = "p")]
    price: f64,
    #[serde(rename = "s")]
    size: Option<u32>,
    #[serde(rename = "c", default)]
    conditions: Option<Vec<i32>>,
    #[serde(rename = "x")]
    exchange: Option<i32>,
    #[serde(rename = "t")]
    ts_ms: i64,
}

#[derive(Debug, Deserialize)]
struct RawEquityTrade {
    #[serde(rename = "sym")]
    sym: Option<String>,
    #[serde(rename = "p")]
    price: f64,
    #[serde(rename = "q")]
    size: Option<u32>,
    #[serde(rename = "c", default)]
    conditions: Option<Vec<i32>>,
    #[serde(rename = "x")]
    exchange: Option<i32>,
    #[serde(rename = "t")]
    ts_ms: i64,
    #[serde(rename = "i")]
    trade_id: Option<String>,
}

#[derive(Debug, Error)]
pub enum WsError {
    #[error("websocket error: {0}")]
    Websocket(#[from] tokio_tungstenite::tungstenite::Error),
}
