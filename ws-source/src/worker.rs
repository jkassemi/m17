// Copyright (c) James Kassemi, SC, US. All rights reserved.

use core_types::*;
use data_client::DataClient;
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use futures::{SinkExt, StreamExt};
use std::time::Duration;

// Stub WS worker for equities trades/NBBO
pub struct WsWorker {
    url: String,  // e.g., "wss://placeholder.com/equity-trades"
}

impl WsWorker {
    pub fn new(url: String) -> Self {
        Self { url }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            let (ws_stream, _) = connect_async(&self.url).await?;
            let (mut write, mut read) = ws_stream.split();

            // Stub: Send ping every 30s
            let ping_task = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    if let Err(_) = write.send(tokio_tungstenite::tungstenite::Message::Ping(vec![])).await {
                        break;
                    }
                }
            });

            // Stub: Read messages and emit DataBatch (empty for now)
            while let Some(msg) = read.next().await {
                match msg? {
                    tokio_tungstenite::tungstenite::Message::Text(text) => {
                        // Parse and emit DataBatch<OptionTrade> or similar
                        tracing::info!("Received: {}", text);
                    }
                    _ => {}
                }
            }

            // Reconnect on disconnect
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}
