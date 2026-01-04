use std::fs;
use std::sync::{Arc, OnceLock};
use anyhow::Context;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use http::Request;
use log::{error, info, warn};
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::{Error, Message, Utf8Bytes};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use url::Url;
use crate::config::{WorkerConfig};
use crate::util::{Outgoing, WebsocketOutgoing};
use crate::websocket::client::{ClientHandler, WebsocketClient};
use crate::websocket::WebsocketMessage;

static STATE: OnceLock<Arc<WorkerState>> = OnceLock::new();

pub async fn init() -> anyhow::Result<bool> {
    let config_file_path = "config/worker_config.json";
    if !fs::exists(config_file_path)? {
        return Ok(false);
    }
    let config: WorkerConfig = serde_json::from_reader(fs::File::open(config_file_path)?)?;

    // connect websocket
    let url = Url::parse(&config.master_uri)
        .context("Invalid master websocket URL")?;

    let mut request = url.as_str().into_client_request()?;
    request.headers_mut().insert("X-YukiNet-Worker-Id", config.worker_id.parse()?);
    request.headers_mut().insert("X-YukiNet-Secret", config.secret.parse()?);

    let handler = WebsocketClientHandler { };
    let ws_client = WebsocketClient::connect(request, handler).await?;

    let state: Arc<WorkerState> = WorkerState {
        config,
        ws_client
    }.into();
    STATE.set(state).unwrap();

    Ok(true)
}

pub fn is_enabled() -> bool {
    STATE.get().is_some()
}

pub async fn shutdown() {
    if !is_enabled() {
        return;
    }

    info!("Worker shutting down...");
    info!("Note - Websocket errors during shutdown are expected.");

    STATE.get().unwrap().ws_client.shutdown("Worker shutting down".to_string()).await;
}

#[derive(Debug)]
struct WebsocketClientHandler {
    
}

#[async_trait]
impl ClientHandler for WebsocketClientHandler {
    async fn on_connected(&self, client: Arc<WebsocketClient>) -> anyhow::Result<()> {
        info!("Connected to master server.");
        Ok(())
    }
}

#[derive(Debug)]
struct WorkerState {
    config: WorkerConfig,
    ws_client: Arc<WebsocketClient>,
}