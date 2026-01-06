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

    info!("Begining resource verification phase of worker initialization.");
    verify_resources().await?;
    info!("Resource verification complete.");

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

async fn verify_resources() -> anyhow::Result<()> {
    let res = send_ws_msg(WebsocketMessage::ResourceRequestVerifyChecksums).await?;
    let WebsocketMessage::ResourceVerifyChecksums(resources) = res else {
        anyhow::bail!("ResourceVerifyChecksums failed");
    };

    Ok(())
}

async fn send_ws_msg(msg: WebsocketMessage) -> anyhow::Result<WebsocketMessage> {
    let msg_str = serde_json::to_string(&msg)?;
    let res = STATE.get().unwrap().ws_client.send_ws_msg(Message::Text(msg_str.into())).await?;

    match res {
        Message::Text(text) => {
            let msg: WebsocketMessage = serde_json::from_str(&text)?;
            Ok(msg)
        }
        _ => {
            Err(anyhow::anyhow!("Unexpected response message type"))
        }
    }
}

#[derive(Debug)]
struct WebsocketClientHandler;

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