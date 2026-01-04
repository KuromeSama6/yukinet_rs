use std::fs;
use std::sync::{Arc, OnceLock};
use anyhow::Context;
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
use crate::websocket::WebsocketMessage;

static STATE: OnceLock<Arc<WorkerState>> = OnceLock::new();

pub async fn init() -> anyhow::Result<bool> {
    let config_file_path = "config/worker_config.json";
    if !fs::exists(config_file_path)? {
        return Ok(false);
    }
    let config: WorkerConfig = serde_json::from_reader(fs::File::open(config_file_path)?)?;
    let state = WorkerState {
        config,
        websocket_tx: OnceLock::new(),
    };
    let state = Arc::new(state);
    STATE.set(state.clone());

    connect_websocket(state).await?;

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

    let state = STATE.get().unwrap();
    state.send_ws_msg(WebsocketMessage::WorkerDisconnect("Worker shutting down".to_string())).await;
}

async fn connect_websocket(state: Arc<WorkerState>) -> anyhow::Result<()> {
    let (ready_tx, ready_rx) = oneshot::channel();

    let url = Url::parse(&state.config.master_uri)
        .context("Invalid master websocket URL")?;

    let mut request = url.as_str().into_client_request()?;
    request.headers_mut().insert("X-YukiNet-Worker-Id", state.config.worker_id.parse()?);
    request.headers_mut().insert("X-YukiNet-Secret", state.config.secret.parse()?);

    tokio::spawn(async move {
        let res = websocket_loop(request, state, ready_tx).await;
        if let Err(e) = res {
            error!("Websocket loop exited with error: {}", e);
        }
    });

    ready_rx.await?;

    Ok(())
}

async fn websocket_loop(conn_request: Request<()>, state: Arc<WorkerState>, ready_tx: oneshot::Sender<()>) -> anyhow::Result<()> {
    let uri = conn_request.uri().clone();
    info!("Connecting to master at {uri}...");

    let res = tokio_tungstenite::connect_async(conn_request).await;

    if let Err(e) = res {
        match e {
            Error::Http(response) => {
                let status = response.status();
                let reason = status.canonical_reason().unwrap_or("Unknown Error");
                error!("Failed to connect to master at {uri}: HTTP {} {}", status.as_u16(), reason);
                return Err(anyhow::anyhow!("HTTP error {} {}", status.as_u16(), reason));
            }
            _ => {
                error!("Failed to connect to master at {uri}: {}", e);
                return Err(anyhow::anyhow!("Connection error: {}", e));
            }
        }
    }

    let (ws, res) = res.unwrap();

    info!("Connected to master at {uri}");
    ready_tx.send(()).unwrap();

    let (mut tx, mut rx) = ws.split();
    let (websocket_tx, mut websocket_rx) = mpsc::channel(32);
    state.websocket_tx.set(websocket_tx).unwrap();

    loop {
        tokio::select! {
            res = rx.next() => {
                match res {
                    Some(msg) => {

                    }
                    None => {
                        warn!("Websocket connection closed by master.");
                        break;
                    }
                }
            }

            Some(outgoing) = websocket_rx.recv() => {
                let msg_str = serde_json::to_string(&outgoing.msg)?;
                tx.send(Message::text(Utf8Bytes::from(msg_str))).await?;

                // always expect an ack for outgoing messages
                let res = rx.next().await;
                match res {
                    Some(Ok(Message::Text(text))) => {
                        let ws_msg: WebsocketMessage = serde_json::from_str(&text)?;
                        outgoing.ack.send(Ok(ws_msg)).unwrap();
                    }
                    Some(Ok(_)) => {
                        warn!("Unexpected non-text websocket message from master.");
                        outgoing.ack.send(Err(anyhow::anyhow!("Unexpected message from master."))).unwrap();
                    }
                    Some(Err(e)) => {
                        error!("Websocket error receiving ack from master: {}", e);
                        outgoing.ack.send(Err(anyhow::anyhow!("Websocket error: {}", e))).unwrap();
                    }
                    None => {
                        warn!("Websocket connection closed by master while waiting for ack.");
                        outgoing.ack.send(Err(anyhow::anyhow!("Websocket connection closed by master."))).unwrap();
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

struct WorkerState {
    config: WorkerConfig,
    websocket_tx: OnceLock<Sender<WebsocketOutgoing>>,
}

impl WorkerState {
    pub async fn send_ws_msg(&self, msg: WebsocketMessage) -> anyhow::Result<WebsocketMessage> {
        let (msg, ack) = Outgoing::new(msg);

        if let Some(tx) = self.websocket_tx.get() {
            tx.send(msg).await?;
        }

        ack.await?
    }
}