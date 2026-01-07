use std::fs;
use std::sync::{Arc, OnceLock, RwLock};
use anyhow::{bail, Context};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use http::Request;
use log::{debug, error, info, warn};
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::{Error, Message, Utf8Bytes};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use url::Url;
use crate::config::{WorkerConfig};
use crate::{master, resources};
use crate::util::{Outgoing};
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
        ws_client,
        resource_download_rx: RwLock::new(None),
    }.into();
    STATE.set(state).unwrap();

    if !master::is_enabled() {
        info!("Begining resource verification phase of worker initialization.");
        verify_resources().await?;
        info!("Resource verification complete.");

    } else {
        info!("Skipping resource verification because master mode is enabled.");
    }

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
    let res = send_ws_msg(WebsocketMessage::ResourceRequestChecksums).await?;
    let WebsocketMessage::ResourceVerifyChecksums(checksums) = res else {
        anyhow::bail!("ResourceVerifyChecksums failed");
    };

    let diff = resources::diff_checksums(&checksums).await;
    if diff.len() == 0 {
        info!("All resources up to date.");
        return Ok(());
    }

    info!("{} resources need to be updated.", diff.len());

    for path in diff {
        update_resource(path.as_str()).await?;
    }
    Ok(())
}

async fn update_resource(path: &str) -> anyhow::Result<()> {
    info!("Update resource: {path}");

    debug!("step 1");
    let WebsocketMessage::ResourceBeginDownload {len, path, checksum} = send_ws_msg(WebsocketMessage::ResourceRequestDownload(path.to_string())).await? else {
        bail!("ResourceRequestDownload failed");
    };
    debug!("step 2");

    let (tx, rx) = oneshot::channel();

    let download = ResourceDownload {
        path: path.to_string(),
        fin_tx: tx
    };
    STATE.get().unwrap().resource_download_rx.write().unwrap().replace(download);

    debug!("step 3");
    send_ws_msg(WebsocketMessage::ResourceStartTransfer).await?;

    debug!("step 4");
    rx.await?;
    debug!("step 5");

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
            Err(anyhow::anyhow!("Unexpected response message type {:?}", res))
        }
    }
}

async fn handle_ws_msg(msg: WebsocketMessage) -> anyhow::Result<Option<WebsocketMessage>> {
    match msg {
        _ => {
            warn!("Unhandled master websocket message: {:?}", msg);
            Ok(None)
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

    async fn on_msg(&self, client: Arc<WebsocketClient>, msg: Message) -> anyhow::Result<Option<Message>> {
        debug!("on_msg {:?}", msg);

        match msg {
            Message::Text(text) => {
                let msg = serde_json::from_str(text.as_str())?;

                let res =  handle_ws_msg(msg).await?;
                match res {
                    Some(res) => {
                        let res_str = serde_json::to_string(&res)?;
                        Ok(Some(Message::Text(res_str.into())))
                    }
                    None => Ok(None),
                }
            }
            Message::Binary(data) => {
                let state = STATE.get().unwrap();

                {
                    let dl = state.resource_download_rx.read().unwrap();
                    if let Some(download) = &*dl {
                        debug!("Received file chunk for {}, len={}", download.path, data.len());

                        return Ok(None);
                    }
                }

                warn!("Received unexpected binary websocket message of length {}", data.len());
                Ok(None)
            }
            _ => {
                warn!("Unsupported websocket message: {:?} ", msg);
                Ok(None)
            }
        }
    }
}

#[derive(Debug)]
struct WorkerState {
    config: WorkerConfig,
    ws_client: Arc<WebsocketClient>,
    resource_download_rx: RwLock<Option<ResourceDownload>>,
}

impl WorkerState {
}

#[derive(Debug)]
struct ResourceDownload {
    path: String,
    fin_tx: oneshot::Sender<()>,
}