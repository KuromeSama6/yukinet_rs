use std::fs;
use std::io::Cursor;
use std::sync::{Arc, OnceLock, RwLock};
use anyhow::{bail, Context};
use async_trait::async_trait;
use clap::builder::Str;
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
use crate::message::WebsocketMessage;
use crate::util::{Outgoing};
use crate::util::buf::{EzReader, EzWriteBuf};
use crate::websocket::client::{ClientHandler, WebsocketClient};


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
    let cws_client = ws_client.clone();

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

    STATE.get().unwrap().ws_client.shutdown("Worker shutting down".to_string()).await;
}

async fn verify_resources() -> anyhow::Result<()> {
    let res = ws_request(WebsocketMessage::ResourceRequestChecksums).await?;
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
    let WebsocketMessage::ResourceBeginDownload {len, path, checksum} = ws_request(WebsocketMessage::ResourceRequestDownload(path.to_string())).await? else {
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
    ws_send(WebsocketMessage::ResourceStartTransfer).await?;

    debug!("step 4");
    rx.await?;
    debug!("step 5");

    Ok(())
}

async fn ws_send(msg: WebsocketMessage) -> anyhow::Result<()> {
    STATE.get().unwrap().ws_client.send_json(&msg).await?;

    Ok(())
}

async fn ws_request(msg: WebsocketMessage) -> anyhow::Result<WebsocketMessage> {
    let res = STATE.get().unwrap().ws_client.request_json(&msg).await?;

    Ok(res)
}

async fn handle_ws_msg(msg: WebsocketMessage) -> anyhow::Result<()> {
    match msg {
        WebsocketMessage::Ping => {
            debug!("ping from master");
        }

        _ => {
            bail!("Unhandled master websocket message: {:?}", msg);
        }
    }

    Ok(())
}

async fn handle_ws_request(msg: WebsocketMessage) -> anyhow::Result<WebsocketMessage> {
    match msg {
        WebsocketMessage::Ping => {
            debug!("ping request from master");
            Ok(WebsocketMessage::Ack)
        }
        _ => {
            bail!("Unhandled master websocket request: {:?}", msg);
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

    async fn on_msg(&self, client: Arc<WebsocketClient>, msg: String) -> anyhow::Result<()> {
        let msg = serde_json::from_str(&msg)?;
        handle_ws_msg(msg).await?;

        Ok(())
    }

    async fn on_request(&self, client: Arc<WebsocketClient>, msg: String) -> anyhow::Result<String> {
        let msg = serde_json::from_str(&msg)?;
        let res = handle_ws_request(msg).await?;

        Ok(serde_json::to_string(&res)?)
    }

    async fn on_msg_bin(&self, client: Arc<WebsocketClient>, msg: &mut EzReader<Cursor<Vec<u8>>>) -> anyhow::Result<()> {

        Ok(())
    }

    async fn on_request_bin(&self, client: Arc<WebsocketClient>, msg: &mut EzReader<Cursor<Vec<u8>>>, res: &mut EzWriteBuf) -> anyhow::Result<()> {

        Ok(())
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