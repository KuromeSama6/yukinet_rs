use std::cell::Cell;
use std::collections::HashMap;
use std::fmt::{format, Display, Formatter};
use std::fs::{exists, File};
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use anyhow::{anyhow, bail};
use async_trait::async_trait;
use futures_util::future::err;
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use tokio::io::AsyncReadExt;
use crate::config::{MasterConfig};
use tokio::{net, time};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, Mutex, OnceCell, RwLock};
use tokio::sync::mpsc::{Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::{Bytes, Message};
use tokio_util::sync::CancellationToken;
use crate::error::WebsocketError;
use crate::error::WebsocketError::{AuthenticationFailure, ClientDisconnect, NotAuthenticated, Protocol, UnknownMessage};
use crate::master::MasterCommand::Shutdown;
use crate::message::WebsocketMessage;
use crate::resources;
use crate::resources::Resource;
use crate::util::buf::{EzReader, EzWriteBuf};
use crate::util::Outgoing;
use crate::websocket::server::{Client, ServerHandler, WebsocketServer};

static RESOURCE_CHUNK_SIZE: usize = 1024 * 32;

static STATE: OnceLock<Arc<MasterState>> = OnceLock::new();
static WEBSOCKET: OnceLock<Arc<WebsocketServer>> = OnceLock::new();

pub async fn init() -> anyhow::Result<bool> {
    let config_path = "config/master_config.json";
    if !exists(config_path)? {
        return Ok(false);
    }

    let config: MasterConfig = serde_json::from_reader(File::open(config_path)?)?;
    let (cmd_tx, cmd_rx) = mpsc::channel(32);
    let state = Arc::new(MasterState::new(config, cmd_tx));
    STATE.set(state.clone()).unwrap();

    // command loop
    tokio::spawn(master_command_loop(state.clone(), cmd_rx));

    // websocket
    let handler = WebsocketHandler {
        state: state.clone(),
        connect_headers: RwLock::new(HashMap::new()),
    };

    let addr: SocketAddr = format!("{}:{}", state.config.websocket_host, state.config.websocket_port).parse()?;

    let ws_server = WebsocketServer::start_new(&addr, handler).await?;
    WEBSOCKET.set(ws_server).unwrap();

    Ok(true)
}

pub fn is_enabled() -> bool {
    STATE.get().is_some()
}

pub async fn shutdown() {
    if !is_enabled() {
        return;
    }

    info!("Master shutting down...");
    master_send_cmd(Shutdown).await;
}

async fn master_command_loop(state: Arc<MasterState>, mut cmd_rx: mpsc::Receiver<Outgoing<MasterCommand>>) {
    while let Some(outgoing) = cmd_rx.recv().await {
        match outgoing.msg {
            Shutdown => {
                info!("Master command loop shutting down.");
                state.shutdown.cancel();
                outgoing.ack.send(Ok(()));

                break;
            }
            _ => {
                outgoing.ack.send(Ok(()));
            }
        }
    }
}

async fn master_send_cmd(cmd: MasterCommand) -> anyhow::Result<()> {
    let state = STATE.get().unwrap();
    let (outgoing, ack) = Outgoing::new(cmd);

    state.cmd_tx.send(outgoing).await?;

    ack.await?
}

async fn handle_ws_msg(worker: Arc<Worker>, msg: WebsocketMessage) -> anyhow::Result<()> {
    match msg {
        WebsocketMessage::Ping => {
            debug!("ping from worker {:?}", worker);
        }

        WebsocketMessage::ResourceStartTransfer => {
            let current_download = worker.current_download.read().await;
            if current_download.is_none() {
                bail!("Worker {} requested resource transfer without a pending download.", worker.addr);
            }

            let cworker = worker.clone();
            tokio::spawn(async move {
                cworker.transfer_resource().await;
            });
        }

        WebsocketMessage::ResourceFinishTransfer => {
            worker.clear_res_download().await?;
        }

        _ => {
            bail!("Received unknown message from worker {}: {:?}", worker.addr, msg);
        }
    }

    Ok(())
}

async fn handle_ws_request(worker: Arc<Worker>, msg: WebsocketMessage) -> anyhow::Result<WebsocketMessage> {
    match msg {
        WebsocketMessage::Ping => {
            Ok(WebsocketMessage::Ack)
        }

        WebsocketMessage::ResourceRequestChecksums => {
            let map = resources::checksum_map().await;

            Ok(WebsocketMessage::ResourceVerifyChecksums(map))
        }

        WebsocketMessage::ResourceRequestDownload(path) => {
            let resource = resources::get_resource(path.as_str()).await;
            if resource.is_none() {
                warn!("Worker {} requested unknown resource: {}", worker.addr, path);
                bail!("Unknown resource requested");
            }
            let resource = resource.unwrap();

            let ret = WebsocketMessage::ResourceBeginDownload {
                len: resource.fingerprint.size,
                path: path.clone(),
                checksum: resource.checksum.clone()
            };

            worker.set_res_download(path.clone()).await?;

            Ok(ret)
        }

        _ => {
            warn!("Received unknown request from worker {}: {:?}", worker.addr, msg);
            bail!("Unknown request");
        }
    }
}


#[derive(Debug)]
struct WebsocketHandler {
    state: Arc<MasterState>,
    connect_headers: RwLock<HashMap<SocketAddr, String>>,
}

#[async_trait]
impl ServerHandler for WebsocketHandler {
    async fn handle_header(&self, server: Arc<WebsocketServer>, addr: SocketAddr, req: &Request, res: Response) -> Result<Response, ErrorResponse> {
        let id_header = req.headers().get("X-YukiNet-Worker-Id");
        if id_header.is_none() {
            error!("Worker authentication failure, missing id header!");
            let err = Response::builder()
                .status(401)
                .body(Some("Unauthorized".into()))
                .unwrap();
            return Err(err);
        }

        let secret_header = req.headers().get("X-YukiNet-Secret");
        if secret_header.is_none() {
            error!("Worker authentication failure, missing secret header!");
            let err = Response::builder()
                .status(401)
                .body(Some("Unauthorized".into()))
                .unwrap();
            return Err(err);
        }

        let secret = secret_header.unwrap().to_str().unwrap();
        if secret != self.state.config.secret {
            error!("Worker authentication failure, secret mismatch!");
            let err = Response::builder()
                .status(401)
                .body(Some("Unauthorized".into()))
                .unwrap();
            return Err(err);
        }

        let id = id_header.unwrap().to_str().unwrap().to_string();

        let mut write = self.connect_headers.write().await;
        write.insert(addr, id);

        Ok(res)
    }

    async fn on_connected(&self, server: Arc<WebsocketServer>, addr: SocketAddr) -> anyhow::Result<()> {
        let client_id = self.connect_headers.read().await.get(&addr).unwrap().clone();

        let client = server.get_client(addr).await.unwrap();

        if self.state.get_worker_by_id(&client_id).await.is_some() {
            error!("Worker authentication failure, duplicate client ID: {}", client_id);
            client.graceful_disconnect("Duplicate client ID").await?;
            return Err(anyhow!("Duplicate client ID"));
        }

        info!("Worker connected: {addr} with id {client_id}");

        let worker = Worker::new(client_id, addr);
        self.state.add_worker(worker).await;

        Ok(())
    }

    async fn on_msg(&self, server: Arc<WebsocketServer>, addr: SocketAddr, msg: String) -> anyhow::Result<()> {
        let worker = self.state.get_worker(addr).await.unwrap();
        let msg: WebsocketMessage = serde_json::from_str(&msg)?;

        handle_ws_msg(worker, msg).await?;

        Ok(())
    }

    async fn on_request(&self, server: Arc<WebsocketServer>, addr: SocketAddr, msg: String) -> anyhow::Result<String> {
        let worker = self.state.get_worker(addr).await.unwrap();
        let msg: WebsocketMessage = serde_json::from_str(&msg)?;

        let res = handle_ws_request(worker, msg).await?;

        Ok(serde_json::to_string(&res)?)
    }

    async fn on_msg_bin(&self, server: Arc<WebsocketServer>, addr: SocketAddr, msg: &mut EzReader<Cursor<Vec<u8>>>) -> anyhow::Result<()> {
        bail!("Received unexpected binary message from worker: {}", addr);
    }

    async fn on_request_bin(&self, server: Arc<WebsocketServer>, addr: SocketAddr, msg: &mut EzReader<Cursor<Vec<u8>>>, res: &mut EzWriteBuf) -> anyhow::Result<()> {
        bail!("Received unexpected binary request from worker: {}", addr);
    }

    async fn on_disconnect(&self, server: Arc<WebsocketServer>, addr: SocketAddr) -> anyhow::Result<()> {
        let worker = self.state.get_worker(addr).await.unwrap();
        warn!("Worker disconnected: {addr}");

        Ok(())
    }
}

#[derive(Debug)]
struct MasterState {
    config: MasterConfig,
    workers: RwLock<HashMap<SocketAddr, Arc<Worker>>>,
    cmd_tx: Sender<Outgoing<MasterCommand>>,
    shutdown: CancellationToken
}

impl MasterState {
    pub fn new(config: MasterConfig, cmd_tx: Sender<Outgoing<MasterCommand>>) -> Self {
        MasterState {
            config,
            workers: RwLock::new(HashMap::new()),
            cmd_tx,
            shutdown: CancellationToken::new()
        }
    }

    pub async fn get_worker(&self, addr: SocketAddr) -> Option<Arc<Worker>> {
        let workers = self.workers.read().await;

        workers.get(&addr).cloned()
    }

    pub async fn get_worker_by_id(&self, id: &str) -> Option<Arc<Worker>> {
        let workers = self.workers.read().await;

        for worker in workers.values() {
            if worker.id == id {
                return Some(worker.clone());
            }
        }

        None
    }

    pub async fn is_authenticated(&self, addr: &SocketAddr) -> bool {
        let workers = self.workers.read().await;

        workers.contains_key(addr)
    }

    pub async fn add_worker(&self, worker: Worker) {
        let mut workers = self.workers.write().await;

        workers.insert(worker.addr, Arc::new(worker));
    }

    pub async fn remove_worker(&self, addr: &SocketAddr) {
        let mut workers = self.workers.write().await;

        workers.remove(addr);
    }
}

#[derive(Debug)]
struct Worker {
    id: String,
    addr: SocketAddr,
    current_download: Arc<RwLock<Option<String>>>,
}

impl Worker {
    pub fn new(id: String, addr: SocketAddr) -> Self {
        let ret = Worker {
            id,
            addr,
            current_download: Arc::new(RwLock::new(None)),
        };

        ret
    }

    pub async fn send(&self, msg: WebsocketMessage) -> anyhow::Result<()> {
        let websocket = WEBSOCKET.get().unwrap();
        websocket.get_client(self.addr).await.unwrap().send_json(&msg).await?;

        Ok(())
    }

    pub async fn request(&self, msg: WebsocketMessage) -> anyhow::Result<WebsocketMessage> {
        let websocket = WEBSOCKET.get().unwrap();
        let res = websocket.get_client(self.addr).await.unwrap().request_json(&msg).await?;

        Ok(res)
    }

    pub async fn set_res_download(&self, path: String) -> anyhow::Result<()> {
        let mut write = self.current_download.write().await;
        *write = Some(path);

        Ok(())
    }

    pub async fn transfer_resource(&self) -> anyhow::Result<()> {
        let current = self.current_download.read().await;
        if current.is_none() {
            bail!("No current resource download");
        }
        let path = current.as_ref().unwrap().clone();
        let resource = resources::get_resource(path.as_str()).await.unwrap();
        let mut reader = resource.read_buf().await?;
        let mut buf = [0u8; RESOURCE_CHUNK_SIZE];

        let mut count = 0usize;

        let websocket = WEBSOCKET.get().unwrap().get_client(self.addr).await.unwrap();

        loop {
            let n = reader.read(&mut buf).await?;
            count += n;

            let chunk = buf[..n].to_vec();
            websocket.send_binary(chunk).await?;

            if count >= resource.fingerprint.size as usize {
                break;
            }

            if n == 0 {
                bail!("Unexpected EOF while reading resource file: {}", path);
            }
        }

        Ok(())
    }

    pub async fn clear_res_download(&self) -> anyhow::Result<()> {
        let mut write = self.current_download.write().await;
        *write = None;

        Ok(())
    }
}

impl Display for Worker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Worker {{ id: {}, addr: {} }}", self.id, self.addr)
    }
}

enum MasterCommand {
    Shutdown
}