use std::collections::HashMap;
use std::fmt::format;
use std::fs::{exists, File};
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use anyhow::anyhow;
use async_trait::async_trait;
use futures_util::future::err;
use futures_util::{SinkExt, StreamExt};
use log::{error, info, warn};
use crate::config::{MasterConfig};
use tokio::net;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, Mutex, OnceCell, RwLock};
use tokio::sync::mpsc::{Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use crate::error::WebsocketError;
use crate::error::WebsocketError::{AuthenticationFailure, ClientDisconnect, NotAuthenticated, Protocol, UnknownMessage};
use crate::master::MasterCommand::Shutdown;
use crate::util::{Outgoing, WebsocketOutgoing};
use crate::websocket::server::{Client, ServerHandler, WebsocketServer};
use crate::websocket::WebsocketMessage;

static STATE: OnceLock<Arc<MasterState>> = OnceLock::new();

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
        connect_headers: OnceLock::new()
    };

    let addr: SocketAddr = format!("{}:{}", state.config.websocket_host, state.config.websocket_port).parse()?;

    let ws_server = WebsocketServer::start_new(&addr, handler).await?;

    Ok(true)
}

pub async fn is_enabled() -> bool {
    STATE.get().is_some()
}

pub async fn shutdown() {
    if !is_enabled().await {
        return;
    }

    info!("Master shutting down...");
    master_send_cmd(Shutdown).await;
}

async fn master_command_loop(state: Arc<MasterState>, mut cmd_rx: mpsc::Receiver<Outgoing<MasterCommand, ()>>) {
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

#[derive(Debug)]
struct WebsocketHandler {
    state: Arc<MasterState>,
    connect_headers: OnceLock<String>,
}

#[async_trait]
impl ServerHandler for WebsocketHandler {
    fn handle_header(&self, server: Arc<WebsocketServer>, req: &Request, res: Response) -> Result<Response, ErrorResponse> {
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
        self.connect_headers.set(id).unwrap();

        Ok(res)
    }

    async fn on_connected(&self, server: Arc<WebsocketServer>, addr: SocketAddr) -> anyhow::Result<()> {
        let client_id = self.connect_headers.get().unwrap().clone();
        let client = server.get_client(addr).await.unwrap();

        if self.state.get_worker_by_id(&client_id).await.is_some() {
            error!("Worker authentication failure, duplicate client ID: {}", client_id);
            client.graceful_disconnect("Duplicate client ID").await?;
            return Err(anyhow!("Duplicate client ID"));
        }

        info!("Worker connected: {addr} with id {client_id}");

        let (worker, mut client_rx) = Worker::new(client_id, addr);
        self.state.add_worker(worker).await;

        Ok(())
    }
}

#[derive(Debug)]
struct MasterState {
    config: MasterConfig,
    workers: RwLock<HashMap<SocketAddr, Worker>>,
    cmd_tx: Sender<Outgoing<MasterCommand, ()>>,
    shutdown: CancellationToken
}

impl MasterState {
    pub fn new(config: MasterConfig, cmd_tx: Sender<Outgoing<MasterCommand, ()>>) -> Self {
        MasterState {
            config,
            workers: RwLock::new(HashMap::new()),
            cmd_tx,
            shutdown: CancellationToken::new()
        }
    }

    pub async fn get_worker(&self, addr: SocketAddr) -> Option<Worker> {
        let workers = self.workers.read().await;

        workers.get(&addr).cloned()
    }

    pub async fn get_worker_by_id(&self, id: &str) -> Option<Worker> {
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

        workers.insert(worker.addr, worker);
    }

    pub async fn remove_worker(&self, addr: &SocketAddr) {
        let mut workers = self.workers.write().await;

        workers.remove(addr);
    }
}

#[derive(Clone, Debug)]
struct Worker {
    id: String,
    addr: SocketAddr,
    websocket_tx: Sender<WebsocketOutgoing>
}

impl Worker {
    pub fn new(id: String, addr: SocketAddr) -> (Self, mpsc::Receiver<WebsocketOutgoing>) {
        let (tx, rx) = mpsc::channel(32);

        let ret = Worker {
            id,
            addr,
            websocket_tx: tx
        };

        (ret, rx)
    }

    pub async fn send_ws_msg(&self, msg: WebsocketMessage) -> anyhow::Result<WebsocketMessage> {
        let (msg, ack) = Outgoing::new(msg);
        self.websocket_tx.send(msg).await?;

        let res = ack.await?;

        res
    }
}

enum MasterCommand {
    Shutdown
}