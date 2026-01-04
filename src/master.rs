use std::collections::HashMap;
use std::fmt::format;
use std::fs::{exists, File};
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use anyhow::anyhow;
use futures_util::future::err;
use futures_util::{SinkExt, StreamExt};
use log::{error, info, warn};
use crate::config::{MasterConfig};
use tokio::net;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, Mutex, OnceCell, RwLock};
use tokio::sync::mpsc::{Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use crate::error::WebsocketError;
use crate::error::WebsocketError::{AuthenticationFailure, ClientDisconnect, NotAuthenticated, Protocol, UnknownMessage};
use crate::master::MasterCommand::Shutdown;
use crate::util::{Outgoing, WebsocketOutgoing};
use crate::websocket::WebsocketMessage;

static STATE: OnceLock<Arc<MasterState>> = OnceLock::new();

pub async fn init() -> anyhow::Result<bool> {
    let config_path = "config/master_config.json";
    if !exists(config_path)? {
        return Ok(false);
    }

    let config: MasterConfig = serde_json::from_reader(File::open(config_path)?)?;
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let state = Arc::new(MasterState::new(config, cmd_tx));
    STATE.set(state.clone()).unwrap();

    // command loop
    tokio::spawn(master_command_loop(state.clone(), cmd_rx));

    // websocket
    start_websocket_server(state).await?;

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
    master_send_cmd(Shutdown);
}

async fn start_websocket_server(state: Arc<MasterState>) -> anyhow::Result<()> {
    let addr_str = format!("{}:{}", state.config.websocket_host, state.config.websocket_port);

    let addr: SocketAddr = addr_str.parse()?;
    let (tx, rx) = oneshot::channel();

    tokio::spawn(async move {
        let res = websocket_loop(state, addr, tx).await;
        if let Err(e) = res {
            error!("Websocket server exited with error: {:?}", e);
        }
    });

    rx.await?;

    Ok(())
}

async fn websocket_loop(state: Arc<MasterState>, addr: SocketAddr, ready_tx: oneshot::Sender<()>) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("Websocket server started at {}", addr);
    ready_tx.send(()).unwrap();

    loop {
        tokio::select! {
            Ok((stream, addr)) = listener.accept() => {
                info!("Client connected: {addr}, awaiting authentication.");
                let state = state.clone();

                tokio::spawn(async move {
                    let res = websocket_handle_connection(state, addr, stream).await;
                    if let Err(e) = res {
                        error!("Websocket connection error for addr {addr}, {e}");
                    }
                });
            }

            _ = state.shutdown.cancelled() => {
                info!("Websocket server shutting down.");
                break;
            }
        }
    }

    Ok(())
}

async fn websocket_handle_connection(state: Arc<MasterState>, addr: SocketAddr, stream: net::TcpStream) -> anyhow::Result<()> {
    let connect_headers: OnceCell<String> = OnceCell::new();

    let ws = tokio_tungstenite::accept_hdr_async(stream, |req: &Request, res: Response| {
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
        if secret != state.config.secret {
            error!("Worker authentication failure, secret mismatch!");
            let err = Response::builder()
                .status(401)
                .body(Some("Unauthorized".into()))
                .unwrap();
            return Err(err);
        }

        let id = id_header.unwrap().to_str().unwrap().to_string();
        connect_headers.set(id).unwrap();

        Ok(res)
    }).await?;

    let (mut tx, mut rx) = ws.split();
    let worker_id = connect_headers.get().unwrap().clone();

    if state.get_worker_by_id(&worker_id).await.is_some() {
        error!("Worker authentication failure, duplicate worker ID: {}", worker_id);
        tx.send(Message::Close(None)).await;
        tx.flush().await;
        tx.close().await?;
        return Err(anyhow!("Duplicate worker ID"));
    }

    info!("Worker connected: {addr} with id {worker_id}");

    let (worker, mut worker_rx) = Worker::new(worker_id, addr);
    state.add_worker(worker).await;

    loop {
        let state = state.clone();

        tokio::select! {
            Some(msg) = rx.next() => {
                match msg {
                    Ok(msg) => {
                        let res = websocket_handle_msg(state, addr, msg).await;
                        match res {
                            Ok(response_msg) => {
                                let msg_str = serde_json::to_string(&response_msg)?;
                                tx.send(Message::text(msg_str)).await?;
                            }
                            Err(ClientDisconnect(_)) => {
                                warn!("Client requested disconnect: {addr}");
                                break;
                            }
                            Err(err) => {
                                error!("Websocket error for addr {addr}: {err}");
                                break;
                            }
                        }

                    }
                    Err(err) => {
                        error!("Unexpected websocket error for addr {addr}: {err}");
                        break;
                    }
                }
            }

            Some(outgoing) = worker_rx.recv() => {
                let msg = outgoing.msg;
                let msg_str = serde_json::to_string(&msg)?;
                tx.send(Message::text(msg_str)).await?;
                
                // always expect response
                let res = rx.next().await;
                match res {
                    Some(Ok(Message::Text(text))) => {
                        let ws_msg: WebsocketMessage = serde_json::from_str(&text)?;
                        outgoing.ack.send(Ok(ws_msg)).unwrap();
                    }
                    Some(Ok(_)) => {
                        warn!("Unexpected non-text websocket message from worker.");
                        outgoing.ack.send(Err(anyhow!("Unexpected non-text websocket message from worker."))).unwrap();
                    }
                    Some(Err(e)) => {
                        error!("Websocket error receiving response from worker: {}", e);
                        outgoing.ack.send(Err(anyhow!("Websocket error: {}", e))).unwrap();
                    }
                    None => {
                        warn!("Websocket connection closed by worker while waiting for response.");
                        outgoing.ack.send(Err(anyhow!("Websocket connection closed by worker."))).unwrap();
                        break;
                    }
                }
                
            }

            _ = state.shutdown.cancelled() => {
                tx.send(Message::Close(None)).await?;
                break;
            }
        }
    }

    warn!("Websocket client disconnected: {addr}");
    state.remove_worker(&addr).await;

    Ok(())
}

async fn websocket_handle_msg(state: Arc<MasterState>, addr: SocketAddr, msg: Message) -> anyhow::Result<WebsocketMessage, WebsocketError> {
    match msg {
        Message::Text(text) => {
            let msg: WebsocketMessage = serde_json::from_str(&text)?;

            websocket_handle_msg_text(state, addr, msg).await
        }

        Message::Close(frame) => {
            Err(ClientDisconnect(frame.unwrap().reason.to_string()))
        }

        _ => {
             Err(UnknownMessage)
        }
    }
}

async fn websocket_handle_msg_text(state: Arc<MasterState>, addr: SocketAddr, msg: WebsocketMessage) -> anyhow::Result<WebsocketMessage, WebsocketError> {
    if !state.is_authenticated(&addr).await {
        return Err(NotAuthenticated);
    }

    let worker = state.get_worker(addr).await.unwrap();

    match msg {
        WebsocketMessage::WorkerDisconnect(reason) => {
            warn!("Worker at {addr} requested disconnect: {reason}");
            return Err(ClientDisconnect(reason));
        }
        _ => {
            warn!("Unexpected websocket message: {msg:?}");
        }
    }

    Ok(WebsocketMessage::Ack)
}

async fn websocket_send(worker: &Worker, msg: WebsocketMessage) -> anyhow::Result<()> {
    Ok(())
}

async fn master_command_loop(state: Arc<MasterState>, mut cmd_rx: UnboundedReceiver<MasterCommand>) {
    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            Shutdown => {
                info!("Master command loop shutting down.");
                state.shutdown.cancel();
                break;
            }
            _ => {}
        }
    }
}

fn master_send_cmd(cmd: MasterCommand) {
    let state = STATE.get().unwrap();

    state.cmd_tx.send(cmd).unwrap();
}

#[derive(Debug)]
struct MasterState {
    config: MasterConfig,
    workers: RwLock<HashMap<SocketAddr, Worker>>,
    cmd_tx: UnboundedSender<MasterCommand>,
    shutdown: CancellationToken
}

impl MasterState {
    pub fn new(config: MasterConfig, cmd_tx: UnboundedSender<MasterCommand>) -> Self {
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