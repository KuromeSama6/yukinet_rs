use std::collections::HashMap;
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use std::fs::File;
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, OnceLock};
use anyhow::anyhow;
use log::{error, info, warn};
use once_cell::sync::OnceCell;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{Mutex, RwLock};
use tokio::sync::oneshot::{Receiver, Sender};
use uuid::Uuid;
use crate::data::{MasterConfig};
use crate::data::WebsocketMessage;
use crate::data::WebsocketMessage::RegistrationAck;
use crate::error::WebsocketError;
use crate::error::WebsocketError::{ClientDisconnect, NotAuthenticated, UnknownMessage};

static ENABLED: OnceCell<bool> = OnceCell::new();
static WORKERS_TX: OnceLock<Arc<RwLock<HashMap<SocketAddr, UnboundedSender<RemoteWorkerCommand>>>>> = OnceLock::new();

pub async fn init() -> bool {
    let config_path = "config/master_config.json";
    if !std::fs::exists(config_path).unwrap_or(false) {
        info!("Not running as master as the master config file does not exist. To run YukiNet as master, please create the config/master_config.json file.");
        ENABLED.set(false).unwrap();
        return false;
    }

    ENABLED.set(true).unwrap();
    WORKERS_TX.set(Arc::new(RwLock::new(HashMap::new()))).unwrap();

    let file = File::open(config_path).unwrap();
    let config: MasterConfig = serde_json::from_reader(file).unwrap();
    let config = Arc::new(config);

    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        if let Err(e) = run_websocket_server(config, ready_tx).await {
            log::error!("WebSocket server failed: {}", e);
        }
    });
    ready_rx.await.ok();

    true
}

pub fn is_enabled() -> bool {
    ENABLED.get().unwrap().clone()
}

pub async fn shutdown() {
    if !is_enabled() {
        return;
    }

    info!("Shutting down master...");
    for (_, tx) in WORKERS_TX.get().unwrap().read().await.iter() {
        tx.send(RemoteWorkerCommand::Disconnect).ok();
    }
}

async fn run_websocket_server(config: Arc<MasterConfig>, ready_tx: Sender<()>) -> anyhow::Result<()> {
    let addr_str = format!("{}:{}", config.websocket_host, config.websocket_port);
    let listener = tokio::net::TcpListener::bind(&addr_str).await?;

    info!("Master WS listening on {}", &addr_str);
    ready_tx.send(()).ok();

    loop {
        let (stream, _) = listener.accept().await?;
        let config = config.clone();

        tokio::spawn(async move {
            let state = WebsocketState::new(config);
            handle_websocket_connection(stream, state).await;
        });
    }

}

async fn handle_websocket_connection(stream: tokio::net::TcpStream, mut state: WebsocketState) -> anyhow::Result<()> {
    let ws = tokio_tungstenite::accept_async(stream).await?;
    let addr = ws.get_ref().peer_addr()?;

    info!("Worker connected: {}, awaiting authentication.", addr);

    let (mut tx, mut rx) = ws.split();

    loop {
        tokio::select! {
            msg = rx.next() => {
                if let Some(msg) = msg {
                    match msg {
                        Ok(msg) => {
                            let res = handle_msg(addr, msg, &mut tx, &mut state).await;
                            if let Err(e) = res {
                                match e {
                                    ClientDisconnect => {
                                        break;
                                    }
                                    _ => {
                                        log::error!("Error handling message from {}: {}", addr, e);
                                        break;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            log::error!("WebSocket error: {}", e);
                            break;
                        }
                    }

                } else {
                    warn!("Connection closed by worker: {}", addr);
                    break;
                }
            }

            Some(cmd) = recv_worker_cmd(&state, addr)  => {
                match cmd {
                    _ => {}
                }
            }
        }
    }

    warn!("Worker disconnected: {}", addr);
    state.remove_worker(&addr);

    Ok(())
}

async fn handle_msg(addr: SocketAddr, msg: Message, write: &mut (impl SinkExt<Message> + Unpin), state: &mut WebsocketState) -> anyhow::Result<(), WebsocketError> {
    match msg {
        Message::Text(text) => {
            let message: WebsocketMessage = serde_json::from_str(&text)?;
            let res = handle_msg_text(addr, &message, state).await?;

            if let Some(response) = res {
                let response_text = serde_json::to_string(&response)?;
                write.send(Message::Text(Utf8Bytes::from(response_text))).await;
            }
        }
        Message::Binary(bin) => {
            info!("Received binary message: {:?}", bin);
        }
        Message::Pong(payload) => {
            info!("Received pong message: {:?}", payload);
            write.send(Message::Pong(payload)).await;
        }
        Message::Close(frame) => {
            return Err(ClientDisconnect);
        }
        _ => {
            info!("Received other type of message: {:?}", msg);
        }
    }

    Ok(())
}

async fn handle_msg_text(addr: SocketAddr, msg: &WebsocketMessage, state: &mut WebsocketState) -> anyhow::Result<Option<WebsocketMessage>, WebsocketError> {
    if let WebsocketMessage::Registration { worker_id, secret} = msg {
        info!("Received registration from worker ID: {}", worker_id);

        if secret != &state.config.secret {
            error!("Authentication failed for worker ID: {}", worker_id);
            return Err(WebsocketError::AuthenticationFailure);
        }

        state.add_worker(worker_id.to_string(), addr).await;
        info!("Worker ID: {} authenticated successfully.", worker_id);

        return Ok(Some(RegistrationAck));
    }

    if !state.is_authenticated(addr).await {
        return Err(NotAuthenticated);
    }

    match msg {
        _ => {
            return Err(UnknownMessage);
        }
    }

    Ok(None)
}

async fn recv_worker_cmd(state: &WebsocketState, addr: SocketAddr) -> Option<RemoteWorkerCommand> {
    let rx = {
        let worker = state.get_worker(&addr).await?;

        worker.cmd_rx.clone()
    };

    rx.lock().await.recv().await
}

async fn send_worker_cmd(addr: SocketAddr, cmd: RemoteWorkerCommand) -> anyhow::Result<()> {
    let workers_tx = WORKERS_TX.get().unwrap().read().await;

    if let Some(tx) = workers_tx.get(&addr) {
        tx.send(cmd)?;
        Ok(())

    } else {
        Err(anyhow::anyhow!("Worker not found"))
    }
}

#[derive(Debug)]
struct Worker {
    id: String,
    authenticated: bool,
    addr: SocketAddr,
    cmd_rx: Arc<Mutex<UnboundedReceiver<RemoteWorkerCommand>>>,
}

struct WebsocketState {
    config: Arc<MasterConfig>,
    workers: RwLock<HashMap<SocketAddr, Arc<Worker>>>,
}

impl WebsocketState {
    fn new(config: Arc<MasterConfig>) -> Self {
        WebsocketState {
            config,
            workers: RwLock::new(HashMap::new())
        }
    }
    async fn add_worker(&mut self, worker_id: String, addr: SocketAddr) {
        let (tx, rx) = unbounded_channel();

        let worker = Worker {
            id: worker_id.clone(),
            authenticated: false,
            addr,
            cmd_rx: Arc::new(Mutex::new(rx)),
        };

        self.workers.write().await.insert(addr, Arc::new(worker));
        WORKERS_TX.get().unwrap()
            .write()
            .await
            .insert(addr, tx);
    }

    async fn get_worker(&self, addr: &SocketAddr) -> Option<Arc<Worker>> {
        let worker = self.workers.read().await;

        worker.get(addr).cloned()
    }

    async fn is_authenticated(&self, addr: SocketAddr) -> bool {
        self.workers.read().await.contains_key(&addr)
    }

    async fn remove_worker(&self, addr: &SocketAddr) {
        self.workers.write().await.remove(addr);
        WORKERS_TX.get().unwrap()
            .write()
            .await
            .remove(addr);
    }
}

enum RemoteWorkerCommand {
    Disconnect,
}