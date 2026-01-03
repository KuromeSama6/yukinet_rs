use std::fs;
use std::fs::File;
use std::sync::Arc;
use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use log::info;
use once_cell::sync::OnceCell;
use serde_json::json;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Sender;
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use crate::data::{WorkerConfig};
use crate::data::WebsocketMessage;
use crate::worker::WorkerCommand::Shutdown;

static ENABLED: OnceCell<bool> = OnceCell::new();
static CMD_TX: OnceCell<UnboundedSender<WorkerCommand>> = OnceCell::new();

pub async fn init() -> bool {
    let config_path = "config/worker_config.json";
    if !fs::exists(config_path).unwrap() {
        info!("Not running as worker as the worker config file does not exist. To run YukiNet as worker, please create the config/worker_config.json file.");
        ENABLED.set(false).unwrap();
        return false;
    }
    ENABLED.set(true).unwrap();
    
    
    let file = File::open(config_path).unwrap();
    let config: WorkerConfig = serde_json::from_reader(file).unwrap();

    info!("Starting YukiNet worker...");

    // init chan
    let (tx, rx) = unbounded_channel();
    CMD_TX.set(tx).unwrap();

    let state = WorkerState::new(Arc::new(config), rx);

    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        if let Err(err) = run_websocket_client(state, ready_tx).await {
            log::error!("Failed to run worker: {}", err);
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
    
    send_worker_cmd(Shutdown).await;
}

async fn run_websocket_client(mut state: WorkerState, ready_tx: Sender<()>) -> anyhow::Result<()> {
    let addr_str = &state.config.master_uri;
    let (ws, _) = tokio_tungstenite::connect_async(addr_str)
        .await
        .with_context(|| format!("Failed to connect to master at {}", addr_str))?;

    info!("Connected to master at {}", addr_str);
    ready_tx.send(()).ok();

    let (mut tx, mut rx) = ws.split();

    // send registration message
    let register_msg = serde_json::to_string(&WebsocketMessage::registration(state.config.as_ref()))?;
    tx.send(Message::Text(Utf8Bytes::from(register_msg))).await?;

    loop {
        tokio::select! {
            Some(msg) = rx.next() => {
                let msg = msg?;
                match msg {
                    Message::Text(text) => {
                        let message: WebsocketMessage = serde_json::from_str(&text.to_string())?;
                        handle_text_response(message).await?;
                    }
                    Message::Ping(ping) => {
                        tx.send(Message::Pong(ping)).await?;
                    }
                    _ => {}
                }
            }

            Some(command) = state.cmd_rx.recv() => {
                match command {
                    Shutdown => {
                        info!("Shutting down worker...");
                        let frame = CloseFrame {
                            code: CloseCode::Normal,
                            reason: "Worker disconnecting".into()
                        };
                        tx.send(Message::Close(Some(frame))).await?;

                        break;
                    }
                    
                    _ => {}
                }
            }
        }
    }

    Ok(())
}

async fn handle_text_response(msg: WebsocketMessage) -> anyhow::Result<()> {
    match msg {
        WebsocketMessage::RegistrationAck => {
            info!("Successfully registered with master.");
        }
        _ => {}
    }

    Ok(())
}

async fn send_worker_cmd(cmd: WorkerCommand) {
    CMD_TX.get().map(|tx| {
        let _ = tx.send(cmd);
    });
}


struct WorkerState {
    config: Arc<WorkerConfig>,
    cmd_rx: UnboundedReceiver<WorkerCommand>,
}

impl WorkerState {
    pub fn new(config: Arc<WorkerConfig>, rx: UnboundedReceiver<WorkerCommand>) -> Self {
        Self {
            config,
            cmd_rx: rx
        }
    }
}

enum WorkerCommand {
    RequestResourceSync,
    Shutdown,
}