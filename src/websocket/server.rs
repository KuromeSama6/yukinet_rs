use std::collections::HashMap;
use std::fmt::{format, Debug};
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
use tokio_tungstenite::tungstenite::{Bytes, Message};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_util::sync::CancellationToken;
use crate::util::{Outgoing};

#[derive(Debug)]
pub struct WebsocketServer {
    clients: RwLock<HashMap<SocketAddr, Client>>,
    cmd_tx: UnboundedSender<Message>,
    cmd_rx: UnboundedReceiver<Message>,
    shutdown: CancellationToken,
    handler: Arc<dyn ServerHandler>
}

impl WebsocketServer {
    pub async fn start_new(addr: &SocketAddr, handler: impl ServerHandler) -> anyhow::Result<Arc<Self>> {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        let ret = WebsocketServer {
            clients: RwLock::new(HashMap::new()),
            cmd_tx,
            cmd_rx,
            shutdown: CancellationToken::new(),
            handler: Arc::new(handler)
        };

        let ret = Arc::new(ret);
        let state = ret.clone();

        let (tx, rx) = oneshot::channel();

        let addr = addr.clone();
        tokio::spawn(async move {
            let res = websocket_loop(state, addr, tx).await;
            if let Err(e) = res {
                error!("Websocket server exited with error: {:?}", e);
            }
        });

        rx.await?;

        Ok(ret)
    }

    pub async fn get_client(&self, addr: SocketAddr) -> Option<Client> {
        let clients = self.clients.read().await;

        clients.get(&addr).cloned()
    }

    pub async fn has_client(&self, addr: &SocketAddr) -> bool {
        let clients = self.clients.read().await;

        clients.contains_key(addr)
    }

    pub async fn add_client(&self, client: Client) {
        let mut clients = self.clients.write().await;

        clients.insert(client.addr, client);
    }

    pub async fn remove_client(&self, addr: &SocketAddr) {
        let mut clients = self.clients.write().await;

        clients.remove(addr);
    }
}

#[async_trait]
pub trait ServerHandler: Debug + Send + Sync + 'static {
    fn handle_header(&self, server: Arc<WebsocketServer>, req: &Request, res: Response) -> Result<Response, ErrorResponse> {
        Ok(res)
    }
    async fn on_connected(&self, server: Arc<WebsocketServer>, addr: SocketAddr) -> anyhow::Result<()> {
        Ok(())
    }
    async fn on_msg(&self, server: Arc<WebsocketServer>, addr: SocketAddr, msg: Message) -> anyhow::Result<Option<Message>> {
        Ok(None)
    }
    async fn on_disconnected(&self, server: Arc<WebsocketServer>, addr: SocketAddr) -> anyhow::Result<()> {
        Ok(())
    }
}

async fn websocket_loop(state: Arc<WebsocketServer>, addr: SocketAddr, ready_tx: oneshot::Sender<()>) -> anyhow::Result<()> {
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

async fn websocket_handle_connection(state: Arc<WebsocketServer>, addr: SocketAddr, stream: net::TcpStream) -> anyhow::Result<()> {
    let connect_headers: OnceCell<String> = OnceCell::new();

    let ws = tokio_tungstenite::accept_hdr_async(stream, |req: &Request, res: Response| state.handler.handle_header(state.clone(), req, res)).await?;

    let (mut tx, mut rx) = ws.split();

    info!("Client connected: {addr}");
    let (client_tx, mut client_rx) = mpsc::channel(32);
    let client_close = Arc::new(CancellationToken::new());
    let client = Client {
        addr,
        tx: client_tx,
        close: client_close.clone(),
    };

    state.add_client(client).await;

    state.handler.on_connected(state.clone(), addr).await?;

    loop {
        let state = state.clone();

        tokio::select! {
            Some(msg) = rx.next() => {
                match msg {
                    Ok(msg) => {
                        match msg {
                            Message::Pong(_) => {},
                            Message::Close(frame) => {
                                warn!("Websocket connection closed by client {addr}: {:?}", frame);
                                break;
                            }
                            _ => {
                                let res = state.handler.on_msg(state.clone(), addr, msg).await;
                                match res {
                                    Ok(Some(response_msg)) => {
                                        tx.send(response_msg).await?;
                                    }
                                    Ok(None) => {
                                        tx.send(Message::Pong(Bytes::new())).await?;
                                    }
                                    Err(err) => {
                                        error!("Websocket error for addr {addr}: {err}");
                                        break;
                                    }
                                }
                            }
                        }

                    }
                    Err(err) => {
                        error!("Unexpected websocket error for addr {addr}: {err}");
                        break;
                    }
                }
            }

            Some(outgoing) = client_rx.recv() => {
                let msg = outgoing.msg;
                tx.send(msg).await?;

                // always expect response
                let res = rx.next().await;
                match res {
                    Some(Ok(msg)) => {
                        warn!("Unexpected non-text websocket message from client.");
                        outgoing.ack.send(Ok(msg)).unwrap();
                    }
                    Some(Err(e)) => {
                        error!("Websocket error receiving response from client: {}", e);
                        outgoing.ack.send(Err(anyhow!("Websocket error: {}", e))).unwrap();
                    }
                    None => {
                        warn!("Websocket connection closed by client while waiting for response.");
                        outgoing.ack.send(Err(anyhow!("Websocket connection closed by client."))).unwrap();
                        break;
                    }
                }

            }

            _ = client_close.cancelled() => {
                tx.send(Message::Close(None)).await?;
                break;
            }

            _ = state.shutdown.cancelled() => {
                tx.send(Message::Close(None)).await?;
                break;
            }
        }
    }

    warn!("Websocket client disconnected: {addr}");
    state.remove_client(&addr).await;
    state.handler.on_disconnected(state.clone(), addr).await?;

    Ok(())
}

#[derive(Clone, Debug)]
pub struct Client {
    addr: SocketAddr,
    tx: Sender<Outgoing<Message, Message>>,
    close: Arc<CancellationToken>,
}

impl Client {
    pub async fn send_ws_msg(&self, msg: Message) -> anyhow::Result<Message> {
        let (outgoing, rx) = Outgoing::new(msg);

        self.tx.send(outgoing).await.map_err(|e| anyhow!("Failed to send websocket message to client: {}", e))?;
        let res = rx.await.map_err(|e| anyhow!("Failed to receive websocket response from client: {}", e))??;

        Ok(res)
    }

    pub async fn graceful_disconnect(&self, reason: &str) -> anyhow::Result<()> {
        let close_frame = CloseFrame {
            code: CloseCode::Normal,
            reason: reason.into(),
        };

        self.send_ws_msg(Message::Close(Some(close_frame))).await?;
        self.close.cancel();

        Ok(())
    }
}