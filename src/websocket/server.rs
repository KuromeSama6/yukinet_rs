use std::collections::HashMap;
use std::fmt::{format, Debug};
use std::fs::{exists, File};
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use anyhow::{anyhow, bail};
use async_trait::async_trait;
use futures_util::future::err;
use futures_util::{SinkExt, StreamExt};
use log::{error, info, warn};
use serde::de::DeserializeOwned;
use serde::Serialize;
use crate::config::{MasterConfig};
use tokio::{net, task};
use tokio::net::TcpListener;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, Mutex, OnceCell, RwLock};
use tokio::sync::mpsc::{Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::{Bytes, Message};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_util::bytes::Buf;
use tokio_util::bytes::buf::Reader;
use tokio_util::sync::CancellationToken;
use crate::message::WebsocketMessage;
use crate::util::{Outgoing};
use crate::util::buf::{EzReader, EzWriteBuf};
use crate::websocket::{codec, MessageRequest};
use crate::websocket::codec::{MessageBinary, MessageText};

#[derive(Debug)]
pub struct WebsocketServer {
    clients: RwLock<HashMap<SocketAddr, Arc<Client>>>,
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

    pub async fn get_client(&self, addr: SocketAddr) -> Option<Arc<Client>> {
        let clients = self.clients.read().await;

        clients.get(&addr).cloned()
    }

    pub async fn has_client(&self, addr: &SocketAddr) -> bool {
        let clients = self.clients.read().await;

        clients.contains_key(addr)
    }

    pub async fn add_client(&self, client: Arc<Client>) {
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
    async fn handle_header(&self, server: Arc<WebsocketServer>, addr: SocketAddr, req: &Request, res: Response) -> Result<Response, ErrorResponse> {
        Ok(res)
    }
    async fn on_connected(&self, server: Arc<WebsocketServer>, addr: SocketAddr) -> anyhow::Result<()> {
        Ok(())
    }
    async fn on_msg(&self, server: Arc<WebsocketServer>, addr: SocketAddr, msg: String) -> anyhow::Result<()>;
    async fn on_request(&self, server: Arc<WebsocketServer>, addr: SocketAddr, msg: String) -> anyhow::Result<String>;
    async fn on_msg_bin(&self, server: Arc<WebsocketServer>, addr: SocketAddr, msg: &mut EzReader<Cursor<Vec<u8>>>) -> anyhow::Result<()>;
    async fn on_request_bin(&self, server: Arc<WebsocketServer>, addr: SocketAddr, msg: &mut EzReader<Cursor<Vec<u8>>>, res: &mut EzWriteBuf) -> anyhow::Result<()>;

    async fn on_disconnect(&self, server: Arc<WebsocketServer>, addr: SocketAddr) -> anyhow::Result<()> {
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

    let ws = tokio_tungstenite::accept_hdr_async(stream, |req: &Request, res: Response| websocket_handle_headers(state.clone(), addr.clone(), req, res)).await?;

    let (mut tx, mut rx) = ws.split();

    info!("Client connected: {addr}");
    let (client_tx, mut client_rx) = mpsc::channel(32);
    
    let client_close = Arc::new(CancellationToken::new());
    let client = Arc::new(Client {
        addr,
        tx: client_tx,
        close: client_close.clone(),
        outbound_requests: RwLock::new(HashMap::new()),
    });

    state.add_client(client.clone()).await;

    state.handler.on_connected(state.clone(), addr).await?;

    loop {
        let state = state.clone();

        tokio::select! {
            Some(msg) = rx.next() => {
                match msg {
                    Ok(msg) => {
                        let cmsg = msg.clone();
                        match msg {
                            Message::Text(text) => {
                                let inbound = codec::parse_message_json(&text)?;
                                let inbound_id = inbound.id;

                                if inbound.response {
                                    let Some(req) = client.outbound_requests.write().await.remove(&inbound_id) else {
                                        bail!("Received inbound response to request #{inbound_id} but the request is found???");
                                    };

                                    req.tx.send(cmsg);

                                } else {
                                    if inbound.id != 0 {
                                        // wants a response
                                        let res = state.handler.on_request(state.clone(), addr, inbound.text.clone()).await?;

                                        let outbound = MessageText {
                                            id: inbound_id,
                                            response: true,
                                            text: res
                                        };

                                        tx.send(outbound.serialize_msg()?).await?

                                    } else {
                                        // does not require a response
                                        state.handler.on_msg(state.clone(), addr, inbound.text.clone()).await?;
                                    }

                                }

                            }
                            Message::Binary(data) => {
                                let inbound = codec::parse_message_binary(&data)?;
                                let inbound_id = inbound.id;

                                if inbound.response {
                                    let Some(req) = client.outbound_requests.write().await.remove(&inbound_id) else {
                                        bail!("Received inbound binary response to request #{inbound_id} but the request is found???");
                                    };

                                    req.tx.send(cmsg);

                                } else {
                                    let mut data_reader = inbound.consume_reader();

                                    if inbound_id != 0 {
                                        // wants a response
                                        let mut res = EzWriteBuf::default();
                                        state.handler.on_request_bin(state.clone(), addr, &mut data_reader, &mut res).await?;

                                        let outbound = MessageBinary {
                                            id: inbound_id,
                                            response: true,
                                            data: res.consume_bytes(),
                                        };

                                        tx.send(Message::Binary(outbound.serialize()?.into())).await?

                                    } else {
                                        // does not require a response
                                        state.handler.on_msg_bin(state.clone(), addr, &mut data_reader).await?;
                                    }
                                }
                            }

                            Message::Pong(_) => {},
                            Message::Close(frame) => {
                                warn!("Websocket connection closed by client {addr}: {:?}", frame);
                                break;
                            }
                            _ => {
                                error!("Unexpected message type {} from client {addr}", msg);
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

            Some(outgoing) = client_rx.recv() => {
                let msg = outgoing.msg;
                tx.send(msg).await?;
                outgoing.ack.send(Ok(())).unwrap();
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
    state.handler.on_disconnect(state.clone(), addr).await?;

    Ok(())
}

fn websocket_handle_headers(state: Arc<WebsocketServer>, addr: SocketAddr, req: &Request, res: Response) -> Result<Response, ErrorResponse> {
    let handle = Handle::current();
    let cstate = state.clone();

    let ret = task::block_in_place(|| {
        handle.block_on(async move {
            cstate.handler.handle_header(cstate.clone(), addr, req, res).await
        })
    });

    ret
}

#[derive(Debug)]
pub struct Client {
    addr: SocketAddr,
    tx: Sender<Outgoing<Message>>,
    outbound_requests: RwLock<HashMap<u64, MessageRequest>>,
    close: Arc<CancellationToken>,
}

impl Client {
    pub async fn send_json<T: Serialize>(&self, data: &T) -> anyhow::Result<()> {
        let text = serde_json::to_string(data)?;
        let outbound = MessageText {
            id: 0,
            response: false,
            text: text,
        };

        let text = serde_json::to_string(&outbound)?;
        let (outgoing, ack) = Outgoing::new(Message::Text(text.into()));

        self.tx.send(outgoing).await?;
        ack.await?;

        Ok(())
    }

    pub async fn request_json<T: Serialize, TRes: DeserializeOwned>(&self, data: &T) -> anyhow::Result<TRes> {
        let text = serde_json::to_string(data)?;
        let (req, rx) = MessageRequest::new();

        let outbound = MessageText {
            id: req.id,
            response: true,
            text: text,
        };

        let text = serde_json::to_string(&outbound)?;
        let (outgoing, ack) = Outgoing::new(Message::Text(text.into()));

        self.tx.send(outgoing).await?;
        ack.await?;

        let id = req.id;
        self.outbound_requests.write().await
            .insert(id, req);

        let msg = rx.await?;
        self.outbound_requests.write().await.remove(&id);

        let Message::Text(text) = msg else {
            bail!("request_json expects a text response");
        };

        let res_str = codec::parse_message_json(&text)?.text;

        let res = serde_json::from_str(res_str.as_str())?;

        Ok(res)
    }

    pub async fn send_binary(&self, data: Vec<u8>) -> anyhow::Result<()> {
        let (req, rx) = MessageRequest::new();
        let outbound = MessageBinary {
            id: req.id,
            response: false,
            data
        };

        let bytes = outbound.serialize()?;
        let (outgoing, ack) = Outgoing::new(Message::Binary(bytes.into()));

        self.tx.send(outgoing).await?;
        ack.await?;

        Ok(())
    }

    pub async fn request_binary(&self, data: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        let (req, rx) = MessageRequest::new();
        let outbound = MessageBinary {
            id: req.id,
            response: false,
            data
        };

        let bytes = outbound.serialize()?;
        let (outgoing, ack) = Outgoing::new(Message::Binary(bytes.into()));

        self.tx.send(outgoing).await?;
        ack.await?;

        let id = req.id;
        self.outbound_requests.write().await
            .insert(id, req);

        let msg = rx.await?;
        self.outbound_requests.write().await.remove(&id);

        let Message::Binary(data) = msg else {
            bail!("send_binary expects a binary response");
        };

        let ret = codec::parse_message_binary(&data)?;

        Ok(ret.data)
    }

    pub async fn graceful_disconnect(&self, reason: &str) -> anyhow::Result<()> {
        let close_frame = CloseFrame {
            code: CloseCode::Normal,
            reason: reason.into(),
        };

        let (outgoing, ack) = Outgoing::new(Message::Close(Some(close_frame)));

        self.tx.send(outgoing).await?;
        ack.await?;

        self.close.cancel();

        Ok(())
    }
}