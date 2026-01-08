use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use anyhow::bail;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio_tungstenite::tungstenite::{Bytes, Error, Message, Utf8Bytes};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_util::sync::CancellationToken;
use url::Url;
use crate::util::buf::{EzReader, EzWriteBuf};
use crate::websocket::codec::{MessageBinary, MessageText};
use crate::websocket::{codec, MessageRequest};

#[derive(Debug)]
pub struct WebsocketClient {
    websocket_tx: OnceLock<mpsc::Sender<Message>>,
    handler: Arc<dyn ClientHandler>,
    outbound_requests: RwLock<HashMap<u64, MessageRequest>>,
}

impl WebsocketClient {
    pub async fn connect(request: Request, handler: impl ClientHandler) -> anyhow::Result<Arc<Self>> {
        let ret: Arc<Self> = WebsocketClient {
            websocket_tx: OnceLock::new(),
            handler: Arc::new(handler),
            outbound_requests: RwLock::new(HashMap::new()),
        }.into();

        let (ready_tx, ready_rx) = oneshot::channel();
        {
            let state = ret.clone();
            tokio::spawn(async move {
                let res = websocket_loop(request, state, ready_tx).await;
                if let Err(e) = res {
                    error!("Websocket loop exited with error: {}", e);
                }
            });
        }

        ready_rx.await?;

        Ok(ret)
    }

    pub async fn send_json<T: Serialize>(&self, msg: T) -> anyhow::Result<()> {
        let content = serde_json::to_string(&msg)?;
        let msg = MessageText {
            id: 0,
            response: false,
            text: content
        };
        let msg = Message::Text(serde_json::to_string(&msg)?.into());

        self.websocket_tx.get().unwrap().send(msg).await?;

        Ok(())
    }

    pub async fn request_json<T: Serialize, TRes: DeserializeOwned>(&self, msg: T) -> anyhow::Result<TRes> {
        let content = serde_json::to_string(&msg)?;
        let (req, rx) = MessageRequest::new();
        let msg = MessageText {
            id: req.id,
            response: false,
            text: content
        };
        let msg = Message::Text(serde_json::to_string(&msg)?.into());

        let req_id = req.id;

        {
            let mut requests = self.outbound_requests.write().await;
            requests.insert(req_id, req);
        }

        self.websocket_tx.get().unwrap().send(msg).await?;

        let res = rx.await?;

        {
            let mut requests = self.outbound_requests.write().await;
            requests.remove(&req_id);
        }

        let Message::Text(text) = res else {
            bail!("Unexpected response type for request_json, expected Text, got {res}");
        };

        let msg_text = codec::parse_message_json(&text)?;

        Ok(serde_json::from_str(&msg_text.text)?)
    }

    pub async fn send_binary(&self, data: Vec<u8>) -> anyhow::Result<()> {
        let msg = MessageBinary {
            id: 0,
            response: false,
            data,
        };
        let msg = Message::Binary(msg.serialize()?.into());

        self.websocket_tx.get().unwrap().send(msg).await?;

        Ok(())
    }

    pub async fn request_binary(&self, data: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        let (req, rx) = MessageRequest::new();
        let msg = MessageBinary {
            id: req.id,
            response: false,
            data,
        };
        let msg = Message::Binary(msg.serialize()?.into());

        let req_id = req.id;

        {
            let mut requests = self.outbound_requests.write().await;
            requests.insert(req_id, req);
        }

        self.websocket_tx.get().unwrap().send(msg).await?;

        let res = rx.await?;

        {
            let mut requests = self.outbound_requests.write().await;
            requests.remove(&req_id);
        }

        let Message::Binary(data) = res else {
            bail!("Unexpected response type for send_binary_request, expected Binary, got {res}");
        };

        let msg_bin = codec::parse_message_binary(&data)?;

        Ok(msg_bin.data)
    }

    pub async fn shutdown(&self, reason: String) -> anyhow::Result<()> {
        let frame = CloseFrame {
            code: CloseCode::Normal,
            reason: reason.into()
        };
        
        self.websocket_tx.get().unwrap().send(Message::Close(Some(frame))).await?;

        Ok(())
    }
}

#[async_trait]
pub trait ClientHandler: Debug + Send + Sync + 'static {
    async fn on_connected(&self, client: Arc<WebsocketClient>) -> anyhow::Result<()> {
        Ok(())
    }
    async fn on_msg(&self, client: Arc<WebsocketClient>, msg: String) -> anyhow::Result<()>;
    async fn on_request(&self, client: Arc<WebsocketClient>, msg: String) -> anyhow::Result<String>;
    async fn on_msg_bin(&self, client: Arc<WebsocketClient>, msg: &mut EzReader<Cursor<Vec<u8>>>) -> anyhow::Result<()>;
    async fn on_request_bin(&self, client: Arc<WebsocketClient>, msg: &mut EzReader<Cursor<Vec<u8>>>, res: &mut EzWriteBuf) -> anyhow::Result<()>;
}

async fn websocket_loop(conn_request: http::Request<()>, state: Arc<WebsocketClient>, ready_tx: oneshot::Sender<()>) -> anyhow::Result<()> {
    let uri = conn_request.uri().clone();
    info!("Connecting to websocket server at {uri}...");

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
    let (mut tx, mut rx) = ws.split();
    let (websocket_tx, mut outbound_rx) = mpsc::channel(32);
    state.websocket_tx.set(websocket_tx).unwrap();

    ready_tx.send(()).unwrap();

    info!("Websocket connection to server established at {}", uri);

    state.handler.on_connected(state.clone()).await?;

    loop {
        tokio::select! {
            res = rx.next() => {
                match res {
                    Some(msg) => {
                        let msg = msg?;
                        match msg.clone() {
                            Message::Text(bytes) => {
                                let msg_text: MessageText = serde_json::from_str(bytes.as_str())?;
                                let id = msg_text.id;

                                if msg_text.response {
                                    // handle response from server
                                    let mut write = state.outbound_requests.write().await;
                                    let Some(mut req) = write.remove(&id) else {
                                        bail!("Received response for unknown request ID: {}", id);
                                    };

                                    let _ = req.tx.send(msg);

                                } else {
                                    if id != 0 {
                                        // send request
                                        let res_str = state.handler.on_request(state.clone(), msg_text.text.to_string()).await?;
                                        let res = MessageText {
                                            id,
                                            response: true,
                                            text: res_str,
                                        };

                                        tx.send(Message::Text(serde_json::to_string(&res)?.into())).await?;

                                    } else {
                                        // handle normal message
                                        state.handler.on_msg(state.clone(), msg_text.text.to_string()).await?;
                                    }
                                }
                            }

                            Message::Binary(data) => {
                                let inbound = codec::parse_message_binary(&data)?;
                                let id = inbound.id;

                                if inbound.response {
                                    // handle response from server
                                    let mut write = state.outbound_requests.write().await;
                                    let Some(mut req) = write.remove(&id) else {
                                        bail!("Received response for unknown request ID: {}", id);
                                    };

                                    let _ = req.tx.send(msg);

                                } else {
                                    let mut reader = inbound.to_reader();

                                    if id != 0 {
                                        // send request
                                        let mut res_buf = EzWriteBuf::default();
                                        state.handler.on_request_bin(state.clone(), &mut reader, &mut res_buf).await?;
                                        let res_data = res_buf.to_bytes();

                                        let res = MessageBinary {
                                            id,
                                            response: true,
                                            data: res_data,
                                        };

                                        tx.send(Message::Binary(res.serialize()?.into())).await?;

                                    } else {
                                        // handle normal message
                                        state.handler.on_msg_bin(state.clone(), &mut reader).await?;
                                    }
                                }
                            }

                            Message::Pong(_) => {}
                            Message::Close(frame) => {
                                warn!("Websocket connection closed by master: {:?}", frame);
                                break;
                            }
                            _ => {
                                warn!("Received unexpected message from master: {}", msg);
                                break;
                            }
                        }
                    }
                    None => {
                        warn!("Websocket connection closed by master.");
                        break;
                    }
                }
            }

            Some(msg) = outbound_rx.recv() => {
                tx.send(msg).await?;
            }
        }
    }

    warn!("Websocket loop exiting.");

    Ok(())
}