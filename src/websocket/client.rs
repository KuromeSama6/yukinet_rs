use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::{Bytes, Error, Message, Utf8Bytes};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_util::sync::CancellationToken;
use url::Url;
use crate::util::{Outgoing};

#[derive(Debug)]
pub struct WebsocketClient {
    websocket_tx: OnceLock<mpsc::Sender<Outgoing<Message, Message>>>,
    handler: Arc<dyn ClientHandler>,
}

impl WebsocketClient {
    pub async fn connect(request: Request, handler: impl ClientHandler) -> anyhow::Result<Arc<Self>> {
        let ret: Arc<Self> = WebsocketClient {
            websocket_tx: OnceLock::new(),
            handler: Arc::new(handler),
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

    pub async fn send_ws_msg(&self, msg: Message) -> anyhow::Result<Message> {
        let (outgoing, ack) = Outgoing::new(msg);

        let websocket_tx = self.websocket_tx.get().ok_or_else(|| anyhow::anyhow!("Websocket not connected"))?;
        websocket_tx.send(outgoing).await?;

        ack.await?
    }

    pub async fn shutdown(&self, reason: String) -> anyhow::Result<()> {
        let frame = CloseFrame {
            code: CloseCode::Normal,
            reason: reason.into()
        };

        let (outgoing, ack) = Outgoing::new(Message::Close(Some(frame)));

        self.websocket_tx.get().unwrap().send(outgoing).await?;

        ack.await?;

        Ok(())
    }
}

#[async_trait]
pub trait ClientHandler: Debug + Send + Sync + 'static {
    async fn on_connected(&self, client: Arc<WebsocketClient>) -> anyhow::Result<()> {
        Ok(())
    }
    async fn on_msg(&self, client: Arc<WebsocketClient>, msg: Message) -> anyhow::Result<Option<Message>> {
        Ok(None)
    }
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
    let (websocket_tx, mut websocket_rx) = mpsc::channel(32);
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
                            Message::Pong(_) => {}
                            Message::Close(frame) => {
                                warn!("Websocket connection closed by master: {:?}", frame);
                                break;
                            }
                            _ => {
                                let response = state.handler.on_msg(state.clone(), msg).await?;
                                if let Some(resp_msg) = response {
                                    tx.send(resp_msg).await?;

                                } else {
                                    tx.send(Message::Pong(Bytes::new())).await?;
                                }
                            }
                        }
                    }
                    None => {
                        warn!("Websocket connection closed by master.");
                        break;
                    }
                }
            }

            Some(outgoing) = websocket_rx.recv() => {
                tx.send(outgoing.msg).await?;

                // always expect an ack for outgoing messages
                let res = rx.next().await;
                match res {
                    Some(Ok(msg)) => {
                        outgoing.ack.send(Ok(msg)).unwrap();
                    }
                    Some(Err(e)) => {
                        error!("Websocket error receiving ack from master: {}", e);
                        outgoing.ack.send(Err(anyhow::anyhow!("Websocket error: {}", e))).unwrap();
                    }
                    None => {
                        warn!("Websocket connection closed by master while waiting for ack.");
                        outgoing.ack.send(Err(anyhow::anyhow!("Websocket connection closed by master."))).unwrap();
                        break;
                    }
                }
            }
        }
    }

    warn!("Websocket loop exiting.");

    Ok(())
}