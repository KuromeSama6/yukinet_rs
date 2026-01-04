use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum WebsocketMessage {
    Ack,
    WorkerDisconnect(String)
}