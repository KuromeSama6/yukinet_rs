pub mod server;
pub mod client;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum WebsocketMessage {
    Ping,
    Ack,
    WorkerDisconnect(String)
}