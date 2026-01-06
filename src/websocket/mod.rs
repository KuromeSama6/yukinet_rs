pub mod server;
pub mod client;

use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::util::Sha256Sum;

#[derive(Debug, Serialize, Deserialize)]
pub enum WebsocketMessage {
    Ping,
    Ack,
    ResourceRequestVerifyChecksums,
    ResourceVerifyChecksums(HashMap<String, Sha256Sum>),
    WorkerDisconnect(String),
}