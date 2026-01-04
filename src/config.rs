use std::net::SocketAddr;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedReceiver;
use uuid::{uuid, Uuid};
use crate::util::generate_random_bytes;

#[derive(Debug, Serialize, Deserialize)]
pub struct MasterConfig {
    pub websocket_host: String,
    pub websocket_port: u16,
    pub http_host: String,
    pub http_port: u16,
    pub secret: String,
}

impl Default for MasterConfig {
    fn default() -> Self {
        let secret = generate_random_bytes(64);

        MasterConfig {
            websocket_host: "0.0.0.0".to_string(),
            websocket_port: 7940,
            http_host: "0.0.0.0".to_string(),
            http_port: 23662,
            secret: hex::encode(secret),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub worker_id: String,
    pub master_uri: String,
    pub secret: String,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: Uuid::now_v7().to_string(),
            master_uri: "ws://127.0.0.1:7940/".to_string(),
            secret: "".to_string(),
        }
    }
}