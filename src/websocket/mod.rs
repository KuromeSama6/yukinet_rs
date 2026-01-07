pub mod server;
pub mod client;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::Message;
use crate::resources::ChecksumMap;
use crate::util::{Outgoing, Sha256Sum};

static REQUESTID_COUNTER: AtomicU64 = AtomicU64::new(0);

pub type WebsocketOutgoing = Outgoing<Message, Message>;

#[derive(Debug, Serialize, Deserialize)]
pub enum WebsocketMessage {
    Ping,
    Ack,
    Nop,

    ResourceRequestChecksums,
    ResourceVerifyChecksums(ChecksumMap),
    ResourceRequestDownload(String),
    ResourceBeginDownload {
        len: u64,
        path: String,
        checksum: Sha256Sum,
    },
    ResourceStartTransfer,
    
    WorkerDisconnect(String),
}

#[derive(Debug)]
pub struct MessageRequest {
    id: u64,
    tx: oneshot::Sender<Message>
}

impl MessageRequest {
    pub fn new() -> (Self, oneshot::Receiver<Message>) {
        let id = REQUESTID_COUNTER.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        
        let ret = Self {
            id,
            tx
        };

        (ret, rx)
    }
}