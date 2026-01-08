pub mod server;
pub mod client;
pub mod codec;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use anyhow::bail;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::Message;
use crate::resources::ChecksumMap;
use crate::util::{Outgoing, Sha256Sum};

static REQUESTID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct MessageRequest {
    id: u64,
    tx: oneshot::Sender<Message>
}

impl MessageRequest {
    pub fn new() -> (Self, oneshot::Receiver<Message>) {
        let id = REQUESTID_COUNTER.fetch_add(1, Ordering::SeqCst);
        if id == 0 {
            panic!("Request ID overflow or misconfiguration, request ID should never be 0!")
        }
        
        let (tx, rx) = oneshot::channel();

        let ret = Self {
            id,
            tx
        };

        (ret, rx)
    }
}