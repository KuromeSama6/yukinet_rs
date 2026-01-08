use serde::{Deserialize, Serialize};
use crate::resources::ChecksumMap;
use crate::util::Sha256Sum;

#[derive(Debug, Serialize, Deserialize)]
pub enum WebsocketMessage {
    Ping,
    Ack,

    ResourceRequestChecksums,
    ResourceVerifyChecksums(ChecksumMap),
    ResourceRequestDownload(String),
    ResourceBeginDownload {
        len: u64,
        path: String,
        checksum: Sha256Sum,
    },
    ResourceStartTransfer,
    ResourceFinishTransfer,

    WorkerDisconnect(String),
}