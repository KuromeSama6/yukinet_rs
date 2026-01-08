pub mod buf;

use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use rand::Rng;
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

pub struct Outgoing<TRes> {
    pub msg: TRes,
    pub ack: oneshot::Sender<anyhow::Result<()>>
}

impl<TRes> Outgoing<TRes> {
    pub fn new(msg: TRes) -> (Self, oneshot::Receiver<anyhow::Result<()>>) {
        let (tx, rx) = oneshot::channel();
        let ret = Outgoing {
            msg,
            ack: tx
        };

        (ret, rx)
    }
}

pub fn generate_random_bytes(length: usize) -> Vec<u8> {
    let mut rng = rand::rng();
    let mut ret = vec![0u8; length];
    rng.fill(&mut ret[..]);

    ret
}

pub type Sha256Sum = [u8; 32];
pub fn sha256_sum(path: &PathBuf) -> anyhow::Result<Sha256Sum> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();

    let mut buffer = [0u8; 8192];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }

    let result = hasher.finalize();
    Ok(result.into())
}