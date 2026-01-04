use rand::Rng;
use tokio::sync::oneshot;
use crate::websocket::WebsocketMessage;

pub struct Outgoing<TRes, TAck> {
    pub msg: TRes,
    pub ack: oneshot::Sender<anyhow::Result<TAck>>
}

impl<TRes, TAck> Outgoing<TRes, TAck> {
    pub fn new(msg: TRes) -> (Self, oneshot::Receiver<anyhow::Result<TAck>>) {
        let (tx, rx) = oneshot::channel();
        let ret = Outgoing {
            msg,
            ack: tx
        };

        (ret, rx)
    }
}

pub type WebsocketOutgoing = Outgoing<WebsocketMessage, WebsocketMessage>;

pub fn generate_random_bytes(length: usize) -> Vec<u8> {
    let mut rng = rand::rng();
    let mut ret = vec![0u8; length];
    rng.fill(&mut ret[..]);

    ret
}