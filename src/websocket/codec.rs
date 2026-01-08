use std::fs::read;
use std::io::{Cursor, Read};
use anyhow::bail;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use tokio_util::bytes::{Buf, Bytes};
use crate::util::buf::{EzReader, EzWriteBuf};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MessageText {
    pub id: u64,
    pub response: bool,
    pub text: String,
}

impl MessageText {
    pub fn serialize_msg(&self) -> anyhow::Result<Message> {
        let serialized = serde_json::to_string(self)?;
        Ok(Message::Text(serialized.into()))
    }
}

#[derive(Debug)]
pub struct MessageBinary {
    pub id: u64,
    pub response: bool,
    pub data: Vec<u8>,
}

impl MessageBinary {
    pub fn serialize(&self) -> anyhow::Result<Vec<u8>> {
        let mut write = EzWriteBuf::default();
        write.write_u64(self.id)?;
        write.write_bool(self.response)?;
        write.write_all(&self.data)?;

        Ok(write.consume_bytes())
    }

    pub fn consume_reader(self) -> EzReader<Cursor<Vec<u8>>> {
        let cursor = Cursor::new(self.data);

        EzReader::new(cursor)
    }
}

pub fn parse_message_json(msg: &Utf8Bytes) -> anyhow::Result<MessageText> {
    let msg: MessageText = serde_json::from_str(&msg.to_string())?;
    let text = msg.text.clone();

    Ok(MessageText {
        id: msg.id,
        response: msg.response,
        text,
    })
}

pub fn parse_message_binary(data: &Bytes) -> anyhow::Result<MessageBinary> {
    let mut reader = EzReader::new(data.to_owned().reader());
    let id = reader.read_u64()?;
    let response = reader.read_bool()?;

    let mut data = Vec::new();
    reader.read_to_end(&mut data)?;

    Ok(MessageBinary {
        id,
        response,
        data,
    })
}