use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::tungstenite::Message;
use std::fs::File;
use std::net::{SocketAddr, TcpListener};
use log::info;
use crate::data::MasterConfig;

pub fn init() -> bool {
    let config_path = "config/master_config.json";
    if !std::fs::exists(config_path).unwrap_or(false) {
        info!("Not running as master as the master config file does not exist. To run YukiNet as master, please create the config/master_config.json file.");
        return false;
    }

    let file = File::open(config_path).unwrap();
    let config: MasterConfig = serde_json::from_reader(file).unwrap();

    tokio::spawn(async move {
        if let Err(e) = run_websocket_server(&config).await {
            log::error!("WebSocket server failed: {}", e);
        }
    });

    true
}

async fn run_websocket_server(config: &MasterConfig) -> anyhow::Result<()> {
    let addr_str = format!("{}:{}", config.websocket_host, config.websocket_port);
    let listener = tokio::net::TcpListener::bind(&addr_str).await?;
    info!("Master WS listening on {}", &addr_str);

    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let res = handle_websocket_connection(stream).await;
        });
    }
}

async fn handle_websocket_connection(stream: tokio::net::TcpStream) -> anyhow::Result<()> {
    let ws = tokio_tungstenite::accept_async(stream).await?;
    let (mut write, mut read) = ws.split();

    while let Some(msg) = read.next().await {
        handle_msg(msg?).await;
    }

    Ok(())
}

async fn handle_msg(msg: Message) {
    match msg {
        Message::Text(text) => {
            info!("Received text message: {}", text);
        }
        Message::Binary(bin) => {
            info!("Received binary message of length: {}", bin.len());
        }
        _ => {
            info!("Received other type of message");
        }
    }
}