use std::fs;
use std::fs::File;
use log::info;
use crate::data::WorkerConfig;

pub fn init() -> bool {
    let config_path = "config/worker_config.json";
    if !fs::exists(config_path).unwrap() {
        info!("Not running as worker as the worker config file does not exist. To run YukiNet as worker, please create the config/worker_config.json file.");
        return false;
    }

    let file = File::open(config_path).unwrap();
    let config: WorkerConfig = serde_json::from_reader(file).unwrap();

    info!("Starting YukiNet worker...");
    tokio::spawn(async move {
        if let Err(err) = run_websocket_client(&config).await {
            log::error!("Failed to run worker: {}", err);
        }
    });

    true
}

async fn run_websocket_client(config: &WorkerConfig) -> anyhow::Result<()> {
    let addr_str = &config.master_uri;
    let ws = tokio_tungstenite::connect_async(addr_str).await?;
    info!("Connected to master at {}", addr_str);

    Ok(())
}