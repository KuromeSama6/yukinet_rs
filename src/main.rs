use config::MasterConfig;
use std::fs;
use std::fs::{read, File};
use std::process::exit;
use log4rs::append::console::ConsoleAppender;
use log::{error, info, log};
use crate::config::WorkerConfig;

mod config;
mod constants;
mod logging;
mod util;
mod console;
mod error;
mod master;
mod worker;
mod websocket;
mod command;
mod resources;
mod asyncutil;

#[tokio::main]
async fn main() {
    println!("{}", constants::SPLASH_MESSAGE);
    app_init();

    info!("YukiNet starting up...");

    run_setup();

    let init_systems_ok = init_modules().await;
    if let Err(e) = init_systems_ok {
        error!("Failed to initialize service modules: {}", e);
        exit(1);
    }

    console::start_loop();
    info!("Exiting...");

    shutdown().await;

    info!("Goodbye.");
}

fn app_init() {
    logging::init();
}

fn run_setup() {
    let config_dir_path = "config";
    fs::create_dir_all(config_dir_path).unwrap();

    let master_config_path = format!("{}/master_config.json", config_dir_path);
    let worker_config_path = format!("{}/worker_config.json", config_dir_path);

    if !fs::exists(&master_config_path).unwrap() && !fs::exists(&worker_config_path).unwrap() {
        info!("No configuration files found. Running initial setup...");

        let default_master_config = MasterConfig::default();
        let default_worker_config = WorkerConfig::default();

        fs::write(&master_config_path, serde_json::to_string_pretty(&default_master_config).unwrap()).unwrap();
        fs::write(&worker_config_path, serde_json::to_string_pretty(&default_worker_config).unwrap()).unwrap();

        info!("Default configuration files created at '{}'. Please review and modify them as needed before restarting the application.", config_dir_path);
        exit(0);
    }
}

async fn init_modules() -> anyhow::Result<()> {
    resources::init().await?;

    let master_running = master::init().await?;
    if !master_running {
        info!("No master server is running. Running in worker-only mode.");
    }
    
    let worker_running = worker::init().await?;
    if !worker_running {
        info!("No worker configuration found. Running in master-only mode.");
    }
    
    if !master_running && !worker_running {
        error!("Neither master nor worker systems could be initialized. Please check your configuration files.");
        exit(1);
    }

    Ok(())
}

async fn shutdown() {
    worker::shutdown().await;
    master::shutdown().await;
}