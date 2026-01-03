use data::MasterConfig;
use std::fs;
use std::fs::{read, File};
use std::process::exit;
use log4rs::append::console::ConsoleAppender;
use log::{error, info, log};
use crate::data::WorkerConfig;

mod data;
mod constants;
mod logging;
mod util;
mod console;
mod master;
mod worker;
mod error;

#[tokio::main]
async fn main() {
    println!("{}", constants::SPLASH_MESSAGE);
    app_init();

    run_firsttime_setup();

    info!("Starting master and worker websockets...");
    init_systems().await;

    console::start_loop();
    info!("Exiting...");

    shutdown().await;

    info!("Goodbye.");
}

fn app_init() {
    logging::init();
}

fn run_firsttime_setup() {
    let exists = fs::exists(".yukinet").unwrap_or(false);

    if !exists {
        info!("First time setup detected, creating necessary files...");
        File::create(".yukinet").unwrap();
        fs::create_dir_all("resources").unwrap();
        fs::create_dir_all("config").unwrap();

        let master_config = MasterConfig::default();
        let master_config_file = File::create("config/master_config.json").unwrap();
        serde_json::to_writer_pretty(master_config_file, &master_config).unwrap();

        let worker_config = WorkerConfig::default();
        let worker_config_file = File::create("config/worker_config.json").unwrap();
        serde_json::to_writer_pretty(worker_config_file, &worker_config).unwrap();

        info!("Please configure the application before running again.");

        exit(0);
    }
}

async fn init_systems() {
    let running_master = master::init().await;
    let running_worker = worker::init().await;

    if !running_master && !running_worker {
        error!("Neither master nor worker mode is enabled. At least one mode must be enabled to run the application. Please check your configuration files.");
        exit(1);
    }
}

async fn shutdown() {
    worker::shutdown().await;
    master::shutdown().await;
}