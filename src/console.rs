use log::{error, info};
use crate::command::Command;
use crate::worker;

pub fn start_loop() {
    info!("Ready. Enter `help` for help. Hit Ctrl-C to exit.");

    loop {
        if !console_loop_once() {
            break;
        }
    }

    info!("Console shutting down.");
}

fn console_loop_once() -> bool {
    let input = inquire::Text::new("YukiNet $")
        .prompt();

    let input = match input {
        Ok(input) => input,
        Err(err) => return false
    };

    true
}

fn handle_cmd(cmd: Command) {

}