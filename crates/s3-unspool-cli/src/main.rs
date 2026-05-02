mod app;
mod aws;
mod cli;
mod commands;
mod endpoint;
mod reports;
mod ui;

use std::io::{self, Write};

#[tokio::main]
async fn main() {
    if let Err(err) = app::run().await {
        let _ = writeln!(io::stderr().lock(), "× {err}");
        std::process::exit(1);
    }
}
