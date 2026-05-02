use std::env;

use crate::cli::cli;
use crate::commands::{run_unzip, run_zip};
use crate::ui::Output;

pub(crate) async fn run() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .with_ansi(false)
        .with_target(false)
        .init();

    let matches = cli().get_matches_from(env::args_os());
    let output = Output::from_matches(&matches);

    match matches.subcommand() {
        Some(("zip", matches)) => run_zip(matches, &output).await,
        Some(("unzip", matches)) => run_unzip(matches, &output).await,
        _ => unreachable!("subcommand is required by clap"),
    }
}
