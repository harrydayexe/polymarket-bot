mod cli;
mod client;
mod clock;
mod commands;
mod config;
mod market_registry;
mod messages;

use std::process::ExitCode;

use clap::Parser;
use cli::Cli;

use crate::config::ConfigError;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    env_logger::Builder::new()
        .filter_level(cli.command.common().verbose.log_level_filter())
        .parse_default_env()
        .init();

    match commands::dispatch(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => match e.downcast::<ConfigError>() {
            Ok(cfg_err) => {
                eprintln!("{:?}", miette::Report::new(cfg_err));
                ExitCode::from(78)
            }
            Err(e) => {
                eprintln!("error: {e:#}");
                ExitCode::FAILURE
            }
        },
    }
}
