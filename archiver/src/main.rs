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
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;

use crate::config::ConfigError;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_max_level(cli.command.common().verbose.tracing_level_filter())
        .with_writer(std::io::stderr)
        .init();

    // Handle cancellation via CTRL+C or SIGTERM
    let token = CancellationToken::new();
    tokio::spawn({
        let shutdown = token.clone();
        async move {
            let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
            shutdown.cancel();
        }
    });

    match commands::dispatch(cli, token).await {
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
