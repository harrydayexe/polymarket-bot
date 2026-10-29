mod archive;
mod cli;
mod client;
mod clock;
mod commands;
mod config;
mod connection_manager;
mod market_registry;
mod messages;
mod recovery;
mod snapshot_worker;
mod status;
mod types;
mod writer;

use std::process::ExitCode;

use clap::Parser;
use cli::Cli;
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;

use crate::{
    cli::Commands,
    config::{Config, ConfigError},
};

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_max_level(log_level(&cli))
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

/// The `-v` / `-q` flags if given. Otherwise `run` logs at the config file's `log_level`, and
/// the other commands only warnings and errors.
fn log_level(cli: &Cli) -> tracing::level_filters::LevelFilter {
    let common = cli.command.common();
    if !common.verbose.is_present()
        && let Commands::Run(_) = cli.command
        && let Ok(config) = Config::load(&common.config)
        && let Ok(level) = config.log_level.parse()
    {
        return level;
    }
    common.verbose.tracing_level_filter()
}
