mod cli;
mod client;
mod clock;
mod commands;
mod config;
mod market_registry;

use clap::Parser;
use cli::Cli;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    env_logger::Builder::new()
        .filter_level(cli.command.common().verbose.log_level_filter())
        .parse_default_env()
        .init();

    let _ = commands::dispatch(cli).await;
}
