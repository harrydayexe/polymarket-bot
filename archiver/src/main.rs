mod cli;
mod clock;
mod commands;
mod config;
mod connections;
mod envelope;
mod market_registry;
// mod types;

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
