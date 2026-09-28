mod cli;
mod clock;
mod commands;
mod config;
mod connections;
mod market_registry;
mod types;

use clap::Parser;
use cli::Cli;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let _ = commands::dispatch(cli);
}
