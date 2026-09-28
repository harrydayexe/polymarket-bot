mod cli;
mod commands;
mod connections;
mod market_registry;
mod types;

use clap::Parser;
use cli::Cli;

fn main() {
    let cli = Cli::parse();
    let _ = commands::dispatch(cli);
}
