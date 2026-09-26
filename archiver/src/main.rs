mod cli;
mod commands;
mod market_registry;

use clap::Parser;
use cli::Cli;

fn main() {
    let cli = Cli::parse();
    let _ = commands::dispatch(cli);
}
