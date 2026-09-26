mod cli;
mod commands;

use clap::Parser;
use cli::Cli;

fn main() {
    let cli = Cli::parse();
    let _ = commands::dispatch(cli);
}
