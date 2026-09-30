mod cli;
mod clock;
mod commands;
mod config;
mod connections;
mod envelope;
mod gamma_client;
mod market_registry;
mod types;

use clap::Parser;
use cli::Cli;

use crate::gamma_client::client::{APIClient, GammaClient};

#[tokio::main]
async fn main() {
    // let cli = Cli::parse();
    // let _ = commands::dispatch(cli);
    let client = APIClient::default();
    let politics_id = client.fetch_tag_id("politics".to_string()).await.unwrap();
    println!("Politics ID resolved to: {}", politics_id);
}
