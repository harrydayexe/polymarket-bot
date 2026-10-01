mod cli;
mod clock;
mod commands;
mod config;
mod connections;
mod envelope;
mod gamma_client;
mod market_registry;
mod types;

use std::sync::Arc;

use clap::Parser;
use cli::Cli;

use crate::{
    config::Config,
    gamma_client::client::{APIClient, GammaClient},
};

#[tokio::main]
async fn main() {
    // let cli = Cli::parse();
    // let _ = commands::dispatch(cli);
    let config: Arc<Config> = Arc::new(Config::default());
    let client = APIClient::new(config);
    let keyset = client
        .fetch_page(vec!["politics".to_string(), "sports".to_string()], None)
        .await
        .unwrap();
    let num_markets = keyset.markets.len();
    println!("Keyset page resolved with {} markets", num_markets);

    for market in keyset.markets {
        if let Some(question) = market.question {
            println!("{}", question);
        }
    }
}
