use std::sync::Arc;

use crate::client::MarcasiteClient;
use crate::clock::SystemClock;
use crate::config::Config;
use crate::market_registry::registry::Registry;
use crate::market_registry::update_from_gamma::fetch_markets_from_gamma;

use crate::cli::CommonArgs;

pub async fn execute(args: CommonArgs) -> anyhow::Result<()> {
    let config = Arc::new(Config::load(&args.config)?);
    let client = MarcasiteClient::new(config.clone());
    let registry = Registry::default();
    let clock = Arc::new(SystemClock);

    let plan = fetch_markets_from_gamma(&client, &registry, config.clone(), clock.clone())
        .await
        .unwrap();

    println!("{}", plan);

    Ok(())
}
