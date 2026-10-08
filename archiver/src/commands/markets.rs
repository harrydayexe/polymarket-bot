use std::sync::Arc;

use crate::client::MarcasiteClient;
use crate::clock::SystemClock;
use crate::config::Config;
use crate::market_registry::fetch_from_gamma::fetch_markets_from_gamma;
use crate::market_registry::registry::Registry;

use crate::cli::CommonArgs;

pub async fn execute(args: CommonArgs) -> anyhow::Result<()> {
    let config = Arc::new(Config::load(&args.config)?);
    let client = MarcasiteClient::new(config.clone());
    let mut registry = Registry::default();
    let clock = Arc::new(SystemClock);

    fetch_markets_from_gamma(&client, &mut registry, config.clone(), clock.clone())
        .await
        .unwrap();

    println!("Discovered Markets:");
    println!("{}", registry);

    Ok(())
}
