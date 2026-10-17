use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::client::MarcasiteClient;
use crate::clock::{Clock, SystemClock};
use crate::config::Config;
use crate::market_registry::fetch_from_gamma::fetch_from_gamma;
use crate::market_registry::registry::Registry;

use crate::cli::CommonArgs;

pub async fn execute(args: CommonArgs, token: CancellationToken) -> anyhow::Result<()> {
    let config = Arc::new(Config::load(&args.config)?);
    let client = Arc::new(MarcasiteClient::new(config.clone()));
    let mut registry = Registry::new(config.clone());
    let clock = Arc::new(SystemClock);

    let Some(plan) = fetch_from_gamma(client, &mut registry, config, clock.now(), token).await?
    else {
        return Ok(());
    };

    println!("Discovered Markets:");
    println!("{}", plan);

    Ok(())
}
