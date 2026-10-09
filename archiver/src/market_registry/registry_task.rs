use std::{sync::Arc, time::Duration};

use marcasite::types::ConditionId;
use tokio::time;
use tokio_util::sync::CancellationToken;

use crate::{
    client::APIClient,
    clock::SharedClock,
    config::Config,
    market_registry::{
        fetch_from_gamma::fetch_markets_from_gamma, registry::Registry,
        tracked_market::TrackedMarket,
    },
};

pub enum RegistryMsg {
    MarketResolved(ConditionId),
    NewMarket(TrackedMarket),
}

pub async fn registry_task(
    token: CancellationToken,
    client: Arc<impl APIClient>,
    config: Arc<Config>,
    clock: SharedClock,
) {
    let mut registry = Registry::default();
    let mut tick = time::interval(Duration::from_secs(config.registry_interval_s));

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                break;
            }
            _ = tick.tick() => {
                if let Err(e) = fetch_markets_from_gamma(client.clone(), &mut registry, config.clone(), clock.clone(), token.clone()) .await {
                    tracing::warn!(error = format!("{e:#}"), "registry update failed");
                }
            }
        }
    }
}
