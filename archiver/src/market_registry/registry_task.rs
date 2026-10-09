use std::{sync::Arc, time::Duration};

use marcasite::types::ConditionId;
use tokio::time;
use tokio_util::sync::CancellationToken;

use crate::{
    client::APIClient,
    clock::SharedClock,
    config::Config,
    market_registry::{registry::Registry, tracked_market::TrackedMarket},
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
                todo!("Need to implement cancellation");
            }
            _ = tick.tick() => {
                todo!("Need to implement tick");
            }
        }
    }
}
