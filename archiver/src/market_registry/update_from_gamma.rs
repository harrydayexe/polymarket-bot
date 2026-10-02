use std::sync::Arc;

use anyhow::Result;

use crate::{
    client::APIClient,
    clock::SharedClock,
    config::Config,
    market_registry::registry::{PlannedChanges, Registry, plan_changes},
};

pub async fn fetch_markets_from_gamma(
    client: &impl APIClient,
    registry: &Registry,
    config: Arc<Config>,
    clock: SharedClock,
) -> Result<PlannedChanges> {
    let results = client.fetch_page(&config.category_tags).await?;

    plan_changes(registry, results, config, clock).await
}
