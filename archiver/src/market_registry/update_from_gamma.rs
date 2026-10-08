use std::sync::Arc;

use anyhow::Result;

use crate::{
    client::APIClient, clock::SharedClock, config::Config, market_registry::registry::Registry,
};

pub async fn fetch_markets_from_gamma(
    client: &impl APIClient,
    registry: &mut Registry,
    config: Arc<Config>,
    clock: SharedClock,
) -> Result<()> {
    let results = client.fetch_page(&config.category_tags).await?;

    registry.get_changes(results, config, clock).await
}
