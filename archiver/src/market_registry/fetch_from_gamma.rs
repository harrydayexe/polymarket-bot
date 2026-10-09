use std::sync::Arc;

use anyhow::Result;
use tokio_util::sync::CancellationToken;

use crate::{
    client::APIClient, clock::SharedClock, config::Config, market_registry::registry::Registry,
};

pub async fn fetch_markets_from_gamma(
    client: Arc<impl APIClient>,
    registry: &mut Registry,
    config: Arc<Config>,
    clock: SharedClock,
    token: CancellationToken,
) -> Result<()> {
    let at = clock.now();
    tokio::select! {
        _ = token.cancelled() => Ok(()),
        r = async {
            let results = client.fetch_page(&config.category_tags).await?;
            registry.get_changes(results, config, at).await?;
            Ok(())
        } => r,
    }
}
