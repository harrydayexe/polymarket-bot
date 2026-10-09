use std::sync::Arc;

use anyhow::Result;
use tokio_util::sync::CancellationToken;

use crate::{
    client::APIClient,
    clock::SharedClock,
    config::Config,
    market_registry::{registry::Registry, token_changes::TokenChanges},
};

pub async fn fetch_markets_from_gamma(
    client: Arc<impl APIClient>,
    registry: &mut Registry,
    config: Arc<Config>,
    clock: SharedClock,
    token: CancellationToken,
) -> Result<Option<TokenChanges>> {
    let at = clock.now();
    tokio::select! {
        _ = token.cancelled() => Ok(None),
        r = async {
            let results = client.fetch_page(&config.category_tags).await?;
            let changes = registry.get_changes(results, config, at).await?;
            tracing::info!("registry updated");
            Ok(Some(changes))
        } => r,
    }
}
