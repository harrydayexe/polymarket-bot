use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, Utc};
use tokio_util::sync::CancellationToken;

use crate::{
    client::APIClient,
    config::Config,
    market_registry::registry::{PlannedChanges, Registry},
};

pub async fn fetch_from_gamma(
    client: Arc<impl APIClient>,
    registry: &mut Registry,
    config: Arc<Config>,
    at: DateTime<Utc>,
    token: CancellationToken,
) -> Result<Option<PlannedChanges>> {
    tokio::select! {
        _ = token.cancelled() => Ok(None),
        r = async {
            let results = client.fetch_page(&config.category_tags).await?;
            let plan = registry.plan_changes(results, at).await?;
            tracing::info!("registry plan resolved");
            Ok(Some(plan))
        } => r,
    }
}
