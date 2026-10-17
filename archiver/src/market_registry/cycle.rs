use std::time::Duration;

use chrono::{DateTime, Utc};
use marcasite::gamma::Market;
use thiserror::Error;

use crate::{
    client::APIClient,
    market_registry::{
        market_info::MarketInfo,
        not_enough_markets_error::NotEnoughMarketsError,
        registry::{PlannedChanges, Registry},
        selection::parse_markets,
    },
};

/// Why a registry cycle changed nothing. None of these stop the registry: it keeps its current set
/// and tries again next cycle.
#[derive(Debug, Error)]
pub enum CycleError {
    #[error("fetching markets from Gamma failed: {0:#}")]
    Fetch(anyhow::Error),
    #[error("Gamma did not respond within {0:?}")]
    Timeout(Duration),
    #[error("Gamma response rejected: {0}")]
    Rejected(#[from] NotEnoughMarketsError),
}

/// Fetch and parse the open-market listing. Malformed markets are skipped with a warning.
pub async fn fetch_listing(
    client: &impl APIClient,
    fetched_at: DateTime<Utc>,
) -> Result<Vec<MarketInfo>, CycleError> {
    let markets = client
        .fetch_open_markets()
        .await
        .map_err(CycleError::Fetch)?;
    Ok(parse_and_log(markets, fetched_at))
}

/// Fetch everything one registry cycle needs from Gamma and plan the changes.
pub async fn plan_cycle(
    client: &impl APIClient,
    registry: &Registry,
    at: DateTime<Utc>,
) -> Result<PlannedChanges, CycleError> {
    let listing = fetch_listing(client, at).await?;
    let accepted_count = registry.check_listing(&listing)?;

    let missing = registry.missing_from(&listing);
    let followup = if missing.is_empty() {
        Vec::new()
    } else {
        let markets = client
            .fetch_markets(&missing)
            .await
            .map_err(CycleError::Fetch)?;
        parse_and_log(markets, at)
    };

    Ok(registry.plan_changes(listing, followup, accepted_count, at))
}

fn parse_and_log(markets: Vec<Market>, fetched_at: DateTime<Utc>) -> Vec<MarketInfo> {
    let (parsed, skipped) = parse_markets(markets, fetched_at);
    for market in &skipped {
        tracing::warn!(
            condition_id = ?market.condition_id,
            reason = %market.reason,
            "skipping malformed market from Gamma"
        );
    }
    parsed
}
