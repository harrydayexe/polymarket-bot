use std::sync::Arc;

use anyhow::Result;

use crate::{
    clock::Clock,
    config::Config,
    gamma_client::client::GammaClient,
    market_registry::{
        market_info::MarketInfo,
        registry::{PlannedChanges, Registry, plan_changes},
    },
};

pub async fn fetch_markets_from_gamma(
    gamma: &impl GammaClient,
    registry: &Registry,
    config: Arc<Config>,
    clock: &impl Clock,
) -> Result<PlannedChanges> {
    let mut markets = Vec::<MarketInfo>::new();
    let mut count = 1;

    let mut cursor = None;
    while let Some(next_cursor) =
        fetch_next_page(gamma, &mut markets, &config.category_tags, cursor).await?
    {
        println!("Fetched Page: {}", count);
        count += 1;
        cursor = Some(next_cursor);
    }

    Ok(plan_changes(registry, markets, config, clock))
}

async fn fetch_next_page(
    gamma: &impl GammaClient,
    markets: &mut Vec<MarketInfo>,
    tags: &[String],
    after_cursor: Option<String>,
) -> Result<Option<String>> {
    let resp = gamma.fetch_page(tags, after_cursor).await?;

    let market_info: Vec<MarketInfo> = resp
        .markets
        .into_iter()
        .filter_map(|x| MarketInfo::try_from(x).ok())
        .collect();

    markets.extend(market_info);

    Ok(resp.next_cursor)
}
