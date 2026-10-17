use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::TryStreamExt;
use marcasite::gamma::{GammaClient, Market, TagId};
use marcasite::types::ConditionId;
use rust_decimal::Decimal;

use crate::config::Config;

/// How many condition IDs to look up per Gamma request, to keep the URL a sensible length.
const CONDITION_IDS_PER_REQUEST: usize = 50;

/// Gamma API client to pull data from the API.
#[async_trait]
pub trait APIClient: Send + Sync + 'static {
    /// Fetch every page of open markets in the configured categories at or above the liquidity
    /// floor.
    async fn fetch_open_markets(&self) -> Result<Vec<Market>>;

    /// Fetch markets by condition ID, whether open or closed.
    async fn fetch_markets(&self, ids: &[ConditionId]) -> Result<Vec<Market>>;
}

/// An implementation of [APIClient] which connects to the Gamma API
pub struct MarcasiteClient {
    /// Underlying client to send API requests
    client: GammaClient,
    /// IDs of the configured category tags
    tag_ids: Vec<TagId>,
    min_liquidity_usd: Decimal,
}

impl MarcasiteClient {
    /// Create a client for the configured Gamma URL and resolve the configured category tags.
    ///
    /// # Errors
    ///
    /// If the client cannot be built, Gamma cannot be reached, or a tag does not exist.
    pub async fn connect(config: &Config) -> Result<Self> {
        let client = GammaClient::builder()
            .base_url(&config.gamma_url)
            .build()
            .context("failed to create Gamma API client")?;

        let client_ref = &client;
        let tag_ids =
            futures::future::try_join_all(config.category_tags.iter().map(|tag| async move {
                client_ref
                    .get_tag_by_slug(tag)
                    .send()
                    .await
                    .with_context(|| format!("failed to look up category tag {tag:?}"))?
                    .id
                    .with_context(|| format!("category tag {tag:?} has no ID"))
            }))
            .await?;

        Ok(Self {
            client,
            tag_ids,
            min_liquidity_usd: config.min_liquidity_usd,
        })
    }
}

#[async_trait]
impl APIClient for MarcasiteClient {
    async fn fetch_open_markets(&self) -> Result<Vec<Market>> {
        self.client
            .list_markets_keyset()
            .tag_ids(self.tag_ids.clone())
            .closed(false)
            .liquidity_num_min(self.min_liquidity_usd)
            .include_tag(true)
            .into_stream()
            .try_collect()
            .await
            .context("failed to list open markets")
    }

    async fn fetch_markets(&self, ids: &[ConditionId]) -> Result<Vec<Market>> {
        let mut markets = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(CONDITION_IDS_PER_REQUEST) {
            // The keyset listing leaves out closed markets unless `closed` is set, so ask for both.
            for closed in [true, false] {
                let page: Vec<Market> = self
                    .client
                    .list_markets_keyset()
                    .condition_ids(chunk.to_vec())
                    .closed(closed)
                    .include_tag(true)
                    .into_stream()
                    .try_collect()
                    .await
                    .context("failed to look up markets by condition ID")?;
                markets.extend(page);
            }
        }
        Ok(markets)
    }
}
