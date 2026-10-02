use std::sync::Mutex;
use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result};
use futures::stream::{self, StreamExt, TryStreamExt};
use marcasite::Paginated;
use marcasite::gamma::TagId;
use marcasite::gamma::{GammaClient, Market};

use crate::config::Config;

/// Gamma API client to pull data from the API.
pub trait APIClient: Send + Sync + 'static {
    /// Fetch the id of a tag.
    async fn fetch_tag_id(&self, tag: &String) -> Result<TagId>;

    /// Fetch all the markets for a set of tags.
    ///
    /// The implementation of this function should use reasonable defaults (such as excluding closed
    /// markets).
    ///
    /// # Arguments
    ///
    /// * `tags` - The tags to fetch markets for.
    /// * `after_cursor` - An optional cursor for stable paging. By default will fetch the first
    ///   page.
    async fn fetch_page(&self, tags: &[String]) -> Result<Paginated<Market>>;
}

/// An implementation of GammaClient which connects to the Gamma API
pub struct MarcasiteClient {
    /// Underlying client to send API requests
    client: GammaClient,
    /// Caches a map of resolved tags to their IDs
    tag_map: Mutex<HashMap<String, TagId>>,
    /// Holds the config for the client
    config: Arc<Config>,
}

impl MarcasiteClient {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            client: GammaClient::new()
                .context("failed to create Gamma API Client")
                .unwrap(),
            tag_map: Mutex::new(HashMap::new()),
            config,
        }
    }
}

impl APIClient for MarcasiteClient {
    async fn fetch_tag_id(&self, tag: &String) -> Result<TagId> {
        // Lock, copy out, and release before any .await
        let cached = self.tag_map.lock().unwrap().get(tag).cloned();
        if let Some(id) = cached {
            return Ok(id);
        }

        let tag_id = self
            .client
            .get_tag_by_slug(tag)
            .send()
            .await
            .context("failed to retrieve tag by name")?
            .id
            .context("received data did not include a tag ID")?;

        self.tag_map
            .lock()
            .unwrap()
            .insert(tag.clone(), tag_id.clone());

        Ok(tag_id)
    }

    async fn fetch_page(&self, tags: &[String]) -> Result<Paginated<Market>> {
        let tag_ids: Vec<TagId> = stream::iter(tags)
            .map(|tag| self.fetch_tag_id(tag))
            .buffered(10)
            .try_collect()
            .await
            .context("failed to retrieve IDs for tag names")?;

        Ok(self
            .client
            .list_markets_keyset()
            .tag_ids(tag_ids)
            .closed(false)
            .liquidity_num_min(self.config.min_liquidity_usd)
            .into_stream())
    }
}
