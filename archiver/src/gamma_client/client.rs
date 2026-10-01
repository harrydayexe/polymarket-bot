use std::sync::Mutex;
use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use futures::stream::{self, StreamExt, TryStreamExt};

use crate::{
    config::Config,
    gamma_client::{keyset_markets_response::KeysetMarketsResponse, tag_slug::TagSlug},
};

/// Gamma API client to pull data from the API.
pub trait GammaClient: Send + Sync {
    /// Fetch the id of a tag.
    async fn fetch_tag_id(&self, tag: String) -> Result<String>;

    /// Fetch a page of data from the Gamma API endpoint.
    ///
    /// # Arguments
    ///
    /// * `tags` - The tags to fetch markets for.
    /// * `after_cursor` - An optional cursor for stable paging. By default will fetch the first
    ///   page.
    async fn fetch_page(
        &self,
        tags: Vec<String>,
        after_cursor: Option<String>,
    ) -> Result<KeysetMarketsResponse>;
}

const BASE_URL: &str = "https://gamma-api.polymarket.com";
const TAG_SLUG_ENDPOINT: &str = "tags/slug";
const MARKETS_KEYSET_ENDPOINT: &str = "markets/keyset";

/// An implementation of GammaClient which connects to the Gamma API
pub struct APIClient {
    /// Underlying client to send API requests
    client: reqwest::Client,
    /// Caches a map of resolved tags to their IDs
    tag_map: Mutex<HashMap<String, String>>,
    /// Holds the config for the client
    config: Arc<Config>,
}

impl APIClient {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            client: reqwest::Client::new(),
            tag_map: Mutex::new(HashMap::new()),
            config,
        }
    }
}

impl GammaClient for APIClient {
    async fn fetch_tag_id(&self, tag: String) -> Result<String> {
        // Lock, copy out, and release before any .await
        let cached = self.tag_map.lock().unwrap().get(&tag).cloned();
        if let Some(id) = cached {
            return Ok(id);
        }

        let tag_resp: TagSlug = self
            .client
            .get(format!(
                "{}/{TAG_SLUG_ENDPOINT}/{tag}",
                self.config.gamma_url
            ))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        self.tag_map
            .lock()
            .unwrap()
            .insert(tag, tag_resp.id.clone());

        Ok(tag_resp.id)
    }

    async fn fetch_page(
        &self,
        tags: Vec<String>,
        after_cursor: Option<String>,
    ) -> Result<KeysetMarketsResponse> {
        let tag_ids: Vec<String> = stream::iter(tags)
            .map(|tag| self.fetch_tag_id(tag))
            .buffered(10)
            .try_collect()
            .await?;

        let req = self
            .client
            .get(format!(
                "{}/{MARKETS_KEYSET_ENDPOINT}",
                self.config.gamma_url
            ))
            .query(&[("limit", "100")])
            .query(&[("after_cursor", after_cursor.as_deref())])
            .query(&tag_ids.iter().map(|id| ("tag_id", id)).collect::<Vec<_>>())
            .build()?;

        println!("{}", req.url());

        let resp: KeysetMarketsResponse = self
            .client
            .execute(req)
            .await?
            .error_for_status()?
            .json()
            .await?;

        Ok(resp)
    }
}
