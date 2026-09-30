use std::collections::HashMap;

use anyhow::Ok;

use crate::gamma_client::tag_slug::TagSlug;

/// Gamma API client to pull data from the API.
pub trait GammaClient: Send + Sync {
    /// Fetch the id of a tag.
    async fn fetch_tag_id(&self, tag: String) -> Result<String, anyhow::Error>;

    // /// Fetch a page of data from the Gamma API endpoint.
    // ///
    // /// # Arguments
    // ///
    // /// * `after_cursor` - An optional cursor for stable paging. By default will fetch the first
    // /// page.
    // async fn fetch_page(&self, after_cursor: Option<String>) -> Result<String, anyhow::Error>;
}

const BASE_URL: &str = "https://gamma-api.polymarket.com";
const TAG_SLUG_ENDPOINT: &str = "tags/slug";

/// An implementation of GammaClient which connects to the Gamma API
pub struct APIClient {
    /// Underlying client to send API requests
    client: reqwest::Client,
    /// Caches a map of resolved tags to their IDs
    tag_map: HashMap<String, String>,
}

impl Default for APIClient {
    fn default() -> Self {
        Self {
            client: reqwest::Client::new(),
            tag_map: HashMap::<String, String>::new(),
        }
    }
}

impl GammaClient for APIClient {
    async fn fetch_tag_id(&self, tag: String) -> Result<String, anyhow::Error> {
        let tag: TagSlug = self
            .client
            .get(format!("{BASE_URL}/{TAG_SLUG_ENDPOINT}/{tag}"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        Ok(tag.id)
    }
}
