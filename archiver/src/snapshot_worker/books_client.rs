use anyhow::{Context, Result};
use async_trait::async_trait;
use marcasite::{clob::ClobClient, types::TokenId};
use serde_json::Value;

use crate::config::Config;

/// Fetches full order books from the CLOB.
#[async_trait]
pub trait BooksClient: Send + Sync + 'static {
    /// The order books of `tokens` (`POST /books`), as JSON objects in the shape of
    /// marcasite's `OrderBookSummary`. Tokens without a book are left out.
    async fn fetch_books(&self, tokens: &[TokenId]) -> Result<Vec<Value>>;
}

/// A [BooksClient] for the configured CLOB REST URL, through marcasite.
pub struct ClobBooksClient {
    client: ClobClient,
}

impl ClobBooksClient {
    /// # Errors
    ///
    /// If the client cannot be built, e.g. because the URL is invalid.
    pub fn new(config: &Config) -> Result<Self> {
        let client = ClobClient::builder()
            .base_url(&config.clob_rest_url)
            .build()
            .context("failed to create CLOB API client")?;
        Ok(Self { client })
    }
}

#[async_trait]
impl BooksClient for ClobBooksClient {
    async fn fetch_books(&self, tokens: &[TokenId]) -> Result<Vec<Value>> {
        let books = self
            .client
            .get_order_books(tokens)
            .await
            .context("failed to fetch order books")?;
        // Archived as JSON, so the record format does not depend on marcasite's types.
        books
            .iter()
            .map(|book| serde_json::to_value(book).context("could not serialise an order book"))
            .collect()
    }
}
