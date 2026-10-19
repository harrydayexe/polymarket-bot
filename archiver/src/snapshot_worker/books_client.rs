use anyhow::{Context, Result};
use async_trait::async_trait;
use marcasite::{
    clob::{ClobClient, OrderBookSummary},
    types::TokenId,
};

use crate::config::Config;

/// Fetches full order books from the CLOB.
#[async_trait]
pub trait BooksClient: Send + Sync + 'static {
    /// The order books of `tokens` (`POST /books`). Tokens without a book are left out.
    async fn fetch_books(&self, tokens: &[TokenId]) -> Result<Vec<OrderBookSummary>>;
}

/// A [BooksClient] for the configured CLOB REST URL.
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
    async fn fetch_books(&self, tokens: &[TokenId]) -> Result<Vec<OrderBookSummary>> {
        self.client
            .get_order_books(tokens)
            .await
            .context("failed to fetch order books")
    }
}
