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

#[cfg(test)]
mod tests {
    use marcasite::clob::OrderBookSummary;
    use serde_json::json;

    use crate::archive::snapshot_book::SnapshotBook;

    /// A token that has never traded, as `/books` sends it live.
    #[test]
    fn untraded_books_decode_and_archive_unchanged() {
        let sent = json!({
            "market": "0x6231ec0ba9d53a7d36c0328dfbd834e778f6e891ec6438d7d3e6f39d377c8f32",
            "asset_id": "110212708220762723780623631669128285743865261243074067496163475281418328413543",
            "timestamp": "1793487370444",
            "hash": "43f855733c179fc034ebe5cae47a30db7c3aeca1",
            "bids": [{"price": "0.001", "size": "4000021.34"}],
            "asks": [{"price": "0.999", "size": "16876"}],
            "min_order_size": "5",
            "tick_size": "0.001",
            "neg_risk": true,
            "last_trade_price": ""
        });
        let book: OrderBookSummary = serde_json::from_value(sent.clone()).unwrap();
        assert_eq!(book.last_trade_price, None);

        let archived = serde_json::to_value(&book).unwrap();
        assert_eq!(archived, sent, "archived as it was sent");
        let read = SnapshotBook::parse(&archived).unwrap();
        assert_eq!(read.last_trade_price, None);
        assert_eq!(read.bids.len(), 1);
    }
}
