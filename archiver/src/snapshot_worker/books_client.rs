use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use marcasite::types::TokenId;
use reqwest::{StatusCode, header::RETRY_AFTER};
use serde_json::{Value, json};
use thiserror::Error;

use crate::config::Config;

/// How long one `/books` request may take, response body included.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Fetches full order books from the CLOB.
#[async_trait]
pub trait BooksClient: Send + Sync + 'static {
    /// The order books of `tokens` (`POST /books`), each exactly as the exchange sent it.
    /// Tokens without a book are left out.
    async fn fetch_books(&self, tokens: &[TokenId]) -> Result<Vec<Value>>;
}

/// `/books` answered with an error status.
#[derive(Debug, Error)]
#[error("POST /books returned {status}: {body}")]
pub struct BooksHttpError {
    pub status: StatusCode,
    /// From the `Retry-After` header, in seconds.
    pub retry_after: Option<Duration>,
    body: String,
}

/// A [BooksClient] for the configured CLOB REST URL.
///
/// It does not decode the books: the archive keeps them as sent (section 9), and a field that
/// changes shape (such as an empty `last_trade_price`) must never cost a whole batch.
pub struct ClobBooksClient {
    client: reqwest::Client,
    url: String,
}

impl ClobBooksClient {
    /// # Errors
    ///
    /// If the HTTP client cannot be built.
    pub fn new(config: &Config) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .user_agent(concat!("archiver/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("failed to create CLOB HTTP client")?;
        Ok(Self {
            client,
            url: format!("{}/books", config.clob_rest_url.trim_end_matches('/')),
        })
    }
}

#[async_trait]
impl BooksClient for ClobBooksClient {
    async fn fetch_books(&self, tokens: &[TokenId]) -> Result<Vec<Value>> {
        let body: Vec<Value> = tokens.iter().map(|t| json!({ "token_id": t })).collect();
        let response = self
            .client
            .post(&self.url)
            .json(&body)
            .send()
            .await
            .context("POST /books failed")?;
        let status = response.status();
        if !status.is_success() {
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.trim().parse::<u64>().ok())
                .map(Duration::from_secs);
            let mut body = response.text().await.unwrap_or_default();
            body.truncate(512);
            return Err(BooksHttpError {
                status,
                retry_after,
                body,
            }
            .into());
        }
        let text = response
            .text()
            .await
            .context("could not read the /books response")?;
        match serde_json::from_str::<Value>(&text).context("/books response is not JSON")? {
            Value::Array(books) => Ok(books),
            other => {
                let mut start = other.to_string();
                start.truncate(256);
                anyhow::bail!("/books response is not a list: {start}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::*;

    /// Serves one HTTP response, and hands back the request body.
    async fn serve_once(response: &'static str) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![];
            let mut buf = [0u8; 4096];
            loop {
                let n = socket.read(&mut buf).await.unwrap();
                request.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&request).to_string();
                if let Some((head, body)) = text.split_once("\r\n\r\n") {
                    let len: usize = head
                        .lines()
                        .find_map(|l| {
                            l.to_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse().unwrap())
                        })
                        .unwrap_or(0);
                    if body.len() >= len {
                        socket.write_all(response.as_bytes()).await.unwrap();
                        return body.to_string();
                    }
                }
            }
        });
        (url, task)
    }

    fn client(url: String) -> ClobBooksClient {
        ClobBooksClient::new(&Config {
            clob_rest_url: url,
            ..Config::default()
        })
        .unwrap()
    }

    #[tokio::test]
    async fn returns_books_as_sent() {
        let body = r#"[{"asset_id":"1","bids":[],"asks":[],"last_trade_price":""},{"asset_id":"2","bids":[],"asks":[],"future_field":1}]"#;
        let response: &'static str = Box::leak(
            format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            )
            .into_boxed_str(),
        );
        let (url, server) = serve_once(response).await;
        let books = client(url)
            .fetch_books(&[TokenId::new("1"), TokenId::new("2")])
            .await
            .unwrap();
        assert_eq!(books, serde_json::from_str::<Vec<Value>>(body).unwrap());
        let request: Value = serde_json::from_str(&server.await.unwrap()).unwrap();
        assert_eq!(request, json!([{"token_id":"1"},{"token_id":"2"}]));
    }

    #[tokio::test]
    async fn an_error_status_carries_retry_after() {
        let (url, _server) = serve_once(
            "HTTP/1.1 429 Too Many Requests\r\nretry-after: 7\r\ncontent-length: 4\r\nconnection: close\r\n\r\nslow",
        )
        .await;
        let err = client(url)
            .fetch_books(&[TokenId::new("1")])
            .await
            .unwrap_err();
        let http = err.downcast_ref::<BooksHttpError>().unwrap();
        assert_eq!(http.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(http.retry_after, Some(Duration::from_secs(7)));
    }
}
