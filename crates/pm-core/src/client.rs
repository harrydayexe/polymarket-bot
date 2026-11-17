//! Where Polymarket's services live and how clients for them are built: one shared HTTP
//! connection pool with common timeouts, user agent and retries ([ClientSettings]), and a
//! [SharedRateLimiter] to keep a group of requests within a budget.

pub mod rate_limiter;

use std::time::Duration;

use marcasite::{Polymarket, RetryPolicy, ws::MarketChannelBuilder};
use serde::Deserialize;
use url::Url;

pub use rate_limiter::{RateLimiter, SharedRateLimiter};

use crate::{invariant, invariant::InvariantViolation};

/// Base URLs of the services the system uses.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Endpoints {
    /// Gamma API: markets, events, tags.
    pub gamma: Url,
    /// CLOB REST API: books, market details, fees.
    pub clob: Url,
    /// Data API v2: trades, wallets.
    pub data: Url,
    /// Market channel websocket.
    pub ws: Url,
}

impl Default for Endpoints {
    /// Polymarket's production services.
    fn default() -> Self {
        let parse = |url: &str| Url::parse(url).expect("valid built-in URL");
        Self {
            gamma: parse("https://gamma-api.polymarket.com"),
            clob: parse("https://clob.polymarket.com"),
            data: parse("https://data-api.polymarket.com"),
            ws: parse("wss://ws-subscriptions-clob.polymarket.com/ws/market"),
        }
    }
}

/// Settings of the HTTP client every REST service shares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientSettings {
    /// Whole-request timeout.
    pub timeout: Duration,
    pub connect_timeout: Duration,
    /// Sent as `User-Agent`; generic by default.
    pub user_agent: String,
    /// Automatic retries of idempotent requests that fail transiently (0 for none).
    pub max_retries: u32,
}

impl Default for ClientSettings {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            user_agent: concat!("pm-core/", env!("CARGO_PKG_VERSION")).into(),
            max_retries: 2,
        }
    }
}

/// Why clients could not be built.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("could not build Polymarket clients: {0}")]
    Build(#[from] marcasite::Error),
    #[error("invalid client settings: {0}")]
    Settings(#[from] InvariantViolation),
}

/// Gamma, CLOB and Data API clients for `endpoints`, sharing one HTTP connection pool
/// configured by `settings`.
///
/// # Errors
///
/// If a timeout is zero, an endpoint is unusable or the HTTP client cannot be built.
pub fn build_clients(
    endpoints: &Endpoints,
    settings: &ClientSettings,
) -> Result<Polymarket, ClientError> {
    invariant!(
        !settings.timeout.is_zero() && !settings.connect_timeout.is_zero(),
        Err(InvariantViolation("client timeouts are non-zero").into())
    );
    invariant!(
        settings.connect_timeout <= settings.timeout,
        Err(InvariantViolation("the connect timeout is within the request timeout").into())
    );
    let mut builder = Polymarket::builder()
        .timeout(settings.timeout)
        .connect_timeout(settings.connect_timeout)
        .user_agent(settings.user_agent.clone())
        .gamma_base_url(endpoints.gamma.as_str())
        .clob_base_url(endpoints.clob.as_str())
        .data_base_url(endpoints.data.as_str());
    if settings.max_retries > 0 {
        builder = builder.retry_policy(RetryPolicy::new(settings.max_retries));
    }
    Ok(builder.build()?)
}

/// A market channel builder for the configured websocket endpoint; the caller adds the
/// subscription and heartbeat settings.
pub fn market_channel(endpoints: &Endpoints) -> MarketChannelBuilder {
    marcasite::ws::MarketChannel::builder().url(endpoints.ws.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_endpoints_by_default() {
        let endpoints = Endpoints::default();
        assert_eq!(endpoints.gamma.host_str(), Some("gamma-api.polymarket.com"));
        assert_eq!(endpoints.ws.scheme(), "wss");
    }

    #[test]
    fn endpoints_from_config_override_some() {
        let endpoints: Endpoints =
            from_json(r#"{"data": "http://127.0.0.1:9000/"}"#).expect("parses");
        assert_eq!(endpoints.data.as_str(), "http://127.0.0.1:9000/");
        assert_eq!(endpoints.clob, Endpoints::default().clob);
        assert!(from_json::<Endpoints>(r#"{"dta": "http://x/"}"#).is_err());
    }

    fn from_json<T: serde::de::DeserializeOwned>(json: &str) -> serde_json::Result<T> {
        serde_json::from_str(json)
    }

    #[test]
    fn clients_point_at_the_endpoints() {
        let endpoints = Endpoints {
            gamma: Url::parse("http://127.0.0.1:1/").unwrap(),
            ..Endpoints::default()
        };
        let clients = build_clients(&endpoints, &ClientSettings::default()).unwrap();
        assert_eq!(clients.gamma().base_url().as_str(), "http://127.0.0.1:1/");
        assert_eq!(
            clients.data().base_url().host_str(),
            Some("data-api.polymarket.com")
        );
    }

    #[test]
    fn zero_timeouts_are_refused() {
        let settings = ClientSettings {
            timeout: Duration::ZERO,
            ..ClientSettings::default()
        };
        assert!(matches!(
            build_clients(&Endpoints::default(), &settings),
            Err(ClientError::Settings(_))
        ));
        let settings = ClientSettings {
            connect_timeout: Duration::from_secs(60),
            ..ClientSettings::default()
        };
        assert!(build_clients(&Endpoints::default(), &settings).is_err());
    }
}
