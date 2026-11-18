//! Where Polymarket's services live and how clients for them are built.
//!
//! [build_clients] gives the archiver one [Polymarket] over one HTTP client with no budget
//! (the archiver paces its own requests). [Clients::build] gives the live system three
//! separate request budgets (D12 of the Phase 1 spec), each a marcasite [HttpClient] with its
//! own [RateLimit] and [RetryPolicy]: `meta` (Gamma, CLOB and geoblock), `data_priority`
//! (identity lookups and window features) and `data_bulk` (volume backfill and wallet paging),
//! so a long backfill never queues ahead of an identity lookup. Rate limiting, retries and
//! `Retry-After` are marcasite's; nothing here re-implements them.

use std::{num::NonZeroU32, time::Duration};

use marcasite::{
    HttpClient, Polymarket, RateLimit, RetryPolicy, data::DataClient, geoblock::GeoblockClient,
    ws::MarketChannelBuilder,
};
use serde::Deserialize;
use url::Url;

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
    /// Geoblock check (`/api/geoblock` under this base).
    pub geoblock: Url,
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
            geoblock: parse("https://polymarket.com"),
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
        .data_base_url(endpoints.data.as_str())
        .geoblock_base_url(endpoints.geoblock.as_str());
    if settings.max_retries > 0 {
        builder = builder.retry_policy(RetryPolicy::new(settings.max_retries));
    }
    Ok(builder.build()?)
}

/// Settings of the live system's HTTP clients (`[http]` in its config): shared timeouts, user
/// agent and retries, and the three request budgets of D12.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HttpConfig {
    /// Whole-request timeout.
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    /// Sent as `User-Agent`; generic by default.
    pub user_agent: String,
    /// Automatic retries of idempotent requests that fail transiently (0 for none). Every
    /// attempt waits for its budget.
    pub max_retries: u32,
    /// Gamma, CLOB and geoblock requests a second.
    pub meta_rps: u32,
    pub meta_burst: u32,
    /// Identity lookups and window features, requests a second.
    pub data_priority_rps: u32,
    pub data_priority_burst: u32,
    /// Volume backfill and wallet paging, requests a second.
    pub data_bulk_rps: u32,
    pub data_bulk_burst: u32,
}

impl Default for HttpConfig {
    fn default() -> Self {
        let settings = ClientSettings::default();
        Self {
            timeout: settings.timeout,
            connect_timeout: settings.connect_timeout,
            user_agent: settings.user_agent,
            max_retries: settings.max_retries,
            meta_rps: 10,
            meta_burst: 1,
            data_priority_rps: 6,
            data_priority_burst: 3,
            data_bulk_rps: 4,
            data_bulk_burst: 1,
        }
    }
}

/// The most Data API requests a second the two Data budgets may add up to (D12).
pub const MAX_DATA_RPS: u32 = 10;

impl HttpConfig {
    /// # Errors
    ///
    /// If a timeout or budget is zero, or the Data budgets add up to more than [MAX_DATA_RPS].
    pub fn validate(&self) -> Result<(), InvariantViolation> {
        invariant!(
            !self.timeout.is_zero() && !self.connect_timeout.is_zero(),
            Err(InvariantViolation("client timeouts are non-zero"))
        );
        invariant!(
            self.connect_timeout <= self.timeout,
            Err(InvariantViolation(
                "the connect timeout is within the request timeout"
            ))
        );
        let budgets = [
            self.meta_rps,
            self.meta_burst,
            self.data_priority_rps,
            self.data_priority_burst,
            self.data_bulk_rps,
            self.data_bulk_burst,
        ];
        invariant!(
            budgets.iter().all(|&n| n > 0),
            Err(InvariantViolation("every request budget is positive"))
        );
        invariant!(
            self.data_priority_rps.saturating_add(self.data_bulk_rps) <= MAX_DATA_RPS,
            Err(InvariantViolation(
                "the Data API budgets add up to at most 10 a second"
            ))
        );
        Ok(())
    }

    fn http_client(&self, rps: u32, burst: u32) -> Result<HttpClient, ClientError> {
        let positive =
            |n: u32| NonZeroU32::new(n).ok_or(InvariantViolation("request budgets are positive"));
        let mut builder = HttpClient::builder()
            .timeout(self.timeout)
            .connect_timeout(self.connect_timeout)
            .user_agent(self.user_agent.clone())
            .rate_limit(RateLimit::per_second(positive(rps)?).with_burst(positive(burst)?));
        if self.max_retries > 0 {
            builder = builder.retry_policy(RetryPolicy::new(self.max_retries));
        }
        Ok(builder.build()?)
    }
}

/// The live system's REST clients, one request budget each (D12). Clones of a client share
/// its budget.
#[derive(Debug, Clone)]
pub struct Clients {
    /// Gamma, CLOB (and its Data client, unused) on the `meta` budget.
    pub meta: Polymarket,
    /// Data API on the `priority` budget: identity resolver, window features.
    pub data_priority: DataClient,
    /// Data API on the `bulk` budget: volume backfill, wallet-level paging.
    pub data_bulk: DataClient,
    /// Geoblock check, on the `meta` budget.
    pub geoblock: GeoblockClient,
}

impl Clients {
    /// # Errors
    ///
    /// If the config is invalid ([HttpConfig::validate]), an endpoint is unusable or an HTTP
    /// client cannot be built.
    pub fn build(endpoints: &Endpoints, config: &HttpConfig) -> Result<Self, ClientError> {
        config.validate()?;
        let meta_http = config.http_client(config.meta_rps, config.meta_burst)?;
        let meta = Polymarket::builder()
            .http_client(meta_http)
            .gamma_base_url(endpoints.gamma.as_str())
            .clob_base_url(endpoints.clob.as_str())
            .data_base_url(endpoints.data.as_str())
            .geoblock_base_url(endpoints.geoblock.as_str())
            .build()?;
        let data = |rps, burst| -> Result<DataClient, ClientError> {
            Ok(DataClient::builder()
                .http_client(config.http_client(rps, burst)?)
                .base_url(endpoints.data.as_str())
                .build()?)
        };
        let clients = Self {
            data_priority: data(config.data_priority_rps, config.data_priority_burst)?,
            data_bulk: data(config.data_bulk_rps, config.data_bulk_burst)?,
            geoblock: meta.geoblock().clone(),
            meta,
        };
        invariant!(
            clients.meta.http_client().has_rate_limit(),
            Err(InvariantViolation("the meta client has a budget").into())
        );
        Ok(clients)
    }
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

    #[test]
    fn http_config_defaults_follow_d12() {
        let config = HttpConfig::default();
        assert_eq!(
            (config.data_priority_rps, config.data_priority_burst),
            (6, 3)
        );
        assert_eq!((config.data_bulk_rps, config.data_bulk_burst), (4, 1));
        assert_eq!(config.meta_rps, 10);
        assert!(config.validate().is_ok());
        let too_much = HttpConfig {
            data_bulk_rps: 5,
            ..HttpConfig::default()
        };
        assert!(too_much.validate().is_err());
        let zero = HttpConfig {
            meta_burst: 0,
            ..HttpConfig::default()
        };
        assert!(matches!(
            Clients::build(&Endpoints::default(), &zero),
            Err(ClientError::Settings(_))
        ));
        let parsed: HttpConfig =
            from_json(r#"{"timeout": "5s", "data_bulk_rps": 2}"#).expect("parses");
        assert_eq!(parsed.timeout, Duration::from_secs(5));
        assert_eq!(parsed.data_bulk_rps, 2);
    }

    mod budgets {
        //! The limiter itself is marcasite's (and tested there); these check the wiring: which
        //! clients share a budget. Real sockets cannot run on paused tokio time (the clock
        //! jumps while I/O is pending), so these use real time with small numbers and assert
        //! lower bounds for throttled requests and a generous upper bound for the free one.

        use std::time::Instant;

        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path},
        };

        use super::*;

        const TRADES: &str =
            r#"{"data": [], "pagination": {"limit": 100, "offset": 0, "has_more": false}}"#;

        async fn server() -> MockServer {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/v2/trades"))
                .respond_with(ResponseTemplate::new(200).set_body_raw(TRADES, "application/json"))
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/markets"))
                .respond_with(ResponseTemplate::new(200).set_body_raw("[]", "application/json"))
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/clob-markets/0xabc"))
                .respond_with(ResponseTemplate::new(200).set_body_raw("{}", "application/json"))
                .mount(&server)
                .await;
            server
        }

        fn clients(server: &MockServer, config: HttpConfig) -> Clients {
            let url = Url::parse(&server.uri()).unwrap();
            let endpoints = Endpoints {
                gamma: url.clone(),
                clob: url.clone(),
                data: url.clone(),
                geoblock: url,
                ..Endpoints::default()
            };
            Clients::build(&endpoints, &config).unwrap()
        }

        #[tokio::test]
        async fn bulk_backlog_does_not_delay_priority() {
            let server = server().await;
            let clients = clients(&server, HttpConfig::default());
            let start = Instant::now();
            // 9 bulk requests at 4 a second, burst 1: the last starts 2 s in.
            let bulk: Vec<_> = (0..9)
                .map(|_| {
                    let data = clients.data_bulk.clone();
                    tokio::spawn(async move { data.list_trades().limit(1).send().await })
                })
                .collect();
            tokio::time::sleep(Duration::from_millis(100)).await;
            let asked = Instant::now();
            clients
                .data_priority
                .list_trades()
                .limit(1)
                .send()
                .await
                .unwrap();
            let priority_took = asked.elapsed();
            for task in bulk {
                task.await.unwrap().unwrap();
            }
            let bulk_took = start.elapsed();
            assert!(
                bulk_took >= Duration::from_millis(2000),
                "bulk took {bulk_took:?}"
            );
            assert!(
                priority_took < Duration::from_millis(500),
                "priority waited {priority_took:?} behind the bulk backlog"
            );
        }

        #[tokio::test]
        async fn gamma_and_clob_share_the_meta_budget() {
            let server = server().await;
            let config = HttpConfig {
                meta_rps: 4,
                meta_burst: 1,
                ..HttpConfig::default()
            };
            let clients = clients(&server, config);
            let start = Instant::now();
            for _ in 0..3 {
                clients
                    .meta
                    .gamma()
                    .list_markets()
                    .limit(1)
                    .send()
                    .await
                    .unwrap();
                clients
                    .meta
                    .clob()
                    .get_clob_market_info("0xabc")
                    .await
                    .unwrap();
            }
            // 6 requests at 4 a second, burst 1, on one budget: the last starts 1.25 s in.
            let took = start.elapsed();
            assert!(took >= Duration::from_millis(1250), "took {took:?}");
        }
    }
}
