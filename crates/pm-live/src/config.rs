//! `pmlive.toml`. Every key has a default; unknown keys are refused.

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use pm_core::{
    client::{Endpoints, HttpConfig},
    market::{RegistryConfig, SelectionRules},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// The whole config file.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub run: RunConfig,
    pub endpoints: Endpoints,
    pub http: HttpConfig,
    pub ingest: IngestConfig,
    pub engine: EngineConfig,
}

/// Where the run keeps its files.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RunConfig {
    /// Holds `engine_log/` and `records/`.
    pub data_dir: PathBuf,
    /// The dated fee table (§3.7).
    pub fee_table: PathBuf,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./pmlive-data"),
            fee_table: PathBuf::from("config/fee_schedule_history.toml"),
        }
    }
}

/// `[ingest]`: universe, registry, websocket connections and backfill (C2).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestConfig {
    /// Gamma tag slugs of the universe; the same as the archiver's.
    pub category_tags: Vec<String>,
    /// Liquidity floor for joining the universe; the same as the archiver's.
    pub min_liquidity_usd: Decimal,
    #[serde(with = "humantime_serde")]
    pub registry_interval: Duration,
    /// A `new_market` message brings the next registry poll forward, but polls stay at least
    /// this far apart.
    #[serde(with = "humantime_serde")]
    pub new_market_min_gap: Duration,
    /// How long a closed or resolved market stays subscribed.
    #[serde(with = "humantime_serde")]
    pub closed_grace_period: Duration,
    pub tokens_per_connection: usize,
    /// Backfill requests in flight at once (all on the `data_bulk` budget).
    pub backfill_concurrency: usize,
    /// Pages of 1000 rows per backfill batch before giving up (`complete = false`).
    pub backfill_max_pages: u32,
    #[serde(with = "humantime_serde")]
    pub reconnect_initial_backoff: Duration,
    #[serde(with = "humantime_serde")]
    pub reconnect_max_backoff: Duration,
    #[serde(with = "humantime_serde")]
    pub reconnect_stable_after: Duration,
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    /// The channel sends a ping this often.
    #[serde(with = "humantime_serde")]
    pub heartbeat_interval: Duration,
    /// The connection is dropped if nothing, not even a pong, arrives for this long.
    #[serde(with = "humantime_serde")]
    pub idle_timeout: Duration,
    /// After a decode error, tokens still without a `book` this long are resubscribed.
    #[serde(with = "humantime_serde")]
    pub resnapshot_after_decode_error: Duration,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            category_tags: vec!["politics".into(), "geopolitics".into()],
            min_liquidity_usd: Decimal::from(50_000),
            registry_interval: Duration::from_secs(60),
            new_market_min_gap: Duration::from_secs(30),
            closed_grace_period: Duration::from_hours(6),
            tokens_per_connection: 250,
            backfill_concurrency: 2,
            backfill_max_pages: 50,
            reconnect_initial_backoff: Duration::from_secs(1),
            reconnect_max_backoff: Duration::from_secs(30),
            reconnect_stable_after: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
            resnapshot_after_decode_error: Duration::from_secs(10),
        }
    }
}

impl IngestConfig {
    pub fn registry_config(&self) -> RegistryConfig {
        RegistryConfig {
            selection: SelectionRules {
                category_tags: self.category_tags.clone(),
                min_liquidity_usd: self.min_liquidity_usd,
            },
            registry_interval: self.registry_interval,
            closed_grace_period: self.closed_grace_period,
        }
    }
}

/// `[engine]`: state kept by the engine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EngineConfig {
    /// Book versions older than this (by receive time) leave the ring, except the newest.
    #[serde(with = "humantime_serde")]
    pub book_ring_window: Duration,
    /// Most book versions kept per token.
    pub book_ring_capacity: usize,
    /// Tokens untrusted this long while their connection is up raise an alarm.
    #[serde(with = "humantime_serde")]
    pub untrusted_alarm_after: Duration,
    /// Rolling volume window (§3.6).
    #[serde(with = "humantime_serde")]
    pub volume_window: Duration,
    /// Prints kept beyond the window for late queries.
    #[serde(with = "humantime_serde")]
    pub volume_slack: Duration,
    /// How often counters are written as metrics.
    #[serde(with = "humantime_serde")]
    pub metrics_interval: Duration,
    /// Copied from `[ingest]` at load time; the engine places tokens on slots.
    #[serde(skip)]
    pub tokens_per_connection: usize,
    /// Copied from `[ingest]` at load time.
    #[serde(skip)]
    pub resnapshot_after_decode_error: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        let ingest = IngestConfig::default();
        Self {
            book_ring_window: Duration::from_secs(5),
            book_ring_capacity: 256,
            untrusted_alarm_after: Duration::from_secs(120),
            volume_window: Duration::from_hours(24),
            volume_slack: Duration::from_secs(600),
            metrics_interval: Duration::from_secs(60),
            tokens_per_connection: ingest.tokens_per_connection,
            resnapshot_after_decode_error: ingest.resnapshot_after_decode_error,
        }
    }
}

/// Why the config could not be loaded.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("could not read {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("could not parse {path}: {source}")]
    Parse {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("invalid config: {0}")]
    Invalid(String),
}

impl Config {
    /// # Errors
    ///
    /// If the file cannot be read or parsed, or a value is invalid ([Self::validate]).
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.into(),
            source,
        })?;
        Self::from_toml_str(&text, path)
    }

    /// # Errors
    ///
    /// As [Self::load].
    pub fn from_toml_str(text: &str, path: &Path) -> Result<Self, ConfigError> {
        let mut config: Config = toml::from_str(text).map_err(|source| ConfigError::Parse {
            path: path.into(),
            source,
        })?;
        config.sync();
        config.validate()?;
        Ok(config)
    }

    /// Copies the `[ingest]` values the engine needs into `[engine]`.
    pub fn sync(&mut self) {
        self.engine.tokens_per_connection = self.ingest.tokens_per_connection;
        self.engine.resnapshot_after_decode_error = self.ingest.resnapshot_after_decode_error;
    }

    /// # Errors
    ///
    /// The first problem found.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |message: &str| Err(ConfigError::Invalid(message.into()));
        let ingest = &self.ingest;
        let engine = &self.engine;
        if let Err(violation) = self.http.validate() {
            return Err(ConfigError::Invalid(violation.to_string()));
        }
        if ingest.category_tags.is_empty() {
            return invalid("ingest.category_tags is empty");
        }
        if ingest.tokens_per_connection < 2 {
            return invalid("ingest.tokens_per_connection must hold a pair of tokens");
        }
        if ingest.backfill_concurrency == 0 || ingest.backfill_max_pages == 0 {
            return invalid("ingest.backfill_concurrency and backfill_max_pages are positive");
        }
        let positive = [
            ingest.registry_interval,
            ingest.reconnect_initial_backoff,
            ingest.connect_timeout,
            ingest.heartbeat_interval,
            ingest.resnapshot_after_decode_error,
            // Zero would turn every `new_market` hint (thousands an hour) into a Gamma poll.
            ingest.new_market_min_gap,
            ingest.reconnect_stable_after,
            engine.untrusted_alarm_after,
            engine.book_ring_window,
            engine.volume_window,
            engine.metrics_interval,
        ];
        if positive.iter().any(Duration::is_zero) {
            return invalid("durations must be positive");
        }
        if ingest.reconnect_initial_backoff > ingest.reconnect_max_backoff {
            return invalid("ingest.reconnect_initial_backoff exceeds reconnect_max_backoff");
        }
        if ingest.idle_timeout <= ingest.heartbeat_interval {
            return invalid("ingest.idle_timeout must exceed heartbeat_interval");
        }
        if engine.book_ring_capacity == 0 {
            return invalid("engine.book_ring_capacity is positive");
        }
        if !matches!(self.endpoints.ws.scheme(), "ws" | "wss") {
            return invalid("endpoints.ws must be a ws:// or wss:// URL");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Result<Config, ConfigError> {
        Config::from_toml_str(text, Path::new("test.toml"))
    }

    #[test]
    fn defaults_follow_the_spec() {
        let config = parse("").unwrap();
        assert_eq!(config.ingest.min_liquidity_usd, Decimal::from(50_000));
        assert_eq!(config.ingest.registry_interval, Duration::from_secs(60));
        assert_eq!(config.ingest.tokens_per_connection, 250);
        assert_eq!(config.ingest.reconnect_max_backoff, Duration::from_secs(30));
        assert_eq!(config.http.data_bulk_rps, 4);
        assert_eq!(config.engine.book_ring_capacity, 256);
        assert_eq!(config.engine.tokens_per_connection, 250);
    }

    #[test]
    fn values_and_unknown_keys() {
        let config = parse(
            "[ingest]\ntokens_per_connection = 100\nregistry_interval = \"2m\"\n\
             resnapshot_after_decode_error = \"3s\"\n[http]\ndata_bulk_rps = 2\n",
        )
        .unwrap();
        assert_eq!(config.ingest.registry_interval, Duration::from_secs(120));
        assert_eq!(config.engine.tokens_per_connection, 100);
        assert_eq!(
            config.engine.resnapshot_after_decode_error,
            Duration::from_secs(3)
        );
        assert_eq!(config.http.data_bulk_rps, 2);
        assert!(matches!(
            parse("[ingest]\ntoken_per_connection = 1\n"),
            Err(ConfigError::Parse { .. })
        ));
        assert!(matches!(
            parse("[ingest]\ntokens_per_connection = 1\n"),
            Err(ConfigError::Invalid(_))
        ));
        assert!(matches!(
            parse("[http]\ndata_bulk_rps = 9\n"),
            Err(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn the_example_config_loads() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/pmlive.example.toml");
        let config = Config::load(&path).unwrap();
        assert_eq!(config.ingest.category_tags, ["politics", "geopolitics"]);
    }
}
