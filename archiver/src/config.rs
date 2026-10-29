use humantime::format_duration;
use miette::{Diagnostic, NamedSource, SourceSpan};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use thiserror::Error;
use url::Url;

macro_rules! ensure {
    ($cond:expr, $($arg:tt)+) => {
        if !$cond {
            return Err(ConfigError::Validation(format!($($arg)+)));
        }
    };
}

macro_rules! bail {
    ($($arg:tt)+) => {
        return Err(ConfigError::Validation(format!($($arg)+)))
    };
}

#[derive(Error, Diagnostic, Debug)]
pub enum ConfigError {
    #[error("could not read config file {path}")]
    #[diagnostic(code(archiver::config::io))]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("invalid config file")]
    #[diagnostic(code(archiver::config::parse))]
    Parse {
        #[source_code]
        src: NamedSource<String>,
        #[label("{message}")]
        span: Option<SourceSpan>,
        message: String,
    },

    #[error("invalid config: {0}")]
    #[diagnostic(code(archiver::config::validation))]
    Validation(String),
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub archive_dir: PathBuf,
    pub category_tags: Vec<String>,
    pub min_liquidity_usd: Decimal,
    #[serde(with = "humantime_serde")]
    pub registry_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub closed_grace_period: Duration,
    pub tokens_per_connection: usize,
    #[serde(with = "humantime_serde")]
    pub heartbeat_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub heartbeat_timeout: Duration,
    /// How long a connection may take to open its socket, and then to send its subscribe.
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub backoff_initial: Duration,
    #[serde(with = "humantime_serde")]
    pub backoff_max: Duration,
    /// How long a connection must stream before its backoff sequence starts again from
    /// `backoff_initial`.
    #[serde(with = "humantime_serde")]
    pub backoff_reset_after: Duration,
    /// How long a subscribed token may go without a full book before a REST snapshot is
    /// requested for it, and that snapshot may take before the token is marked unhealthy.
    #[serde(with = "humantime_serde")]
    pub missing_book_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub snapshot_interval: Duration,
    pub snapshot_batch_size: usize,
    pub snapshot_max_requests_per_s: u64,
    pub write_queue_capacity: usize,
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub sync_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub raw_retention: Duration,
    pub min_free_disk_gb: u64,
    pub websocket_url: url::Url,
    pub clob_rest_url: String,
    pub gamma_url: String,
    pub log_level: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            archive_dir: "./archive".into(),
            category_tags: vec!["politics".into(), "geopolitics".into()],
            min_liquidity_usd: dec!(1000),
            registry_interval: Duration::from_secs(300),
            closed_grace_period: Duration::from_hours(6),
            tokens_per_connection: 250,
            heartbeat_interval: Duration::from_secs(10),
            heartbeat_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            backoff_initial: Duration::from_secs(1),
            backoff_max: Duration::from_secs(60),
            backoff_reset_after: Duration::from_mins(5),
            missing_book_timeout: Duration::from_mins(2),
            snapshot_interval: Duration::from_mins(60),
            snapshot_batch_size: 500,
            snapshot_max_requests_per_s: 2,
            write_queue_capacity: 100_000,
            flush_interval: Duration::from_secs(1),
            sync_interval: Duration::from_secs(5),
            raw_retention: Duration::from_hours(90 * 24),
            min_free_disk_gb: 10,
            websocket_url: Url::parse("wss://ws-subscriptions-clob.polymarket.com/ws/market")
                .unwrap(),
            clob_rest_url: "https://clob.polymarket.com".into(),
            gamma_url: "https://gamma-api.polymarket.com".into(),
            log_level: "info".into(),
        }
    }
}

impl Config {
    /// A short fingerprint of the settings, recorded in `run_start` so later analysis can tell
    /// whether they changed between runs. Stable for a given program version.
    pub fn fingerprint(&self) -> String {
        // FNV-1a over the debug form: every setting, in declaration order.
        let hash = format!("{self:?}")
            .bytes()
            .fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
                (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
            });
        format!("{hash:016x}")
    }

    /// Reads, parses and validates the config file at `path`.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;

        Self::from_toml_str(text, path)
    }

    /// Parses and validates config from TOML text. Missing keys take their defaults.
    pub fn from_toml_str(text: String, path: &Path) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(&text).map_err(|e| ConfigError::Parse {
            span: e.span().map(|r| (r.start, r.len()).into()),
            message: e.message().to_string(),
            src: NamedSource::new(path.display().to_string(), text),
        })?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let non_zero = [
            ("tokens_per_connection", self.tokens_per_connection as u64),
            ("snapshot_batch_size", self.snapshot_batch_size as u64),
            (
                "snapshot_max_requests_per_s",
                self.snapshot_max_requests_per_s,
            ),
            ("write_queue_capacity", self.write_queue_capacity as u64),
        ];
        for (key, value) in non_zero {
            ensure!(value > 0, "{key} must be greater than 0");
        }
        let non_zero_durations = [
            ("registry_interval", self.registry_interval),
            ("heartbeat_interval", self.heartbeat_interval),
            ("connect_timeout", self.connect_timeout),
            ("backoff_initial", self.backoff_initial),
            ("missing_book_timeout", self.missing_book_timeout),
            ("snapshot_interval", self.snapshot_interval),
            ("flush_interval", self.flush_interval),
            ("sync_interval", self.sync_interval),
        ];
        for (key, value) in non_zero_durations {
            ensure!(!value.is_zero(), "{key} must be greater than 0");
        }

        ensure!(
            self.min_liquidity_usd >= Decimal::ZERO,
            "min_liquidity_usd must be a non-negative number, got {}",
            self.min_liquidity_usd
        );
        ensure!(
            self.heartbeat_timeout > self.heartbeat_interval,
            "heartbeat_timeout ({}) must be longer than heartbeat_interval ({})",
            format_duration(self.heartbeat_timeout),
            format_duration(self.heartbeat_interval)
        );
        ensure!(
            self.backoff_initial <= self.backoff_max,
            "backoff_initial ({}) must not exceed backoff_max ({})",
            format_duration(self.backoff_initial),
            format_duration(self.backoff_max)
        );
        ensure!(
            self.tokens_per_connection >= 2,
            "tokens_per_connection ({}) must be at least 2, so both tokens of a market fit on one connection",
            self.tokens_per_connection
        );
        ensure!(
            self.snapshot_batch_size <= 500,
            "snapshot_batch_size ({}) must not exceed 500, the maximum the endpoint accepts",
            self.snapshot_batch_size
        );
        ensure!(
            !self.category_tags.is_empty(),
            "category_tags must not be empty"
        );
        ensure!(
            self.websocket_url.scheme() == "ws" || self.websocket_url.scheme() == "wss",
            "websocket_url must start with ws:// or wss://"
        );
        for (key, url) in [
            ("clob_rest_url", &self.clob_rest_url),
            ("gamma_url", &self.gamma_url),
        ] {
            ensure!(
                url.starts_with("http://") || url.starts_with("https://"),
                "{key} must start with http:// or https://"
            );
        }
        if !["trace", "debug", "info", "warn", "error"].contains(&self.log_level.as_str()) {
            bail!(
                "log_level must be one of trace, debug, info, warn, error; got {:?}",
                self.log_level
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Result<Config, ConfigError> {
        Config::from_toml_str(text.to_string(), Path::new("config.toml"))
    }

    /// Returns the message of a validation error, panicking on any other outcome.
    fn validation_err(text: &str) -> String {
        match parse(text) {
            Err(ConfigError::Validation(msg)) => msg,
            other => panic!("expected validation error, got {other:?}"),
        }
    }

    /// Returns the message of a parse error and the source text its label points at,
    /// panicking on any other outcome.
    fn parse_err(text: &str) -> (String, &str) {
        match parse(text) {
            Err(ConfigError::Parse {
                message,
                span: Some(span),
                src,
            }) => {
                assert_eq!(src.name(), "config.toml");
                (message, &text[span.offset()..span.offset() + span.len()])
            }
            other => panic!("expected parse error with a span, got {other:?}"),
        }
    }

    #[test]
    fn empty_file_gives_defaults() {
        let config = parse("").unwrap();
        let default = Config::default();
        assert_eq!(format!("{config:?}"), format!("{default:?}"));
        assert_eq!(config.tokens_per_connection, 250);
        assert_eq!(config.heartbeat_timeout, Duration::from_secs(30));
    }

    #[test]
    fn shipped_config_file_is_valid() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.toml");
        Config::load(&path).unwrap();
    }

    #[test]
    fn file_value_overrides_default() {
        let config = parse("tokens_per_connection = 100").unwrap();
        assert_eq!(config.tokens_per_connection, 100);
    }

    #[test]
    fn duration_value_is_parsed() {
        let config = parse("closed_grace_period = \"6h 30m\"").unwrap();
        assert_eq!(config.closed_grace_period, Duration::from_mins(390));
    }

    #[test]
    fn negative_interval_is_refused() {
        let (_, spanned) = parse_err("heartbeat_interval = \"-5s\"");
        assert_eq!(spanned, "\"-5s\"");
    }

    #[test]
    fn unitless_duration_is_refused() {
        let (_, spanned) = parse_err("heartbeat_interval = 5");
        assert_eq!(spanned, "5");
    }

    #[test]
    fn zero_interval_is_refused() {
        let msg = validation_err("registry_interval = \"0s\"");
        assert!(
            msg.contains("registry_interval must be greater than 0"),
            "{msg}"
        );
    }

    #[test]
    fn heartbeat_timeout_shorter_than_interval_is_refused() {
        let msg = validation_err("heartbeat_interval = \"30s\"\nheartbeat_timeout = \"10s\"");
        assert!(
            msg.contains("heartbeat_timeout (10s) must be longer"),
            "{msg}"
        );
    }

    #[test]
    fn heartbeat_timeout_equal_to_interval_is_refused() {
        let msg = validation_err("heartbeat_interval = \"10s\"\nheartbeat_timeout = \"10s\"");
        assert!(msg.contains("heartbeat_timeout"), "{msg}");
    }

    #[test]
    fn backoff_initial_above_max_is_refused() {
        let msg = validation_err("backoff_initial = \"2m\"");
        assert!(
            msg.contains("backoff_initial (2m) must not exceed"),
            "{msg}"
        );
    }

    #[test]
    fn connection_too_small_for_a_market_is_refused() {
        let msg = validation_err("tokens_per_connection = 1");
        assert!(
            msg.contains("tokens_per_connection (1) must be at least 2"),
            "{msg}"
        );
    }

    #[test]
    fn oversized_snapshot_batch_is_refused() {
        let msg = validation_err("snapshot_batch_size = 501");
        assert!(msg.contains("snapshot_batch_size"), "{msg}");
    }

    #[test]
    fn negative_liquidity_is_refused() {
        let msg = validation_err("min_liquidity_usd = -1.0");
        assert!(msg.contains("min_liquidity_usd"), "{msg}");
    }

    #[test]
    fn bad_log_level_is_refused() {
        let msg = validation_err("log_level = \"loud\"");
        assert!(msg.contains("log_level"), "{msg}");
    }

    #[test]
    fn bad_url_scheme_is_refused() {
        let msg = validation_err("websocket_url = \"https://example.com\"");
        assert!(msg.contains("websocket_url"), "{msg}");
    }

    #[test]
    fn unknown_key_is_refused_by_name() {
        let (msg, spanned) = parse_err("tokens_per_conection = 100");
        assert!(msg.contains("tokens_per_conection"), "{msg}");
        assert_eq!(spanned, "tokens_per_conection");
    }

    #[test]
    fn wrong_type_is_refused() {
        let (_, spanned) = parse_err("tokens_per_connection = \"lots\"");
        assert_eq!(spanned, "\"lots\"");
    }

    #[test]
    fn missing_file_is_refused() {
        let path = Path::new("/nonexistent/config.toml");
        match Config::load(path) {
            Err(ConfigError::Io {
                path: err_path,
                source,
            }) => {
                assert_eq!(err_path, path);
                assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
            }
            other => panic!("expected io error, got {other:?}"),
        }
    }
}
