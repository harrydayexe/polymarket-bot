use anyhow::{Context, bail, ensure};
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub archive_dir: PathBuf,
    pub category_tags: Vec<String>,
    pub min_liquidity_usd: f64,
    pub registry_interval_s: u64,
    pub closed_grace_period_h: u64,
    pub tokens_per_connection: usize,
    pub heartbeat_interval_s: u64,
    pub heartbeat_timeout_s: u64,
    pub backoff_initial_s: u64,
    pub backoff_max_s: u64,
    pub snapshot_interval_min: u64,
    pub snapshot_batch_size: usize,
    pub snapshot_max_requests_per_s: u64,
    pub write_queue_capacity: usize,
    pub flush_interval_s: u64,
    pub sync_interval_s: u64,
    pub raw_retention_days: u32,
    pub min_free_disk_gb: u64,
    pub websocket_url: String,
    pub clob_rest_url: String,
    pub gamma_url: String,
    pub log_level: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            archive_dir: "./archive".into(),
            category_tags: vec!["politics".into(), "geopolitics".into()],
            min_liquidity_usd: 1_000.0,
            registry_interval_s: 60,
            closed_grace_period_h: 6,
            tokens_per_connection: 250,
            heartbeat_interval_s: 10,
            heartbeat_timeout_s: 30,
            backoff_initial_s: 1,
            backoff_max_s: 60,
            snapshot_interval_min: 60,
            snapshot_batch_size: 500,
            snapshot_max_requests_per_s: 2,
            write_queue_capacity: 100_000,
            flush_interval_s: 1,
            sync_interval_s: 5,
            raw_retention_days: 90,
            min_free_disk_gb: 10,
            websocket_url: "wss://ws-subscriptions-clob.polymarket.com/ws/market".into(),
            clob_rest_url: "https://clob.polymarket.com".into(),
            gamma_url: "https://gamma-api.polymarket.com".into(),
            log_level: "info".into(),
        }
    }
}

impl Config {
    /// Reads, parses and validates the config file at `path`.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file {}", path.display()))?;
        Self::from_toml_str(&text)
            .with_context(|| format!("invalid config file {}", path.display()))
    }

    /// Parses and validates config from TOML text. Missing keys take their defaults.
    pub fn from_toml_str(text: &str) -> anyhow::Result<Self> {
        let config: Self = toml::from_str(text)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> anyhow::Result<()> {
        let non_zero = [
            ("registry_interval_s", self.registry_interval_s),
            ("tokens_per_connection", self.tokens_per_connection as u64),
            ("heartbeat_interval_s", self.heartbeat_interval_s),
            ("heartbeat_timeout_s", self.heartbeat_timeout_s),
            ("backoff_initial_s", self.backoff_initial_s),
            ("backoff_max_s", self.backoff_max_s),
            ("snapshot_interval_min", self.snapshot_interval_min),
            ("snapshot_batch_size", self.snapshot_batch_size as u64),
            (
                "snapshot_max_requests_per_s",
                self.snapshot_max_requests_per_s.into(),
            ),
            ("write_queue_capacity", self.write_queue_capacity as u64),
            ("flush_interval_s", self.flush_interval_s),
            ("sync_interval_s", self.sync_interval_s),
        ];
        for (key, value) in non_zero {
            ensure!(value > 0, "{key} must be greater than 0");
        }

        ensure!(
            self.min_liquidity_usd.is_finite() && self.min_liquidity_usd >= 0.0,
            "min_liquidity_usd must be a non-negative number, got {}",
            self.min_liquidity_usd
        );
        ensure!(
            self.heartbeat_timeout_s > self.heartbeat_interval_s,
            "heartbeat_timeout_s ({}) must be longer than heartbeat_interval_s ({})",
            self.heartbeat_timeout_s,
            self.heartbeat_interval_s
        );
        ensure!(
            self.backoff_initial_s <= self.backoff_max_s,
            "backoff_initial_s ({}) must not exceed backoff_max_s ({})",
            self.backoff_initial_s,
            self.backoff_max_s
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
            self.websocket_url.starts_with("ws://") || self.websocket_url.starts_with("wss://"),
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

    fn err(text: &str) -> String {
        format!("{:#}", Config::from_toml_str(text).unwrap_err())
    }

    #[test]
    fn empty_file_gives_defaults() {
        let config = Config::from_toml_str("").unwrap();
        let default = Config::default();
        assert_eq!(format!("{config:?}"), format!("{default:?}"));
        assert_eq!(config.tokens_per_connection, 250);
        assert_eq!(config.heartbeat_timeout_s, 30);
    }

    #[test]
    fn shipped_config_file_is_valid() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.toml");
        Config::load(&path).unwrap();
    }

    #[test]
    fn file_value_overrides_default() {
        let config = Config::from_toml_str("tokens_per_connection = 100").unwrap();
        assert_eq!(config.tokens_per_connection, 100);
    }

    #[test]
    fn negative_interval_is_refused() {
        let msg = err("heartbeat_interval_s = -5");
        assert!(msg.contains("heartbeat_interval_s"), "{msg}");
    }

    #[test]
    fn zero_interval_is_refused() {
        let msg = err("registry_interval_s = 0");
        assert!(
            msg.contains("registry_interval_s must be greater than 0"),
            "{msg}"
        );
    }

    #[test]
    fn heartbeat_timeout_shorter_than_interval_is_refused() {
        let msg = err("heartbeat_interval_s = 30\nheartbeat_timeout_s = 10");
        assert!(
            msg.contains("heartbeat_timeout_s (10) must be longer"),
            "{msg}"
        );
    }

    #[test]
    fn heartbeat_timeout_equal_to_interval_is_refused() {
        let msg = err("heartbeat_interval_s = 10\nheartbeat_timeout_s = 10");
        assert!(msg.contains("heartbeat_timeout_s"), "{msg}");
    }

    #[test]
    fn backoff_initial_above_max_is_refused() {
        let msg = err("backoff_initial_s = 120");
        assert!(
            msg.contains("backoff_initial_s (120) must not exceed"),
            "{msg}"
        );
    }

    #[test]
    fn oversized_snapshot_batch_is_refused() {
        let msg = err("snapshot_batch_size = 501");
        assert!(msg.contains("snapshot_batch_size"), "{msg}");
    }

    #[test]
    fn negative_liquidity_is_refused() {
        let msg = err("min_liquidity_usd = -1.0");
        assert!(msg.contains("min_liquidity_usd"), "{msg}");
    }

    #[test]
    fn bad_log_level_is_refused() {
        let msg = err("log_level = \"loud\"");
        assert!(msg.contains("log_level"), "{msg}");
    }

    #[test]
    fn bad_url_scheme_is_refused() {
        let msg = err("websocket_url = \"https://example.com\"");
        assert!(msg.contains("websocket_url"), "{msg}");
    }

    #[test]
    fn unknown_key_is_refused_by_name() {
        let msg = err("tokens_per_conection = 100");
        assert!(msg.contains("tokens_per_conection"), "{msg}");
    }

    #[test]
    fn wrong_type_is_refused() {
        let msg = err("tokens_per_connection = \"lots\"");
        assert!(msg.contains("tokens_per_connection"), "{msg}");
    }

    #[test]
    fn missing_file_is_refused() {
        let msg = format!(
            "{:#}",
            Config::load(Path::new("/nonexistent/config.toml")).unwrap_err()
        );
        assert!(msg.contains("/nonexistent/config.toml"), "{msg}");
    }
}
