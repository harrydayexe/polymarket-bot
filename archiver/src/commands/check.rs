use std::{path::Path, time::Duration};

use anyhow::{Context, Result};
use chrono::Utc;
use futures::StreamExt;
use marcasite::{
    types::TokenId,
    ws::{MarketChannel, MarketSubscription},
};

use crate::{
    archive::layout::{LOCK_FILE, STATE_DIR},
    cli::CommonArgs,
    client::{APIClient, MarcasiteClient},
    commands::Exit,
    config::Config,
    market_registry::selection::{parse_markets, select},
};

/// Most the system clock may differ from the exchange's.
const MAX_CLOCK_OFFSET: Duration = Duration::from_secs(2);
/// How long to wait for the first websocket message.
const FIRST_MESSAGE_TIMEOUT: Duration = Duration::from_secs(15);

/// Pre-flight checks (section 12.1). Run on the server itself: Polymarket's regional
/// restrictions depend on its address.
pub async fn execute(args: CommonArgs) -> Result<()> {
    let config = Config::load(&args.config)?;
    println!("PASS config: {}", args.config.display());

    let mut failed = 0;
    let mut report = |name: &str, result: Result<String>| match result {
        Ok(detail) => println!("PASS {name}: {detail}"),
        Err(e) => {
            failed += 1;
            println!("FAIL {name}: {e:#}");
        }
    };

    report("archive folder", writable(&config.archive_dir));
    report("free disk", free_disk(&config));
    if recorder_running(&config.archive_dir) {
        println!("note: an `archiver run` is active on this archive");
    }

    let token = match gamma(&config).await {
        Ok((detail, token)) => {
            report("gamma", Ok(detail));
            token
        }
        Err(e) => {
            report("gamma", Err(e));
            None
        }
    };
    report("clob and clock", clob_time(&config).await);
    report("websocket", websocket(&config, token).await);

    if failed > 0 {
        println!("{failed} check(s) failed");
        return Err(Exit(1).into());
    }
    println!("all checks passed");
    Ok(())
}

fn writable(root: &Path) -> Result<String> {
    let state = root.join(STATE_DIR);
    std::fs::create_dir_all(&state)
        .with_context(|| format!("could not create {}", state.display()))?;
    let probe = state.join(".check");
    std::fs::write(&probe, b"ok")
        .with_context(|| format!("could not write {}", probe.display()))?;
    std::fs::remove_file(&probe)?;
    Ok(format!("{} is writable", root.display()))
}

fn free_disk(config: &Config) -> Result<String> {
    let free = fs4::available_space(&config.archive_dir).context("could not read free space")?;
    let gb = free as f64 / 1e9;
    anyhow::ensure!(
        gb >= config.min_free_disk_gb as f64,
        "{gb:.1} GB free, below min_free_disk_gb ({} GB)",
        config.min_free_disk_gb
    );
    Ok(format!("{gb:.1} GB free"))
}

fn recorder_running(root: &Path) -> bool {
    std::fs::File::open(root.join(LOCK_FILE))
        .map(|f| matches!(f.try_lock(), Err(std::fs::TryLockError::WouldBlock)))
        .unwrap_or(false)
}

/// Lists the qualifying markets; returns a token to try the websocket with.
async fn gamma(config: &Config) -> Result<(String, Option<TokenId>)> {
    let client = MarcasiteClient::connect(config).await?;
    let markets = client.fetch_open_markets().await?;
    let (parsed, _) = parse_markets(markets, Utc::now());
    let selected: Vec<_> = parsed.iter().filter(|m| select(m, config)).collect();
    anyhow::ensure!(
        !selected.is_empty(),
        "reachable, but no market qualifies under the selection rules"
    );
    Ok((
        format!("{} markets qualify", selected.len()),
        Some(selected[0].tokens.yes.clone()),
    ))
}

/// Reaches the CLOB REST API and compares its clock with ours.
async fn clob_time(config: &Config) -> Result<String> {
    let url = format!("{}/time", config.clob_rest_url.trim_end_matches('/'));
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let before = Utc::now();
    let response = client.get(&url).send().await?.error_for_status()?;
    let text = response.text().await?;
    let after = Utc::now();
    let server: i64 = text
        .trim()
        .parse()
        .with_context(|| format!("unexpected /time response {text:?}"))?;
    // The server sends whole seconds: compare with our clock halfway through the request,
    // allowing for the rounding.
    let ours = before + (after - before) / 2;
    let offset = (ours.timestamp_millis() - server * 1000 - 500).abs();
    anyhow::ensure!(
        offset <= MAX_CLOCK_OFFSET.as_millis() as i64 + 500,
        "system clock is about {:.1} s off the exchange's; check time sync (NTP)",
        offset as f64 / 1000.0
    );
    Ok(format!(
        "reachable, clock within {:.1} s",
        offset as f64 / 1000.0
    ))
}

async fn websocket(config: &Config, token: Option<TokenId>) -> Result<String> {
    let token = token.context("not checked: no token from Gamma to subscribe to")?;
    let mut channel = MarketChannel::builder()
        .url(config.websocket_url.as_str())
        .connect_timeout(config.connect_timeout)
        .heartbeat_interval(config.heartbeat_interval)
        .idle_timeout(config.heartbeat_timeout)
        .connect(MarketSubscription::new([token]).custom_feature_enabled(true))
        .await
        .context("could not connect")?;
    let first = tokio::time::timeout(FIRST_MESSAGE_TIMEOUT, channel.next()).await;
    channel.close();
    match first {
        Ok(Some(Ok(_))) => Ok("connected and received a message".to_string()),
        Ok(Some(Err(e))) => Err(e).context("connected, but the stream failed"),
        Ok(None) => anyhow::bail!("connected, but the server closed the stream"),
        Err(_) => anyhow::bail!(
            "connected, but no message within {}s",
            FIRST_MESSAGE_TIMEOUT.as_secs()
        ),
    }
}
