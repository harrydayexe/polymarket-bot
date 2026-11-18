//! `pmlive`: the live paper-trading system's command line.

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use clap::{Parser, Subcommand, ValueEnum};
use pm_core::{
    client::Clients,
    clock::{SharedClock, SystemClock},
    fees::FeeTable,
    market::parse_markets,
    random::{RandomSource, SystemRandom},
};
use pm_live::{
    config::Config,
    ingest::registry::{GammaSource, MarketSource},
    runtime::{self, Sources},
    sink::JsonlSink,
};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(version, about = "Live paper-trading system for Polymarket")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Runs until stopped (SIGINT or SIGTERM).
    Run {
        #[arg(short, long, default_value = "./pmlive.toml")]
        config: PathBuf,
        #[arg(long, value_enum, default_value_t = Mode::Shadow)]
        mode: Mode,
    },
    /// Prints how many markets the registry would select right now.
    Markets {
        #[arg(short, long, default_value = "./pmlive.toml")]
        config: PathBuf,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum Mode {
    /// Detectors, scoring, marks and strategy intents logged; no paper fills.
    Shadow,
    /// Shadow plus the paper broker (needs C7).
    Paper,
}

fn load(path: &std::path::Path) -> Result<(Config, Clients)> {
    let config = Config::load(path).with_context(|| format!("loading {}", path.display()))?;
    let clients =
        Clients::build(&config.endpoints, &config.http).context("building HTTP clients")?;
    Ok((config, clients))
}

async fn shutdown_signal(token: CancellationToken) {
    let mut term = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(term) => term,
        Err(error) => {
            tracing::error!(%error, "cannot listen for SIGTERM");
            return;
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
    tracing::info!("shutdown requested");
    token.cancel();
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    match Cli::parse().command {
        Command::Markets { config } => {
            let (config, clients) = load(&config)?;
            let source = GammaSource::connect(clients.meta.gamma().clone(), &config.ingest).await?;
            let (markets, skipped) = parse_markets(source.open_markets().await?, Utc::now());
            let rules = config.ingest.registry_config().selection;
            let selected = markets
                .iter()
                .filter(|m| pm_core::market::qualifies(m, &rules))
                .count();
            let with_fees = markets.iter().filter(|m| m.fee_params.is_some()).count();
            println!(
                "listed {} (skipped {}), selected {selected}, with a Gamma fee schedule {with_fees}",
                markets.len(),
                skipped.len()
            );
            Ok(())
        }
        Command::Run { config, mode } => {
            if matches!(mode, Mode::Paper) {
                bail!("paper mode needs the paper broker (C7), which is not built yet");
            }
            let (config, clients) = load(&config)?;
            let table = FeeTable::load(&config.run.fee_table).with_context(|| {
                format!("loading the fee table {}", config.run.fee_table.display())
            })?;
            let clock: SharedClock = Arc::new(SystemClock);
            let run_id = format!(
                "r-{}-{}",
                clock.now().format("%Y%m%d-%H%M"),
                SystemRandom.random_id(4)
            );
            let sink = JsonlSink::create(&config.run.data_dir, &run_id)
                .context("creating the records file")?;
            tracing::info!(run_id, records = %sink.path().display(), "starting in shadow mode");
            let markets =
                GammaSource::connect(clients.meta.gamma().clone(), &config.ingest).await?;
            let sources = Sources {
                markets: Arc::new(markets),
                clob: Arc::new(clients.meta.clob().clone()),
                data_bulk: clients.data_bulk.clone(),
            };
            let token = CancellationToken::new();
            tokio::spawn(shutdown_signal(token.clone()));
            let outcome = runtime::run(
                Arc::new(config),
                &run_id,
                clock,
                sources,
                table,
                Box::new(sink),
                token,
            )
            .await?;
            tracing::info!(
                files = outcome.log_files.len(),
                envelopes = outcome.engine.seq(),
                "stopped cleanly"
            );
            Ok(())
        }
    }
}
