use std::{collections::HashSet, time::Duration};

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Args;
use marcasite::types::TokenId;

use crate::{
    archive::layout::TRACKED_TOKENS_FILE,
    cli::CommonArgs,
    commands::Exit,
    config::Config,
    messages::{ControlBody, Record, SnapshotReason},
    snapshot_worker::books_client::{BooksClient, BooksHttpError, ClobBooksClient},
    status::status_file::{TrackedTokensFile, read_json},
    types::{RunId, SystemRandom},
    writer::writer::Writer,
};

/// Attempts per batch, waiting 1, 2 and 4 s (or the server's `Retry-After`) in between.
const ATTEMPTS: u32 = 4;

#[derive(Args)]
#[group(id = "which", required = true, multiple = false, args = ["all", "tokens"])]
pub struct SnapshotArgs {
    #[command(flatten)]
    pub common: CommonArgs,

    /// Snapshot every token the recorder tracks (from state/tracked_tokens.json)
    #[arg(short, long)]
    all: bool,

    /// Snapshot these tokens
    #[arg(short, long, value_delimiter = ',', num_args = 1..)]
    tokens: Option<Vec<String>>,
}

/// Takes REST snapshots now and writes them to the archive as a run of their own, next to a
/// running recorder if there is one.
pub async fn execute(args: SnapshotArgs) -> Result<()> {
    let config = Config::load(&args.common.config)?;
    let root = &config.archive_dir;
    let tokens: Vec<TokenId> = if args.all {
        let path = root.join(TRACKED_TOKENS_FILE);
        read_json::<TrackedTokensFile>(&path)?
            .with_context(|| format!("no {}: has the recorder run here?", path.display()))?
            .tokens
    } else {
        let mut seen = HashSet::new();
        args.tokens
            .unwrap_or_default()
            .into_iter()
            .filter(|t| seen.insert(t.clone()))
            .map(TokenId::new)
            .collect()
    };
    anyhow::ensure!(!tokens.is_empty(), "no tokens to snapshot");

    let client = ClobBooksClient::new(&config)?;
    let run_id = RunId::create(Utc::now(), &mut SystemRandom);
    let mut writer = Writer::new(root, run_id.clone());
    let pause = Duration::from_secs_f64(1.0 / config.snapshot_max_requests_per_s as f64);

    let (mut books_written, mut failed) = (0, 0);
    for (i, batch) in tokens.chunks(config.snapshot_batch_size).enumerate() {
        if i > 0 {
            tokio::time::sleep(pause).await;
        }
        match fetch(&client, batch).await {
            Ok(books) => {
                books_written += books.len();
                if books.len() < batch.len() {
                    println!(
                        "warning: no book for {} of {} tokens",
                        batch.len() - books.len(),
                        batch.len()
                    );
                }
                writer.write(&Record::Snapshot {
                    recv_ts: Utc::now(),
                    requested_tokens: batch.to_vec(),
                    reason: SnapshotReason::Manual,
                    books,
                })?;
            }
            Err(e) => {
                failed += batch.len();
                println!("failed for {} tokens: {e:#}", batch.len());
                writer.write(&Record::Control {
                    recv_ts: Utc::now(),
                    conn_id: None,
                    body: ControlBody::SnapshotFailed {
                        tokens: batch.iter().cloned().collect(),
                        error: format!("{e:#}"),
                        attempts: ATTEMPTS,
                    },
                })?;
            }
        }
    }
    writer
        .close_all()
        .context("could not close the snapshot files")?;

    println!(
        "{books_written} books for {} tokens written as run {run_id}",
        tokens.len()
    );
    if failed > 0 {
        println!("{failed} tokens failed");
        return Err(Exit(1).into());
    }
    Ok(())
}

async fn fetch(client: &ClobBooksClient, batch: &[TokenId]) -> Result<Vec<serde_json::Value>> {
    let mut attempt = 1;
    loop {
        match client.fetch_books(batch).await {
            Ok(books) => return Ok(books),
            Err(e) if attempt < ATTEMPTS => {
                let wait = Duration::from_secs(1 << (attempt - 1)).max(
                    e.downcast_ref::<BooksHttpError>()
                        .and_then(|e| e.retry_after)
                        .unwrap_or_default(),
                );
                eprintln!("attempt {attempt} failed, retrying in {wait:?}: {e:#}");
                tokio::time::sleep(wait).await;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}
