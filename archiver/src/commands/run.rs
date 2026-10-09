use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    cli::CommonArgs,
    client::MarcasiteClient,
    clock::SystemClock,
    config::Config,
    market_registry::registry_task::registry_task,
    messages::{ConnectionManagerMsg, RegistryMsg},
};

pub async fn execute(_args: CommonArgs, token: CancellationToken) -> anyhow::Result<()> {
    println!("Running Archiver...");

    let config = Arc::new(Config::load(&_args.config)?);
    let client = Arc::new(MarcasiteClient::new(config.clone()));
    let clock = Arc::new(SystemClock);

    let tracker = TaskTracker::new();
    let registry_token = token.child_token();

    let (registry_tx, registry_rx) = mpsc::channel::<RegistryMsg>(64);
    let (conn_mgr_tx, conn_mgr_rx) = mpsc::channel::<ConnectionManagerMsg>(256);
    // let (write_tx, write_rx)       = mpsc::channel::<WriteMsg>(10_000);

    tracker.spawn(registry_task(
        registry_rx,
        conn_mgr_tx,
        registry_token,
        client,
        config,
        clock,
    ));

    // No more tasks to be added.
    tracker.close();

    // run until every task finishes on its own, or shutdown is requested
    // and the tasks then get a grace period to wind down
    tokio::select! {
        _ = tracker.wait() => {}     // all tasks ended naturally
        _ = token.cancelled() => {
            if tokio::time::timeout(std::time::Duration::from_secs(10), tracker.wait()).await.is_err() {
                tracing::warn!("tasks did not shut down within 10s");
            }
        }
    }

    Ok(())
}
