use std::sync::Arc;

use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    cli::CommonArgs, client::MarcasiteClient, clock::SystemClock, config::Config,
    market_registry::registry_task::registry_task,
};

pub async fn execute(_args: CommonArgs, token: CancellationToken) -> anyhow::Result<()> {
    println!("Running Archiver...");

    let config = Arc::new(Config::load(&_args.config)?);
    let client = Arc::new(MarcasiteClient::new(config.clone()));
    let clock = Arc::new(SystemClock);

    let tracker = TaskTracker::new();
    let registry_token = token.child_token();

    tracker.spawn(registry_task(registry_token, client, config, clock));

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
