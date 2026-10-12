use std::{sync::Arc, time::Duration};

use tokio::{sync::mpsc, time::timeout};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    cli::CommonArgs,
    client::MarcasiteClient,
    clock::SystemClock,
    config::Config,
    market_registry::registry_task::registry_task,
    messages::{ConnectionManagerMsg, Record, RegistryMsg},
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
    let (write_tx, write_rx) = mpsc::channel::<Record>(10_000);

    tracker.spawn(registry_task(
        registry_rx,
        conn_mgr_tx.clone(),
        write_tx.clone(),
        registry_token,
        client,
        config,
        clock,
    ));

    // No more tasks to be added.
    tracker.close();

    // run until every task finishes on its own, or shutdown is requested
    // and the tasks then get a grace period to wind down
    let clean = tokio::select! {
        _ = tracker.wait() => true,     // all tasks ended naturally
        _ = token.cancelled() => {
            timeout(Duration::from_secs(10), tracker.wait()).await.is_ok()
        }
    };

    if !clean {
        tracing::warn!("tasks did not shut down within 10s");
    }

    // TODO: add the control message
    let _ = timeout(Duration::from_secs(1), write_tx.send(Record::Control)).await;
    drop(write_tx);

    // TODO: Spawn writer and use it here. Do not use tracker for this
    // if timeout(Duration::from_secs(4), writer_h).await.is_err() {
    //     tracing::error!("writer did not finish draining");
    // }

    // TODO: mark clean_stop = clean in the status file

    Ok(())
}
