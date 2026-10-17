use std::{sync::Arc, time::Duration};

use futures::FutureExt;
use tokio::{sync::mpsc, time::timeout};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    cli::CommonArgs,
    client::MarcasiteClient,
    clock::{Clock, SystemClock},
    config::Config,
    market_registry::registry_task::registry_task,
    messages::{ConnectionManagerMsg, ControlBody, Record, RegistryMsg},
};

pub async fn execute(_args: CommonArgs, token: CancellationToken) -> anyhow::Result<()> {
    println!("Running Archiver...");

    let config = Arc::new(Config::load(&_args.config)?);
    let client = Arc::new(MarcasiteClient::connect(&config).await?);
    let clock = Arc::new(SystemClock);

    let tracker = TaskTracker::new();
    let registry_token = token.child_token();

    let (registry_tx, registry_rx) = mpsc::unbounded_channel::<RegistryMsg>();
    let (conn_mgr_tx, conn_mgr_rx) = mpsc::channel::<ConnectionManagerMsg>(256);
    let (write_tx, write_rx) = mpsc::channel::<Record>(10_000);

    let registry = tracker.spawn(supervise(
        "registry",
        token.clone(),
        registry_task(
            registry_rx,
            conn_mgr_tx.clone(),
            write_tx.clone(),
            registry_token,
            client,
            config,
            clock.clone(),
        ),
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

    // A task that is unfinished, panicked or returned an error makes the stop unclean.
    let failed = !matches!(registry.now_or_never(), Some(Ok(true)));
    let clean = clean && !failed;

    // TODO: add the control message
    let _ = timeout(
        Duration::from_secs(1),
        write_tx.send(Record::Control {
            recv_ts: clock.now(),
            conn_id: None,
            body: ControlBody::RunStop { clean },
        }),
    )
    .await;
    drop(write_tx);

    // TODO: Spawn writer and use it here. Do not use tracker for this
    // if timeout(Duration::from_secs(4), writer_h).await.is_err() {
    //     tracing::error!("writer did not finish draining");
    // }

    // TODO: mark clean_stop = clean in the status file

    if failed {
        anyhow::bail!("a task failed, see the log for details");
    }

    Ok(())
}

/// Run a task to completion. On error, log it and cancel `shutdown` so the rest of the
/// process stops too.
///
/// # Returns
///
/// `true` if the task finished without error.
async fn supervise(
    name: &'static str,
    shutdown: CancellationToken,
    task: impl Future<Output = anyhow::Result<()>>,
) -> bool {
    match task.await {
        Ok(()) => true,
        Err(e) => {
            tracing::error!(task = name, error = format!("{e:#}"), "task failed");
            shutdown.cancel();
            false
        }
    }
}
