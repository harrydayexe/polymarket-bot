//! Wiring: the I/O tasks, the engine loop and the executor of the engine's actions.
//!
//! ```text
//! slots (one ReconnectingMarketChannel each) ─┐
//! registry task (Gamma, CLOB fees)  ──────────┼─> events ─> engine loop ─> engine log
//! backfill task (Data API, data_bulk) ────────┤               │ step()
//! ticker (100 ms) ────────────────────────────┘               v
//!                                                         executor: slots, hints, backfill
//!                                                         requests, record sink
//! ```
//!
//! A task that fails cancels the run; the process exits non-zero and its supervisor restarts it.

use std::{
    collections::BTreeMap,
    future::Future,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use futures_util::FutureExt;
use marcasite::{data::DataClient, ws::ReconnectingMarketChannelHandle};
use pm_core::{archive::record::ConnSlotId, clock::SharedClock, fees::FeeTable};
use tokio::{
    sync::mpsc,
    time::{self, MissedTickBehavior},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    action::{Action, RegistryHint},
    config::Config,
    engine::Engine,
    engine_log::EngineLogWriter,
    event::{Event, Stamped},
    ingest::{
        backfill::{BackfillRequest, backfill_task},
        registry::{ClobFees, MarketSource, RegistryState, registry_task},
        ws::{open_channel, run_slot},
    },
    sink::RecordSink,
};

/// Envelopes waiting for the engine; senders wait when it is full.
pub const EVENT_QUEUE: usize = 65_536;
/// How often the engine sends a `Tick`.
pub const TICK_INTERVAL: Duration = Duration::from_millis(100);
/// How long tasks get to stop after cancellation.
pub const SHUTDOWN_LIMIT: Duration = Duration::from_secs(10);
const FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const SUMMARY_INTERVAL: Duration = Duration::from_secs(60);

/// The outside services a run reads.
pub struct Sources {
    pub markets: Arc<dyn MarketSource>,
    pub clob: Arc<dyn ClobFees>,
    /// The Data API client on the `data_bulk` budget.
    pub data_bulk: DataClient,
}

/// What a run leaves behind.
pub struct RunOutcome {
    pub engine: Engine,
    pub log_files: Vec<PathBuf>,
}

/// Runs `task` and cancels the run if it fails or panics. Returns whether it finished cleanly.
async fn supervise(
    name: &'static str,
    token: CancellationToken,
    failed: Arc<AtomicBool>,
    task: impl Future<Output = Result<()>>,
) {
    let outcome = std::panic::AssertUnwindSafe(task).catch_unwind().await;
    let error = match outcome {
        Ok(Ok(())) => return,
        Ok(Err(error)) => format!("{error:#}"),
        Err(_) => "panicked".to_string(),
    };
    tracing::error!(task = name, %error, "task failed; stopping the run");
    failed.store(true, Ordering::SeqCst);
    token.cancel();
}

/// Carries out the engine's actions.
struct Executor {
    config: Arc<Config>,
    clock: SharedClock,
    events: mpsc::Sender<Stamped>,
    slots: BTreeMap<ConnSlotId, ReconnectingMarketChannelHandle>,
    tracker: TaskTracker,
    token: CancellationToken,
    failed: Arc<AtomicBool>,
    hints: mpsc::UnboundedSender<RegistryHint>,
    backfill: mpsc::UnboundedSender<BackfillRequest>,
    sink: Box<dyn RecordSink>,
}

impl Executor {
    fn execute(&mut self, actions: Vec<Action>) -> Result<()> {
        for action in actions {
            match action {
                Action::OpenSlot { slot, tokens } => {
                    let channel =
                        open_channel(&self.config.endpoints.ws, &self.config.ingest, tokens)
                            .with_context(|| format!("could not open slot {slot}"))?;
                    let handle = channel.handle();
                    let tokens = {
                        let handle = handle.clone();
                        move || handle.asset_ids()
                    };
                    tracing::info!(%slot, tokens = handle.asset_ids().len(), "opening connection slot");
                    let task = run_slot(
                        slot.clone(),
                        channel,
                        tokens,
                        self.clock.clone(),
                        self.events.clone(),
                    );
                    self.tracker.spawn(supervise(
                        "slot",
                        self.token.clone(),
                        self.failed.clone(),
                        async move { task.await.map_err(anyhow::Error::from) },
                    ));
                    self.slots.insert(slot, handle);
                }
                Action::Subscribe { slot, tokens } => self.handle(&slot)?.subscribe(tokens)?,
                Action::Unsubscribe { slot, tokens } => self.handle(&slot)?.unsubscribe(tokens)?,
                Action::Resnapshot { slot, tokens } => {
                    tracing::info!(%slot, tokens = tokens.len(), "resubscribing to force fresh books");
                    let handle = self.handle(&slot)?;
                    handle.unsubscribe(tokens.clone())?;
                    handle.subscribe(tokens)?;
                }
                Action::CloseSlot { slot } => {
                    if let Some(handle) = self.slots.remove(&slot) {
                        tracing::info!(%slot, "closing connection slot");
                        handle.close();
                    }
                }
                Action::RequestBackfill {
                    conditions,
                    from,
                    until,
                } => {
                    // The backfill task stopping means the run is ending.
                    let _ = self.backfill.send(BackfillRequest {
                        conditions,
                        from,
                        until,
                    });
                }
                Action::RegistryHint(hint) => {
                    let _ = self.hints.send(hint);
                }
                Action::Persist(record) => self
                    .sink
                    .write(&record)
                    .context("could not store a record")?,
            }
        }
        Ok(())
    }

    fn handle(&self, slot: &ConnSlotId) -> Result<&ReconnectingMarketChannelHandle> {
        self.slots
            .get(slot)
            .with_context(|| format!("the engine addressed unknown slot {slot}"))
    }

    fn close_all(&mut self) {
        for handle in self.slots.values() {
            handle.close();
        }
    }
}

async fn ticker(
    clock: SharedClock,
    out: mpsc::Sender<Stamped>,
    token: CancellationToken,
) -> Result<()> {
    let mut tick = time::interval(TICK_INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = token.cancelled() => return Ok(()),
            _ = tick.tick() => {
                // A full queue already wakes the engine; skip this tick rather than wait.
                if let Err(mpsc::error::TrySendError::Closed(_)) = out.try_send(Stamped::at(clock.now(), Event::Tick)) {
                    return Ok(());
                }
            }
        }
    }
}

/// Runs the live system until `token` is cancelled or a task fails.
///
/// # Errors
///
/// If a task failed, the engine hit an invariant, or the engine log could not be written.
pub async fn run(
    config: Arc<Config>,
    run_id: &str,
    clock: SharedClock,
    sources: Sources,
    fee_table: FeeTable,
    sink: Box<dyn RecordSink>,
    token: CancellationToken,
) -> Result<RunOutcome> {
    let (events_tx, mut events) = mpsc::channel(EVENT_QUEUE);
    let (hints_tx, hints) = mpsc::unbounded_channel();
    let (backfill_tx, backfill_rx) = mpsc::unbounded_channel();
    let tracker = TaskTracker::new();
    let failed = Arc::new(AtomicBool::new(false));

    tracker.spawn(supervise(
        "registry",
        token.clone(),
        failed.clone(),
        registry_task(
            RegistryState::new(&config.ingest, fee_table),
            sources.markets,
            sources.clob,
            clock.clone(),
            events_tx.clone(),
            hints,
            token.child_token(),
        ),
    ));
    tracker.spawn(supervise(
        "backfill",
        token.clone(),
        failed.clone(),
        backfill_task(
            sources.data_bulk,
            config.ingest.backfill_concurrency,
            config.ingest.backfill_max_pages,
            clock.clone(),
            events_tx.clone(),
            backfill_rx,
            token.child_token(),
        ),
    ));
    tracker.spawn(supervise(
        "ticker",
        token.clone(),
        failed.clone(),
        ticker(clock.clone(), events_tx.clone(), token.child_token()),
    ));

    let mut engine = Engine::new(config.engine.clone());
    let mut log = EngineLogWriter::new(config.run.data_dir.join("engine_log"), run_id);
    let mut executor = Executor {
        config: config.clone(),
        clock: clock.clone(),
        events: events_tx,
        slots: BTreeMap::new(),
        tracker: tracker.clone(),
        token: token.clone(),
        failed: failed.clone(),
        hints: hints_tx,
        backfill: backfill_tx,
        sink,
    };
    let flush_every = chrono::TimeDelta::from_std(FLUSH_INTERVAL).unwrap_or(chrono::TimeDelta::MAX);
    let summary_every =
        chrono::TimeDelta::from_std(SUMMARY_INTERVAL).unwrap_or(chrono::TimeDelta::MAX);
    let mut last_flush = clock.now();
    let mut last_summary = clock.now();
    let mut outcome: Result<()> = Ok(());
    tracing::info!(run_id, "pmlive running");
    loop {
        let stamped = tokio::select! {
            biased;
            () = token.cancelled() => break,
            stamped = events.recv() => match stamped {
                Some(stamped) => stamped,
                None => break,
            },
        };
        let envelope = engine.accept(stamped);
        let now = envelope.available_at;
        let step = log
            .append(&envelope)
            .context("could not write the engine log")
            .and_then(|()| engine.step(envelope).context("engine invariant failed"))
            .and_then(|actions| executor.execute(actions));
        if let Err(error) = step {
            outcome = Err(error);
            break;
        }
        if now - last_flush >= flush_every {
            last_flush = now;
            if let Err(error) = log
                .flush()
                .context("could not flush the engine log")
                .and_then(|()| executor.sink.flush().context("could not flush records"))
            {
                outcome = Err(error);
                break;
            }
        }
        if now - last_summary >= summary_every {
            last_summary = now;
            let books: Vec<_> = engine
                .markets()
                .flat_map(|m| [&m.info.tokens.yes, &m.info.tokens.no])
                .collect();
            let untrusted = books
                .iter()
                .filter(|t| engine.book(t).is_some_and(|b| !b.is_trusted()))
                .count();
            let connected = engine.slots().values().filter(|s| s.connected()).count();
            tracing::info!(
                markets = engine.markets().count(),
                slots = engine.slots().len(),
                connected,
                untrusted,
                seq = engine.seq(),
                "summary"
            );
        }
    }

    tracing::info!("pmlive stopping");
    if let Err(error) = executor.execute(engine.flush_metrics()) {
        tracing::warn!(error = %format!("{error:#}"), "could not store the last metrics");
    }
    token.cancel();
    executor.close_all();
    // Tasks blocked on a full queue see it closed and stop.
    drop(events);
    tracker.close();
    if time::timeout(SHUTDOWN_LIMIT, tracker.wait()).await.is_err() {
        tracing::warn!("tasks did not stop in time");
    }
    executor.sink.flush().context("could not flush records")?;
    let log_files = log.finish().context("could not finish the engine log")?;
    outcome?;
    if failed.load(Ordering::SeqCst) {
        bail!("a task failed");
    }
    Ok(RunOutcome { engine, log_files })
}

#[cfg(test)]
mod tests;
