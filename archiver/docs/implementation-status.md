---
date: 2026-10-17
type: project
---
# Implementation Status

A comparison of the archiver as it stands (commit `0507a62`) against `docs/design.md`. This is an overview of what exists and what is missing, not a code review. Section numbers refer to the design document.

## 1. Summary

The project is in its foundation stage. The pure decision logic for a single websocket connection is complete and thoroughly tested, configuration loading and the market registry are complete. Nothing yet touches a websocket, writes to disk, or takes a snapshot, so `archiver run` does not record any data.

| Area | Status |
| --- | --- |
| Configuration (12.2) | Done |
| Connection state machine (5.5, 7) | Done (pure logic only) |
| Backoff calculator (5.7) | Done |
| Clock, random source, `RunId` (5.2, 5.7) | Done |
| Market registry (5.4, 6) | Done |
| Records, messages, control bodies (5.6) | Partial |
| CLI skeleton (12.1) | Partial: all subcommands parse, only `markets` does real work |
| Connection manager: token assignment, sockets, gap tracker | Not started |
| Write queue (bounded, non-blocking) | Not started (placeholder channel only) |
| Writer, file layout, rotation, durability (8) | Not started |
| Frame inspector | Not started |
| Snapshot worker and rate limiter (9) | Not started |
| Status reporter, status file, health rules (13) | Not started |
| Startup recovery, clean shutdown sequence (7, 12.1) | Not started |
| Compactor, Parquet output (11) | Not started |
| Book rebuild and `verify` (5.8, 10) | Not started |
| `gaps`, `check`, `status`, `snapshot` commands | Stubs only |
| Test infrastructure (14.1) | Mostly not started |
| Integration, end-to-end, CLI and soak tests (14.4–14.6) | Not started |

## 2. What is built

### 2.1 Configuration (`src/config.rs`)

Complete against section 12.2. Loaded from TOML with every key defaulted, so an empty file works. Unknown keys are rejected by name, durations use humantime strings (`"10s"`, `"6h 30m"`), and parse errors point at the offending text via miette. Validation covers non-zero counts and intervals, heartbeat timeout longer than interval, backoff initial not above max, batch size at most 500, URL schemes and log level.

Differences from the design:

- Keys drop the `_s` / `_min` / `_h` suffixes because they are humantime durations (`heartbeat_interval` rather than `heartbeat_interval_s`).
- Two extra settings: `connect_timeout` and `backoff_reset_after`, which the design hard-coded.
- `registry_interval` defaults to 5 minutes, not 60 seconds.
- The shipped `config.toml` sets `min_liquidity_usd = 2000000`, far above the design's 1,000 (presumably for development).
- Command-line flags cannot override config values yet; the only flags are `--config` and verbosity.
- `log_level` and `clob_rest_url` are validated but not used: logging is driven by the `-v` flag, and no CLOB REST client exists yet.
- A config error exits with code 78 rather than the design's 2.

### 2.2 Connection state machine (`src/connection_manager/connection.rs`, `types.rs`)

The decision logic from sections 5.5 and 7 is implemented as a pure `step(connection, event, now, config, rng) -> actions` function. It covers every state and transition in the design: connecting with timeout, subscribing, streaming with ping schedule and pong deadline, backoff with jitter and reset after stable streaming, live subscribe and unsubscribe, token changes in every state, closing when the last token is removed, and shutdown from any state. It emits `OpenGaps` / `CloseGaps` / `Emit` actions that follow the gap contract in section 10.

Deliberate deviations, all reasonable for Rust:

- No separate `Closing` state; closing is a single step straight to `Closed` (documented in `types.rs`).
- Socket events are tagged with their `ConnAttemptId`, so late events from an abandoned socket are dropped instead of affecting the current attempt.
- An explicit `SubscribeSent` event moves Subscribing to Streaming, with its own timeout, and token changes that arrive mid-subscribe are reconciled when it completes.
- `Tick` carries the socket reader's `last_message_at` instead of a separate shared atomic read.
- `SendSubscribe` has no `custom_features` flag; the code that sends the message will need to set `custom_feature_enabled: true` itself.

What is **not** there is everything around it: nothing creates `Connection`s, carries out their actions, or opens a socket. `manager.rs` and `manager_task.rs` are empty files.

### 2.3 Backoff, clock and IDs

- `backoff_delay` (`backoff_delay.rs`): doubling from `backoff_initial`, capped at `backoff_max`, 0–50% jitter from an injected `RandomSource`. Done.
- `Clock` (`clock.rs`): a wall-clock trait with `SystemClock` and a test-only `TokioClock` whose time follows tokio's pausable clock. Monotonic time uses `tokio::time::Instant` directly, which can also be paused in tests. This is a Rust-idiomatic replacement for the design's `mono_now` / `sleep_until` interface.
- `RandomSource`, `SystemRandom`, `ScriptedRandom` and `RunId::create` (`types.rs`): done. `RunId` is not yet created or used by `run`.
- `ConnSlotId` and `ConnAttemptId`: done. `TokenId` and `ConditionId` come from marcasite.

### 2.4 Market registry (`src/market_registry/`)

Complete against sections 5.4 and 6 (reworked after a code review, commits `cedb6b7` and `0507a62`).

- `MarketInfo` (`market_info.rs`) carries the design's fields, built from a Gamma `Market`. Markets that are malformed or whose outcomes are not Yes/No are skipped with a warning (`selection::parse_markets`); the rest of the cycle goes ahead.
- `selection::select` applies the joining rules locally: a configured category tag, active, open and accepting orders, liquidity at or above the floor.
- `Registry` (`registry.rs`) is pure state. Each cycle: `expire` removes `Closing` markets past their grace period (it runs even when Gamma is down), `check_listing` counts qualifying markets and runs the sanity check, `missing_from` lists tracked markets absent from the open listing, and `plan_changes` / `apply_plan` produce market records and `TokenChanges`.
- Stay-tracked rules: tracked markets missing from the listing are looked up by condition ID (with both `closed=true` and `closed=false`, because the keyset listing leaves out closed markets otherwise). Closed ones enter `Closing` (`ClosedOnGamma`); markets below the floor stay tracked; markets Gamma no longer returns at all are kept, with a warning.
- `resolve_from_websocket` starts the grace period from the frame's receive time. Notices for markets not yet tracked are remembered, so a lagging Gamma cannot add a resolved market as open.
- Removed markets cannot be re-added for three registry intervals (`recently_removed`, not in the design), to cover Gamma lag.
- "Details changed" compares question, dates, tags, tick size and token IDs. Only a token-ID change causes an unsubscribe and resubscribe.
- `TokenChanges` is keyed by condition ID, with both tokens of a market together.
- `registry_task` (`registry_task.rs`): timer with missed ticks delayed, and the cycle bounded by `registry_interval`. A burst of `NewMarketSeen` messages coalesces into one early refresh. Market records (`Added`/`Updated`, one per market) are written before `TokenChanges` is sent, and nothing is sent when nothing changed. Gamma failures are a typed `CycleError` (fetch, timeout, rejected): logged, with `registry_rejected` written for a rejection. Only a closed channel is fatal, and `run` then exits non-zero with an unclean `RunStop`.
- `MarcasiteClient::connect` uses `gamma_url`, resolves the category tags once at startup, and fails if a tag is unknown.

Differences from the design:

- The grace period for a market closed on Gamma starts when the registry sees the closure, not at Gamma's `closedTime`, which is an unformatted string.
- `MarketInfo.slug` is optional, and `accepting_orders` is an extra field (needed by the joining rules).

### 2.5 Records and messages (`src/messages.rs`)

`ControlBody`, `GapReason` and `SnapshotReason` cover every case in section 5.6. `Record` has only `Market` and `Control`; `WsFrame` and `Snapshot` records do not exist yet, and records carry no `run_id` (the writer could add it). `GapClose` has an `attempts` field where the design has `reason`. `Record::Market` matches the design (`market` plus `Added`/`Updated`). `ConnectionManagerMsg` has only `TokenChanges`, keyed by condition ID with both tokens (no `ForConnection`, `BooksSeen` or `Tick`), and there is no `SnapshotMsg`. Shutdown goes through a `CancellationToken` rather than a `Shutdown` message, which is the idiomatic tokio choice.

### 2.6 Command-line interface (`src/cli.rs`, `src/commands/`)

All eight subcommands from section 12.1 exist in clap with their arguments (`--date`/`--since`, `--tokens`, `--all`), and a SIGINT/SIGTERM handler cancels a shared token.

| Command | State |
| --- | --- |
| `run` | Loads config, spawns the registry task only (supervised: an error cancels the process, makes `RunStop` unclean and exits non-zero), waits for cancellation with a 10 s grace period, then tries to send `RunStop` to a write queue nothing reads. No connections, writer, snapshots, status file or recovery |
| `markets` | Works: fetches from Gamma once and prints the selected markets, skipped markets, and counts |
| `check`, `status`, `snapshot`, `compact`, `verify`, `gaps` | Print a placeholder line and exit 0 |

## 3. What still needs building

Listed roughly in the order the recording path depends on them.

### 3.1 Recording path (`archiver run`)

1. **Write queue.** Replace the placeholder `mpsc::channel(10_000)` with a bounded queue sized from `write_queue_capacity`, a non-blocking `try_put` that drops and counts on overflow, and depth and high-water counters.
2. **Writer.** The only component that writes files: `FileKey` from `recv_ts` in UTC, the `raw/ws`, `raw/snapshots`, `control`, `markets` layout (8.3), run ID in file names, `.jsonl.open` while writing then rename on hourly rotation, `seq` assignment, the envelope (8.1), flush every second and sync every 5 s, and exit on write failure after recording a gap.
3. **Frame inspector.** Cheap text match for `book`, `market_resolved` and `new_market` after each frame is written, feeding `BooksSeen` to the connection manager and notices to the registry.
4. **Connection manager.**
   - Token assignment: least-loaded connection first, both tokens of a market on the same connection, no rebalancing, new slot when all are full, drop empty slots.
   - The executor that carries out `ConnectionAction`s: open the socket (via marcasite's `ws` feature, already enabled in `Cargo.toml`), send subscribe/unsubscribe with `custom_feature_enabled`, send `PING`, close.
   - One socket reader task per connection that stamps receive time, pushes `WsFrame` records, reports `PONG` and close events, and publishes `last_message_at`.
   - A ~100 ms tick loop feeding `Tick` into every connection.
   - `GapTracker` to make double opens and unmatched closes harmless and to turn gap actions into control records.
   - `awaiting_book` tracking: request a `missing_book` snapshot after 2 minutes, then mark the token unhealthy.
5. **Snapshot worker.** Deduplicating FIFO, batches of up to 500, `POST /books` through a CLOB client (none exists yet), token-bucket rate limiter, 3 retries then `snapshot_failed` and re-queue, and the hourly schedule spread across the hour.
6. **Status reporter.** Shared atomic counters, the status file every 5 s written by temp-file-and-rename, and `state/tracked_tokens.json`.
7. **Startup and shutdown.** Create the `RunId`, write `run_start` with version and config hash, rename leftover `.open` files, and write a `process_restart` gap from the previous status file if it lacks a clean stop. On shutdown, close connections, drain the queue, rename files, write `run_stop`, set the clean-stop flag, all within 15 s.
8. **Periodic summary log line** every 60 s (section 13).

### 3.2 Registry work remaining

- Nothing sends `RegistryMsg` yet; the frame inspector will.
- `Registry.last_success_at` is recorded but not read until the status reporter exists.

### 3.3 Other commands

- `check`: config valid, archive folder writable, free disk, all three sources reachable, clock in sync.
- `status`: read the status file, apply the section 13 health rules as a pure function, exit 3 when unhealthy.
- `snapshot --all | --tokens`: reuse the snapshot worker's fetch and write path.
- `gaps`: read control records for a date and list gaps with durations and reasons.
- Command-line overrides of config values, and the design's exit codes.

### 3.4 Offline side

None of this exists, and no Parquet dependency is in `Cargo.toml` yet. `rust_decimal` is already present for the fixed-point types.

- `parse_frame` into `ParsedEvent`s (never failing; unknown input becomes `UnparsedEvent`).
- Compactor: refuse if `.open` files exist, route events to the nine tables, fixed-point decimals, accounting check, write to a temporary folder and swap in, day manifest, raw retention and compression.
- `OrderBook` with `from_book`, `apply`, `equals`, best bid/ask.
- `verify`: rebuild between anchor and snapshot, report `Match`, `Mismatch` or `NotCheckable` per window.

## 4. Testing

### 4.1 What exists

113 unit tests, all passing (`just archiver test`, about 0.4 s).

| Module | Tests | Coverage against section 14 |
| --- | --- | --- |
| Connection state machine | 49 | Every case in the "Connection state machine" and "Heartbeat and liveness" tables, plus stale-event handling, shutdown from each state, and a randomised test checking the gap contract over many event sequences, as section 10 asks for |
| Configuration | 17 | Every case in the "Configuration" table except command-line override, which is not implemented. Also checks the shipped `config.toml` is valid |
| Backoff calculator | 4 | First failure, doubling, cap, jitter bounds. The reset and no-reset cases are covered in the connection tests |
| Registry | 43 | The "selection rules" and "difference and sanity check" tables, plus market parsing (outcome order, missing fields, wrong token count), details changed, token-ID changes, websocket resolution before tracking, the re-add block, and `registry_task` against a fake Gamma (error keeps the set, rejection writes `registry_rejected`, notice bursts coalesce, expiry while Gamma is down, cancellation, fatal channel loss) |

The connection tests use their own harness with tokio's paused clock and a scripted random source, which serves as the design's fake clock for that module. `TokioClock` exists for wall-clock time in other tests but is not used yet.

### 4.2 What is missing

**Unit tests for code that already exists:** none missing; see 4.1.

**Unit tests for components not yet built** (section 14.2–14.3): token assignment, snapshot queue and rate limiter, envelope builder, writer rotation and durability, gap tracker, startup recovery, health evaluation, compactor, book rebuild.

**Test infrastructure** (section 14.1):

| Tool | State |
| --- | --- |
| Fake clock | Partly there: tokio paused time plus `TokioClock` |
| Fake Polymarket server (websocket, `/books`, Gamma) | Not started |
| Recorded fixtures from real Polymarket | None |
| Temporary archive folder helper | Not started |
| Archive reader helper | Not started |

**Higher layers:** there is no `tests/` directory, so none of the 16 integration scenarios (14.4), the end-to-end daily cycle (14.5), the CLI exit-code tests (14.5) or the soak test (14.6) exist. These depend on the writer, connection manager and fake server.

## 5. Design document drift

Places where `design.md` no longer matches the code and could be updated:

- Config key names and the two extra settings (2.1).
- No `Closing` connection state; socket events carry attempt IDs; `SubscribeSent` event (2.2).
- The `Clock` interface is wall-clock only, with monotonic time from tokio (2.3).
- Shutdown via cancellation token rather than `Shutdown` messages (2.5).
- `TokenId` and `ConditionId` come from marcasite, and Gamma access goes through marcasite rather than raw REST.
- Registry (5.4, 6): the `recently_removed` re-add block, remembered `market_resolved` notices for untracked markets, the follow-up lookup of tracked markets missing from the open listing, `TokenChanges` keyed by condition ID rather than `List<MarketInfo>`, and `registry_interval` defaulting to 5 minutes.
