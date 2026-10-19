---
date: 2026-10-17
type: project
---
# Implementation Status

A comparison of the archiver as it stands (after the connection manager's websocket handling) against `docs/design.md`. This is an overview of what exists and what is missing, not a code review. Section numbers refer to the design document.

## 1. Summary

The project is in its foundation stage. Configuration loading, the market registry and the connection manager's websocket handling are complete: `archiver run` tracks markets, keeps websocket connections open for their tokens and turns every message into a record. Nothing yet writes to disk or takes a snapshot, so the records are discarded and `archiver run` does not record any data.

| Area | Status |
| --- | --- |
| Configuration (12.2) | Done |
| Connection state machine (5.5, 7) | Done (pure logic only) |
| Backoff calculator (5.7) | Done |
| Clock, random source, `RunId` (5.2, 5.7) | Done |
| Market registry (5.4, 6) | Done |
| Records, messages, control bodies (5.6) | Partial |
| CLI skeleton (12.1) | Partial: all subcommands parse, only `markets` does real work |
| Connection manager: token assignment, sockets, gap tracker (5.5, 7) | Done (no `awaiting_book` tracking) |
| Write queue (bounded, non-blocking) | Partial: bounded channel sized from config, frames put without waiting; no depth or high-water counters |
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

The decision logic from sections 5.5 and 7 is implemented as a pure `step(connection, event, now, config, rng) -> actions` function, shaped around marcasite's market channel (2.6), which owns the socket-level work. States are Connecting, Streaming, Backoff and Closed. It covers backoff with jitter and reset after stable streaming, live subscribe and unsubscribe, token changes in every state, closing when the last token is removed, and shutdown from any state. It emits `OpenGaps` / `CloseGaps` / `Emit` actions that follow the gap contract in section 10.

Deviations from the design:

- No `Subscribing` state. `OpenSocket` carries the tokens, and the channel sends the subscription as part of connecting, so `Opened` goes straight to Streaming. Token changes that arrive while connecting are reconciled with a live subscribe or unsubscribe once open.
- No heartbeat logic (no `SendPing`, `PongReceived` or pong deadline). The channel sends `PING` every `heartbeat_interval` and ends the stream when nothing arrives within `heartbeat_timeout`; that arrives as `TimedOut` and opens a `heartbeat_timeout` gap.
- No connect timeout. The channel bounds the handshake and subscription send with `connect_timeout` and reports a failure.
- A refused handshake's `Retry-After` (`Failed { retry_after }`) lengthens the backoff to at least that long.
- No separate `Closing` state; closing is a single step straight to `Closed` (documented in `types.rs`).
- Socket events are tagged with their `ConnAttemptId`, so late events from an abandoned socket are dropped instead of affecting the current attempt.
- `Tick` carries the socket reader's `last_message_at` instead of a separate shared atomic read.
- `SendSubscribe` has no `custom_features` flag; the manager always sets `custom_feature_enabled: true`.

The connection manager (2.6) drives these state machines.

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

`ControlBody`, `GapReason` and `SnapshotReason` cover every case in section 5.6. `Record` has `WsFrame`, `Market` and `Control`. `WsFrame` holds marcasite's decoded `MarketEvent`, not the raw text (2.6). `ControlBody` has an extra `FrameDecodeFailed`; the `Snapshot` record does not exist yet, and records carry no `run_id` (the writer could add it). `Record::Market` matches the design (`market` plus `Added`/`Updated`). `ConnectionManagerMsg` has only `TokenChanges`, keyed by condition ID with both tokens: socket events go to the manager on its own internal channel and ticks come from its own timer, so there is no `ForConnection` or `Tick`, and no `BooksSeen` yet. `SnapshotMsg` has only `SnapshotRequest`. Shutdown goes through a `CancellationToken` rather than a `Shutdown` message, which is the idiomatic tokio choice.

### 2.6 Connection manager (`src/connection_manager/`)

Complete against sections 5.5, 5.7 (gap tracker) and 7, except `awaiting_book` (3.1).

- **Token assignment** (`assignment.rs`): pure. New markets go to the least-loaded slot with room for both tokens (lowest slot ID on a tie), or to a new slot. Tokens never move. A slot whose last token is removed is forgotten and its ID (`c-0001`, `c-0002`, …) never reused. `TokenChanges` are applied removals first, in condition-ID order.
- **Gap tracker** (`gap_tracker.rs`): pure. Double opens keep the earlier start, closes without an open gap write nothing (silently, not with a warning as section 14.3 says, because connections close gaps on every resume and unsubscribe whether or not one was open), and shutdown closes anything left open. `GapClose` carries a `reason`.
- **Socket task** (`socket.rs`), one per connection attempt, on `marcasite::ws::MarketChannel`. The manager configures the channel's URL, `connect_timeout`, heartbeat (`heartbeat_interval`) and idle timeout (`heartbeat_timeout`), and the task connects with a `MarketSubscription` for the attempt's tokens with `custom_feature_enabled`. It hands the channel's handle to the manager, which sends live subscribe and unsubscribe through it. Each event is stamped with its receive time (to the microsecond), published as `last_message_at` (an atomic), and `try_send` as a `WsFrame` record. A message that fails marcasite's schema decode cannot be archived, so it becomes a `frame_decode_failed` control record quoting marcasite's error (the first 256 bytes of the message). A timeout error becomes `TimedOut`; any other final error, or a normal close, becomes `Closed`. When the queue is full the task drops the record, reports `QueueOverflow` once, and reports the number dropped when the channel ends, which the manager writes as a `queue_overflow` control record. Closing cancels the task, which closes the channel and still records events received until the close completes.
- **What the archive holds instead of raw text.** The design stores each frame's exact text (`raw`). The channel only yields decoded events, so the writer will serialise `MarketEvent` (marcasite's `Serialize`). That is not byte-identical to the wire: a frame holding an array becomes one record per element, unrecognised messages are kept as their JSON value (`MarketEvent::Unknown`), and recognised ones are re-encoded from typed fields, so undocumented fields are lost. Prices and sizes stay exact decimals, never floating point.
- **Manager** (`manager.rs`, `manager_task.rs`): steps the connections and carries out their actions (open and close channels, send live subscribe and unsubscribe, request snapshots, write control and gap records); a failed send is fed back as `SendFailed`. Socket reports come first, then registry changes, then a 100 ms tick that copies `last_message_at` in and ends backoff waits (so a gap can start up to one tick before the last message, never after it). Closed connections are dropped. On shutdown every connection closes, gaps close with `shutdown`, and the sockets get 3 s to finish. Control records are written with a waiting send; only a closed write queue or snapshot channel is fatal.
- `run` spawns it supervised next to the registry. Until the writer and snapshot worker exist, `run` discards the records and snapshot requests so producers never block.

Tested against a fake market channel on a local port (`fake_server.rs`), with shrunk timings: events recorded in order (typed, array frames split, unrecognised messages kept, undecodable ones recorded as control records), the subscription message, a server close (gap from the last message, reconnect subscribes every token), refused connections (one gap for the whole outage), shutdown with a gap open, live subscribe and unsubscribe, closing an empty connection, spreading markets over connections, and a full write queue. The channel's heartbeat and idle timeout are marcasite's and are not tested here.

### 2.7 Command-line interface (`src/cli.rs`, `src/commands/`)

All eight subcommands from section 12.1 exist in clap with their arguments (`--date`/`--since`, `--tokens`, `--all`), and a SIGINT/SIGTERM handler cancels a shared token.

| Command | State |
| --- | --- |
| `run` | Loads config, spawns the registry and connection manager (supervised: an error cancels the process, makes `RunStop` unclean and exits non-zero), waits for cancellation with a 10 s grace period, then sends `RunStop`. Records and snapshot requests are discarded. No writer, snapshots, status file or recovery |
| `markets` | Works: fetches from Gamma once and prints the selected markets, skipped markets, and counts |
| `check`, `status`, `snapshot`, `compact`, `verify`, `gaps` | Print a placeholder line and exit 0 |

## 3. What still needs building

Listed roughly in the order the recording path depends on them.

### 3.1 Recording path (`archiver run`)

1. **Write queue.** Depth and high-water counters for the status file. The queue is already sized from `write_queue_capacity`, and socket tasks already drop and count on overflow.
2. **Writer.** The only component that writes files: `FileKey` from `recv_ts` in UTC, the `raw/ws`, `raw/snapshots`, `control`, `markets` layout (8.3), run ID in file names, `.jsonl.open` while writing then rename on hourly rotation, `seq` assignment, the envelope (8.1) with the serialised `MarketEvent` in place of `raw` (2.6), flush every second and sync every 5 s, and exit on write failure after recording a gap.
3. **Frame inspector.** A match on `MarketEvent::Book`, `MarketResolved` and `NewMarket` (no text search needed, since events arrive decoded) after each event is written, feeding `BooksSeen` to the connection manager and notices to the registry.
4. **Connection manager: `awaiting_book`.** Track tokens subscribed without a full book, request a `missing_book` snapshot after 2 minutes, then mark the token unhealthy. Needs `BooksSeen` from the frame inspector and snapshot worker. Also replace `run`'s placeholder consumers of the write queue and snapshot channel once the writer and snapshot worker exist.
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

122 tests, all passing (`just archiver test`, under 1 s).

| Module | Tests | Coverage against section 14 |
| --- | --- | --- |
| Connection state machine | 38 | The cases of the "Connection state machine" table that still apply (heartbeat and liveness belong to marcasite now), plus `Retry-After`, a timeout becoming a `heartbeat_timeout` gap, token changes while connecting, stale-event handling, shutdown from each state, and a randomised test checking the gap contract over many event sequences, as section 10 asks for |
| Configuration | 18 | Every case in the "Configuration" table except command-line override, which is not implemented. Also checks the shipped `config.toml` is valid |
| Backoff calculator | 4 | First failure, doubling, cap, jitter bounds. The reset and no-reset cases are covered in the connection tests |
| Token assignment | 7 | Every case in the "Token assignment" table, plus ignoring already placed and unknown tokens |
| Gap tracker | 4 | Every case in the "Gap tracker" table except "Start time", which is the state machine's (covered there) |
| Connection manager | 8 | The "Streaming path" group of section 14.4 against a fake websocket server: integration scenarios 2, 4, 11 without the writer, and live subscribe and unsubscribe; plus decoding outcomes and `last_message_at` precision |
| Registry | 43 | The "selection rules" and "difference and sanity check" tables, plus market parsing (outcome order, missing fields, wrong token count), details changed, token-ID changes, websocket resolution before tracking, the re-add block, and `registry_task` against a fake Gamma (error keeps the set, rejection writes `registry_rejected`, notice bursts coalesce, expiry while Gamma is down, cancellation, fatal channel loss) |

The connection tests use their own harness with tokio's paused clock and a scripted random source, which serves as the design's fake clock for that module. `TokioClock` exists for wall-clock time in other tests but is not used yet.

### 4.2 What is missing

**Unit tests for code that already exists:** none missing; see 4.1.

**Unit tests for components not yet built** (section 14.2–14.3): snapshot queue and rate limiter, envelope builder, writer rotation and durability, gap tracker, startup recovery, health evaluation, compactor, book rebuild.

**Test infrastructure** (section 14.1):

| Tool | State |
| --- | --- |
| Fake clock | Partly there: tokio paused time plus `TokioClock` |
| Fake Polymarket server (websocket, `/books`, Gamma) | Websocket only (`connection_manager/fake_server.rs`): scripted connections that answer `PING`, refused handshakes |
| Recorded fixtures from real Polymarket | None |
| Temporary archive folder helper | Not started |
| Archive reader helper | Not started |

**Higher layers:** there is no `tests/` directory. The connection manager's tests cover part of the streaming path, but none of the 16 integration scenarios (14.4) runs against the archive on disk, the end-to-end daily cycle (14.5), the CLI exit-code tests (14.5) or the soak test (14.6) exist. These depend on the writer, connection manager and fake server.

## 5. Design document drift

Places where `design.md` no longer matches the code and could be updated:

- Config key names and the two extra settings (2.1).
- No `Subscribing` or `Closing` connection state, no heartbeat or connect timeout in the state machine (marcasite's channel owns them); `Retry-After` honoured; socket events carry attempt IDs (2.2).
- Websocket records hold marcasite's decoded `MarketEvent`, not the raw frame text, and undecodable messages are a `frame_decode_failed` control record (2.6).
- `GapClose` carries a reason; a close without an open gap is silent, not a warning (2.6).
- `tokens_per_connection` must be at least 2, so a market fits on one connection.
- The `Clock` interface is wall-clock only, with monotonic time from tokio (2.3).
- Shutdown via cancellation token rather than `Shutdown` messages (2.5).
- `TokenId` and `ConditionId` come from marcasite, and Gamma access goes through marcasite rather than raw REST.
- Registry (5.4, 6): the `recently_removed` re-add block, remembered `market_resolved` notices for untracked markets, the follow-up lookup of tracked markets missing from the open listing, `TokenChanges` keyed by condition ID rather than `List<MarketInfo>`, and `registry_interval` defaulting to 5 minutes.
