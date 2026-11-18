# pm-live

The live paper-trading system (components C2–C9 of the Phase 1 spec): library plus the `pmlive` binary. So far: C2 live ingest and the engine skeleton it feeds.

## Shape

I/O tasks turn the outside world into `event::Envelope`s (`event_time` = exchange time where there is one, `available_at` = receive time). One engine task consumes them in order through the pure `engine::Engine::step` (no I/O, no clock reads) and returns `action::Action`s, which the `runtime` executor carries out. Every envelope is appended to the engine log (`engine_log`, hourly JSONL, zstd when the hour closes), so a run replays bit for bit.

| Module | Contents |
| --- | --- |
| `config` | `pmlive.toml` (`config/pmlive.example.toml` at the repo root) |
| `event` | `Envelope`, `Event`, `LiveMarket`, `BackfillPrint` |
| `action` | `Action`, `Record` (rows C8 will store in Postgres), `Metric` |
| `engine` | `Engine`; `books` (ring of versions + trust, §3.4; a YES/NO mirror check, not wired into the engine yet), `volume` (24 h pair volume with backfill merge, §3.6), `ordering` (price_change vs last_trade_price metric, D3) |
| `ingest::ws` | One `ReconnectingMarketChannel` per slot (D11); pure `translate` of the C2 table |
| `ingest::registry` | Gamma poll through pm-core `Registry`; fees gamma → clob → dated table (D13) |
| `ingest::backfill` | `/v2/trades` per ≤ 20 conditions on the `data_bulk` budget (D12) |
| `sink` | `Persist` rows to `<data_dir>/records/<run_id>.jsonl` until C8 |
| `runtime` | Wiring, engine loop, executor, supervision, shutdown |

Ownership: the registry task owns universe selection and fee resolution; the engine owns slot placement (`pm_core::market::TokenAssignment`) and issues `OpenSlot`/`Subscribe`/`Unsubscribe`/`CloseSlot`/`Resnapshot`. Anything in the engine that produces actions iterates ordered maps or sorted vectors, so replays are deterministic; keep it that way.

Observed live (18 Nov 2026) and relied on: `/v2/trades` ignores `start` without a user (backfill pages newest-first and filters); initial `book` snapshots carry the time of the book's last change (ring ages by receive time and keeps the newest version); unsubscribe + subscribe yields a fresh `book`; `new_market` arrives for every market on Polymarket with no tags (hints are coalesced, `new_market_min_gap`).

## Tests

Engine logic is tested through `step` with explicit times; slot translation on scripted streams on paused time; anything with real sockets (`pm_core::test_util::fake_ws`, wiremock) on real time with short timings, because the paused clock jumps while I/O is pending. `runtime::tests` runs the whole thing end to end and replays its engine log.

## Code Style

Same as `crates/pm-core/AGENTS.md`: `invariant!` for anomalous conditions with an explicit recovery, about two assertions per function, no floats for money.

## Commands

Run from the repo root; from inside `crates/pm-live/`, drop the `live` prefix.

| Command | Purpose |
|---|---|
| `just live build` | Build the library and `pmlive` |
| `just live check` | Type-check all targets |
| `just live test [args]` | Run the tests |
| `just live lint` | Clippy, warnings as errors |
| `just live fmt` | Format |
| `just live ci` | Format check, lint and tests |
| `just live run <args>` | Run `pmlive`, e.g. `just live run run --config config/pmlive.example.toml` |
