# archiver

A Rust CLI that records Polymarket market data. It picks the markets worth tracking from the Gamma API, streams their order-book activity over the CLOB websocket, and (once the writer exists) writes everything to a local archive for offline analysis.

It talks to Polymarket through the [marcasite](https://crates.io/crates/marcasite) crate.

> **Status:** early. The market registry and websocket connection handling work, but nothing is written to disk yet, so `run` does not record data. Only `markets` and `run` do real work; the other subcommands are placeholders. See [`docs/implementation-status.md`](docs/implementation-status.md) for detail and [`docs/design.md`](docs/design.md) for the full design (partly out of date).

## How it works

- **Market registry:** every `registry_interval`, fetches open markets from Gamma and selects those with a configured category tag that are active, accepting orders and above `min_liquidity_usd`. Markets that close keep being recorded for `closed_grace_period`.
- **Connection manager:** spreads the selected markets' tokens across websocket connections (`tokens_per_connection` each), reconnects with exponential backoff, and records a gap whenever a connection is down.
- **Writer, snapshots, compaction, verification:** not built yet. The plan is hourly JSONL files under `archive_dir`, periodic REST order-book snapshots, daily compaction to Parquet, and an offline check that rebuilt books match the snapshots.

## Building

Requires a Rust toolchain (edition 2024) and [just](https://just.systems). From this directory:

```sh
just build           # debug binary at target/debug/archiver
just build-release   # optimised binary at target/release/archiver
just test            # run the tests
just ci              # format check, clippy and tests
```

From the repo root, prefix recipes with the module name, e.g. `just archiver build`.

## Usage

```sh
archiver <COMMAND> [OPTIONS]
# or, without building first:
just run <COMMAND> [OPTIONS]
```

| Command | Purpose |
|---|---|
| `run` | Start the recorder. Runs until Ctrl+C or SIGTERM |
| `markets` | Print the markets the registry would select right now, with counts |
| `check` | Pre-flight checks *(placeholder)* |
| `status` | Read the status file; exit 0 if healthy, 3 if not *(placeholder)* |
| `snapshot [--all \| --tokens A,B]` | Take REST snapshots now and write them to the archive *(placeholder)* |
| `compact [--date YYYY-MM-DD]` | Compact finished days (`--since` is an alias) *(placeholder)* |
| `verify [--date YYYY-MM-DD]` | Offline consistency check and report *(placeholder)* |
| `gaps [--date YYYY-MM-DD] [--tokens A,B]` | List recorded gaps with durations and reasons *(placeholder)* |

Every command accepts:

- `-c, --config <PATH>`: config file, default `./config.toml`
- `-v` / `-q`: more or less logging (default level is warn; `-v` info, `-vv` debug, `-vvv` trace). Logs go to stderr.

A config error exits with code 78; any other error exits with 1.

## Configuration

Configuration is a TOML file (`config.toml` in this directory is a working example). Every key is optional and falls back to its default, so an empty file is valid. Unknown keys are rejected. Durations are strings such as `"10s"`, `"5m"` or `"6h 30m"`.

| Key | Default | Meaning |
|---|---|---|
| `archive_dir` | `"./archive"` | Root folder of the archive |
| `category_tags` | `["politics", "geopolitics"]` | Gamma tags to include; must not be empty |
| `min_liquidity_usd` | `1000` | Liquidity a market needs to start being tracked |
| `registry_interval` | `"5m"` | How often to refresh the market list |
| `closed_grace_period` | `"6h"` | How long to keep recording after a market closes |
| `tokens_per_connection` | `250` | Tokens per websocket connection (at least 2) |
| `heartbeat_interval` | `"10s"` | How often to ping the websocket |
| `heartbeat_timeout` | `"30s"` | Silence before a connection is treated as dead; must exceed `heartbeat_interval` |
| `connect_timeout` | `"10s"` | Time allowed to open a socket and send the subscription |
| `backoff_initial` | `"1s"` | First reconnect delay |
| `backoff_max` | `"60s"` | Longest reconnect delay |
| `backoff_reset_after` | `"5m"` | Stable streaming time after which backoff starts over |
| `snapshot_interval` | `"60m"` | How often to take REST order-book snapshots |
| `snapshot_batch_size` | `500` | Tokens per snapshot request (max 500) |
| `snapshot_max_requests_per_s` | `2` | Snapshot request rate limit |
| `write_queue_capacity` | `100000` | Records buffered before new ones are dropped |
| `flush_interval` | `"1s"` | How often the writer flushes files |
| `sync_interval` | `"5s"` | How often the writer syncs files to disk |
| `raw_retention` | `"90d"` | How long raw files are kept after compaction |
| `min_free_disk_gb` | `10` | Free space below which `status` reports unhealthy |
| `websocket_url` | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | CLOB market websocket |
| `clob_rest_url` | `https://clob.polymarket.com` | CLOB REST API |
| `gamma_url` | `https://gamma-api.polymarket.com` | Gamma API |
| `log_level` | `"info"` | Validated but currently unused; use `-v` instead |

Settings for parts not yet built (snapshots, writer, retention, disk checks) are accepted and validated but have no effect yet.
