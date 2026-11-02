# archiver

A Rust CLI that records Polymarket order book data. It picks the markets worth tracking from the Gamma API, streams their order book activity over the CLOB websocket, takes hourly REST snapshots, and writes everything to a local archive. Separate commands compact each finished day to Parquet and check that the books rebuild.

It talks to Polymarket through the [marcasite](https://crates.io/crates/marcasite) crate.

> **Status:** recording works and can be deployed. `compact` and `verify` work but are new; see [`docs/implementation-status.md`](docs/implementation-status.md). The on-disk format is in [`docs/archive-format.md`](docs/archive-format.md), and the original design in [`docs/design.md`](docs/design.md) (partly out of date).

## How it works

- **Market registry:** every `registry_interval`, fetches open markets from Gamma and selects those that have a configured category tag, are active and accepting orders, and are above `min_liquidity_usd`. Markets that close keep being recorded for `closed_grace_period`.
- **Connection manager:** spreads the selected markets' tokens across websocket connections (`tokens_per_connection` each) and reconnects with exponential backoff. It records a gap whenever a connection is down. If a token gets no full book within `missing_book_timeout`, it asks for a REST snapshot, then marks the token unhealthy.
- **Snapshot worker:** takes REST `/books` snapshots of every token once per `snapshot_interval`, and after each subscribe.
- **Writer:** the only component that writes files. It writes hourly JSONL files per stream, flushes every `flush_interval` and syncs every `sync_interval`. It renames finished files from `.jsonl.open` to `.jsonl`, then zstd-compresses finished market data files in the background.
- **Status:** `state/status.json` is rewritten every 5 s, and `archiver status` applies the health rules to it. Every minute a summary line goes to the log.
- **Crash recovery:** on start, the recorder renames files a crashed run left open. It records a `process_restart` gap for every previously tracked token, covering the time since that run's last data on disk.

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
| `run` | Start the recorder. Runs until Ctrl+C or SIGTERM, then stops cleanly within 15 s |
| `markets` | Print the markets the registry would select right now, with counts |
| `check` | Pre-flight checks: archive folder, free disk, Gamma, CLOB REST and clock, websocket. Exit 1 if any fail |
| `status` | Read the status file and apply the health rules; exit 0 if healthy, 3 if not |
| `snapshot --all \| --tokens A,B` | Take REST snapshots now and write them to the archive (`--all`: every tracked token) |
| `compact [--date D \| --since D]` | Compact a finished UTC day (default yesterday) to Parquet, or every finished, uncompacted day since D |
| `verify [--date D]` | Rebuild each token's book between full books and report matches, mismatches and windows that cannot be checked (default yesterday) |
| `gaps [--date D] [--tokens A,B]` | List recorded gaps with durations and reasons (default today) |

Every command accepts:

- `-c, --config <PATH>`: config file, default `./config.toml`
- `-v` / `-q`: more or less logging (`-v` info, `-vv` debug, `-vvv` trace). Without them, `run` logs at the config's `log_level` and the other commands only warnings. Logs go to stderr.

Exit codes: 0 success, 1 error, 2 invalid arguments, 3 unhealthy (`status`), 78 invalid configuration.

## Deploying

1. Build on (or for) the server: `just build-release`, then copy `target/release/archiver` to `/usr/local/bin/`.
2. Write `/etc/archiver/config.toml`, starting from `config.toml`. Set `archive_dir` and choose `min_liquidity_usd` deliberately (see below).
3. Run `archiver check --config /etc/archiver/config.toml` **on the server**: Polymarket's regional restrictions depend on its address.
4. Install the units in [`deploy/`](deploy): `archiver.service` (the recorder, restarted 5 s after any exit), and `archiver-daily.timer` with `archiver-daily.service` (compact and verify at 01:30 UTC; set `--since` to your first day). Install `healthcheck.sh` from cron to alert when `status` is unhealthy.
5. Back up `parquet/` (and ideally the raw files) off the machine daily.

**Sizing.** Measured live on 2026-10-29 at `min_liquidity_usd = 50000`: about 950 markets (1,900 tokens) on 8 connections, about 22,000 messages a minute. That is roughly 24 GB a day of raw JSON, or about 3 GB once compressed (zstd shrinks it about 8×). The recorder used about 100 MB of memory and 4% of one CPU. Volume scales with activity more than with market count: at `1000` (the design default) about 9,800 markets (19,700 tokens, 79 connections) qualify; the shipped `config.toml` has `2000000` (38 markets). Measure your actual growth in the first days. Raw files are never deleted yet (`raw_retention` has no effect).

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
| `missing_book_timeout` | `"2m"` | Time a subscribed token may go without a full book before a snapshot is requested, and then before it is marked unhealthy |
| `snapshot_interval` | `"60m"` | How often to take REST order-book snapshots |
| `snapshot_batch_size` | `500` | Tokens per snapshot request (max 500) |
| `snapshot_max_requests_per_s` | `2` | Snapshot request rate limit |
| `write_queue_capacity` | `100000` | Records buffered before new ones are dropped |
| `flush_interval` | `"1s"` | How often the writer flushes files |
| `sync_interval` | `"5s"` | How often the writer syncs files to disk |
| `raw_retention` | `"90d"` | How long raw files are kept after compaction (not implemented yet: raw files are kept) |
| `min_free_disk_gb` | `10` | Free space below which `status` reports unhealthy |
| `websocket_url` | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | CLOB market websocket |
| `clob_rest_url` | `https://clob.polymarket.com` | CLOB REST API |
| `gamma_url` | `https://gamma-api.polymarket.com` | Gamma API |
| `log_level` | `"info"` | Log level of `run` when no `-v`/`-q` is given |
