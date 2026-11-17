# archiver

A Rust CLI that records Polymarket order book data. It picks the markets worth tracking from the Gamma API, streams their order book activity over the CLOB websocket, takes hourly REST snapshots, and writes everything to a local archive. Separate commands compact each finished day to Parquet and check that the books rebuild.

It talks to Polymarket through the [marcasite](https://crates.io/crates/marcasite) crate.

> **Status:** recording works and can be deployed. `compact` and `verify` work but are newer; see [`docs/implementation-status.md`](docs/implementation-status.md). The on-disk format is in [`docs/archive-format.md`](docs/archive-format.md), and the original design in [`docs/design.md`](docs/design.md) (partly out of date).

## How it works

- **Market registry:** every `registry_interval`, fetches open markets from Gamma and selects those that have a configured category tag, are active and accepting orders, and are above `min_liquidity_usd`. Markets that close keep being recorded for `closed_grace_period`.
- **Connection manager:** spreads the selected markets' tokens across websocket connections (`tokens_per_connection` each) and reconnects with exponential backoff. It records a gap whenever a connection is down. If a token gets no full book within `missing_book_timeout`, it asks for a REST snapshot, then marks the token unhealthy.
- **Snapshot worker:** takes REST `/books` snapshots of every token once per `snapshot_interval`, and after each subscribe.
- **Writer:** the only component that writes files. It writes hourly JSONL files per stream, flushes every `flush_interval` and syncs every `sync_interval`. It renames finished files from `.jsonl.open` to `.jsonl`, then zstd-compresses finished market data files in the background.
- **Status:** `state/status.json` is rewritten every 5 s, and `archiver status` applies the health rules to it (recorder running, connections streaming, messages arriving, market list refreshed, queue, disk space, yesterday compacted). Every minute a summary line goes to the log.
- **Crash recovery:** on start, the recorder renames files a crashed run left open. It records a `process_restart` gap for every previously tracked token, covering the time since that run's last data on disk.

## Building

Requires a Rust toolchain (edition 2024) and [just](https://just.systems). The archiver is a member of the repository's Cargo workspace and uses the shared `pm-core` crate (`../crates/pm-core`), so build output goes to the workspace's `target/` folder at the repo root. From this directory:

```sh
just build           # debug binary at ../target/debug/archiver
just build-release   # optimised binary at ../target/release/archiver
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
| `compact [--date D \| --since D \| --all]` | Compact a finished UTC day (default yesterday) to Parquet, or every finished, uncompacted day (since D, or at all). Then delete the raw market data of compacted days older than `raw_retention` |
| `verify [--date D]` | Rebuild each token's book between full books and report matches, mismatches and windows that cannot be checked (default yesterday) |
| `gaps [--date D] [--tokens A,B]` | List recorded gaps with durations and reasons (default today) |

Every command accepts:

- `-c, --config <PATH>`: config file, default `./config.toml`
- `-v` / `-q`: more or less logging (`-v` info, `-vv` debug, `-vvv` trace). Without them, `run` logs at the config's `log_level` and the other commands only warnings. Logs go to stderr.

Exit codes: 0 success, 1 error, 2 invalid arguments, 3 unhealthy (`status`), 78 invalid configuration.

## Deploying

Both targets run the same binary and the same three jobs: the recorder (`archiver run`, always on), a daily `archiver compact --all` followed by `archiver verify`, and a periodic `archiver status` to tell you when something is wrong. Run `archiver check` on the machine that will record: Polymarket's regional restrictions depend on its address.

### On a laptop

For trying it out and for short recordings. A laptop that sleeps or changes network just leaves recorded gaps; the recorder reconnects by itself.

1. Install a Rust toolchain ([rustup](https://rustup.rs)) and a C compiler (Xcode command line tools on macOS, `build-essential` on Debian/Ubuntu). [just](https://just.systems) is optional: every recipe is a one-line `cargo` command.
2. Build: `cargo build --release` in this directory (of a clone of the whole repository). The binary is `../target/release/archiver`.
3. Copy `config.toml` somewhere of your own, e.g. `~/polymarket/config.toml`, and set `archive_dir` to an absolute path such as `/home/you/polymarket/archive`. Choose `min_liquidity_usd` (see Sizing) and lower `min_free_disk_gb` if the disk is small.
4. `archiver check -c ~/polymarket/config.toml`. Every line should say `PASS`.
5. Start recording in a terminal you can leave open (`tmux` or `screen` help): `archiver run -c ~/polymarket/config.toml`. Stop it with Ctrl+C; it finishes its files and exits within 15 s.
6. From another terminal, `archiver status -c ...` shows what it is doing, and `archiver gaps -c ...` what it missed.
7. Once a day (any time after 00:00 UTC), run `archiver compact --all -c ...` and then `archiver verify -c ...`. Missing a day does no harm: `--all` catches up.

To have it start with the laptop on Linux, use the units below as user units (`~/.config/systemd/user/`, without the `User=`, `Group=` and sandboxing lines, and with your own paths), then `systemctl --user enable --now archiver archiver-daily.timer` and `loginctl enable-linger $USER`.

### On a server

Any small Linux machine with systemd works: the recorder needs about 100 MB of memory and a few percent of one core. Disk is what to size (see Sizing). Building can need a couple of GB of memory, so on a smaller machine either add swap for the build, or build on another Linux machine of the same architecture and copy the binary over.

```sh
# 1. Build and install (on the server, in this directory of a clone of the whole repository)
sudo apt-get install -y build-essential ca-certificates   # Debian/Ubuntu
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
cargo build --release
sudo install -m 755 ../target/release/archiver /usr/local/bin/archiver
sudo install -m 755 deploy/healthcheck.sh /usr/local/bin/archiver-healthcheck

# 2. A user, the archive folder and the config
sudo useradd --system --home /var/lib/archiver --create-home archiver
sudo install -d /etc/archiver
sudo install -m 644 config.toml /etc/archiver/config.toml
sudoedit /etc/archiver/config.toml    # archive_dir = "/var/lib/archiver/archive", min_liquidity_usd, ...

# 3. Pre-flight checks, as the user the recorder will run as
sudo -u archiver archiver check --config /etc/archiver/config.toml

# 4. The recorder, the daily compaction and the health check
sudo cp deploy/*.service deploy/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now archiver archiver-daily.timer archiver-health.timer

# 5. Look at it
sudo -u archiver archiver status --config /etc/archiver/config.toml
journalctl -u archiver -f
```

- **The clock must be synchronised** (`timedatectl` should say `System clock synchronized: yes`); `check` fails if it is more than 2 s off.
- **Alerts.** `archiver-health.timer` runs `archiver status` every 5 minutes and, as shipped, only writes problems to the system log (`journalctl -t archiver-health`). Edit `notify` in `/usr/local/bin/archiver-healthcheck` to send them somewhere you will see.
- **Updating.** Build the new version, `sudo install` it over the old one and `sudo systemctl restart archiver`. The restart is a clean stop and start: a few seconds without data, between the old run's `run_stop` record and the new run's `run_start`.
- **Backups.** Copy `parquet/`, `control/` and `markets/` off the machine daily (`rsync` or `rclone` from cron). Raw files older than `raw_retention` are deleted once compacted, so `parquet/` is the long-term copy; its `frames` table holds every raw line.
- **If the disk fills,** the recorder exits rather than lose data silently, and systemd keeps restarting it until there is space. `status` reports low space (`min_free_disk_gb`) well before that.

**Sizing.** Measured live on 2026-10-29 at `min_liquidity_usd = 50000`: about 950 markets (1,900 tokens) on 8 connections, about 22,000 messages a minute. That is roughly 24 GB a day of raw JSON, or about 3 GB once compressed (zstd shrinks it about 8×; each hour is compressed when it ends, so about 1 GB of uncompressed data is on disk at a time). The recorder used about 100 MB of memory and 4% of one CPU. Volume scales with activity more than with market count: at `1000` (the default) about 9,800 markets (19,700 tokens, 79 connections) qualify; the shipped `config.toml` has `2000000` (a few dozen markets, well under 1 GB a day). Each day is on disk twice until its raw files pass `raw_retention`: once as raw files, once as Parquet. Parquet is never deleted, so plan to move old days elsewhere. Measure your actual growth in the first days.

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
| `raw_retention` | `"90d"` | Age at which `compact` deletes a compacted day's raw websocket and snapshot files (at least `"1d"`). Their lines stay in the Parquet `frames` table; control and market files are always kept |
| `min_free_disk_gb` | `10` | Free space below which `status` reports unhealthy |
| `websocket_url` | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | CLOB market websocket |
| `clob_rest_url` | `https://clob.polymarket.com` | CLOB REST API |
| `gamma_url` | `https://gamma-api.polymarket.com` | Gamma API |
| `log_level` | `"info"` | Log level of `run` when no `-v`/`-q` is given |
