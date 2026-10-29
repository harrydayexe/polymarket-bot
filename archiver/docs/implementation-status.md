---
date: 2026-10-29
type: project
---
# Implementation Status

How the archiver compares with `docs/design.md`, as of the branch that completed the recording path. Section numbers refer to the design. The on-disk format is in `archive-format.md`; the connection manager's handover notes are in `connection-manager.md`.

## 1. Summary

The recording path is complete and has been run against live Polymarket. `archiver run` tracks markets, streams their websocket data, takes REST snapshots and writes everything to disk. It recovers from crashes and stops cleanly. The operator commands work, and the offline commands (`compact`, `verify`) work in a first version.

| Area | Status |
| --- | --- |
| Configuration (12.2) | Done (no command-line overrides) |
| Market registry (5.4, 6) | Done |
| Connection manager, gap tracker (5.5, 7) | Done, including `awaiting_book` |
| Snapshot worker, rate limiter (5.7, 9) | Done |
| Records, line format, folder layout (5.6, 8.1–8.3) | Done; see `archive-format.md` |
| Writer: rotation, durability, failure exit (8.4) | Done, plus zstd compression of finished market data files |
| Frame inspector (5.3) | Done |
| Status reporter, status file, summary log (13) | Done |
| Startup recovery, clean shutdown (7, 12.1) | Done |
| `check`, `status`, `markets`, `snapshot`, `gaps` | Done |
| `compact` (11) | First version: all tables, accounting, atomic swap, manifest. No raw retention |
| `verify`, `OrderBook` (5.8, 10) | First version |
| Integration and end-to-end tests (14.4, 14.5) | Partial (see 4) |

## 2. Live run (2026-10-29)

A run at `min_liquidity_usd = 50000` tracked 946 markets (1,892 tokens) on 8 connections. It received a full book for every token on subscribe and about 20,000 messages a minute, with no undecodable messages. The queue's high-water mark was 1,829 of 100,000. It stopped cleanly in 2 s. Restarting after a hard kill recorded a `process_restart` gap for 1,896 tokens and renamed the 3 files left open. Compacting the recorded data balanced the accounting with nothing in `unparsed`. `verify` found 227 matching windows and no mismatches, which is early support for the first assumption in section 16: `size` in `price_change` is the new total.

The live run found one bug, now fixed. `/books` sends `"last_trade_price": ""` for a token that has not traded, and marcasite 0.1.3 rejects that, so every batch containing such a token failed. The snapshot worker now posts to `/books` itself and archives each book as sent (see 3).

## 3. Differences from the design

Earlier differences (config key names, marcasite's channel owning heartbeats, no `Subscribing`/`Closing` states, registry details) are unchanged; see the previous version of this file in git history and `connection-manager.md`. New in this round:

- **Websocket records hold re-encoded events, not raw text** (a consequence of marcasite's channel, see `connection-manager.md` 3.1). The field is `event`, not `raw`. One line per array element.
- **Snapshot records hold each book exactly as sent** (`books`), the design's intent, read leniently with `archive::snapshot_book`. Retries honour `Retry-After`.
- **Finished hourly websocket and snapshot files are compressed to `.jsonl.zst`** in the background, because raw JSON runs at about 20 GB a day for ~1,900 tokens. Control and market files are not compressed. The design compressed raw files only after compaction.
- **Files close 10 s after their hour ends**, for records stamped just before the boundary. A record arriving later still gets a new part file (`<run>-p2.jsonl`), never a reopened one.
- **`BooksSeen` comes from the writer only**, for both websocket books and snapshot books, since it sees both after they are written. The snapshot worker does not send it.
- **`missing_book_timeout`** (default 2 min) is a config key rather than a constant.
- **The restart gap starts at `durable_through`**, the latest websocket record known to be synced, rather than the last message received, so it never starts after data that was lost. With no status file to go on, it starts at the status file's modification time less 10 s.
- **The archive is locked** (`state/run.lock`), so a second `run` on the same archive refuses to start.
- **A `new_market` message triggers an early registry refresh only if its tags include a configured category.** Live, they arrive with `tags: []`, so in practice new markets join at the next registry cycle (`registry_interval`, 5 min).
- **Health rules:** "no tokens tracked" for over 10 minutes is a problem, and a cleanly stopped recorder reports as not running. The compaction rule applies only when yesterday has raw data. Unhealthy tokens are listed as a warning, not a failure.
- **Status file:** no `last_pong_age_s` (marcasite handles pongs), but per-connection `since` and `last_message_at` are included, plus `durable_through`.
- **`run` logs at `log_level`** unless `-v`/`-q` is given. Logs are plain text when not on a terminal.
- **Exit codes:** a config error still exits 78, not 2.

## 4. Still to do

Roughly in order of value:

1. **Raw retention** (`raw_retention` has no effect): delete raw files of compacted days after the retention period, once the compactor has been trusted for a while.
2. **Integration scenarios against the archive on disk** (14.4). `commands::run` has three in-process end-to-end tests: a clean recording, crash recovery and the lock. Missing scenarios include a slow or full disk, an hour boundary under load, and a Gamma outage through the whole program. There are no process-level CLI tests (14.5) or soak test (14.6) beyond the live run above.
3. **Recorded fixtures** from real data for the compactor and verifier tests (they use hand-written events).
4. **Command-line overrides of config values.**
5. `verify` reports each token's first book of a day as `no_anchor`, and each run boundary as not checkable.
6. Performance of `compact` on a full day is unmeasured. It took 10 s for 3 minutes of data in a debug build, so measure a release build on a real day.

## 5. Open decisions for the owner

- **Liquidity floor and categories** (design 16). At `min_liquidity_usd = 1000`, about 9,800 markets (19,700 tokens) qualify, about 10 times the 50,000 floor's count. The shipped `config.toml` has 2,000,000 (38 markets).
- **`registry_interval`** is 5 minutes, which cannot meet the "tracked within 2 minutes" acceptance criterion; the design had 60 s.
- **Disk and backup:** plan for several GB a day compressed at the 50,000 floor, more at lower floors.
