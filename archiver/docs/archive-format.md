---
date: 2026-10-29
type: project
---
# Archive format

What `archiver run` writes, for anyone reading the archive. This replaces design sections 8.1–8.3 where they differ. The code is the reference: `src/archive/` (layout, line format, reader) and `src/messages.rs` (control records).

## Folders

```text
archive/
  raw/ws/date=YYYY-MM-DD/hour=HH/<run_id>.jsonl[.zst]          websocket messages
  raw/snapshots/date=YYYY-MM-DD/hour=HH/<run_id>.jsonl[.zst]   REST /books snapshots
  control/date=YYYY-MM-DD/<run_id>.jsonl                       connects, gaps, run start/stop...
  markets/date=YYYY-MM-DD/<run_id>.jsonl                       market details when added or changed
  parquet/date=YYYY-MM-DD/<table>.parquet + manifest.json      written by `compact`
  reports/verify/date=YYYY-MM-DD.json                          written by `verify`
  state/status.json, state/tracked_tokens.json, state/run.lock
```

- Dates and hours are UTC, from each record's receive time.
- A file being written ends in `.jsonl.open`. It is renamed to `.jsonl` 10 s after its hour (or day) ends, or when the run stops. Readers must ignore `.open` files. After a crash, the next start renames them; their last line may be cut off.
- Finished `raw/` files are compressed to `.jsonl.zst` (zstd) in the background soon after they close. If both `x.jsonl` and `x.jsonl.zst` exist, the `.zst` is complete and the plain file is a leftover. Control and market files stay uncompressed.
- A record that reaches the writer after its hour's file has closed goes in a new file of that hour, `<run_id>-p2.jsonl`.
- The run ID is `r-YYYYMMDD-HHMM-xxxx` (start time plus a random suffix). Manual `archiver snapshot` runs get their own run ID.

## Lines

One JSON object per line. Every line starts with:

| Field | Meaning |
| --- | --- |
| `recv_ts` | Receive time, microseconds since the epoch (UTC) |
| `seq` | Counter per run: one per record, in the order written. Within a run it has no gaps across all four streams |
| `run_id` | The run |
| `conn_id` | Connection attempt (`c-0003-7`) for websocket and connection records, else `null` |
| `source` | `ws`, `snapshot`, `control` or `market` |

Then, by source:

- **`ws`**: `event`, one market channel message. A frame holding a JSON array is one line per element. Known message types (`book`, `price_change`, `last_trade_price`, `tick_size_change`, `best_bid_ask`, `new_market`, `market_resolved`) are re-encoded by marcasite from its typed fields: prices and sizes stay exact decimal strings and exchange timestamps stay strings, but fields marcasite does not know are dropped. Anything else is kept exactly as received. A known message that fails to decode is not here; it becomes a `frame_decode_failed` control record quoting its start.
- **`snapshot`**: `reason` (`subscribe`, `new_token`, `scheduled`, `manual`, `missing_book`), `requested_tokens`, and `books`, the `/books` response's books exactly as sent. A requested token with no book is simply absent. Read them with `archive::snapshot_book::SnapshotBook::parse`, which allows for empty strings such as `"last_trade_price": ""`.
- **`control`**: `kind` and its fields. Times are microseconds; token lists are sorted.

  | `kind` | Fields |
  | --- | --- |
  | `run_start` | `program_version`, `config_hash`, `previous_run` |
  | `run_stop` | `clean` |
  | `connect` / `disconnect` | `slot`, `token_count` / `reason` |
  | `subscribe` / `unsubscribe` | `slot`, `tokens` |
  | `gap_open` / `gap_close` | `tokens`, `start` / `end`, `reason`, `detail` (the error, for `connect_failed` and `connection_closed`) |
  | `snapshot_failed` | `tokens`, `error`, `attempts` |
  | `queue_overflow` | `slot`, `dropped` |
  | `registry_rejected` | `reason`, `fetched`, `previous` |
  | `token_unhealthy` | `token`, `since` |
  | `frame_decode_failed` | `error` |

  Gap reasons: `connect_failed`, `connection_closed`, `heartbeat_timeout`, `queue_overflow`, `process_restart`, `write_failure`, `shutdown`, `unsubscribed`, `resumed`. The gap contract in design section 10 holds. A crashed run's gap is recorded by the next run as a `gap_open` and `gap_close` with reason `process_restart`, starting at the last websocket data known to be on disk.
- **`market`**: `change` (`added` or `updated`) and `market`: `condition_id`, `question`, `slug`, `event_id`, `tags`, `tokens` (`yes`, `no`), `start_date`, `end_date` (µs), `active`, `closed`, `accepting_orders`, `liquidity_usd`, `tick_size`, `neg_risk`, `fetched_at`.

## Ordering

Within a run, `seq` is the order of writing. On one connection that is the order of arrival. Records from different producers may have `recv_ts` slightly out of `seq` order. Runs do not overlap, except manual snapshot runs.

## State files

`state/status.json` is rewritten every 5 s (see `src/status/status_file.rs`; `archiver status` reads it). `state/tracked_tokens.json` lists the tokens tracked, rewritten when the set changes. Both are written to a temporary file and then renamed.
