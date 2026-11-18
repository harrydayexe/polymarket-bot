# fixtures

Golden test files shared by the Rust, OCaml and Python code. A change to a contract updates the fixture first, then every implementation, in one change.

| Folder | Contract | Used by |
| --- | --- | --- |
| `tier1/` | §3 shared definitions: normalising prints, grouping, Tier 1 measurement | `pm-core` (`normalize::grouper` tests), C3, P2 |
| `scoring/` | The C5 score formula | C5, P2 (not yet written) |
| `protocol/` | Strategy protocol v1 messages | C6, O1 (not yet written) |
| `books/` | §3.4 YES/NO books are mirrors (`mirror_yes_no.json`, a live pair) | `pm-live` (`engine::books` tests), C3 |
| `fees/` | §3.7 taker fee and rounding (`taker_fee.json`); fallback lookup in `config/fee_schedule_history.toml` (`fallback.json`) | `pm-core` (`units`, `fees` tests), C7, P2, P3 |

Each file states its own conventions (times are integer microseconds since the epoch, decimals are strings compared as numbers).
