# fixtures

Golden test files shared by the Rust, OCaml and Python code. A change to a contract updates the fixture first, then every implementation, in one change.

| Folder | Contract | Used by |
| --- | --- | --- |
| `tier1/` | §3 shared definitions: normalising prints, grouping, Tier 1 measurement | `pm-core` (`normalize::grouper` tests), C3, P2 |
| `scoring/` | The C5 score formula | C5, P2 (not yet written) |
| `protocol/` | Strategy protocol v1 messages | C6, O1 (not yet written) |

Each file states its own conventions (times are integer microseconds since the epoch, decimals are strings compared as numbers).
