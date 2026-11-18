# pm-core

Shared Rust library for the archiver and the live paper-trading system (component C1 of the Phase 1 spec). Library only, no binary.

| Module | Contents |
| --- | --- |
| `book` | `OrderBook` rebuilt from `book`/`price_change`; `depth_within`, `walk`, `mid` |
| `clock` | `Clock`, `SharedClock`, `SystemClock`; `TokioClock` (feature `test-util`) |
| `random` | `RandomSource`, `SystemRandom`, `SeededRandom` (ChaCha20); `ScriptedRandom` (feature `test-util`) |
| `market` | `MarketInfo`, `MarketTokens`, `qualifies`, `sanity_check`, `Registry` planning |
| `archive` | Archive layout, record types (`Record`, `ControlBody`, ids), line format, reader |
| `units` | `Price`, `Shares`, `Usd`, `Probability`, `Logit`, `logit`/`inv_logit`; `FeeParams`, `FeeSource`, `fee_per_share` (unrounded), `fee_charged` (5 dp half-even), both over `marcasite_core::fees::taker_fee` |
| `fees` | `FeeTable`: the dated fee table `config/fee_schedule_history.toml` (repo root), `lookup(category, at)`, `fallback(tags, at)` |
| `normalize` | `NormalisedPrint` (§3.2), `Grouper` (§3.3), `PairVolume` (§3.6) |
| `client` | `Endpoints`, `ClientSettings` + `build_clients` (archiver: one client, no budget), `HttpConfig` + `Clients::build` (live: `meta`, `data_priority`, `data_bulk` budgets via marcasite `RateLimit`) |
| `invariant` | `invariant!` macro and `InvariantViolation` |
| `test_util` | Fixtures for other crates' tests (feature `test-util`) |

Rate limiting, retries, websocket reconnection and the fee formula are marcasite's (0.3, declared once in the root `[workspace.dependencies]`); do not re-implement them here. `MarketInfo.fees_enabled`/`fee_params` are `#[serde(skip)]` so the archive's market records do not change.

Code moved from the archiver keeps its behaviour: the archive format must not change (the archiver's `archive_format_matches_the_fixture` test checks it). Fees are checked against `fixtures/fees/*.json`. `normalize::grouper` is checked against `fixtures/tier1/grouping.json` at the repo root, which the Python research code also uses; change the fixture first, then both sides.

## Code Style
The code's assertions density should average to minimally two assertions per function. Assertions must be used to check for anomalous conditions that should never happen in real-life executions. Assertions must be side-effect free and should be defined as Boolean tests. When an assertion fails, an explicit recovery action must be taken such as returning an error condition to the caller of the function that executes the failing assertion. Any assertion for which a static checking tool can prove that it can never fail or never hold violates this rule.

Use `invariant!(condition, recovery)` from `src/invariant.rs` for these: it logs the failed condition and returns `recovery` (usually `Err(InvariantViolation("…").into())`).

No floats for money: prices, sizes and amounts are `units::{Price, Shares, Usd}` over `Decimal`, rounded half-even to 6 places only at the end of an expression.

## Commands

Commands below are run from the repo root; from inside `crates/pm-core/`, drop the `core` prefix (e.g. `just test`). Recipes build with `--all-features` so the `test-util` helpers are checked too.

| Command | Purpose |
|---|---|
| `just core build` | Build the library |
| `just core check` | Type-check all targets |
| `just core doc` | Build the API documentation |
| `just core test [args]` | Run the tests; extra args go to `cargo test` |
| `just core lint` | Lint with clippy, warnings as errors |
| `just core fmt` | Format all code |
| `just core ci` | Format check, lint and tests |

Prefer these recipes over the raw underlying commands.
