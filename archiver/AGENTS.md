# archiver

Rust CLI that archives Polymarket API data.

## Docs
There are some doc files present in the docs/ directory. These are symlinks into my obsidian vault.

Code shared with the live system lives in the `pm-core` crate (`../crates/pm-core`): order books, clock and randomness, market parsing/selection/registry planning, and the archive's layout, record types, line format and reader. The archiver re-imports these under their old module paths (`crate::book`, `crate::market_registry::registry`, `crate::archive::envelope`, ...), so most archiver code is unchanged. Test helpers it needs from there come from `pm_core::test_util` (feature `test-util`, enabled in dev-dependencies). The archive format must not change: `commands::run::tests::archive_format_matches_the_fixture` compares a short run against `tests/fixtures/archive_format.txt`.

We are now using the marcasite crate for connection to the Polymarket APIs, and some of the types (such as TokenId) are defined in marcasite rather than in the application itself.

When implementing features from the design document, prefer to use a Rust idiomatic approach when possible, as the design doc is language agnostic and does not make attempts to design specifically for rust.

## Commands

Tasks run through [just](https://just.systems). Commands below are run from the repo root; from inside `archiver/`, drop the `archiver` prefix (e.g. `just test`). Run `just --list --list-submodules` for everything.

| Command | Purpose |
|---|---|
| `just archiver build` | Build a debug binary |
| `just archiver check` | Type-check without producing a binary |
| `just archiver test [args]` | Run the test suite; extra args go to `cargo test` |
| `just archiver lint` | Lint with clippy, warnings as errors |
| `just archiver fmt` | Format all code |
| `just archiver run <subcommand> [args]` | Run the CLI via `cargo run`, e.g. `just archiver run gaps --help` |
| `just archiver ci` | Format check, lint and tests |

Prefer these recipes over the raw underlying commands.
