# archiver

Rust CLI that archives Polymarket API data.

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
