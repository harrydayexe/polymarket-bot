# Polymarket-Bot

Polyglot monorepo. Each application lives in its own top-level directory with its own justfile, mounted as a module in the root justfile. Rust code is one Cargo workspace (`Cargo.toml` at the root, members `archiver` and `crates/*`, one `Cargo.lock`, build output in `target/` at the root); shared Rust libraries live under `crates/`. Golden test files shared across languages live in `fixtures/`.

## Commands

Tasks run through [just](https://just.systems). Run `just --list --list-submodules` for everything, and `just --usage <recipe>` for arguments.

| Command | Purpose |
|---|---|
| `just ci` | Run CI checks (format check, lint, tests) for every app |
| `just test` | Run tests for every app |
| `just lint` | Lint every app |
| `just fmt` | Format every app |

Prefer these recipes over the raw underlying commands.

### Modules

| Module | Location | Example |
|---|---|---|
| `archiver` | `archiver/` | `just archiver test` |
| `core` | `crates/pm-core/` | `just core test` |
| `live` | `crates/pm-live/` | `just live test` |

Each module directory has its own AGENTS.md with module-specific commands. When adding an app or crate, add it to the workspace members if it is Rust, add a `mod <name>` to the root justfile and wire its recipes into the root `ci`, `test`, `lint` and `fmt` recipes.
