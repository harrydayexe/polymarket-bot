# Polymarket-Bot

Polyglot monorepo. Each application lives in its own top-level directory with its own justfile, mounted as a module in the root justfile.

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

Each module directory has its own AGENTS.md with module-specific commands. When adding an app, add a `mod <name>` to the root justfile and wire its recipes into the root `ci`, `test`, `lint` and `fmt` recipes.
