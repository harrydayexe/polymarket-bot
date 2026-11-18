set shell := ["bash", "-euo", "pipefail", "-c"]

# Polymarket API data archiver (Rust)
[group("apps")]
mod archiver

# Shared Rust library: books, markets, archive format, units, print normalisation
[group("crates")]
mod core 'crates/pm-core'

# Live paper-trading system: ingest, engine (Rust)
[group("apps")]
mod live 'crates/pm-live'

# List all recipes, including app modules
[private]
default:
    @just --list --list-submodules

# Run CI checks for every app
[group("ci")]
ci: archiver::ci core::ci live::ci

# Run tests for every app
[group("ci")]
test: archiver::test core::test live::test

# Lint every app
[group("ci")]
lint: archiver::lint core::lint live::lint

# Format every app
[group("ci")]
fmt: archiver::fmt core::fmt live::fmt
