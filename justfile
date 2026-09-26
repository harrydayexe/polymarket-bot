set shell := ["bash", "-euo", "pipefail", "-c"]

# Polymarket API data archiver (Rust)
[group("apps")]
mod archiver

# List all recipes, including app modules
[private]
default:
    @just --list --list-submodules

# Run CI checks for every app
[group("ci")]
ci: archiver::ci

# Run tests for every app
[group("ci")]
test: archiver::test

# Lint every app
[group("ci")]
lint: archiver::lint

# Format every app
[group("ci")]
fmt: archiver::fmt
