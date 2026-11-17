# pm-core

Shared library for the Polymarket archiver and the live paper-trading system: order books, injected clock and randomness, market selection and registry planning, the archive's format and reader, money and probability units, and print normalisation (prints as buys of one outcome, grouping into sweeps, rolling pair volume).

```toml
[dependencies]
pm-core = { path = "../crates/pm-core" }

[dev-dependencies]
pm-core = { path = "../crates/pm-core", features = ["test-util"] }
```

The `test-util` feature adds `TokioClock`, `ScriptedRandom` and the market fixtures in `pm_core::test_util` for other crates' tests.

See `AGENTS.md` for the module list and commands, and `cargo doc --open -p pm-core` for the API.
