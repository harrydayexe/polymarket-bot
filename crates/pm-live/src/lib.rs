//! The live paper-trading system (components C2–C9 of the Phase 1 spec). So far: live ingest
//! (C2) and the engine skeleton it feeds.
//!
//! Async I/O tasks turn the outside world into [event::Envelope]s; one engine task consumes
//! them in order through a pure, deterministic [engine::Engine::step] and returns
//! [action::Action]s for the I/O side to carry out. Every envelope is written to the
//! [engine_log], so a run can be replayed.
//!
//! | Module | Contents |
//! | --- | --- |
//! | [config] | `pmlive.toml`: endpoints, HTTP budgets, ingest and engine settings |
//! | [event] | Envelopes and the events the engine consumes |
//! | [action] | What the engine asks the I/O side to do, and the rows it stores |
//! | [engine] | The engine: books and trust, registry view and slots, rolling volume, timers |
//! | [ingest] | Websocket slots, the registry poll with fees, the volume backfill |
//! | [engine_log] | Hourly JSONL log of every envelope, compressed when the hour closes |
//! | [sink] | Where persisted rows go until the Postgres writer (C8) exists |
//! | [runtime] | Wiring, the engine loop, the action executor and shutdown |

pub mod action;
pub mod config;
pub mod engine;
pub mod engine_log;
pub mod event;
pub mod ingest;
pub mod runtime;
pub mod sink;

#[cfg(test)]
pub(crate) mod test_support;
