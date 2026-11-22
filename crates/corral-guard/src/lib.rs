//! corral-guard: the per-host guard (GUARDS.md). The guard samples the host, computes levels, resolves the
//! processes it watches, answers admission requests, keeps the kill-switch state and the host-lock helpers, and
//! decides. In `observe` mode (the default) it only logs what it *would* do. In `enforce` mode (opt-in per host,
//! M3) `enforce.rs` carries the decisions out: it is the only module with signalling code, and
//! `tests/no_signals.rs` keeps it that way.

pub mod admit;
pub mod client;
pub mod compare;
pub mod decisions;
pub mod enforce;
pub mod engine;
pub mod guard;
pub mod levels;
pub mod lockcli;
pub mod procs;
pub mod sample;
pub mod server;
pub mod settings;
pub mod state;

pub mod testing;
