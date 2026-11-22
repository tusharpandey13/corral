//! corral-guard: the per-host guard (GUARDS.md). Milestone M2: **observe mode only**. The guard samples the host,
//! computes levels, resolves the processes it watches, answers admission requests, keeps the kill-switch state and
//! the host-lock helpers, and logs every decision it *would* take. It never signals a process: there is no signalling
//! code in this crate, and `tests/no_signals.rs` keeps it that way until enforce mode (M3) adds it deliberately.

pub mod admit;
pub mod client;
pub mod compare;
pub mod decisions;
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
