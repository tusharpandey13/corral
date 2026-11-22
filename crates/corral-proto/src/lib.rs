//! corral-proto: message schemas, subjects, job state machine, capability tokens, configuration.
//!
//! Everything here is pure (no I/O besides reading a config file), so the scheduler, worker and CLI share one
//! definition of the wire format (`docs/PROTOCOL.md`).

pub mod api;
pub mod config;
pub mod envelope;
pub mod guardcfg;
pub mod ident;
pub mod job;
pub mod perms;
pub mod progress;
pub mod state;
pub mod subjects;
pub mod token;
pub mod util;

/// Wire protocol version spoken by this build (PROTOCOL.md section 2).
pub const PROTOCOL_VERSION: u32 = 1;
/// Protocol versions this build accepts.
pub const SUPPORTED_VERSIONS: [u32; 1] = [1];
pub mod bus;
