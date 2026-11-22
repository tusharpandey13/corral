//! corral-worker: admission, runtime adapter, log publishing and the claim/run loop.

pub mod admission;
pub mod attempt;
pub mod input;
pub mod link;
pub mod lock;
pub mod logs;
pub mod native;
pub mod probe;
pub mod run;
pub mod runtime;
pub mod settings;
pub mod testing;
pub mod worker;
