//! corral-sched: the scheduler (job state in SQLite, placement, leases, HTTP for bulk bytes).

pub mod gc;
pub mod http;
pub mod nats;
pub mod service;
pub mod settings;
pub mod store;

use std::sync::atomic::{AtomicI64, Ordering};

/// Time source. The scheduler's clock alone defines lease times (PROTOCOL.md 6.3).
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
    }
}

/// A manually advanced clock for tests.
pub struct FakeClock(AtomicI64);

impl FakeClock {
    pub fn new(start_ms: i64) -> FakeClock {
        FakeClock(AtomicI64::new(start_ms))
    }
    pub fn advance_ms(&self, ms: i64) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }
    pub fn advance_s(&self, s: i64) {
        self.advance_ms(s * 1000);
    }
}

impl Clock for FakeClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests;
