//! The worker's view of the host guard (GUARDS.md 1, 6): "the worker never kills for resource reasons; it asks the
//! guard". The trait lives here so the worker does not depend on the guard crate; `corral-guard` implements it over
//! the local socket and the CLI wires the two together.

use corral_proto::api::{Blocked, DeclineReason};
use corral_proto::job::Class;
use std::sync::Mutex;

/// The guard's answer to an admission request.
#[derive(Debug, Clone, Default)]
pub struct GuardAnswer {
    pub ok: bool,
    pub reservation: Option<String>,
    /// Every failing check, the first being the headline.
    pub reasons: Vec<DeclineReason>,
}

/// What the worker tells the guard about a started job.
#[derive(Debug, Clone)]
pub struct GuardReg {
    pub name: String,
    pub pid: u32,
    pub pgid: u32,
    pub pid_start: String,
    pub tier: Option<u32>,
    pub budget_mb: Option<u64>,
    pub pausable: bool,
    pub heavy: bool,
    pub reservation: Option<String>,
}

/// One action event from the guard's `subscribe` stream (`kill`, `pause`, `resume_unit`, ...).
#[derive(Debug, Clone)]
pub struct GuardEvent {
    pub kind: String,
    /// The registration name of the unit (`corral:<job>:<attempt>`), when the decision is about one.
    pub unit: Option<String>,
    pub reason: String,
    pub detail: String,
}

pub trait HostGuard: Send + Sync + 'static {
    /// Blocking. `reservation` re-checks as if that reservation were not holding RAM and a slot (the check under the
    /// lock). `Err` = the guard cannot be reached: the caller fails closed.
    fn admit(&self, name: &str, ram_mb: u64, class: Class, reserve_s: u64, reservation: Option<&str>) -> Result<GuardAnswer, String>;
    /// The failing checks of the `default` profile right now, with `since` (R-ADM-04 waiting reasons).
    fn blocked(&self) -> Result<Vec<Blocked>, String>;
    fn register(&self, r: &GuardReg) -> Result<String, String>;
    fn unregister(&self, id: &str);
    fn unreserve(&self, token: &str);
    /// Deliver action events until `on` returns `false`; reconnects while the guard restarts. Blocking: run it on its
    /// own thread.
    fn watch(&self, on: &mut dyn FnMut(GuardEvent) -> bool);
}

/// A scripted guard for tests of the worker.
#[derive(Default)]
pub struct FakeGuard {
    /// Failing checks the next `admit` answers with (empty = admitted).
    pub refuse: Mutex<Vec<DeclineReason>>,
    /// Failing checks only the second `admit` (the one under the lock, carrying a reservation) answers with.
    pub refuse_under_lock: Mutex<Vec<DeclineReason>>,
    /// When set, every call fails as if the guard were unreachable.
    pub down: Mutex<Option<String>>,
    pub calls: Mutex<Vec<String>>,
    pub registered: Mutex<Vec<GuardReg>>,
    pub blocked: Mutex<Vec<Blocked>>,
    pub events: Mutex<Vec<GuardEvent>>,
}

impl FakeGuard {
    pub fn new() -> std::sync::Arc<FakeGuard> {
        std::sync::Arc::new(FakeGuard::default())
    }
    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
    fn note(&self, s: String) -> Result<(), String> {
        self.calls.lock().unwrap_or_else(|e| e.into_inner()).push(s);
        match &*self.down.lock().unwrap_or_else(|e| e.into_inner()) {
            Some(w) => Err(w.clone()),
            None => Ok(()),
        }
    }
}

impl HostGuard for FakeGuard {
    fn admit(&self, name: &str, ram_mb: u64, _class: Class, reserve_s: u64, reservation: Option<&str>) -> Result<GuardAnswer, String> {
        self.note(format!("admit {name} {ram_mb} reserve={reserve_s} under={}", reservation.unwrap_or("-")))?;
        let mut why = self.refuse.lock().unwrap_or_else(|e| e.into_inner()).clone();
        if reservation.is_some() {
            why.extend(self.refuse_under_lock.lock().unwrap_or_else(|e| e.into_inner()).clone());
        }
        Ok(GuardAnswer { ok: why.is_empty(), reservation: (why.is_empty() && reserve_s > 0).then(|| "tok-1".to_string()), reasons: why })
    }
    fn blocked(&self) -> Result<Vec<Blocked>, String> {
        self.note("blocked".into())?;
        Ok(self.blocked.lock().unwrap_or_else(|e| e.into_inner()).clone())
    }
    fn register(&self, r: &GuardReg) -> Result<String, String> {
        self.note(format!("register {}", r.name))?;
        self.registered.lock().unwrap_or_else(|e| e.into_inner()).push(r.clone());
        Ok(format!("reg-{}", r.name))
    }
    fn unregister(&self, id: &str) {
        let _ = self.note(format!("unregister {id}"));
    }
    fn unreserve(&self, token: &str) {
        let _ = self.note(format!("unreserve {token}"));
    }
    fn watch(&self, on: &mut dyn FnMut(GuardEvent) -> bool) {
        let evs: Vec<GuardEvent> = std::mem::take(&mut *self.events.lock().unwrap_or_else(|e| e.into_inner()));
        for e in evs {
            if !on(e) {
                return;
            }
        }
    }
}
