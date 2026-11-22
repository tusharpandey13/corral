//! Local admission (GUARDS.md 4, 7.4). In M1 the worker applies the `default` checks itself: kill/pause state,
//! memory projection, heavy slot and the host lock. The same decision is made twice: before the claim, and again
//! while holding the lock (R-ADM-01, R-ADM-02). A decline never counts as an attempt (R-ADM-03).

use crate::lock::LockStore;
use crate::probe::MemoryProbe;
use corral_proto::api::{Admission, Blocked, DeclineReason, Slots};
use corral_proto::job::Class;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct AdmitConfig {
    pub heavy_slots: u32,
    pub light_slots: u32,
    pub ram_cap_mb: Option<u64>,
    pub admit_floor_mb: Option<u64>,
    pub lock_ttl_s: u64,
}

#[derive(Debug, Clone)]
pub struct Need {
    pub id: String,
    /// The attempt number: two attempts of one job (an old one still cleaning up, a new one claimed) never share a
    /// slot, a reservation or a lock owner name.
    pub attempt: u32,
    pub class: Class,
    pub ram_mb: u64,
    /// `auto` (the host's heavy lock), `none`, or a name (M1 has one lock per host).
    pub lock: String,
    pub pid: u32,
}

#[derive(Default)]
struct State {
    running: HashMap<(String, u32), Entry>,
    paused: Option<String>,
    draining: bool,
}

struct Entry {
    class: Class,
    ram_mb: u64,
    footprint_mb: u64,
}

pub struct Admitter {
    cfg: AdmitConfig,
    mem: Arc<dyn MemoryProbe>,
    lock: Arc<dyn LockStore>,
    st: Arc<Mutex<State>>,
}

/// Proof of admission. Dropping it frees the slot and releases the lock on every exit path.
pub struct Ticket {
    key: (String, u32),
    agent: Option<String>,
    lock: Arc<dyn LockStore>,
    st: Arc<Mutex<State>>,
}

impl Ticket {
    pub fn holds_lock(&self) -> bool {
        self.agent.is_some()
    }
    pub fn lock_agent(&self) -> Option<&str> {
        self.agent.as_deref()
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        if let Some(a) = &self.agent {
            self.lock.release(a);
        }
        self.st.lock().unwrap_or_else(|e| e.into_inner()).running.remove(&self.key);
    }
}

fn reason(check: &str, text: impl Into<String>) -> DeclineReason {
    DeclineReason { check: check.into(), reason: text.into() }
}

impl Admitter {
    pub fn new(cfg: AdmitConfig, mem: Arc<dyn MemoryProbe>, lock: Arc<dyn LockStore>) -> Admitter {
        Admitter { cfg, mem, lock, st: Arc::new(Mutex::new(State::default())) }
    }

    fn st(&self) -> std::sync::MutexGuard<'_, State> {
        self.st.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn pause(&self, why: &str) {
        self.st().paused = Some(why.to_string());
    }
    pub fn resume(&self) {
        let mut s = self.st();
        s.paused = None;
        s.draining = false;
    }
    pub fn drain(&self) {
        self.st().draining = true;
    }
    pub fn paused(&self) -> Option<String> {
        self.st().paused.clone()
    }
    pub fn draining(&self) -> bool {
        self.st().draining
    }
    pub fn running_ids(&self) -> Vec<String> {
        let mut v: Vec<String> = self.st().running.keys().map(|(id, _)| id.clone()).collect();
        v.sort();
        v.dedup();
        v
    }

    pub fn update_footprint(&self, id: &str, attempt: u32, mb: u64) {
        if let Some(e) = self.st().running.get_mut(&(id.to_string(), attempt)) {
            e.footprint_mb = mb;
        }
    }

    fn floor_mb(&self, total: u64) -> u64 {
        self.cfg.admit_floor_mb.unwrap_or(total / 5)
    }

    /// Declared-but-not-yet-used memory of running jobs: `max(0, ram - footprint)` each.
    fn committed(st: &State) -> u64 {
        st.running.values().map(|e| e.ram_mb.saturating_sub(e.footprint_mb)).sum()
    }

    fn slots(&self, st: &State) -> Slots {
        let heavy = st.running.values().filter(|e| e.class == Class::Heavy).count() as u32;
        let light = st.running.values().filter(|e| e.class == Class::Light).count() as u32;
        Slots { heavy_free: self.cfg.heavy_slots.saturating_sub(heavy), light_free: self.cfg.light_slots.saturating_sub(light) }
    }

    /// What this worker tells the scheduler (claims and presence).
    pub fn summary(&self) -> (Slots, Admission) {
        let st = self.st();
        let slots = self.slots(&st);
        let mut blocked = Vec::new();
        let mut admissible = 0;
        match self.mem.read() {
            Ok(m) => {
                let floor = self.floor_mb(m.total_mb);
                admissible = m.available_mb.saturating_sub(Self::committed(&st)).saturating_sub(floor);
                if let Some(cap) = self.cfg.ram_cap_mb {
                    admissible = admissible.min(cap);
                }
            }
            Err(e) => blocked.push(Blocked { check: "memory".into(), profile: None, reason: format!("unknown ({e})"), since: None }),
        }
        let ours = st.running.values().any(|e| e.class == Class::Heavy);
        if !ours && let Some(h) = self.lock.holder() {
            blocked.push(Blocked { check: "lock".into(), profile: None, reason: h, since: None });
        }
        if let Some(p) = &st.paused {
            blocked.push(Blocked { check: "queue_not_paused".into(), profile: None, reason: p.clone(), since: None });
        }
        let heavy_ok = blocked.is_empty() && admissible > 0;
        (slots, Admission { heavy_ok, exclusive_ok: false, profiles_ok: vec!["default".into()], admissible_ram_mb: admissible, blocked })
    }

    fn check(&self, need: &Need, st: &State) -> Result<(), Vec<DeclineReason>> {
        let mut why = Vec::new();
        if let Some(p) = &st.paused {
            why.push(reason("queue_not_paused", p.clone()));
        }
        let slots = self.slots(st);
        match need.class {
            Class::Heavy if slots.heavy_free == 0 => why.push(reason("heavy_slot", "no free heavy slot")),
            Class::Light if slots.light_free == 0 => why.push(reason("light_slot", "no free light slot")),
            _ => {}
        }
        match self.mem.read() {
            Ok(m) => {
                let floor = self.floor_mb(m.total_mb);
                let after = m.available_mb as i64 - Self::committed(st) as i64 - need.ram_mb as i64;
                if after < floor as i64 {
                    why.push(reason(
                        "memory",
                        format!(
                            "available {} MB - committed {} MB - job {} MB = {} MB, below the admit floor {} MB",
                            m.available_mb,
                            Self::committed(st),
                            need.ram_mb,
                            after,
                            floor
                        ),
                    ));
                }
                if let Some(cap) = self.cfg.ram_cap_mb
                    && need.ram_mb > cap
                {
                    why.push(reason("memory", format!("job needs {} MB, above admissible_ram_cap {cap} MB", need.ram_mb)));
                }
            }
            Err(e) => why.push(reason("memory", format!("unknown ({e})"))),
        }
        if why.is_empty() { Ok(()) } else { Err(why) }
    }

    /// Admit a job or return the reasons it cannot start now. The full check runs a second time while holding the
    /// host lock; if it fails there, the lock is released and nothing starts (R-ADM-01).
    pub fn try_start(&self, need: &Need) -> Result<Ticket, Vec<DeclineReason>> {
        // First check (cheap, no lock).
        {
            let st = self.st();
            self.check(need, &st)?;
        }
        let wants_lock = need.class == Class::Heavy && need.lock != "none";
        let agent = format!("corral:{}:{}", need.id, need.attempt);
        if wants_lock && let Err(busy) = self.lock.acquire(&agent, "corral job", need.pid, self.cfg.lock_ttl_s) {
            return Err(vec![reason("lock", busy.0)]);
        }
        // Second check, under the lock, and the reservation in the same critical section.
        let mut st = self.st();
        if let Err(why) = self.check(need, &st) {
            if wants_lock {
                self.lock.release(&agent);
            }
            return Err(why);
        }
        let key = (need.id.clone(), need.attempt);
        st.running.insert(key.clone(), Entry { class: need.class, ram_mb: need.ram_mb, footprint_mb: 0 });
        Ok(Ticket { key, agent: wants_lock.then_some(agent), lock: self.lock.clone(), st: self.st.clone() })
    }

    /// Keep the host lock alive (`ttl / 3`) while the attempt runs (R-LOCK-03).
    pub fn renew_agent(&self, agent: Option<&str>) {
        if let Some(a) = agent {
            let _ = self.lock.renew(a);
        }
    }

    pub fn lock_ttl_s(&self) -> u64 {
        self.cfg.lock_ttl_s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lock::FakeLock;
    use crate::testing::FakeMemory;

    fn cfg() -> AdmitConfig {
        AdmitConfig { heavy_slots: 1, light_slots: 2, ram_cap_mb: None, admit_floor_mb: Some(2000), lock_ttl_s: 300 }
    }

    fn need(id: &str, ram: u64) -> Need {
        Need { id: id.into(), attempt: 1, class: Class::Heavy, ram_mb: ram, lock: "auto".into(), pid: 1 }
    }

    fn rig(mem: Arc<FakeMemory>) -> (Admitter, Arc<FakeLock>) {
        let lock = Arc::new(FakeLock::default());
        (Admitter::new(cfg(), mem, lock.clone()), lock)
    }

    /// U-03 / R-ADM-02: admit exactly at the floor, refuse one MiB below.
    #[test]
    fn projection_boundary_is_exact() {
        let mem = FakeMemory::constant(10_000, 6_000);
        let (a, _) = rig(mem.clone());
        assert!(a.try_start(&need("j-1", 4_000)).is_ok(), "6000 - 4000 = 2000 = floor: admitted");
        let mem = FakeMemory::constant(10_000, 5_999);
        let (a, lock) = rig(mem);
        let e = a.try_start(&need("j-1", 4_000)).err().unwrap();
        assert_eq!(e[0].check, "memory");
        assert!(lock.is_free(), "a refused admission never leaves the lock held");
    }

    /// U-11 / R-ADM-01: a check that flips between lock acquire and re-check declines, and the lock is released.
    #[test]
    fn recheck_under_the_lock_declines_and_releases() {
        let mem = FakeMemory::scripted(10_000, &[8_000, 1_000]); // pre-check ok, re-check (under lock) bad
        let (a, lock) = rig(mem);
        let e = a.try_start(&need("j-1", 1_000)).err().expect("declined");
        assert_eq!(e[0].check, "memory");
        assert!(lock.is_free(), "lock released after the failed re-check");
        assert!(a.running_ids().is_empty(), "no slot consumed");
        assert!(lock.log.lock().unwrap().iter().any(|l| l.starts_with("acquire")), "the lock WAS taken before the re-check");
    }

    #[test]
    fn one_heavy_job_per_host_and_light_jobs_still_fit() {
        let (a, lock) = rig(FakeMemory::constant(10_000, 9_000));
        let t = a.try_start(&need("j-1", 1_000)).unwrap();
        assert!(t.holds_lock());
        let e = a.try_start(&need("j-2", 1_000)).err().unwrap();
        assert!(e.iter().any(|r| r.check == "heavy_slot"));
        let light = Need { id: "j-3".into(), attempt: 1, class: Class::Light, ram_mb: 100, lock: "auto".into(), pid: 1 };
        let lt = a.try_start(&light).unwrap();
        assert!(!lt.holds_lock(), "light jobs take no host lock");
        drop(t);
        assert!(lock.is_free(), "dropping the ticket releases the lock");
        assert!(a.try_start(&need("j-2", 1_000)).is_ok());
    }

    /// R-SCHED-05 / R-ADM-01 (M1 review): two attempts of the same job id never share a slot or a reservation:
    /// cleaning up the old attempt must not free what the new one holds.
    #[test]
    fn attempts_of_one_job_do_not_share_slots() {
        let (a, _) = rig(FakeMemory::constant(10_000, 9_000));
        let light = |attempt| Need { id: "j-1".into(), attempt, class: Class::Light, ram_mb: 100, lock: "auto".into(), pid: 1 };
        let old = a.try_start(&light(1)).unwrap();
        let new = a.try_start(&light(2)).unwrap();
        assert_eq!(a.summary().0.light_free, 0, "both attempts hold a slot");
        drop(old); // the stale attempt finishes its cleanup late
        assert_eq!(a.summary().0.light_free, 1, "the new attempt still holds its slot");
        assert_eq!(a.running_ids(), vec!["j-1".to_string()]);
        a.update_footprint("j-1", 1, 999); // a late update from the old attempt touches nothing
        drop(new);
        assert!(a.running_ids().is_empty());
    }

    /// The host lock is owned per attempt: an old attempt's release cannot free the lock a new attempt holds.
    #[test]
    fn lock_owner_names_include_the_attempt() {
        let (a, lock) = rig(FakeMemory::constant(10_000, 9_000));
        let t = a.try_start(&Need { attempt: 2, ..need("j-1", 100) }).unwrap();
        assert_eq!(t.lock_agent(), Some("corral:j-1:2"));
        lock.release("corral:j-1:1"); // what the old attempt's teardown would do
        assert!(!lock.is_free(), "the new attempt's lock survives the old attempt's release");
        drop(t);
        assert!(lock.is_free());
    }

    #[test]
    fn busy_lock_declines_with_the_holder_named() {
        let (a, lock) = rig(FakeMemory::constant(10_000, 9_000));
        lock.hold_foreign("other-agent");
        let e = a.try_start(&need("j-1", 100)).err().unwrap();
        assert_eq!(e[0].check, "lock");
        assert!(e[0].reason.contains("other-agent"));
        let (_, adm) = a.summary();
        assert!(!adm.heavy_ok && adm.blocked.iter().any(|b| b.check == "lock"));
    }

    #[test]
    fn committed_memory_counts_until_the_job_uses_it() {
        let (a, _) = rig(FakeMemory::constant(10_000, 8_000));
        let cfg2 = AdmitConfig { heavy_slots: 2, ..cfg() };
        let lock2 = Arc::new(FakeLock::default());
        let a2 = Admitter::new(cfg2, FakeMemory::constant(10_000, 8_000), lock2);
        let _t = a2.try_start(&Need { lock: "none".into(), ..need("j-1", 5_000) }).unwrap();
        // 8000 available but 5000 is promised to j-1 and not used yet: only 1000 above the floor remains
        let (_, adm) = a2.summary();
        assert_eq!(adm.admissible_ram_mb, 8_000 - 5_000 - 2_000);
        assert!(a2.try_start(&Need { lock: "none".into(), ..need("j-2", 2_000) }).is_err());
        a2.update_footprint("j-1", 1, 5_000);
        assert_eq!(a2.summary().1.admissible_ram_mb, 8_000 - 2_000, "once used, MemAvailable already reflects it");
        drop(a);
    }

    #[test]
    fn unknown_memory_fails_closed_and_paused_workers_refuse() {
        let (a, _) = rig(FakeMemory::failing("no /proc"));
        let e = a.try_start(&need("j-1", 1)).err().unwrap();
        assert!(e[0].reason.contains("unknown"));
        let (a, _) = rig(FakeMemory::constant(10_000, 9_000));
        a.pause("exit code 4 of j-0");
        assert!(a.try_start(&need("j-1", 1)).is_err());
        a.resume();
        assert!(a.try_start(&need("j-1", 1)).is_ok());
    }

    #[test]
    fn ram_cap_limits_what_is_admissible() {
        let lock = Arc::new(FakeLock::default());
        let a = Admitter::new(AdmitConfig { ram_cap_mb: Some(1_000), ..cfg() }, FakeMemory::constant(64_000, 60_000), lock);
        assert_eq!(a.summary().1.admissible_ram_mb, 1_000);
        assert!(a.try_start(&need("j-1", 1_500)).is_err());
    }
}
