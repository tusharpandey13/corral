//! Enforce mode (GUARDS.md 3.3, R-GUARD-02..04, R-GUARD-10): the only place in the guard that sends a signal.
//!
//! The engine plans (`kill`, `pause`, `resume_unit`, `hook` decisions); this module carries them out, and only
//! against what the sample resolved. The rules, all enforced here and tested:
//!
//! * a pid is signalled only if it is in the planned unit **and** its start time right now equals the one the sample
//!   recorded (pid reuse); a pid without a recorded start time is not signalled (fail closed);
//! * pid 0/1, the guard itself, and units holding a `never` process are never signalled;
//! * a whole process group is signalled only when the unit *is* that group and its leader still verifies; the group
//!   must not be the guard's own;
//! * TERM, then KILL after `kill_grace` for the pids that still verify; nothing is looked up or killed by name;
//! * every pause is recorded on disk (`paused.json`, written atomically and before the first STOP; a pause that cannot
//!   be recorded is not sent) as key -> (group, pids with start times). The engine's paused set is reconciled against
//!   it every tick, so a unit that vanished from a sample, was escalated, or lost a race is still continued; entries
//!   that cannot be checked right now (`ps` failed) stay and are retried (note 43);
//! * start times are read with one batched `ps` call under a timeout; a failed or hung `ps` means "unknown", never
//!   "gone", and never a signal (note 42).

use crate::decisions::Decision;
use crate::procs::Unit;
use crate::settings::HookSettings;
use corral_proto::util::PipeDrain;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sig {
    Term,
    Kill,
    Stop,
    Cont,
}

/// The system calls the executor needs. Tests inject a fake; `RealSys` is the only implementation that signals.
pub trait Sys: Send + Sync {
    /// The start times (as `ps` prints them) of those of `pids` that exist right now. `Err` means the answer could not
    /// be had (`ps` failed or hung): the caller must treat that as unknown, not as "gone".
    fn starts(&self, pids: &[u32]) -> Result<HashMap<u32, String>, String>;
    fn send_pid(&self, pid: u32, sig: Sig) -> Result<(), String>;
    fn send_group(&self, pgid: u32, sig: Sig) -> Result<(), String>;
    /// The guard's own process group (never signalled).
    fn own_pgid(&self) -> u32;
}

pub struct RealSys {
    /// The `ps` program (tests point it at a fake that fails or hangs).
    pub ps: String,
    pub timeout: Duration,
}

impl Default for RealSys {
    fn default() -> RealSys {
        RealSys { ps: "ps".into(), timeout: Duration::from_secs(3) }
    }
}

fn rsig(s: Sig) -> rustix::process::Signal {
    match s {
        Sig::Term => rustix::process::Signal::TERM,
        Sig::Kill => rustix::process::Signal::KILL,
        Sig::Stop => rustix::process::Signal::STOP,
        Sig::Cont => rustix::process::Signal::CONT,
    }
}

/// `ps -o pid=,lstart=` lines -> pid -> normalised start time.
pub fn parse_pid_starts(text: &str) -> HashMap<u32, String> {
    text.lines()
        .filter_map(|l| {
            let mut it = l.split_whitespace();
            let pid: u32 = it.next()?.parse().ok()?;
            let start = it.collect::<Vec<_>>().join(" ");
            (!start.is_empty()).then_some((pid, start))
        })
        .collect()
}

impl Sys for RealSys {
    fn starts(&self, pids: &[u32]) -> Result<HashMap<u32, String>, String> {
        if pids.is_empty() {
            return Ok(HashMap::new());
        }
        let list = pids.iter().map(u32::to_string).collect::<Vec<_>>().join(",");
        let o = crate::sample::run_cmd_status(&self.ps, &["-o".into(), "pid=,lstart=".into(), "-p".into(), list], self.timeout)?;
        let got = parse_pid_starts(&String::from_utf8_lossy(&o.stdout));
        // `ps -p` exits 1 when some pid is missing but still prints the others; nothing printed and a message on stderr
        // (or any other status) is a failure, not "all gone"
        if o.status.success() || !got.is_empty() || (o.status.code() == Some(1) && o.stderr.iter().all(|b| b.is_ascii_whitespace())) {
            Ok(got)
        } else {
            Err(format!("{} exited with {}: {}", self.ps, o.status, String::from_utf8_lossy(&o.stderr).trim()))
        }
    }
    fn send_pid(&self, pid: u32, sig: Sig) -> Result<(), String> {
        let p = rustix::process::Pid::from_raw(pid as i32).ok_or("bad pid")?;
        rustix::process::kill_process(p, rsig(sig)).map_err(|e| e.to_string())
    }
    fn send_group(&self, pgid: u32, sig: Sig) -> Result<(), String> {
        let p = rustix::process::Pid::from_raw(pgid as i32).ok_or("bad pgid")?;
        rustix::process::kill_process_group(p, rsig(sig)).map_err(|e| e.to_string())
    }
    fn own_pgid(&self) -> u32 {
        rustix::process::getpgrp().as_raw_nonzero().get() as u32
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HookOutcome {
    pub code: Option<i32>,
    pub timed_out: bool,
    pub tail: String,
}

pub trait HookRunner: Send + Sync {
    fn run(&self, cmd: &[String], timeout: Duration) -> Result<HookOutcome, String>;
}

/// Runs a restore hook with a clean environment as the guard's user; at its timeout the hook's own process group
/// (which the guard created for it) is ended. R-GUARD-07.
pub struct RealHooks;

impl HookRunner for RealHooks {
    fn run(&self, cmd: &[String], timeout: Duration) -> Result<HookOutcome, String> {
        use std::os::unix::process::CommandExt;
        let (prog, args) = cmd.split_first().ok_or("empty hook command")?;
        let mut child = std::process::Command::new(prog)
            .args(args)
            .env_clear()
            .env("PATH", "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .process_group(0)
            .spawn()
            .map_err(|e| format!("cannot run {prog}: {e}"))?;
        let pgid = child.id();
        let readers: Vec<_> =
            [child.stdout.take().map(PipeDrain::spawn), child.stderr.take().map(PipeDrain::spawn)].into_iter().flatten().collect();
        let deadline = std::time::Instant::now() + timeout;
        let (status, timed_out) = loop {
            match child.try_wait() {
                Ok(Some(s)) => break (Some(s), false),
                Ok(None) if std::time::Instant::now() >= deadline => {
                    // our own child's group, recorded by us at spawn
                    if let Some(p) = rustix::process::Pid::from_raw(pgid as i32) {
                        let _ = rustix::process::kill_process_group(p, rustix::process::Signal::KILL);
                    }
                    let _ = child.wait();
                    break (None, true);
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(20)),
                Err(e) => return Err(e.to_string()),
            }
        };
        // a background process the hook left behind may hold the pipes open: wait for the output only briefly
        let until = std::time::Instant::now() + Duration::from_millis(300);
        let mut all = Vec::new();
        for h in readers {
            all.extend(h.finish(until.saturating_duration_since(std::time::Instant::now())));
        }
        let text = String::from_utf8_lossy(&all).to_string();
        let tail: String = text.chars().rev().take(300).collect::<Vec<_>>().into_iter().rev().collect();
        Ok(HookOutcome { code: status.and_then(|s| s.code()), timed_out, tail: tail.trim().to_string() })
    }
}

/// Decisions produced by background work (kill escalation, hook results) wait here until the guard logs them.
pub type Outbox = Arc<Mutex<Vec<Decision>>>;

static EMPTY: String = String::new();

/// What one pause stopped: the unit's key, its group when the unit is that group, and the pids with the start times
/// they had when they were stopped.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct PausedEntry {
    key: String,
    pgid: Option<u32>,
    pids: Vec<(u32, String)>,
}

pub struct Executor {
    sys: Arc<dyn Sys>,
    hooks: Arc<dyn HookRunner>,
    grace: Duration,
    me: u32,
    outbox: Outbox,
    inflight: Arc<Mutex<HashSet<String>>>,
    paused_file: PathBuf,
    paused: Mutex<Vec<PausedEntry>>,
    level: Mutex<String>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Read `paused.json`: the current format, or the first M3 format (a bare list of pids, continued under the key "").
/// A file that exists but cannot be read is reported, never treated as empty without a word.
fn load_paused(path: &std::path::Path) -> (Vec<PausedEntry>, Option<String>) {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return (vec![], None),
        Err(e) => return (vec![], Some(format!("{}: {e}", path.display()))),
    };
    if let Ok(v) = serde_json::from_slice::<Vec<PausedEntry>>(&bytes) {
        return (v, None);
    }
    match serde_json::from_slice::<Vec<(u32, String)>>(&bytes) {
        Ok(old) if !old.is_empty() => (vec![PausedEntry { key: String::new(), pgid: None, pids: old }], None),
        Ok(_) => (vec![], None),
        Err(e) => {
            // keep the evidence for the owner; the pids in it cannot be recovered
            let _ = std::fs::rename(path, path.with_extension("json.corrupt"));
            (vec![], Some(format!("{} is not valid ({e}); kept as paused.json.corrupt", path.display())))
        }
    }
}

impl Executor {
    pub fn new(sys: Arc<dyn Sys>, hooks: Arc<dyn HookRunner>, grace: Duration, state_dir: &std::path::Path) -> Executor {
        let paused_file = state_dir.join("paused.json");
        let (paused, problem) = load_paused(&paused_file);
        let e = Executor {
            sys,
            hooks,
            grace,
            me: std::process::id(),
            outbox: Arc::new(Mutex::new(Vec::new())),
            inflight: Arc::new(Mutex::new(HashSet::new())),
            paused_file,
            paused: Mutex::new(paused),
            level: Mutex::new("ok".into()),
        };
        if let Some(p) = problem {
            tracing::error!("paused-process record unreadable, processes stopped by an earlier run may stay stopped: {p}");
            e.note(0, "skipped", "paused_state_unreadable", format!("the record of paused processes is unreadable: {p}"), None);
        }
        e
    }

    pub fn outbox(&self) -> Outbox {
        self.outbox.clone()
    }

    pub fn set_level(&self, level: &str) {
        *lock(&self.level) = level.to_string();
    }

    pub fn idle(&self) -> bool {
        lock(&self.inflight).is_empty()
    }

    fn note(&self, now: i64, kind: &str, reason: &str, detail: String, u: Option<&Unit>) {
        let mut d = Decision::new(now, kind, &lock(&self.level), reason, detail);
        if let Some(u) = u {
            d.tier = Some(u.tier);
            d.matcher = Some(u.matcher.clone());
            d.unit = Some(u.reg_name.clone().unwrap_or_else(|| u.key.clone()));
            d.pids = u.pids.clone();
            d.comm = Some(u.comm.clone());
        }
        lock(&self.outbox).push(d);
    }

    fn signable(&self, pid: u32) -> bool {
        pid > 1 && pid != self.me
    }

    /// The pids of the unit that may be signalled right now: still the process the sample saw. `Err`: the start times
    /// could not be read (nothing is signalled on a guess).
    fn verified(&self, u: &Unit) -> Result<Vec<(u32, String)>, String> {
        if u.has_never {
            return Ok(vec![]);
        }
        let cand: Vec<(u32, &String)> = u
            .pids
            .iter()
            .copied()
            .zip(u.starts.iter().chain(std::iter::repeat(&EMPTY)))
            .filter(|(p, s)| self.signable(*p) && !s.is_empty())
            .collect();
        if cand.is_empty() {
            return Ok(vec![]);
        }
        let now = self.sys.starts(&cand.iter().map(|(p, _)| *p).collect::<Vec<_>>())?;
        Ok(cand.into_iter().filter(|(p, s)| now.get(p) == Some(*s)).map(|(p, s)| (p, s.clone())).collect())
    }

    /// The group to signal as a whole: the unit is that group and its leader still verifies.
    fn group_of(&self, pgid: Option<u32>, ok: &[(u32, String)]) -> Option<u32> {
        let g = pgid?;
        (g > 1 && g != self.sys.own_pgid() && ok.iter().any(|(p, _)| *p == g)).then_some(g)
    }

    fn send(&self, pgid: Option<u32>, ok: &[(u32, String)], sig: Sig) {
        let group = self.group_of(pgid, ok);
        if let Some(g) = group {
            let _ = self.sys.send_group(g, sig);
        }
        for (pid, _) in ok {
            // members outside the whole-group signal (descendants that left the group), and the rest if no group
            if group.is_none() || Some(*pid) != group {
                let _ = self.sys.send_pid(*pid, sig);
            }
        }
    }

    /// TERM now; KILL after the grace period for whatever still verifies. Returns at once: the escalation runs on its
    /// own thread, and a unit already being killed is not started twice.
    pub fn kill(&self, now: i64, u: &Unit, reason: &str) {
        let ok = match self.verified(u) {
            Ok(ok) => ok,
            Err(e) => {
                // not "gone": the engine tries the unit again after its retry time
                self.note(now, "skipped", "start_time_unverifiable", format!("kill deferred, start times could not be read: {e}"), Some(u));
                return;
            }
        };
        if ok.is_empty() {
            self.note(
                now,
                "kill_done",
                reason,
                "nothing to signal: every pid is gone, was reused, or has no recorded start time".into(),
                Some(u),
            );
            return;
        }
        if !lock(&self.inflight).insert(u.key.clone()) {
            return;
        }
        self.send(u.pgid, &ok, Sig::Term);
        if lock(&self.paused).iter().any(|e| e.key == u.key) {
            // a stopped process holds a TERM undelivered until it is continued (note 43)
            self.send(u.pgid, &ok, Sig::Cont);
        }
        let (sys, unit, key, grace) = (self.sys.clone(), u.clone(), u.key.clone(), self.grace);
        let (outbox, inflight, me) = (self.outbox.clone(), self.inflight.clone(), self.me);
        let level = lock(&self.level).clone();
        let reason = reason.to_string();
        let n = ok.len();
        std::thread::spawn(move || {
            std::thread::sleep(grace);
            let pids: Vec<u32> = ok.iter().map(|(p, _)| *p).collect();
            // `ps` can fail exactly when memory is short: ask again a few times before giving up on the KILL
            let mut now_starts = sys.starts(&pids);
            for _ in 0..3 {
                if now_starts.is_ok() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(250));
                now_starts = sys.starts(&pids);
            }
            let detail = match now_starts {
                Ok(m) => {
                    let still: Vec<(u32, String)> = ok.iter().filter(|(p, s)| *p > 1 && *p != me && m.get(p) == Some(s)).cloned().collect();
                    let group = unit.pgid.filter(|g| *g > 1 && *g != sys.own_pgid() && still.iter().any(|(p, _)| p == g));
                    if let Some(g) = group {
                        let _ = sys.send_group(g, Sig::Kill);
                    }
                    for (p, _) in &still {
                        if Some(*p) != group {
                            let _ = sys.send_pid(*p, Sig::Kill);
                        }
                    }
                    format!("TERM sent to {n} process(es); {} still running after {} ms got KILL", still.len(), grace.as_millis())
                }
                Err(e) => format!(
                    "TERM sent to {n} process(es); KILL not sent, the survivors could not be verified ({e}); the unit is tried again"
                ),
            };
            let mut d = Decision::new(corral_proto_now(), "kill_done", &level, reason, detail);
            d.tier = Some(unit.tier);
            d.matcher = Some(unit.matcher.clone());
            d.unit = Some(unit.reg_name.clone().unwrap_or_else(|| unit.key.clone()));
            d.pids = unit.pids.clone();
            d.comm = Some(unit.comm.clone());
            lock(&outbox).push(d);
            lock(&inflight).remove(&key);
        });
    }

    /// Persist the paused list; an error is returned so the caller can refuse to STOP.
    fn save_paused(&self, list: &[PausedEntry]) -> std::io::Result<()> {
        crate::state::write_atomic(&self.paused_file, &serde_json::to_string(list).unwrap_or_else(|_| "[]".into()))
    }

    pub fn pause(&self, now: i64, u: &Unit) {
        let ok = match self.verified(u) {
            Ok(ok) => ok,
            Err(e) => {
                self.note(now, "skipped", "pause_unverified", format!("pause skipped, start times could not be read: {e}"), Some(u));
                return;
            }
        };
        if ok.is_empty() {
            self.note(now, "skipped", "pause_unverified", "pause skipped: no pid of the unit verifies".into(), Some(u));
            return;
        }
        let group = self.group_of(u.pgid, &ok);
        {
            let mut p = lock(&self.paused);
            let before = p.clone();
            match p.iter_mut().find(|e| e.key == u.key) {
                Some(e) => {
                    e.pgid = group.or(e.pgid);
                    for x in &ok {
                        if !e.pids.contains(x) {
                            e.pids.push(x.clone());
                        }
                    }
                }
                None => p.push(PausedEntry { key: u.key.clone(), pgid: group, pids: ok.clone() }),
            }
            // recorded before the first STOP: a crash in between still resumes them. No record, no STOP.
            if let Err(e) = self.save_paused(&p) {
                *p = before;
                drop(p);
                self.note(now, "skipped", "pause_unrecorded", format!("pause skipped: cannot record it in paused.json: {e}"), Some(u));
                return;
            }
        }
        self.send(u.pgid, &ok, Sig::Stop);
    }

    /// Continue one recorded entry. `false`: the start times could not be read, try again later.
    fn cont_entry(&self, e: &PausedEntry) -> bool {
        let pids: Vec<u32> = e.pids.iter().map(|(p, _)| *p).filter(|p| self.signable(*p)).collect();
        let Ok(now) = self.sys.starts(&pids) else { return false };
        let alive: Vec<(u32, String)> = e.pids.iter().filter(|(p, s)| self.signable(*p) && now.get(p) == Some(s)).cloned().collect();
        self.send(e.pgid, &alive, Sig::Cont);
        true
    }

    /// Continue the entry of `key` (if any); it stays recorded while the check cannot be made.
    pub fn resume_key(&self, key: &str) {
        let mut p = lock(&self.paused);
        let before = p.len();
        p.retain(|e| e.key != key || !self.cont_entry(e));
        if p.len() != before {
            let _ = self.save_paused(&p);
        }
    }

    pub fn resume(&self, u: &Unit) {
        self.resume_key(&u.key);
    }

    /// Every tick: continue each recorded entry whose key the engine no longer holds as paused (the unit vanished from a
    /// sample, was escalated, was killed, or an earlier continue could not be checked). Returns how many were done.
    pub fn reconcile(&self, paused_keys: &[String]) -> usize {
        let mut p = lock(&self.paused);
        let before = p.len();
        p.retain(|e| paused_keys.contains(&e.key) || !self.cont_entry(e));
        let n = before - p.len();
        if n > 0 {
            let _ = self.save_paused(&p);
        }
        n
    }

    /// CONT every process this guard (any previous run of it) stopped and that still verifies. Startup and shutdown.
    /// Entries that cannot be checked right now stay recorded (and are retried by `reconcile`).
    pub fn resume_all_paused(&self) -> usize {
        self.reconcile(&[])
    }

    /// How many entries are still recorded as paused.
    pub fn paused_count(&self) -> usize {
        lock(&self.paused).len()
    }

    /// Run one restore hook on its own thread; its outcome is logged (R-GUARD-07).
    pub fn hook(&self, now: i64, h: &HookSettings) {
        let (runner, cmd, to, outbox, name) =
            (self.hooks.clone(), h.cmd.clone(), Duration::from_millis(h.timeout_ms), self.outbox.clone(), h.name.clone());
        let level = lock(&self.level).clone();
        std::thread::spawn(move || {
            let (reason, detail) = match runner.run(&cmd, to) {
                Ok(o) if o.timed_out => {
                    ("hook_timeout", format!("hook {name:?} killed at its {} ms timeout; output tail: {}", to.as_millis(), o.tail))
                }
                Ok(o) if o.code == Some(0) => ("hook_ok", format!("hook {name:?} exited 0; output tail: {}", o.tail)),
                Ok(o) => ("hook_failed", format!("hook {name:?} exited {:?}; output tail: {}", o.code, o.tail)),
                Err(e) => ("hook_failed", format!("hook {name:?} could not run: {e}")),
            };
            lock(&outbox).push(Decision::new(now, "hook_done", &level, reason, detail));
        });
    }
}

fn corral_proto_now() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::procs::KillKind;
    use crate::testing::FakeSys;
    use std::collections::HashMap;
    use std::os::unix::process::CommandExt;
    use std::process::{Child, Command, Stdio};

    struct NoHooks;
    impl HookRunner for NoHooks {
        fn run(&self, _: &[String], _: Duration) -> Result<HookOutcome, String> {
            Ok(HookOutcome { code: Some(0), timed_out: false, tail: String::new() })
        }
    }

    fn unit(pids: &[(u32, &str)], pgid: Option<u32>) -> Unit {
        Unit {
            key: format!("g:{}", pids[0].0),
            matcher: "m".into(),
            tier: 1,
            heavy: true,
            pausable: false,
            kill: KillKind::Group,
            budget_mb: None,
            notify: true,
            protected: false,
            protect_unresolved: false,
            has_never: false,
            reg_id: None,
            reg_name: None,
            leader: pids[0].0,
            pids: pids.iter().map(|p| p.0).collect(),
            starts: pids.iter().map(|p| p.1.to_string()).collect(),
            pgid,
            comm: "prog".into(),
            footprint_mb: 1,
        }
    }

    fn exec(sys: Arc<FakeSys>, grace_ms: u64) -> Executor {
        let d = std::env::temp_dir().join(format!("cg-enf-{}", corral_proto::util::random_b32(6)));
        std::fs::create_dir_all(&d).unwrap();
        Executor::new(sys, Arc::new(NoHooks), Duration::from_millis(grace_ms), &d)
    }

    fn wait_idle(e: &Executor) {
        for _ in 0..200 {
            if e.idle() {
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        panic!("executor still busy");
    }

    /// R-GUARD-04 / U-06: a pid whose start time changed (reused) is never signalled; one without a recorded start
    /// time is not either; the unit's verified pids are.
    #[test]
    fn pid_reuse_and_missing_start_times_are_not_signalled() {
        let sys = Arc::new(FakeSys::default());
        lock(&sys.starts).extend([(100, "T1".to_string()), (101, "OTHER".to_string()), (102, "T3".to_string())]);
        let e = exec(sys.clone(), 30);
        // 100 verifies, 101 was reused by a new process, 102 has no recorded start
        e.kill(0, &unit(&[(100, "T1"), (101, "T2"), (102, "")], None), "system_memory");
        wait_idle(&e);
        let sent = lock(&sys.sent).clone();
        assert!(sent.iter().all(|(_, p, _)| *p == 100), "only the verified pid is signalled: {sent:?}");
        assert_eq!(sent.iter().map(|s| s.2).collect::<Vec<_>>(), vec![Sig::Term, Sig::Kill], "TERM, then KILL after the grace");
    }

    /// R-GUARD-03: a unit with a `never` member, pid 1 and the guard itself are never signalled.
    #[test]
    fn never_pid1_and_self_are_untouchable() {
        let sys = Arc::new(FakeSys::default());
        let me = std::process::id();
        lock(&sys.starts).extend([(1, "t".to_string()), (me, "t".to_string()), (200, "t".to_string())]);
        let e = exec(sys.clone(), 20);
        let mut n = unit(&[(200, "t")], None);
        n.has_never = true;
        e.kill(0, &n, "kill_switch");
        e.kill(0, &unit(&[(1, "t"), (me, "t")], None), "kill_switch");
        wait_idle(&e);
        assert!(lock(&sys.sent).is_empty(), "{:?}", lock(&sys.sent));
    }

    /// A whole group is signalled once when the unit is that group; never the guard's own group; a leader that no
    /// longer verifies turns it into per-pid signals of the survivors that do.
    #[test]
    fn group_signal_only_for_a_verified_group_unit() {
        let sys = Arc::new(FakeSys::default());
        lock(&sys.starts).extend([(10, "a".to_string()), (11, "b".to_string())]);
        let e = exec(sys.clone(), 20);
        e.kill(0, &unit(&[(10, "a"), (11, "b")], Some(10)), "ram_exceeded");
        wait_idle(&e);
        let sent = lock(&sys.sent).clone();
        assert_eq!(sent[0], ("group".to_string(), 10, Sig::Term));
        assert!(sent.contains(&("pid".to_string(), 11, Sig::Term)));
        lock(&sys.sent).clear();
        lock(&sys.starts).insert(4, "a".into());
        e.kill(0, &unit(&[(4, "a")], Some(4)), "ram_exceeded"); // 4 is the guard's own group in this fake
        wait_idle(&e);
        assert!(lock(&sys.sent).iter().all(|(k, _, _)| k != "group"), "the guard's own group is never signalled");
    }

    fn exec_in(sys: Arc<FakeSys>, grace_ms: u64) -> (Executor, std::path::PathBuf) {
        let d = std::env::temp_dir().join(format!("cg-enf-{}", corral_proto::util::random_b32(6)));
        std::fs::create_dir_all(&d).unwrap();
        (Executor::new(sys, Arc::new(NoHooks), Duration::from_millis(grace_ms), &d), d)
    }

    fn sent_kinds(sys: &FakeSys) -> Vec<Sig> {
        lock(&sys.sent).iter().map(|s| s.2).collect()
    }

    /// H1 / note 43: a paused unit that the engine forgets (it vanished from a sample, was escalated, was killed) is
    /// still continued by reconcile, and the record is emptied.
    #[test]
    fn reconcile_continues_a_recorded_pause_the_engine_no_longer_holds() {
        let sys = Arc::new(FakeSys::default());
        lock(&sys.starts).extend([(10, "a".to_string()), (11, "b".to_string())]);
        let (e, dir) = exec_in(sys.clone(), 20);
        let u = unit(&[(10, "a"), (11, "b")], Some(10));
        e.pause(0, &u);
        assert_eq!(e.paused_count(), 1);
        assert!(std::fs::read_to_string(dir.join("paused.json")).unwrap().contains("\"g:10\""));
        assert_eq!(e.reconcile(&["g:10".into()]), 0, "still paused by the engine: left alone");
        assert_eq!(e.reconcile(&[]), 1);
        let sent = lock(&sys.sent).clone();
        assert!(sent.contains(&("group".into(), 10, Sig::Cont)), "{sent:?}");
        assert_eq!(e.paused_count(), 0);
        assert_eq!(std::fs::read_to_string(dir.join("paused.json")).unwrap(), "[]");
    }

    /// H1 / note 43: members that joined the group after the sample are continued too, because the whole group is
    /// continued while the leader still verifies; and a pid that was reused is not signalled at all.
    #[test]
    fn continue_reaches_the_group_and_skips_a_reused_pid() {
        let sys = Arc::new(FakeSys::default());
        lock(&sys.starts).extend([(10, "a".to_string()), (11, "b".to_string())]);
        let (e, _) = exec_in(sys.clone(), 20);
        e.pause(0, &unit(&[(10, "a"), (11, "b")], Some(10)));
        assert_eq!(lock(&sys.sent)[0], ("group".to_string(), 10, Sig::Stop));
        lock(&sys.sent).clear();
        lock(&sys.starts).insert(11, "REUSED".into());
        e.resume_key("g:10");
        let sent = lock(&sys.sent).clone();
        assert_eq!(sent, vec![("group".to_string(), 10, Sig::Cont)], "group once; the reused 11 is not signalled: {sent:?}");
    }

    /// H2 / note 43: when `ps` fails at recovery the entry is kept and retried; nothing is signalled on a guess and
    /// nothing is forgotten.
    #[test]
    fn a_failed_ps_at_recovery_keeps_the_pause_for_a_retry() {
        let sys = Arc::new(FakeSys::default());
        lock(&sys.starts).insert(10, "a".to_string());
        let (e, dir) = exec_in(sys.clone(), 20);
        e.pause(0, &unit(&[(10, "a")], None));
        lock(&sys.sent).clear();
        let again = Executor::new(sys.clone(), Arc::new(NoHooks), Duration::from_millis(20), &dir);
        sys.set_fail(true);
        assert_eq!(again.resume_all_paused(), 0);
        assert_eq!(again.paused_count(), 1, "kept");
        assert!(lock(&sys.sent).is_empty());
        assert!(std::fs::read_to_string(dir.join("paused.json")).unwrap().contains("\"a\""), "still on disk");
        sys.set_fail(false);
        assert_eq!(again.reconcile(&[]), 1);
        assert_eq!(sent_kinds(&sys), vec![Sig::Cont]);
    }

    /// H2 / note 43: a paused.json that cannot be parsed is reported (decision and a kept copy), not silently empty;
    /// the first M3 format (a list of pids) still recovers.
    #[test]
    fn a_corrupt_paused_record_is_loud_and_the_old_format_still_recovers() {
        let sys = Arc::new(FakeSys::default());
        let d = std::env::temp_dir().join(format!("cg-enf-{}", corral_proto::util::random_b32(6)));
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join("paused.json"), "{ not json").unwrap();
        let e = Executor::new(sys.clone(), Arc::new(NoHooks), Duration::from_millis(20), &d);
        let out = lock(&e.outbox).clone();
        assert!(out.iter().any(|d| d.reason == "paused_state_unreadable"), "{out:?}");
        assert!(d.join("paused.json.corrupt").exists());
        std::fs::write(d.join("paused.json"), r#"[[10,"a"]]"#).unwrap();
        lock(&sys.starts).insert(10, "a".into());
        let e = Executor::new(sys.clone(), Arc::new(NoHooks), Duration::from_millis(20), &d);
        assert_eq!(e.resume_all_paused(), 1);
        assert_eq!(sent_kinds(&sys), vec![Sig::Cont]);
    }

    /// H2: a pause that cannot be recorded is not sent (no record, no STOP).
    #[test]
    fn a_pause_that_cannot_be_recorded_is_not_sent() {
        let sys = Arc::new(FakeSys::default());
        lock(&sys.starts).insert(10, "a".to_string());
        let (e, dir) = exec_in(sys.clone(), 20);
        std::fs::create_dir(dir.join("paused.json")).unwrap(); // the rename onto it fails
        e.pause(0, &unit(&[(10, "a")], None));
        assert!(lock(&sys.sent).is_empty(), "{:?}", lock(&sys.sent));
        assert_eq!(e.paused_count(), 0);
        assert!(lock(&e.outbox).iter().any(|d| d.reason == "pause_unrecorded"));
    }

    /// H3 / note 42: a `ps` that cannot run means nothing is signalled and the kill is reported as deferred, not as
    /// "nothing to signal"; and the KILL step does not fire on a guess either.
    #[test]
    fn a_failing_ps_defers_the_kill_and_the_kill_step() {
        let sys = Arc::new(FakeSys::default());
        lock(&sys.starts).insert(10, "a".to_string());
        let (e, _) = exec_in(sys.clone(), 60);
        sys.set_fail(true);
        e.kill(0, &unit(&[(10, "a")], None), "system_memory");
        assert!(lock(&sys.sent).is_empty());
        assert!(lock(&e.outbox).iter().any(|d| d.reason == "start_time_unverifiable"));
        assert!(e.idle(), "nothing in flight: the engine's retry can start it again");
        sys.set_fail(false);
        e.kill(0, &unit(&[(10, "a")], None), "system_memory");
        sys.set_fail(true); // ps breaks between TERM and the end of the grace period
        wait_idle(&e);
        assert_eq!(sent_kinds(&sys), vec![Sig::Term], "TERM only: the survivors could not be verified");
        assert!(lock(&e.outbox).iter().any(|d| d.detail.contains("could not be verified")));
    }

    /// Note 42: a pid reused between TERM and KILL is not sent KILL; a leader that died during the grace period turns
    /// the group KILL into per-pid KILLs of the verified survivors.
    #[test]
    fn reuse_and_a_dying_leader_during_the_grace_period() {
        let sys = Arc::new(FakeSys::default());
        lock(&sys.starts).extend([(10, "a".to_string()), (11, "b".to_string())]);
        let (e, _) = exec_in(sys.clone(), 80);
        e.kill(0, &unit(&[(10, "a"), (11, "b")], Some(10)), "ram_exceeded");
        lock(&sys.starts).remove(&10); // the leader died after TERM
        lock(&sys.starts).insert(11, "b".into());
        wait_idle(&e);
        let sent = lock(&sys.sent).clone();
        assert!(sent.contains(&("pid".to_string(), 11, Sig::Kill)), "survivor 11 killed by pid: {sent:?}");
        assert!(!sent.contains(&("group".to_string(), 10, Sig::Kill)), "no group KILL once the leader is gone: {sent:?}");
        lock(&sys.sent).clear();
        lock(&sys.starts).extend([(10, "a".to_string()), (11, "b".to_string())]);
        e.kill(0, &unit(&[(10, "a"), (11, "b")], Some(10)), "ram_exceeded");
        lock(&sys.starts).insert(11, "NEW".into()); // 11 was reused by an unrelated process
        wait_idle(&e);
        let sent = lock(&sys.sent).clone();
        assert!(!sent.contains(&("pid".to_string(), 11, Sig::Kill)), "reused pid spared: {sent:?}");
        assert!(sent.contains(&("group".to_string(), 10, Sig::Kill)));
    }

    /// L6 / note 43: TERM sent to a unit that is stopped is followed by CONT, or the TERM waits undelivered.
    #[test]
    fn killing_a_paused_unit_continues_it_after_the_term() {
        let sys = Arc::new(FakeSys::default());
        lock(&sys.starts).insert(10, "a".to_string());
        let (e, _) = exec_in(sys.clone(), 20);
        let u = unit(&[(10, "a")], None);
        e.pause(0, &u);
        lock(&sys.sent).clear();
        e.kill(0, &u, "system_memory");
        wait_idle(&e);
        let k = sent_kinds(&sys);
        assert_eq!(&k[..2], &[Sig::Term, Sig::Cont], "{k:?}");
    }

    /// H3 / note 42: the real start-time lookup against a fake `ps`: a hung one is cut off at the timeout, a failing
    /// one is an error, "every pid is gone" is an empty answer, and a partial answer (exit 1) is kept.
    #[test]
    fn the_real_start_lookup_survives_a_hung_or_failing_ps() {
        let dir = std::env::temp_dir().join(format!("cg-ps-{}", corral_proto::util::random_b32(6)));
        std::fs::create_dir_all(&dir).unwrap();
        let script = |name: &str, body: &str, ms: u64| {
            let p = dir.join(name);
            std::fs::write(&p, format!("#!/bin/sh\n{body}\n")).unwrap();
            std::fs::set_permissions(&p, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
            RealSys { ps: p.to_string_lossy().to_string(), timeout: Duration::from_millis(ms) }
        };
        let hung = script("hung", "sleep 3", 300);
        let t = std::time::Instant::now();
        assert!(hung.starts(&[10]).unwrap_err().contains("timed out"));
        assert!(t.elapsed() < Duration::from_secs(2), "the tick thread is not frozen by a hung ps");
        assert!(script("fail", "echo 'ps: cannot fork' >&2; exit 2", 5000).starts(&[10]).is_err());
        assert_eq!(script("gone", "exit 1", 5000).starts(&[10]).unwrap(), HashMap::new());
        let part = script("part", "echo '  10 Mon Nov 22 10:00:00 2026'; exit 1", 5000);
        assert_eq!(part.starts(&[10, 11]).unwrap(), HashMap::from([(10, "Mon Nov 22 10:00:00 2026".to_string())]));
        assert!(RealSys { ps: "/nonexistent/ps".into(), ..RealSys::default() }.starts(&[10]).is_err());
        assert_eq!(RealSys::default().starts(&[]).unwrap(), HashMap::new());
    }

    fn sleeper(shell: &str) -> Child {
        Command::new("sh")
            .args(["-c", shell])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap()
    }

    fn real_unit(c: &Child, extra_pids: &[u32]) -> Unit {
        let mut pids = vec![c.id()];
        pids.extend_from_slice(extra_pids);
        let starts = pids.iter().map(|p| corral_worker::probe::start_time_of(*p)).collect();
        Unit { pids, starts, pgid: Some(c.id()), ..unit(&[(c.id(), "x")], None) }
    }

    fn alive(pid: u32) -> bool {
        !corral_worker::probe::start_time_of(pid).is_empty()
    }

    /// R-GUARD-01/02 end to end on processes this test started: a TERM-ignoring unit dies by KILL after the grace, its
    /// descendants with it, and a bystander (a similar command line, another group) survives.
    #[test]
    fn real_kill_escalates_and_spares_bystanders() {
        let mut target = sleeper("trap '' TERM; while :; do sleep 1; done");
        let mut bystander = sleeper("trap '' TERM; while :; do sleep 1; done");
        std::thread::sleep(Duration::from_millis(400));
        let u = real_unit(&target, &[]);
        let e = Executor::new(Arc::new(RealSys::default()), Arc::new(NoHooks), Duration::from_millis(500), &std::env::temp_dir());
        e.kill(0, &u, "system_memory");
        wait_idle(&e);
        let st = target.wait().unwrap();
        assert!(!st.success());
        assert!(alive(bystander.id()), "the bystander was not touched");
        let _ = bystander.kill();
        let _ = bystander.wait();
        let out = lock(&e.outbox).clone();
        assert!(out.iter().any(|d| d.kind == "kill_done" && d.detail.contains("KILL")), "{out:?}");
    }

    /// Pause stops the unit, records it on disk; resume continues it; `resume_all_paused` (startup) does too.
    #[test]
    fn real_pause_resume_and_recovery() {
        let mut c = sleeper("sleep 30");
        std::thread::sleep(Duration::from_millis(300));
        let dir = std::env::temp_dir().join(format!("cg-enf-p-{}", corral_proto::util::random_b32(6)));
        std::fs::create_dir_all(&dir).unwrap();
        let e = Executor::new(Arc::new(RealSys::default()), Arc::new(NoHooks), Duration::from_millis(100), &dir);
        let u = real_unit(&c, &[]);
        let state = |pid: u32| {
            let o = Command::new("ps").args(["-o", "state=", "-p", &pid.to_string()]).output().unwrap();
            String::from_utf8_lossy(&o.stdout).trim().to_string()
        };
        e.pause(0, &u);
        std::thread::sleep(Duration::from_millis(300));
        assert!(state(c.id()).starts_with('T'), "stopped: {}", state(c.id()));
        assert!(std::fs::read_to_string(dir.join("paused.json")).unwrap().contains(&c.id().to_string()));
        // a restarted guard reads the file and continues what is still the same process
        let e2 = Executor::new(Arc::new(RealSys::default()), Arc::new(NoHooks), Duration::from_millis(100), &dir);
        assert_eq!(e2.resume_all_paused(), 1);
        std::thread::sleep(Duration::from_millis(300));
        assert!(!state(c.id()).starts_with('T'), "continued: {}", state(c.id()));
        let _ = c.kill();
        let _ = c.wait();
    }

    #[test]
    fn hooks_report_their_outcome_and_a_hanging_hook_is_killed() {
        let ok = RealHooks.run(&["sh".into(), "-c".into(), "echo restored".into()], Duration::from_secs(5)).unwrap();
        assert_eq!((ok.code, ok.timed_out, ok.tail.as_str()), (Some(0), false, "restored"));
        let t = std::time::Instant::now();
        let hung = RealHooks.run(&["sh".into(), "-c".into(), "sleep 30".into()], Duration::from_millis(300)).unwrap();
        assert!(hung.timed_out && t.elapsed() < Duration::from_secs(5));
        assert!(RealHooks.run(&["/nonexistent/hook".into()], Duration::from_secs(1)).is_err());
    }

    /// L3 / note 44: a hook that leaves a background child holding its stdout does not block the guard past the
    /// hook's own exit.
    #[test]
    fn a_hook_that_leaves_a_background_child_does_not_block() {
        let t = std::time::Instant::now();
        let o = RealHooks.run(&["sh".into(), "-c".into(), "echo done; sleep 6 &".into()], Duration::from_secs(10)).unwrap();
        assert_eq!((o.code, o.tail.as_str()), (Some(0), "done"));
        assert!(t.elapsed() < Duration::from_secs(4), "returned after {:?}", t.elapsed());
    }
}
