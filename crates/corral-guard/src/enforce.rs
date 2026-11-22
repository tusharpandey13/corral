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
//! * every pause is recorded on disk (`paused.json`) so a guard that dies while processes are stopped resumes them
//!   (verified by start time) when it comes back, and on shutdown.

use crate::decisions::Decision;
use crate::procs::Unit;
use crate::settings::HookSettings;
use std::collections::HashSet;
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
    /// The start time of `pid` right now as `ps` prints it; `None` when the process is gone.
    fn start_of(&self, pid: u32) -> Option<String>;
    fn send_pid(&self, pid: u32, sig: Sig) -> Result<(), String>;
    fn send_group(&self, pgid: u32, sig: Sig) -> Result<(), String>;
    /// The guard's own process group (never signalled).
    fn own_pgid(&self) -> u32;
}

pub struct RealSys;

fn rsig(s: Sig) -> rustix::process::Signal {
    match s {
        Sig::Term => rustix::process::Signal::TERM,
        Sig::Kill => rustix::process::Signal::KILL,
        Sig::Stop => rustix::process::Signal::STOP,
        Sig::Cont => rustix::process::Signal::CONT,
    }
}

impl Sys for RealSys {
    fn start_of(&self, pid: u32) -> Option<String> {
        Some(corral_worker::probe::start_time_of(pid)).filter(|s| !s.is_empty())
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
        use std::io::Read;
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
        let readers: Vec<_> = [
            child.stdout.take().map(|o| Box::new(o) as Box<dyn Read + Send>),
            child.stderr.take().map(|o| Box::new(o) as Box<dyn Read + Send>),
        ]
        .into_iter()
        .flatten()
        .map(|mut r| {
            std::thread::spawn(move || {
                let mut b = Vec::new();
                let _ = r.read_to_end(&mut b);
                b
            })
        })
        .collect();
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
        let mut all = Vec::new();
        for h in readers {
            all.extend(h.join().unwrap_or_default());
        }
        let text = String::from_utf8_lossy(&all).to_string();
        let tail: String = text.chars().rev().take(300).collect::<Vec<_>>().into_iter().rev().collect();
        Ok(HookOutcome { code: status.and_then(|s| s.code()), timed_out, tail: tail.trim().to_string() })
    }
}

/// Decisions produced by background work (kill escalation, hook results) wait here until the guard logs them.
pub type Outbox = Arc<Mutex<Vec<Decision>>>;

pub struct Executor {
    sys: Arc<dyn Sys>,
    hooks: Arc<dyn HookRunner>,
    grace: Duration,
    me: u32,
    outbox: Outbox,
    inflight: Arc<Mutex<HashSet<String>>>,
    paused_file: PathBuf,
    paused: Arc<Mutex<Vec<(u32, String)>>>,
    level: Mutex<String>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Executor {
    pub fn new(sys: Arc<dyn Sys>, hooks: Arc<dyn HookRunner>, grace: Duration, state_dir: &std::path::Path) -> Executor {
        let paused_file = state_dir.join("paused.json");
        let paused: Vec<(u32, String)> = std::fs::read(&paused_file).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default();
        Executor {
            sys,
            hooks,
            grace,
            me: std::process::id(),
            outbox: Arc::new(Mutex::new(Vec::new())),
            inflight: Arc::new(Mutex::new(HashSet::new())),
            paused_file,
            paused: Arc::new(Mutex::new(paused)),
            level: Mutex::new("ok".into()),
        }
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

    /// The pids of the unit that may be signalled right now: still the process the sample saw.
    fn verified(&self, u: &Unit) -> Vec<(u32, String)> {
        if u.has_never {
            return vec![];
        }
        u.pids
            .iter()
            .zip(u.starts.iter().chain(std::iter::repeat(&String::new())))
            .filter(|(pid, start)| {
                **pid > 1 && **pid != self.me && !start.is_empty() && self.sys.start_of(**pid).as_deref() == Some(start.as_str())
            })
            .map(|(p, s)| (*p, s.clone()))
            .collect()
    }

    /// The group to signal as a whole: the unit is that group and its leader still verifies.
    fn group_of(&self, u: &Unit, ok: &[(u32, String)]) -> Option<u32> {
        let g = u.pgid?;
        (g > 1 && g != self.sys.own_pgid() && ok.iter().any(|(p, _)| *p == g)).then_some(g)
    }

    fn send(&self, u: &Unit, ok: &[(u32, String)], sig: Sig) {
        let group = self.group_of(u, ok);
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
        let ok = self.verified(u);
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
        self.send(u, &ok, Sig::Term);
        let (sys, unit, key, grace) = (self.sys.clone(), u.clone(), u.key.clone(), self.grace);
        let (outbox, inflight, me) = (self.outbox.clone(), self.inflight.clone(), self.me);
        let level = lock(&self.level).clone();
        let reason = reason.to_string();
        let n = ok.len();
        let this_grace = grace;
        let paused = self.paused.clone();
        let paused_file = self.paused_file.clone();
        std::thread::spawn(move || {
            std::thread::sleep(this_grace);
            let still: Vec<(u32, String)> =
                ok.iter().filter(|(p, s)| *p > 1 && *p != me && sys.start_of(*p).as_deref() == Some(s.as_str())).cloned().collect();
            let group = unit.pgid.filter(|g| *g > 1 && *g != sys.own_pgid() && still.iter().any(|(p, _)| p == g));
            if let Some(g) = group {
                let _ = sys.send_group(g, Sig::Kill);
            }
            for (p, _) in &still {
                if Some(*p) != group {
                    let _ = sys.send_pid(*p, Sig::Kill);
                }
            }
            // a stopped (paused) unit that was killed is no longer ours to resume
            lock(&paused).retain(|(p, _)| !ok.iter().any(|(q, _)| q == p));
            let _ = std::fs::write(&paused_file, serde_json::to_vec(&*lock(&paused)).unwrap_or_default());
            let mut d = Decision::new(
                corral_proto_now(),
                "kill_done",
                &level,
                reason,
                format!("TERM sent to {n} process(es); {} still running after {} ms got KILL", still.len(), grace.as_millis()),
            );
            d.tier = Some(unit.tier);
            d.matcher = Some(unit.matcher.clone());
            d.unit = Some(unit.reg_name.clone().unwrap_or_else(|| unit.key.clone()));
            d.pids = unit.pids.clone();
            d.comm = Some(unit.comm.clone());
            lock(&outbox).push(d);
            lock(&inflight).remove(&key);
        });
    }

    fn save_paused(&self) {
        let _ = std::fs::write(&self.paused_file, serde_json::to_vec(&*lock(&self.paused)).unwrap_or_default());
    }

    pub fn pause(&self, now: i64, u: &Unit) {
        let ok = self.verified(u);
        if ok.is_empty() {
            self.note(now, "skipped", "pause_unverified", "pause skipped: no pid of the unit verifies".into(), Some(u));
            return;
        }
        {
            let mut p = lock(&self.paused);
            for e in &ok {
                if !p.contains(e) {
                    p.push(e.clone());
                }
            }
        }
        self.save_paused(); // recorded before the first STOP: a crash in between still resumes them
        self.send(u, &ok, Sig::Stop);
    }

    pub fn resume(&self, u: &Unit) {
        let ok = self.verified(u);
        self.send(u, &ok, Sig::Cont);
        lock(&self.paused).retain(|(p, _)| !ok.iter().any(|(q, _)| q == p));
        self.save_paused();
    }

    /// CONT every process this guard (any previous run of it) stopped and that still verifies. Startup and shutdown.
    pub fn resume_all_paused(&self) -> usize {
        let list: Vec<(u32, String)> = std::mem::take(&mut *lock(&self.paused));
        let mut n = 0;
        for (pid, start) in list {
            if pid > 1 && pid != self.me && self.sys.start_of(pid).as_deref() == Some(start.as_str()) {
                let _ = self.sys.send_pid(pid, Sig::Cont);
                n += 1;
            }
        }
        self.save_paused();
        n
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
    use std::collections::HashMap;
    use std::os::unix::process::CommandExt;
    use std::process::{Child, Command, Stdio};

    #[derive(Default)]
    struct FakeSys {
        starts: Mutex<HashMap<u32, String>>,
        sent: Mutex<Vec<(String, u32, Sig)>>,
    }
    impl Sys for FakeSys {
        fn start_of(&self, pid: u32) -> Option<String> {
            lock(&self.starts).get(&pid).cloned()
        }
        fn send_pid(&self, pid: u32, sig: Sig) -> Result<(), String> {
            lock(&self.sent).push(("pid".into(), pid, sig));
            Ok(())
        }
        fn send_group(&self, pgid: u32, sig: Sig) -> Result<(), String> {
            lock(&self.sent).push(("group".into(), pgid, sig));
            Ok(())
        }
        fn own_pgid(&self) -> u32 {
            4
        }
    }
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
        let e = Executor::new(Arc::new(RealSys), Arc::new(NoHooks), Duration::from_millis(500), &std::env::temp_dir());
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
        let e = Executor::new(Arc::new(RealSys), Arc::new(NoHooks), Duration::from_millis(100), &dir);
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
        let e2 = Executor::new(Arc::new(RealSys), Arc::new(NoHooks), Duration::from_millis(100), &dir);
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
}
