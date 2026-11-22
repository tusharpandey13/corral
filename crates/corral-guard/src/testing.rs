//! Fakes for tests of this crate and of the CLI: a guard on fake probes and a scripted process table, in a temp
//! directory. Nothing here touches real system state (AGENTS.md "Tests").

use crate::enforce::{Executor, HookOutcome, HookRunner, Sig, Sys};
use crate::guard::{Clock, Deps, Guard, ProtectResolver};
use crate::procs::{ProcInfo, ProcSource, Protect};
use crate::sample::fake::{self, Cells};
use crate::sample::{MemReading, PressureReading};
use crate::settings::GuardSettings;
use corral_proto::config::Config;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

pub struct ManualClock(pub AtomicI64);
impl Clock for ManualClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// A process table the test edits.
#[derive(Default)]
pub struct ListProcs {
    pub table: Mutex<Vec<ProcInfo>>,
    pub cwd: Mutex<std::collections::BTreeMap<u32, String>>,
}
impl ProcSource for ListProcs {
    fn snapshot(&self) -> Option<Vec<ProcInfo>> {
        Some(self.table.lock().unwrap().clone())
    }
    fn cwd(&self, pid: u32) -> Option<String> {
        self.cwd.lock().unwrap().get(&pid).cloned()
    }
    fn cgroup(&self, _pid: u32) -> Option<String> {
        None
    }
    fn exe(&self, _pid: u32) -> Option<String> {
        None
    }
}
struct Shared(Arc<ListProcs>);
impl ProcSource for Shared {
    fn snapshot(&self) -> Option<Vec<ProcInfo>> {
        self.0.snapshot()
    }
    fn cwd(&self, pid: u32) -> Option<String> {
        self.0.cwd(pid)
    }
    fn cgroup(&self, pid: u32) -> Option<String> {
        self.0.cgroup(pid)
    }
    fn exe(&self, pid: u32) -> Option<String> {
        self.0.exe(pid)
    }
}

/// Fake system calls for the executor: a table of live pids and their start times, a log of every signal, and a switch
/// that makes the start-time lookup fail (a `ps` that cannot fork).
#[derive(Default)]
pub struct FakeSys {
    pub starts: Mutex<std::collections::HashMap<u32, String>>,
    pub sent: Mutex<Vec<(String, u32, Sig)>>,
    pub fail: std::sync::atomic::AtomicBool,
}

impl FakeSys {
    pub fn set_fail(&self, on: bool) {
        self.fail.store(on, Ordering::SeqCst);
    }
    pub fn signals(&self) -> Vec<(String, u32, Sig)> {
        self.sent.lock().unwrap().clone()
    }
}

impl Sys for FakeSys {
    fn starts(&self, pids: &[u32]) -> Result<std::collections::HashMap<u32, String>, String> {
        if self.fail.load(Ordering::SeqCst) {
            return Err("ps: fork failed".into());
        }
        Ok(self.starts.lock().unwrap().iter().filter(|(p, _)| pids.contains(p)).map(|(p, s)| (*p, s.clone())).collect())
    }
    fn send_pid(&self, pid: u32, sig: Sig) -> Result<(), String> {
        self.sent.lock().unwrap().push(("pid".into(), pid, sig));
        Ok(())
    }
    fn send_group(&self, pgid: u32, sig: Sig) -> Result<(), String> {
        self.sent.lock().unwrap().push(("group".into(), pgid, sig));
        Ok(())
    }
    fn own_pgid(&self) -> u32 {
        4
    }
}

pub struct NoHooks;
impl HookRunner for NoHooks {
    fn run(&self, _: &[String], _: std::time::Duration) -> Result<HookOutcome, String> {
        Ok(HookOutcome { code: Some(0), timed_out: false, tail: String::new() })
    }
}

struct NoResolve;
impl ProtectResolver for NoResolve {
    fn resolve(&self, _cmd: &[String]) -> Protect {
        Err("no resolver in tests".into())
    }
}

pub struct Rig {
    pub guard: Arc<Guard>,
    pub cells: Arc<Cells>,
    pub procs: Arc<ListProcs>,
    pub clock: Arc<ManualClock>,
    pub dir: PathBuf,
    pub config_path: PathBuf,
    /// Set by `enforcing_fake`.
    pub sys: Arc<FakeSys>,
}

pub fn tmpdir(tag: &str) -> PathBuf {
    // short: unix socket paths are limited to about 100 bytes
    let d = std::env::temp_dir().join(format!("cg-{tag}-{}", corral_proto::util::random_b32(5)));
    std::fs::create_dir_all(&d).unwrap();
    d
}

impl Rig {
    /// `extra` is appended to a config with `[guard]` pointing at `dir`; the sample interval is an hour so the
    /// only automatic round is the first one.
    pub fn new(extra: &str) -> Rig {
        Rig::in_dir(&tmpdir("rig"), extra)
    }

    pub fn in_dir(dir: &Path, extra: &str) -> Rig {
        Rig::build(dir, extra, false, "observe")
    }

    /// With the host's real process table (`ps`), for tests that start their own sentinel processes.
    pub fn with_real_ps(extra: &str) -> Rig {
        Rig::build(&tmpdir("rig"), extra, true, "observe")
    }

    /// Enforce mode with the real process table: for tests that start their own processes and watch them die.
    pub fn enforcing(extra: &str) -> Rig {
        Rig::build(&tmpdir("rig"), extra, true, "enforce")
    }

    /// Enforce mode on a scripted process table and fake system calls (`rig.sys`): nothing real is signalled.
    pub fn enforcing_fake(extra: &str) -> Rig {
        Rig::build_with(&tmpdir("rig"), extra, false, "enforce", true)
    }

    fn build(dir: &Path, extra: &str, real_ps: bool, mode: &str) -> Rig {
        Rig::build_with(dir, extra, real_ps, mode, false)
    }

    fn build_with(dir: &Path, extra: &str, real_ps: bool, mode: &str, fake_sys: bool) -> Rig {
        let cfg_text = format!(
            "version = 1\nhost = \"laptop\"\n[guard]\nmode = \"{mode}\"\nkill_grace = \"400ms\"\nsample = \"1h\"\nstate_dir = \"{d}/state\"\nsocket = \"{d}/g.sock\"\nmin_disk = \"1G\"\n{extra}",
            d = dir.display(),
            mode = mode
        );
        let config_path = dir.join("corral.toml");
        std::fs::write(&config_path, &cfg_text).unwrap();
        let cfg = Config::parse(&cfg_text).unwrap();
        let settings = GuardSettings::resolve(&cfg, true).unwrap();
        let cells = Arc::new(Cells::default());
        *cells.mem.lock().unwrap() = Some(MemReading { total_mb: 16_000, avail_mb: 12_000, swap_mb: 0 });
        *cells.pressure.lock().unwrap() = PressureReading { level: Some(1), psi_full_avg10: None };
        *cells.ac.lock().unwrap() = Some(true);
        *cells.disk_mb.lock().unwrap() = Some(100_000);
        *cells.idle.lock().unwrap() = Some(1000);
        let procs = Arc::new(ListProcs::default());
        let clock = Arc::new(ManualClock(AtomicI64::new(1_700_000_000_000)));
        let sys = Arc::new(FakeSys::default());
        let mut guard = Guard::new(
            settings,
            Deps {
                probes: fake::probes(&cells),
                procs: if real_ps { Box::new(crate::procs::PsSource) } else { Box::new(Shared(procs.clone())) },
                disk: Box::new(fake::F(cells.clone())),
                resolver: Box::new(NoResolve),
                clock: clock.clone(),
            },
        )
        .unwrap();
        if fake_sys {
            let x = Executor::new(sys.clone(), Arc::new(NoHooks), std::time::Duration::from_millis(150), &dir.join("state"));
            guard = guard.with_executor(x);
        }
        let guard = Arc::new(guard);
        Rig { guard, cells, procs, clock, dir: dir.to_path_buf(), config_path, sys }
    }

    pub fn set_free_pct(&self, pct: f64) {
        *self.cells.mem.lock().unwrap() = Some(MemReading { total_mb: 16_000, avail_mb: (160.0 * pct) as u64, swap_mb: 0 });
    }

    /// One sample interval later, one decision round.
    pub fn tick(&self) {
        self.clock.0.fetch_add(2_000, Ordering::SeqCst);
        self.guard.tick();
    }

    pub fn socket(&self) -> PathBuf {
        self.guard.settings.socket.clone()
    }

    pub fn decisions(&self) -> Vec<crate::decisions::Decision> {
        let text = std::fs::read_to_string(self.guard.settings.state_dir.join("decisions.jsonl")).unwrap_or_default();
        crate::decisions::read_log(&text).0
    }
}
