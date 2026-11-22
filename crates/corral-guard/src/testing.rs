//! Fakes for tests of this crate and of the CLI: a guard on fake probes and a scripted process table, in a temp
//! directory. Nothing here touches real system state (AGENTS.md "Tests").

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
        Rig::build(dir, extra, false)
    }

    /// With the host's real process table (`ps`), for tests that start their own sentinel processes.
    pub fn with_real_ps(extra: &str) -> Rig {
        Rig::build(&tmpdir("rig"), extra, true)
    }

    fn build(dir: &Path, extra: &str, real_ps: bool) -> Rig {
        let cfg_text = format!(
            "version = 1\nhost = \"laptop\"\n[guard]\nmode = \"observe\"\nsample = \"1h\"\nstate_dir = \"{d}/state\"\nsocket = \"{d}/g.sock\"\nmin_disk = \"1G\"\n{extra}",
            d = dir.display()
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
        let guard = Arc::new(
            Guard::new(
                settings,
                Deps {
                    probes: fake::probes(&cells),
                    procs: if real_ps { Box::new(crate::procs::PsSource) } else { Box::new(Shared(procs.clone())) },
                    disk: Box::new(fake::F(cells.clone())),
                    resolver: Box::new(NoResolve),
                    clock: clock.clone(),
                },
            )
            .unwrap(),
        );
        Rig { guard, cells, procs, clock, dir: dir.to_path_buf(), config_path }
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
