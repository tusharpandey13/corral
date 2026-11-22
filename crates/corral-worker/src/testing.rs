//! Test doubles for the worker's seams. Public so integration tests (and later milestones) can use them.

use crate::probe::{MemInfo, MemoryProbe};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// Scripted memory: each `read` pops the next available value; the last one repeats.
pub struct FakeMemory {
    total_mb: u64,
    seq: Mutex<VecDeque<u64>>,
    last: Mutex<Option<u64>>,
    err: Option<String>,
    pub reads: Mutex<u64>,
}

impl FakeMemory {
    pub fn constant(total_mb: u64, available_mb: u64) -> Arc<FakeMemory> {
        Self::scripted(total_mb, &[available_mb])
    }
    pub fn scripted(total_mb: u64, available: &[u64]) -> Arc<FakeMemory> {
        Arc::new(FakeMemory {
            total_mb,
            seq: Mutex::new(available.iter().copied().collect()),
            last: Mutex::new(None),
            err: None,
            reads: Mutex::new(0),
        })
    }
    pub fn failing(msg: &str) -> Arc<FakeMemory> {
        Arc::new(FakeMemory {
            total_mb: 0,
            seq: Mutex::new(VecDeque::new()),
            last: Mutex::new(None),
            err: Some(msg.into()),
            reads: Mutex::new(0),
        })
    }
    /// Replace the script (e.g. to flip memory mid-test).
    pub fn set(&self, available_mb: u64) {
        *self.seq.lock().unwrap() = VecDeque::from([available_mb]);
        *self.last.lock().unwrap() = None;
    }
}

impl MemoryProbe for FakeMemory {
    fn read(&self) -> Result<MemInfo, String> {
        *self.reads.lock().unwrap() += 1;
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        let mut seq = self.seq.lock().unwrap();
        let mut last = self.last.lock().unwrap();
        let v = match seq.pop_front() {
            Some(v) => {
                *last = Some(v);
                v
            }
            None => last.unwrap_or(0),
        };
        Ok(MemInfo { total_mb: self.total_mb, available_mb: v })
    }
}

// ---- FakeRuntime ----------------------------------------------------------------------------------------------

use crate::runtime::*;
use corral_proto::job::JobSpec;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::sync::Notify;

/// What a fake job does.
#[derive(Debug, Clone)]
pub enum Behavior {
    /// Print `stdout`, then exit with `code` after `after_ms`.
    Exit { code: i32, after_ms: u64, stdout: String },
    /// Run until signalled (TERM or KILL ends it).
    Hang,
    /// Die from a signal nobody in corral sent (OOM killer, `kill -9` by someone else).
    ExternalSignal { after_ms: u64 },
    /// Run until KILLed: TERM is ignored.
    IgnoreTerm,
    /// Grow beyond the declared RAM, then hang.
    Grow { footprint_mb: u64 },
}

pub struct FakeState {
    exit: Mutex<Option<ExitInfo>>,
    notify: Notify,
    footprint: Mutex<u64>,
    ignore_term: bool,
}

pub struct FakeRunning {
    st: Arc<FakeState>,
    info: RunInfo,
}

pub struct FakeRuntime {
    pub behavior: Box<dyn Fn(&JobSpec) -> Behavior + Send + Sync>,
    /// Every signal the worker sent, in order.
    pub signals: Mutex<Vec<UnitSignal>>,
    next_pid: AtomicU32,
}

impl FakeRuntime {
    pub fn new(f: impl Fn(&JobSpec) -> Behavior + Send + Sync + 'static) -> Arc<FakeRuntime> {
        Arc::new(FakeRuntime { behavior: Box::new(f), signals: Mutex::new(vec![]), next_pid: AtomicU32::new(40_000) })
    }
}

impl FakeState {
    fn finish(&self, e: ExitInfo) {
        let mut x = self.exit.lock().unwrap();
        if x.is_none() {
            *x = Some(e);
        }
        drop(x);
        self.notify.notify_waiters();
    }
}

impl Runtime for FakeRuntime {
    type Prepared = ();
    type Running = FakeRunning;

    fn describe(&self) -> RuntimeInfo {
        RuntimeInfo { name: "fake".into(), caps: vec![], cost_tier: 0 }
    }

    async fn prepare(&self, ctx: &AttemptCtx) -> Result<(), RuntimeError> {
        std::fs::create_dir_all(&ctx.run_dir).map_err(|e| RuntimeError::Other(e.to_string()))
    }

    async fn start(&self, ctx: &AttemptCtx, _p: &()) -> Result<FakeRunning, RuntimeError> {
        let pid = self.next_pid.fetch_add(1, Ordering::SeqCst);
        let (out, err) = (ctx.run_dir.join("stdout"), ctx.run_dir.join("stderr"));
        std::fs::write(&err, "").map_err(|e| RuntimeError::Other(e.to_string()))?;
        std::fs::write(&out, "").map_err(|e| RuntimeError::Other(e.to_string()))?;
        let behavior = (self.behavior)(&ctx.spec);
        let ignore_term = matches!(behavior, Behavior::IgnoreTerm);
        let st = Arc::new(FakeState { exit: Mutex::new(None), notify: Notify::new(), footprint: Mutex::new(1), ignore_term });
        match behavior {
            Behavior::Exit { code, after_ms, stdout } => {
                let (st2, out2) = (st.clone(), out.clone());
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(after_ms)).await;
                    let _ = std::fs::write(&out2, stdout);
                    st2.finish(ExitInfo { code: Some(code), signal: None, by_runtime: None });
                });
            }
            Behavior::Hang | Behavior::IgnoreTerm => {}
            Behavior::ExternalSignal { after_ms } => {
                let st2 = st.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(after_ms)).await;
                    st2.finish(ExitInfo { code: None, signal: Some(9), by_runtime: None });
                });
            }
            Behavior::Grow { footprint_mb } => *st.footprint.lock().unwrap() = footprint_mb,
        }
        Ok(FakeRunning { st, info: RunInfo { pid, pgid: pid, out, err } })
    }

    fn info(&self, r: &FakeRunning) -> RunInfo {
        r.info.clone()
    }

    fn sample(&self, r: &FakeRunning) -> Sample {
        Sample { footprint_mb: *r.st.footprint.lock().unwrap(), alive: r.st.exit.lock().unwrap().is_none() }
    }

    fn signal(&self, r: &FakeRunning, sig: UnitSignal) -> Result<(), RuntimeError> {
        self.signals.lock().unwrap().push(sig);
        match sig {
            UnitSignal::Term if r.st.ignore_term => {}
            UnitSignal::Term => r.st.finish(ExitInfo { code: None, signal: Some(15), by_runtime: None }),
            UnitSignal::Kill => r.st.finish(ExitInfo { code: None, signal: Some(9), by_runtime: None }),
            _ => {}
        }
        Ok(())
    }

    async fn wait(&self, r: &FakeRunning) -> Result<ExitInfo, RuntimeError> {
        loop {
            let n = r.st.notify.notified();
            if let Some(e) = r.st.exit.lock().unwrap().clone() {
                return Ok(e);
            }
            n.await;
        }
    }

    async fn collect(&self, _c: &AttemptCtx, _p: &(), _paths: &[String], _dest: &Path) -> Result<bool, RuntimeError> {
        Ok(false)
    }

    async fn destroy(&self, ctx: &AttemptCtx, _p: (), keep: bool) -> Result<(), RuntimeError> {
        if !keep {
            let _ = std::fs::remove_dir_all(&ctx.run_dir);
        }
        Ok(())
    }
}
