//! The runtime adapter interface (RUNTIMES.md section 3). All execution goes through this trait (R-RT-01); v1 ships
//! only `native`. The worker owns the protocol and job semantics (timeout, cancel); a runtime owns "how to run this
//! command and get its bytes and exit status back".

use corral_proto::job::{JobSpec, KillReason};
use std::future::Future;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct RuntimeInfo {
    pub name: String,
    pub caps: Vec<String>,
    /// 0 = owned hardware (R-RT-02).
    pub cost_tier: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CostEstimate {
    pub per_hour: f64,
    pub startup_s: u64,
    pub currency: String,
}

/// Everything a runtime needs to know about one attempt.
#[derive(Debug, Clone)]
pub struct AttemptCtx {
    pub job_id: String,
    pub attempt: u32,
    pub spec: JobSpec,
    pub worker: String,
    pub run_dir: PathBuf,
    pub http_url: String,
    pub input_url: Option<String>,
    pub input_token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RunInfo {
    pub pid: u32,
    pub pgid: u32,
    pub out: PathBuf,
    pub err: PathBuf,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Sample {
    pub footprint_mb: u64,
    pub alive: bool,
    /// The footprint could not be measured (process table unreadable or empty). Never read as "uses no memory":
    /// the worker fails closed after a few consecutive unknown samples.
    pub unknown: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnitSignal {
    Term,
    Kill,
    Stop,
    Cont,
}

#[derive(Debug, Clone, Default)]
pub struct ExitInfo {
    pub code: Option<i32>,
    pub signal: Option<i32>,
    /// Set when the runtime or its provider stopped the job (the job then ends `killed`, never retried).
    pub by_runtime: Option<KillReason>,
}

#[derive(Debug)]
pub enum RuntimeError {
    /// The input could not be fetched or unpacked (the attempt is declined: it may work later).
    Input(String),
    Spawn(String),
    Other(String),
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeError::Input(m) => write!(f, "input: {m}"),
            RuntimeError::Spawn(m) => write!(f, "spawn: {m}"),
            RuntimeError::Other(m) => write!(f, "{m}"),
        }
    }
}

pub trait Runtime: Send + Sync + 'static {
    type Prepared: Send + Sync + 'static;
    type Running: Send + Sync + 'static;

    /// Static description, sent in claims and presence.
    fn describe(&self) -> RuntimeInfo;

    /// Optional estimate for placement and budget (phase 2). Native: zero cost.
    fn estimate(&self, _spec: &JobSpec) -> Option<CostEstimate> {
        None
    }

    /// Acquire whatever the job needs to run (native: run dir and input).
    fn prepare(&self, ctx: &AttemptCtx) -> impl Future<Output = Result<Self::Prepared, RuntimeError>> + Send;

    /// Start the command in its own unit (process group).
    fn start(&self, ctx: &AttemptCtx, p: &Self::Prepared) -> impl Future<Output = Result<Self::Running, RuntimeError>> + Send;

    fn info(&self, r: &Self::Running) -> RunInfo;

    /// Resource sample for heartbeats and the watchdog.
    fn sample(&self, r: &Self::Running) -> Sample;

    /// Deliver a signal to the whole unit (group plus recorded descendants).
    fn signal(&self, r: &Self::Running, sig: UnitSignal) -> Result<(), RuntimeError>;

    /// Wait for exit. Cancel-safe.
    fn wait(&self, r: &Self::Running) -> impl Future<Output = Result<ExitInfo, RuntimeError>> + Send;

    /// Pack artifact paths (relative to the job's working directory) into a `tar.zst` at `dest`. `Ok(false)` when
    /// none of the paths exist.
    fn collect(
        &self,
        ctx: &AttemptCtx,
        p: &Self::Prepared,
        paths: &[String],
        dest: &Path,
    ) -> impl Future<Output = Result<bool, RuntimeError>> + Send;

    /// Release everything. Idempotent. `keep` retains the run directory for post-mortem (failed attempts).
    fn destroy(&self, ctx: &AttemptCtx, p: Self::Prepared, keep: bool) -> impl Future<Output = Result<(), RuntimeError>> + Send;
}
