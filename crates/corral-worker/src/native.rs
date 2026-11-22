//! The `native` runtime: the job is a child process of the worker, in its own process group, as the worker's user
//! (RUNTIMES.md section 2).
//!
//! Safety rules (AGENTS.md): signals go only to the process group this runtime started and to descendants it
//! recorded itself (pid plus start time); nothing is ever killed by name.

use crate::input;
use crate::probe::{ProcessTable, PsTable, footprint_mb, unit_members_tagged};
use crate::runtime::*;
use corral_proto::job::{Class, Input};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use tokio::process::{Child, Command};

/// Everything needed to launch one job process, independent of who it runs as.
pub struct LaunchSpec<'a> {
    pub argv: &'a [String],
    pub workdir: &'a Path,
    pub env: Vec<(String, String)>,
    /// Low CPU priority (non-exclusive heavy jobs, R-EXEC-04).
    pub background: bool,
}

/// How a job process is launched: the run-as abstraction. The runtime asks a launcher for the `Command` and never
/// assumes the job runs as the worker's user, in the worker's home or in any checkout of the owner. `SameUser` is
/// the only implementation of M3; a launcher that starts the job as another (runner) user belongs to the job-profile
/// work and must also take over signalling, which a different uid cannot receive from this worker unprivileged.
pub trait Launcher: Send + Sync {
    /// The command that starts the job in its own process group (the runtime sets `process_group(0)` and stdio).
    fn command(&self, spec: &LaunchSpec<'_>) -> Command;
    /// The `HOME` the job sees.
    fn home(&self) -> String;
}

/// Runs the job as the worker's own user.
pub struct SameUser;

impl Launcher for SameUser {
    fn command(&self, spec: &LaunchSpec<'_>) -> Command {
        // The wrappers exec the command, so the pid is the job's. macOS: `taskpolicy -b` = background QoS.
        let mut c = if spec.background {
            let mut c = Command::new("nice");
            c.args(["-n", "10"]);
            if cfg!(target_os = "macos") {
                c.args(["taskpolicy", "-b"]);
            }
            c.args(spec.argv);
            c
        } else {
            let mut c = Command::new(&spec.argv[0]);
            c.args(&spec.argv[1..]);
            c
        };
        c.current_dir(spec.workdir).env_clear().envs(spec.env.iter().cloned());
        c
    }
    fn home(&self) -> String {
        std::env::var("HOME").unwrap_or_else(|_| "/".into())
    }
}

pub struct NativeRuntime {
    launcher: Arc<dyn Launcher>,
    work_dir: PathBuf,
    job_path: String,
    env_passthrough: Vec<String>,
    table: Arc<dyn ProcessTable>,
}

pub struct NativePrepared {
    pub src: PathBuf,
    pub workdir: PathBuf,
    git_repo: Option<String>,
}

pub struct NativeRunning {
    child: tokio::sync::Mutex<Child>,
    pid: u32,
    pgid: u32,
    out: PathBuf,
    err: PathBuf,
    /// Environment entries every descendant inherits (Linux): finds children that escaped the process group.
    tag: Vec<String>,
    /// Descendants that left the process group (setsid): pid -> start time, recorded while sampling.
    escapees: Mutex<HashMap<u32, String>>,
}

impl NativeRuntime {
    pub fn new(work_dir: PathBuf, job_path: String, env_passthrough: Vec<String>) -> NativeRuntime {
        NativeRuntime { launcher: Arc::new(SameUser), work_dir, job_path, env_passthrough, table: Arc::new(PsTable) }
    }

    pub fn with_launcher(mut self, l: Arc<dyn Launcher>) -> NativeRuntime {
        self.launcher = l;
        self
    }

    pub fn with_table(mut self, t: Arc<dyn ProcessTable>) -> NativeRuntime {
        self.table = t;
        self
    }

    fn env_for(&self, ctx: &AttemptCtx) -> Vec<(String, String)> {
        let home = self.launcher.home();
        let mut env: Vec<(String, String)> = vec![
            ("PATH".into(), self.job_path.clone()),
            ("HOME".into(), home),
            ("LANG".into(), std::env::var("LANG").unwrap_or_else(|_| "C.UTF-8".into())),
            ("TMPDIR".into(), ctx.run_dir.join("tmp").to_string_lossy().into()),
            ("CORRAL_JOB_ID".into(), ctx.job_id.clone()),
            ("CORRAL_JOB".into(), ctx.job_id.clone()),
            ("CORRAL_ATTEMPT".into(), ctx.attempt.to_string()),
            ("CORRAL_RAM_MB".into(), ctx.spec.ram().to_string()),
            ("CORRAL_WORKER".into(), ctx.worker.clone()),
            ("CORRAL_OUT".into(), ctx.run_dir.join("out").to_string_lossy().into()),
        ];
        for name in &self.env_passthrough {
            if let Ok(v) = std::env::var(name) {
                env.push((name.clone(), v));
            }
        }
        // The job's own env last, but it can never replace corral's variables.
        for (k, v) in &ctx.spec.env {
            if !k.starts_with("CORRAL_") && !k.is_empty() && !k.contains('=') && !k.contains('\0') {
                env.retain(|(n, _)| n != k);
                env.push((k.clone(), v.clone()));
            }
        }
        env
    }
}

fn sig(s: UnitSignal) -> rustix::process::Signal {
    match s {
        UnitSignal::Term => rustix::process::Signal::TERM,
        UnitSignal::Kill => rustix::process::Signal::KILL,
        UnitSignal::Stop => rustix::process::Signal::STOP,
        UnitSignal::Cont => rustix::process::Signal::CONT,
    }
}

/// The unit's members in `snap`; descendants that left the process group are recorded as escapees.
fn members_of(table: &dyn ProcessTable, r: &NativeRunning, snap: &[crate::probe::Proc]) -> Vec<crate::probe::Proc> {
    let members = unit_members_tagged(table, snap, r.pgid, r.pid, &r.tag);
    let mut esc = r.escapees.lock().unwrap_or_else(|e| e.into_inner());
    for m in members.iter().filter(|m| m.pgid != r.pgid) {
        esc.insert(m.pid, m.start.clone());
    }
    members
}

/// Measure the unit. An unreadable or empty table is `unknown`, never a footprint of zero (fail closed).
fn measure(table: &dyn ProcessTable, r: &NativeRunning) -> Sample {
    let Some(snap) = table.snapshot() else { return Sample { footprint_mb: 0, alive: true, unknown: true } };
    let members = members_of(table, r, &snap);
    Sample { footprint_mb: footprint_mb(&members), alive: !members.is_empty(), unknown: members.is_empty() }
}

fn signal_unit(table: &dyn ProcessTable, r: &NativeRunning, s: UnitSignal) {
    // The group signal needs no table; recorded escapees are signalled only when the table could be read.
    let snap = table.snapshot().unwrap_or_default();
    members_of(table, r, &snap);
    if let Some(g) = rustix::process::Pid::from_raw(r.pgid as i32) {
        let _ = rustix::process::kill_process_group(g, sig(s));
    }
    // Recorded escapees: only if the pid still has the start time we recorded (pid reuse).
    let esc = r.escapees.lock().unwrap_or_else(|e| e.into_inner()).clone();
    for (pid, start) in esc {
        if snap.iter().any(|p| p.pid == pid && p.start == start)
            && let Some(p) = rustix::process::Pid::from_raw(pid as i32)
        {
            let _ = rustix::process::kill_process(p, sig(s));
        }
    }
}

/// What to KILL once the leader has been reaped: its group only while the table still shows a member in it (the group
/// id is free for reuse by an unrelated group otherwise), and the recorded escapees that still have their start time.
/// An unreadable table gives nothing: it cannot be verified, so nothing is signalled (L5).
fn sweep_targets(table: &dyn ProcessTable, r: &NativeRunning) -> (bool, Vec<u32>) {
    let Some(snap) = table.snapshot() else { return (false, vec![]) };
    members_of(table, r, &snap);
    let group = snap.iter().any(|p| p.pgid == r.pgid);
    let esc = r.escapees.lock().unwrap_or_else(|e| e.into_inner()).clone();
    let pids = esc.into_iter().filter(|(pid, start)| snap.iter().any(|p| p.pid == *pid && p.start == *start)).map(|(p, _)| p).collect();
    (group, pids)
}

fn sweep_after_exit(table: &dyn ProcessTable, r: &NativeRunning) {
    let (group, pids) = sweep_targets(table, r);
    if group && let Some(g) = rustix::process::Pid::from_raw(r.pgid as i32) {
        let _ = rustix::process::kill_process_group(g, sig(UnitSignal::Kill));
    }
    for pid in pids {
        if let Some(p) = rustix::process::Pid::from_raw(pid as i32) {
            let _ = rustix::process::kill_process(p, sig(UnitSignal::Kill));
        }
    }
}

impl Runtime for NativeRuntime {
    type Prepared = NativePrepared;
    type Running = NativeRunning;

    fn describe(&self) -> RuntimeInfo {
        RuntimeInfo { name: "native".into(), caps: vec![], cost_tier: 0 }
    }

    fn estimate(&self, _spec: &corral_proto::job::JobSpec) -> Option<CostEstimate> {
        Some(CostEstimate { per_hour: 0.0, startup_s: 0, currency: "none".into() })
    }

    async fn prepare(&self, ctx: &AttemptCtx) -> Result<NativePrepared, RuntimeError> {
        let run = ctx.run_dir.clone();
        for d in ["tmp", "out"] {
            std::fs::create_dir_all(run.join(d)).map_err(|e| RuntimeError::Other(e.to_string()))?;
        }
        let src = run.join("src");
        let work_dir = self.work_dir.clone();
        let spec_input = ctx.spec.input.clone();
        let (http, in_url, in_tok) = (ctx.http_url.clone(), ctx.input_url.clone(), ctx.input_token.clone());
        let src2 = src.clone();
        let run2 = run.clone();
        let git_repo = tokio::task::spawn_blocking(move || -> Result<Option<String>, String> {
            match spec_input {
                Input::None => {
                    std::fs::create_dir_all(&src2).map_err(|e| e.to_string())?;
                    Ok(None)
                }
                Input::Git { repo, sha, .. } => {
                    input::checkout_git(&work_dir, &repo, &sha, &src2)?;
                    Ok(Some(repo))
                }
                Input::Snapshot { sha256, .. } => {
                    let (path, tok) = in_url.zip(in_tok).ok_or("snapshot input but the claim carried no download url")?;
                    let archive = run2.join("input.tar.zst");
                    let got = input::http_download(&http, &path, &tok, &archive)?;
                    if !got.eq_ignore_ascii_case(&sha256) {
                        return Err(format!("snapshot hash mismatch (got {got}, want {sha256})"));
                    }
                    input::unpack_snapshot(&archive, &src2, &run2.join("snapshot-meta"))?;
                    let _ = std::fs::remove_file(&archive);
                    Ok(None)
                }
            }
        })
        .await
        .map_err(|e| RuntimeError::Other(e.to_string()))?
        .map_err(RuntimeError::Input)?;
        let workdir = src.join(&ctx.spec.cwd);
        std::fs::create_dir_all(&workdir).map_err(|e| RuntimeError::Other(e.to_string()))?;
        Ok(NativePrepared { src, workdir, git_repo })
    }

    async fn start(&self, ctx: &AttemptCtx, p: &NativePrepared) -> Result<NativeRunning, RuntimeError> {
        let out = ctx.run_dir.join("stdout");
        let err = ctx.run_dir.join("stderr");
        let open = |p: &Path| std::fs::File::create(p).map_err(|e| RuntimeError::Other(format!("{}: {e}", p.display())));
        let (fo, fe) = (open(&out)?, open(&err)?);
        let cmd = &ctx.spec.cmd;
        // Non-exclusive heavy jobs run at low priority (R-EXEC-04).
        let background = !ctx.spec.exclusive && ctx.spec.class == Class::Heavy;
        let mut c = self.launcher.command(&LaunchSpec { argv: cmd, workdir: &p.workdir, env: self.env_for(ctx), background });
        c.stdin(Stdio::null()).stdout(Stdio::from(fo)).stderr(Stdio::from(fe)).process_group(0).kill_on_drop(false);
        let child = c.spawn().map_err(|e| RuntimeError::Spawn(format!("{}: {e}", cmd[0])))?;
        let pid = child.id().ok_or_else(|| RuntimeError::Spawn("child exited before it could be tracked".into()))?;
        let tag = vec![format!("CORRAL_JOB_ID={}", ctx.job_id), format!("CORRAL_ATTEMPT={}", ctx.attempt)];
        Ok(NativeRunning { child: tokio::sync::Mutex::new(child), pid, pgid: pid, out, err, tag, escapees: Mutex::new(HashMap::new()) })
    }

    fn info(&self, r: &NativeRunning) -> RunInfo {
        RunInfo { pid: r.pid, pgid: r.pgid, out: r.out.clone(), err: r.err.clone() }
    }

    fn sample(&self, r: &NativeRunning) -> Sample {
        measure(self.table.as_ref(), r)
    }

    fn signal(&self, r: &NativeRunning, s: UnitSignal) -> Result<(), RuntimeError> {
        signal_unit(self.table.as_ref(), r, s);
        Ok(())
    }

    async fn wait(&self, r: &NativeRunning) -> Result<ExitInfo, RuntimeError> {
        use std::os::unix::process::ExitStatusExt;
        let status = r.child.lock().await.wait().await.map_err(|e| RuntimeError::Other(e.to_string()))?;
        // The leader is reaped; sweep what it left behind (the group cannot be reused while members exist).
        sweep_after_exit(self.table.as_ref(), r);
        Ok(ExitInfo { code: status.code(), signal: status.signal(), by_runtime: None })
    }

    async fn collect(&self, _ctx: &AttemptCtx, p: &NativePrepared, paths: &[String], dest: &Path) -> Result<bool, RuntimeError> {
        let (base, paths, dest) = (p.workdir.clone(), paths.to_vec(), dest.to_path_buf());
        tokio::task::spawn_blocking(move || input::pack(&base, &paths, &dest))
            .await
            .map_err(|e| RuntimeError::Other(e.to_string()))?
            .map_err(RuntimeError::Other)
    }

    async fn destroy(&self, ctx: &AttemptCtx, p: NativePrepared, keep: bool) -> Result<(), RuntimeError> {
        if keep {
            return Ok(());
        }
        let (work_dir, run, src) = (self.work_dir.clone(), ctx.run_dir.clone(), p.src.clone());
        tokio::task::spawn_blocking(move || {
            if let Some(repo) = &p.git_repo {
                input::remove_worktree(&work_dir, repo, &src);
            }
            let _ = std::fs::remove_dir_all(&run);
        })
        .await
        .map_err(|e| RuntimeError::Other(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::probe::Proc;

    struct Table(Option<Vec<Proc>>);
    impl ProcessTable for Table {
        fn snapshot(&self) -> Option<Vec<Proc>> {
            self.0.clone()
        }
    }

    fn running() -> NativeRunning {
        let child = Command::new("sleep").arg("30").kill_on_drop(true).spawn().expect("sleep");
        NativeRunning {
            child: tokio::sync::Mutex::new(child),
            pid: 40_000,
            pgid: 40_000,
            out: PathBuf::new(),
            err: PathBuf::new(),
            tag: vec![],
            escapees: Mutex::new(HashMap::new()),
        }
    }

    /// L5 (note 42): once the leader is reaped its group is KILLed only while a member still shows in the table; an
    /// unreadable table signals nothing.
    #[tokio::test]
    async fn the_group_is_swept_only_while_it_has_members() {
        let r = running();
        let member = Proc { pid: 40_001, ppid: 1, pgid: 40_000, rss_kb: 1, start: "a".into() };
        let other = Proc { pid: 50_000, ppid: 1, pgid: 50_000, rss_kb: 1, start: "b".into() };
        assert!(sweep_targets(&Table(Some(vec![member])), &r).0);
        assert_eq!(sweep_targets(&Table(Some(vec![other])), &r), (false, vec![]), "group id possibly reused: left alone");
        assert_eq!(sweep_targets(&Table(None), &r), (false, vec![]));
        // a recorded escapee is swept by pid only while its start time matches
        r.escapees.lock().unwrap().insert(60_000, "t1".into());
        let same = Proc { pid: 60_000, ppid: 1, pgid: 60_000, rss_kb: 1, start: "t1".into() };
        let reused = Proc { pid: 60_000, ppid: 1, pgid: 60_000, rss_kb: 1, start: "t2".into() };
        assert_eq!(sweep_targets(&Table(Some(vec![same])), &r).1, vec![60_000]);
        assert!(sweep_targets(&Table(Some(vec![reused])), &r).1.is_empty());
    }

    /// G1 (M1 review): an unreadable or empty process table is "unknown", never a footprint of zero.
    #[tokio::test]
    async fn unreadable_table_is_unknown_not_zero() {
        let r = running();
        assert!(measure(&Table(None), &r).unknown);
        assert!(measure(&Table(Some(vec![])), &r).unknown, "the unit cannot be empty while its leader runs");
        let p = Proc { pid: 40_000, ppid: 1, pgid: 40_000, rss_kb: 2048, start: "x".into() };
        let s = measure(&Table(Some(vec![p])), &r);
        assert!(!s.unknown);
        assert_eq!(s.footprint_mb, 2);
    }
}
