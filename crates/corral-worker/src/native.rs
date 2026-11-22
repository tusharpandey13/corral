//! The `native` runtime: the job is a child process of the worker, in its own process group, as the worker's user
//! (RUNTIMES.md section 2).
//!
//! Safety rules (AGENTS.md): signals go only to the process group this runtime started and to descendants it
//! recorded itself (pid plus start time); nothing is ever killed by name.

use crate::input;
use crate::probe::{ProcessTable, PsTable, footprint_mb, unit_members};
use crate::runtime::*;
use corral_proto::job::{Class, Input};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use tokio::process::{Child, Command};

pub struct NativeRuntime {
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
    /// Descendants that left the process group (setsid): pid -> start time, recorded while sampling.
    escapees: Mutex<HashMap<u32, String>>,
}

impl NativeRuntime {
    pub fn new(work_dir: PathBuf, job_path: String, env_passthrough: Vec<String>) -> NativeRuntime {
        NativeRuntime { work_dir, job_path, env_passthrough, table: Arc::new(PsTable) }
    }

    pub fn with_table(mut self, t: Arc<dyn ProcessTable>) -> NativeRuntime {
        self.table = t;
        self
    }

    fn env_for(&self, ctx: &AttemptCtx) -> Vec<(String, String)> {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/".into());
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

fn signal_unit(table: &dyn ProcessTable, r: &NativeRunning, s: UnitSignal) {
    let snap = table.snapshot();
    let members = unit_members(&snap, r.pgid, r.pid);
    {
        let mut esc = r.escapees.lock().unwrap_or_else(|e| e.into_inner());
        for m in members.iter().filter(|m| m.pgid != r.pgid) {
            esc.insert(m.pid, m.start.clone());
        }
    }
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
        // Non-exclusive jobs run at low CPU priority (R-EXEC-04). `nice` execs the command, so the pid is the job's.
        let mut c = if !ctx.spec.exclusive && ctx.spec.class == Class::Heavy {
            let mut c = Command::new("nice");
            c.args(["-n", "10"]).args(cmd);
            c
        } else {
            let mut c = Command::new(&cmd[0]);
            c.args(&cmd[1..]);
            c
        };
        c.current_dir(&p.workdir)
            .env_clear()
            .envs(self.env_for(ctx))
            .stdin(Stdio::null())
            .stdout(Stdio::from(fo))
            .stderr(Stdio::from(fe))
            .process_group(0)
            .kill_on_drop(false);
        let child = c.spawn().map_err(|e| RuntimeError::Spawn(format!("{}: {e}", cmd[0])))?;
        let pid = child.id().ok_or_else(|| RuntimeError::Spawn("child exited before it could be tracked".into()))?;
        Ok(NativeRunning { child: tokio::sync::Mutex::new(child), pid, pgid: pid, out, err, escapees: Mutex::new(HashMap::new()) })
    }

    fn info(&self, r: &NativeRunning) -> RunInfo {
        RunInfo { pid: r.pid, pgid: r.pgid, out: r.out.clone(), err: r.err.clone() }
    }

    fn sample(&self, r: &NativeRunning) -> Sample {
        let snap = self.table.snapshot();
        let members = unit_members(&snap, r.pgid, r.pid);
        {
            let mut esc = r.escapees.lock().unwrap_or_else(|e| e.into_inner());
            for m in members.iter().filter(|m| m.pgid != r.pgid) {
                esc.insert(m.pid, m.start.clone());
            }
        }
        Sample { footprint_mb: footprint_mb(&members), alive: !members.is_empty() }
    }

    fn signal(&self, r: &NativeRunning, s: UnitSignal) -> Result<(), RuntimeError> {
        signal_unit(self.table.as_ref(), r, s);
        Ok(())
    }

    async fn wait(&self, r: &NativeRunning) -> Result<ExitInfo, RuntimeError> {
        use std::os::unix::process::ExitStatusExt;
        let status = r.child.lock().await.wait().await.map_err(|e| RuntimeError::Other(e.to_string()))?;
        // The leader is reaped; sweep what it left behind (the group cannot be reused while members exist).
        signal_unit(self.table.as_ref(), r, UnitSignal::Kill);
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
