//! One attempt, from claim to the final report: admission outcome, input, process, logs, watchdog, artifacts,
//! `finished`. Safety rules live here: a killed job is reported `killed` and never restarted (the scheduler decides
//! about requeues, and only for lost attempts); every wait has a reason (declines carry theirs); only the process
//! group this worker started is signalled.

use crate::admission::Ticket;
use crate::hostguard::GuardReg;
use crate::link::SchedLink;
use crate::logs::LogPublisher;
use crate::runtime::*;
use crate::worker::Worker;
use corral_proto::PROTOCOL_VERSION;
use corral_proto::api::*;
use corral_proto::envelope::ErrorCode;
use corral_proto::job::{JobView, KillReason, REASON_CANCELLED};
use corral_proto::util::rfc3339;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};

/// Consecutive one-second samples without a readable footprint after which the job is stopped (fail closed).
const UNKNOWN_SAMPLES_LIMIT: u32 = 5;

/// State shared between the attempt task, its heartbeat task and control pushes.
pub struct AttemptFlags {
    pub token: String,
    pub cancel: AtomicBool,
    /// The scheduler said `stale_lease`: kill, discard, report nothing (R-QUEUE-06).
    pub stale: AtomicBool,
    pub phase: Mutex<&'static str>,
    pub footprint_mb: AtomicU64,
    pub pid: AtomicU32,
    pub seq_out: Arc<AtomicU64>,
    pub seq_err: Arc<AtomicU64>,
    pub seq_sys: Arc<AtomicU64>,
    /// The host guard killed this attempt's unit, and why (set from the guard's event stream, R-GUARD-08).
    pub guard_kill: Mutex<Option<KillReason>>,
    pub guard_paused: AtomicBool,
}

impl AttemptFlags {
    pub fn new(token: &str) -> AttemptFlags {
        AttemptFlags {
            token: token.into(),
            cancel: AtomicBool::new(false),
            stale: AtomicBool::new(false),
            phase: Mutex::new("preparing"),
            footprint_mb: AtomicU64::new(0),
            pid: AtomicU32::new(0),
            seq_out: Arc::new(AtomicU64::new(0)),
            seq_err: Arc::new(AtomicU64::new(0)),
            seq_sys: Arc::new(AtomicU64::new(0)),
            guard_kill: Mutex::new(None),
            guard_paused: AtomicBool::new(false),
        }
    }
    fn set_phase(&self, p: &'static str) {
        *self.phase.lock().unwrap_or_else(|e| e.into_inner()) = p;
    }
}

/// Why this worker stopped the job (as opposed to the job ending on its own).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cause {
    Timeout,
    RamExceeded,
    Cancelled,
    Stale,
}

/// U-23: decide how an attempt ended. A stop that corral decided on carries its reason; a signal nobody in corral
/// sent is `external_signal`; everything else is the job's own exit.
pub fn classify(cause: Option<Cause>, exit: &ExitInfo, duration_s: u64, peak_mb: u64) -> Finished {
    let mut f = Finished {
        outcome: "exited".into(),
        exit_code: exit.code,
        signal: exit.signal,
        reason: None,
        detail: None,
        footprint_peak_mb: Some(peak_mb),
        duration_s: Some(duration_s),
        disturbed: false,
        log_dropped_bytes: 0,
        log_seq: BTreeMap::new(),
    };
    let killed = |f: &mut Finished, reason: &str, detail: &str| {
        f.outcome = "killed".into();
        f.reason = Some(reason.into());
        f.detail = Some(detail.into());
    };
    match cause {
        Some(Cause::Cancelled) => killed(&mut f, REASON_CANCELLED, "cancelled by request"),
        Some(Cause::Timeout) => killed(&mut f, KillReason::Timeout.as_str(), "the job's timeout expired"),
        Some(Cause::RamExceeded) => killed(&mut f, KillReason::RamExceeded.as_str(), "footprint exceeded the declared RAM"),
        Some(Cause::Stale) => killed(&mut f, KillReason::StaleLease.as_str(), "the attempt's lease was revoked"),
        None => {
            if let Some(r) = exit.by_runtime {
                killed(&mut f, r.as_str(), "stopped by the host guard or the runtime");
            } else if let Some(s) = exit.signal {
                killed(&mut f, KillReason::ExternalSignal.as_str(), &format!("terminated by signal {s} that corral did not send"));
            } else if exit.code.is_none() {
                f.exit_code = Some(-1);
            }
        }
    }
    f
}

#[derive(Serialize, Deserialize, Default)]
pub struct AttemptState {
    pub job: String,
    pub attempt: u32,
    pub token: String,
    pub pid: u32,
    pub pgid: u32,
    pub start: String,
    pub phase: String,
    pub finished: Option<Finished>,
}

pub fn state_path(run_dir: &std::path::Path) -> PathBuf {
    run_dir.join("attempt.json")
}

pub fn write_state(run_dir: &std::path::Path, s: &AttemptState) {
    let tmp = run_dir.join("attempt.json.tmp");
    if std::fs::write(&tmp, serde_json::to_vec(s).expect("json")).is_ok() {
        let _ = std::fs::rename(tmp, state_path(run_dir));
    }
}

/// Append a corral note to the attempt's `sys` log stream.
fn sys_note(run_dir: &std::path::Path, text: &str) {
    use std::io::Write;
    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(run_dir.join("sys")) {
        let _ = writeln!(f, "[corral] {text}");
    }
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<L: SchedLink, R: Runtime> Worker<L, R> {
    /// Send an attempt event; `stale_lease` marks the attempt stale.
    async fn send_event(&self, job: &str, attempt: u32, flags: &AttemptFlags, ev: AttemptEvent) -> bool {
        let req = EventReq { lease_token: flags.token.clone(), attempt, event: ev };
        match self.link.event(job, &req).await {
            Ok(_) => true,
            Err(e) if e.error == ErrorCode::StaleLease => {
                flags.stale.store(true, Ordering::SeqCst);
                false
            }
            Err(e) => {
                tracing::warn!(job, "could not deliver event: {e}");
                false
            }
        }
    }

    async fn hb_loop(self: Arc<Self>, job: String, attempt: u32, flags: Arc<AttemptFlags>) {
        loop {
            tokio::time::sleep(Duration::from_millis(self.st.hb_interval_ms)).await;
            let seqs: BTreeMap<String, u64> = [("out", &flags.seq_out), ("err", &flags.seq_err), ("sys", &flags.seq_sys)]
                .iter()
                .map(|(n, a)| (n.to_string(), a.load(Ordering::SeqCst)))
                .collect();
            let hb = HbReq {
                lease_token: flags.token.clone(),
                attempt,
                phase: flags.phase.lock().unwrap_or_else(|e| e.into_inner()).to_string(),
                pid: Some(flags.pid.load(Ordering::SeqCst)).filter(|p| *p != 0),
                footprint_mb: Some(flags.footprint_mb.load(Ordering::SeqCst)),
                cpu_s: None,
                log_seq: seqs,
            };
            match self.link.heartbeat(&job, &hb).await {
                Ok(r) => {
                    if r.cancel {
                        flags.cancel.store(true, Ordering::SeqCst);
                    }
                }
                Err(e) if e.error == ErrorCode::StaleLease => flags.stale.store(true, Ordering::SeqCst),
                Err(_) => {} // scheduler or NATS away: the job keeps running and logs spool
            }
        }
    }

    /// Run one claimed attempt to its end. `admit` is the local admission outcome made at claim time.
    pub async fn run_attempt(self: Arc<Self>, claim: ClaimReply, admit: Result<Ticket, Vec<DeclineReason>>) {
        let (Some(job), Some(token)) = (claim.job.clone(), claim.lease_token.clone()) else { return };
        let flags = Arc::new(AttemptFlags::new(&token));
        self.register(&job.id, claim.attempt, flags.clone());
        let hb = AbortOnDrop(tokio::spawn(self.clone().hb_loop(job.id.clone(), claim.attempt, flags.clone())));
        self.attempt_body(&claim, &job, &flags, admit).await;
        drop(hb);
        self.unregister(&job.id, claim.attempt);
        self.claim_notify.notify_one();
        self.presence_notify.notify_one();
    }

    async fn attempt_body(&self, claim: &ClaimReply, job: &JobView, flags: &Arc<AttemptFlags>, admit: Result<Ticket, Vec<DeclineReason>>) {
        let attempt = claim.attempt;
        let id = job.id.clone();
        let ticket = match admit {
            Ok(t) => t,
            Err(reasons) => {
                self.note_decline();
                self.send_event(&id, attempt, flags, AttemptEvent::Declined { reasons }).await;
                return;
            }
        };
        let run_dir = self.st.work_dir.join("runs").join(&id).join(attempt.to_string());
        let _ = std::fs::create_dir_all(&run_dir);
        let ctx = AttemptCtx {
            job_id: id.clone(),
            attempt,
            spec: job.spec.clone(),
            worker: self.st.host.clone(),
            run_dir: run_dir.clone(),
            http_url: self.st.http_url.clone(),
            input_url: claim.input_url.clone(),
            input_token: claim.input_token.clone(),
        };
        // Keep the host lock alive while the attempt lives (R-LOCK-03); the ticket releases it on every exit path.
        let renewer = ticket.lock_agent().map(|_| {
            let adm = self.adm.clone();
            let every = Duration::from_secs((self.adm.lock_ttl_s() / 3).max(1));
            let agent = ticket.lock_agent().map(str::to_string);
            AbortOnDrop(tokio::spawn(async move {
                loop {
                    tokio::time::sleep(every).await;
                    adm.renew_agent(agent.as_deref());
                }
            }))
        });

        sys_note(&run_dir, &format!("admitted on {} (attempt {attempt})", self.st.host));
        let prepared = match self.rt.prepare(&ctx).await {
            Ok(p) => p,
            Err(e) => {
                // An input that cannot be fetched now may be fine later: decline (not an attempt) with the reason.
                let reasons = vec![DeclineReason { check: "input".into(), reason: e.to_string() }];
                self.note_decline();
                self.send_event(&id, attempt, flags, AttemptEvent::Declined { reasons }).await;
                let _ = std::fs::remove_dir_all(&run_dir);
                return;
            }
        };
        let running = match self.rt.start(&ctx, &prepared).await {
            Ok(r) => r,
            Err(e) => {
                sys_note(&run_dir, &format!("could not start the command: {e}"));
                let mut f = classify(None, &ExitInfo { code: Some(127), ..Default::default() }, 0, 0);
                f.detail = Some(e.to_string());
                self.finish(&id, attempt, flags, &run_dir, f, None).await;
                let _ = self.rt.destroy(&ctx, prepared, true).await;
                drop(renewer);
                return;
            }
        };
        let info = self.rt.info(&running);
        flags.pid.store(info.pid, Ordering::SeqCst);
        flags.set_phase("running");
        let proc_start = crate::probe::start_time_of(info.pid);
        write_state(
            &run_dir,
            &AttemptState {
                job: id.clone(),
                attempt,
                token: flags.token.clone(),
                pid: info.pid,
                pgid: info.pgid,
                start: proc_start,
                phase: "running".into(),
                finished: None,
            },
        );
        sys_note(&run_dir, &format!("started pid {} in process group {}", info.pid, info.pgid));
        // The guard watches the unit from now on: tier 0, pausable, budgeted by the declared RAM (R-GUARD-06).
        let ram = self.ram_for(&job.spec);
        if let Err(e) = ticket.register_with_guard(GuardReg {
            name: format!("corral:{id}:{attempt}"),
            pid: info.pid,
            pgid: info.pgid,
            pid_start: proc_start_for_guard(info.pid),
            tier: Some(0),
            budget_mb: Some(ram),
            pausable: true,
            heavy: job.spec.class == corral_proto::job::Class::Heavy,
            reservation: None,
        }) {
            sys_note(
                &run_dir,
                &format!("the host guard did not take this job's registration ({e}); only the worker's own budget watchdog applies"),
            );
        }
        // Keep the host awake while the job runs (R-AWAKE-01): a named hold bound to the job's pid, renewed by its own
        // task, released on every exit path.
        let mut hold = None;
        if let Some(ka) = self.adm.keepawake() {
            match crate::keepawake::HoldGuard::take(
                ka,
                crate::keepawake::job_owner(&id, attempt),
                Duration::from_secs(job.spec.timeout_s),
                Some(info.pid),
                false,
            )
            .await
            {
                Ok(h) => hold = Some(h),
                Err(e) => sys_note(&run_dir, &format!("no keep-awake hold ({e}); the host may sleep while this job runs")),
            }
        }
        self.send_event(&id, attempt, flags, AttemptEvent::Started { pid: info.pid, pgid: info.pgid }).await;

        // Log publishers and progress.
        let (done_tx, done_rx) = watch::channel(false);
        let (ptx, mut prx) = mpsc::unbounded_channel::<(u8, String)>();
        let mk = |stream: &'static str, path: PathBuf, seq: Arc<AtomicU64>, progress: Option<mpsc::UnboundedSender<(u8, String)>>| {
            LogPublisher {
                link: self.link.clone(),
                job: id.clone(),
                attempt,
                stream,
                path,
                spool_max: self.st.log_spool_max,
                progress,
                seq_out: seq,
                poll: Duration::from_millis(100),
            }
        };
        let pubs = [
            tokio::spawn(mk("out", info.out.clone(), flags.seq_out.clone(), Some(ptx.clone())).run(done_rx.clone())),
            tokio::spawn(mk("err", info.err.clone(), flags.seq_err.clone(), Some(ptx.clone())).run(done_rx.clone())),
            tokio::spawn(mk("sys", run_dir.join("sys"), flags.seq_sys.clone(), None).run(done_rx.clone())),
        ];
        drop(ptx);
        let progress_task = {
            let (link, id2) = (self.link.clone(), id.clone());
            tokio::spawn(async move {
                // At most 2 progress messages per second per job.
                let mut tick = tokio::time::interval(Duration::from_millis(500));
                let mut latest: Option<(u8, String)> = None;
                loop {
                    tokio::select! {
                        m = prx.recv() => match m { Some(m) => latest = Some(m), None => break },
                        _ = tick.tick() => {
                            if let Some((pct, text)) = latest.take() {
                                link.publish_progress(&id2, &ProgressMsg { v: PROTOCOL_VERSION, attempt, pct, text, t: rfc3339(now_ms()) }).await;
                            }
                        }
                    }
                }
                if let Some((pct, text)) = latest.take() {
                    link.publish_progress(&id2, &ProgressMsg { v: PROTOCOL_VERSION, attempt, pct, text, t: rfc3339(now_ms()) }).await;
                }
            })
        };

        // Run, watching the clock, the budget, cancel and stale flags.
        let started = Instant::now();
        let timeout = Duration::from_secs(job.spec.timeout_s);
        let grace = Duration::from_millis(self.st.kill_grace_ms);
        let budget = (self.ram_for(&job.spec) as f64 * self.st.budget_slack) as u64;
        let mut cause: Option<Cause> = None;
        let mut term_at: Option<Instant> = None;
        let mut kill_sent = false;
        let mut peak = 0u64;
        let mut unknown_samples = 0u32;
        let mut last_sample = Instant::now().checked_sub(Duration::from_secs(1)).unwrap_or_else(Instant::now);
        let exit = {
            let wait = self.rt.wait(&running);
            tokio::pin!(wait);
            loop {
                tokio::select! {
                    r = &mut wait => break r,
                    _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                }
                if cause.is_none() {
                    if flags.stale.load(Ordering::SeqCst) {
                        cause = Some(Cause::Stale);
                    } else if flags.cancel.load(Ordering::SeqCst) {
                        cause = Some(Cause::Cancelled);
                    } else if started.elapsed() >= timeout {
                        cause = Some(Cause::Timeout);
                    } else if last_sample.elapsed() >= Duration::from_secs(1) {
                        last_sample = Instant::now();
                        let s = self.rt.sample(&running);
                        // An unmeasurable job is not a job that uses no memory: after a few samples in a row the
                        // guard fails closed and stops it as `ram_exceeded`.
                        unknown_samples = if s.unknown { unknown_samples + 1 } else { 0 };
                        if !s.unknown {
                            peak = peak.max(s.footprint_mb);
                            flags.footprint_mb.store(s.footprint_mb, Ordering::SeqCst);
                            self.adm.update_footprint(&id, attempt, s.footprint_mb);
                        }
                        if s.footprint_mb > budget || unknown_samples >= UNKNOWN_SAMPLES_LIMIT {
                            cause = Some(Cause::RamExceeded);
                        }
                    }
                    if let Some(c) = cause {
                        sys_note(&run_dir, &format!("stopping the job: {c:?} (TERM, then KILL after {} ms)", self.st.kill_grace_ms));
                        let _ = self.rt.signal(&running, UnitSignal::Term);
                        term_at = Some(Instant::now());
                    }
                } else if !kill_sent && term_at.is_some_and(|t| t.elapsed() >= grace) {
                    let _ = self.rt.signal(&running, UnitSignal::Kill);
                    kill_sent = true;
                }
            }
        };
        let mut exit = exit.unwrap_or_else(|e| {
            sys_note(&run_dir, &format!("lost track of the process: {e}"));
            ExitInfo { code: None, signal: Some(0), by_runtime: None }
        });
        // A kill by the host guard carries its reason (R-GUARD-08). The guard announces before it signals, but the
        // event travels on another connection: when the job died of a signal nobody in corral sent, wait briefly.
        // Only an exit that looks like a kill (a signal, or the 128+TERM/KILL a shell reports) can be the guard's: a job
        // that ended by itself with its own code after the event arrived keeps that code (M4).
        let kill_like = exit.signal.is_some() || matches!(exit.code, Some(137 | 143));
        if cause.is_none() && exit.by_runtime.is_none() && kill_like {
            let mut waited = 0;
            while self.adm.guard().is_some() && flags.guard_kill.lock().unwrap_or_else(|e| e.into_inner()).is_none() && waited < 30 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                waited += 1;
            }
            exit.by_runtime = *flags.guard_kill.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(r) = exit.by_runtime {
                sys_note(&run_dir, &format!("stopped by the host guard: {}", r.as_str()));
            }
        }
        let duration_s = started.elapsed().as_secs();
        let mut fin = classify(cause, &exit, duration_s, peak);

        // pause_on_exit (R-EXEC-06): the worker pauses with the reason before anything else can start.
        if let Some(code) = exit.code
            && cause.is_none()
            && job.spec.pause_on_exit.contains(&code)
        {
            self.adm.pause(&format!("job {id} exited with code {code} (pause_on_exit)"));
            sys_note(&run_dir, &format!("worker paused: exit code {code} is in pause_on_exit"));
        }
        // The process is gone: free the slot and the host lock now; reporting follows.
        drop(renewer);
        if let Some(mut h) = hold.take() {
            // the provider is an external command: release it off the async threads
            let _ = tokio::task::spawn_blocking(move || h.release()).await;
        }
        drop(ticket);

        if cause == Some(Cause::Stale) {
            // Discard unsent logs and report nothing: the scheduler already gave this attempt away.
            for p in &pubs {
                p.abort();
            }
            progress_task.abort();
            let _ = self.rt.destroy(&ctx, prepared, false).await;
            return;
        }
        let _ = done_tx.send(true);
        let mut dropped = 0;
        for p in pubs {
            if let Ok(r) = p.await {
                dropped += r.dropped;
            }
        }
        let _ = progress_task.await;
        fin.log_dropped_bytes = dropped;
        fin.log_seq = [("out", &flags.seq_out), ("err", &flags.seq_err), ("sys", &flags.seq_sys)]
            .iter()
            .map(|(n, a)| (n.to_string(), a.load(Ordering::SeqCst)))
            .collect();

        // Artifacts.
        if !job.spec.artifacts.is_empty()
            && let (Some(url), Some(tok)) = (claim.artifact_url.clone(), claim.http_token.clone())
        {
            let dest = run_dir.join("artifacts.tar.zst");
            let (state, bytes) = match self.rt.collect(&ctx, &prepared, &job.spec.artifacts, &dest).await {
                Ok(true) => {
                    let bytes = std::fs::metadata(&dest).map(|m| m.len()).unwrap_or(0);
                    (upload_artifacts(&self.st.http_url, &url, &tok, &dest).await, bytes)
                }
                Ok(false) => ("none".to_string(), 0),
                Err(e) => {
                    sys_note(&run_dir, &format!("artifact collection failed: {e}"));
                    ("none".to_string(), 0)
                }
            };
            sys_note(&run_dir, &format!("artifacts: {state} ({bytes} bytes)"));
            self.send_event(&id, attempt, flags, AttemptEvent::Artifacts { state, bytes, sha256: None }).await;
        }
        // The `sys` note above must also reach followers: publish one more time is not needed; notes after the
        // publishers stopped stay in the local file.
        let failed = fin.outcome != "exited" || fin.exit_code != Some(0);
        self.finish(&id, attempt, flags, &run_dir, fin, Some(&exit)).await;
        let _ = self.rt.destroy(&ctx, prepared, failed).await;
    }

    /// Persist then deliver `finished`, retrying until the scheduler has it (or says the lease is stale).
    async fn finish(
        &self,
        id: &str,
        attempt: u32,
        flags: &Arc<AttemptFlags>,
        run_dir: &std::path::Path,
        f: Finished,
        _exit: Option<&ExitInfo>,
    ) {
        let mut st =
            std::fs::read(state_path(run_dir)).ok().and_then(|b| serde_json::from_slice::<AttemptState>(&b).ok()).unwrap_or_default();
        st.job = id.into();
        st.attempt = attempt;
        st.token = flags.token.clone();
        st.phase = "finishing".into();
        st.finished = Some(f.clone());
        write_state(run_dir, &st);
        let mut delay = Duration::from_millis(500);
        loop {
            let req = EventReq { lease_token: flags.token.clone(), attempt, event: AttemptEvent::Finished(f.clone()) };
            match self.link.event(id, &req).await {
                Ok(_) => break,
                Err(e) if e.error == ErrorCode::StaleLease => break,
                Err(e) if e.error == ErrorCode::NotFound => break,
                Err(e) => {
                    tracing::warn!(job = id, "finished not delivered yet: {e}");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(10));
                }
            }
        }
        let _ = std::fs::remove_file(state_path(run_dir));
    }
}

/// Upload with up to three tries (T-Q4). Returns the artifact state for the event.
async fn upload_artifacts(http_url: &str, path: &str, token: &str, file: &std::path::Path) -> String {
    for i in 0..3u64 {
        let (u, p, t, f) = (http_url.to_string(), path.to_string(), token.to_string(), file.to_path_buf());
        match tokio::task::spawn_blocking(move || crate::input::http_put_file(&u, &p, &t, &f)).await {
            Ok(Ok(200)) => return "uploaded".into(),
            Ok(Ok(413)) => return "refused".into(),
            Ok(Ok(403)) | Ok(Ok(401)) => return "none".into(),
            _ => tokio::time::sleep(Duration::from_millis(300 * (i + 1))).await,
        }
    }
    "none".into()
}

fn proc_start_for_guard(pid: u32) -> String {
    crate::probe::start_time_of(pid)
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exit(code: Option<i32>, signal: Option<i32>) -> ExitInfo {
        ExitInfo { code, signal, by_runtime: None }
    }

    /// U-23: own kills carry their reason, unexplained signals are `external_signal`, plain exits are exits.
    #[test]
    fn finish_classification() {
        let f = classify(None, &exit(Some(0), None), 1, 0);
        assert_eq!((f.outcome.as_str(), f.exit_code), ("exited", Some(0)));
        let f = classify(None, &exit(Some(3), None), 1, 0);
        assert_eq!((f.outcome.as_str(), f.exit_code), ("exited", Some(3)));
        for (cause, reason) in [
            (Cause::Timeout, "timeout"),
            (Cause::RamExceeded, "ram_exceeded"),
            (Cause::Cancelled, "cancelled"),
            (Cause::Stale, "stale_lease"),
        ] {
            let f = classify(Some(cause), &exit(None, Some(15)), 1, 0);
            assert_eq!((f.outcome.as_str(), f.reason.as_deref()), ("killed", Some(reason)));
        }
        let f = classify(None, &exit(None, Some(9)), 1, 0);
        assert_eq!((f.outcome.as_str(), f.reason.as_deref()), ("killed", Some("external_signal")), "OOM killer, kill -9 by someone else");
        let f = classify(None, &ExitInfo { code: None, signal: None, by_runtime: Some(KillReason::SystemMemory) }, 1, 0);
        assert_eq!(f.reason.as_deref(), Some("system_memory"));
    }

    /// A job that exits on its own with a non-zero code while we were killing it is still our kill (the cause wins).
    #[test]
    fn our_cause_wins_over_the_exit_code() {
        let f = classify(Some(Cause::Timeout), &exit(Some(143), None), 1, 0);
        assert_eq!(f.outcome, "killed");
    }
}
