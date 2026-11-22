//! The worker: claim loop, presence, control pushes, restart recovery.

use crate::admission::{Admitter, Need};
use crate::attempt::{AttemptFlags, AttemptState, state_path};
use crate::link::SchedLink;
use crate::runtime::Runtime;
use crate::settings::WorkerSettings;
use corral_proto::PROTOCOL_VERSION;
use corral_proto::api::*;
use corral_proto::envelope::ErrorCode;
use corral_proto::util::rfc3339;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// The live attempts, keyed by job and attempt number: an old attempt finishing its cleanup never removes the entry of
/// the attempt that replaced it.
#[derive(Default)]
struct Registry(HashMap<(String, u32), Arc<AttemptFlags>>);

impl Registry {
    /// The attempt of `job` whose lease token is `token` (a control push must name the current lease).
    fn find(&self, job: &str, token: &str) -> Option<&Arc<AttemptFlags>> {
        self.0.iter().find(|((j, _), f)| j == job && f.token == token).map(|(_, f)| f)
    }
}

pub struct Worker<L: SchedLink, R: Runtime> {
    pub link: Arc<L>,
    pub rt: Arc<R>,
    pub adm: Arc<Admitter>,
    pub st: Arc<WorkerSettings>,
    pub claim_notify: Notify,
    pub presence_notify: Notify,
    attempts: Mutex<Registry>,
    last_decline: Mutex<Option<Instant>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl<L: SchedLink, R: Runtime> Worker<L, R> {
    pub fn new(link: Arc<L>, rt: Arc<R>, adm: Arc<Admitter>, st: Arc<WorkerSettings>) -> Arc<Self> {
        Arc::new(Worker {
            link,
            rt,
            adm,
            st,
            claim_notify: Notify::new(),
            presence_notify: Notify::new(),
            attempts: Mutex::new(Registry::default()),
            last_decline: Mutex::new(None),
            tasks: Mutex::new(Vec::new()),
        })
    }

    pub(crate) fn register(&self, id: &str, attempt: u32, f: Arc<AttemptFlags>) {
        self.attempts.lock().unwrap_or_else(|e| e.into_inner()).0.insert((id.into(), attempt), f);
    }
    pub(crate) fn unregister(&self, id: &str, attempt: u32) {
        self.attempts.lock().unwrap_or_else(|e| e.into_inner()).0.remove(&(id.to_string(), attempt));
    }
    pub(crate) fn note_decline(&self) {
        *self.last_decline.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());
    }

    /// Stop everything this worker is doing without telling the scheduler (what a crash looks like).
    pub fn abort_all(&self) {
        for t in self.tasks.lock().unwrap_or_else(|e| e.into_inner()).drain(..) {
            t.abort();
        }
    }

    pub fn running_jobs(&self) -> Vec<String> {
        self.adm.running_ids()
    }

    /// RAM the job is admitted and watched against: what it declared, else `light_ram_default` (a light job may omit
    /// `ram_mb`, but it is never unlimited).
    pub(crate) fn ram_for(&self, spec: &corral_proto::job::JobSpec) -> u64 {
        match spec.ram() {
            0 => self.st.light_ram_default_mb,
            r => r,
        }
    }

    fn claim_req(&self) -> ClaimReq {
        let (slots, admission) = self.adm.summary();
        let info = self.rt.describe();
        let mut caps = self.st.caps.clone();
        for c in info.caps {
            if !caps.contains(&c) {
                caps.push(c);
            }
        }
        ClaimReq {
            version: self.st.version.clone(),
            runtime: info.name,
            cost_tier: info.cost_tier,
            caps,
            slots,
            admission,
            running: self.adm.running_ids(),
            leases: vec![],
            paused: self.adm.paused(),
            draining: self.adm.draining(),
        }
    }

    /// Claim while there is room. Local admission happens here, before the attempt task starts, so two claims can
    /// never both believe the same slot is free.
    pub async fn try_claim_all(self: &Arc<Self>) {
        loop {
            if self.adm.paused().is_some() || self.adm.draining() {
                return;
            }
            let req = self.claim_req();
            if req.slots.heavy_free == 0 && req.slots.light_free == 0 {
                return;
            }
            let reply = match self.link.claim(&req).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::debug!("claim failed: {e}");
                    return;
                }
            };
            let Some(job) = reply.job.clone() else { return };
            let need = Need {
                id: job.id.clone(),
                attempt: reply.attempt,
                class: job.spec.class,
                ram_mb: self.ram_for(&job.spec),
                lock: job.spec.lock.clone(),
                pid: std::process::id(),
            };
            let admit = self.adm.try_start(&need);
            let declined = admit.is_err();
            let task = tokio::spawn(self.clone().run_attempt(reply, admit));
            self.tasks.lock().unwrap_or_else(|e| e.into_inner()).push(task);
            if declined {
                return; // the decline carries its reasons; do not spin on the same job
            }
        }
    }

    /// Claim on wake-ups, after every finished attempt, and at least every `claim_retry_ms` (covers lost wakes).
    pub async fn claim_loop(self: Arc<Self>) {
        loop {
            let since = self.last_decline.lock().unwrap_or_else(|e| e.into_inner()).map(|t| t.elapsed());
            if let Some(s) = since
                && s < Duration::from_secs(2)
            {
                tokio::time::sleep(Duration::from_secs(2) - s).await;
            }
            self.try_claim_all().await;
            tokio::select! {
                _ = self.claim_notify.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(self.st.claim_retry_ms)) => {}
            }
        }
    }

    pub fn presence(&self) -> Presence {
        let (slots, admission) = self.adm.summary();
        let info = self.rt.describe();
        let mut caps = self.st.caps.clone();
        for c in info.caps {
            if !caps.contains(&c) {
                caps.push(c);
            }
        }
        Presence {
            v: PROTOCOL_VERSION,
            from: format!("worker:{}", self.st.host),
            host: self.st.host.clone(),
            version: self.st.version.clone(),
            runtime: info.name,
            cost_tier: info.cost_tier,
            caps,
            slots,
            admission,
            running: self.adm.running_ids(),
            leases: vec![],
            metrics: Metrics::default(),
            paused: self.adm.paused(),
            draining: self.adm.draining(),
            kill_switch: false,
            t: rfc3339(crate::attempt::now_ms()),
        }
    }

    /// Presence every `presence_interval_ms` and on any admission change.
    pub async fn presence_loop(self: Arc<Self>) {
        loop {
            let p = self.presence();
            if let Err(e) = self.link.put_presence(serde_json::to_vec(&p).expect("json")).await {
                tracing::debug!("presence not delivered: {e}");
            }
            tokio::select! {
                _ = self.presence_notify.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(self.st.presence_interval_ms)) => {}
            }
        }
    }

    /// Apply a control push (PROTOCOL.md 9).
    pub fn handle_ctl(&self, c: &Ctl) {
        match c.action.as_str() {
            "cancel" => {
                if let (Some(job), Some(tok)) = (&c.job, &c.lease_token)
                    && let Some(f) = self.attempts.lock().unwrap_or_else(|e| e.into_inner()).find(job, tok)
                {
                    f.cancel.store(true, Ordering::SeqCst);
                }
            }
            "kill_stale" => {
                // Like `cancel`, it must name the lease it is about: nobody can stop a job by guessing its id.
                if let (Some(job), Some(tok)) = (&c.job, &c.lease_token)
                    && let Some(f) = self.attempts.lock().unwrap_or_else(|e| e.into_inner()).find(job, tok)
                {
                    f.stale.store(true, Ordering::SeqCst);
                }
            }
            "pause" => self.adm.pause(c.reason.as_deref().unwrap_or("paused by owner")),
            "resume" => {
                self.adm.resume();
                self.claim_notify.notify_one();
            }
            "drain" => self.adm.drain(),
            other => tracing::warn!("unknown control action {other:?}"),
        }
        self.presence_notify.notify_one();
    }

    /// After a restart: any attempt the previous worker process left behind is ours to finish. Whatever is still
    /// running of it is killed (never two attempts), and the attempt is reported `lost_local`; a pending `finished`
    /// is delivered as recorded.
    pub async fn recover(self: &Arc<Self>) {
        let runs = self.st.work_dir.join("runs");
        let Ok(jobs) = std::fs::read_dir(&runs) else { return };
        for job in jobs.flatten() {
            let Ok(attempts) = std::fs::read_dir(job.path()) else { continue };
            for att in attempts.flatten() {
                let dir = att.path();
                let Some(state) = std::fs::read(state_path(&dir)).ok().and_then(|b| serde_json::from_slice::<AttemptState>(&b).ok()) else {
                    continue;
                };
                let finished = match state.finished.clone() {
                    Some(f) => f,
                    None => {
                        self.kill_leftovers(&state).await;
                        corral_proto::api::Finished {
                            outcome: "lost_local".into(),
                            exit_code: None,
                            signal: None,
                            reason: None,
                            detail: Some("the worker restarted and found this attempt without having seen it end".into()),
                            footprint_peak_mb: None,
                            duration_s: None,
                            disturbed: false,
                            log_dropped_bytes: 0,
                            log_seq: Default::default(),
                        }
                    }
                };
                let req = EventReq { lease_token: state.token.clone(), attempt: state.attempt, event: AttemptEvent::Finished(finished) };
                match self.link.event(&state.job, &req).await {
                    Ok(_) => {}
                    Err(e) if matches!(e.error, ErrorCode::StaleLease | ErrorCode::NotFound) => {}
                    Err(e) => {
                        tracing::warn!(job = %state.job, "recovery report not delivered, will retry on next start: {e}");
                        continue;
                    }
                }
                let _ = std::fs::remove_dir_all(&dir);
            }
        }
    }

    /// Stop everything a previous run of this worker left behind for this attempt, whether or not its leader is
    /// still alive: the members of the recorded process group (started no earlier than the leader, so a stranger
    /// that reused the number is never touched) and, on Linux, any process carrying the attempt's
    /// `CORRAL_JOB_ID`/`CORRAL_ATTEMPT`. TERM first, then KILL for whatever is still the same process after the
    /// grace period (never two attempts, R-QUEUE-06, IT-25).
    async fn kill_leftovers(&self, s: &AttemptState) {
        use crate::probe::{AttemptKey, PsTable, attempt_survivors};
        use rustix::process::{Pid, Signal, kill_process, kill_process_group};
        let key = AttemptKey { job: &s.job, attempt: s.attempt, pgid: s.pgid, leader_start: &s.start };
        let signal_all = |procs: &[crate::probe::Proc], sig: Signal| {
            if procs.iter().any(|p| p.pgid == s.pgid)
                && let Some(g) = Pid::from_raw(s.pgid as i32)
            {
                let _ = kill_process_group(g, sig);
            }
            for p in procs.iter().filter(|p| p.pgid != s.pgid) {
                if let Some(pid) = Pid::from_raw(p.pid as i32) {
                    let _ = kill_process(pid, sig);
                }
            }
        };
        let first = attempt_survivors(&PsTable, &key);
        if first.is_empty() {
            return;
        }
        signal_all(&first, Signal::TERM);
        tokio::time::sleep(Duration::from_millis(self.st.kill_grace_ms.min(2000))).await;
        let still: Vec<_> =
            attempt_survivors(&PsTable, &key).into_iter().filter(|p| first.iter().any(|f| f.pid == p.pid && f.start == p.start)).collect();
        signal_all(&still, Signal::KILL);
    }

    /// Remove finished runs older than `keep_failed_runs`.
    pub fn sweep_runs(&self) {
        let runs = self.st.work_dir.join("runs");
        let Ok(jobs) = std::fs::read_dir(&runs) else { return };
        for job in jobs.flatten() {
            let Ok(attempts) = std::fs::read_dir(job.path()) else { continue };
            for att in attempts.flatten() {
                let old = att
                    .metadata()
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .and_then(|m| m.elapsed().ok())
                    .is_some_and(|a| a.as_millis() as u64 > self.st.keep_failed_ms);
                if old && !state_path(&att.path()).exists() {
                    let _ = std::fs::remove_dir_all(att.path());
                }
            }
            let _ = std::fs::remove_dir(job.path());
        }
    }

    pub fn spawn_task(&self, t: JoinHandle<()>) {
        self.tasks.lock().unwrap_or_else(|e| e.into_inner()).push(t);
    }

    /// Spawn the long-running loops. `abort_all` stops them (and running attempts' tasks).
    pub fn spawn_loops(self: &Arc<Self>) {
        let mut t = self.tasks.lock().unwrap_or_else(|e| e.into_inner());
        t.push(tokio::spawn(self.clone().claim_loop()));
        t.push(tokio::spawn(self.clone().presence_loop()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// R-QUEUE-06 (M1 review): the registry keeps attempts of one job apart, and a control push must name the
    /// current lease token (`kill_stale` included).
    #[test]
    fn registry_is_per_attempt_and_token_checked() {
        let mut r = Registry::default();
        let (old, new) = (Arc::new(AttemptFlags::new("tok-1")), Arc::new(AttemptFlags::new("tok-2")));
        r.0.insert(("j-1".into(), 1), old);
        r.0.insert(("j-1".into(), 2), new.clone());
        assert!(r.find("j-1", "tok-2").is_some_and(|f| Arc::ptr_eq(f, &new)));
        assert!(r.find("j-1", "guess").is_none(), "a wrong token finds nothing");
        assert!(r.find("j-2", "tok-2").is_none());
        r.0.remove(&("j-1".to_string(), 1)); // the old attempt cleans up late
        assert!(r.find("j-1", "tok-2").is_some(), "the new attempt is still registered");
    }
}
