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

pub struct Worker<L: SchedLink, R: Runtime> {
    pub link: Arc<L>,
    pub rt: Arc<R>,
    pub adm: Arc<Admitter>,
    pub st: Arc<WorkerSettings>,
    pub claim_notify: Notify,
    pub presence_notify: Notify,
    attempts: Mutex<HashMap<String, Arc<AttemptFlags>>>,
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
            attempts: Mutex::new(HashMap::new()),
            last_decline: Mutex::new(None),
            tasks: Mutex::new(Vec::new()),
        })
    }

    pub(crate) fn register(&self, id: &str, f: Arc<AttemptFlags>) {
        self.attempts.lock().unwrap_or_else(|e| e.into_inner()).insert(id.into(), f);
    }
    pub(crate) fn unregister(&self, id: &str) {
        self.attempts.lock().unwrap_or_else(|e| e.into_inner()).remove(id);
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
                class: job.spec.class,
                ram_mb: job.spec.ram(),
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
                    && let Some(f) = self.attempts.lock().unwrap_or_else(|e| e.into_inner()).get(job)
                    && &f.token == tok
                {
                    f.cancel.store(true, Ordering::SeqCst);
                }
            }
            "kill_stale" => {
                if let Some(job) = &c.job
                    && let Some(f) = self.attempts.lock().unwrap_or_else(|e| e.into_inner()).get(job)
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

    /// After a restart: any attempt the previous worker process left behind is ours to finish. A live process
    /// group whose pid and start time we recorded is killed (never two attempts), and the attempt is reported
    /// `lost_local`; a pending `finished` is delivered as recorded.
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
                        let now_start = crate::probe::start_time_of(state.pid);
                        if state.pid != 0 && !now_start.is_empty() && now_start == state.start {
                            self.kill_recorded(&state);
                        }
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

    /// Signal a process group recorded by a previous run of this worker. Only called after the pid's start time
    /// matched what we recorded, so pid reuse cannot make us signal a stranger.
    fn kill_recorded(&self, s: &AttemptState) {
        use rustix::process::{Pid, Signal, kill_process_group};
        if let Some(g) = Pid::from_raw(s.pgid as i32) {
            let _ = kill_process_group(g, Signal::TERM);
            std::thread::sleep(Duration::from_millis(self.st.kill_grace_ms.min(2000)));
            if crate::probe::start_time_of(s.pid) == s.start {
                let _ = kill_process_group(g, Signal::KILL);
            }
        }
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
