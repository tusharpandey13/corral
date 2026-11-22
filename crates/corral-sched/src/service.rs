//! Request handling: authorization, validation, idempotency, effects. No network code here, so the whole API is
//! unit-testable with a fake clock and an in-memory store.

use crate::Clock;
use crate::settings::Settings;
use crate::store::{Effect, Store, StoreError};
use corral_proto::api::*;
use corral_proto::envelope::{ApiError, Envelope, ErrorCode, body, decode_request, encode_error, encode_reply};
use corral_proto::ident::Identity;
use corral_proto::job::{Class, Input, JobSpec, JobState};
use corral_proto::subjects::{self, WorkSubject};
use corral_proto::token::{self, Token};
use corral_proto::util::rfc3339;
use corral_proto::{PROTOCOL_VERSION, subjects as sj};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::mpsc;

/// Something to publish after a request committed.
#[derive(Debug, Clone)]
pub enum Out {
    /// Core NATS publish.
    Core { subject: String, payload: Vec<u8> },
    /// JetStream publish (stream captures the subject).
    Js { subject: String, payload: Vec<u8> },
    /// Create the push consumers that stream one job to a follower (the NATS layer does it; followers cannot).
    Follow { job: String, deliver: String },
}

#[derive(Default, Debug, Clone)]
pub struct Counters {
    pub claims: BTreeMap<String, u64>,
    pub declines: BTreeMap<String, u64>,
    pub kills: BTreeMap<String, u64>,
    pub lease_expiries: u64,
    pub lost: u64,
}

#[derive(Debug, Clone)]
pub struct PresenceEntry {
    pub presence: Option<Presence>,
    pub seen_ms: i64,
}

pub struct Service {
    pub settings: Settings,
    store: Mutex<Store>,
    clock: Arc<dyn Clock>,
    presence: Mutex<HashMap<String, PresenceEntry>>,
    key: Vec<u8>,
    out: mpsc::UnboundedSender<Out>,
    counters: Mutex<Counters>,
    nats_ok: AtomicBool,
}

const KNOWN_PROFILES: [&str; 3] = ["default", "measurement", "gui"];
const MUTATING: [&str; 8] =
    ["submit", "cancel", "resubmit", "admin.pause", "admin.resume", "admin.drain", "admin.retention", "admin.kill_switch"];

fn ok_json<T: Serialize>(v: &T) -> Result<Vec<u8>, ApiError> {
    Ok(encode_reply(v))
}

impl Service {
    pub fn new(settings: Settings, store: Store, clock: Arc<dyn Clock>, key: Vec<u8>) -> (Arc<Service>, mpsc::UnboundedReceiver<Out>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let svc = Service {
            settings,
            store: Mutex::new(store),
            clock,
            presence: Mutex::new(HashMap::new()),
            key,
            out: tx,
            counters: Mutex::new(Counters::default()),
            nats_ok: AtomicBool::new(false),
        };
        (Arc::new(svc), rx)
    }

    pub fn now(&self) -> i64 {
        self.clock.now_ms()
    }

    pub fn db(&self) -> MutexGuard<'_, Store> {
        self.store.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn set_nats_ok(&self, ok: bool) {
        self.nats_ok.store(ok, Ordering::SeqCst);
    }

    pub fn nats_ok(&self) -> bool {
        self.nats_ok.load(Ordering::SeqCst)
    }

    pub fn counters(&self) -> Counters {
        self.counters.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn bump(&self, f: impl FnOnce(&mut Counters)) {
        f(&mut self.counters.lock().unwrap_or_else(|e| e.into_inner()));
    }

    // ---- presence ----

    pub fn presence_update(&self, host: &str, p: Option<Presence>) {
        let now = self.now();
        self.presence.lock().unwrap_or_else(|e| e.into_inner()).insert(host.to_string(), PresenceEntry { presence: p, seen_ms: now });
    }

    pub fn presence_snapshot(&self) -> Vec<(String, Option<Presence>, i64)> {
        let mut v: Vec<_> = self
            .presence
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(h, e)| (h.clone(), e.presence.clone(), e.seen_ms))
            .collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }

    pub fn worker_fresh(&self, host: &str) -> bool {
        let now = self.now();
        self.presence.lock().unwrap_or_else(|e| e.into_inner()).get(host).is_some_and(|e| now - e.seen_ms < self.settings.presence_ttl_ms)
    }

    // ---- effects ----

    fn send(&self, o: Out) {
        let _ = self.out.send(o);
    }

    /// Publish side effects. Called with the store lock released.
    pub fn emit(&self, fx: Vec<Effect>) {
        for e in fx {
            match e {
                Effect::Job(ev) => {
                    match ev.state.as_str() {
                        "killed" => self.bump(|c| *c.kills.entry(ev.reason.clone().unwrap_or_default()).or_default() += 1),
                        "lost" => self.bump(|c| c.lost += 1),
                        "orphaned" => self.bump(|c| c.lease_expiries += 1),
                        _ => {}
                    }
                    self.send(Out::Js { subject: sj::job_event(&ev.id), payload: serde_json::to_vec(&ev).expect("json") });
                }
                Effect::Ctl { worker, ctl } => {
                    self.send(Out::Core { subject: sj::ctl(&worker), payload: serde_json::to_vec(&ctl).expect("json") })
                }
                Effect::Wake { job } => {
                    let caps = self.db().get(&job).ok().flatten().map(|j| j.spec.caps).unwrap_or_default();
                    for (host, p, _) in self.presence_snapshot() {
                        if p.as_ref().is_none_or(|p| caps.iter().all(|c| p.caps.contains(c))) {
                            let w = Wake { v: PROTOCOL_VERSION, reason: "queued".into(), job: Some(job.clone()) };
                            self.send(Out::Core { subject: sj::wake(&host), payload: serde_json::to_vec(&w).expect("json") });
                        }
                    }
                }
            }
        }
    }

    // ---- entry point ----

    /// Handle one request on `corral.api.*` or `corral.work.*` and return the reply bytes.
    pub fn handle(&self, subject: &str, payload: &[u8]) -> Vec<u8> {
        match self.handle_inner(subject, payload) {
            Ok(b) => b,
            Err(e) => encode_error(&e),
        }
    }

    fn handle_inner(&self, subject: &str, payload: &[u8]) -> Result<Vec<u8>, ApiError> {
        let (env, value) = decode_request(payload)?;
        let from =
            Identity::parse(&env.from).ok_or_else(|| ApiError::bad(format!("bad from {:?}", env.from)).with("field", json!("from")))?;
        let now = self.now();
        if let Some(a) = subjects::parse_api(subject) {
            // R-SEC-03: the identity pinned by the subject must equal the payload's `from`.
            let pinned = Identity::from_slug(a.who).ok_or_else(|| ApiError::forbidden(format!("bad identity in subject: {}", a.who)))?;
            if pinned != from {
                self.audit(now, &env.from, subject, None, "forbidden", "from does not match subject identity");
                return Err(ApiError::forbidden(format!("from {from} does not match the identity {pinned} pinned by the subject")));
            }
            if !matches!(from, Identity::Agent(_) | Identity::Owner) {
                return Err(ApiError::forbidden("only agents and the owner use the client API"));
            }
            return self.api(&from, a.verb, &env, &value, subject, now);
        }
        if let Some(w) = subjects::parse_work(subject) {
            let (worker, _) = match &w {
                WorkSubject::Claim { worker } => (*worker, None::<&str>),
                WorkSubject::Heartbeat { worker, job } | WorkSubject::Event { worker, job } => (*worker, Some(*job)),
            };
            let pinned = Identity::Worker(worker.to_string());
            if pinned != from {
                self.audit(now, &env.from, subject, None, "forbidden", "from does not match subject identity");
                return Err(ApiError::forbidden(format!("from {from} does not match the identity {pinned} pinned by the subject")));
            }
            return self.work(worker, w, &env, &value, subject, now);
        }
        Err(ApiError::bad(format!("unknown subject {subject}")))
    }

    fn audit(&self, now: i64, who: &str, subject: &str, job: Option<&str>, kind: &str, detail: &str) {
        self.db().audit_write(now, who, subject, job, kind, detail);
    }

    // ---- client API ----

    fn api(&self, from: &Identity, verb: &str, env: &Envelope, value: &Value, subject: &str, now: i64) -> Result<Vec<u8>, ApiError> {
        let who = from.to_string();
        let mutating = MUTATING.contains(&verb);
        if mutating && let Some(r) = self.db().remembered(&who, &env.req_id).map_err(ApiError::from)? {
            return Ok(r);
        }
        self.db().audit_write(now, &who, subject, None, "api", verb);
        let reply = match verb {
            "submit" => self.submit(from, env, value, now)?,
            "get" => {
                let r: GetReq = body(value)?;
                let st = self.db();
                let j = st.get(&r.id).map_err(ApiError::from)?.ok_or_else(|| ApiError::not_found(format!("no job {}", r.id)))?;
                let waiting = st.explain(&j, &self.presence_snapshot(), now, &self.settings).map_err(ApiError::from)?;
                encode_reply(&st.view(&j, waiting))
            }
            "list" => {
                let r: ListReq = body(value)?;
                let st = self.db();
                let rows = st.list(r.all, r.state.as_deref(), r.limit.unwrap_or(50)).map_err(ApiError::from)?;
                let snap = self.presence_snapshot();
                let jobs = rows
                    .iter()
                    .map(|j| {
                        let waiting = st.explain(j, &snap, now, &self.settings).unwrap_or_default();
                        st.view(j, waiting)
                    })
                    .collect();
                encode_reply(&ListReply { jobs })
            }
            "workers" => self.workers(now)?,
            "follow" => {
                let r: FollowReq = body(value)?;
                let prefix = format!("{}.", from.inbox_prefix());
                let ok_chars = r.deliver.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
                if !r.deliver.starts_with(&prefix)
                    || r.deliver.len() > 120
                    || !ok_chars
                    || r.deliver.ends_with('.')
                    || r.deliver.contains("..")
                {
                    return Err(ApiError::bad(format!("deliver must be a plain subject under {prefix}")).with("field", json!("deliver")));
                }
                self.db().get(&r.id).map_err(ApiError::from)?.ok_or_else(|| ApiError::not_found(format!("no job {}", r.id)))?;
                self.send(Out::Follow { job: r.id, deliver: r.deliver });
                encode_reply(&json!({ "ok": true }))
            }
            "cancel" => self.cancel(from, env, value, now)?,
            "resubmit" => self.resubmit(from, env, value, now)?,
            "artifacts" => self.artifacts(from, value, now)?,
            "admin.pause" | "admin.resume" | "admin.drain" | "admin.kill_switch" | "admin.retention" => {
                self.admin(from, verb, env, value, now)?
            }
            v if v.starts_with("lease.") => {
                return Err(ApiError::new(ErrorCode::Unavailable, "interactive leases are not available yet (milestone M5)"));
            }
            other => return Err(ApiError::bad(format!("unknown api verb {other:?}"))),
        };
        if mutating {
            self.db().remember(&who, &env.req_id, &reply, now).map_err(ApiError::from)?;
        }
        Ok(reply)
    }

    /// R-SEC-04: an action named in `policy.require_grant` needs a grant in `authz`. Phase 2 verifies grants; until a
    /// verifier exists any `authz` is invalid, so the same error is returned with the same schema.
    fn require(&self, env: &Envelope, action: &str, binding: &str) -> Result<(), ApiError> {
        if self.settings.require_grant.iter().any(|a| a == action) {
            let _ = env.authz.as_ref();
            return Err(ApiError::new(ErrorCode::GrantRequired, format!("action {action} needs an approval grant"))
                .with("action", json!(format!("corral:{action}")))
                .with("binding_message", json!(binding)));
        }
        Ok(())
    }

    fn submit(&self, from: &Identity, env: &Envelope, value: &Value, now: i64) -> Result<Vec<u8>, ApiError> {
        let spec: JobSpec = body(value)?;
        let st = &self.settings;
        let field = |f: &str, m: &str| ApiError::bad(m.to_string()).with("field", json!(f));
        if spec.kind != "command" {
            return Err(field("kind", "only kind \"command\" is supported in this milestone"));
        }
        if spec.parent.is_some() {
            return Err(field("parent", "interactive lease steps are not available yet (milestone M5)"));
        }
        if spec.cmd.is_empty() || spec.cmd[0].is_empty() {
            return Err(field("cmd", "cmd must not be empty"));
        }
        if spec.class == Class::Heavy && spec.ram_mb.is_none_or(|r| r == 0) {
            return Err(field("ram_mb", "heavy jobs must declare ram_mb (there is no silent default)"));
        }
        if spec.artifacts_max_mb > st.artifacts_cap_mb {
            return Err(field("artifacts_max_mb", &format!("artifacts_max_mb is at most {}", st.artifacts_cap_mb)));
        }
        if spec.priority > 100 {
            return Err(field("priority", "priority is 0-100"));
        }
        if spec.priority > 80 && !from.is_owner() {
            return Err(ApiError::forbidden("priority above 80 is owner-only"));
        }
        if spec.priority > 80 {
            self.require(env, "priority_high", &format!("submit job with priority {}", spec.priority))?;
        }
        if spec.timeout_s == 0 || spec.timeout_s > st.max_timeout_s {
            return Err(field("timeout_s", &format!("timeout_s must be 1..={}", st.max_timeout_s)));
        }
        if !KNOWN_PROFILES.contains(&spec.profile.as_str()) {
            return Err(field("profile", &format!("unknown profile {:?}", spec.profile)));
        }
        if spec.max_cost_tier != 0 {
            return Err(field("max_cost_tier", "v1 accepts only max_cost_tier 0"));
        }
        if spec.cwd.starts_with('/') || spec.cwd.split('/').any(|c| c == "..") {
            return Err(field("cwd", "cwd must be relative and stay inside the input root"));
        }
        if spec.lock == "none" && spec.class == Class::Heavy {
            if !from.is_owner() {
                return Err(ApiError::forbidden("lock = none on a heavy job is owner-only"));
            }
            self.require(env, "lock_none", "submit a heavy job without a host lock")?;
        }
        if spec.exclusive {
            self.require(env, "exclusive", "submit an exclusive (benchmark-grade) job")?;
        }
        match &spec.input {
            Input::Git { repo, sha, .. } if repo.is_empty() || sha.is_empty() => {
                return Err(field("input", "git input needs repo and a resolved sha"));
            }
            Input::Snapshot { sha256, bytes, .. } => {
                if sha256.len() != 64 || !sha256.chars().all(|c| c.is_ascii_hexdigit()) {
                    return Err(field("input.sha256", "sha256 must be 64 hex characters"));
                }
                if *bytes == 0 || *bytes > st.input_max_mb * 1024 * 1024 {
                    return Err(ApiError::new(ErrorCode::TooLarge, format!("snapshot exceeds the {} MB cap", st.input_max_mb)));
                }
            }
            _ => {}
        }
        let mut spec = spec;
        spec.retry_of = None;
        let out = self.db().submit(spec, &from.to_string(), now, None).map_err(ApiError::from)?;
        let j = &out.job;
        let mut reply =
            SubmitReply { id: j.id.clone(), state: j.state.as_str().into(), upload_url: None, upload_token: None, unplaceable: None };
        if let Some(u) = &j.upload_id {
            reply.upload_url = Some(format!("/v1/inputs/{u}"));
            reply.upload_token =
                Some(token::mint(&self.key, &Token { job: u.clone(), verb: "input:put".into(), lease: None, exp: now / 1000 + 3600 }));
        }
        self.emit(out.effects);
        let _ = env;
        ok_json(&reply)
    }

    fn cancel(&self, from: &Identity, env: &Envelope, value: &Value, now: i64) -> Result<Vec<u8>, ApiError> {
        let r: CancelReq = body(value)?;
        {
            let j = self.db().get(&r.id).map_err(ApiError::from)?.ok_or_else(|| ApiError::not_found(format!("no job {}", r.id)))?;
            if !from.is_owner() && j.submitter != from.to_string() {
                return Err(ApiError::forbidden("only the submitter or the owner may cancel a job"));
            }
            if j.submitter != from.to_string() {
                self.require(env, "cancel_other", &format!("cancel job {} submitted by {}", r.id, j.submitter))?;
            }
        }
        let (j, fx) = self.db().cancel(&r.id, &from.to_string(), r.reason.as_deref(), now).map_err(ApiError::from)?;
        self.emit(fx);
        ok_json(&CancelReply { id: j.id, state: j.state.as_str().into(), cancel_requested: j.cancel_requested })
    }

    fn resubmit(&self, from: &Identity, env: &Envelope, value: &Value, now: i64) -> Result<Vec<u8>, ApiError> {
        let r: ResubmitReq = body(value)?;
        let src = self.db().resubmit_source(&r.id).map_err(ApiError::from)?;
        if !src.state.is_terminal() {
            return Err(ApiError::new(ErrorCode::Conflict, format!("job is still {}", src.state.as_str())));
        }
        if src.state == JobState::Killed {
            // R-QUEUE-08: an agent must not quietly restart what a guard stopped.
            if !from.is_owner() {
                return Err(ApiError::forbidden("resubmitting a killed job is owner-only"));
            }
            self.require(env, "resubmit_killed", &format!("resubmit killed job {} ({})", src.id, src.reason.clone().unwrap_or_default()))?;
        } else if !from.is_owner() && src.submitter != from.to_string() {
            return Err(ApiError::forbidden("only the submitter or the owner may resubmit a job"));
        }
        let mut spec = src.spec.clone();
        spec.retry_of = Some(src.id.clone());
        let reuse = match (&spec.input, &src.upload_id) {
            (Input::Snapshot { .. }, Some(u)) => {
                if !self.settings.data_dir.join("inputs").join(u).exists() {
                    return Err(ApiError::new(ErrorCode::Conflict, "the snapshot input has been deleted by retention; submit again"));
                }
                Some(u.clone())
            }
            _ => None,
        };
        let out = self.db().submit(spec, &from.to_string(), now, reuse.as_deref()).map_err(ApiError::from)?;
        let id = out.job.id.clone();
        self.emit(out.effects);
        ok_json(&SubmitReply { id, state: out.job.state.as_str().into(), upload_url: None, upload_token: None, unplaceable: None })
    }

    fn artifacts(&self, from: &Identity, value: &Value, now: i64) -> Result<Vec<u8>, ApiError> {
        let r: ArtifactsReq = body(value)?;
        let j = self.db().get(&r.id).map_err(ApiError::from)?.ok_or_else(|| ApiError::not_found(format!("no job {}", r.id)))?;
        if !from.is_owner() && j.submitter != from.to_string() {
            return Err(ApiError::forbidden("only the submitter or the owner may fetch a job's artifacts"));
        }
        let attempt = r.attempt.unwrap_or(j.attempts);
        let exp = now / 1000 + 24 * 3600;
        let mk = |verb: &str| token::mint(&self.key, &Token { job: j.id.clone(), verb: verb.into(), lease: None, exp });
        ok_json(&ArtifactsReply {
            url: format!("/v1/jobs/{}/artifacts?attempt={attempt}", j.id),
            token: mk("artifacts:get"),
            state: j.artifacts_state.clone().unwrap_or_else(|| "none".into()),
            log_token: Some(mk("log:get")),
            attempt,
        })
    }

    fn workers(&self, now: i64) -> Result<Vec<u8>, ApiError> {
        let st = self.db();
        let mut rows = Vec::new();
        for (host, p, seen) in self.presence_snapshot() {
            let (paused, draining) = st.worker_flags(&host).map_err(ApiError::from)?;
            let flags = match (paused, draining) {
                (None, false) => None,
                (p, d) => Some(
                    format!("{}{}", p.map(|r| format!("paused: {r}")).unwrap_or_default(), if d { " draining" } else { "" })
                        .trim()
                        .to_string(),
                ),
            };
            rows.push(WorkerRow { online: now - seen < self.settings.presence_ttl_ms, host, presence: p, flags });
        }
        let queue_paused = st.queue_paused().map_err(ApiError::from)?;
        ok_json(&WorkersReply { workers: rows, queue_paused })
    }

    fn admin(&self, from: &Identity, verb: &str, env: &Envelope, value: &Value, now: i64) -> Result<Vec<u8>, ApiError> {
        if !from.is_owner() {
            return Err(ApiError::forbidden("admin actions are owner-only"));
        }
        self.require(env, "admin", &format!("admin action {verb}"))?;
        let r: AdminReq = body(value)?;
        let who = from.to_string();
        let detail = match verb {
            "admin.pause" | "admin.resume" => {
                let pause = verb == "admin.pause";
                match &r.worker {
                    None => {
                        self.db().set_queue_paused(pause, now, &who).map_err(ApiError::from)?;
                        format!("queue {}", if pause { "paused" } else { "resumed" })
                    }
                    Some(w) => {
                        let (_, draining) = self.db().worker_flags(w).map_err(ApiError::from)?;
                        let reason = r.reason.clone().unwrap_or_else(|| "paused by owner".into());
                        self.db()
                            .set_worker_flags(w, pause.then_some(reason.as_str()), if pause { draining } else { false }, now, &who)
                            .map_err(ApiError::from)?;
                        self.ctl(w, if pause { "pause" } else { "resume" }, &who, r.reason.as_deref());
                        format!("worker {w} {}", if pause { "paused" } else { "resumed" })
                    }
                }
            }
            "admin.drain" => {
                let w = r.worker.clone().ok_or_else(|| ApiError::bad("drain needs a worker").with("field", json!("worker")))?;
                let (paused, _) = self.db().worker_flags(&w).map_err(ApiError::from)?;
                self.db().set_worker_flags(&w, paused.as_deref(), true, now, &who).map_err(ApiError::from)?;
                self.ctl(&w, "drain", &who, r.reason.as_deref());
                format!("worker {w} draining")
            }
            "admin.retention" => {
                let n = crate::gc::run(self, now).map_err(|e| ApiError::new(ErrorCode::Unavailable, e.to_string()))?;
                format!("retention removed {n} jobs")
            }
            _ => return Err(ApiError::new(ErrorCode::Unavailable, "the kill switch is part of the guard (milestone M2)")),
        };
        ok_json(&AdminReply { ok: true, detail: Some(detail) })
    }

    fn ctl(&self, worker: &str, action: &str, by: &str, reason: Option<&str>) {
        let c = Ctl {
            v: PROTOCOL_VERSION,
            action: action.into(),
            job: None,
            lease_token: None,
            by: Some(by.into()),
            reason: reason.map(str::to_string),
        };
        self.send(Out::Core { subject: sj::ctl(worker), payload: serde_json::to_vec(&c).expect("json") });
    }

    // ---- worker API ----

    fn work(&self, worker: &str, w: WorkSubject<'_>, env: &Envelope, value: &Value, subject: &str, now: i64) -> Result<Vec<u8>, ApiError> {
        match w {
            WorkSubject::Claim { .. } => {
                if let Some(r) = self.db().remembered(&env.from, &env.req_id).map_err(ApiError::from)? {
                    return Ok(r);
                }
                let req: ClaimReq = body(value)?;
                self.touch_presence(worker, &req, now);
                let outcome = self.db().claim(worker, &req, now, &self.settings).map_err(ApiError::from)?;
                let reply = match outcome {
                    None => {
                        self.bump(|c| *c.claims.entry("none".into()).or_default() += 1);
                        ClaimReply {
                            job: None,
                            attempt: 0,
                            lease_token: None,
                            lease_ttl_s: 0,
                            input_url: None,
                            artifact_url: None,
                            http_token: None,
                            input_token: None,
                            retry_after_s: self.settings.retry_after_s,
                        }
                    }
                    Some(o) => {
                        self.bump(|c| *c.claims.entry("job".into()).or_default() += 1);
                        let exp = now / 1000 + o.job.spec.timeout_s as i64 + 3600;
                        let (input_url, input_token) = match (&o.job.spec.input, &o.job.upload_id) {
                            (Input::Snapshot { .. }, Some(u)) => (
                                Some(format!("/v1/inputs/{u}")),
                                Some(token::mint(
                                    &self.key,
                                    &Token { job: u.clone(), verb: "input:get".into(), lease: Some(o.token.clone()), exp },
                                )),
                            ),
                            _ => (None, None),
                        };
                        let http_token = token::mint(
                            &self.key,
                            &Token { job: o.job.id.clone(), verb: "artifacts:put".into(), lease: Some(o.token.clone()), exp },
                        );
                        let view = {
                            let st = self.db();
                            st.view(&o.job, vec![])
                        };
                        let reply = ClaimReply {
                            attempt: o.job.attempts,
                            lease_token: Some(o.token.clone()),
                            lease_ttl_s: (self.settings.lease_ttl_ms / 1000) as u64,
                            input_url,
                            artifact_url: Some(format!("/v1/jobs/{}/{}/artifacts", o.job.id, o.job.attempts)),
                            http_token: Some(http_token),
                            input_token,
                            retry_after_s: 0,
                            job: Some(view),
                        };
                        self.emit(o.effects);
                        reply
                    }
                };
                let bytes = encode_reply(&reply);
                self.db().remember(&env.from, &env.req_id, &bytes, now).map_err(ApiError::from)?;
                Ok(bytes)
            }
            WorkSubject::Heartbeat { job, .. } => {
                let hb: HbReq = body(value)?;
                let out = self.db().heartbeat(worker, job, &hb, now, &self.settings).map_err(ApiError::from)?;
                self.emit(out.effects);
                ok_json(&HbReply { ok: true, lease_until: Some(rfc3339(out.lease_until)), cancel: out.cancel })
            }
            WorkSubject::Event { job, .. } => {
                if let Some(r) = self.db().remembered(&env.from, &env.req_id).map_err(ApiError::from)? {
                    return Ok(r);
                }
                let ev: EventReq = body(value)?;
                if let AttemptEvent::Declined { reasons } = &ev.event {
                    let first = reasons.first().map(|r| r.check.clone()).unwrap_or_else(|| "unknown".into());
                    self.bump(|c| *c.declines.entry(first).or_default() += 1);
                }
                let fx = self.db().attempt_event(worker, job, &ev.lease_token, ev.event, now, &self.settings).map_err(|e| match e {
                    StoreError::Api(a) => a,
                    other => other.into(),
                })?;
                self.emit(fx);
                let bytes = encode_reply(&EventReply { ok: true });
                self.db().remember(&env.from, &env.req_id, &bytes, now).map_err(ApiError::from)?;
                let _ = subject;
                Ok(bytes)
            }
        }
    }

    fn touch_presence(&self, worker: &str, req: &ClaimReq, now: i64) {
        let p = Presence {
            v: PROTOCOL_VERSION,
            from: format!("worker:{worker}"),
            host: worker.into(),
            version: req.version.clone(),
            runtime: req.runtime.clone(),
            cost_tier: req.cost_tier,
            caps: req.caps.clone(),
            slots: req.slots.clone(),
            admission: req.admission.clone(),
            running: req.running.clone(),
            leases: req.leases.clone(),
            metrics: Metrics::default(),
            paused: req.paused.clone(),
            draining: req.draining,
            kill_switch: false,
            t: rfc3339(now),
        };
        self.presence_update(worker, Some(p));
    }

    // ---- periodic ----

    /// Lease expiry, grace, `not_after`, claim deadline, upload expiry (R-QUEUE-05, R-QUEUE-07, R-EXEC-02).
    pub fn tick(&self) -> Result<(), ApiError> {
        let now = self.now();
        let fx = self.db().tick(now, &self.settings).map_err(ApiError::from)?;
        self.emit(fx);
        Ok(())
    }

    /// Record a progress message (R-IO-06) for `status`.
    pub fn progress(&self, worker: &str, job: &str, pct: u8) {
        let _ = self.db().set_progress(job, worker, pct);
    }

    // ---- paths ----

    pub fn jobs_dir(&self) -> PathBuf {
        self.settings.data_dir.join("jobs")
    }
    pub fn inputs_dir(&self) -> PathBuf {
        self.settings.data_dir.join("inputs")
    }

    /// Would any known, online worker ever take a job needing `ram` MB? Used only for diagnostics.
    pub fn explain_for(&self, id: &str) -> Vec<corral_proto::job::Waiting> {
        let now = self.now();
        let st = self.db();
        st.get(id)
            .ok()
            .flatten()
            .map(|j| st.explain(&j, &self.presence_snapshot(), now, &self.settings).unwrap_or_default())
            .unwrap_or_default()
    }
}
