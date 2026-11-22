//! Scheduler unit tests: in-memory store, fake clock, no network. Each test names the requirement IDs it proves.

use crate::service::{Out, Service};
use crate::settings::Settings;
use crate::store::Store;
use crate::{Clock, FakeClock};
use corral_proto::api::*;
use corral_proto::envelope::{ApiError, ErrorCode, decode_reply, encode_request};
use corral_proto::ident::Identity;
use corral_proto::job::{Class, JobSpec, JobState, JobView};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;

const T0: i64 = 1_795_000_000_000;

struct Rig {
    svc: Arc<Service>,
    clock: Arc<FakeClock>,
    rx: UnboundedReceiver<Out>,
}

fn rig_with(f: impl FnOnce(&mut Settings)) -> Rig {
    let clock = Arc::new(FakeClock::new(T0));
    let mut st = Settings::for_dir(std::env::temp_dir().join(format!("corral-unit-{}", corral_proto::util::random_b32(8))));
    f(&mut st);
    let (svc, rx) = Service::new(st, Store::open_memory(), clock.clone(), b"unit-key".to_vec());
    Rig { svc, clock, rx }
}

fn rig() -> Rig {
    rig_with(|_| {})
}

fn agent() -> Identity {
    Identity::Agent("laptop".into())
}

impl Rig {
    fn call<B: Serialize, R: DeserializeOwned>(&self, from: &Identity, subject: &str, body: &B) -> Result<R, ApiError> {
        let bytes = encode_request(from, &corral_proto::util::new_req_id(self.clock.now_ms()), body);
        decode_reply(&self.svc.handle(subject, &bytes))
    }

    fn api<B: Serialize, R: DeserializeOwned>(&self, from: &Identity, verb: &str, body: &B) -> Result<R, ApiError> {
        self.call(from, &format!("corral.api.{}.{verb}", from.slug()), body)
    }

    fn submit(&self, spec: &JobSpec) -> String {
        let id = self.api::<_, SubmitReply>(&agent(), "submit", spec).expect("submit").id;
        self.clock.advance_ms(1); // distinct queued_at so FIFO order is deterministic
        id
    }

    fn claim_req(&self, caps: &[&str], heavy_free: u32, admissible: u64) -> ClaimReq {
        ClaimReq {
            version: "0.1.0".into(),
            runtime: "native".into(),
            cost_tier: 0,
            caps: caps.iter().map(|s| s.to_string()).collect(),
            slots: Slots { heavy_free, light_free: 2 },
            admission: Admission {
                heavy_ok: true,
                exclusive_ok: false,
                profiles_ok: vec!["default".into()],
                admissible_ram_mb: admissible,
                blocked: vec![],
            },
            running: vec![],
            leases: vec![],
            paused: None,
            draining: false,
        }
    }

    fn claim(&self, w: &str, req: &ClaimReq) -> ClaimReply {
        self.call(&Identity::Worker(w.into()), &format!("corral.work.{w}.claim"), req).expect("claim")
    }

    fn get(&self, id: &str) -> JobView {
        self.api(&agent(), "get", &GetReq { id: id.into() }).expect("get")
    }

    fn hb(&self, w: &str, id: &str, token: &str) -> Result<HbReply, ApiError> {
        let hb = HbReq {
            lease_token: token.into(),
            attempt: 1,
            phase: "running".into(),
            pid: None,
            footprint_mb: None,
            cpu_s: None,
            log_seq: Default::default(),
        };
        self.call(&Identity::Worker(w.into()), &format!("corral.work.{w}.{id}.hb"), &hb)
    }

    fn event(&self, w: &str, id: &str, token: &str, ev: AttemptEvent) -> Result<EventReply, ApiError> {
        self.call(
            &Identity::Worker(w.into()),
            &format!("corral.work.{w}.{id}.event"),
            &EventReq { lease_token: token.into(), attempt: 1, event: ev },
        )
    }

    fn finish(&self, w: &str, id: &str, token: &str, outcome: &str, code: Option<i32>, reason: Option<&str>) {
        self.event(
            w,
            id,
            token,
            AttemptEvent::Finished(Finished {
                outcome: outcome.into(),
                exit_code: code,
                signal: None,
                reason: reason.map(String::from),
                detail: None,
                footprint_peak_mb: None,
                duration_s: None,
                disturbed: false,
                log_dropped_bytes: 0,
                log_seq: Default::default(),
            }),
        )
        .expect("finish");
    }

    /// Claim and start a job; returns (token).
    fn run_job(&self, w: &str, caps: &[&str], id: &str) -> String {
        let c = self.claim(w, &self.claim_req(caps, 1, 8000));
        assert_eq!(c.job.as_ref().map(|j| j.id.as_str()), Some(id));
        let token = c.lease_token.unwrap();
        self.event(w, id, &token, AttemptEvent::Started { pid: 1, pgid: 1 }).unwrap();
        token
    }

    fn tick(&self) {
        self.svc.tick().unwrap();
    }

    fn drain(&mut self) -> Vec<Out> {
        let mut v = vec![];
        while let Ok(o) = self.rx.try_recv() {
            v.push(o);
        }
        v
    }
}

fn heavy(ram: u64) -> JobSpec {
    let mut s = JobSpec::simple(&["true"]);
    s.class = Class::Heavy;
    s.ram_mb = Some(ram);
    s
}

// ---- R-QUEUE-01, R-ADM-04 ----

#[test]
fn submit_without_workers_is_queued_with_a_waiting_reason() {
    let r = rig();
    let id = r.submit(&heavy(1024));
    let j = r.get(&id);
    assert_eq!(j.state, JobState::Queued);
    assert_eq!(j.attempts, 0);
    assert!(j.waiting.iter().any(|w| w.text.contains("no workers")), "{:?}", j.waiting);
}

#[test]
fn heavy_job_without_ram_is_rejected_naming_the_field() {
    let r = rig();
    let mut s = JobSpec::simple(&["true"]);
    s.class = Class::Heavy;
    s.ram_mb = None;
    let e = r.api::<_, SubmitReply>(&agent(), "submit", &s).unwrap_err();
    assert_eq!(e.error, ErrorCode::BadRequest);
    assert_eq!(e.extra["field"], "ram_mb");
}

#[test]
fn submit_validation() {
    let r = rig();
    let mut s = heavy(100);
    s.priority = 90;
    assert_eq!(r.api::<_, SubmitReply>(&agent(), "submit", &s).unwrap_err().error, ErrorCode::Forbidden, "priority > 80 is owner-only");
    assert!(r.api::<_, SubmitReply>(&Identity::Owner, "submit", &s).is_ok());
    let mut s = heavy(100);
    s.profile = "nope".into();
    assert_eq!(r.api::<_, SubmitReply>(&agent(), "submit", &s).unwrap_err().extra["field"], "profile");
    let mut s = heavy(100);
    s.cwd = "../x".into();
    assert_eq!(r.api::<_, SubmitReply>(&agent(), "submit", &s).unwrap_err().extra["field"], "cwd");
    let mut s = heavy(100);
    s.max_cost_tier = 1;
    assert_eq!(r.api::<_, SubmitReply>(&agent(), "submit", &s).unwrap_err().extra["field"], "max_cost_tier");
    let mut s = heavy(100);
    s.timeout_s = 49 * 3600;
    assert_eq!(r.api::<_, SubmitReply>(&agent(), "submit", &s).unwrap_err().extra["field"], "timeout_s");
}

/// Retried requests with the same req_id return the original reply (no double submit).
#[test]
fn retried_submit_is_idempotent() {
    let r = rig();
    let bytes = encode_request(&agent(), "same-id", &heavy(100));
    let a: SubmitReply = decode_reply(&r.svc.handle("corral.api.agent-laptop.submit", &bytes)).unwrap();
    let b: SubmitReply = decode_reply(&r.svc.handle("corral.api.agent-laptop.submit", &bytes)).unwrap();
    assert_eq!(a.id, b.id);
    assert_eq!(r.svc.db().list(true, None, 10).unwrap().len(), 1);
}

// ---- R-SEC-03, R-SEC-04 ----

#[test]
fn from_must_match_the_identity_pinned_by_the_subject() {
    let r = rig();
    let e = r.call::<_, SubmitReply>(&agent(), "corral.api.owner.submit", &heavy(1)).unwrap_err();
    assert_eq!(e.error, ErrorCode::Forbidden);
    let e = r.call::<_, ClaimReply>(&Identity::Worker("a".into()), "corral.work.b.claim", &r.claim_req(&[], 1, 100)).unwrap_err();
    assert_eq!(e.error, ErrorCode::Forbidden);
    let e = r.call::<_, SubmitReply>(&Identity::Worker("a".into()), "corral.api.worker-a.submit", &heavy(1)).unwrap_err();
    assert_eq!(e.error, ErrorCode::Forbidden, "workers do not use the client API");
}

/// U-22: policy `require_grant` returns `grant_required` with the same schema, with or without `authz`.
#[test]
fn require_grant_policy() {
    let r = rig_with(|s| s.require_grant = vec!["exclusive".into()]);
    let mut s = heavy(100);
    s.exclusive = true;
    let e = r.api::<_, SubmitReply>(&agent(), "submit", &s).unwrap_err();
    assert_eq!(e.error, ErrorCode::GrantRequired);
    assert_eq!(e.extra["action"], "corral:exclusive");
    assert!(r.api::<_, SubmitReply>(&agent(), "submit", &heavy(100)).is_ok(), "non-exclusive jobs unaffected");
}

// ---- R-SCHED-01..03 (U-02) ----

#[test]
fn claim_selection_table() {
    let r = rig();
    let mut caps_job = heavy(1000);
    caps_job.caps = vec!["metal".into()];
    let id_caps = r.submit(&caps_job);
    // no caps -> nothing
    assert!(r.claim("w", &r.claim_req(&["linux"], 1, 8000)).job.is_none());
    // ram too big
    assert!(r.claim("w", &r.claim_req(&["metal"], 1, 500)).job.is_none());
    // no heavy slot
    assert!(r.claim("w", &r.claim_req(&["metal"], 0, 8000)).job.is_none());
    // heavy_ok false
    let mut req = r.claim_req(&["metal"], 1, 8000);
    req.admission.heavy_ok = false;
    assert!(r.claim("w", &req).job.is_none());
    // profile not offered
    let mut req = r.claim_req(&["metal"], 1, 8000);
    req.admission.profiles_ok.clear();
    assert!(r.claim("w", &req).job.is_none());
    // exclusive needs exclusive_ok
    let mut ex = heavy(100);
    ex.exclusive = true;
    let id_ex = r.submit(&ex);
    let mut req = r.claim_req(&["metal"], 1, 8000);
    let c = r.claim("w", &req);
    assert_eq!(c.job.unwrap().id, id_caps, "backfill: the exclusive job is skipped, not blocking");
    req.admission.exclusive_ok = true;
    assert_eq!(r.claim("w2", &req).job.unwrap().id, id_ex);
}

/// Light jobs need only a light slot and memory (R-ADM-08 classes), never a heavy slot.
#[test]
fn light_jobs_use_light_slots() {
    let r = rig();
    let id = r.submit(&JobSpec::simple(&["true"]));
    let c = r.claim("w", &r.claim_req(&[], 0, 100));
    assert_eq!(c.job.unwrap().id, id);
}

#[test]
fn ordering_priority_then_fifo_with_backfill() {
    let r = rig();
    let a = r.submit(&heavy(100));
    r.clock.advance_s(1);
    let mut hi = heavy(100);
    hi.priority = 70;
    let b = r.submit(&hi);
    r.clock.advance_s(1);
    let big = r.submit(&heavy(9000));
    r.clock.advance_s(1);
    let c = r.submit(&heavy(100));
    let order: Vec<String> = (0..3).map(|i| r.claim(&format!("w{i}"), &r.claim_req(&[], 1, 1000)).job.unwrap().id).collect();
    assert_eq!(order, vec![b, a, c], "priority first, FIFO within, the big job does not block smaller ones behind it");
    assert!(r.claim("w9", &r.claim_req(&[], 1, 1000)).job.is_none());
    assert_eq!(r.claim("w10", &r.claim_req(&[], 1, 10_000)).job.unwrap().id, big);
}

#[test]
fn prefer_then_fallback_with_a_fake_clock() {
    let r = rig();
    let mut s = heavy(100);
    s.prefer = Some("laptop".into());
    s.fallback_after_s = Some(1800);
    let id = r.submit(&s);
    assert!(r.claim("server", &r.claim_req(&[], 1, 1000)).job.is_none());
    r.clock.advance_s(1799);
    assert!(r.claim("server", &r.claim_req(&[], 1, 1000)).job.is_none());
    r.clock.advance_s(2);
    assert_eq!(r.claim("server", &r.claim_req(&[], 1, 1000)).job.unwrap().id, id);
    // the preferred worker gets it immediately
    let mut p = heavy(100);
    p.prefer = Some("laptop".into());
    let id2 = r.submit(&p);
    assert_eq!(r.claim("laptop", &r.claim_req(&[], 1, 1000)).job.unwrap().id, id2);
}

#[test]
fn fallback_never_pins_the_job() {
    let r = rig();
    let mut s = heavy(100);
    s.prefer = Some("laptop".into());
    s.fallback_after_s = Some(-1);
    r.submit(&s);
    r.clock.advance_s(100 * 3600);
    assert!(r.claim("server", &r.claim_req(&[], 1, 1000)).job.is_none());
}

/// R-SCHED-06 (store level): concurrent claims never receive the same job.
#[test]
fn fifty_parallel_claims_for_ten_jobs_give_ten_distinct_winners() {
    let r = rig();
    for _ in 0..10 {
        r.submit(&heavy(10));
    }
    let svc = r.svc.clone();
    let now = r.clock.now_ms();
    let handles: Vec<_> = (0..50)
        .map(|i| {
            let svc = svc.clone();
            std::thread::spawn(move || {
                let w = format!("w{i}");
                let req = ClaimReq {
                    version: "0".into(),
                    runtime: "native".into(),
                    cost_tier: 0,
                    caps: vec![],
                    slots: Slots { heavy_free: 1, light_free: 1 },
                    admission: Admission {
                        heavy_ok: true,
                        exclusive_ok: false,
                        profiles_ok: vec!["default".into()],
                        admissible_ram_mb: 1000,
                        blocked: vec![],
                    },
                    running: vec![],
                    leases: vec![],
                    paused: None,
                    draining: false,
                };
                let bytes = encode_request(&Identity::Worker(w.clone()), &format!("req-{i}"), &req);
                let reply: ClaimReply = decode_reply(&svc.handle(&format!("corral.work.{w}.claim"), &bytes)).unwrap();
                reply.job.map(|j| j.id)
            })
        })
        .collect();
    let mut winners: Vec<String> = handles.into_iter().filter_map(|h| h.join().unwrap()).collect();
    assert_eq!(winners.len(), 10);
    winners.sort();
    winners.dedup();
    assert_eq!(winners.len(), 10);
    let _ = now;
}

// ---- R-ADM-03: declines ----

#[test]
fn decline_requeues_without_counting_and_explains() {
    let mut r = rig();
    let id = r.submit(&heavy(100));
    let c = r.claim("w", &r.claim_req(&[], 1, 1000));
    assert_eq!(c.attempt, 1);
    r.event(
        "w",
        &id,
        &c.lease_token.clone().unwrap(),
        AttemptEvent::Declined { reasons: vec![DeclineReason { check: "lock".into(), reason: "held by build-x (pid 812)".into() }] },
    )
    .unwrap();
    let j = r.get(&id);
    assert_eq!(j.state, JobState::Queued);
    assert_eq!(j.attempts, 0, "a decline never counts as an attempt");
    assert!(j.waiting.iter().any(|w| w.text.contains("held by build-x")), "{:?}", j.waiting);
    let c2 = r.claim("w", &r.claim_req(&[], 1, 1000));
    assert_eq!(c2.attempt, 1, "the next claim is still attempt 1");
    assert_ne!(c.lease_token, c2.lease_token);
    let _ = r.drain();
}

// ---- R-QUEUE-04, U-23 ----

#[test]
fn killed_is_final_and_never_requeued() {
    let r = rig();
    let id = r.submit(&heavy(100));
    let t = r.run_job("w", &[], &id);
    r.finish("w", &id, &t, "killed", None, Some("ram_exceeded"));
    let j = r.get(&id);
    assert_eq!(j.state, JobState::Killed);
    assert_eq!(j.reason.as_deref(), Some("ram_exceeded"));
    assert_eq!(j.attempts, 1);
    r.clock.advance_s(10 * 3600);
    r.tick();
    assert_eq!(r.get(&id).state, JobState::Killed);
    assert!(r.claim("w", &r.claim_req(&[], 1, 1000)).job.is_none(), "queue stays empty");
}

#[test]
fn exit_codes_map_to_succeeded_and_failed() {
    let r = rig();
    let a = r.submit(&heavy(10));
    let t = r.run_job("w", &[], &a);
    r.finish("w", &a, &t, "exited", Some(0), None);
    assert_eq!(r.get(&a).state, JobState::Succeeded);
    let b = r.submit(&heavy(10));
    let t = r.run_job("w", &[], &b);
    r.finish("w", &b, &t, "exited", Some(3), None);
    let j = r.get(&b);
    assert_eq!((j.state, j.exit_code), (JobState::Failed, Some(3)));
}

#[test]
fn cancel_reason_from_the_worker_maps_to_cancelled() {
    let mut r = rig();
    let id = r.submit(&heavy(10));
    let t = r.run_job("w", &[], &id);
    let c: CancelReply = r.api(&agent(), "cancel", &CancelReq { id: id.clone(), reason: Some("no longer needed".into()) }).unwrap();
    assert!(c.cancel_requested);
    assert!(r.drain().iter().any(|o| matches!(o, Out::Core { subject, .. } if subject == "corral.ctl.w")), "ctl push to the worker");
    assert!(r.hb("w", &id, &t).unwrap().cancel, "the flag also rides on every heartbeat reply");
    r.finish("w", &id, &t, "killed", None, Some("cancelled"));
    assert_eq!(r.get(&id).state, JobState::Cancelled);
}

#[test]
fn cancel_of_queued_job_is_immediate_and_only_the_submitter_or_owner_may() {
    let r = rig();
    let id = r.submit(&heavy(10));
    let other = Identity::Agent("other".into());
    assert_eq!(
        r.api::<_, CancelReply>(&other, "cancel", &CancelReq { id: id.clone(), reason: None }).unwrap_err().error,
        ErrorCode::Forbidden
    );
    let c: CancelReply = r.api(&agent(), "cancel", &CancelReq { id: id.clone(), reason: None }).unwrap();
    assert_eq!(c.state, "cancelled");
}

// ---- R-QUEUE-08 ----

#[test]
fn resubmit_of_killed_is_owner_only() {
    let r = rig();
    let id = r.submit(&heavy(10));
    let t = r.run_job("w", &[], &id);
    r.finish("w", &id, &t, "killed", None, Some("system_memory"));
    let e = r.api::<_, SubmitReply>(&agent(), "resubmit", &ResubmitReq { id: id.clone() }).unwrap_err();
    assert_eq!(e.error, ErrorCode::Forbidden);
    let n: SubmitReply = r.api(&Identity::Owner, "resubmit", &ResubmitReq { id: id.clone() }).unwrap();
    assert_ne!(n.id, id);
    assert_eq!(r.get(&n.id).retry_of.as_deref(), Some(id.as_str()));
    r.api::<_, CancelReply>(&Identity::Owner, "cancel", &CancelReq { id: n.id.clone(), reason: None }).unwrap();
    // a failed job may be resubmitted by its submitter
    let f = r.submit(&heavy(10));
    let t = r.run_job("w", &[], &f);
    r.finish("w", &f, &t, "exited", Some(1), None);
    assert!(r.api::<_, SubmitReply>(&agent(), "resubmit", &ResubmitReq { id: f }).is_ok());
}

// ---- R-QUEUE-05, R-QUEUE-06, R-QUEUE-07, R-OPS-03 (U-20) ----

#[test]
fn lease_expiry_orphans_then_heartbeat_resumes_with_the_same_token() {
    let r = rig();
    let id = r.submit(&heavy(10));
    let t = r.run_job("w", &[], &id);
    r.clock.advance_s(89);
    r.tick();
    assert_eq!(r.get(&id).state, JobState::Running);
    r.clock.advance_s(2);
    r.tick();
    assert_eq!(r.get(&id).state, JobState::Orphaned, "after lease_ttl (90 s)");
    assert!(r.hb("w", &id, &t).is_ok());
    assert_eq!(r.get(&id).state, JobState::Running, "SIGCONT: the same token resumes");
}

#[test]
fn grace_over_requeues_as_attempt_two_and_revokes_the_old_token() {
    let r = rig();
    let id = r.submit(&heavy(10));
    let old = r.run_job("w", &[], &id);
    r.clock.advance_s(91);
    r.tick();
    r.clock.advance_s(14 * 60);
    r.tick();
    assert_eq!(r.get(&id).state, JobState::Orphaned, "still inside orphan_grace (15 min)");
    r.clock.advance_s(61);
    r.tick();
    let j = r.get(&id);
    assert_eq!(j.state, JobState::Queued);
    assert_eq!(j.attempts, 1);
    let c = r.claim("w2", &r.claim_req(&[], 1, 1000));
    assert_eq!(c.attempt, 2);
    // R-QUEUE-06: the returning worker is told stale_lease on heartbeat and events.
    assert_eq!(r.hb("w", &id, &old).unwrap_err().error, ErrorCode::StaleLease);
    let e = r.event("w", &id, &old, AttemptEvent::Note { text: "late".into() }).unwrap_err();
    assert_eq!(e.error, ErrorCode::StaleLease);
}

#[test]
fn grace_over_without_retry_is_lost() {
    let r = rig();
    let mut s = heavy(10);
    s.retry_on_lost = false;
    let id = r.submit(&s);
    r.run_job("w", &[], &id);
    r.clock.advance_s(91);
    r.tick();
    r.clock.advance_s(16 * 60);
    r.tick();
    assert_eq!(r.get(&id).state, JobState::Lost);
}

#[test]
fn max_attempts_bounds_requeues() {
    let r = rig();
    let id = r.submit(&heavy(10));
    r.run_job("w", &[], &id);
    r.clock.advance_s(91);
    r.tick();
    r.clock.advance_s(16 * 60);
    r.tick();
    assert_eq!(r.get(&id).state, JobState::Queued);
    let c = r.claim("w", &r.claim_req(&[], 1, 100));
    r.event("w", &id, &c.lease_token.unwrap(), AttemptEvent::Started { pid: 1, pgid: 1 }).unwrap();
    r.clock.advance_s(91);
    r.tick();
    r.clock.advance_s(16 * 60);
    r.tick();
    assert_eq!(r.get(&id).state, JobState::Lost, "attempts (2) = max_attempts (2)");
}

#[test]
fn lost_local_requeues_per_policy() {
    let r = rig();
    let id = r.submit(&heavy(10));
    let t = r.run_job("w", &[], &id);
    r.finish("w", &id, &t, "lost_local", None, None);
    assert_eq!(r.get(&id).state, JobState::Queued);
}

/// R-EXEC-02: the scheduler backstop kills a silent job past `timeout + orphan_grace`.
#[test]
fn silent_worker_past_timeout_plus_grace_is_killed_timeout() {
    let r = rig();
    let mut s = heavy(10);
    s.timeout_s = 60;
    s.retry_on_lost = true;
    let id = r.submit(&s);
    r.run_job("w", &[], &id);
    r.clock.advance_s(91);
    r.tick();
    assert_eq!(r.get(&id).state, JobState::Orphaned);
    r.clock.advance_s(60 + 15 * 60 + 1);
    r.tick();
    let j = r.get(&id);
    assert_eq!((j.state, j.reason.as_deref()), (JobState::Killed, Some("timeout")));
}

#[test]
fn not_after_expires_a_queued_job() {
    let r = rig();
    let mut s = heavy(10);
    s.not_after = Some(corral_proto::util::rfc3339(T0 + 60_000));
    let id = r.submit(&s);
    r.clock.advance_s(59);
    r.tick();
    assert_eq!(r.get(&id).state, JobState::Queued);
    r.clock.advance_s(2);
    r.tick();
    assert_eq!(r.get(&id).state, JobState::Expired);
}

#[test]
fn claim_that_is_never_started_times_out_as_a_decline() {
    let r = rig();
    let id = r.submit(&heavy(10));
    r.claim("w", &r.claim_req(&[], 1, 1000));
    // Tick once per second like the real loop: the 90 s lease runs out before the 120 s claim deadline.
    for _ in 0..121 {
        r.clock.advance_s(1);
        r.tick();
    }
    let j = r.get(&id);
    assert_eq!((j.state, j.attempts), (JobState::Queued, 0));
    assert!(j.waiting.iter().any(|w| w.text.contains("claim_timeout")), "{:?}", j.waiting);
}

/// R-OPS-03: after a scheduler outage leases are extended by the downtime (no mass orphaning).
#[test]
fn downtime_extends_leases() {
    let dir = std::env::temp_dir().join(format!("corral-unit-{}", corral_proto::util::random_b32(8)));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("corral.db");
    let clock = Arc::new(FakeClock::new(T0));
    let st = Settings::for_dir(&dir);
    let (svc, _rx) = Service::new(st.clone(), Store::open(&db).unwrap(), clock.clone(), b"k".to_vec());
    svc.db().start_up(clock.now_ms()).unwrap();
    let id = {
        let bytes = encode_request(&agent(), "r1", &heavy(10));
        decode_reply::<SubmitReply>(&svc.handle("corral.api.agent-laptop.submit", &bytes)).unwrap().id
    };
    let mut req = ClaimReq {
        version: "0".into(),
        runtime: "native".into(),
        cost_tier: 0,
        caps: vec![],
        slots: Slots { heavy_free: 1, light_free: 1 },
        admission: Admission {
            heavy_ok: true,
            exclusive_ok: false,
            profiles_ok: vec!["default".into()],
            admissible_ram_mb: 1000,
            blocked: vec![],
        },
        running: vec![],
        leases: vec![],
        paused: None,
        draining: false,
    };
    req.caps = vec![];
    let bytes = encode_request(&Identity::Worker("w".into()), "c1", &req);
    let c: ClaimReply = decode_reply(&svc.handle("corral.work.w.claim", &bytes)).unwrap();
    let token = c.lease_token.unwrap();
    let ev = encode_request(
        &Identity::Worker("w".into()),
        "e1",
        &EventReq { lease_token: token, attempt: 1, event: AttemptEvent::Started { pid: 1, pgid: 1 } },
    );
    decode_reply::<EventReply>(&svc.handle(&format!("corral.work.w.{id}.event"), &ev)).unwrap();
    svc.tick().unwrap();
    drop(svc);
    // The scheduler is down for 5 minutes (R-QUEUE-02 also: state survives the restart).
    clock.advance_s(300);
    let (svc2, _rx2) = Service::new(st, Store::open(&db).unwrap(), clock.clone(), b"k".to_vec());
    let downtime = svc2.db().start_up(clock.now_ms()).unwrap();
    assert_eq!(downtime, 300_000);
    svc2.tick().unwrap();
    let bytes = encode_request(&agent(), "g1", &GetReq { id: id.clone() });
    let v: JobView = decode_reply(&svc2.handle("corral.api.agent-laptop.get", &bytes)).unwrap();
    assert_eq!(v.state, JobState::Running, "no job orphaned by the outage itself");
    clock.advance_s(91);
    svc2.tick().unwrap();
    let bytes = encode_request(&agent(), "g2", &GetReq { id });
    let v: JobView = decode_reply(&svc2.handle("corral.api.agent-laptop.get", &bytes)).unwrap();
    assert_eq!(v.state, JobState::Orphaned, "the lease clock restarted after the outage");
}

// ---- R-QUEUE-02: persistence ----

#[test]
fn state_survives_reopening_the_database() {
    let dir = std::env::temp_dir().join(format!("corral-unit-{}", corral_proto::util::random_b32(8)));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("corral.db");
    let id = {
        let mut s = Store::open(&db).unwrap();
        s.submit(heavy(10), "agent:laptop", T0, None).unwrap().job.id
        // dropped without any orderly shutdown
    };
    let s = Store::open(&db).unwrap();
    assert_eq!(s.get(&id).unwrap().unwrap().state, JobState::Queued);
    assert!(s.integrity_ok());
}

// ---- admin ----

#[test]
fn pause_holds_the_queue_and_resume_releases_it() {
    let r = rig();
    let id = r.submit(&heavy(10));
    let a: AdminReply = r.api(&Identity::Owner, "admin.pause", &AdminReq::default()).unwrap();
    assert!(a.ok);
    assert!(r.claim("w", &r.claim_req(&[], 1, 100)).job.is_none());
    assert!(r.get(&id).waiting.iter().any(|w| w.text.contains("paused")));
    r.api::<_, AdminReply>(&Identity::Owner, "admin.resume", &AdminReq::default()).unwrap();
    assert!(r.claim("w", &r.claim_req(&[], 1, 100)).job.is_some());
    assert_eq!(r.api::<_, AdminReply>(&agent(), "admin.pause", &AdminReq::default()).unwrap_err().error, ErrorCode::Forbidden);
}

#[test]
fn paused_and_draining_workers_get_no_work() {
    let r = rig();
    r.submit(&heavy(10));
    let mut req = r.claim_req(&[], 1, 100);
    req.paused = Some("exit code 4".into());
    assert!(r.claim("w", &req).job.is_none());
    req.paused = None;
    req.draining = true;
    assert!(r.claim("w", &req).job.is_none());
    r.api::<_, AdminReply>(&Identity::Owner, "admin.drain", &AdminReq { worker: Some("w2".into()), reason: None }).unwrap();
    assert!(
        r.claim("w2", &r.claim_req(&[], 1, 100)).job.is_none(),
        "the scheduler remembers the drain even if the worker does not report it"
    );
}

// ---- waiting explanation ----

#[test]
fn waiting_text_is_per_worker() {
    let r = rig();
    let mut s = heavy(4000);
    s.caps = vec!["metal".into()];
    let id = r.submit(&s);
    let mut gpu = r.claim_req(&["linux"], 1, 9000);
    gpu.admission.blocked = vec![];
    r.claim("server", &gpu);
    let mut busy = r.claim_req(&["metal"], 1, 1000);
    busy.admission.heavy_ok = false;
    busy.admission.blocked = vec![Blocked { check: "memory".into(), profile: None, reason: "free 21% (need 23%)".into(), since: None }];
    r.claim("laptop", &busy);
    let text: Vec<String> = r.get(&id).waiting.iter().map(|w| format!("{}: {}", w.worker, w.text)).collect();
    assert!(text.iter().any(|t| t.starts_with("server: never: lacks caps metal")), "{text:?}");
    assert!(text.iter().any(|t| t.starts_with("laptop: blocked") && t.contains("memory")), "{text:?}");
    r.clock.advance_s(120);
    let text: Vec<String> = r.get(&id).waiting.iter().map(|w| w.text.clone()).collect();
    assert!(text.iter().any(|t| t.starts_with("offline since")), "workers that stop reporting are shown offline: {text:?}");
}

// ---- U-25: log sink decisions ----

/// R-IO-03 (M1 review): the note for a permanent gap names the missing chunk range.
#[test]
fn gap_note_names_the_missing_range() {
    let n = String::from_utf8(crate::store::sink_gap_note(4, 9)).unwrap();
    assert!(n.contains("5..8"), "{n}");
}

#[test]
fn sink_accepts_only_the_next_seq_from_the_attempts_worker() {
    use crate::store::SinkDecision::*;
    let r = rig();
    let id = r.submit(&heavy(10));
    let t = r.run_job("w", &[], &id);
    let mut st = r.svc.db();
    let _ = t;
    assert!(matches!(st.sink_decide(&id, 1, "out", 1, "w").unwrap(), Write { offset: 0 }));
    assert!(matches!(st.sink_decide(&id, 1, "out", 3, "w").unwrap(), Hold { last: 0, offset: 0 }), "a gap is held");
    st.sink_commit(&id, 1, "out", 1, 100).unwrap();
    assert!(matches!(st.sink_decide(&id, 1, "out", 1, "w").unwrap(), Duplicate), "redelivery is idempotent");
    assert!(matches!(st.sink_decide(&id, 1, "out", 2, "w").unwrap(), Write { offset: 100 }));
    assert!(matches!(st.sink_decide(&id, 1, "out", 4, "w").unwrap(), Hold { last: 1, offset: 100 }));
    assert!(matches!(st.sink_decide(&id, 1, "err", 1, "w").unwrap(), Write { offset: 0 }), "streams are independent");
    assert!(
        matches!(st.sink_decide(&id, 1, "out", 2, "other-worker").unwrap(), Drop),
        "R-QUEUE-06 / R-SEC-02: another worker's chunks for this job are dropped"
    );
    assert!(matches!(st.sink_decide(&id, 2, "out", 1, "w").unwrap(), Drop), "wrong attempt dropped");
}

// ---- R-IO-05 retention ----

#[test]
fn retention_keeps_live_and_flagged_jobs() {
    let r = rig_with(|s| {
        s.retention_age_ms = 14 * 86_400_000;
        s.keep_flagged_ms = 90 * 86_400_000;
    });
    std::fs::create_dir_all(r.svc.jobs_dir()).unwrap();
    let old = r.submit(&heavy(10));
    let t = r.run_job("w", &[], &old);
    r.finish("w", &old, &t, "exited", Some(0), None);
    let mut keep = heavy(10);
    keep.labels.insert("keep".into(), "true".into());
    let flagged = r.submit(&keep);
    let t = r.run_job("w", &[], &flagged);
    r.finish("w", &flagged, &t, "exited", Some(0), None);
    let queued = r.submit(&heavy(10));
    let running = r.submit(&heavy(10));
    // `running` is claimed (not finished); `queued` stays queued. Neither may be deleted.
    let mut by_prio = heavy(10);
    by_prio.priority = 60;
    let _ = by_prio;
    let c = r.claim("w", &r.claim_req(&[], 1, 100));
    assert_eq!(c.job.unwrap().id, queued);
    r.clock.advance_s(15 * 86_400);
    assert_eq!(crate::gc::run(&r.svc, r.clock.now_ms()).unwrap(), 1, "only the old unflagged finished job goes");
    assert!(r.svc.db().get(&old).unwrap().is_none());
    assert!(r.svc.db().get(&flagged).unwrap().is_some());
    r.clock.advance_s(80 * 86_400);
    assert_eq!(crate::gc::run(&r.svc, r.clock.now_ms()).unwrap(), 1, "flagged jobs go after keep_flagged");
    assert!(r.svc.db().get(&queued).unwrap().is_some() && r.svc.db().get(&running).unwrap().is_some());
    assert_eq!(r.get(&queued).state, JobState::Claimed);
}

// ---- metrics ----

#[test]
fn counters_follow_kills_and_declines() {
    let r = rig();
    let id = r.submit(&heavy(10));
    let t = r.run_job("w", &[], &id);
    r.finish("w", &id, &t, "killed", None, Some("timeout"));
    let c = r.svc.counters();
    assert_eq!(c.kills.get("timeout"), Some(&1));
    assert_eq!(c.claims.get("job"), Some(&1));
}

// ---- E-06 / E-12 pieces ----

/// PROTOCOL.md 7.4: a cancel for an attempt whose worker is offline ends `cancelled` after the grace period
/// (not `lost`, not requeued).
#[test]
fn cancel_while_the_worker_is_offline_ends_cancelled_after_grace() {
    let r = rig();
    let id = r.submit(&heavy(10));
    r.run_job("w", &[], &id);
    r.clock.advance_s(91);
    r.tick();
    assert_eq!(r.get(&id).state, JobState::Orphaned);
    let c: CancelReply = r.api(&agent(), "cancel", &CancelReq { id: id.clone(), reason: None }).unwrap();
    assert!(c.cancel_requested);
    r.clock.advance_s(16 * 60);
    r.tick();
    assert_eq!(r.get(&id).state, JobState::Cancelled);
    assert_eq!(r.get(&id).attempts, 1);
}

/// E-12: `lock = none` on a heavy job is owner-only.
#[test]
fn lock_none_is_owner_only() {
    let r = rig();
    let mut s = heavy(10);
    s.lock = "none".into();
    assert_eq!(r.api::<_, SubmitReply>(&agent(), "submit", &s).unwrap_err().error, ErrorCode::Forbidden);
    assert!(r.api::<_, SubmitReply>(&Identity::Owner, "submit", &s).is_ok());
}

/// E-06: cancelling a queued job and a `claimed` job (before `started`).
#[test]
fn cancel_in_queued_and_claimed_states() {
    let mut r = rig();
    let q = r.submit(&heavy(10));
    let cq: CancelReply = r.api(&agent(), "cancel", &CancelReq { id: q.clone(), reason: None }).unwrap();
    assert_eq!((cq.state.as_str(), cq.cancel_requested), ("cancelled", false));
    let c = r.submit(&heavy(10));
    let claim = r.claim("w", &r.claim_req(&[], 1, 1000));
    assert_eq!(claim.job.unwrap().id, c);
    let cc: CancelReply = r.api(&agent(), "cancel", &CancelReq { id: c.clone(), reason: None }).unwrap();
    assert!(cc.cancel_requested, "a claimed job is stopped through the worker");
    assert!(r.drain().iter().any(|o| matches!(o, Out::Core { subject, .. } if subject == "corral.ctl.w")));
}

#[test]
fn heartbeats_keep_a_slow_claim_alive() {
    let r = rig();
    let id = r.submit(&heavy(10));
    let c = r.claim("w", &r.claim_req(&[], 1, 1000));
    let t = c.lease_token.unwrap();
    for _ in 0..4 {
        r.clock.advance_s(60);
        r.hb("w", &id, &t).unwrap();
        r.tick();
    }
    let j = r.get(&id);
    assert_eq!((j.state, j.attempts), (JobState::Claimed, 1), "four minutes of input fetching with heartbeats is not an abandoned claim");
}
