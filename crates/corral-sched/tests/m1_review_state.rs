//! Regression tests for the M1 review (state machine and claim lifecycle). In-memory store, explicit clock.
//! Each test names the requirement IDs it proves.

use corral_proto::api::*;
use corral_proto::job::{Class, JobSpec, JobState};
use corral_sched::settings::Settings;
use corral_sched::store::Store;

const T0: i64 = 1_795_000_000_000;

fn creq() -> ClaimReq {
    serde_json::from_value(serde_json::json!({
        "version":"0.1.0","runtime":"native","cost_tier":0,"caps":[],
        "slots":{"heavy_free":1,"light_free":2},
        "admission":{"heavy_ok":true,"exclusive_ok":false,"profiles_ok":["default"],"admissible_ram_mb":100000,"blocked":[]},
        "running":[],"leases":[]
    }))
    .unwrap()
}

fn hb(tok: &str) -> HbReq {
    serde_json::from_value(serde_json::json!({"lease_token":tok,"attempt":1,"phase":"preparing","log_seq":{}})).unwrap()
}

fn job() -> JobSpec {
    let mut s = JobSpec::simple(&["true"]);
    s.class = Class::Heavy;
    s.ram_mb = Some(64);
    s
}

fn setup() -> (Store, Settings, String, String) {
    let st = Settings::for_dir(std::env::temp_dir().join("corral-review-state-unit"));
    let mut s = Store::open_memory();
    let id = s.submit(job(), "agent:laptop", T0, None).unwrap().job.id;
    let c = s.claim("w1", &creq(), T0, &st).unwrap().unwrap();
    (s, st, id, c.token)
}

fn declined() -> AttemptEvent {
    AttemptEvent::Declined { reasons: vec![DeclineReason { check: "lock".into(), reason: "busy".into() }] }
}

/// Tick once per second, like the real scheduler loop, so every deadline is crossed in order.
fn tick_to(s: &mut Store, st: &Settings, from: i64, to: i64) {
    let mut t = from;
    while t < to {
        t += 1000;
        s.tick(t, st).unwrap();
    }
}

/// R-QUEUE-03 / R-QUEUE-05 / R-ADM-03: a claim that never started and is revived by a heartbeat goes back to
/// `claimed` (not `running`), so `started` and `declined` are still accepted and the claim deadline still applies.
#[test]
fn revived_unstarted_claim_returns_to_claimed_and_can_start_or_decline() {
    let (mut s, st, id, tok) = setup();
    let t = T0 + st.lease_ttl_ms + 1;
    s.tick(t, &st).unwrap();
    assert_eq!(s.get(&id).unwrap().unwrap().state, JobState::Orphaned);
    s.heartbeat("w1", &id, &hb(&tok), t + 1, &st).unwrap();
    assert_eq!(s.get(&id).unwrap().unwrap().state, JobState::Claimed, "never started: not running");
    // Started after the revival is accepted and moves to running.
    s.attempt_event("w1", &id, &tok, AttemptEvent::Started { pid: 1, pgid: 1 }, t + 2, &st).unwrap();
    assert_eq!(s.get(&id).unwrap().unwrap().state, JobState::Running);

    // And a decline after a revival requeues without counting an attempt.
    let (mut s, st, id, tok) = setup();
    s.tick(t, &st).unwrap();
    s.attempt_event("w1", &id, &tok, declined(), t + 1, &st).unwrap();
    let j = s.get(&id).unwrap().unwrap();
    assert_eq!((j.state, j.attempts), (JobState::Queued, 0));
}

/// R-QUEUE-05: a started attempt that is revived is still `running` (the timeout backstop stays armed).
#[test]
fn revived_started_attempt_returns_to_running() {
    let (mut s, st, id, tok) = setup();
    s.attempt_event("w1", &id, &tok, AttemptEvent::Started { pid: 1, pgid: 1 }, T0 + 1, &st).unwrap();
    let t = T0 + st.lease_ttl_ms + 2;
    s.tick(t, &st).unwrap();
    assert_eq!(s.get(&id).unwrap().unwrap().state, JobState::Orphaned);
    s.heartbeat("w1", &id, &hb(&tok), t + 1, &st).unwrap();
    let j = s.get(&id).unwrap().unwrap();
    assert_eq!(j.state, JobState::Running);
    assert!(j.started_at.is_some());
}

/// R-ADM-03 / R-QUEUE-04: a cancel that raced with a decline ends `cancelled`, it is never requeued.
#[test]
fn cancel_then_decline_ends_cancelled() {
    let (mut s, st, id, tok) = setup();
    s.cancel(&id, "agent:laptop", Some("stop"), T0 + 1).unwrap();
    s.attempt_event("w1", &id, &tok, declined(), T0 + 2, &st).unwrap();
    assert_eq!(s.get(&id).unwrap().unwrap().state, JobState::Cancelled);
    assert!(s.claim("w1", &creq(), T0 + 3, &st).unwrap().is_none(), "a cancelled job is not claimable");
}

/// R-QUEUE-06: a cancel that raced with `lost_local` (retry allowed) ends `cancelled`, it is never requeued.
#[test]
fn cancel_then_lost_local_ends_cancelled() {
    let (mut s, st, id, tok) = setup();
    s.attempt_event("w1", &id, &tok, AttemptEvent::Started { pid: 1, pgid: 1 }, T0 + 1, &st).unwrap();
    s.cancel(&id, "agent:laptop", Some("stop"), T0 + 2).unwrap();
    let f: Finished = serde_json::from_value(serde_json::json!({"outcome":"lost_local"})).unwrap();
    s.attempt_event("w1", &id, &tok, AttemptEvent::Finished(f), T0 + 3, &st).unwrap();
    assert_eq!(s.get(&id).unwrap().unwrap().state, JobState::Cancelled);
}

/// PROTOCOL.md 6.1: the claim deadline requeues a silent claim as a decline (`claim_timeout`), also when a cancel
/// was requested (then it ends `cancelled`).
#[test]
fn silent_claim_times_out_as_a_decline_not_a_lost_attempt() {
    let (mut s, st, id, _tok) = setup();
    tick_to(&mut s, &st, T0, T0 + st.claim_deadline_ms + 2000);
    let j = s.get(&id).unwrap().unwrap();
    assert_eq!((j.state, j.attempts), (JobState::Queued, 0), "claim_timeout is a decline, not a lost attempt");
    assert!(j.last_decline.as_deref().is_some_and(|d| d.contains("claim_timeout")), "{:?}", j.last_decline);
}

#[test]
fn silent_claim_with_cancel_requested_ends_cancelled() {
    let (mut s, st, id, _tok) = setup();
    s.cancel(&id, "agent:laptop", None, T0 + 1).unwrap();
    tick_to(&mut s, &st, T0, T0 + st.claim_deadline_ms + 2000);
    assert_eq!(s.get(&id).unwrap().unwrap().state, JobState::Cancelled);
}
