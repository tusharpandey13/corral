//! M1 security and operations: IT-21 (nkeys, subject permissions, identity pinning), IT-22 (HTTP capability
//! tokens), IT-23 (/metrics, /healthz), config R-SEC-06.

mod common;

use async_nats::{ConnectOptions, Event, ServerError};
use common::*;
use corral_proto::bus::Bus;
use corral_proto::envelope::{ErrorCode, decode_reply, encode_request};
use corral_proto::ident::Identity;
use corral_proto::job::JobState;
use corral_proto::token::{self, Token};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Connect with raw async-nats and collect the server's permission errors.
async fn raw(nats: &Nats, id: &str) -> (async_nats::Client, Arc<Mutex<Vec<String>>>) {
    let errs = Arc::new(Mutex::new(Vec::new()));
    let e2 = errs.clone();
    let c = ConnectOptions::with_nkey(nats.seed(id))
        .event_callback(move |ev| {
            let e2 = e2.clone();
            async move {
                if let Event::ServerError(ServerError::Other(s)) = ev {
                    e2.lock().unwrap().push(s);
                }
            }
        })
        .connect(&nats.url)
        .await
        .unwrap();
    (c, errs)
}

/// IT-21 / R-SEC-01: anonymous connections are refused; a wrong key is refused.
#[tokio::test]
async fn anonymous_and_unknown_keys_are_refused() {
    let nats = Nats::start(&[]);
    assert!(Bus::connect(&nats.url, Identity::Owner, None, false, None).await.is_err());
    let stranger = nkeys::KeyPair::new_user().seed().unwrap();
    assert!(Bus::connect(&nats.url, Identity::Owner, Some(stranger), false, None).await.is_err());
}

/// IT-21 / R-SEC-02: each identity can publish and subscribe only on its own subjects; every forbidden attempt fails.
#[tokio::test]
async fn forbidden_publishes_and_subscriptions_are_refused_per_identity() {
    let nats = Nats::start(&[]);
    let cases: &[(&str, &[&str], &[&str])] = &[
        // identity, forbidden publish subjects, forbidden subscribe subjects
        (
            "agent:laptop",
            &[
                "corral.api.owner.submit",
                "corral.api.agent-other.submit",
                "corral.work.w1.claim",
                "corral.ctl.w1",
                "corral.log.w1.j.out",
                "corral.event.j-1",
            ],
            &["corral.ctl.w1", "corral.api.>", "corral.work.>", "_INBOX_owner.>"],
        ),
        (
            "worker:w1",
            &[
                "corral.api.owner.submit",
                "corral.work.w2.claim",
                "corral.work.w2.j-1.event",
                "corral.ctl.w2",
                "corral.log.w2.j.out",
                "$KV.corral_workers.w2",
            ],
            &["corral.ctl.w2", "corral.wake.w2", "corral.api.>", "_INBOX_scheduler.>"],
        ),
    ];
    for (id, pubs, subs) in cases {
        let (c, errs) = raw(&nats, id).await;
        for p in *pubs {
            c.publish(p.to_string(), "{}".into()).await.unwrap();
        }
        for s in *subs {
            let _ = c.subscribe(s.to_string()).await;
        }
        c.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let got = errs.lock().unwrap().clone();
        for p in *pubs {
            assert!(
                got.iter().any(|e| e.contains("Publish") && e.contains(&format!("\"{p}\""))),
                "{id}: publish to {p} was not refused; server said {got:?}"
            );
        }
        for s in *subs {
            assert!(
                got.iter().any(|e| e.contains("Subscription") && e.contains(&format!("\"{s}\""))),
                "{id}: subscribe to {s} was not refused; server said {got:?}"
            );
        }
    }
}

/// IT-21 / R-SEC-03: a `from` that disagrees with the identity pinned by the subject is `forbidden`.
#[tokio::test]
async fn payload_from_must_match_the_subject() {
    let nats = Nats::start(&[]);
    let _s = start_scheduler(&nats, &tmpdir("sched"), |_| {}).await;
    let bus = nats.bus("agent:laptop").await;
    let payload = encode_request(&Identity::Owner, "r1", &spec(&["true"]));
    let msg = bus.client.request("corral.api.agent-laptop.submit", payload.into()).await.unwrap();
    let e = decode_reply::<serde_json::Value>(&msg.payload).unwrap_err();
    assert_eq!(e.error, ErrorCode::Forbidden);
    // and the audit trail recorded it
    assert!(_s.running.service.db().audit_count("forbidden").unwrap() >= 1);
}

/// R-SEC-04 over the wire: an unsupported protocol version is answered `version_unsupported` with the list.
#[tokio::test]
async fn unsupported_version_is_answered() {
    let nats = Nats::start(&[]);
    let _s = start_scheduler(&nats, &tmpdir("sched"), |_| {}).await;
    let bus = nats.bus("agent:laptop").await;
    let msg =
        bus.client.request("corral.api.agent-laptop.get", r#"{"v":7,"from":"agent:laptop","req_id":"x","id":"j"}"#.into()).await.unwrap();
    let e = decode_reply::<serde_json::Value>(&msg.payload).unwrap_err();
    assert_eq!(e.error, ErrorCode::VersionUnsupported);
    assert_eq!(e.extra["supported"], serde_json::json!([1]));
}

/// IT-22 / R-SEC-05: a token for job A cannot read job B; expired tokens get 401; no token gets 401.
#[tokio::test]
async fn http_tokens_are_scoped_and_expire() {
    let nats = Nats::start(&[]);
    let s = start_scheduler(&nats, &tmpdir("sched"), |_| {}).await;
    let _w = start_worker(&nats, "w1", &s.http, &tmpdir("w1"), |_| {}).await;
    let c = client(&nats, "agent:laptop", &s.http).await;
    let mut sp = spec(&["bash", "-c", "echo data > a.txt"]);
    sp.artifacts = vec!["a.txt".into()];
    let a = c.submit(&sp, None).await.unwrap().id;
    let b = c.submit(&sp, None).await.unwrap().id;
    for id in [&a, &b] {
        until("done", 15, || async { c.get(id).await.unwrap().state == JobState::Succeeded }).await;
        until("uploaded", 15, || async { c.get(id).await.unwrap().artifacts_state.as_deref() == Some("uploaded") }).await;
    }
    let reply: corral_proto::api::ArtifactsReply =
        c.bus.call("corral.api.agent-laptop.artifacts", &corral_proto::api::ArtifactsReq { id: a.clone(), attempt: None }).await.unwrap();
    let get = |path: String, tok: Option<String>| {
        let url = format!("{}{}", s.http, path);
        tokio::task::spawn_blocking(move || {
            let mut r = ureq::get(&url).config().http_status_as_error(false).build();
            if let Some(t) = tok {
                r = r.header("Authorization", format!("Bearer {t}"));
            }
            r.call().map(|x| x.status().as_u16()).unwrap_or(0)
        })
    };
    assert_eq!(get(format!("/v1/jobs/{a}/artifacts?attempt=1"), Some(reply.token.clone())).await.unwrap(), 200);
    assert_eq!(
        get(format!("/v1/jobs/{b}/artifacts?attempt=1"), Some(reply.token.clone())).await.unwrap(),
        403,
        "a token for job A cannot read job B"
    );
    assert_eq!(get(format!("/v1/jobs/{a}/artifacts?attempt=1"), None).await.unwrap(), 401);
    assert_eq!(get(format!("/v1/jobs/{a}/artifacts?attempt=1"), Some("garbage".into())).await.unwrap(), 401);
    let key = std::fs::read(s.data_dir.join("http.key")).unwrap();
    let expired = token::mint(&key, &Token { job: a.clone(), verb: "artifacts:get".into(), lease: None, exp: 1 });
    assert_eq!(get(format!("/v1/jobs/{a}/artifacts?attempt=1"), Some(expired)).await.unwrap(), 401, "expired token");
    let wrong_verb = token::mint(&key, &Token { job: a.clone(), verb: "log:get".into(), lease: None, exp: i64::MAX });
    assert_eq!(get(format!("/v1/jobs/{a}/artifacts?attempt=1"), Some(wrong_verb)).await.unwrap(), 403);
    // the log archive (Range supported)
    let log_tok = reply.log_token.unwrap();
    assert_eq!(get(format!("/v1/jobs/{a}/logs/1/sys"), Some(log_tok)).await.unwrap(), 200);
}

/// IT-23 / R-OPS-02: `/healthz` and `/metrics` scrape.
#[tokio::test]
async fn metrics_and_health_scrape() {
    let nats = Nats::start(&[]);
    let s = start_scheduler(&nats, &tmpdir("sched"), |_| {}).await;
    let _w = start_worker(&nats, "w1", &s.http, &tmpdir("w1"), |_| {}).await;
    let c = client(&nats, "agent:laptop", &s.http).await;
    let id = c.submit(&spec(&["true"]), None).await.unwrap().id;
    until("done", 15, || async { c.get(&id).await.unwrap().state == JobState::Succeeded }).await;
    let http = s.http.clone();
    let (hz, metrics) = tokio::task::spawn_blocking(move || {
        let hz = ureq::get(&format!("{http}/healthz")).call().unwrap().into_body().read_to_string().unwrap();
        let m = ureq::get(&format!("{http}/metrics")).call().unwrap().into_body().read_to_string().unwrap();
        (hz, m)
    })
    .await
    .unwrap();
    assert!(hz.contains("\"sqlite\":true") && hz.contains("\"nats\":true"), "{hz}");
    for want in [
        "corral_jobs{state=\"succeeded\"} 1",
        "corral_claims_total{result=\"job\"}",
        "corral_worker_up{worker=\"w1\"} 1",
        "corral_storage_bytes",
    ] {
        assert!(metrics.contains(want), "missing {want} in:\n{metrics}");
    }
}
