//! M1 review regressions for R-SEC-02 / R-SEC-03 / R-IO-03 / R-QUEUE-06: what a hostile agent can do with the bus
//! when a job's own output is attacker-controlled. Every test here failed on the M1 merge commit.

mod common;

use common::*;
use corral_cli::follow::{FollowOpts, MemSink, follow};
use corral_proto::api::FollowReq;
use corral_proto::envelope::{ErrorCode, encode_request};
use corral_proto::ident::Identity;
use corral_proto::job::JobState;
use futures::StreamExt;
use std::time::Duration;

async fn stream_msgs(nats: &Nats, stream: &str) -> u64 {
    let b = nats.bus("scheduler").await;
    let mut s = b.js.get_stream(stream).await.unwrap();
    s.info().await.unwrap().state.messages
}

/// R-SEC-02: a request that names a JetStream API subject as its reply subject (the scheduler may publish on
/// `$JS.API.>`) is not answered there: it cannot purge the log or event streams through the scheduler.
#[tokio::test]
async fn scheduler_does_not_answer_into_foreign_reply_subjects() {
    let nats = Nats::start(&[]);
    let sched = start_scheduler(&nats, &tmpdir("sched"), |_| {}).await;
    let _w = start_worker(&nats, "w1", &sched.http, &tmpdir("w1"), |_| {}).await;
    let owner = client(&nats, "owner", &sched.http).await;
    let r = owner.submit(&spec(&["bash", "-c", "echo owner-secret-log"]), None).await.unwrap();
    let mut sink = MemSink::default();
    follow(&owner, &r.id, FollowOpts { follow: true, sys: false }, &mut sink).await.unwrap();
    let (logs, events) = (stream_msgs(&nats, "CORRAL_LOGS").await, stream_msgs(&nats, "CORRAL_EVENTS").await);
    assert!(logs > 0 && events > 0);

    let sink_created = async || {
        let b = nats.bus("scheduler").await;
        let stream = b.js.get_stream("CORRAL_LOGS").await.unwrap();
        let mut c: async_nats::jetstream::consumer::PullConsumer = stream.get_consumer("sink").await.unwrap();
        c.info().await.unwrap().created
    };
    let created = sink_created().await;
    let agent = nats.bus("agent:laptop").await;
    // Observable form of the same hole: agents may subscribe to `corral.event.>`, so a reply sent there is seen.
    let mut leak = agent.client.subscribe("corral.event.leak.>").await.unwrap();
    let probe = encode_request(&Identity::Agent("laptop".into()), "rq-leak", &serde_json::json!({"id": r.id}));
    agent.client.publish_with_reply("corral.api.agent-laptop.get", "corral.event.leak.1", probe.into()).await.unwrap();
    for target in ["$JS.API.STREAM.PURGE.CORRAL_LOGS", "$JS.API.STREAM.PURGE.CORRAL_EVENTS", "$JS.API.CONSUMER.DELETE.CORRAL_LOGS.sink"] {
        let payload = encode_request(&Identity::Agent("laptop".into()), &format!("rq-{target}"), &serde_json::json!({"id": r.id}));
        agent.client.publish_with_reply("corral.api.agent-laptop.get", target, payload.into()).await.unwrap();
    }
    agent.client.flush().await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), leak.next()).await.is_err(),
        "the scheduler answered into a subject outside the caller's inbox"
    );
    assert_eq!(stream_msgs(&nats, "CORRAL_LOGS").await, logs, "the log stream was purged through the scheduler");
    assert_eq!(stream_msgs(&nats, "CORRAL_EVENTS").await, events, "the event stream was purged through the scheduler");
    // The archive consumer was neither deleted nor recreated (a delete is not answered by the scheduler's reply).
    assert_eq!(sink_created().await, created, "the sink consumer was deleted through the scheduler's reply");
    // The archive consumer is still there and still archives.
    let again = owner.submit(&spec(&["bash", "-c", "echo still-archived"]), None).await.unwrap();
    follow(&owner, &again.id, FollowOpts { follow: true, sys: false }, &mut MemSink::default()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let archived = std::fs::read_to_string(sched.data_dir.join("jobs").join(&again.id).join("1.out.log")).unwrap_or_default();
    assert!(archived.contains("still-archived"), "log archiving stopped: {archived:?}");
}

/// R-SEC-02: an agent cannot create a JetStream consumer that delivers its job's output to a worker's control
/// subject (pause/drain/resume/cancel), nor edit or delete the scheduler's `sink` consumer.
#[tokio::test]
async fn agent_cannot_create_or_edit_consumers() {
    let nats = Nats::start(&[]);
    let sched = start_scheduler(&nats, &tmpdir("sched"), |_| {}).await;
    let w = start_worker(&nats, "w1", &sched.http, &tmpdir("w1"), |_| {}).await;
    let c = client(&nats, "agent:laptop", &sched.http).await;
    // The job prints a valid control message for the worker.
    let r = c.submit(&spec(&["bash", "-c", r#"echo '{"v":1,"action":"pause","reason":"pwned by agent"}'"#]), None).await.unwrap();
    follow(&c, &r.id, FollowOpts { follow: true, sys: false }, &mut MemSink::default()).await.unwrap();

    let agent = nats.bus("agent:laptop").await;
    let push = serde_json::json!({"stream_name":"CORRAL_LOGS","config":{
        "deliver_subject":"corral.ctl.w1","filter_subject":format!("corral.log.w1.{}.out", r.id),
        "ack_policy":"none","deliver_policy":"all"}});
    let sink_edit = serde_json::json!({"stream_name":"CORRAL_LOGS","config":{
        "durable_name":"sink","name":"sink","filter_subject":"corral.log.nobody.>","ack_policy":"explicit","deliver_policy":"all"}});
    let attacks: Vec<(&str, Vec<u8>)> = vec![
        ("$JS.API.CONSUMER.CREATE.CORRAL_LOGS", serde_json::to_vec(&push).unwrap()),
        ("$JS.API.CONSUMER.CREATE.CORRAL_LOGS.sink", serde_json::to_vec(&sink_edit).unwrap()),
        ("$JS.API.CONSUMER.DELETE.CORRAL_LOGS.sink", vec![]),
    ];
    for (subject, body) in attacks {
        // A refused publish gets no answer (the server reports a permission violation instead).
        let got = tokio::time::timeout(Duration::from_secs(2), agent.client.request(subject.to_string(), body.into())).await;
        assert!(!matches!(got, Ok(Ok(_))), "{subject} was answered for an agent");
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(w.worker.adm.paused().is_none(), "the worker obeyed a control message injected by an agent");
    let b = nats.bus("scheduler").await;
    let stream = b.js.get_stream("CORRAL_LOGS").await.unwrap();
    let mut names = stream.consumer_names();
    let mut have_sink = false;
    while let Some(n) = names.next().await {
        have_sink |= n.unwrap() == "sink";
    }
    assert!(have_sink, "the scheduler's sink consumer is gone");
}

/// R-SEC-02: `follow` only streams into the caller's own inbox space. Naming a worker's control subject (or another
/// identity's inbox) as the delivery subject is refused, and nothing is delivered there.
#[tokio::test]
async fn follow_refuses_foreign_delivery_subjects() {
    let nats = Nats::start(&[]);
    let sched = start_scheduler(&nats, &tmpdir("sched"), |_| {}).await;
    let w = start_worker(&nats, "w1", &sched.http, &tmpdir("w1"), |_| {}).await;
    let c = client(&nats, "agent:laptop", &sched.http).await;
    let r = c.submit(&spec(&["bash", "-c", "echo hi"]), None).await.unwrap();
    follow(&c, &r.id, FollowOpts { follow: true, sys: false }, &mut MemSink::default()).await.unwrap();
    for deliver in ["corral.ctl.w1", "_INBOX_owner.steal", "_INBOX_agent-laptop.", "_INBOX_agent-laptop.a.>", "_INBOX_agent-laptopx.a"] {
        let e = c
            .bus
            .call::<_, serde_json::Value>(&c.subject("follow"), &FollowReq { id: r.id.clone(), deliver: deliver.into() })
            .await
            .unwrap_err();
        assert_eq!(e.error, ErrorCode::BadRequest, "{deliver}");
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(w.worker.adm.paused().is_none());
}

/// R-SEC-02 / R-QUEUE-06: log chunks carry no lease token, so what a follower can read never lets it speak for the
/// worker (forge `finished`, cancel or kill the attempt).
#[tokio::test]
async fn followers_never_see_a_lease_token() {
    let nats = Nats::start(&[]);
    let sched = start_scheduler(&nats, &tmpdir("sched"), |_| {}).await;
    let _w = start_worker(&nats, "w1", &sched.http, &tmpdir("w1"), |_| {}).await;
    let owner = client(&nats, "owner", &sched.http).await;
    let r = owner.submit(&spec(&["bash", "-c", "echo hi; sleep 1"]), None).await.unwrap();
    until("running", 10, || async { owner.get(&r.id).await.unwrap().state != JobState::Queued }).await;
    let agent = client(&nats, "agent:laptop", &sched.http).await;
    let mut sub = agent.bus.client.subscribe("_INBOX_agent-laptop.review.1.log").await.unwrap();
    let _: serde_json::Value = agent
        .bus
        .call(&agent.subject("follow"), &FollowReq { id: r.id.clone(), deliver: "_INBOX_agent-laptop.review.1".into() })
        .await
        .unwrap();
    // The scheduler's own record of the live lease token.
    let token = sched.running.service.db().get(&r.id).unwrap().unwrap().token.unwrap();
    let mut seen = 0;
    while let Ok(Some(m)) = tokio::time::timeout(Duration::from_secs(3), sub.next()).await {
        seen += 1;
        let h = m.headers.clone().expect("headers");
        assert!(h.get("Corral-Lease").is_none(), "a lease header is readable by followers");
        assert!(h.iter().all(|(_, vals)| vals.iter().all(|v| !v.as_str().contains(&token))), "the token leaked in a header");
        assert!(!String::from_utf8_lossy(&m.payload).contains(&token));
        if seen >= 2 {
            break;
        }
    }
    assert!(seen >= 1, "no log message arrived");
}

/// R-SEC-02 / R-QUEUE-06: a follower is delivered exactly the job's messages; ones from other jobs are not.
#[tokio::test]
async fn follow_streams_only_the_requested_job() {
    let nats = Nats::start(&[]);
    let sched = start_scheduler(&nats, &tmpdir("sched"), |_| {}).await;
    let _w = start_worker(&nats, "w1", &sched.http, &tmpdir("w1"), |s| s.light_slots = 2).await;
    let c = client(&nats, "agent:laptop", &sched.http).await;
    let a = c.submit(&spec(&["bash", "-c", "echo job-a"]), None).await.unwrap();
    until("a done", 10, || async { c.get(&a.id).await.unwrap().state.is_terminal() }).await;
    let b = c.submit(&spec(&["bash", "-c", "echo job-b"]), None).await.unwrap();
    let mut sink = MemSink::default();
    follow(&c, &b.id, FollowOpts { follow: true, sys: false }, &mut sink).await.unwrap();
    assert_eq!(String::from_utf8_lossy(&sink.out), "job-b\n");
}

/// R-IO-03 (M1 review): a log sequence gap that never fills is recorded in the archive and skipped after a bounded
/// number of redeliveries, so it cannot stall the archive of every job behind it.
#[tokio::test]
async fn permanent_log_gap_is_recorded_and_archiving_moves_on() {
    let nats = Nats::start(&[]);
    let sched = start_scheduler(&nats, &tmpdir("sched"), |s| s.sink_hold_max = 3).await;
    let _w = start_worker(&nats, "w1", &sched.http, &tmpdir("w1"), |_| {}).await;
    let c = client(&nats, "owner", &sched.http).await;
    let r = c.submit(&spec(&["bash", "-c", "echo before-gap"]), None).await.unwrap();
    let v = follow(&c, &r.id, FollowOpts { follow: true, sys: false }, &mut MemSink::default()).await.unwrap();
    let last = v.log_final["out"];
    let path = sched.data_dir.join("jobs").join(&r.id).join("1.out.log");
    until("first chunk archived", 10, || async { std::fs::read_to_string(&path).is_ok_and(|t| t.contains("before-gap")) }).await;

    // The worker's own identity publishes chunk last+2: chunk last+1 never exists.
    let wb = nats.bus("worker:w1").await;
    let mut h = async_nats::HeaderMap::new();
    h.insert("Nats-Msg-Id", format!("{}/1/out/{}", r.id, last + 2).as_str());
    h.insert("Corral-Seq", (last + 2).to_string().as_str());
    h.insert("Corral-Attempt", "1");
    h.insert("Corral-Stream", "out");
    wb.js.publish_with_headers(format!("corral.log.w1.{}.out", r.id), h, "after-gap\n".into()).await.unwrap().await.unwrap();
    until("gap recorded", 15, || async { std::fs::read_to_string(&path).is_ok_and(|t| t.contains("after-gap")) }).await;
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(text.contains("are missing"), "{text:?}");
    assert!(
        text.find("before-gap").unwrap() < text.find("are missing").unwrap()
            && text.find("are missing").unwrap() < text.find("after-gap").unwrap()
    );
    // And later jobs are archived normally.
    let next = c.submit(&spec(&["bash", "-c", "echo later-job"]), None).await.unwrap();
    follow(&c, &next.id, FollowOpts { follow: true, sys: false }, &mut MemSink::default()).await.unwrap();
    let p2 = sched.data_dir.join("jobs").join(&next.id).join("1.out.log");
    until("later job archived", 10, || async { std::fs::read_to_string(&p2).is_ok_and(|t| t.contains("later-job")) }).await;
}

/// R-IO-06 (M1 review): the scheduler must be allowed to subscribe to progress messages, or `status` never shows
/// progress. The server refused the subscription and nobody noticed.
#[tokio::test]
async fn scheduler_receives_progress_for_status() {
    let nats = Nats::start(&[]);
    let sched = start_scheduler(&nats, &tmpdir("sched"), |_| {}).await;
    let _w = start_worker(&nats, "w1", &sched.http, &tmpdir("w1"), |_| {}).await;
    let c = client(&nats, "owner", &sched.http).await;
    let r = c.submit(&spec(&["bash", "-c", "echo '##corral progress=42 building'; sleep 3"]), None).await.unwrap();
    until("progress in the job view", 10, || async { c.get(&r.id).await.unwrap().progress == Some(42) }).await;
}
