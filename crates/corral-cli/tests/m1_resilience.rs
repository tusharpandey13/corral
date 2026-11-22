//! M1 resilience: scheduler crash and downtime (IT-02, IT-24), orphan and resume (IT-04), fencing (IT-05),
//! worker restart (IT-25), NATS restart mid-job (IT-19, IT-25), resubmit rights (IT-06).

mod common;

use common::*;
use corral_proto::job::JobState;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// IT-02 / R-QUEUE-02: the scheduler dies right after a submit reply; on restart the job exists, same state.
#[tokio::test]
async fn scheduler_crash_keeps_the_job() {
    let nats = Nats::start(&[]);
    let data = tmpdir("sched");
    let s1 = start_scheduler(&nats, &data, |_| {}).await;
    let c = client(&nats, "agent:laptop", &s1.http).await;
    let id = c.submit(&spec(&["true"]), None).await.unwrap().id;
    s1.kill();
    let s2 = start_scheduler(&nats, &data, |_| {}).await;
    let c2 = client(&nats, "agent:laptop", &s2.http).await;
    let j = c2.get(&id).await.unwrap();
    assert_eq!(j.state, JobState::Queued);
    assert!(s2.running.service.db().integrity_ok(), "INV-8");
}

/// IT-04 / R-QUEUE-05: a silent worker's job goes `orphaned`; the worker's next heartbeat resumes it with the same
/// token, no new attempt.
#[tokio::test]
async fn orphaned_then_resumed_by_a_late_heartbeat() {
    let nats = Nats::start(&[]);
    let s = start_scheduler(&nats, &tmpdir("sched"), |st| {
        st.lease_ttl_ms = 1500;
        st.orphan_grace_ms = 30_000;
    })
    .await;
    let _w = start_worker(&nats, "w1", &s.http, &tmpdir("w1"), |st| st.hb_interval_ms = 3500).await;
    let c = client(&nats, "agent:laptop", &s.http).await;
    let id = c.submit(&spec(&["sleep", "31341"]), None).await.unwrap().id;
    let orphaned = Arc::new(std::sync::atomic::AtomicBool::new(false));
    until("orphaned then running again", 20, || {
        let (c, id, orphaned) = (&c, id.clone(), orphaned.clone());
        async move {
            let st = c.get(&id).await.unwrap().state;
            if st == JobState::Orphaned {
                orphaned.store(true, Ordering::SeqCst);
            }
            st == JobState::Running && orphaned.load(Ordering::SeqCst)
        }
    })
    .await;
    let j = c.get(&id).await.unwrap();
    assert_eq!(j.attempts, 1, "resumed, not requeued");
    c.cancel(&id, None).await.unwrap();
    until("cancelled", 15, || async { c.get(&id).await.unwrap().state == JobState::Cancelled }).await;
}

/// IT-05 / R-QUEUE-06: after requeue a returning worker is told `stale_lease` on its first heartbeat and kills its
/// process group; only the new attempt survives.
#[tokio::test]
async fn returning_worker_with_a_stale_lease_kills_its_attempt() {
    let nats = Nats::start(&[]);
    let s = start_scheduler(&nats, &tmpdir("sched"), |st| {
        st.lease_ttl_ms = 1000;
        st.orphan_grace_ms = 1000;
    })
    .await;
    // w1 is slow to heartbeat (a laptop that slept); w2 is healthy and will take the requeued job.
    let _w1 = start_worker(&nats, "w1", &s.http, &tmpdir("w1"), |st| st.hb_interval_ms = 7000).await;
    let c = client(&nats, "agent:laptop", &s.http).await;
    let id = c.submit(&spec(&["sleep", "31342"]), None).await.unwrap().id;
    until("attempt 1 running on w1", 10, || async { c.get(&id).await.unwrap().state == JobState::Running }).await;
    let _w2 = start_worker(&nats, "w2", &s.http, &tmpdir("w2"), |_| {}).await;
    until("requeued and running as attempt 2 on w2", 20, || async {
        let j = c.get(&id).await.unwrap();
        j.state == JobState::Running && j.attempts == 2 && j.worker.as_deref() == Some("w2")
    })
    .await;
    assert_eq!(procs_matching("sleep 31342").len(), 2, "both attempts are alive for a moment");
    until("w1 heard stale_lease and killed its group", 20, || async { procs_matching("sleep 31342").len() == 1 }).await;
    let j = c.get(&id).await.unwrap();
    assert_eq!((j.state, j.attempts), (JobState::Running, 2), "the stale attempt's death was not reported");
    c.cancel(&id, None).await.unwrap();
    until("cancelled", 15, || async { c.get(&id).await.unwrap().state == JobState::Cancelled }).await;
    until("nothing left", 10, || async { procs_matching("sleep 31342").is_empty() }).await;
}

/// IT-24 / R-OPS-03: the scheduler is down for longer than the lease; after the restart no job is `orphaned`.
#[tokio::test]
async fn scheduler_downtime_does_not_orphan_running_jobs() {
    let nats = Nats::start(&[]);
    let data = tmpdir("sched");
    let s1 = start_scheduler(&nats, &data, |st| st.lease_ttl_ms = 2000).await;
    let _w = start_worker(&nats, "w1", &s1.http, &tmpdir("w1"), |_| {}).await;
    let c = client(&nats, "agent:laptop", &s1.http).await;
    let id = c.submit(&spec(&["sleep", "31343"]), None).await.unwrap().id;
    until("running", 10, || async { c.get(&id).await.unwrap().state == JobState::Running }).await;
    s1.kill();
    tokio::time::sleep(Duration::from_millis(3500)).await; // longer than lease_ttl
    let s2 = start_scheduler(&nats, &data, |st| st.lease_ttl_ms = 2000).await;
    let c2 = client(&nats, "agent:laptop", &s2.http).await;
    for _ in 0..8 {
        let j = c2.get(&id).await.unwrap();
        assert_eq!(j.state, JobState::Running, "no mass orphaning after an outage");
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    assert_eq!(c2.get(&id).await.unwrap().attempts, 1);
    c2.cancel(&id, None).await.unwrap();
    until("cancelled", 15, || async { c2.get(&id).await.unwrap().state == JobState::Cancelled }).await;
}

/// IT-25 (worker): a worker that restarts mid-job never leaves two live attempts; the old process group is killed
/// before the attempt is reported lost and requeued (attempt 2).
#[tokio::test]
async fn worker_restart_never_runs_two_attempts() {
    let nats = Nats::start(&[]);
    let s = start_scheduler(&nats, &tmpdir("sched"), |_| {}).await;
    let wd = tmpdir("w1");
    let w1 = start_worker(&nats, "w1", &s.http, &wd, |_| {}).await;
    let c = client(&nats, "agent:laptop", &s.http).await;
    let id = c.submit(&spec(&["sleep", "31344"]), None).await.unwrap().id;
    until("running", 10, || async { c.get(&id).await.unwrap().state == JobState::Running }).await;
    let max_live = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sampler = {
        let (m, stop) = (max_live.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                m.fetch_max(procs_matching("sleep 31344").len(), Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(20));
            }
        })
    };
    // The worker "crashes": its tasks stop, its child keeps running.
    w1.worker.abort_all();
    assert_eq!(procs_matching("sleep 31344").len(), 1);
    let _w1b = start_worker(&nats, "w1", &s.http, &wd, |_| {}).await;
    until("attempt 2 running", 20, || async {
        let j = c.get(&id).await.unwrap();
        j.state == JobState::Running && j.attempts == 2
    })
    .await;
    stop.store(true, Ordering::SeqCst);
    sampler.join().unwrap();
    assert_eq!(max_live.load(Ordering::SeqCst), 1, "INV-3: never two live attempts of one job");
    c.cancel(&id, None).await.unwrap();
    until("cancelled", 15, || async { c.get(&id).await.unwrap().state == JobState::Cancelled }).await;
}

/// IT-19 / R-IO-03, IT-25 (NATS): NATS restarts mid-job; the job keeps running (no duplicate attempt) and the
/// scheduler's archive ends up byte-identical to the worker's local file.
#[tokio::test]
async fn nats_restart_mid_job_loses_no_log_bytes() {
    let mut nats = Nats::start(&[]);
    let s = start_scheduler(&nats, &tmpdir("sched"), |_| {}).await;
    let wd = tmpdir("w1");
    let _w = start_worker(&nats, "w1", &s.http, &wd, |_| {}).await;
    let c = client(&nats, "agent:laptop", &s.http).await;
    // Exit 1 so the worker keeps the run directory (the local file is the source of truth to compare with).
    let id = c.submit(&spec(&["bash", "-c", "for i in $(seq 1 40); do echo line $i; sleep 0.15; done; exit 1"]), None).await.unwrap().id;
    until("some output", 10, || async { c.get(&id).await.unwrap().state == JobState::Running }).await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    nats.restart(Duration::from_millis(1500));
    until("job finished", 40, || async { c.get(&id).await.map(|j| j.state.is_terminal()).unwrap_or(false) }).await;
    let j = c.get(&id).await.unwrap();
    assert_eq!((j.state, j.attempts), (JobState::Failed, 1), "no duplicate attempt");
    let local = std::fs::read(wd.join("runs").join(&id).join("1/stdout")).unwrap();
    assert!(String::from_utf8_lossy(&local).contains("line 40"));
    let archive = s.data_dir.join("jobs").join(&id).join("1.out.log");
    until("archive caught up", 30, || async { std::fs::read(&archive).map(|a| a == local).unwrap_or(false) }).await;
}

/// IT-06 / R-QUEUE-08: an agent may not resubmit a killed job; the owner may, and the new job records `retry_of`.
#[tokio::test]
async fn resubmit_of_a_killed_job_needs_the_owner() {
    let nats = Nats::start(&[]);
    let s = start_scheduler(&nats, &tmpdir("sched"), |_| {}).await;
    let _w = start_worker(&nats, "w1", &s.http, &tmpdir("w1"), |_| {}).await;
    let agent = client(&nats, "agent:laptop", &s.http).await;
    let owner = client(&nats, "owner", &s.http).await;
    let mut sp = spec(&["sleep", "30"]);
    sp.timeout_s = 1;
    let id = agent.submit(&sp, None).await.unwrap().id;
    until("killed", 20, || async { agent.get(&id).await.unwrap().state == JobState::Killed }).await;
    let e = agent.resubmit(&id).await.unwrap_err();
    assert_eq!(corral_cli::exitcode::for_error(&e), corral_cli::exitcode::AUTH_DENIED);
    let n = owner.resubmit(&id).await.unwrap();
    assert_eq!(owner.get(&n.id).await.unwrap().retry_of.as_deref(), Some(id.as_str()));
    owner.cancel(&n.id, None).await.unwrap();
}
