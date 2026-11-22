//! M1 review regression: worker restart when the job leader died while the worker was down (IT-25, R-EXEC-01).
mod common;
use common::*;
use corral_proto::job::JobState;
use std::time::Duration;

fn kill_all(pids: &[u32]) {
    for p in pids {
        let _ = std::process::Command::new("kill").arg(p.to_string()).status(); // pids this test started
    }
}

/// Kills the test's own marker processes even when an assertion fails.
struct Cleanup;
impl Drop for Cleanup {
    fn drop(&mut self) {
        kill_all(&procs_matching("sleep 31777"));
    }
}

/// IT-25 / R-EXEC-01 / R-QUEUE-06: the job leader exits while the worker is down, leaving a group member running.
/// After the restart the recorded leader is gone, but the surviving member must still be killed before the attempt
/// is requeued, so two copies never run side by side.
#[tokio::test]
async fn restart_with_dead_leader_kills_the_surviving_group_member() {
    let _cleanup = Cleanup;
    let nats = Nats::start(&[]);
    let s = start_scheduler(&nats, &tmpdir("sched"), |_| {}).await;
    let wd = tmpdir("w1");
    let w1 = start_worker(&nats, "w1", &s.http, &wd, |_| {}).await;
    let c = client(&nats, "agent:laptop", &s.http).await;
    let id = c
        .submit(&spec(&["bash", "-c", "S=31777; sleep $S & if [ \"$CORRAL_ATTEMPT\" = 1 ]; then sleep 2; else sleep 30; fi; exit 0"]), None)
        .await
        .unwrap()
        .id;
    until("running", 10, || async { c.get(&id).await.unwrap().state == JobState::Running }).await;
    until("child", 5, || async { !procs_matching("sleep 31777").is_empty() }).await;
    w1.worker.abort_all();
    // The leader exits (and is reaped) while the worker is down; its child lives on in the old group.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let before = procs_matching("sleep 31777");
    if before.len() != 1 {
        kill_all(&before);
    }
    assert_eq!(before.len(), 1, "precondition: the group member outlived its leader");
    let _w1b = start_worker(&nats, "w1", &s.http, &wd, |_| {}).await;
    until("attempt 2", 20, || async {
        let j = c.get(&id).await.unwrap();
        j.attempts == 2 && j.state == JobState::Running
    })
    .await;
    // Attempt 2 runs its own `sleep 31777`; the survivor of attempt 1 must be gone (exactly one copy).
    until("one copy only", 5, || async { procs_matching("sleep 31777").len() == 1 }).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let live = procs_matching("sleep 31777");
    kill_all(&live); // survivors of a failing run must not outlive the test
    c.cancel(&id, Some("test over".into())).await.unwrap();
    until("cancelled", 10, || async { c.get(&id).await.unwrap().state.is_terminal() }).await;
    assert_eq!(live.len(), 1, "the old attempt's group member survived the restart");
}
