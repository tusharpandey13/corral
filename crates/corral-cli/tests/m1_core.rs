//! M1 integration tests against a real nats-server (SPEC.md 13, TESTING.md 4): IT-01, IT-12, IT-13, IT-16, IT-17,
//! IT-18, IT-28 and the happy paths of input, logs and events.

mod common;

use common::*;
use corral_cli::follow::{FollowOpts, MemSink, exit_code_for, follow};
use corral_proto::job::{Class, Input, JobState};
use std::time::{Duration, Instant};

async fn rig() -> (Nats, Sched, corral_cli::client::Client, W) {
    let nats = Nats::start(&[]);
    let sched = start_scheduler(&nats, &tmpdir("sched"), |_| {}).await;
    let w = start_worker(&nats, "w1", &sched.http, &tmpdir("w1"), |_| {}).await;
    let c = client(&nats, "agent:laptop", &sched.http).await;
    (nats, sched, c, w)
}

async fn run_to_end(c: &corral_cli::client::Client, spec: &corral_proto::job::JobSpec) -> (corral_proto::job::JobView, MemSink) {
    let r = c.submit(spec, None).await.unwrap();
    let mut sink = MemSink::default();
    let j = follow(c, &r.id, FollowOpts { follow: true, sys: false }, &mut sink).await.unwrap();
    (j, sink)
}

/// IT-17 / R-IO-04: `echo hi; exit 3` prints hi quickly and the exit code is the job's.
#[tokio::test]
async fn echo_hi_exit_3() {
    let (_n, _s, c, _w) = rig().await;
    // warm-up so the worker is connected and has reported presence
    let _ = run_to_end(&c, &spec(&["true"])).await;
    let t = Instant::now();
    let (j, sink) = run_to_end(&c, &spec(&["bash", "-c", "echo hi; exit 3"])).await;
    assert_eq!(String::from_utf8_lossy(&sink.out), "hi\n");
    assert_eq!(j.state, JobState::Failed);
    assert_eq!(exit_code_for(&j), 3);
    assert!(t.elapsed() < Duration::from_secs(3), "took {:?}", t.elapsed());
}

/// R-IO-04: killed, lost, cancelled and expired jobs exit 125; stdout and stderr stay separate.
#[tokio::test]
async fn stderr_is_separate_and_success_is_zero() {
    let (_n, _s, c, _w) = rig().await;
    let (j, sink) = run_to_end(&c, &spec(&["bash", "-c", "echo to-out; echo to-err 1>&2"])).await;
    assert_eq!(exit_code_for(&j), 0);
    assert_eq!(String::from_utf8_lossy(&sink.out), "to-out\n");
    assert!(String::from_utf8_lossy(&sink.err).contains("to-err"));
}

/// IT-01 / R-QUEUE-01 / R-ADM-04: with no worker online, submit returns an id in under a second, state queued.
#[tokio::test]
async fn submit_with_no_worker_is_fast_and_queued() {
    let nats = Nats::start(&[]);
    let sched = start_scheduler(&nats, &tmpdir("sched"), |_| {}).await;
    let c = client(&nats, "agent:laptop", &sched.http).await;
    let t = Instant::now();
    let r = c.submit(&spec(&["true"]), None).await.unwrap();
    assert!(t.elapsed() < Duration::from_secs(1), "{:?}", t.elapsed());
    let j = c.get(&r.id).await.unwrap();
    assert_eq!(j.state, JobState::Queued);
    assert!(!j.waiting.is_empty(), "a queued job always says why");
}

/// IT-13 / R-EXEC-02: `sleep 60` with a 2 s timeout ends `killed:timeout`.
#[tokio::test]
async fn timeout_kills_a_real_process() {
    let (_n, _s, c, _w) = rig().await;
    let mut s = spec(&["sleep", "60"]);
    s.timeout_s = 2;
    let t = Instant::now();
    let (j, _) = run_to_end(&c, &s).await;
    assert_eq!((j.state, j.reason.as_deref()), (JobState::Killed, Some("timeout")));
    assert_eq!(exit_code_for(&j), 125);
    assert!(t.elapsed() < Duration::from_secs(10));
    assert_eq!(j.attempts, 1);
}

/// IT-16 / R-EXEC-05: a clean environment; the submitter's and the worker's secrets never reach the job.
#[tokio::test]
async fn job_environment_is_clean() {
    // SAFETY-free: set_var is unsafe in edition 2024, so use a child-visible canary through the test process env
    // only via the command we spawn; the worker's own environment is whatever cargo gave it.
    let (_n, _s, c, _w) = rig().await;
    let mut s = spec(&["env"]);
    s.env.insert("FROM_JOB_SPEC".into(), "yes".into());
    s.env.insert("CORRAL_JOB_ID".into(), "spoofed".into());
    let (j, sink) = run_to_end(&c, &s).await;
    assert_eq!(exit_code_for(&j), 0);
    let out = String::from_utf8_lossy(&sink.out).to_string();
    let names: Vec<&str> = out.lines().filter_map(|l| l.split_once('=').map(|(k, _)| k)).collect();
    for want in [
        "PATH",
        "HOME",
        "LANG",
        "TMPDIR",
        "CORRAL_JOB_ID",
        "CORRAL_ATTEMPT",
        "CORRAL_RAM_MB",
        "CORRAL_WORKER",
        "CORRAL_OUT",
        "FROM_JOB_SPEC",
    ] {
        assert!(names.contains(&want), "{want} missing from {names:?}");
    }
    assert!(out.contains(&format!("CORRAL_JOB_ID={}", j.id)), "the job cannot spoof corral's variables");
    for k in &names {
        assert!(
            ["PATH", "HOME", "LANG", "TMPDIR", "PWD", "SHLVL", "_", "OLDPWD", "FROM_JOB_SPEC"].contains(k) || k.starts_with("CORRAL_"),
            "unexpected variable {k} leaked into the job"
        );
    }
}

/// IT-28 / R-IO-06: progress lines reach a follower.
#[tokio::test]
async fn progress_reaches_the_follower() {
    let (_n, _s, c, _w) = rig().await;
    let (_j, sink) = run_to_end(&c, &spec(&["bash", "-c", "echo '##corral progress=42 building'; sleep 1.2; echo done"])).await;
    let err = String::from_utf8_lossy(&sink.err).to_string();
    assert!(err.contains("progress 42% building"), "{err}");
    assert!(String::from_utf8_lossy(&sink.out).contains("##corral progress=42"), "the line stays in the log");
}

/// R-IO-01: a git ref is resolved to a sha at submit and the worker runs exactly that commit.
#[tokio::test]
async fn git_input_runs_the_resolved_sha() {
    let (_n, _s, c, w) = rig().await;
    let repo = tmpdir("repo");
    let sh = |args: &[&str]| {
        assert!(
            std::process::Command::new("git")
                .args(["-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false"])
                .args(args)
                .current_dir(&repo)
                .output()
                .unwrap()
                .status
                .success(),
            "{args:?}"
        );
    };
    sh(&["init", "-q", "-b", "main", "."]);
    std::fs::write(repo.join("hello.txt"), "v1\n").unwrap();
    sh(&["add", "."]);
    sh(&["commit", "-q", "-m", "one"]);
    let (repo_s, sha) = corral_cli::submit::resolve_git(&repo, Some(&repo.to_string_lossy()), "main").unwrap();
    std::fs::write(repo.join("hello.txt"), "v2\n").unwrap();
    sh(&["commit", "-q", "-am", "two"]);
    let mut s = spec(&["cat", "hello.txt"]);
    s.input = Input::Git { repo: repo_s, git_ref: "main".into(), sha };
    let (j, sink) = run_to_end(&c, &s).await;
    assert_eq!(exit_code_for(&j), 0);
    assert_eq!(String::from_utf8_lossy(&sink.out), "v1\n", "the resolved commit, not the later one");
    drop(w);
}

/// R-IO-01 / R-IO-05: a snapshot round trip over HTTP carries a dirty file and excludes an ignored one.
#[tokio::test]
async fn snapshot_round_trip_and_artifacts() {
    let (_n, _s, c, _w) = rig().await;
    let repo = tmpdir("snaprepo");
    let sh = |args: &[&str]| {
        assert!(
            std::process::Command::new("git")
                .args(["-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false"])
                .args(args)
                .current_dir(&repo)
                .output()
                .unwrap()
                .status
                .success()
        );
    };
    sh(&["init", "-q", "."]);
    std::fs::write(repo.join(".gitignore"), "ignored.txt\n").unwrap();
    std::fs::write(repo.join("a.txt"), "committed\n").unwrap();
    sh(&["add", "."]);
    sh(&["commit", "-q", "-m", "init"]);
    std::fs::write(repo.join("a.txt"), "dirty\n").unwrap();
    std::fs::write(repo.join("ignored.txt"), "x\n").unwrap();
    let snap = corral_cli::snapshot::build(&repo, 10, false, &tmpdir("scratch")).unwrap();
    let mut s = spec(&["bash", "-c", "cat a.txt; ls ignored.txt 2>&1 | head -1; mkdir -p out; echo result > out/r.txt"]);
    s.input = Input::Snapshot { sha256: snap.sha256.clone(), bytes: snap.bytes, base_sha: snap.base_sha.clone(), repo_hint: None };
    s.artifacts = vec!["out/".into()];
    let r = c.submit(&s, Some(&snap)).await.unwrap();
    assert_eq!(r.state, "uploading");
    let mut sink = MemSink::default();
    let j = follow(&c, &r.id, FollowOpts { follow: true, sys: false }, &mut sink).await.unwrap();
    let out = String::from_utf8_lossy(&sink.out).to_string();
    assert!(out.starts_with("dirty\n"), "{out}");
    assert!(out.contains("No such file") || out.contains("cannot access"), "ignored file must be absent: {out}");
    assert_eq!(j.state, JobState::Succeeded);
    // artifacts come back over HTTP
    until("artifacts uploaded", 10, || async { c.get(&r.id).await.unwrap().artifacts_state.as_deref() == Some("uploaded") }).await;
    let dest = tmpdir("dl");
    c.artifacts(&r.id, &dest).await.unwrap();
    assert_eq!(std::fs::read_to_string(dest.join("out/r.txt")).unwrap(), "result\n");
}

/// IT-18 / R-IO-05: artifacts over the per-job cap are refused with 413 and the job record says so.
#[tokio::test]
async fn oversized_artifacts_are_refused_and_recorded() {
    let (_n, _s, c, _w) = rig().await;
    let mut s = spec(&["bash", "-c", "head -c 3145728 /dev/urandom > big.bin"]);
    s.artifacts = vec!["big.bin".into()];
    s.artifacts_max_mb = 1;
    let (j, _) = run_to_end(&c, &s).await;
    assert_eq!(j.state, JobState::Succeeded, "the job itself finishes");
    until("refusal recorded", 10, || async { c.get(&j.id).await.unwrap().artifacts_state.as_deref() == Some("refused") }).await;
    assert!(c.artifacts(&j.id, &tmpdir("dl")).await.is_err());
}

/// IT-12 / R-EXEC-01: a job that forks (including a child that calls setsid) is gone after cancel; an unrelated
/// process with a similar command line survives.
#[tokio::test]
async fn cancel_kills_the_whole_process_tree_but_not_bystanders() {
    if !have("python3") {
        eprintln!("skipped: python3 not available");
        return;
    }
    let (_n, _s, c, _w) = rig().await;
    let sentinel = std::process::Command::new("sleep").arg("31338").spawn().unwrap();
    let script = "import os, subprocess; subprocess.Popen(['sleep','31337'], start_new_session=True); subprocess.Popen(['sleep','31337']); os.system('sleep 31337')";
    let r = c.submit(&spec(&["python3", "-c", script]), None).await.unwrap();
    until("job running with its children", 15, || async { procs_matching("sleep 31337").len() >= 3 }).await;
    c.cancel(&r.id, Some("test".into())).await.unwrap();
    until("job cancelled", 15, || async { c.get(&r.id).await.unwrap().state == JobState::Cancelled }).await;
    until("every descendant gone", 10, || async { procs_matching("sleep 31337").is_empty() }).await;
    assert!(procs_matching("sleep 31338").contains(&sentinel.id()), "an unrelated process with a similar name must survive (INV-1)");
    let mut sentinel = sentinel;
    let _ = sentinel.kill();
    let _ = sentinel.wait();
}

/// G1 / R-QUEUE-04: a job growing past its declared RAM is killed `ram_exceeded`, attempts stay 1, nothing requeued.
#[tokio::test]
async fn memory_hog_is_killed_ram_exceeded() {
    if !have("python3") {
        eprintln!("skipped: python3 not available");
        return;
    }
    let (_n, _s, c, _w) = rig().await;
    let mut s = spec(&["python3", "-c", "b = bytearray(200*1024*1024); import time; time.sleep(60)"]);
    s.ram_mb = Some(50);
    let (j, _) = run_to_end(&c, &s).await;
    assert_eq!((j.state, j.reason.as_deref()), (JobState::Killed, Some("ram_exceeded")));
    assert_eq!(j.attempts, 1);
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(c.get(&j.id).await.unwrap().attempts, 1, "a killed job is never retried");
}

/// R-ADM-01 end to end with a real lock file: a foreign lock holder makes the job wait with the holder named.
#[tokio::test]
async fn foreign_lock_holder_blocks_with_a_reason() {
    let nats = Nats::start(&[]);
    let sched = start_scheduler(&nats, &tmpdir("sched"), |_| {}).await;
    let wd = tmpdir("w1");
    let _w = start_worker(&nats, "w1", &sched.http, &wd, |_| {}).await;
    let c = client(&nats, "agent:laptop", &sched.http).await;
    std::fs::create_dir_all(wd.join("locks")).unwrap();
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64();
    std::fs::write(
        wd.join("locks/heavy.lock"),
        serde_json::json!({"agent":"legacy-bench","purpose":"emulator","apps":[],"pid":1,"started":now,"heartbeat":now,"ttl_s":300.0})
            .to_string(),
    )
    .unwrap();
    let r = c.submit(&spec(&["echo", "ok"]), None).await.unwrap();
    until("lock named in the waiting reasons", 10, || async {
        let j = c.get(&r.id).await.unwrap();
        j.state == JobState::Queued && j.waiting.iter().any(|w| w.text.contains("legacy-bench"))
    })
    .await;
    std::fs::remove_file(wd.join("locks/heavy.lock")).unwrap();
    let mut sink = MemSink::default();
    let j = follow(&c, &r.id, FollowOpts { follow: true, sys: false }, &mut sink).await.unwrap();
    assert_eq!(j.state, JobState::Succeeded);
    assert_eq!(j.attempts, 1, "the earlier waits were declines, not attempts");
    let _ = Class::Heavy;
}

/// E-04 / R-SCHED-01, R-SCHED-03: caps routing across two workers, and small jobs backfill behind one that waits.
#[tokio::test]
async fn caps_routing_and_backfill_across_workers() {
    let nats = Nats::start(&[]);
    let s = start_scheduler(&nats, &tmpdir("sched"), |_| {}).await;
    let _w1 = start_worker(&nats, "w1", &s.http, &tmpdir("w1"), |st| st.caps.push("gpu".into())).await;
    let _w2 = start_worker(&nats, "w2", &s.http, &tmpdir("w2"), |_| {}).await;
    let c = client(&nats, "agent:laptop", &s.http).await;
    let mut gpu = spec(&["bash", "-c", "echo $CORRAL_WORKER"]);
    gpu.caps = vec!["gpu".into()];
    let mut huge = spec(&["true"]);
    huge.ram_mb = Some(500_000); // fits nowhere
    let huge_id = c.submit(&huge, None).await.unwrap().id;
    let (j, sink) = run_to_end(&c, &gpu).await;
    assert_eq!(j.state, JobState::Succeeded);
    assert_eq!(String::from_utf8_lossy(&sink.out), "w1\n", "caps route the job to the only worker that has them");
    let (_, sink) = run_to_end(&c, &spec(&["bash", "-c", "echo small"])).await;
    assert_eq!(String::from_utf8_lossy(&sink.out), "small\n", "the small job ran although a bigger one was queued first");
    let h = c.get(&huge_id).await.unwrap();
    assert_eq!(h.state, JobState::Queued);
    assert!(h.waiting.iter().any(|w| w.text.contains("memory")), "{:?}", h.waiting);
    c.cancel(&huge_id, None).await.unwrap();
}
