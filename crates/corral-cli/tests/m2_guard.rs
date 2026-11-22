//! M2: the guard surfaces of the `corral` binary against a live (in-process, fake-probe) guard and against a real
//! `corral guard run` daemon: admit (R-ADM-06), run (R-ADM-01), lock (R-LOCK-01/02/04), the kill switch (R-GUARD-05),
//! one guard per host (R-GUARD-01 / IT-11) and the decision comparison (GUARDS.md 10.3).

use corral_guard::testing::Rig;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_corral");

#[derive(Debug)]
struct Out {
    code: i32,
    out: String,
    err: String,
}

fn corral_sync(config: &Path, sock: &Path, args: &[String]) -> Out {
    let o = Command::new(BIN)
        .arg("--config")
        .arg(config)
        .args(args)
        .env("CORRAL_GUARD_SOCKET", sock)
        .env_remove("CORRAL_LOCK_TOKEN")
        .env("NO_COLOR", "1")
        .output()
        .unwrap();
    Out {
        code: o.status.code().unwrap_or(-1),
        out: String::from_utf8_lossy(&o.stdout).to_string(),
        err: String::from_utf8_lossy(&o.stderr).to_string(),
    }
}

struct Live {
    rig: Rig,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    server: Option<tokio::task::JoinHandle<Result<(), String>>>,
}

impl Live {
    async fn start(extra: &str) -> Live {
        let rig = Rig::new(extra);
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(corral_guard::server::serve(rig.guard.clone(), async move {
            let _ = rx.await;
        }));
        for _ in 0..200 {
            if rig.socket().exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // the server's own first round has run once it answers
        rig.tick();
        Live { rig, stop: Some(tx), server: Some(server) }
    }

    async fn corral(&self, args: &[&str]) -> Out {
        let (cfg, sock) = (self.rig.config_path.clone(), self.rig.socket());
        let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        tokio::task::spawn_blocking(move || corral_sync(&cfg, &sock, &args)).await.unwrap()
    }

    async fn shutdown(mut self) {
        let _ = self.stop.take().unwrap().send(());
        self.server.take().unwrap().await.unwrap().unwrap();
    }
}

/// R-ADM-06 (IT-10): `corral admit` exit codes with a live guard, a refusing guard, bad usage, a stopped guard and
/// no guard; the legacy output flavour.
#[tokio::test(flavor = "multi_thread")]
async fn admit_exit_codes_and_output() {
    let g = Live::start("").await;
    let ok = g.corral(&["admit", "--name", "t", "--ram", "1G"]).await;
    assert_eq!((ok.code, ok.out.trim()), (0, "ok"), "{ok:?}");
    let res = g.corral(&["admit", "--name", "t", "--ram", "1G", "--reserve", "60s"]).await;
    assert_eq!(res.code, 0);
    assert!(res.out.starts_with("ok reservation=") && res.out.contains(" until="), "{res:?}");
    // the heavy slot is now reserved: refused, with the first reason and the others
    let refused = g.corral(&["admit", "--name", "u", "--ram", "1G"]).await;
    assert_eq!(refused.code, 1, "{refused:?}");
    assert!(refused.out.starts_with("refused: 1 of 1 heavy slots in use"), "{refused:?}");
    let legacy = g.corral(&["admit", "--ram", "1G", "--legacy-output"]).await;
    assert_eq!((legacy.code, legacy.out.lines().count()), (1, 1), "{legacy:?}");
    // light work does not need the slot
    assert_eq!(g.corral(&["admit", "--class", "light", "--ram", "100M"]).await.code, 0);
    let json = g.corral(&["admit", "--ram", "1G", "--json"]).await;
    assert!(json.out.contains("\"ok\":false") && json.out.contains("heavy_slot"), "{json:?}");
    // usage
    assert_eq!(g.corral(&["admit", "--ram", "lots"]).await.code, 2);
    assert_eq!(g.corral(&["admit", "--class", "huge"]).await.code, 2);
    assert_eq!(g.corral(&["admit", "--profile", "measurement"]).await.code, 2, "profiles beyond default arrive with M4");
    // the kill switch
    let stop = g.corral(&["guard", "stop", "--reason", "drill", "--by", "owner"]).await;
    assert_eq!(stop.code, 0, "{stop:?}");
    assert!(stop.out.contains("no process is signalled"), "{stop:?}");
    let st = g.corral(&["admit", "--class", "light"]).await;
    assert_eq!(st.code, 1);
    assert!(st.out.contains("kill switch on") && st.out.contains("by owner: drill"), "{st:?}");
    assert_eq!(g.corral(&["guard", "resume"]).await.out.trim(), "resumed");
    let status = g.corral(&["guard", "status"]).await;
    assert!(
        status.out.contains("kill switch: off") && status.out.contains("mode observe") && status.out.contains("level ok"),
        "{status:?}"
    );
    let explain = g.corral(&["guard", "explain", "-n", "50"]).await;
    assert!(explain.out.contains("admit") && explain.out.contains("stop") && explain.out.contains("resume"), "{explain:?}");
    let metrics = g.corral(&["guard", "metrics"]).await;
    assert!(metrics.out.contains("corral_guard_admits_total{result=\"refused\"}"), "{metrics:?}");
    let (cfg, sock) = (g.rig.config_path.clone(), g.rig.socket());
    g.shutdown().await;
    // no guard: exit 5, and the legacy shim output for callers that must treat it as refused
    let gone = corral_sync(&cfg, &sock, &["admit".into()]);
    assert_eq!(gone.code, 5, "{gone:?}");
    let gone = corral_sync(&cfg, &sock, &["admit".into(), "--legacy-output".into()]);
    assert_eq!((gone.code, gone.out.trim()), (1, "refused: guard unreachable"));
}

/// R-ADM-01 / GUARDS 4.2: `corral run` admits, locks, re-checks, registers, runs in its own group, passes the
/// command's exit code, releases everything, and does not start a refused command.
#[tokio::test(flavor = "multi_thread")]
async fn run_wraps_a_command_with_admission_and_lock() {
    let dir = corral_guard::testing::tmpdir("run");
    let lock_path = dir.join("emulator.lock");
    let g = Live::start(&format!("[guard.locks.emulator]\npath = \"{}\"\nttl = \"60s\"\n", lock_path.display())).await;
    let marker = dir.join("ran");
    let script = format!(
        "cat '{}' > '{}'; echo \"$CORRAL_RUN_NAME $CORRAL_ADMIT_TOKEN\" >> '{}'; exit 3",
        lock_path.display(),
        marker.display(),
        marker.display()
    );
    let r = g.corral(&["run", "--name", "job", "--ram", "500M", "--lock", "emulator", "--", "sh", "-c", &script]).await;
    assert_eq!(r.code, 3, "the command's exit code: {r:?}");
    let seen = std::fs::read_to_string(&marker).unwrap();
    assert!(seen.contains("\"agent\":\"corral-run:job\"") && seen.contains("job "), "the lock was held while it ran: {seen}");
    assert!(!lock_path.exists(), "released on exit");
    let st = g.corral(&["guard", "status", "--json"]).await;
    assert!(st.out.contains("\"registrations\": []"), "unregistered after the run: {}", st.out);
    let st = g.corral(&["admit", "--ram", "500M"]).await;
    assert_eq!(st.code, 0, "the reservation and the slot are gone: {st:?}");

    // refused: the command never starts
    g.corral(&["guard", "stop"]).await;
    let _ = std::fs::remove_file(&marker);
    let r = g.corral(&["run", "--name", "no", "--", "sh", "-c", &format!("touch '{}'", marker.display())]).await;
    assert_eq!(r.code, 1, "{r:?}");
    assert!(r.out.contains("refused: kill switch on"), "{r:?}");
    assert!(!marker.exists());
    g.corral(&["guard", "resume"]).await;

    // lock held by someone else: declined with the holder, nothing runs
    let held = g.corral(&["lock", "acquire", "emulator", "--agent", "legacy-tool"]).await;
    assert_eq!(held.code, 0, "{held:?}");
    let r = g.corral(&["run", "--name", "blocked", "--lock", "emulator", "--", "sh", "-c", &format!("touch '{}'", marker.display())]).await;
    assert_eq!(r.code, 1, "{r:?}");
    assert!(r.out.contains("held by legacy-tool"), "{r:?}");
    assert!(!marker.exists());
    // --wait: waits for the lock with the reason printed, then runs
    let (cfg, sock, lp) = (g.rig.config_path.clone(), g.rig.socket(), lock_path.clone());
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let _ = std::fs::remove_file(lp);
    });
    let m2 = marker.display().to_string();
    let waited = tokio::task::spawn_blocking(move || {
        corral_sync(
            &cfg,
            &sock,
            &[
                "run".into(),
                "--name".into(),
                "w".into(),
                "--lock".into(),
                "emulator".into(),
                "--wait".into(),
                "20s".into(),
                "--".into(),
                "sh".into(),
                "-c".into(),
                format!("touch '{m2}'"),
            ],
        )
    })
    .await
    .unwrap();
    release.await.unwrap();
    assert_eq!(waited.code, 0, "{waited:?}");
    assert!(waited.err.contains("waiting: lock: held by legacy-tool"), "{waited:?}");
    assert!(marker.exists());
    g.shutdown().await;
}

/// R-LOCK-01/02/04: the lock surface on one lease file; holder-only release; busy names the holder; nested exec
/// re-enters with the holder's token.
#[tokio::test(flavor = "multi_thread")]
async fn lock_commands() {
    let dir = corral_guard::testing::tmpdir("lock");
    let f = dir.join("a.lock");
    let cfg = dir.join("none.toml");
    std::fs::write(&cfg, "version = 1\n").unwrap();
    let sock = dir.join("unused.sock");
    let c = |args: &[&str]| corral_sync(&cfg, &sock, &args.iter().map(|s| s.to_string()).collect::<Vec<_>>());
    let p = f.to_str().unwrap();
    assert_eq!(c(&["lock", "acquire", "--path", p, "--agent", "corral:one", "--apps", "emulator,adb"]).code, 0);
    let text = std::fs::read_to_string(&f).unwrap();
    for k in ["\"agent\"", "\"purpose\"", "\"apps\":[\"emulator\",\"adb\"]", "\"pid\"", "\"started\"", "\"heartbeat\"", "\"ttl_s\""] {
        assert!(text.contains(k), "{k} missing in {text}");
    }
    let busy = c(&["lock", "acquire", "--path", p, "--agent", "corral:two"]);
    assert_eq!(busy.code, 1);
    assert!(busy.err.contains("held by corral:one"), "{busy:?}");
    assert_eq!(c(&["lock", "status", "--path", p]).code, 1, "held");
    assert_eq!(c(&["lock", "renew", "--path", p, "--agent", "corral:one"]).code, 0);
    assert_eq!(c(&["lock", "renew", "--path", p, "--agent", "corral:two"]).code, 2);
    let not_holder = c(&["lock", "release", "--path", p, "--agent", "corral:two"]);
    assert_eq!(not_holder.code, 2, "{not_holder:?}");
    assert!(f.exists());
    assert_eq!(c(&["lock", "release", "--path", p, "--agent", "corral:one"]).code, 0);
    assert_eq!(c(&["lock", "status", "--path", p]).out.trim(), "free");
    assert_eq!(c(&["lock", "status", "--path", p]).code, 0);
    // wait with a timeout
    c(&["lock", "acquire", "--path", p, "--agent", "corral:one"]);
    let t = Instant::now();
    let w = c(&["lock", "wait", "--path", p, "--agent", "corral:two", "--timeout", "1s"]);
    assert_eq!(w.code, 1);
    assert!(t.elapsed() < Duration::from_secs(8));
    c(&["lock", "release", "--path", p, "--agent", "corral:one"]);
    // exec: holds around a command, the child inherits a token and re-enters; released afterwards
    let nested =
        format!("'{BIN}' --config '{}' lock exec --path '{p}' --agent corral:child -- sh -c 'test -f \"{p}\" && exit 7'", cfg.display());
    let r = c(&["lock", "exec", "--path", p, "--agent", "corral:outer", "--", "sh", "-c", &nested]);
    assert_eq!(r.code, 7, "the nested exec re-entered and ran: {r:?}");
    assert!(!f.exists(), "outer released on exit, the nested one did not release early");
    // without the token the nested exec is busy
    let no_token =
        format!("env -u CORRAL_LOCK_TOKEN '{BIN}' --config '{}' lock exec --path '{p}' --agent corral:child -- true", cfg.display());
    assert_eq!(c(&["lock", "exec", "--path", p, "--agent", "corral:outer", "--", "sh", "-c", &no_token]).code, 1);
    // stale takeover rules (R-LOCK-02): a stale non-corral holder is declined, a stale corral one is taken over
    let stale = |agent: &str| {
        std::fs::write(
            &f,
            format!("{{\"agent\":\"{agent}\",\"purpose\":\"x\",\"apps\":[],\"pid\":1,\"started\":1.0,\"heartbeat\":1.0,\"ttl_s\":300.0}}"),
        )
        .unwrap();
    };
    stale("legacy-tool");
    let r = c(&["lock", "acquire", "--path", p, "--agent", "corral:new"]);
    assert_eq!(r.code, 1);
    assert!(r.err.contains("not taking over a non-corral holder"), "{r:?}");
    stale("corral:dead");
    assert_eq!(c(&["lock", "acquire", "--path", p, "--agent", "corral:new"]).code, 0);
    let log = std::fs::read_to_string(dir.join("a.lock.log")).unwrap();
    assert_eq!(log.lines().count(), 1);
    assert!(log.contains("takeover") && log.contains("corral:dead"), "{log}");
}

/// GUARDS 10.3 G0: the comparison of the guard's decision log with a legacy log; exit 1 on a legacy-only action.
#[tokio::test(flavor = "multi_thread")]
async fn compare_against_a_legacy_log() {
    use corral_guard::procs::ProcInfo;
    let g = Live::start("[[guard.matchers]]\nname = \"llm\"\nselect = { cmdline = 'llama-server' }\ntier = 1\n").await;
    *g.rig.procs.table.lock().unwrap() = vec![ProcInfo {
        pid: 812,
        ppid: 1,
        pgid: 812,
        rss_kb: 3_000_000,
        user: "u".into(),
        start: "Mon Nov 22 10:00:00 2026".into(),
        cmdline: "llama-server -m x".into(),
    }];
    g.rig.set_free_pct(14.0);
    g.rig.tick();
    // the manual clock starts at 2023-11-14T22:13:20Z and moved 2 s per tick
    let at = |s: u32| format!("2023-11-14 22:13:{s:02}");
    let dir = g.rig.dir.clone();
    let legacy = dir.join("legacy.log");
    std::fs::write(
        &legacy,
        format!("{} watch start\n{} kill tier 1 pids: 812  (level=14% pressure=1 swap=0MB) reason=memory_critical\n", at(10), at(24)),
    )
    .unwrap();
    let ok = g.corral(&["guard", "compare", "--legacy", legacy.to_str().unwrap(), "--utc-offset", "+00:00"]).await;
    assert_eq!(ok.code, 0, "{ok:?}");
    assert!(ok.out.contains("matched 1, legacy-only 0"), "{ok:?}");
    std::fs::write(&legacy, format!("{} kill tier 2 pids: 900 reason=memory_critical\n", at(24))).unwrap();
    let bad = g.corral(&["guard", "compare", "--legacy", legacy.to_str().unwrap(), "--utc-offset", "+00:00", "--json"]).await;
    assert_eq!(bad.code, 1, "{bad:?}");
    assert!(bad.out.contains("\"legacy_only\"") && bad.out.contains("kill_tier"), "{bad:?}");
    g.shutdown().await;
}

struct Daemon(Child);
impl Drop for Daemon {
    fn drop(&mut self) {
        // a process this test started itself
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn daemon(cfg: &Path, sock: &Path) -> Daemon {
    Daemon(
        Command::new(BIN)
            .arg("--config")
            .arg(cfg)
            .args(["guard", "run"])
            .env("CORRAL_GUARD_SOCKET", sock)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    )
}

fn wait_for(sock: &Path) -> bool {
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(20) {
        if corral_guard::client::request(sock, "status", serde_json::json!({})).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

/// The real daemon with the host's real (read-only) probes, in a temp directory: serves, refuses a second instance
/// (R-GUARD-01 / IT-11), keeps the kill switch across a restart (R-GUARD-05) and signals nobody.
#[test]
fn real_daemon_one_per_host_and_the_kill_switch_persists() {
    let dir: PathBuf = corral_guard::testing::tmpdir("dmn");
    let cfg = dir.join("corral.toml");
    let sock = dir.join("g.sock");
    std::fs::write(
        &cfg,
        format!(
            "version = 1\nhost = \"laptop\"\n[guard]\nstate_dir = \"{d}/state\"\nsocket = \"{d}/g.sock\"\nsample = \"500ms\"\n",
            d = dir.display()
        ),
    )
    .unwrap();
    let mut d1 = daemon(&cfg, &sock);
    assert!(wait_for(&sock), "the daemon did not come up");
    let st = corral_sync(&cfg, &sock, &["guard".into(), "status".into(), "--json".into()]);
    assert_eq!(st.code, 0, "{st:?}");
    let v: serde_json::Value = serde_json::from_str(&st.out).unwrap();
    assert_eq!(v["mode"], "observe");
    assert!(v["sample"]["total_mb"].as_u64().unwrap_or(0) > 0, "a real memory reading: {v}");
    // a second guard on the same state refuses
    let second = Command::new(BIN).arg("--config").arg(&cfg).args(["guard", "run"]).env("CORRAL_GUARD_SOCKET", &sock).output().unwrap();
    assert!(!second.status.success());
    assert!(String::from_utf8_lossy(&second.stderr).contains("already running"), "{}", String::from_utf8_lossy(&second.stderr));
    assert_eq!(corral_sync(&cfg, &sock, &["guard".into(), "stop".into(), "--reason".into(), "restart test".into()]).code, 0);
    // restart (the first daemon dies without cleaning up: the stale socket file must not matter)
    let _ = d1.0.kill();
    let _ = d1.0.wait();
    let _d2 = daemon(&cfg, &sock);
    assert!(wait_for(&sock), "the restarted daemon did not come up");
    let r = corral_sync(&cfg, &sock, &["admit".into(), "--class".into(), "light".into()]);
    assert_eq!(r.code, 1, "{r:?}");
    assert!(r.out.contains("kill switch on") && r.out.contains("restart test"), "{r:?}");
    assert_eq!(corral_sync(&cfg, &sock, &["guard".into(), "resume".into()]).code, 0);
    // every decision round left a log line the comparison can read
    assert!(std::fs::read_to_string(dir.join("state/decisions.jsonl")).unwrap().contains("\"kind\":\"stop\""));
}

/// M3: `enforce` is accepted by the config (opt-in); an unknown mode is a usage error naming the key.
#[test]
fn guard_mode_values() {
    let dir = corral_guard::testing::tmpdir("enf");
    let cfg = dir.join("c.toml");
    std::fs::write(&cfg, "version = 1\n[guard]\nmode = \"fire\"\n").unwrap();
    let o = Command::new(BIN).arg("--config").arg(&cfg).args(["guard", "run"]).output().unwrap();
    assert_eq!(o.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&o.stderr).contains("guard.mode"));
}
