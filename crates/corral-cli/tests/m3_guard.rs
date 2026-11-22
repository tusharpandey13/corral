//! M3: the legacy-guard shims (R-MIG-01, golden outputs), `corral guard await`, `corral run --hold` and the exit-125
//! report of a run the guard killed, against live in-process guards on fake probes. Processes started here are this
//! test's own.

use corral_guard::testing::Rig;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

const BIN: &str = env!("CARGO_BIN_EXE_corral");

#[derive(Debug)]
#[allow(dead_code)]
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
    async fn start(rig: Rig) -> Live {
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

/// R-MIG-01 (golden): the legacy guard's calls and their outputs and exit codes, from GUARDS.md 10.2.
#[tokio::test(flavor = "multi_thread")]
async fn the_shim_keeps_the_legacy_cli_output_and_exit_codes() {
    let g = Live::start(Rig::new("")).await;
    // admit [name]: `ok` / exit 0 (a 60 s reservation is taken)
    let ok = g.corral(&["shim", "macos", "admit", "build"]).await;
    assert_eq!((ok.code, ok.out.as_str()), (0, "ok\n"), "{ok:?}");
    // the slot is reserved: `refused: <reason>` on one line, exit 1; the name is `legacy:<name>`, `?` when missing
    let no = g.corral(&["shim", "macos", "admit"]).await;
    assert_eq!(no.code, 1, "{no:?}");
    assert_eq!(no.out.lines().count(), 1);
    assert!(no.out.starts_with("refused: 1 of 1 heavy slots in use"), "{no:?}");
    let log = g.corral(&["guard", "explain", "-n", "20", "--json"]).await.out;
    assert!(log.contains("legacy:build") && log.contains("legacy:?"), "{log}");
    // status, macOS then Linux field names
    let st = g.corral(&["shim", "macos", "status"]).await;
    assert_eq!((st.code, st.out.as_str()), (0, "free=75% pressure=1 swap=0MB stop=off\n"), "{st:?}");
    let st = g.corral(&["shim", "linux", "status"]).await;
    assert_eq!(st.out, "avail=12000MB temp=?C stop=off\n", "{st:?}");
    // stop / resume
    let stop = g.corral(&["shim", "macos", "--program", "heavy-guard", "stop"]).await;
    assert_eq!((stop.code, stop.out.as_str()), (0, "heavy jobs killed; new ones blocked until: heavy-guard resume\n"), "{stop:?}");
    let st = g.corral(&["shim", "macos", "status"]).await;
    assert_eq!(st.out, "free=75% pressure=1 swap=0MB stop=ON\n", "{st:?}");
    let blocked = g.corral(&["shim", "macos", "admit", "x"]).await;
    assert_eq!(blocked.code, 1);
    assert!(blocked.out.contains("kill switch on"), "{blocked:?}");
    let res = g.corral(&["shim", "macos", "resume"]).await;
    assert_eq!((res.code, res.out.as_str()), (0, "resumed\n"));
    // `guard status --legacy-output` is the same text the shim prints
    assert_eq!(g.corral(&["guard", "status", "--legacy-output"]).await.out.lines().count(), 1);
    // no guard: refused, exit 1, never a hang
    let (cfg, sock) = (g.rig.config_path.clone(), g.rig.socket());
    g.shutdown().await;
    let gone = corral_sync(&cfg, &sock, &["shim".into(), "macos".into(), "admit".into(), "x".into()]);
    assert_eq!((gone.code, gone.out.as_str()), (1, "refused: guard unreachable\n"), "{gone:?}");
}

/// The tier lines of the legacy `status`: `tier N: pids`, never units left out.
#[test]
fn legacy_status_lists_tiers_and_pids() {
    let s = serde_json::json!({
        "sample": { "free_pct": 18.4, "pressure": 2, "swap_mb": 5100 },
        "kill_switch": null,
        "units": [
            { "tier": 2, "pids": [30, 31], "never": false },
            { "tier": 1, "pids": [20], "never": false },
            { "tier": 1, "pids": [21, 20], "never": false },
            { "tier": 3, "pids": [99], "never": true }
        ]
    });
    assert_eq!(
        corral_cli::guardcmd::legacy_status_text(&s, "macos"),
        "free=18% pressure=2 swap=5100MB stop=off\ntier 1: 20 21\ntier 2: 30 31\n"
    );
}

/// `watch`: blocks while the guard lives and exits 1 once it is gone (a leftover keep-alive unit does not loop).
#[tokio::test(flavor = "multi_thread")]
async fn watch_blocks_while_the_guard_is_alive() {
    let g = Live::start(Rig::new("")).await;
    let (cfg, sock) = (g.rig.config_path.clone(), g.rig.socket());
    let mut child = Command::new(BIN)
        .arg("--config")
        .arg(&cfg)
        .args(["guard", "await", "--interval", "100ms"])
        .env("CORRAL_GUARD_SOCKET", &sock)
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(child.try_wait().unwrap().is_none(), "still blocking while the guard answers");
    g.shutdown().await;
    let st = tokio::task::spawn_blocking(move || child.wait().unwrap()).await.unwrap();
    assert_eq!(st.code(), Some(1));
}

fn fake_awake(dir: &Path) -> PathBuf {
    let p = dir.join("fake-awake");
    std::fs::write(
        &p,
        format!(
            "#!/bin/sh\necho \"$@\" >> \"{d}/calls\"\ncase \"$1\" in hold) echo \"hold $5 until 2026-11-22 18:00:00 (pid 1)\";; holds) echo '[]';; esac\n",
            d = dir.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
    p
}

/// R-AWAKE-01 (`corral run --hold`): a named hold bound to the command's pid while it runs, released at the end.
#[tokio::test(flavor = "multi_thread")]
async fn run_hold_takes_and_releases_a_named_hold() {
    let dir = corral_guard::testing::tmpdir("hold");
    let cmd = fake_awake(&dir);
    let g = Live::start(Rig::new(&format!("[guard.keepawake]\nprovider = \"hold-cli\"\ncommand = \"{}\"\n", cmd.display()))).await;
    let r = g.corral(&["run", "--name", "my build", "--hold", "--", "sh", "-c", "exit 3"]).await;
    assert_eq!(r.code, 3, "{r:?}");
    let calls = std::fs::read_to_string(dir.join("calls")).unwrap();
    // the first call is the provider probe (`holds --json`) that admission makes
    let lines: Vec<&str> = calls.lines().filter(|l| l.starts_with("hold ") || l.starts_with("release")).collect();
    assert!(lines[0].starts_with("hold --for 7800s --owner corral:run:my_build --pid "), "{calls}");
    assert_eq!(lines.last(), Some(&"release --owner corral:run:my_build"), "{calls}");
    assert!(!calls.contains("--all"));
    g.shutdown().await;
}

/// GUARDS 4.2: a run the guard killed ends exit 125 with the reason on stderr.
#[tokio::test(flavor = "multi_thread")]
async fn a_run_killed_by_the_guard_exits_125_with_the_reason() {
    let g = Live::start(Rig::enforcing("")).await;
    let (cfg, sock) = (g.rig.config_path.clone(), g.rig.socket());
    let child = Command::new(BIN)
        .arg("--config")
        .arg(&cfg)
        .args(["run", "--name", "hog", "--ram", "20M", "--", "perl", "-e", "$x = 'a' x (60*1024*1024); sleep 60"])
        .env("CORRAL_GUARD_SOCKET", &sock)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2500)).await; // admitted, registered, allocated
    let rig_guard = g.rig.guard.clone();
    tokio::task::spawn_blocking(move || rig_guard.tick()).await.unwrap();
    let o = tokio::task::spawn_blocking(move || child.wait_with_output().unwrap()).await.unwrap();
    assert_eq!(o.status.code(), Some(125), "{}", String::from_utf8_lossy(&o.stderr));
    assert!(String::from_utf8_lossy(&o.stderr).contains("killed by the guard: ram_exceeded"));
    g.shutdown().await;
}
