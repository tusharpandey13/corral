//! The real `corral` binary: keygen, nats-config, scheduler and worker daemons from TOML files, and the client
//! commands with their exit codes. On Linux the worker runs the job; elsewhere the worker's memory probe is not
//! available until M3 and the job must wait with a reason (fail closed).

mod common;

use common::{free_port, tmpdir};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_corral");

struct Proc(Child);
impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn corral(config: &Path, args: &[&str]) -> (i32, String, String) {
    let out = Command::new(BIN).arg("--config").arg(config).args(args).env("NO_COLOR", "1").output().unwrap();
    (out.status.code().unwrap_or(-1), String::from_utf8_lossy(&out.stdout).to_string(), String::from_utf8_lossy(&out.stderr).to_string())
}

fn keygen(dir: &Path, name: &str) -> (PathBuf, String) {
    let seed = dir.join(format!("{name}.nk"));
    let out = Command::new(BIN).args(["keygen", "--out"]).arg(&seed).output().unwrap();
    assert!(out.status.success());
    (seed, String::from_utf8_lossy(&out.stdout).trim().to_string())
}

#[test]
fn daemons_and_client_from_config_files() {
    let dir = tmpdir("bin");
    let (nats_port, http_port) = (free_port(), free_port());
    let ids = [("scheduler", "scheduler"), ("worker:server", "worker"), ("agent:laptop", "agent"), ("owner", "owner")];
    let keys: Vec<(String, PathBuf, String)> = ids
        .iter()
        .map(|(id, n)| {
            let (seed, public) = keygen(&dir, n);
            (id.to_string(), seed, public)
        })
        .collect();
    let mut cmd = Command::new(BIN);
    cmd.args(["nats-config", "--listen", &format!("127.0.0.1:{nats_port}"), "--store-dir"]).arg(dir.join("js"));
    for (id, _, public) in &keys {
        cmd.arg("--user").arg(format!("{id}={public}"));
    }
    let conf = cmd.output().unwrap();
    assert!(conf.status.success(), "{}", String::from_utf8_lossy(&conf.stderr));
    std::fs::write(dir.join("nats.conf"), &conf.stdout).unwrap();
    let nats = Proc(
        Command::new("nats-server")
            .arg("-c")
            .arg(dir.join("nats.conf"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("nats-server (brew install nats-server)"),
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    while std::net::TcpStream::connect(("127.0.0.1", nats_port)).is_err() {
        assert!(Instant::now() < deadline, "nats-server did not start");
        std::thread::sleep(Duration::from_millis(50));
    }
    std::thread::sleep(Duration::from_millis(300));

    let seed = |id: &str| keys.iter().find(|k| k.0 == id).unwrap().1.display().to_string();
    let client = |id: &str| {
        format!(
            "[client]\nnats_url = \"nats://127.0.0.1:{nats_port}\"\nhttp_url = \"http://127.0.0.1:{http_port}\"\nidentity = \"{id}\"\nnkey_seed_file = \"{}\"\n",
            seed(id)
        )
    };
    let sched_cfg = dir.join("scheduler.toml");
    std::fs::write(
        &sched_cfg,
        format!(
            "version = 1\nhost = \"server\"\n{}\n[scheduler]\ndata_dir = \"{}\"\nlisten = [\"127.0.0.1\"]\nhttp_port = {http_port}\n",
            client("scheduler"),
            dir.join("sched").display()
        ),
    )
    .unwrap();
    let worker_cfg = dir.join("worker.toml");
    std::fs::write(
        &worker_cfg,
        format!(
            "version = 1\nhost = \"server\"\n{}\n[worker]\nwork_dir = \"{}\"\nadmit_floor = \"256M\"\n",
            client("worker:server"),
            dir.join("work").display()
        ),
    )
    .unwrap();
    let agent_cfg = dir.join("agent.toml");
    std::fs::write(&agent_cfg, format!("version = 1\n{}", client("agent:laptop"))).unwrap();
    let owner_cfg = dir.join("owner.toml");
    std::fs::write(&owner_cfg, format!("version = 1\n{}", client("owner"))).unwrap();

    // R-OPS-04: bad configuration is a usage error that names the key.
    let bad = dir.join("bad.toml");
    std::fs::write(&bad, "version = 1\n[scheduler]\nlease_tll = \"90s\"\n").unwrap();
    let (code, _, err) = corral(&bad, &["config", "check"]);
    assert_eq!(code, 2);
    assert!(err.contains("lease_tll"), "{err}");
    assert_eq!(corral(&sched_cfg, &["config", "check"]).0, 0);

    let _sched = Proc(
        Command::new(BIN).arg("--config").arg(&sched_cfg).arg("scheduler").stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap(),
    );
    let _worker =
        Proc(Command::new(BIN).arg("--config").arg(&worker_cfg).arg("worker").stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap());
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (code, out, _) = corral(&agent_cfg, &["workers"]);
        if code == 0 && out.contains("server") {
            break;
        }
        assert!(Instant::now() < deadline, "scheduler and worker did not come up (workers: {code} {out})");
        std::thread::sleep(Duration::from_millis(300));
    }

    // Usage errors and missing ids use the documented exit codes.
    assert_eq!(corral(&agent_cfg, &["submit", "--no-input", "--", "true"]).0, 2, "heavy without --ram is a usage error");
    assert_eq!(corral(&agent_cfg, &["status", "j-20200101-aaaaaa"]).0, 3);
    assert_eq!(corral(&agent_cfg, &["admin", "pause"]).0, 4, "admin is owner-only");

    let (code, id, err) = corral(&agent_cfg, &["submit", "--no-input", "--light", "--", "echo hello; exit 3"]);
    assert_eq!(code, 0, "{err}");
    let id = id.trim().to_string();
    if cfg!(target_os = "linux") {
        let (code, out, _) = corral(&agent_cfg, &["wait", &id]);
        assert_eq!((code, out.as_str()), (3, "hello\n"));
        let (code, out, _) = corral(&agent_cfg, &["status", &id]);
        assert_eq!(code, 0);
        assert!(out.contains("failed") && out.contains("exit 3"), "{out}");
    } else {
        // No Linux memory probe here: admission fails closed and says why (every wait has a reason).
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let (_, out, _) = corral(&agent_cfg, &["status", &id]);
            if out.contains("unknown") {
                assert!(out.contains("queued"), "{out}");
                break;
            }
            assert!(Instant::now() < deadline, "no waiting reason: {out}");
            std::thread::sleep(Duration::from_millis(500));
        }
    }
    let (code, out, _) = corral(&owner_cfg, &["admin", "pause"]);
    assert_eq!((code, out.trim()), (0, "queue paused"));
    let (_, out, _) = corral(&owner_cfg, &["workers"]);
    assert!(out.contains("queue is paused"), "{out}");
    drop(nats);
}
