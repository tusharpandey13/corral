//! The local guard socket (GUARDS.md 6): newline-delimited JSON over a Unix socket, peer uid checked, plus the
//! sampling loop and the one-guard-per-host instance lock (R-GUARD-01).

use crate::guard::{CmdResolver, Deps, Guard, SystemClock};
use crate::procs::PsSource;
use crate::sample::{HwmonThermal, Probes, StatvfsDisk};
use crate::settings::GuardSettings;
use serde_json::{Value, json};
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

/// Take the per-host guard lock: `flock` on `<state_dir>/guard.lock`, held for the life of the process and released by
/// the kernel when it dies. A second guard on the same state exits with "already running".
pub fn instance_lock(state_dir: &Path) -> Result<std::fs::File, String> {
    std::fs::create_dir_all(state_dir).map_err(|e| format!("{}: {e}", state_dir.display()))?;
    let path = state_dir.join("guard.lock");
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&path)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    match rustix::fs::flock(&f, rustix::fs::FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => {
            let _ = f.set_len(0);
            let _ = writeln!(f, "{}", std::process::id());
            Ok(f)
        }
        Err(e) if e == rustix::io::Errno::WOULDBLOCK => Err(format!("corral guard is already running (lock {})", path.display())),
        Err(e) => Err(format!("{}: {e}", path.display())),
    }
}

/// Probes, process table and clock of this host.
pub fn host_deps(s: &GuardSettings) -> Deps {
    let sensor =
        s.sensor.as_ref().map(|(chip, label)| HwmonThermal { root: "/sys/class/hwmon".into(), chip: chip.clone(), label: label.clone() });
    Deps {
        probes: Probes::for_host(sensor, s.zone.clone()),
        procs: Box::new(PsSource),
        disk: Box::new(StatvfsDisk),
        resolver: Box::new(CmdResolver),
        clock: Arc::new(SystemClock),
    }
}

async fn serve_conn(guard: Arc<Guard>, stream: UnixStream, own_uid: u32) {
    let allowed = stream.peer_cred().map(|c| c.uid() == own_uid || c.uid() == 0).unwrap_or(false);
    let (rd, mut wr) = stream.into_split();
    let mut rd = BufReader::new(rd);
    loop {
        let mut line = String::new();
        match (&mut rd).take(1 << 20).read_line(&mut line).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
        if !line.ends_with('\n') {
            let _ = wr.write_all(b"{\"v\":1,\"ok\":false,\"error\":\"request too long or truncated\"}\n").await;
            return;
        }
        if !allowed {
            let _ = wr.write_all(b"{\"v\":1,\"ok\":false,\"error\":\"forbidden: caller is not the guard's user or root\"}\n").await;
            return;
        }
        let req: Value = match serde_json::from_str(line.trim()) {
            Ok(v) => v,
            Err(e) => {
                let _ = wr.write_all(format!("{}\n", json!({ "v": 1, "ok": false, "error": format!("bad json: {e}") })).as_bytes()).await;
                continue;
            }
        };
        if req.get("op").and_then(Value::as_str) == Some("subscribe") && req.get("v").and_then(Value::as_u64) == Some(1) {
            let topics: Vec<String> = req
                .get("topics")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(|t| t.as_str().map(String::from)).collect())
                .unwrap_or_default();
            let mut rx = guard.subscribe();
            if wr.write_all(b"{\"v\":1,\"ok\":true}\n").await.is_err() {
                return;
            }
            loop {
                match rx.recv().await {
                    Ok(ev) => {
                        let is_level = ev.contains("\"kind\":\"level\"");
                        let wanted = topics.is_empty() || topics.iter().any(|t| (t == "level") == is_level);
                        if wanted && wr.write_all(format!("{ev}\n").as_bytes()).await.is_err() {
                            return;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(_) => return,
                }
            }
        }
        let g = guard.clone();
        let reply = tokio::task::spawn_blocking(move || g.handle(&req))
            .await
            .unwrap_or_else(|_| json!({ "v": 1, "ok": false, "error": "internal error" }));
        if wr.write_all(format!("{reply}\n").as_bytes()).await.is_err() {
            return;
        }
    }
}

/// Bind the socket (mode 0660, parent directory created 0700 if missing) and serve until `stop` resolves.
pub async fn serve(guard: Arc<Guard>, stop: impl std::future::Future<Output = ()>) -> Result<(), String> {
    let path = guard.settings.socket.clone();
    if let Some(dir) = path.parent() {
        if !dir.exists() {
            std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
            let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700));
        }
    }
    // We hold the instance lock, so a socket file here is stale.
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path).map_err(|e| format!("cannot bind {}: {e}", path.display()))?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o660)).map_err(|e| format!("{}: {e}", path.display()))?;
    let own_uid = rustix::process::getuid().as_raw();

    let sampler = {
        let g = guard.clone();
        tokio::spawn(async move {
            let mut every = tokio::time::interval(std::time::Duration::from_millis(g.settings.sample_ms));
            every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                every.tick().await;
                let g2 = g.clone();
                if tokio::task::spawn_blocking(move || g2.tick()).await.is_err() {
                    tracing::error!("a sample round panicked");
                }
            }
        })
    };
    let accept = {
        let g = guard.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((s, _)) => {
                        tokio::spawn(serve_conn(g.clone(), s, own_uid));
                    }
                    Err(e) => {
                        tracing::warn!("accept: {e}");
                        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    }
                }
            }
        })
    };
    stop.await;
    sampler.abort();
    accept.abort();
    let _ = std::fs::remove_file(&path);
    Ok(())
}

/// `corral guard run`: take the instance lock, make one decision round, then serve until `stop`.
pub async fn run(settings: GuardSettings, stop: impl std::future::Future<Output = ()>) -> Result<(), String> {
    let _lock = instance_lock(&settings.state_dir)?;
    tracing::info!(mode = "observe", socket = %settings.socket.display(), "guard starting: decisions are logged, no process is signalled");
    let guard = Arc::new(Guard::new(settings.clone(), host_deps(&settings))?);
    {
        let g = guard.clone();
        tokio::task::spawn_blocking(move || g.tick()).await.map_err(|e| e.to_string())?;
    }
    serve(guard, stop).await
}
