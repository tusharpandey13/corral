//! `corral lock ...` and `corral run`: host locks (the lease-file protocol, no guard needed) and the wrapper that
//! admits, locks, re-checks under the lock, registers and runs a command (GUARDS.md 4.2, 5.6, 7.4).

use crate::args::{LockArgs, LockCmd, RunArgs};
use crate::exitcode;
use crate::guardcmd::socket_path;
use corral_guard::client::{ClientError, request};
use corral_guard::lockcli::{self, Acquired, Resolved, TOKEN_ENV, Want};
use corral_proto::config::Config;
use corral_proto::util::{parse_duration_ms, parse_size_mb};
use rustix::process::{Pid, Signal, kill_process_group};
use serde_json::{Value, json};
use std::time::{Duration, Instant};

fn usage(m: impl std::fmt::Display) -> i32 {
    eprintln!("corral: {m}");
    exitcode::USAGE
}

fn default_agent() -> String {
    std::env::var("CORRAL_LOCK_AGENT").unwrap_or_else(|_| format!("corral-cli:{}", std::env::var("USER").unwrap_or_else(|_| "user".into())))
}

fn resolve(cfg: &Config, a: &LockArgs) -> Result<Resolved, String> {
    lockcli::resolve(cfg, a.name.as_deref(), a.path.as_ref(), a.ttl)
}

/// Run `cmd` in its own process group and wait for it; a TERM or INT to us is forwarded to that group (TERM, then
/// KILL after 10 s). Only the group of the process this function started is ever signalled.
async fn supervise(cmd: &[String], env: &[(String, String)], on_start: impl FnOnce(u32)) -> Result<i32, String> {
    use std::os::unix::process::ExitStatusExt;
    use tokio::signal::unix::{SignalKind, signal};
    let mut child = tokio::process::Command::new(&cmd[0])
        .args(&cmd[1..])
        .envs(env.iter().cloned())
        .process_group(0)
        .spawn()
        .map_err(|e| format!("cannot start {:?}: {e}", cmd[0]))?;
    let pid = child.id().ok_or("the command exited before it could be supervised")?;
    on_start(pid);
    let group = Pid::from_raw(pid as i32);
    let (mut term, mut int) =
        (signal(SignalKind::terminate()).map_err(|e| e.to_string())?, signal(SignalKind::interrupt()).map_err(|e| e.to_string())?);
    let mut sent_term = false;
    let status = loop {
        tokio::select! {
            s = child.wait() => break s.map_err(|e| e.to_string())?,
            _ = term.recv() => {}
            _ = int.recv() => {}
            _ = tokio::time::sleep(Duration::from_secs(10)), if sent_term => {
                if let Some(g) = group { let _ = kill_process_group(g, Signal::KILL); }
            }
        }
        if !sent_term {
            sent_term = true;
            if let Some(g) = group {
                let _ = kill_process_group(g, Signal::TERM);
            }
        }
    };
    Ok(status.code().unwrap_or_else(|| 128 + status.signal().unwrap_or(0)))
}

/// Renew `agent`'s lock every `ttl / 3` until aborted.
fn renew_task(r: std::sync::Arc<Resolved>, agent: String) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut every = tokio::time::interval(Duration::from_secs((r.ttl_s / 3).max(1)));
        every.tick().await;
        let mut warned = false;
        loop {
            every.tick().await;
            if let Err(e) = lockcli::renew(&r, &agent)
                && !warned
            {
                warned = true;
                eprintln!("corral: lock renewal failed: {e}");
            }
        }
    })
}

pub async fn lock_cmd(cmd: LockCmd, cfg: &Config) -> i32 {
    let args = match &cmd {
        LockCmd::Acquire(a) | LockCmd::Wait(a) | LockCmd::Renew(a) | LockCmd::Release(a) | LockCmd::Status(a) | LockCmd::Exec(a) => {
            a.clone()
        }
    };
    let r = match resolve(cfg, &args) {
        Ok(r) => r,
        Err(e) => return usage(e),
    };
    let inherited = std::env::var(TOKEN_ENV).ok();
    let agent = args.agent.clone().unwrap_or_else(default_agent);
    let purpose = args.purpose.clone().unwrap_or_else(|| "corral lock".into());
    let take = |agent: &str, token: Option<&str>| {
        lockcli::acquire(
            &r,
            &Want {
                agent,
                purpose: &purpose,
                apps: args.apps.clone(),
                pid: std::process::id(),
                ttl_s: r.ttl_s,
                new_token: token,
                inherited: inherited.as_deref(),
            },
        )
    };
    match cmd {
        LockCmd::Acquire(_) => match take(&agent, None) {
            Ok(Acquired::Taken) => {
                println!("acquired");
                0
            }
            Ok(Acquired::Reentered) => {
                println!("re-entered (held by an ancestor of this process)");
                0
            }
            Err(b) => {
                eprintln!("corral: busy: {}", b.0);
                exitcode::REFUSED
            }
        },
        LockCmd::Wait(_) => {
            let deadline = match args.timeout.as_deref().map(parse_duration_ms).transpose() {
                Ok(d) => d.map(|ms| Instant::now() + Duration::from_millis(ms)),
                Err(e) => return usage(format!("--timeout: {e}")),
            };
            let mut last = String::new();
            let mut delay = Duration::from_secs(1);
            loop {
                match take(&agent, None) {
                    Ok(_) => {
                        println!("acquired");
                        return 0;
                    }
                    Err(b) => {
                        if deadline.is_some_and(|d| Instant::now() >= d) {
                            eprintln!("corral: busy: {}", b.0);
                            return exitcode::REFUSED;
                        }
                        if b.0 != last {
                            eprintln!("corral: waiting: {}", b.0);
                            last = b.0;
                        }
                    }
                }
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(15));
            }
        }
        LockCmd::Renew(_) => match lockcli::renew(&r, &agent) {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("corral: not the holder: {e}");
                exitcode::USAGE
            }
        },
        LockCmd::Release(_) => {
            use corral_worker::lock::LockStatus;
            match r.lock.status() {
                LockStatus::Held { file, .. } if file.agent != agent => {
                    eprintln!("corral: not the holder (held by {})", file.agent);
                    exitcode::USAGE
                }
                _ => {
                    lockcli::release(&r, &agent);
                    0
                }
            }
        }
        LockCmd::Status(_) => {
            let (held, text) = lockcli::describe(&r);
            if args.json {
                let st = match r.lock.status() {
                    corral_worker::lock::LockStatus::Free => json!({ "state": "free" }),
                    corral_worker::lock::LockStatus::Unreadable => json!({ "state": "unreadable" }),
                    corral_worker::lock::LockStatus::Held { file, stale } => {
                        json!({ "state": if stale { "stale" } else { "held" }, "holder": file })
                    }
                };
                println!("{st}");
            } else {
                println!("{text}");
            }
            if held { exitcode::REFUSED } else { 0 }
        }
        LockCmd::Exec(_) => {
            if args.cmd.is_empty() {
                return usage("lock exec needs a command after --");
            }
            let token = lockcli::new_token();
            let how = match take(&agent, Some(&token)) {
                Ok(h) => h,
                Err(b) => {
                    eprintln!("corral: busy: {}", b.0);
                    return exitcode::REFUSED;
                }
            };
            let r = std::sync::Arc::new(r);
            let renew = (how == Acquired::Taken).then(|| renew_task(r.clone(), agent.clone()));
            let env = if how == Acquired::Taken { vec![(TOKEN_ENV.to_string(), token)] } else { vec![] };
            let code = supervise(&args.cmd, &env, |_| {}).await;
            if let Some(h) = renew {
                h.abort();
                lockcli::release(&r, &agent);
            }
            match code {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("corral: {e}");
                    1
                }
            }
        }
    }
}

fn guard_call(sock: &std::path::Path, op: &str, f: Value) -> Result<Value, ClientError> {
    request(sock, op, f)
}

fn first_reason(r: &Value) -> String {
    r["reasons"].as_array().and_then(|a| a.first()).and_then(|v| v["reason"].as_str()).unwrap_or("refused").to_string()
}

/// The reason of a guard `kill` decision about this run in the last two minutes, if any.
fn killed_by_guard(sock: &std::path::Path, unit: &str) -> Option<String> {
    let r = guard_call(sock, "explain", json!({ "n": 100 })).ok()?;
    // the guard's own clock: the decisions carry its timestamps
    let now = r["now_ms"].as_i64()?;
    r["decisions"]
        .as_array()?
        .iter()
        .rev()
        .find(|d| d["kind"] == "kill" && d["unit"] == unit && now - d["t_ms"].as_i64().unwrap_or(0) < 120_000)
        .map(|d| d["reason"].as_str().unwrap_or("unknown").to_string())
}

/// `corral run`.
pub async fn run_cmd(a: RunArgs, cfg: &Config) -> i32 {
    let ram_mb = match a.ram.as_deref().map(parse_size_mb).transpose() {
        Ok(v) => v.unwrap_or(0),
        Err(e) => return usage(format!("--ram: {e}")),
    };
    let budget_mb = match a.budget.as_deref().map(parse_size_mb).transpose() {
        Ok(Some(b)) => Some(b),
        Ok(None) if ram_mb > 0 => Some((ram_mb as f64 * 1.1).ceil() as u64),
        Ok(None) => None,
        Err(e) => return usage(format!("--budget: {e}")),
    };
    let wait = match a.wait.as_deref().map(parse_duration_ms).transpose() {
        Ok(w) => w.map(|ms| Instant::now() + Duration::from_millis(ms)),
        Err(e) => return usage(format!("--wait: {e}")),
    };
    if !matches!(a.class.as_str(), "heavy" | "light") {
        return usage(format!("--class is heavy or light, got {:?}", a.class));
    }
    let sock = match socket_path(cfg) {
        Ok(s) => s,
        Err(e) => return usage(e),
    };
    let name = a.name.clone().unwrap_or_else(|| a.cmd[0].rsplit('/').next().unwrap_or("cmd").to_string());
    let lock = match a.lock.as_deref() {
        Some(n) => match lockcli::resolve(cfg, Some(n), None, None) {
            Ok(r) => Some(std::sync::Arc::new(r)),
            Err(e) => return usage(e),
        },
        None => None,
    };
    let agent = format!("corral-run:{name}");
    let inherited = std::env::var(TOKEN_ENV).ok();
    let lock_token = lockcli::new_token();
    // unique per invocation: two runs of the same program must not share a hold or a unit name (M3)
    let run_id = corral_proto::util::random_b32(6).to_ascii_lowercase();
    let admit_name = format!("run:{name}:{run_id}");
    let mut delay = Duration::from_secs(1);
    let mut last_reason = String::new();

    // Admission, lock, re-check: each failure releases what was taken and either waits or declines.
    let (reservation, held_lock) = loop {
        let decline = |reason: String| -> Option<i32> {
            if wait.is_some_and(|d| Instant::now() < d) {
                None
            } else {
                println!("refused: {reason}");
                Some(exitcode::REFUSED)
            }
        };
        let r = match guard_call(
            &sock,
            "admit",
            json!({ "name": admit_name, "ram_mb": ram_mb, "class": a.class, "profile": a.profile, "reserve_s": 60 }),
        ) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("corral: {e} (treating as refused)");
                return exitcode::UNREACHABLE;
            }
        };
        if let Some(e) = r["error"].as_str() {
            return usage(e);
        }
        let reason = if r["ok"] == json!(true) {
            let token = r["reservation"].as_str().unwrap_or("").to_string();
            let mut holds = false;
            let mut why = None;
            if let Some(l) = &lock {
                match lockcli::acquire(
                    l,
                    &Want {
                        agent: &agent,
                        purpose: &admit_name,
                        apps: vec![],
                        pid: std::process::id(),
                        ttl_s: l.ttl_s,
                        new_token: Some(&lock_token),
                        inherited: inherited.as_deref(),
                    },
                ) {
                    Ok(Acquired::Taken) => holds = true,
                    Ok(Acquired::Reentered) => {}
                    Err(b) => why = Some(format!("lock: {}", b.0)),
                }
            }
            if why.is_none() {
                // the full profile and the memory projection again, now holding the lock (R-ADM-01)
                match guard_call(
                    &sock,
                    "admit",
                    json!({ "name": admit_name, "ram_mb": ram_mb, "class": a.class, "profile": a.profile, "reserve_s": 0, "reservation": token }),
                ) {
                    Ok(c) if c["ok"] == json!(true) => break (token, holds.then(|| lock.clone().expect("lock"))),
                    Ok(c) => why = Some(first_reason(&c)),
                    Err(e) => why = Some(e.to_string()),
                }
            }
            if holds && let Some(l) = &lock {
                lockcli::release(l, &agent);
            }
            let _ = guard_call(&sock, "unreserve", json!({ "token": token }));
            why.unwrap_or_default()
        } else {
            first_reason(&r)
        };
        if let Some(code) = decline(reason.clone()) {
            return code;
        }
        if reason != last_reason {
            eprintln!("corral: waiting: {reason}");
            last_reason = reason;
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(15));
    };

    let renew = held_lock.as_ref().map(|l| renew_task(l.clone(), agent.clone()));
    let mut env = vec![("CORRAL_ADMIT_TOKEN".to_string(), reservation.clone()), ("CORRAL_RUN_NAME".to_string(), name.clone())];
    if held_lock.is_some() {
        env.push((TOKEN_ENV.to_string(), lock_token));
    }
    let reg_id = std::sync::Mutex::new(None::<String>);
    let hold_owner = corral_worker::keepawake::run_owner(&name, &run_id);
    let gate = corral_worker::keepawake::HoldGate::default();
    let held = std::sync::atomic::AtomicBool::new(false);
    let renewer = std::sync::Mutex::new(None::<tokio::task::JoinHandle<()>>);
    let result = supervise(&a.cmd, &env, |pid| {
        if a.hold {
            // window = 2 h + 10 min, renewed every 30 min while the command runs (GUARDS.md 8); bound to its pid
            let window = 2 * 3600 + 600;
            let fields = json!({ "owner": hold_owner, "for_s": window, "bound_pid": pid, "display": false });
            match guard_call(&sock, "hold", fields.clone()) {
                Ok(r) if r["ok"] == json!(true) => {
                    held.store(true, std::sync::atomic::Ordering::SeqCst);
                    let (s, gate) = (sock.clone(), gate.clone());
                    *renewer.lock().unwrap() = Some(tokio::spawn(async move {
                        loop {
                            tokio::time::sleep(Duration::from_secs(30 * 60)).await;
                            let (s, f, gate) = (s.clone(), fields.clone(), gate.clone());
                            // a renewal in flight at release time finishes before the release and nothing follows it (L2)
                            let _ = tokio::task::spawn_blocking(move || gate.renew(|| guard_call(&s, "hold", f))).await;
                        }
                    }));
                }
                Ok(r) => eprintln!("corral: no keep-awake hold ({}); the host may sleep", r["error"].as_str().unwrap_or("refused")),
                Err(e) => eprintln!("corral: no keep-awake hold ({e}); the host may sleep"),
            }
        }
        let start = corral_worker::probe::start_time_of(pid);
        match guard_call(
            &sock,
            "register",
            json!({ "name": admit_name, "pid": pid, "pgid": pid, "pid_start": start, "tier": a.tier, "budget_mb": budget_mb, "pausable": false, "heavy": a.class == "heavy", "reservation": reservation }),
        ) {
            Ok(r) if r["ok"] == json!(true) => *reg_id.lock().unwrap() = r["id"].as_str().map(String::from),
            Ok(r) => eprintln!("corral: the guard did not register this run: {}", r["error"].as_str().unwrap_or("?")),
            Err(e) => eprintln!("corral: the guard did not register this run: {e}"),
        }
    })
    .await;
    if let Some(t) = renewer.lock().unwrap().take() {
        t.abort();
    }
    if held.load(std::sync::atomic::Ordering::SeqCst) {
        let _ = gate.release(|| guard_call(&sock, "release_hold", json!({ "owner": hold_owner })));
    }
    if let Some(id) = reg_id.lock().unwrap().take() {
        let _ = guard_call(&sock, "unregister", json!({ "id": id }));
    }
    let _ = guard_call(&sock, "unreserve", json!({ "token": reservation }));
    if let Some(h) = renew {
        h.abort();
    }
    if let Some(l) = &held_lock {
        lockcli::release(l, &agent);
    }
    match result {
        // Died of a signal: if the guard sent it, say why and exit 125 (GUARDS.md 4.2).
        Ok(c) if c > 128 => match killed_by_guard(&sock, &admit_name) {
            Some(why) => {
                eprintln!("corral: killed by the guard: {why}");
                exitcode::JOB_ENDED
            }
            None => c,
        },
        Ok(c) => c,
        Err(e) => {
            eprintln!("corral: {e}");
            1
        }
    }
}
