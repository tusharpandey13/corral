//! A blocking client for the guard socket, for the CLI (`admit`, `run`, `guard status`, ...).

use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

#[derive(Debug)]
pub enum ClientError {
    /// No guard answers on the socket: callers treat this as "refused" (exit 5).
    Unreachable(String),
    Other(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Unreachable(m) | ClientError::Other(m) => f.write_str(m),
        }
    }
}

/// Send `{"v":1,"op":op,...fields}` and read one reply line. An `ok: false` reply is returned, not an error.
pub fn request(socket: &Path, op: &str, fields: Value) -> Result<Value, ClientError> {
    let mut req = json!({ "v": 1, "op": op });
    if let (Some(o), Some(f)) = (req.as_object_mut(), fields.as_object()) {
        o.extend(f.clone());
    }
    let mut s =
        UnixStream::connect(socket).map_err(|e| ClientError::Unreachable(format!("guard unreachable at {}: {e}", socket.display())))?;
    let _ = s.set_read_timeout(Some(Duration::from_secs(15)));
    let _ = s.set_write_timeout(Some(Duration::from_secs(5)));
    s.write_all(format!("{req}\n").as_bytes()).map_err(|e| ClientError::Unreachable(format!("guard unreachable: {e}")))?;
    let mut line = String::new();
    BufReader::new(s).read_line(&mut line).map_err(|e| ClientError::Other(format!("no reply from the guard: {e}")))?;
    serde_json::from_str(line.trim()).map_err(|e| ClientError::Other(format!("bad reply from the guard: {e}")))
}

/// The worker's [`HostGuard`](corral_worker::hostguard::HostGuard) over the guard socket.
pub struct SocketGuard {
    pub socket: std::path::PathBuf,
}

fn reasons_of(r: &Value) -> Vec<corral_proto::api::DeclineReason> {
    r["reasons"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|v| corral_proto::api::DeclineReason {
            check: v["check"].as_str().unwrap_or("guard").to_string(),
            reason: v["reason"].as_str().unwrap_or("refused").to_string(),
        })
        .collect()
}

fn call(socket: &Path, op: &str, f: Value) -> Result<Value, String> {
    let r = request(socket, op, f).map_err(|e| e.to_string())?;
    match r["error"].as_str() {
        Some(e) => Err(e.to_string()),
        None => Ok(r),
    }
}

impl corral_worker::hostguard::HostGuard for SocketGuard {
    fn admit(
        &self,
        name: &str,
        ram_mb: u64,
        class: corral_proto::job::Class,
        reserve_s: u64,
        reservation: Option<&str>,
    ) -> Result<corral_worker::hostguard::GuardAnswer, String> {
        let class = if class == corral_proto::job::Class::Light { "light" } else { "heavy" };
        let r = call(
            &self.socket,
            "admit",
            json!({ "name": name, "ram_mb": ram_mb, "class": class, "profile": "default", "reserve_s": reserve_s, "reservation": reservation }),
        )?;
        Ok(corral_worker::hostguard::GuardAnswer {
            ok: r["ok"] == json!(true),
            reservation: r["reservation"].as_str().map(String::from),
            reasons: reasons_of(&r),
        })
    }

    fn blocked(&self) -> Result<Vec<corral_proto::api::Blocked>, String> {
        let r = call(&self.socket, "checks", json!({ "profile": "default" }))?;
        Ok(r["results"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|c| c["ok"] == json!(false))
            .map(|c| corral_proto::api::Blocked {
                check: c["check"].as_str().unwrap_or("guard").to_string(),
                profile: Some("default".into()),
                reason: c["reason"].as_str().unwrap_or("").to_string(),
                since: c["since_ms"].as_i64().map(corral_proto::util::rfc3339),
            })
            .collect())
    }

    fn register(&self, r: &corral_worker::hostguard::GuardReg) -> Result<String, String> {
        let reply = call(
            &self.socket,
            "register",
            json!({
                "name": r.name, "pid": r.pid, "pgid": r.pgid, "pid_start": r.pid_start, "tier": r.tier,
                "budget_mb": r.budget_mb, "pausable": r.pausable, "heavy": r.heavy, "reservation": r.reservation,
            }),
        )?;
        reply["id"].as_str().map(String::from).ok_or_else(|| "the guard returned no registration id".into())
    }

    fn unregister(&self, id: &str) {
        let _ = request(&self.socket, "unregister", json!({ "id": id }));
    }

    fn unreserve(&self, token: &str) {
        let _ = request(&self.socket, "unreserve", json!({ "token": token }));
    }

    fn watch(&self, on: &mut dyn FnMut(corral_worker::hostguard::GuardEvent) -> bool) {
        loop {
            if let Ok(mut s) = UnixStream::connect(&self.socket)
                && s.write_all(b"{\"v\":1,\"op\":\"subscribe\",\"topics\":[\"actions\"]}\n").is_ok()
            {
                for line in BufReader::new(s).lines() {
                    let Ok(line) = line else { break };
                    let Ok(v) = serde_json::from_str::<Value>(&line) else { continue };
                    let Some(kind) = v["kind"].as_str() else { continue };
                    let ev = corral_worker::hostguard::GuardEvent {
                        kind: kind.to_string(),
                        unit: v["unit"].as_str().map(String::from),
                        reason: v["reason"].as_str().unwrap_or("").to_string(),
                        detail: v["detail"].as_str().unwrap_or("").to_string(),
                    };
                    if !on(ev) {
                        return;
                    }
                }
            }
            std::thread::sleep(Duration::from_secs(2));
        }
    }
}
