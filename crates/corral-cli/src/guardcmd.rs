//! `corral guard ...` and `corral admit`: thin clients of the local guard socket (GUARDS.md 4.1, 6, 7.5).

use crate::args::{AdmitArgs, GuardCmd};
use crate::exitcode;
use corral_guard::client::{ClientError, request};
use corral_guard::compare::{compare, parse_legacy};
use corral_guard::decisions::read_log;
use corral_guard::settings::GuardSettings;
use corral_proto::config::Config;
use corral_proto::util::{parse_duration_ms, parse_size_mb};
use serde_json::{Value, json};
use std::path::PathBuf;

/// The guard socket: `$CORRAL_GUARD_SOCKET`, else the configuration (`guard.socket`), else the default location.
pub fn socket_path(cfg: &Config) -> Result<PathBuf, String> {
    if let Ok(p) = std::env::var("CORRAL_GUARD_SOCKET")
        && !p.is_empty()
    {
        return Ok(PathBuf::from(p));
    }
    Ok(GuardSettings::from_config(cfg)?.socket)
}

fn unreachable_msg(e: &ClientError) -> i32 {
    eprintln!("corral: {e}");
    match e {
        ClientError::Unreachable(_) => exitcode::UNREACHABLE,
        ClientError::Other(_) => 1,
    }
}

/// `Some(code)` when the guard replied with an error.
fn reply_error(r: &Value) -> Option<i32> {
    if r.get("ok").and_then(Value::as_bool) == Some(false) && r.get("error").is_some() {
        let msg = r["error"].as_str().unwrap_or("error");
        eprintln!("corral: {msg}");
        return Some(if msg.starts_with("forbidden") {
            exitcode::AUTH_DENIED
        } else if msg.starts_with("unavailable") || msg.contains("not available") {
            exitcode::USAGE
        } else {
            1
        });
    }
    None
}

fn local_offset() -> String {
    let z = std::process::Command::new("date")
        .arg("+%z")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default();
    if z.len() == 5 { format!("{}:{}", &z[..3], &z[3..]) } else { "+00:00".into() }
}

pub fn status_text(s: &Value) -> String {
    let mut o = String::new();
    let g = |p: &str| s.pointer(p).cloned().unwrap_or(Value::Null);
    o.push_str(&format!(
        "host {}  mode {}  level {}",
        g("/host").as_str().unwrap_or("?"),
        g("/mode").as_str().unwrap_or("?"),
        g("/level").as_str().unwrap_or("?")
    ));
    if let Some(w) = s["why"].as_str().filter(|w| !w.is_empty()) {
        o.push_str(&format!("  ({w})"));
    }
    o.push('\n');
    let sm = &s["sample"];
    let num = |k: &str| sm[k].as_f64();
    let mut sig = Vec::new();
    if let Some(v) = num("free_pct") {
        sig.push(format!("free={v:.0}%"));
    }
    if let Some(v) = num("avail_mb") {
        sig.push(format!("avail={v:.0}MB"));
    }
    if let Some(v) = num("swap_mb") {
        sig.push(format!("swap={v:.0}MB"));
    }
    if let Some(v) = num("pressure") {
        sig.push(format!("pressure={v:.0}"));
    }
    if let Some(v) = num("psi_full_avg10") {
        sig.push(format!("psi_full={v:.1}"));
    }
    if let Some(v) = num("temp_c") {
        sig.push(format!("temp={v:.0}C"));
    }
    if let Some(v) = sm["thermal_state"].as_str() {
        sig.push(format!("thermal={v}"));
    }
    if let Some(v) = num("idle_s") {
        sig.push(format!("idle={v:.0}s"));
    }
    match sm["on_ac"].as_bool() {
        Some(true) => sig.push("power=ac".into()),
        Some(false) => sig.push("power=battery".into()),
        None => {}
    }
    o.push_str(&format!("signals: {}\n", if sig.is_empty() { "none readable".into() } else { sig.join(" ") }));
    if let Some(errs) = sm["errors"].as_array().filter(|e| !e.is_empty()) {
        o.push_str(&format!("probe errors: {}\n", errs.iter().filter_map(Value::as_str).collect::<Vec<_>>().join("; ")));
    }
    match s["kill_switch"].as_object() {
        Some(k) => o.push_str(&format!(
            "kill switch: ON since {} by {}: {}\n",
            corral_proto::util::rfc3339(k["since_ms"].as_i64().unwrap_or(0)),
            k["by"].as_str().unwrap_or("?"),
            k["reason"].as_str().unwrap_or("")
        )),
        None => o.push_str("kill switch: off\n"),
    }
    if s["table_ok"] == json!(false) {
        o.push_str("process table: unreadable (no decisions on units)\n");
    }
    let units = s["units"].as_array().cloned().unwrap_or_default();
    o.push_str(&format!(
        "watched units: {}  registrations: {}  reservations: {}\n",
        units.len(),
        s["registrations"].as_array().map_or(0, Vec::len),
        s["reservations"].as_array().map_or(0, Vec::len)
    ));
    for u in &units {
        let flags = [(u["protected"] == json!(true), " protected"), (u["never"] == json!(true), " never")]
            .iter()
            .filter(|(b, _)| *b)
            .map(|(_, f)| *f)
            .collect::<String>();
        o.push_str(&format!(
            "  tier {} {} ({}) pids {} {}MB{}\n",
            u["tier"],
            u["comm"].as_str().unwrap_or("?"),
            u["matcher"].as_str().unwrap_or("?"),
            u["pids"],
            u["footprint_mb"],
            flags
        ));
    }
    if let Some(locks) = s["locks"].as_object() {
        for (n, l) in locks {
            o.push_str(&format!("lock {n}: {}\n", l["state"].as_str().unwrap_or("?")));
        }
    }
    for a in s["last_actions"].as_array().into_iter().flatten() {
        o.push_str(&format!(
            "  {} {} {} {}\n",
            a["t"].as_str().unwrap_or(""),
            a["kind"].as_str().unwrap_or(""),
            a["reason"].as_str().unwrap_or(""),
            a["comm"].as_str().unwrap_or("")
        ));
    }
    o
}

fn decision_line(d: &Value) -> String {
    let tier = d["tier"].as_u64().map(|t| format!(" tier {t}")).unwrap_or_default();
    let pids = d["pids"].as_array().filter(|p| !p.is_empty()).map(|p| format!(" pids {}", Value::Array(p.clone()))).unwrap_or_default();
    format!(
        "{} {:<12} level={}{}{} {}: {}",
        d["t"].as_str().unwrap_or(""),
        d["kind"].as_str().unwrap_or(""),
        d["level"].as_str().unwrap_or(""),
        tier,
        pids,
        d["reason"].as_str().unwrap_or(""),
        d["detail"].as_str().unwrap_or("")
    )
}

pub fn guard_cmd(cmd: GuardCmd, cfg: &Config) -> i32 {
    let sock = match socket_path(cfg) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("corral: {e}");
            return exitcode::USAGE;
        }
    };
    let call = |op: &str, f: Value| match request(&sock, op, f) {
        Ok(r) => match reply_error(&r) {
            Some(code) => Err(code),
            None => Ok(r),
        },
        Err(e) => Err(unreachable_msg(&e)),
    };
    match cmd {
        GuardCmd::Run => unreachable!("handled by main"),
        GuardCmd::Status { json } => match call("status", json!({})) {
            Ok(r) => {
                if json {
                    println!("{}", serde_json::to_string_pretty(&r).expect("json"));
                } else {
                    print!("{}", status_text(&r));
                }
                0
            }
            Err(c) => c,
        },
        GuardCmd::Explain { n, json } => match call("explain", json!({ "n": n })) {
            Ok(r) => {
                for d in r["decisions"].as_array().into_iter().flatten() {
                    if json {
                        println!("{d}");
                    } else {
                        println!("{}", decision_line(d));
                    }
                }
                0
            }
            Err(c) => c,
        },
        GuardCmd::Stop { reason, by } => {
            match call("stop", json!({ "by": by.unwrap_or_else(whoami), "reason": reason.unwrap_or_else(|| "no reason given".into()) })) {
                Ok(r) => {
                    println!(
                        "kill switch on{}",
                        r["note"].as_str().filter(|n| !n.is_empty()).map(|n| format!(" ({n})")).unwrap_or_default()
                    );
                    0
                }
                Err(c) => c,
            }
        }
        GuardCmd::Resume { by } => match call("resume", json!({ "by": by.unwrap_or_else(whoami) })) {
            Ok(_) => {
                println!("resumed");
                0
            }
            Err(c) => c,
        },
        GuardCmd::Metrics => match call("metrics", json!({})) {
            Ok(r) => {
                print!("{}", r["text"].as_str().unwrap_or(""));
                0
            }
            Err(c) => c,
        },
        GuardCmd::Compare { legacy, ours, window, utc_offset, json } => compare_cmd(cfg, &legacy, ours, window, utc_offset, json),
    }
}

fn whoami() -> String {
    std::env::var("USER").unwrap_or_else(|_| "cli".into())
}

fn compare_cmd(cfg: &Config, legacy: &PathBuf, ours: Option<PathBuf>, window: u64, offset: Option<String>, json: bool) -> i32 {
    let fail = |m: String| {
        eprintln!("corral: {m}");
        exitcode::USAGE
    };
    let legacy_text = match std::fs::read_to_string(legacy) {
        Ok(t) => t,
        Err(e) => return fail(format!("{}: {e}", legacy.display())),
    };
    let paths = match ours {
        Some(p) => vec![p],
        None => match GuardSettings::from_config(cfg) {
            Ok(s) => vec![s.state_dir.join("decisions.jsonl.1"), s.state_dir.join("decisions.jsonl")],
            Err(e) => return fail(e),
        },
    };
    let mut text = String::new();
    for p in &paths {
        if let Ok(t) = std::fs::read_to_string(p) {
            text.push_str(&t);
        } else if paths.len() == 1 {
            return fail(format!("{}: cannot read the decision log", p.display()));
        }
    }
    let (decisions, bad) = read_log(&text);
    if bad > 0 {
        eprintln!("corral: {bad} decision log lines did not parse and were skipped");
    }
    let (events, unknown) = match parse_legacy(&legacy_text, &offset.unwrap_or_else(local_offset)) {
        Ok(v) => v,
        Err(e) => return fail(e),
    };
    let report = compare(&events, unknown, &decisions, window as i64 * 1000);
    if json {
        println!("{}", serde_json::to_string_pretty(&report).expect("json"));
    } else {
        print!("{}", report.render());
    }
    if report.clean() { 0 } else { exitcode::REFUSED }
}

pub fn admit_cmd(a: AdmitArgs, cfg: &Config) -> i32 {
    let usage = |m: String| {
        eprintln!("corral: {m}");
        exitcode::USAGE
    };
    if !matches!(a.class.as_str(), "heavy" | "light") {
        return usage(format!("--class is heavy or light, got {:?}", a.class));
    }
    let ram_mb = match a.ram.as_deref().map(parse_size_mb).transpose() {
        Ok(v) => v.unwrap_or(0),
        Err(e) => return usage(format!("--ram: {e}")),
    };
    let reserve_s = match a.reserve.as_deref().map(parse_duration_ms).transpose() {
        Ok(v) => v.unwrap_or(0) / 1000,
        Err(e) => return usage(format!("--reserve: {e}")),
    };
    let sock = match socket_path(cfg) {
        Ok(s) => s,
        Err(e) => return usage(e),
    };
    let reply = request(
        &sock,
        "admit",
        json!({ "name": a.name, "ram_mb": ram_mb, "class": a.class, "profile": a.profile, "reserve_s": reserve_s }),
    );
    let r = match reply {
        Ok(r) => r,
        Err(e) => {
            if a.legacy_output {
                println!("refused: guard unreachable");
                return exitcode::REFUSED;
            }
            return unreachable_msg(&e);
        }
    };
    if let Some(code) = reply_error(&r) {
        return code;
    }
    if a.json {
        println!("{r}");
    }
    let reasons: Vec<&Value> = r["reasons"].as_array().into_iter().flatten().collect();
    if r["ok"] == json!(true) {
        if !a.json {
            match r["reservation"].as_str() {
                Some(t) if !a.legacy_output => println!("ok reservation={t} until={}", r["until"].as_str().unwrap_or("")),
                _ => println!("ok"),
            }
        }
        return exitcode::OK;
    }
    if !a.json {
        let text = |v: &Value| v["reason"].as_str().unwrap_or("").to_string();
        println!("refused: {}", reasons.first().map(|v| text(v)).unwrap_or_default());
        if !a.legacy_output && reasons.len() > 1 {
            println!("also: {}", reasons[1..].iter().map(|v| text(v)).collect::<Vec<_>>().join("; "));
        }
    }
    exitcode::REFUSED
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_text_names_what_matters() {
        let s = json!({
            "host": "laptop", "mode": "observe", "level": "shed", "why": "free 19% <= 20%", "table_ok": true,
            "sample": { "free_pct": 19.0, "swap_mb": 5100, "pressure": 1, "errors": ["idle: no session"] },
            "kill_switch": null,
            "units": [{ "tier": 1, "comm": "llama-server", "matcher": "llm", "pids": [812], "footprint_mb": 3400, "protected": false, "never": false }],
            "registrations": [], "reservations": [], "locks": { "emulator": { "state": "free" } },
            "last_actions": []
        });
        let t = status_text(&s);
        for want in [
            "level shed  (free 19% <= 20%)",
            "free=19%",
            "swap=5100MB",
            "probe errors: idle: no session",
            "kill switch: off",
            "tier 1 llama-server (llm) pids [812] 3400MB",
            "lock emulator: free",
        ] {
            assert!(t.contains(want), "{want:?} missing in:\n{t}");
        }
    }
}
