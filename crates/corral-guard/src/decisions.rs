//! The decision log: one JSON line per thing the guard decided (or would have done in observe mode), the format a
//! later step diffs against the legacy guards' logs (`compare.rs`). R-GUARD-08, R-GUARD-10.
//!
//! Lines are versioned (`"v": 1`). Kinds: `level` (the level changed), `would_kill`, `would_pause`, `would_resume`,
//! `would_hook`, `skipped` (an action withheld, with the reason), `admit` (an admission answer), `stop`, `resume`
//! (the kill switch). In enforce mode (M3) the `would_` prefix is dropped.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Decision {
    pub v: u32,
    pub t_ms: i64,
    /// RFC 3339, for people reading the file.
    pub t: String,
    pub host: String,
    pub mode: String,
    pub kind: String,
    pub level: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matcher: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pids: Vec<u32>,
    /// Program base name of the unit's leader (never the arguments).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comm: Option<String>,
    pub reason: String,
    /// The measurements and the rule that produced the decision.
    #[serde(default)]
    pub detail: String,
    /// For `admit`: the answer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ok: Option<bool>,
}

impl Decision {
    pub fn new(t_ms: i64, kind: &str, level: &str, reason: impl Into<String>, detail: impl Into<String>) -> Decision {
        Decision {
            v: 1,
            t_ms,
            t: corral_proto::util::rfc3339(t_ms),
            host: String::new(),
            mode: String::new(),
            kind: kind.into(),
            level: level.into(),
            tier: None,
            matcher: None,
            unit: None,
            pids: vec![],
            comm: None,
            reason: reason.into(),
            detail: detail.into(),
            ok: None,
        }
    }

    pub fn is_action(&self) -> bool {
        matches!(
            self.kind.as_str(),
            "would_kill" | "would_pause" | "would_resume" | "would_hook" | "kill" | "pause" | "resume_unit" | "hook"
        )
    }
}

/// Appends decisions to `<dir>/decisions.jsonl` (10 MB x 3 rotation) and keeps the latest ones for `explain`.
pub struct DecisionLog {
    path: PathBuf,
    host: String,
    mode: String,
    max_bytes: u64,
    size: u64,
    recent: VecDeque<Decision>,
    pub write_errors: u64,
}

const KEEP: usize = 200;

impl DecisionLog {
    pub fn open(dir: &std::path::Path, host: &str, mode: &str) -> DecisionLog {
        let path = dir.join("decisions.jsonl");
        let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        DecisionLog {
            path,
            host: host.into(),
            mode: mode.into(),
            max_bytes: 10 * 1024 * 1024,
            size,
            recent: VecDeque::new(),
            write_errors: 0,
        }
    }

    pub fn with_max_bytes(mut self, n: u64) -> DecisionLog {
        self.max_bytes = n;
        self
    }

    pub fn record(&mut self, mut d: Decision) -> Decision {
        d.host = self.host.clone();
        d.mode = self.mode.clone();
        let line = serde_json::to_string(&d).expect("json");
        tracing::info!(target: "corral_guard::decision", "{line}");
        if self.size + line.len() as u64 + 1 > self.max_bytes {
            self.rotate();
        }
        match OpenOptions::new().create(true).append(true).open(&self.path).and_then(|mut f| {
            use std::os::unix::fs::PermissionsExt;
            let _ = f.set_permissions(std::fs::Permissions::from_mode(0o600));
            writeln!(f, "{line}")
        }) {
            Ok(()) => self.size += line.len() as u64 + 1,
            Err(e) => {
                self.write_errors += 1;
                tracing::warn!("cannot append to {}: {e}", self.path.display());
            }
        }
        self.recent.push_back(d.clone());
        while self.recent.len() > KEEP {
            self.recent.pop_front();
        }
        d
    }

    fn rotate(&mut self) {
        let with = |n: u32| PathBuf::from(format!("{}.{n}", self.path.display()));
        let _ = std::fs::remove_file(with(3));
        for n in (1..3).rev() {
            let _ = std::fs::rename(with(n), with(n + 1));
        }
        let _ = std::fs::rename(&self.path, with(1));
        self.size = 0;
    }

    /// The latest `n` decisions, oldest first; `all` includes the `level` changes.
    pub fn last(&self, n: usize, only_actions: bool) -> Vec<Decision> {
        let v: Vec<&Decision> = self.recent.iter().filter(|d| !only_actions || d.is_action()).collect();
        v[v.len().saturating_sub(n)..].iter().map(|d| (*d).clone()).collect()
    }
}

/// Read a decision log back (comparison, tests). Lines that do not parse are skipped, and counted.
pub fn read_log(text: &str) -> (Vec<Decision>, usize) {
    let mut bad = 0;
    let v = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| {
            let d = serde_json::from_str::<Decision>(l).ok();
            if d.is_none() {
                bad += 1;
            }
            d
        })
        .collect();
    (v, bad)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_rotates_and_reads_back() {
        let dir = std::env::temp_dir().join(format!("cg-dlog-{}", corral_proto::util::random_b32(6)));
        std::fs::create_dir_all(&dir).unwrap();
        let mut log = DecisionLog::open(&dir, "laptop", "observe").with_max_bytes(600);
        for i in 0..12 {
            let mut d = Decision::new(1_000 + i, "would_kill", "shed", "system_memory", "free=19%");
            d.tier = Some(1);
            d.pids = vec![10, 11];
            log.record(d);
        }
        assert_eq!(log.last(5, true).len(), 5);
        assert!(dir.join("decisions.jsonl.1").exists(), "rotated");
        let (back, bad) = read_log(&std::fs::read_to_string(dir.join("decisions.jsonl")).unwrap());
        assert_eq!(bad, 0);
        assert!(!back.is_empty());
        assert_eq!((back[0].host.as_str(), back[0].mode.as_str(), back[0].v), ("laptop", "observe", 1));
        assert_eq!(read_log("not json\n{\"v\":1}\n").1, 2);
    }
}
