//! Job spec, states, kill reasons (PROTOCOL.md sections 4, 5).

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Uploading,
    Queued,
    Claimed,
    Running,
    Orphaned,
    Succeeded,
    Failed,
    Killed,
    Cancelled,
    Lost,
    Expired,
}

impl JobState {
    pub const ALL: [JobState; 11] = [
        JobState::Uploading,
        JobState::Queued,
        JobState::Claimed,
        JobState::Running,
        JobState::Orphaned,
        JobState::Succeeded,
        JobState::Failed,
        JobState::Killed,
        JobState::Cancelled,
        JobState::Lost,
        JobState::Expired,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            JobState::Uploading => "uploading",
            JobState::Queued => "queued",
            JobState::Claimed => "claimed",
            JobState::Running => "running",
            JobState::Orphaned => "orphaned",
            JobState::Succeeded => "succeeded",
            JobState::Failed => "failed",
            JobState::Killed => "killed",
            JobState::Cancelled => "cancelled",
            JobState::Lost => "lost",
            JobState::Expired => "expired",
        }
    }

    pub fn parse(s: &str) -> Option<JobState> {
        JobState::ALL.into_iter().find(|st| st.as_str() == s)
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, JobState::Succeeded | JobState::Failed | JobState::Killed | JobState::Cancelled | JobState::Lost | JobState::Expired)
    }
}

/// Closed set of kill reasons (PROTOCOL.md 5.2). `cancelled` is a worker-reported reason that maps to the
/// `cancelled` state, so it is handled as a plain string at the protocol edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KillReason {
    RamExceeded,
    SystemMemory,
    Thermal,
    KillSwitch,
    Timeout,
    ExternalSignal,
    OomKernel,
    StaleLease,
    LeaseKilled,
}

impl KillReason {
    pub fn as_str(self) -> &'static str {
        match self {
            KillReason::RamExceeded => "ram_exceeded",
            KillReason::SystemMemory => "system_memory",
            KillReason::Thermal => "thermal",
            KillReason::KillSwitch => "kill_switch",
            KillReason::Timeout => "timeout",
            KillReason::ExternalSignal => "external_signal",
            KillReason::OomKernel => "oom_kernel",
            KillReason::StaleLease => "stale_lease",
            KillReason::LeaseKilled => "lease_killed",
        }
    }
    pub fn parse(s: &str) -> Option<KillReason> {
        use KillReason::*;
        [RamExceeded, SystemMemory, Thermal, KillSwitch, Timeout, ExternalSignal, OomKernel, StaleLease, LeaseKilled]
            .into_iter()
            .find(|r| r.as_str() == s)
    }
}

/// The reason the worker reports for its own cancel kills.
pub const REASON_CANCELLED: &str = "cancelled";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Class {
    #[default]
    Heavy,
    Light,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Input {
    Git {
        repo: String,
        #[serde(rename = "ref")]
        git_ref: String,
        sha: String,
    },
    Snapshot {
        sha256: String,
        bytes: u64,
        #[serde(default)]
        base_sha: Option<String>,
        #[serde(default)]
        repo_hint: Option<String>,
    },
    None,
}

fn d_cwd() -> String {
    ".".into()
}
fn d_profile() -> String {
    "default".into()
}
fn d_lock() -> String {
    "auto".into()
}
fn d_priority() -> u8 {
    50
}
fn d_timeout() -> u64 {
    7200
}
fn d_art_mb() -> u64 {
    2048
}
fn d_true() -> bool {
    true
}
fn d_max_attempts() -> u32 {
    2
}
fn d_continue() -> String {
    "continue".into()
}
fn d_notify() -> String {
    "on_fail".into()
}
fn d_kind() -> String {
    "command".into()
}
fn d_none_input() -> Input {
    Input::None
}

/// The job spec of PROTOCOL.md section 4 (the body of `submit`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobSpec {
    #[serde(default = "d_kind")]
    pub kind: String,
    #[serde(default)]
    pub cmd: Vec<String>,
    #[serde(default = "d_cwd")]
    pub cwd: String,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default = "d_none_input")]
    pub input: Input,
    #[serde(default)]
    pub caps: Vec<String>,
    #[serde(default)]
    pub prefer: Option<String>,
    /// Seconds; `None` = default (30 min when `prefer` is set); `-1` = never fall back.
    #[serde(default)]
    pub fallback_after_s: Option<i64>,
    #[serde(default)]
    pub ram_mb: Option<u64>,
    #[serde(default)]
    pub class: Class,
    #[serde(default)]
    pub exclusive: bool,
    #[serde(default = "d_profile")]
    pub profile: String,
    #[serde(default = "d_lock")]
    pub lock: String,
    #[serde(default = "d_priority")]
    pub priority: u8,
    #[serde(default = "d_timeout")]
    pub timeout_s: u64,
    /// RFC 3339.
    #[serde(default)]
    pub not_after: Option<String>,
    #[serde(default)]
    pub artifacts: Vec<String>,
    #[serde(default = "d_art_mb")]
    pub artifacts_max_mb: u64,
    #[serde(default = "d_true")]
    pub retry_on_lost: bool,
    #[serde(default = "d_max_attempts")]
    pub max_attempts: u32,
    #[serde(default)]
    pub pause_on_exit: Vec<i32>,
    #[serde(default = "d_continue")]
    pub on_user_input: String,
    #[serde(default)]
    pub max_cost_tier: u32,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default = "d_notify")]
    pub notify: String,
    #[serde(default)]
    pub parent: Option<String>,
    /// Set by `resubmit`.
    #[serde(default)]
    pub retry_of: Option<String>,
}

impl JobSpec {
    /// A minimal heavy-less spec for tests and tools.
    pub fn simple(cmd: &[&str]) -> JobSpec {
        serde_json::from_value(serde_json::json!({ "cmd": cmd, "class": "light", "ram_mb": 64 })).expect("valid spec")
    }

    /// RAM the job declares; light jobs without a declaration use 0.
    pub fn ram(&self) -> u64 {
        self.ram_mb.unwrap_or(0)
    }

    /// Effective fallback delay in seconds; `None` means "pinned: never fall back".
    pub fn fallback_after(&self) -> Option<i64> {
        match self.fallback_after_s {
            Some(-1) => None,
            Some(s) => Some(s.max(0)),
            None => Some(1800),
        }
    }
}

/// One waiting explanation line for a candidate worker (SPEC.md section 7).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Waiting {
    pub worker: String,
    /// `blocked`, `never`, `offline`, `paused`, `pinned`
    pub kind: String,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobView {
    pub id: String,
    pub state: JobState,
    pub kind: String,
    pub spec: JobSpec,
    pub submitter: String,
    pub attempts: u32,
    #[serde(default)]
    pub worker: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub detail: Option<String>,
    pub created_at: String,
    #[serde(default)]
    pub queued_at: Option<String>,
    #[serde(default)]
    pub started_at: Option<String>,
    #[serde(default)]
    pub finished_at: Option<String>,
    #[serde(default)]
    pub waiting: Vec<Waiting>,
    #[serde(default)]
    pub cancel_requested: bool,
    /// `uploaded`, `refused`, `none`, or absent.
    #[serde(default)]
    pub artifacts_state: Option<String>,
    /// Final log sequence numbers per stream, reported by the worker (lets followers know when they are complete).
    #[serde(default)]
    pub log_final: BTreeMap<String, u64>,
    #[serde(default)]
    pub retry_of: Option<String>,
    #[serde(default)]
    pub progress: Option<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_defaults_match_protocol() {
        let s: JobSpec = serde_json::from_str(r#"{"cmd":["true"]}"#).unwrap();
        assert_eq!(s.cwd, ".");
        assert_eq!(s.priority, 50);
        assert_eq!(s.timeout_s, 7200);
        assert_eq!(s.class, Class::Heavy);
        assert!(s.retry_on_lost);
        assert_eq!(s.max_attempts, 2);
        assert_eq!(s.input, Input::None);
        assert_eq!(s.fallback_after(), Some(1800));
    }

    #[test]
    fn git_input_uses_ref_key() {
        let i: Input = serde_json::from_str(r#"{"kind":"git","repo":"r","ref":"main","sha":"abc"}"#).unwrap();
        assert_eq!(i, Input::Git { repo: "r".into(), git_ref: "main".into(), sha: "abc".into() });
    }

    #[test]
    fn kill_reasons_round_trip() {
        for r in [
            "ram_exceeded",
            "system_memory",
            "thermal",
            "kill_switch",
            "timeout",
            "external_signal",
            "oom_kernel",
            "stale_lease",
            "lease_killed",
        ] {
            assert_eq!(KillReason::parse(r).unwrap().as_str(), r);
        }
        assert!(KillReason::parse("cancelled").is_none());
    }
}
