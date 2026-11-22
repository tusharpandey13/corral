//! Request and reply bodies (PROTOCOL.md sections 4, 6, 9). Envelope fields are added by `envelope`.

use crate::job::JobView;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

// ---- client API ---------------------------------------------------------------------------------------------------

/// `submit` takes a [`JobSpec`] as its body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitReply {
    pub id: String,
    pub state: String,
    #[serde(default)]
    pub upload_url: Option<String>,
    #[serde(default)]
    pub upload_token: Option<String>,
    /// Set when the job cannot be placed on any known worker right now (e.g. RAM above every known worker's cap).
    #[serde(default)]
    pub unplaceable: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetReq {
    pub id: String,
}

/// `follow`: ask the scheduler to stream a job's logs, events and progress into the caller's own inbox space.
/// `deliver` must start with the caller's inbox prefix (`_INBOX_<slug>.`); the scheduler creates three push
/// consumers delivering to `<deliver>.log`, `<deliver>.ev` and `<deliver>.prog`. Followers never get JetStream
/// consumer permissions themselves (PROTOCOL.md 12.1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FollowReq {
    pub id: String,
    pub deliver: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ListReq {
    #[serde(default)]
    pub all: bool,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListReply {
    pub jobs: Vec<JobView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelReq {
    pub id: String,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelReply {
    pub id: String,
    pub state: String,
    pub cancel_requested: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResubmitReq {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactsReq {
    pub id: String,
    #[serde(default)]
    pub attempt: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactsReply {
    pub url: String,
    pub token: String,
    pub state: String,
    /// Token for `GET /v1/jobs/<id>/logs/<attempt>/<stream>` (`log:get`).
    #[serde(default)]
    pub log_token: Option<String>,
    #[serde(default)]
    pub attempt: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AdminReq {
    #[serde(default)]
    pub worker: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminReply {
    pub ok: bool,
    #[serde(default)]
    pub detail: Option<String>,
}

/// One row of `corral workers`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRow {
    pub host: String,
    pub online: bool,
    #[serde(default)]
    pub presence: Option<Presence>,
    #[serde(default)]
    pub flags: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkersReply {
    pub workers: Vec<WorkerRow>,
    pub queue_paused: bool,
}

// ---- worker API ---------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Slots {
    #[serde(default)]
    pub heavy_free: u32,
    #[serde(default)]
    pub light_free: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Blocked {
    pub check: String,
    #[serde(default)]
    pub profile: Option<String>,
    pub reason: String,
    #[serde(default)]
    pub since: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Admission {
    #[serde(default)]
    pub heavy_ok: bool,
    #[serde(default)]
    pub exclusive_ok: bool,
    #[serde(default)]
    pub profiles_ok: Vec<String>,
    #[serde(default)]
    pub admissible_ram_mb: u64,
    #[serde(default)]
    pub blocked: Vec<Blocked>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimReq {
    pub version: String,
    pub runtime: String,
    #[serde(default)]
    pub cost_tier: u32,
    pub caps: Vec<String>,
    pub slots: Slots,
    pub admission: Admission,
    #[serde(default)]
    pub running: Vec<String>,
    #[serde(default)]
    pub leases: Vec<String>,
    /// Worker pause reason (e.g. `pause_on_exit`), if paused. A paused worker is never given work.
    #[serde(default)]
    pub paused: Option<String>,
    /// Draining workers finish what they run and claim nothing.
    #[serde(default)]
    pub draining: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimReply {
    pub job: Option<JobView>,
    #[serde(default)]
    pub attempt: u32,
    #[serde(default)]
    pub lease_token: Option<String>,
    #[serde(default)]
    pub lease_ttl_s: u64,
    #[serde(default)]
    pub input_url: Option<String>,
    #[serde(default)]
    pub artifact_url: Option<String>,
    #[serde(default)]
    pub http_token: Option<String>,
    /// Token for downloading a snapshot input (`input:get`).
    #[serde(default)]
    pub input_token: Option<String>,
    #[serde(default)]
    pub retry_after_s: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HbReq {
    pub lease_token: String,
    pub attempt: u32,
    #[serde(default)]
    pub phase: String,
    #[serde(default)]
    pub pid: Option<u32>,
    #[serde(default)]
    pub footprint_mb: Option<u64>,
    #[serde(default)]
    pub cpu_s: Option<u64>,
    #[serde(default)]
    pub log_seq: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HbReply {
    pub ok: bool,
    #[serde(default)]
    pub lease_until: Option<String>,
    #[serde(default)]
    pub cancel: bool,
}

/// Attempt events (`corral.work.<w>.<id>.event`), PROTOCOL.md 6.4.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AttemptEvent {
    Started {
        pid: u32,
        pgid: u32,
    },
    Declined {
        reasons: Vec<DeclineReason>,
    },
    Finished(Finished),
    Artifacts {
        state: String,
        #[serde(default)]
        bytes: u64,
        #[serde(default)]
        sha256: Option<String>,
    },
    Note {
        text: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeclineReason {
    pub check: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Finished {
    /// `exited`, `killed` or `lost_local`
    pub outcome: String,
    #[serde(default)]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub signal: Option<i32>,
    /// A kill reason (`ram_exceeded`, `timeout`, ...) or `cancelled`.
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub detail: Option<String>,
    #[serde(default)]
    pub footprint_peak_mb: Option<u64>,
    #[serde(default)]
    pub duration_s: Option<u64>,
    #[serde(default)]
    pub disturbed: bool,
    #[serde(default)]
    pub log_dropped_bytes: u64,
    /// Final log sequence number per stream (lets followers know when they have seen everything).
    #[serde(default)]
    pub log_seq: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventReq {
    pub lease_token: String,
    #[serde(default)]
    pub attempt: u32,
    #[serde(flatten)]
    pub event: AttemptEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventReply {
    pub ok: bool,
}

// ---- pushes, presence, job events ---------------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ctl {
    pub v: u32,
    /// `cancel`, `kill_stale`, `pause`, `resume`, `drain`
    pub action: String,
    #[serde(default)]
    pub job: Option<String>,
    #[serde(default)]
    pub lease_token: Option<String>,
    #[serde(default)]
    pub by: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Wake {
    pub v: u32,
    pub reason: String,
    #[serde(default)]
    pub job: Option<String>,
}

/// `corral.event.<id>` payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobEvent {
    pub v: u32,
    pub id: String,
    pub state: String,
    #[serde(default)]
    pub prev: Option<String>,
    #[serde(default)]
    pub attempt: u32,
    #[serde(default)]
    pub worker: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub waiting: Option<Value>,
    #[serde(default)]
    pub log_final: BTreeMap<String, u64>,
    pub t: String,
}

/// `corral.progress.<w>.<id>` payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressMsg {
    pub v: u32,
    pub attempt: u32,
    pub pct: u8,
    pub text: String,
    pub t: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Metrics {
    #[serde(default)]
    pub free_pct: Option<u32>,
    #[serde(default)]
    pub avail_mb: Option<u64>,
}

/// Worker presence, the KV value of `corral_workers.<w>` (PROTOCOL.md 9).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Presence {
    pub v: u32,
    pub from: String,
    pub host: String,
    pub version: String,
    pub runtime: String,
    #[serde(default)]
    pub cost_tier: u32,
    pub caps: Vec<String>,
    pub slots: Slots,
    pub admission: Admission,
    #[serde(default)]
    pub running: Vec<String>,
    #[serde(default)]
    pub leases: Vec<String>,
    #[serde(default)]
    pub metrics: Metrics,
    #[serde(default)]
    pub paused: Option<String>,
    #[serde(default)]
    pub draining: bool,
    #[serde(default)]
    pub kill_switch: bool,
    pub t: String,
}
