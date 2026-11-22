//! SQLite job store: the only writer of job state (R-QUEUE-02, R-QUEUE-03).
//!
//! Every method takes `now` (unix milliseconds) so unit tests drive time with a fake clock. Every state change goes
//! through [`transition`], which uses the single table in `corral_proto::state`.

use crate::settings::Settings;
use corral_proto::PROTOCOL_VERSION;
use corral_proto::api::{AttemptEvent, ClaimReq, DeclineReason, Finished, HbReq, Presence};
use corral_proto::api::{Ctl, JobEvent};
use corral_proto::envelope::{ApiError, ErrorCode};
use corral_proto::job::{Class, Input, JobSpec, JobState, JobView, KillReason, REASON_CANCELLED, Waiting};
use corral_proto::state::{Event, next};
use corral_proto::util::{new_job_id, new_token, random_b32, rfc3339};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug)]
pub enum StoreError {
    Api(ApiError),
    Db(rusqlite::Error),
}

impl From<rusqlite::Error> for StoreError {
    fn from(e: rusqlite::Error) -> Self {
        StoreError::Db(e)
    }
}
impl From<ApiError> for StoreError {
    fn from(e: ApiError) -> Self {
        StoreError::Api(e)
    }
}
impl From<StoreError> for ApiError {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::Api(a) => a,
            StoreError::Db(d) => {
                tracing::error!("database error: {d}");
                ApiError::new(ErrorCode::Unavailable, "internal database error")
            }
        }
    }
}

pub type Res<T> = Result<T, StoreError>;

/// Side effects the caller publishes after the transaction committed.
#[derive(Debug, Clone)]
pub enum Effect {
    Job(JobEvent),
    Ctl { worker: String, ctl: Ctl },
    Wake { job: String },
}

/// Forward-only numbered migrations (SPEC.md section 15).
const MIGRATIONS: &[&str] = &[r#"
CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE jobs(
  id TEXT PRIMARY KEY, state TEXT NOT NULL, kind TEXT NOT NULL, spec TEXT NOT NULL, submitter TEXT NOT NULL,
  priority INTEGER NOT NULL, class TEXT NOT NULL,
  created_at INTEGER NOT NULL, queued_at INTEGER, started_at INTEGER, finished_at INTEGER, updated_at INTEGER NOT NULL,
  not_after INTEGER, attempts INTEGER NOT NULL DEFAULT 0,
  worker TEXT, token TEXT, lease_until INTEGER, orphaned_at INTEGER, claimed_at INTEGER,
  cancel_requested INTEGER NOT NULL DEFAULT 0, cancel_reason TEXT,
  reason TEXT, exit_code INTEGER, detail TEXT, artifacts_state TEXT, log_final TEXT NOT NULL DEFAULT '{}',
  retry_of TEXT, upload_id TEXT, progress INTEGER, last_decline TEXT
);
CREATE INDEX jobs_state ON jobs(state);
CREATE INDEX jobs_upload ON jobs(upload_id);
CREATE TABLE attempts(
  job TEXT NOT NULL, n INTEGER NOT NULL, worker TEXT NOT NULL, token TEXT NOT NULL,
  started_at INTEGER NOT NULL, ended_at INTEGER, outcome TEXT, reason TEXT, exit_code INTEGER
);
CREATE INDEX attempts_job ON attempts(job);
CREATE TABLE events(
  id INTEGER PRIMARY KEY, t INTEGER NOT NULL, identity TEXT, subject TEXT, job TEXT, kind TEXT NOT NULL, detail TEXT
);
CREATE TABLE requests(req_id TEXT NOT NULL, who TEXT NOT NULL, t INTEGER NOT NULL, reply BLOB NOT NULL, PRIMARY KEY(req_id, who));
CREATE TABLE log_cursors(
  job TEXT NOT NULL, attempt INTEGER NOT NULL, stream TEXT NOT NULL, seq INTEGER NOT NULL, bytes INTEGER NOT NULL,
  PRIMARY KEY(job, attempt, stream)
);
CREATE TABLE worker_flags(worker TEXT PRIMARY KEY, paused TEXT, draining INTEGER NOT NULL DEFAULT 0);
"#];

#[derive(Debug, Clone)]
pub struct JobRow {
    pub id: String,
    pub state: JobState,
    pub kind: String,
    pub spec: JobSpec,
    pub submitter: String,
    pub created_at: i64,
    pub queued_at: Option<i64>,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub updated_at: i64,
    pub not_after: Option<i64>,
    pub attempts: u32,
    pub worker: Option<String>,
    pub token: Option<String>,
    pub lease_until: Option<i64>,
    pub orphaned_at: Option<i64>,
    pub claimed_at: Option<i64>,
    pub cancel_requested: bool,
    pub cancel_reason: Option<String>,
    pub reason: Option<String>,
    pub exit_code: Option<i32>,
    pub detail: Option<String>,
    pub artifacts_state: Option<String>,
    pub log_final: BTreeMap<String, u64>,
    pub retry_of: Option<String>,
    pub upload_id: Option<String>,
    pub progress: Option<u8>,
    pub last_decline: Option<String>,
}

const COLS: &str = "id,state,kind,spec,submitter,created_at,queued_at,started_at,finished_at,updated_at,not_after,attempts,worker,token,\
lease_until,orphaned_at,claimed_at,cancel_requested,cancel_reason,reason,exit_code,detail,artifacts_state,log_final,retry_of,upload_id,progress,last_decline";

fn row_to_job(r: &Row<'_>) -> rusqlite::Result<JobRow> {
    let bad = |e: String| rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, e.into());
    let state: String = r.get(1)?;
    let spec: String = r.get(3)?;
    let log_final: String = r.get(23)?;
    Ok(JobRow {
        id: r.get(0)?,
        state: JobState::parse(&state).ok_or_else(|| bad(format!("bad state {state}")))?,
        kind: r.get(2)?,
        spec: serde_json::from_str(&spec).map_err(|e| bad(e.to_string()))?,
        submitter: r.get(4)?,
        created_at: r.get(5)?,
        queued_at: r.get(6)?,
        started_at: r.get(7)?,
        finished_at: r.get(8)?,
        updated_at: r.get(9)?,
        not_after: r.get(10)?,
        attempts: r.get::<_, i64>(11)? as u32,
        worker: r.get(12)?,
        token: r.get(13)?,
        lease_until: r.get(14)?,
        orphaned_at: r.get(15)?,
        claimed_at: r.get(16)?,
        cancel_requested: r.get::<_, i64>(17)? != 0,
        cancel_reason: r.get(18)?,
        reason: r.get(19)?,
        exit_code: r.get::<_, Option<i64>>(20)?.map(|c| c as i32),
        detail: r.get(21)?,
        artifacts_state: r.get(22)?,
        log_final: serde_json::from_str(&log_final).map_err(|e| bad(e.to_string()))?,
        retry_of: r.get(24)?,
        upload_id: r.get(25)?,
        progress: r.get::<_, Option<i64>>(26)?.map(|p| p as u8),
        last_decline: r.get(27)?,
    })
}

fn load(tx: &Connection, id: &str) -> Res<Option<JobRow>> {
    Ok(tx.query_row(&format!("SELECT {COLS} FROM jobs WHERE id=?1"), [id], row_to_job).optional()?)
}

fn save(tx: &Connection, j: &JobRow) -> Res<()> {
    tx.execute(
        "UPDATE jobs SET state=?2, spec=?3, queued_at=?4, started_at=?5, finished_at=?6, updated_at=?7, attempts=?8, worker=?9, token=?10,
         lease_until=?11, orphaned_at=?12, claimed_at=?13, cancel_requested=?14, cancel_reason=?15, reason=?16, exit_code=?17, detail=?18,
         artifacts_state=?19, log_final=?20, progress=?21, last_decline=?22 WHERE id=?1",
        params![
            j.id,
            j.state.as_str(),
            serde_json::to_string(&j.spec).expect("spec json"),
            j.queued_at,
            j.started_at,
            j.finished_at,
            j.updated_at,
            j.attempts as i64,
            j.worker,
            j.token,
            j.lease_until,
            j.orphaned_at,
            j.claimed_at,
            j.cancel_requested as i64,
            j.cancel_reason,
            j.reason,
            j.exit_code.map(|c| c as i64),
            j.detail,
            j.artifacts_state,
            serde_json::to_string(&j.log_final).expect("json"),
            j.progress.map(|p| p as i64),
            j.last_decline,
        ],
    )?;
    Ok(())
}

pub fn audit(tx: &Connection, now: i64, identity: &str, subject: &str, job: Option<&str>, kind: &str, detail: &str) {
    let _ = tx.execute(
        "INSERT INTO events(t,identity,subject,job,kind,detail) VALUES (?1,?2,?3,?4,?5,?6)",
        params![now, identity, subject, job, kind, detail],
    );
}

/// The single place a job changes state. Illegal transitions are rejected and logged (R-QUEUE-03).
fn transition(tx: &Connection, j: &mut JobRow, ev: Event, now: i64, fx: &mut Vec<Effect>) -> Res<()> {
    let prev = j.state;
    let to = match next(prev, ev) {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(job = %j.id, "{e}");
            audit(tx, now, "scheduler", "", Some(&j.id), "illegal_transition", &e.to_string());
            return Err(ApiError::new(ErrorCode::Conflict, e.to_string()).into());
        }
    };
    j.state = to;
    j.updated_at = now;
    if to.is_terminal() {
        j.finished_at = Some(now);
    }
    audit(tx, now, "scheduler", "", Some(&j.id), "transition", &format!("{} -> {} ({ev:?})", prev.as_str(), to.as_str()));
    fx.push(Effect::Job(JobEvent {
        v: PROTOCOL_VERSION,
        id: j.id.clone(),
        state: to.as_str().into(),
        prev: Some(prev.as_str().into()),
        attempt: j.attempts,
        worker: j.worker.clone(),
        reason: j.reason.clone(),
        exit_code: j.exit_code,
        waiting: None,
        log_final: j.log_final.clone(),
        t: rfc3339(now),
    }));
    Ok(())
}

/// The part of a worker's state that decides eligibility; built from a claim or from presence.
pub struct WorkerView<'a> {
    pub caps: &'a [String],
    pub slots: &'a corral_proto::api::Slots,
    pub admission: &'a corral_proto::api::Admission,
    pub paused: Option<&'a str>,
    pub draining: bool,
}

impl<'a> From<&'a ClaimReq> for WorkerView<'a> {
    fn from(c: &'a ClaimReq) -> Self {
        WorkerView { caps: &c.caps, slots: &c.slots, admission: &c.admission, paused: c.paused.as_deref(), draining: c.draining }
    }
}
impl<'a> From<&'a Presence> for WorkerView<'a> {
    fn from(p: &'a Presence) -> Self {
        WorkerView { caps: &p.caps, slots: &p.slots, admission: &p.admission, paused: p.paused.as_deref(), draining: p.draining }
    }
}

/// Why a queued job cannot go to a worker right now (headline reasons for waiting explanations).
#[derive(Debug, PartialEq, Eq)]
pub enum Ineligible {
    QueuePaused,
    WorkerPaused(String),
    Draining,
    /// Permanent: the worker lacks these caps.
    Caps(Vec<String>),
    Ram {
        need: u64,
        have: u64,
    },
    NoSlot(&'static str),
    HeavyBlocked(String),
    ExclusiveBlocked,
    Profile(String),
    /// The job prefers another worker; `until` is when it may fall back (unix ms), `None` = pinned for good.
    Prefers {
        prefer: String,
        until: Option<i64>,
    },
}

impl Ineligible {
    /// `never` reasons do not change by themselves.
    pub fn is_permanent(&self) -> bool {
        matches!(self, Ineligible::Caps(_))
    }
}

impl std::fmt::Display for Ineligible {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Ineligible::QueuePaused => write!(f, "queue is paused"),
            Ineligible::WorkerPaused(r) => write!(f, "worker paused: {r}"),
            Ineligible::Draining => write!(f, "worker is draining"),
            Ineligible::Caps(c) => write!(f, "lacks caps {}", c.join(",")),
            Ineligible::Ram { need, have } => write!(f, "memory: job needs {need} MB, worker can admit {have} MB now"),
            Ineligible::NoSlot(c) => write!(f, "no free {c} slot"),
            Ineligible::HeavyBlocked(r) => write!(f, "{r}"),
            Ineligible::ExclusiveBlocked => write!(f, "exclusive jobs are not admissible now"),
            Ineligible::Profile(p) => write!(f, "profile {p} not offered"),
            Ineligible::Prefers { prefer, until: Some(t) } => write!(f, "prefers {prefer}; may fall back at {}", rfc3339(*t)),
            Ineligible::Prefers { prefer, until: None } => write!(f, "pinned to {prefer}"),
        }
    }
}

/// R-SCHED-01, R-SCHED-02: is this queued job offerable to this worker now?
pub fn eligibility(job: &JobRow, worker: &str, w: &WorkerView<'_>, now: i64, queue_paused: bool) -> Result<(), Ineligible> {
    if queue_paused {
        return Err(Ineligible::QueuePaused);
    }
    if let Some(p) = w.paused {
        return Err(Ineligible::WorkerPaused(p.to_string()));
    }
    if w.draining {
        return Err(Ineligible::Draining);
    }
    let missing: Vec<String> = job.spec.caps.iter().filter(|c| !w.caps.contains(c)).cloned().collect();
    if !missing.is_empty() {
        return Err(Ineligible::Caps(missing));
    }
    if let Some(prefer) = &job.spec.prefer
        && prefer != worker
    {
        match job.spec.fallback_after() {
            None => return Err(Ineligible::Prefers { prefer: prefer.clone(), until: None }),
            Some(s) => {
                let until = job.queued_at.unwrap_or(job.created_at) + s * 1000;
                if now < until {
                    return Err(Ineligible::Prefers { prefer: prefer.clone(), until: Some(until) });
                }
            }
        }
    }
    let ram = job.spec.ram();
    if ram > w.admission.admissible_ram_mb {
        return Err(Ineligible::Ram { need: ram, have: w.admission.admissible_ram_mb });
    }
    match job.spec.class {
        Class::Heavy => {
            if w.slots.heavy_free == 0 {
                return Err(Ineligible::NoSlot("heavy"));
            }
            if !w.admission.heavy_ok {
                let why = w
                    .admission
                    .blocked
                    .first()
                    .map(|b| format!("blocked: {}: {}", b.check, b.reason))
                    .unwrap_or_else(|| "blocked: heavy admission".into());
                return Err(Ineligible::HeavyBlocked(why));
            }
            if job.spec.exclusive && !w.admission.exclusive_ok {
                return Err(Ineligible::ExclusiveBlocked);
            }
            if !w.admission.profiles_ok.contains(&job.spec.profile) {
                return Err(Ineligible::Profile(job.spec.profile.clone()));
            }
        }
        Class::Light => {
            if w.slots.light_free == 0 {
                return Err(Ineligible::NoSlot("light"));
            }
        }
    }
    Ok(())
}

/// `(id, finished_at, keep_flagged, upload_id)`
pub type FinishedRow = (String, i64, bool, Option<String>);

pub struct ClaimOutcome {
    pub job: JobRow,
    pub token: String,
    pub effects: Vec<Effect>,
}

pub struct HbOutcome {
    pub lease_until: i64,
    pub cancel: bool,
    pub effects: Vec<Effect>,
}

pub struct SubmitOutcome {
    pub job: JobRow,
    pub effects: Vec<Effect>,
}

/// The note written into an archived log where chunks `last+1 .. seq` never arrived.
pub fn sink_gap_note(last: u64, seq: u64) -> Vec<u8> {
    format!("\n[corral] log chunks {}..{} are missing (never arrived)\n", last + 1, seq - 1).into_bytes()
}

pub enum SinkDecision {
    /// Write the chunk at this byte offset, then call `sink_commit`.
    Write {
        offset: u64,
    },
    Duplicate,
    /// A gap: redeliver later. `last` is the last sequence written and `offset` the file size, so the caller can
    /// give up after a while and record the gap (`sink_gap_note`).
    Hold {
        last: u64,
        offset: u64,
    },
    Drop,
}

pub struct Store {
    conn: Connection,
}

impl Store {
    pub fn open(path: &Path) -> Res<Store> {
        let existed = path.exists();
        let mut conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        let current: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
        if existed && (current as usize) < MIGRATIONS.len() && current > 0 {
            // A backup copy before each migration (SPEC.md section 15).
            let bak = path.with_extension(format!("db.bak-v{current}"));
            let _ = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)", []);
            let _ = std::fs::copy(path, bak);
        }
        for (i, sql) in MIGRATIONS.iter().enumerate().skip(current as usize) {
            let tx = conn.transaction()?;
            tx.execute_batch(sql)?;
            tx.pragma_update(None, "user_version", (i + 1) as i64)?;
            tx.commit()?;
        }
        Ok(Store { conn })
    }

    pub fn open_memory() -> Store {
        let mut conn = Connection::open_in_memory().expect("memory db");
        for (i, sql) in MIGRATIONS.iter().enumerate() {
            let tx = conn.transaction().expect("tx");
            tx.execute_batch(sql).expect("migration");
            tx.pragma_update(None, "user_version", (i + 1) as i64).expect("version");
            tx.commit().expect("commit");
        }
        Store { conn }
    }

    fn imm(&mut self) -> Res<Transaction<'_>> {
        Ok(self.conn.transaction_with_behavior(TransactionBehavior::Immediate)?)
    }

    pub fn integrity_ok(&self) -> bool {
        self.conn.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0)).map(|s| s == "ok").unwrap_or(false)
    }

    // ---- meta ----

    fn meta_get(c: &Connection, key: &str) -> Res<Option<String>> {
        Ok(c.query_row("SELECT value FROM meta WHERE key=?1", [key], |r| r.get(0)).optional()?)
    }
    fn meta_set(c: &Connection, key: &str, value: &str) -> Res<()> {
        c.execute("INSERT INTO meta(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value", params![key, value])?;
        Ok(())
    }

    pub fn queue_paused(&self) -> Res<bool> {
        Ok(Self::meta_get(&self.conn, "queue_paused")?.is_some_and(|v| v == "1"))
    }

    pub fn set_queue_paused(&mut self, paused: bool, now: i64, who: &str) -> Res<()> {
        let tx = self.imm()?;
        Self::meta_set(&tx, "queue_paused", if paused { "1" } else { "0" })?;
        audit(&tx, now, who, "", None, if paused { "queue_pause" } else { "queue_resume" }, "");
        tx.commit()?;
        Ok(())
    }

    pub fn worker_flags(&self, worker: &str) -> Res<(Option<String>, bool)> {
        Ok(self
            .conn
            .query_row("SELECT paused, draining FROM worker_flags WHERE worker=?1", [worker], |r| {
                Ok((r.get::<_, Option<String>>(0)?, r.get::<_, i64>(1)? != 0))
            })
            .optional()?
            .unwrap_or((None, false)))
    }

    pub fn set_worker_flags(&mut self, worker: &str, paused: Option<&str>, draining: bool, now: i64, who: &str) -> Res<()> {
        let tx = self.imm()?;
        tx.execute(
            "INSERT INTO worker_flags(worker,paused,draining) VALUES(?1,?2,?3) ON CONFLICT(worker) DO UPDATE SET paused=excluded.paused, draining=excluded.draining",
            params![worker, paused, draining as i64],
        )?;
        audit(&tx, now, who, "", None, "worker_flags", &format!("{worker} paused={paused:?} draining={draining}"));
        tx.commit()?;
        Ok(())
    }

    /// R-OPS-03: extend every live lease by the scheduler's own downtime, then record liveness.
    pub fn start_up(&mut self, now: i64) -> Res<i64> {
        let tx = self.imm()?;
        let last: Option<i64> = Self::meta_get(&tx, "last_alive")?.and_then(|v| v.parse().ok());
        let downtime = last.map(|l| (now - l).max(0)).unwrap_or(0);
        if downtime > 0 {
            tx.execute(
                "UPDATE jobs SET lease_until = lease_until + ?1, orphaned_at = CASE WHEN orphaned_at IS NULL THEN NULL ELSE orphaned_at + ?1 END, \
                 claimed_at = CASE WHEN claimed_at IS NULL THEN NULL ELSE claimed_at + ?1 END WHERE state IN ('claimed','running','orphaned')",
                [downtime],
            )?;
            audit(&tx, now, "scheduler", "", None, "downtime_extension", &format!("{downtime} ms"));
        }
        Self::meta_set(&tx, "last_alive", &now.to_string())?;
        tx.commit()?;
        Ok(downtime)
    }

    // ---- idempotency (replies are remembered; retried req_ids return the original) ----

    pub fn remembered(&self, who: &str, req_id: &str) -> Res<Option<Vec<u8>>> {
        Ok(self.conn.query_row("SELECT reply FROM requests WHERE who=?1 AND req_id=?2", params![who, req_id], |r| r.get(0)).optional()?)
    }

    pub fn remember(&mut self, who: &str, req_id: &str, reply: &[u8], now: i64) -> Res<()> {
        self.conn.execute("INSERT OR IGNORE INTO requests(req_id,who,t,reply) VALUES(?1,?2,?3,?4)", params![req_id, who, now, reply])?;
        Ok(())
    }

    // ---- reads ----

    pub fn get(&self, id: &str) -> Res<Option<JobRow>> {
        load(&self.conn, id)
    }

    pub fn list(&self, all: bool, state: Option<&str>, limit: u32) -> Res<Vec<JobRow>> {
        let mut sql = format!("SELECT {COLS} FROM jobs");
        let mut conds = Vec::new();
        if let Some(s) = state {
            conds.push(format!("state='{}'", s.replace('\'', "")));
        } else if !all {
            conds.push("state IN ('uploading','queued','claimed','running','orphaned')".into());
        }
        if !conds.is_empty() {
            sql.push_str(&format!(" WHERE {}", conds.join(" AND ")));
        }
        sql.push_str(" ORDER BY created_at DESC, id DESC LIMIT ?1");
        let mut st = self.conn.prepare(&sql)?;
        let rows = st.query_map([limit as i64], row_to_job)?.collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn queued(&self) -> Res<Vec<JobRow>> {
        let mut st =
            self.conn.prepare(&format!("SELECT {COLS} FROM jobs WHERE state='queued' ORDER BY priority DESC, queued_at ASC, id ASC"))?;
        Ok(st.query_map([], row_to_job)?.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn counts_by_state(&self) -> Res<Vec<(String, u64)>> {
        let mut st = self.conn.prepare("SELECT state, COUNT(*) FROM jobs GROUP BY state")?;
        Ok(st.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u64)))?.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn audit_count(&self, kind: &str) -> Res<u64> {
        Ok(self.conn.query_row("SELECT COUNT(*) FROM events WHERE kind=?1", [kind], |r| r.get::<_, i64>(0))? as u64)
    }

    pub fn audit_write(&self, now: i64, identity: &str, subject: &str, job: Option<&str>, kind: &str, detail: &str) {
        audit(&self.conn, now, identity, subject, job, kind, detail);
    }

    pub fn attempt_count(&self, job: &str) -> Res<u64> {
        Ok(self.conn.query_row("SELECT COUNT(*) FROM attempts WHERE job=?1 AND outcome IS NOT 'declined'", [job], |r| r.get::<_, i64>(0))?
            as u64)
    }

    /// Jobs whose declared input blob is `upload_id` and that are not finished (retention must keep the blob).
    pub fn upload_in_use(&self, upload_id: &str) -> Res<bool> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*) FROM jobs WHERE upload_id=?1 AND state IN ('uploading','queued','claimed','running','orphaned')",
            [upload_id],
            |r| r.get::<_, i64>(0),
        )? > 0)
    }

    // ---- submit ----

    /// Insert a new job. Snapshot inputs start in `uploading`, everything else in `queued`.
    pub fn submit(&mut self, spec: JobSpec, submitter: &str, now: i64, reuse_upload: Option<&str>) -> Res<SubmitOutcome> {
        let not_after = match &spec.not_after {
            Some(s) => Some(
                corral_proto::util::parse_rfc3339_ms(s)
                    .map_err(|e| ApiError::bad(format!("not_after: {e}")).with("field", serde_json::json!("not_after")))?,
            ),
            None => None,
        };
        let uploading = matches!(spec.input, Input::Snapshot { .. }) && reuse_upload.is_none();
        let upload_id = match reuse_upload {
            Some(u) => Some(u.to_string()),
            None => uploading.then(|| format!("s-{}", random_b32(16))),
        };
        let mut fx = Vec::new();
        let tx = self.imm()?;
        let mut id = new_job_id(now);
        while load(&tx, &id)?.is_some() {
            id = new_job_id(now);
        }
        let state = if uploading { JobState::Uploading } else { JobState::Queued };
        tx.execute(
            "INSERT INTO jobs(id,state,kind,spec,submitter,priority,class,created_at,queued_at,updated_at,not_after,retry_of,upload_id)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?8,?10,?11,?12)",
            params![
                id,
                state.as_str(),
                spec.kind,
                serde_json::to_string(&spec).expect("spec json"),
                submitter,
                spec.priority as i64,
                if spec.class == Class::Light { "light" } else { "heavy" },
                now,
                (!uploading).then_some(now),
                not_after,
                spec.retry_of,
                upload_id,
            ],
        )?;
        audit(&tx, now, submitter, "", Some(&id), "submit", state.as_str());
        let job = load(&tx, &id)?.expect("just inserted");
        fx.push(Effect::Job(JobEvent {
            v: PROTOCOL_VERSION,
            id: id.clone(),
            state: state.as_str().into(),
            prev: None,
            attempt: 0,
            worker: None,
            reason: None,
            exit_code: None,
            waiting: None,
            log_final: BTreeMap::new(),
            t: rfc3339(now),
        }));
        if !uploading {
            fx.push(Effect::Wake { job: id });
        }
        tx.commit()?;
        Ok(SubmitOutcome { job, effects: fx })
    }

    /// The snapshot upload was verified: `uploading -> queued`.
    pub fn upload_verified(&mut self, upload_id: &str, now: i64) -> Res<Vec<Effect>> {
        let tx = self.imm()?;
        let mut fx = Vec::new();
        let id: Option<String> =
            tx.query_row("SELECT id FROM jobs WHERE upload_id=?1 AND state='uploading'", [upload_id], |r| r.get(0)).optional()?;
        let Some(id) = id else {
            return Ok(fx);
        };
        let mut j = load(&tx, &id)?.expect("exists");
        transition(&tx, &mut j, Event::UploadVerified, now, &mut fx)?;
        j.queued_at = Some(now);
        save(&tx, &j)?;
        fx.push(Effect::Wake { job: id });
        tx.commit()?;
        Ok(fx)
    }

    pub fn job_for_upload(&self, upload_id: &str) -> Res<Option<JobRow>> {
        Ok(self
            .conn
            .query_row(&format!("SELECT {COLS} FROM jobs WHERE upload_id=?1 ORDER BY created_at LIMIT 1"), [upload_id], row_to_job)
            .optional()?)
    }

    // ---- claim (R-SCHED-06: one BEGIN IMMEDIATE transaction) ----

    pub fn claim(&mut self, worker: &str, req: &ClaimReq, now: i64, st: &Settings) -> Res<Option<ClaimOutcome>> {
        let tx = self.imm()?;
        let queue_paused = Self::meta_get(&tx, "queue_paused")?.is_some_and(|v| v == "1");
        let (flag_pause, flag_drain) = tx
            .query_row("SELECT paused, draining FROM worker_flags WHERE worker=?1", [worker], |r| {
                Ok((r.get::<_, Option<String>>(0)?, r.get::<_, i64>(1)? != 0))
            })
            .optional()?
            .unwrap_or((None, false));
        let mut view = WorkerView::from(req);
        if flag_pause.is_some() {
            view.paused = flag_pause.as_deref();
        }
        view.draining |= flag_drain;
        let mut q = tx.prepare(&format!("SELECT {COLS} FROM jobs WHERE state='queued' ORDER BY priority DESC, queued_at ASC, id ASC"))?;
        let candidates: Vec<JobRow> = q.query_map([], row_to_job)?.collect::<Result<_, _>>()?;
        drop(q);
        let Some(mut job) = candidates.into_iter().find(|j| eligibility(j, worker, &view, now, queue_paused).is_ok()) else {
            return Ok(None);
        };
        let mut fx = Vec::new();
        let token = new_token();
        transition(&tx, &mut job, Event::Claim, now, &mut fx)?;
        job.attempts += 1;
        job.worker = Some(worker.to_string());
        job.token = Some(token.clone());
        job.claimed_at = Some(now);
        job.lease_until = Some(now + st.lease_ttl_ms);
        job.orphaned_at = None;
        tx.execute(
            "INSERT INTO attempts(job,n,worker,token,started_at) VALUES(?1,?2,?3,?4,?5)",
            params![job.id, job.attempts as i64, worker, token, now],
        )?;
        // Re-point the event's attempt number now that it is final.
        if let Some(Effect::Job(e)) = fx.last_mut() {
            e.attempt = job.attempts;
            e.worker = job.worker.clone();
        }
        save(&tx, &job)?;
        tx.commit()?;
        Ok(Some(ClaimOutcome { job, token, effects: fx }))
    }

    fn check_token(j: &JobRow, worker: &str, token: &str) -> Result<(), ApiError> {
        let live = matches!(j.state, JobState::Claimed | JobState::Running | JobState::Orphaned);
        if live && j.worker.as_deref() == Some(worker) && j.token.as_deref() == Some(token) {
            Ok(())
        } else {
            Err(ApiError::new(ErrorCode::StaleLease, "this attempt's lease is no longer current"))
        }
    }

    // ---- heartbeat ----

    /// A valid heartbeat revives an orphaned attempt to where it was: `running` if it had started, else `claimed`
    /// (so `started`/`declined` still work and the claim deadline still applies).
    fn revive_event(j: &JobRow) -> Event {
        if j.started_at.is_some() { Event::Heartbeat } else { Event::HeartbeatUnstarted }
    }

    pub fn heartbeat(&mut self, worker: &str, id: &str, hb: &HbReq, now: i64, st: &Settings) -> Res<HbOutcome> {
        let tx = self.imm()?;
        let mut j = load(&tx, id)?.ok_or_else(|| ApiError::not_found(format!("no job {id}")))?;
        Self::check_token(&j, worker, &hb.lease_token)?;
        let mut fx = Vec::new();
        if j.state == JobState::Orphaned {
            let ev = Self::revive_event(&j);
            transition(&tx, &mut j, ev, now, &mut fx)?;
            j.orphaned_at = None;
        }
        j.lease_until = Some(now + st.lease_ttl_ms);
        if j.state == JobState::Claimed {
            j.claimed_at = Some(now); // a live worker that is still fetching its input has not abandoned the claim
        }
        save(&tx, &j)?;
        tx.commit()?;
        Ok(HbOutcome { lease_until: now + st.lease_ttl_ms, cancel: j.cancel_requested, effects: fx })
    }

    // ---- attempt events ----

    pub fn attempt_event(&mut self, worker: &str, id: &str, token: &str, ev: AttemptEvent, now: i64, st: &Settings) -> Res<Vec<Effect>> {
        let tx = self.imm()?;
        let mut j = load(&tx, id)?.ok_or_else(|| ApiError::not_found(format!("no job {id}")))?;
        // Artifact bookkeeping may arrive after `finished`; it only needs the token to be the job's last one.
        if let AttemptEvent::Artifacts { state, .. } = &ev
            && j.worker.as_deref() == Some(worker)
            && j.token.as_deref() == Some(token)
        {
            // The HTTP side records `uploaded` and `refused` itself; a later `none` (e.g. a transport error after
            // an early 413) must not erase that.
            if !(state == "none" && j.artifacts_state.is_some()) {
                j.artifacts_state = Some(state.clone());
            }
            save(&tx, &j)?;
            tx.commit()?;
            return Ok(vec![]);
        }
        Self::check_token(&j, worker, token)?;
        let mut fx = Vec::new();
        if j.state == JobState::Orphaned && !matches!(ev, AttemptEvent::Note { .. }) {
            // The worker is back and reporting with the current token: that is a valid heartbeat.
            let ev = Self::revive_event(&j);
            transition(&tx, &mut j, ev, now, &mut fx)?;
            j.orphaned_at = None;
            j.lease_until = Some(now + st.lease_ttl_ms);
        }
        match ev {
            AttemptEvent::Note { text } => {
                audit(&tx, now, &format!("worker:{worker}"), "", Some(id), "note", &text);
            }
            AttemptEvent::Artifacts { .. } => unreachable!("handled above"),
            AttemptEvent::Started { pid, pgid } => {
                transition(&tx, &mut j, Event::Started, now, &mut fx)?;
                j.started_at = Some(now);
                audit(&tx, now, &format!("worker:{worker}"), "", Some(id), "started", &format!("pid {pid} pgid {pgid}"));
                save(&tx, &j)?;
            }
            AttemptEvent::Declined { reasons } => {
                j.last_decline =
                    Some(serde_json::to_string(&serde_json::json!({ "worker": worker, "t": now, "reasons": reasons })).expect("json"));
                Self::decline_locked(&tx, &mut j, None, now, &mut fx)?;
                audit(&tx, now, &format!("worker:{worker}"), "", Some(id), "declined", &decline_text(&reasons));
                save(&tx, &j)?;
            }
            AttemptEvent::Finished(f) => {
                Self::finish_locked(&tx, &mut j, f, now, &mut fx)?;
            }
        }
        tx.commit()?;
        Ok(fx)
    }

    /// An attempt that never started goes back to the queue as a decline (R-ADM-03: not counted as an attempt), or
    /// ends `cancelled` when a cancel was requested meanwhile (a cancelled job is never requeued). `j.last_decline`
    /// is set by the caller. Works from `claimed` and (claim deadline passed while silent) from `orphaned`.
    fn decline_locked(tx: &Connection, j: &mut JobRow, reason: Option<&str>, now: i64, fx: &mut Vec<Effect>) -> Res<()> {
        let from_orphaned = j.state == JobState::Orphaned;
        j.attempts = j.attempts.saturating_sub(1);
        if j.cancel_requested {
            j.reason = Some(REASON_CANCELLED.into());
            transition(tx, j, Event::Cancel, now, fx)?;
            return Self::end_attempt(tx, j, "cancelled", Some("cancelled"), None, now);
        }
        transition(tx, j, if from_orphaned { Event::GraceOverRequeue } else { Event::Declined }, now, fx)?;
        Self::end_attempt(tx, j, "declined", reason, None, now)?;
        Self::revoke(j);
        Ok(())
    }

    fn end_attempt(tx: &Connection, j: &JobRow, outcome: &str, reason: Option<&str>, code: Option<i32>, now: i64) -> Res<()> {
        tx.execute(
            "UPDATE attempts SET ended_at=?1, outcome=?2, reason=?3, exit_code=?4 WHERE job=?5 AND token=?6",
            params![now, outcome, reason, code.map(|c| c as i64), j.id, j.token],
        )?;
        Ok(())
    }

    fn finish_locked(tx: &Connection, j: &mut JobRow, f: Finished, now: i64, fx: &mut Vec<Effect>) -> Res<()> {
        j.log_final = f.log_seq.clone();
        j.detail = f.detail.clone();
        let who = format!("worker:{}", j.worker.clone().unwrap_or_default());
        match f.outcome.as_str() {
            "exited" => {
                let code = f.exit_code.unwrap_or(-1);
                j.exit_code = Some(code);
                transition(tx, j, if code == 0 { Event::ExitedOk } else { Event::ExitedFailed }, now, fx)?;
                Self::end_attempt(tx, j, "exited", None, Some(code), now)?;
            }
            "killed" => {
                let reason = f.reason.clone().unwrap_or_else(|| KillReason::ExternalSignal.as_str().into());
                if reason == REASON_CANCELLED {
                    j.reason = Some(REASON_CANCELLED.into());
                    transition(tx, j, Event::Cancel, now, fx)?;
                } else {
                    if KillReason::parse(&reason).is_none() {
                        audit(tx, now, &who, "", Some(&j.id), "unknown_kill_reason", &reason);
                    }
                    j.reason = Some(reason.clone());
                    j.exit_code = f.exit_code;
                    transition(tx, j, Event::Killed, now, fx)?; // R-QUEUE-04: terminal, never requeued
                }
                Self::end_attempt(tx, j, "killed", j.reason.clone().as_deref(), f.exit_code, now)?;
            }
            "lost_local" => {
                let retry = j.spec.retry_on_lost && j.attempts < j.spec.max_attempts;
                if j.cancel_requested {
                    // A cancel that raced with the loss: the job must not come back.
                    j.reason = Some(REASON_CANCELLED.into());
                    Self::end_attempt(tx, j, "cancelled", Some("cancelled"), None, now)?;
                    transition(tx, j, Event::Cancel, now, fx)?;
                } else if retry {
                    j.reason = Some("lost_local".into());
                    Self::end_attempt(tx, j, "lost", Some("lost_local"), None, now)?;
                    transition(tx, j, Event::LostLocalRequeue, now, fx)?;
                    Self::revoke(j);
                    fx.push(Effect::Wake { job: j.id.clone() });
                } else {
                    j.reason = Some("lost_local".into());
                    Self::end_attempt(tx, j, "lost", Some("lost_local"), None, now)?;
                    transition(tx, j, Event::LostLocalFinal, now, fx)?;
                }
            }
            other => return Err(ApiError::bad(format!("unknown outcome {other:?}")).into()),
        }
        audit(tx, now, &who, "", Some(&j.id), "finished", &f.outcome);
        save(tx, j)?;
        Ok(())
    }

    /// Requeue bookkeeping: the old token is revoked so a returning worker is told `stale_lease` (R-QUEUE-06).
    fn revoke(j: &mut JobRow) {
        j.token = None;
        j.worker = None;
        j.lease_until = None;
        j.orphaned_at = None;
        j.claimed_at = None;
        j.started_at = None;
        j.reason = None;
    }

    // ---- cancel ----

    pub fn cancel(&mut self, id: &str, by: &str, reason: Option<&str>, now: i64) -> Res<(JobRow, Vec<Effect>)> {
        let tx = self.imm()?;
        let mut j = load(&tx, id)?.ok_or_else(|| ApiError::not_found(format!("no job {id}")))?;
        let mut fx = Vec::new();
        match j.state {
            JobState::Uploading | JobState::Queued => {
                j.reason = Some(REASON_CANCELLED.into());
                transition(&tx, &mut j, Event::Cancel, now, &mut fx)?;
            }
            JobState::Claimed | JobState::Running | JobState::Orphaned => {
                if !j.cancel_requested {
                    j.cancel_requested = true;
                    j.cancel_reason = reason.map(str::to_string);
                    if let (Some(w), Some(t)) = (j.worker.clone(), j.token.clone()) {
                        fx.push(Effect::Ctl {
                            worker: w,
                            ctl: Ctl {
                                v: PROTOCOL_VERSION,
                                action: "cancel".into(),
                                job: Some(j.id.clone()),
                                lease_token: Some(t),
                                by: Some(by.into()),
                                reason: reason.map(str::to_string),
                            },
                        });
                    }
                }
            }
            _ => return Err(ApiError::new(ErrorCode::Conflict, format!("job is already {}", j.state.as_str())).into()),
        }
        audit(&tx, now, by, "", Some(id), "cancel", reason.unwrap_or(""));
        j.updated_at = now;
        save(&tx, &j)?;
        tx.commit()?;
        Ok((j, fx))
    }

    // ---- tick: lease expiry, grace, expiry, claim deadline, upload expiry ----

    pub fn tick(&mut self, now: i64, st: &Settings) -> Res<Vec<Effect>> {
        let tx = self.imm()?;
        let mut fx = Vec::new();
        let mut st_q =
            tx.prepare(&format!("SELECT {COLS} FROM jobs WHERE state IN ('uploading','queued','claimed','running','orphaned')"))?;
        let jobs: Vec<JobRow> = st_q.query_map([], row_to_job)?.collect::<Result<_, _>>()?;
        drop(st_q);
        for mut j in jobs {
            match j.state {
                JobState::Uploading if now - j.created_at >= st.upload_ttl_ms => {
                    j.reason = Some("input".into());
                    j.detail = Some("snapshot upload did not complete within 1 h".into());
                    transition(&tx, &mut j, Event::UploadFailed, now, &mut fx)?;
                    save(&tx, &j)?;
                }
                JobState::Queued if j.not_after.is_some_and(|t| now >= t) => {
                    j.reason = Some("not_after".into());
                    transition(&tx, &mut j, Event::NotAfterPassed, now, &mut fx)?;
                    save(&tx, &j)?;
                }
                JobState::Claimed => {
                    if j.claimed_at.is_some_and(|c| now - c >= st.claim_deadline_ms) {
                        Self::claim_timeout(&tx, &mut j, now, &mut fx)?;
                    } else if j.lease_until.is_some_and(|l| now > l) {
                        Self::expire_lease(&tx, &mut j, now, &mut fx)?;
                    }
                }
                JobState::Running if j.lease_until.is_some_and(|l| now > l) => {
                    Self::expire_lease(&tx, &mut j, now, &mut fx)?;
                }
                JobState::Orphaned => {
                    let since = j.orphaned_at.unwrap_or(now);
                    let never_started_past_deadline =
                        j.started_at.is_none() && j.claimed_at.is_some_and(|c| now - c >= st.claim_deadline_ms);
                    let silent_deadline = j.started_at.map(|s| s + (j.spec.timeout_s as i64) * 1000 + st.orphan_grace_ms);
                    if never_started_past_deadline {
                        // The lease (90 s) runs out before the claim deadline (120 s), so a silent claim is seen
                        // here: it is still a claim that was never acted on, not a lost attempt.
                        Self::claim_timeout(&tx, &mut j, now, &mut fx)?;
                    } else if silent_deadline.is_some_and(|d| now >= d) {
                        // R-EXEC-02: the worker is silent past timeout + orphan_grace.
                        j.reason = Some(KillReason::Timeout.as_str().into());
                        transition(&tx, &mut j, Event::Killed, now, &mut fx)?;
                        Self::end_attempt(&tx, &j, "killed", Some("timeout"), None, now)?;
                        save(&tx, &j)?;
                    } else if now - since >= st.orphan_grace_ms {
                        if j.cancel_requested {
                            j.reason = Some(REASON_CANCELLED.into());
                            transition(&tx, &mut j, Event::Cancel, now, &mut fx)?;
                            Self::end_attempt(&tx, &j, "cancelled", Some("cancelled"), None, now)?;
                        } else if j.spec.retry_on_lost && j.attempts < j.spec.max_attempts {
                            transition(&tx, &mut j, Event::GraceOverRequeue, now, &mut fx)?;
                            Self::end_attempt(&tx, &j, "lost", Some("orphan_grace"), None, now)?;
                            Self::revoke(&mut j);
                            fx.push(Effect::Wake { job: j.id.clone() });
                        } else {
                            j.reason = Some("orphan_grace".into());
                            transition(&tx, &mut j, Event::GraceOverLost, now, &mut fx)?;
                            Self::end_attempt(&tx, &j, "lost", Some("orphan_grace"), None, now)?;
                        }
                        save(&tx, &j)?;
                    }
                }
                _ => {}
            }
        }
        Self::meta_set(&tx, "last_alive", &now.to_string())?;
        // Drop remembered replies older than 10 minutes.
        tx.execute("DELETE FROM requests WHERE t < ?1", [now - 600_000])?;
        tx.commit()?;
        Ok(fx)
    }

    /// The worker never started a claim within `claim_deadline`: treated as a decline with reason `claim_timeout`
    /// (not an attempt), or as `cancelled` when a cancel was requested.
    fn claim_timeout(tx: &Connection, j: &mut JobRow, now: i64, fx: &mut Vec<Effect>) -> Res<()> {
        j.last_decline = Some(
            serde_json::to_string(&serde_json::json!({
                "worker": j.worker, "t": now, "reasons": [{"check": "claim", "reason": "claim_timeout"}]
            }))
            .expect("json"),
        );
        let cancelled = j.cancel_requested;
        Self::decline_locked(tx, j, Some("claim_timeout"), now, fx)?;
        save(tx, j)?;
        if !cancelled {
            fx.push(Effect::Wake { job: j.id.clone() });
        }
        Ok(())
    }

    fn expire_lease(tx: &Connection, j: &mut JobRow, now: i64, fx: &mut Vec<Effect>) -> Res<()> {
        transition(tx, j, Event::LeaseExpired, now, fx)?;
        j.orphaned_at = Some(now);
        save(tx, j)?;
        Ok(())
    }

    // ---- resubmit ----

    pub fn resubmit_source(&self, id: &str) -> Res<JobRow> {
        load(&self.conn, id)?.ok_or_else(|| ApiError::not_found(format!("no job {id}")).into())
    }

    /// Is `token` the current token of a claimed, running or orphaned attempt?
    pub fn live_token(&self, token: &str) -> bool {
        self.conn
            .query_row("SELECT COUNT(*) FROM jobs WHERE token=?1 AND state IN ('claimed','running','orphaned')", [token], |r| {
                r.get::<_, i64>(0)
            })
            .map(|n| n > 0)
            .unwrap_or(false)
    }

    pub fn set_artifacts_state(&mut self, id: &str, state: &str) -> Res<()> {
        self.conn.execute("UPDATE jobs SET artifacts_state=?2 WHERE id=?1", params![id, state])?;
        Ok(())
    }

    /// Can the database take a write right now? (`/healthz`)
    pub fn writable(&mut self, now: i64) -> bool {
        Self::meta_set(&self.conn, "health_probe", &now.to_string()).is_ok()
    }

    // ---- progress ----

    /// Only the worker that holds the job's live attempt may set its progress.
    pub fn set_progress(&mut self, id: &str, worker: &str, pct: u8) -> Res<()> {
        self.conn.execute(
            "UPDATE jobs SET progress=?3 WHERE id=?1 AND worker=?2 AND state IN ('claimed','running','orphaned')",
            params![id, worker, pct as i64],
        )?;
        Ok(())
    }

    /// Drop audit rows older than `before` (the `events` table otherwise grows for ever).
    pub fn prune_audit(&mut self, before: i64) -> Res<usize> {
        Ok(self.conn.execute("DELETE FROM events WHERE t < ?1", [before])?)
    }

    // ---- log sink (R-IO-03, U-25) ----

    pub fn sink_decide(&mut self, id: &str, attempt: u32, stream: &str, seq: u64, worker: &str) -> Res<SinkDecision> {
        let j = match load(&self.conn, id)? {
            Some(j) => j,
            None => return Ok(SinkDecision::Drop),
        };
        // Only the worker that holds the attempt may append, and only to the attempt's own files: a revoked attempt
        // (requeued, or another worker's) is discarded. The worker comes from the log subject, which a worker can
        // only publish under its own name (R-SEC-02); no secret travels in the log headers.
        if j.worker.as_deref() != Some(worker) || j.attempts != attempt {
            return Ok(SinkDecision::Drop);
        }
        let (last, bytes): (u64, u64) = self
            .conn
            .query_row(
                "SELECT seq, bytes FROM log_cursors WHERE job=?1 AND attempt=?2 AND stream=?3",
                params![id, attempt as i64, stream],
                |r| Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64)),
            )
            .optional()?
            .unwrap_or((0, 0));
        Ok(if seq == last + 1 {
            SinkDecision::Write { offset: bytes }
        } else if seq <= last {
            SinkDecision::Duplicate
        } else {
            SinkDecision::Hold { last, offset: bytes }
        })
    }

    pub fn sink_commit(&mut self, id: &str, attempt: u32, stream: &str, seq: u64, new_bytes: u64) -> Res<()> {
        self.conn.execute(
            "INSERT INTO log_cursors(job,attempt,stream,seq,bytes) VALUES(?1,?2,?3,?4,?5)
             ON CONFLICT(job,attempt,stream) DO UPDATE SET seq=excluded.seq, bytes=excluded.bytes",
            params![id, attempt as i64, stream, seq as i64, new_bytes as i64],
        )?;
        Ok(())
    }

    // ---- retention (R-IO-05) ----

    /// Finished jobs eligible for deletion, oldest first: `(id, finished_at, keep_flagged)`.
    pub fn finished_jobs(&self) -> Res<Vec<FinishedRow>> {
        let mut st = self.conn.prepare("SELECT id, finished_at, spec, upload_id FROM jobs WHERE state IN ('succeeded','failed','killed','cancelled','lost','expired') ORDER BY finished_at ASC")?;
        let rows = st
            .query_map([], |r| {
                let spec: String = r.get(2)?;
                let keep =
                    serde_json::from_str::<JobSpec>(&spec).map(|s| s.labels.get("keep").is_some_and(|v| v == "true")).unwrap_or(false);
                Ok((r.get::<_, String>(0)?, r.get::<_, Option<i64>>(1)?.unwrap_or(0), keep, r.get::<_, Option<String>>(3)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn delete_job(&mut self, id: &str) -> Res<()> {
        let tx = self.imm()?;
        for sql in ["DELETE FROM jobs WHERE id=?1", "DELETE FROM attempts WHERE job=?1", "DELETE FROM log_cursors WHERE job=?1"] {
            tx.execute(sql, [id])?;
        }
        tx.commit()?;
        Ok(())
    }

    // ---- waiting explanation ----

    /// Per-worker reasons a queued job waits (R-ADM-04). `workers` is `(host, presence, last_seen_ms)`.
    pub fn explain(&self, job: &JobRow, workers: &[(String, Option<Presence>, i64)], now: i64, st: &Settings) -> Res<Vec<Waiting>> {
        let mut out = Vec::new();
        let queue_paused = self.queue_paused()?;
        if job.state != JobState::Queued {
            return Ok(out);
        }
        if queue_paused {
            out.push(Waiting { worker: "*".into(), kind: "paused".into(), text: "the queue is paused".into() });
        }
        let known: Vec<&str> = workers.iter().map(|w| w.0.as_str()).collect();
        if known.is_empty() {
            out.push(Waiting { worker: "*".into(), kind: "offline".into(), text: "no workers have connected yet".into() });
        }
        for (host, presence, seen) in workers {
            let online = now - seen < st.presence_ttl_ms;
            let Some(p) = presence else {
                out.push(Waiting { worker: host.clone(), kind: "offline".into(), text: format!("offline since {}", rfc3339(*seen)) });
                continue;
            };
            if !online {
                out.push(Waiting { worker: host.clone(), kind: "offline".into(), text: format!("offline since {}", rfc3339(*seen)) });
                continue;
            }
            let mut view = WorkerView::from(p);
            let (fp, fd) = self.worker_flags(host)?;
            if fp.is_some() {
                view.paused = fp.as_deref();
            }
            view.draining |= fd;
            match eligibility(job, host, &view, now, false) {
                Ok(()) => out.push(Waiting { worker: host.clone(), kind: "blocked".into(), text: "can run it now; a claim is due".into() }),
                Err(e) if e.is_permanent() => out.push(Waiting { worker: host.clone(), kind: "never".into(), text: format!("never: {e}") }),
                Err(e @ Ineligible::Prefers { .. }) => {
                    out.push(Waiting { worker: host.clone(), kind: "pinned".into(), text: e.to_string() })
                }
                Err(e @ Ineligible::WorkerPaused(_)) | Err(e @ Ineligible::Draining) => {
                    out.push(Waiting { worker: host.clone(), kind: "paused".into(), text: e.to_string() })
                }
                Err(e) => {
                    let mut text = format!("blocked: {e}");
                    for b in p.admission.blocked.iter().skip(if matches!(e, Ineligible::HeavyBlocked(_)) { 1 } else { 0 }) {
                        text.push_str(&format!("; also {}: {}", b.check, b.reason));
                    }
                    out.push(Waiting { worker: host.clone(), kind: "blocked".into(), text })
                }
            }
        }
        if let Some(d) = &job.last_decline
            && let Ok(v) = serde_json::from_str::<serde_json::Value>(d)
        {
            let w = v["worker"].as_str().unwrap_or("?").to_string();
            let reasons = v["reasons"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .map(|r| format!("{}: {}", r["check"].as_str().unwrap_or("?"), r["reason"].as_str().unwrap_or("?")))
                        .collect::<Vec<_>>()
                        .join("; ")
                })
                .unwrap_or_default();
            out.push(Waiting { worker: w, kind: "blocked".into(), text: format!("declined at start: {reasons}") });
        }
        Ok(out)
    }

    pub fn view(&self, j: &JobRow, waiting: Vec<Waiting>) -> JobView {
        JobView {
            id: j.id.clone(),
            state: j.state,
            kind: j.kind.clone(),
            spec: j.spec.clone(),
            submitter: j.submitter.clone(),
            attempts: j.attempts,
            worker: j.worker.clone(),
            reason: j.reason.clone(),
            exit_code: j.exit_code,
            detail: j.detail.clone(),
            created_at: rfc3339(j.created_at),
            queued_at: j.queued_at.map(rfc3339),
            started_at: j.started_at.map(rfc3339),
            finished_at: j.finished_at.map(rfc3339),
            waiting,
            cancel_requested: j.cancel_requested,
            artifacts_state: j.artifacts_state.clone(),
            log_final: j.log_final.clone(),
            retry_of: j.retry_of.clone(),
            progress: j.progress,
        }
    }
}

pub fn decline_text(reasons: &[DeclineReason]) -> String {
    reasons.iter().map(|r| format!("{}: {}", r.check, r.reason)).collect::<Vec<_>>().join("; ")
}
