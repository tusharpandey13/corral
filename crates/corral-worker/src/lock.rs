//! Host lock, lease-file kind (GUARDS.md 5.2). The legacy JSON format, `O_EXCL` acquire, tmp + rename renew,
//! holder-only release, takeover through an `O_EXCL` `.takeover` file.
//!
//! The full conformance suite against the reference implementation is part of milestone M3; M1 needs the lock for
//! the "re-check under the lock" rule (R-ADM-01).

use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// A lock file that does not parse is treated as live for this long after its last write (a holder that has just
/// created it may not have written the content yet), and as stale after that.
const UNPARSEABLE_GRACE: std::time::Duration = std::time::Duration::from_secs(10);

pub trait WallClock: Send + Sync {
    fn now_s(&self) -> f64;
}

pub struct SystemWall;

impl WallClock for SystemWall {
    fn now_s(&self) -> f64 {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs_f64()).unwrap_or(0.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LockFile {
    pub agent: String,
    pub purpose: String,
    #[serde(default)]
    pub apps: Vec<String>,
    pub pid: u32,
    pub started: f64,
    pub heartbeat: f64,
    pub ttl_s: f64,
    /// corral's own additions (GUARDS.md 5.2); legacy readers ignore the key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub corral: Option<CorralExtra>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct CorralExtra {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<u32>,
    /// SHA-256 (hex) of the holder's `CORRAL_LOCK_TOKEN`: a child that presents the token re-enters (R-LOCK-04).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_sha: Option<String>,
}

/// Which stale locks may be taken over (GUARDS.md 5.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Takeover {
    /// Stale locks held by a corral agent (the default).
    #[default]
    Corral,
    /// Any stale lock.
    Any,
    /// No stale lock: the owner clears it.
    Never,
}

/// What `status` found.
#[derive(Debug, Clone, PartialEq)]
pub enum LockStatus {
    Free,
    Held { file: LockFile, stale: bool },
    /// The file exists and does not parse.
    Unreadable,
}

impl LockFile {
    pub fn stale(&self, now: f64) -> bool {
        now - self.heartbeat > self.ttl_s
    }

    pub fn describe(&self, now: f64) -> String {
        format!(
            "held by {} ({}, apps {:?}, pid {}), heartbeat {:.0}s ago of ttl {:.0}s",
            self.agent,
            self.purpose,
            self.apps,
            self.pid,
            (now - self.heartbeat).max(0.0),
            self.ttl_s
        )
    }
}

/// Why a lock could not be taken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Busy(pub String);

pub trait LockStore: Send + Sync {
    /// Non-blocking acquire. `Err` carries the holder description.
    fn acquire(&self, agent: &str, purpose: &str, pid: u32, ttl_s: u64) -> Result<(), Busy>;
    fn renew(&self, agent: &str) -> Result<(), String>;
    /// Holder-only, idempotent.
    fn release(&self, agent: &str);
    /// Who holds it now (for presence), if live.
    fn holder(&self) -> Option<String>;
}

pub struct FileLock {
    path: PathBuf,
    clock: Arc<dyn WallClock>,
    takeover: Takeover,
}

impl FileLock {
    pub fn new(path: impl Into<PathBuf>, clock: Arc<dyn WallClock>) -> FileLock {
        FileLock { path: path.into(), clock, takeover: Takeover::Corral }
    }

    pub fn takeover(mut self, t: Takeover) -> FileLock {
        self.takeover = t;
        self
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    pub fn status(&self) -> LockStatus {
        match self.read() {
            None => LockStatus::Free,
            Some(None) => LockStatus::Unreadable,
            Some(Some(file)) => {
                let stale = file.stale(self.clock.now_s());
                LockStatus::Held { file, stale }
            }
        }
    }

    /// Does a live lock carry this token hash? (R-LOCK-04: the holder's child re-enters instead of acquiring.)
    pub fn holds_token(&self, token_sha: &str) -> bool {
        let now = self.clock.now_s();
        matches!(self.read(), Some(Some(f)) if !f.stale(now) && f.corral.as_ref().and_then(|c| c.token_sha.as_deref()) == Some(token_sha))
    }

    fn read(&self) -> Option<Option<LockFile>> {
        match std::fs::read_to_string(&self.path) {
            Ok(t) => Some(serde_json::from_str(&t).ok()), // a file that does not parse is stale
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(_) => Some(None),
        }
    }

    fn create_exclusive(&self, content: &LockFile) -> std::io::Result<()> {
        if let Some(d) = self.path.parent() {
            std::fs::create_dir_all(d)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(d, std::fs::Permissions::from_mode(0o700));
            }
        }
        let mut f = OpenOptions::new().write(true).create_new(true).mode(0o600).open(&self.path)?;
        f.write_all(serde_json::to_string(content).expect("json").as_bytes())?;
        f.sync_all()
    }

    /// Was the lock file modified within the grace period? (A file we cannot stat counts as fresh: not stale.)
    fn younger_than_grace(&self) -> bool {
        std::fs::metadata(&self.path)
            .and_then(|m| m.modified())
            .map(|t| t.elapsed().map(|age| age < UNPARSEABLE_GRACE).unwrap_or(true))
            .unwrap_or(true)
    }

    fn takeover_path(&self) -> PathBuf {
        let mut p = self.path.clone().into_os_string();
        p.push(".takeover");
        PathBuf::from(p)
    }

    fn log_path(&self) -> PathBuf {
        let mut p = self.path.clone().into_os_string();
        p.push(".log");
        PathBuf::from(p)
    }
}

impl FileLock {
    /// Non-blocking acquire of a full lock record (apps, corral extras). `acquire` is this with the common fields.
    pub fn acquire_with(&self, mine: LockFile) -> Result<(), Busy> {
        let now = self.clock.now_s();
        let agent = mine.agent.as_str();
        for _ in 0..3 {
            match self.create_exclusive(&mine) {
                Ok(()) => return Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(Busy(format!("lock file error: {e}"))),
            }
            let current = match self.read() {
                None => continue, // vanished between the create and the read: retry
                Some(c) => c,
            };
            if let Some(h) = &current
                && !h.stale(now)
            {
                return Err(Busy(h.describe(now))); // never take over a live lock (R-LOCK-02)
            }
            if current.is_none() && self.younger_than_grace() {
                // A holder may be between `O_EXCL` create and the write of its content: that is a live lock.
                return Err(Busy(format!("lock file is unreadable but younger than {} s (being written?)", UNPARSEABLE_GRACE.as_secs())));
            }
            let holder = current.as_ref().map(|h| h.agent.clone()).unwrap_or_else(|| "unparseable lock file".into());
            let corral_holder = current.as_ref().is_some_and(|h| h.agent.starts_with("corral"));
            if self.takeover == Takeover::Never && current.is_some() {
                return Err(Busy(format!("stale lock held by {holder}; takeover is disabled for this lock")));
            }
            if !corral_holder && self.takeover != Takeover::Any && current.is_some() {
                return Err(Busy(format!("stale lock held by {holder}; not taking over a non-corral holder")));
            }
            // Takeover: an O_EXCL `.takeover` file serialises racers; re-read; delete; log one line.
            let tk = self.takeover_path();
            if let Ok(meta) = std::fs::metadata(&tk)
                && meta.modified().ok().and_then(|m| m.elapsed().ok()).is_some_and(|age| age.as_secs() > 60)
            {
                let _ = std::fs::remove_file(&tk); // the racer died
            }
            if OpenOptions::new().write(true).create_new(true).mode(0o600).open(&tk).is_err() {
                continue;
            }
            let again = self.read();
            let same_and_stale = match (&again, &current) {
                (Some(a), Some(c)) => a.as_ref().is_some_and(|a| a == c && a.stale(now)),
                (Some(None), None) => true,
                _ => false,
            };
            if same_and_stale {
                let _ = std::fs::remove_file(&self.path);
                if let Ok(mut f) = OpenOptions::new().append(true).create(true).mode(0o600).open(self.log_path()) {
                    let _ = writeln!(f, "{}", serde_json::json!({ "t": now, "event": "takeover", "by": agent, "from": holder }));
                }
            }
            let _ = std::fs::remove_file(&tk);
        }
        Err(Busy("lock is contended; try again".into()))
    }
}

impl LockStore for FileLock {
    fn acquire(&self, agent: &str, purpose: &str, pid: u32, ttl_s: u64) -> Result<(), Busy> {
        let now = self.clock.now_s();
        self.acquire_with(LockFile {
            agent: agent.into(),
            purpose: purpose.into(),
            apps: vec![],
            pid,
            started: now,
            heartbeat: now,
            ttl_s: ttl_s as f64,
            corral: None,
        })
    }

    fn renew(&self, agent: &str) -> Result<(), String> {
        let Some(Some(mut cur)) = self.read() else { return Err("lock is gone".into()) };
        if cur.agent != agent {
            return Err(format!("lock is now held by {}", cur.agent));
        }
        cur.heartbeat = self.clock.now_s();
        let tmp = self.path.with_extension("tmp");
        std::fs::write(&tmp, serde_json::to_string(&cur).expect("json")).map_err(|e| e.to_string())?;
        std::fs::rename(&tmp, &self.path).map_err(|e| e.to_string())
    }

    fn release(&self, agent: &str) {
        if let Some(Some(cur)) = self.read()
            && cur.agent == agent
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn holder(&self) -> Option<String> {
        let now = self.clock.now_s();
        self.read().and_then(|c| c).filter(|c| !c.stale(now)).map(|c| c.describe(now))
    }
}

/// In-memory lock for tests: `O_EXCL` semantics without a file system.
#[derive(Default)]
pub struct FakeLock {
    held: Mutex<Option<(String, String)>>,
    pub log: Mutex<Vec<String>>,
}

impl FakeLock {
    /// Another party takes the lock.
    pub fn hold_foreign(&self, agent: &str) {
        *self.held.lock().unwrap() = Some((agent.into(), "test".into()));
    }
    pub fn is_free(&self) -> bool {
        self.held.lock().unwrap().is_none()
    }
}

impl LockStore for FakeLock {
    fn acquire(&self, agent: &str, purpose: &str, _pid: u32, _ttl: u64) -> Result<(), Busy> {
        let mut h = self.held.lock().unwrap();
        if let Some((a, p)) = &*h {
            return Err(Busy(format!("held by {a} ({p})")));
        }
        *h = Some((agent.into(), purpose.into()));
        self.log.lock().unwrap().push(format!("acquire {agent}"));
        Ok(())
    }
    fn renew(&self, agent: &str) -> Result<(), String> {
        match &*self.held.lock().unwrap() {
            Some((a, _)) if a == agent => Ok(()),
            _ => Err("not the holder".into()),
        }
    }
    fn release(&self, agent: &str) {
        let mut h = self.held.lock().unwrap();
        if h.as_ref().is_some_and(|(a, _)| a == agent) {
            *h = None;
            self.log.lock().unwrap().push(format!("release {agent}"));
        }
    }
    fn holder(&self) -> Option<String> {
        self.held.lock().unwrap().as_ref().map(|(a, p)| format!("held by {a} ({p})"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Wall(AtomicU64);
    impl WallClock for Wall {
        fn now_s(&self) -> f64 {
            self.0.load(Ordering::SeqCst) as f64
        }
    }

    fn tmp() -> PathBuf {
        std::env::temp_dir().join(format!("corral-lock-{}/x.lock", corral_proto::util::random_b32(8)))
    }

    /// R-LOCK-01 (subset; the full conformance suite is milestone M3).
    #[test]
    fn exclusive_acquire_holder_only_release_and_renew() {
        let wall = Arc::new(Wall(AtomicU64::new(1000)));
        let l = FileLock::new(tmp(), wall.clone());
        l.acquire("corral:j-1", "job", 1, 300).unwrap();
        let busy = l.acquire("corral:j-2", "job", 2, 300).unwrap_err();
        assert!(busy.0.contains("corral:j-1"), "{busy:?}");
        l.release("corral:j-2"); // not the holder: no effect
        assert!(l.acquire("corral:j-2", "job", 2, 300).is_err());
        wall.0.store(1100, Ordering::SeqCst);
        l.renew("corral:j-1").unwrap();
        assert!(l.renew("corral:j-2").is_err());
        l.release("corral:j-1");
        l.acquire("corral:j-2", "job", 2, 300).unwrap();
    }

    /// R-LOCK-02: stale corral holders are taken over (one log line), non-corral holders are not.
    #[test]
    fn stale_takeover_rules() {
        let wall = Arc::new(Wall(AtomicU64::new(1000)));
        let path = tmp();
        let l = FileLock::new(path.clone(), wall.clone());
        l.acquire("corral:j-1", "job", 1, 300).unwrap();
        wall.0.store(1000 + 301, Ordering::SeqCst);
        l.acquire("corral:j-2", "job", 2, 300).unwrap();
        let log = std::fs::read_to_string(l.log_path()).unwrap();
        assert_eq!(log.lines().count(), 1);
        assert!(log.contains("takeover") && log.contains("corral:j-1"));
        l.release("corral:j-2");
        std::fs::write(
            &path,
            serde_json::to_string(&LockFile {
                agent: "legacy-tool".into(),
                purpose: "x".into(),
                apps: vec![],
                pid: 5,
                started: 0.0,
                heartbeat: 0.0,
                ttl_s: 300.0,
                corral: None,
            })
            .unwrap(),
        )
        .unwrap();
        let e = l.acquire("corral:j-3", "job", 3, 300).unwrap_err();
        assert!(e.0.contains("not taking over a non-corral holder"), "{e:?}");
    }

    /// R-LOCK-02 (M1 review): a freshly created, still empty lock file is a live holder, not a stale one, so two
    /// acquirers can never both win.
    #[test]
    fn fresh_unparseable_file_is_a_live_lock() {
        let wall = Arc::new(Wall(AtomicU64::new(1000)));
        let path = tmp();
        let l = FileLock::new(path.clone(), wall);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "").unwrap(); // O_EXCL create done, content not yet written
        let busy = l.acquire("corral:j-1", "job", 1, 300).unwrap_err();
        assert!(busy.0.contains("younger"), "{busy:?}");
        assert!(path.exists(), "the file is not removed");
        assert!(!l.log_path().exists(), "no takeover happened");
    }

    #[test]
    fn corrupt_file_is_stale() {
        let wall = Arc::new(Wall(AtomicU64::new(1000)));
        let path = tmp();
        let l = FileLock::new(path.clone(), wall);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "not json").unwrap();
        // R-LOCK-01: a file that does not parse is stale (once it is older than the write grace), so it can be taken
        // over (and the takeover is logged)
        let old = std::time::SystemTime::now() - std::time::Duration::from_secs(60);
        std::fs::File::options().write(true).open(&path).unwrap().set_modified(old).unwrap();
        l.acquire("corral:j-1", "job", 1, 300).unwrap();
        assert!(std::fs::read_to_string(l.log_path()).unwrap().contains("takeover"));
    }
}
