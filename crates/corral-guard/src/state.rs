//! The guard's persistent state (GUARDS.md 3.4, 6): the kill switch (`STOP`, survives restarts and reboots) and
//! the registrations (re-validated by pid start time after a restart).

use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StopState {
    pub since_ms: i64,
    pub by: String,
    pub reason: String,
}

impl StopState {
    pub fn describe(&self) -> String {
        format!("kill switch on (since {}, by {}: {})", corral_proto::util::rfc3339(self.since_ms), self.by, self.reason)
    }
}

/// A unit of work that asked to be watched (`register`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Registration {
    pub id: String,
    pub name: String,
    /// The unit's leader and its process group.
    pub pid: u32,
    pub pgid: u32,
    /// The leader's start time as `ps` prints it: identifies the pid across reuse.
    pub pid_start: String,
    pub tier: Option<u32>,
    pub budget_mb: Option<u64>,
    #[serde(default)]
    pub pausable: bool,
    #[serde(default = "yes")]
    pub heavy: bool,
    pub since_ms: i64,
}

fn yes() -> bool {
    true
}

pub struct StateDir {
    dir: PathBuf,
}

fn write_atomic(path: &Path, content: &str) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    let mut f = OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(&tmp)?;
    f.write_all(content.as_bytes())?;
    f.sync_all()?;
    std::fs::rename(&tmp, path)
}

impl StateDir {
    pub fn open(dir: impl Into<PathBuf>) -> std::io::Result<StateDir> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700));
        }
        Ok(StateDir { dir })
    }

    pub fn path(&self) -> &Path {
        &self.dir
    }

    fn stop_path(&self) -> PathBuf {
        self.dir.join("STOP")
    }

    /// The kill switch. A STOP file that exists but cannot be parsed still counts as ON (fail closed).
    pub fn stop(&self) -> Option<StopState> {
        let text = std::fs::read_to_string(self.stop_path()).ok()?;
        Some(serde_json::from_str(&text).unwrap_or(StopState { since_ms: 0, by: "unknown".into(), reason: "unreadable STOP file".into() }))
    }

    pub fn set_stop(&self, s: &StopState) -> std::io::Result<()> {
        write_atomic(&self.stop_path(), &serde_json::to_string(s).expect("json"))
    }

    pub fn clear_stop(&self) -> std::io::Result<()> {
        match std::fs::remove_file(self.stop_path()) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }

    pub fn load_regs(&self) -> Vec<Registration> {
        std::fs::read_to_string(self.dir.join("registrations.json")).ok().and_then(|t| serde_json::from_str(&t).ok()).unwrap_or_default()
    }

    pub fn save_regs(&self, regs: &[Registration]) -> std::io::Result<()> {
        write_atomic(&self.dir.join("registrations.json"), &serde_json::to_string(regs).expect("json"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub fn tmpdir(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("cg-{tag}-{}", corral_proto::util::random_b32(6)))
    }

    /// R-GUARD-05 / U-28: the kill switch is a file; a new `StateDir` on the same directory sees it.
    #[test]
    fn stop_survives_a_restart_and_unreadable_means_on() {
        let d = tmpdir("state");
        let s = StateDir::open(&d).unwrap();
        assert!(s.stop().is_none());
        s.set_stop(&StopState { since_ms: 1_000, by: "owner".into(), reason: "test".into() }).unwrap();
        let again = StateDir::open(&d).unwrap();
        assert_eq!(again.stop().unwrap().by, "owner");
        std::fs::write(d.join("STOP"), "garbage").unwrap();
        assert_eq!(StateDir::open(&d).unwrap().stop().unwrap().by, "unknown");
        again.clear_stop().unwrap();
        assert!(again.stop().is_none());
        again.clear_stop().unwrap(); // idempotent
    }

    #[test]
    fn registrations_round_trip() {
        let s = StateDir::open(tmpdir("regs")).unwrap();
        assert!(s.load_regs().is_empty());
        let r = Registration {
            id: "r1".into(),
            name: "run:x".into(),
            pid: 10,
            pgid: 10,
            pid_start: "Mon Nov 22 10:00:00 2026".into(),
            tier: None,
            budget_mb: Some(50),
            pausable: false,
            heavy: true,
            since_ms: 5,
        };
        s.save_regs(std::slice::from_ref(&r)).unwrap();
        assert_eq!(s.load_regs(), vec![r]);
    }
}
