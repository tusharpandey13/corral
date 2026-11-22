//! `corral lock` helpers (GUARDS.md 5.6): resolve a configured lock, acquire / re-enter, and describe it. The lock
//! is the legacy lease file, so these work with no guard running and interoperate with the legacy tool.

use corral_proto::config::Config;
use corral_proto::util::{hex, random_b32};
use corral_worker::lock::{Busy, CorralExtra, FileLock, LockFile, LockStatus, LockStore, SystemWall, Takeover, WallClock};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::Arc;

pub const TOKEN_ENV: &str = "CORRAL_LOCK_TOKEN";

pub struct Resolved {
    pub lock: FileLock,
    pub ttl_s: u64,
    pub path: PathBuf,
}

/// A configured lock by name (`[guard.locks.NAME]`) or a bare path.
pub fn resolve(cfg: &Config, name: Option<&str>, path: Option<&PathBuf>, ttl_s: Option<u64>) -> Result<Resolved, String> {
    resolve_with(cfg, name, path, ttl_s, Arc::new(SystemWall))
}

pub fn resolve_with(
    cfg: &Config,
    name: Option<&str>,
    path: Option<&PathBuf>,
    ttl_s: Option<u64>,
    clock: Arc<dyn WallClock>,
) -> Result<Resolved, String> {
    if let Some(p) = path {
        return Ok(Resolved { lock: FileLock::new(p.clone(), clock), ttl_s: ttl_s.unwrap_or(300), path: p.clone() });
    }
    let name = name.ok_or("name a lock (a [guard.locks.NAME] entry) or pass --path FILE")?;
    let s = crate::settings::GuardSettings::from_config(cfg)?;
    let l = s
        .locks
        .get(name)
        .ok_or_else(|| format!("no lock named {name:?} in [guard.locks] (configured: {:?})", s.locks.keys().collect::<Vec<_>>()))?;
    Ok(Resolved { lock: FileLock::new(l.path.clone(), clock).takeover(l.takeover), ttl_s: ttl_s.unwrap_or(l.ttl_s), path: l.path.clone() })
}

pub fn token_sha(token: &str) -> String {
    hex(&Sha256::digest(token.as_bytes()))
}

pub fn new_token() -> String {
    random_b32(24)
}

#[derive(Debug, PartialEq, Eq)]
pub enum Acquired {
    /// This call took the lock; the caller must renew and release it.
    Taken,
    /// The caller's environment carries the holder's token: nothing was acquired and nothing must be released.
    Reentered,
}

pub struct Want<'a> {
    pub agent: &'a str,
    pub purpose: &'a str,
    pub apps: Vec<String>,
    pub pid: u32,
    pub ttl_s: u64,
    /// The token to record (hash only) when this call takes the lock.
    pub new_token: Option<&'a str>,
    /// The token the caller inherited (`CORRAL_LOCK_TOKEN`), if any.
    pub inherited: Option<&'a str>,
}

pub fn acquire(r: &Resolved, w: &Want<'_>) -> Result<Acquired, Busy> {
    if let Some(t) = w.inherited
        && r.lock.holds_token(&token_sha(t))
    {
        return Ok(Acquired::Reentered); // R-LOCK-04
    }
    let now = SystemWall.now_s();
    r.lock
        .acquire_with(LockFile {
            agent: w.agent.into(),
            purpose: w.purpose.into(),
            apps: w.apps.clone(),
            pid: w.pid,
            started: now,
            heartbeat: now,
            ttl_s: w.ttl_s as f64,
            corral: w.new_token.map(|t| CorralExtra { token_sha: Some(token_sha(t)), ..CorralExtra::default() }),
        })
        .map(|()| Acquired::Taken)
}

/// Holder-only (the agent string must match).
pub fn release(r: &Resolved, agent: &str) {
    r.lock.release(agent);
}

pub fn renew(r: &Resolved, agent: &str) -> Result<(), String> {
    r.lock.renew(agent)
}

/// One line for `corral lock status`.
pub fn describe(r: &Resolved) -> (bool, String) {
    match r.lock.status() {
        LockStatus::Free => (false, "free".into()),
        LockStatus::Unreadable => (true, "unreadable lock file (stale once it is older than 10 s)".into()),
        LockStatus::Held { file, stale } => {
            let d = file.describe(SystemWall.now_s());
            (!stale, if stale { format!("stale: {d}") } else { d })
        }
    }
}

pub fn takeover_name(t: Takeover) -> &'static str {
    match t {
        Takeover::Corral => "corral",
        Takeover::Any => "any",
        Takeover::Never => "never",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> PathBuf {
        std::env::temp_dir().join(format!("cg-lock-{}/x.lock", random_b32(6)))
    }

    fn want<'a>(agent: &'a str, new_token: Option<&'a str>, inherited: Option<&'a str>) -> Want<'a> {
        Want { agent, purpose: "test", apps: vec!["emulator".into()], pid: 7, ttl_s: 300, new_token, inherited }
    }

    /// R-LOCK-04: a child that presents the holder's token re-enters; a stranger or a wrong token is busy.
    #[test]
    fn token_reentry_and_busy() {
        let cfg = Config::parse("version = 1\n").unwrap();
        let r = resolve(&cfg, None, Some(&tmp()), None).unwrap();
        let tok = new_token();
        assert_eq!(acquire(&r, &want("corral:holder", Some(&tok), None)).unwrap(), Acquired::Taken);
        let busy = acquire(&r, &want("corral:other", None, None)).unwrap_err();
        assert!(busy.0.contains("corral:holder"), "{busy:?}");
        assert!(acquire(&r, &want("corral:other", None, Some("wrong"))).is_err());
        assert_eq!(acquire(&r, &want("corral:child", None, Some(&tok))).unwrap(), Acquired::Reentered);
        // the file carries only the hash, and the legacy keys
        let text = std::fs::read_to_string(&r.path).unwrap();
        assert!(!text.contains(&tok) && text.contains(&token_sha(&tok)) && text.contains("\"apps\":[\"emulator\"]"), "{text}");
        let (live, line) = describe(&r);
        assert!(live && line.contains("corral:holder"), "{line}");
        release(&r, "corral:holder");
        assert_eq!(describe(&r), (false, "free".into()));
    }

    #[test]
    fn named_locks_come_from_the_config() {
        let cfg = Config::parse(
            "version = 1\n[guard.locks.emulator]\npath = \"/var/tmp/x/emulator.lock\"\nttl = \"120s\"\ntakeover = \"never\"\n",
        )
        .unwrap();
        let r = resolve(&cfg, Some("emulator"), None, None).unwrap();
        assert_eq!((r.ttl_s, r.path.to_str().unwrap()), (120, "/var/tmp/x/emulator.lock"));
        assert!(resolve(&cfg, Some("nope"), None, None).err().unwrap().contains("emulator"));
        assert!(resolve(&cfg, None, None, None).is_err());
    }
}
