//! Keep-awake holds (GUARDS.md 8, R-AWAKE-01..03).
//!
//! A provider keeps the host from sleeping while a job runs, through a *named, time-bounded* hold. corral never
//! touches a keep-awake session it did not create, never kills a process by name, and never calls `release --all`:
//! the only provider that ships is `hold-cli`, an external command implementing the hold contract
//! (`hold --for D --owner N [--pid P] [--display]`, `release --owner N`, `holds --json`), which owns its processes and
//! its own bookkeeping. corral signals nothing here.

use std::io::Read;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Every hold corral creates is named with this prefix; `list` shows only holds that carry it.
pub const OWNER_PREFIX: &str = "corral:";

/// The hold name of a job attempt (per attempt, so an old attempt's late release cannot end a new attempt's hold).
pub fn job_owner(job: &str, attempt: u32) -> String {
    format!("{OWNER_PREFIX}{job}:{attempt}")
}

/// The hold name of a `corral run` command.
pub fn run_owner(name: &str) -> String {
    format!("{OWNER_PREFIX}run:{name}")
}

/// `min(timeout, 2 h) + 10 min` (GUARDS.md 8): renewed every 30 min while the unit runs.
pub fn hold_window(timeout: Duration) -> Duration {
    timeout.min(Duration::from_secs(2 * 3600)) + Duration::from_secs(600)
}

pub const RENEW_EVERY: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Clone, PartialEq)]
pub struct HoldInfo {
    pub owner: String,
    /// Epoch seconds.
    pub until: Option<i64>,
    pub pid: Option<u32>,
    pub bound_pid: Option<u32>,
    pub display: bool,
}

pub trait KeepAwake: Send + Sync {
    fn name(&self) -> &str;
    /// Is the provider available and working? `Err` carries the reason.
    fn probe(&self) -> Result<(), String>;
    /// Start or renew the hold named `owner`. Repeating it renews.
    fn hold(&self, owner: &str, dur: Duration, bound_pid: Option<u32>, display: bool) -> Result<HoldInfo, String>;
    /// End the hold named `owner`. Idempotent: no hold is success.
    fn release(&self, owner: &str) -> Result<(), String>;
    /// The holds corral created (owner starts with [`OWNER_PREFIX`]); a foreign owner's holds are never listed.
    fn list(&self) -> Result<Vec<HoldInfo>, String>;
}

fn valid_owner(owner: &str) -> Result<(), String> {
    if !owner.starts_with(OWNER_PREFIX) || owner.len() == OWNER_PREFIX.len() {
        return Err(format!("hold owner {owner:?} must be named {OWNER_PREFIX}<id>"));
    }
    if owner.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err(format!("hold owner {owner:?} has whitespace or control characters"));
    }
    Ok(())
}

/// An external command implementing the hold contract. `command` is configurable (default `awake` on `PATH`).
pub struct HoldCli {
    command: PathBuf,
    timeout: Duration,
}

impl HoldCli {
    pub fn new(command: impl Into<PathBuf>) -> HoldCli {
        HoldCli { command: command.into(), timeout: Duration::from_secs(20) }
    }

    pub fn with_timeout(mut self, t: Duration) -> HoldCli {
        self.timeout = t;
        self
    }

    /// Run the provider with a deadline. A provider that hangs is abandoned after the deadline, and the child corral
    /// started is ended (it is our own child, recorded by handle: nothing is looked up by name).
    fn run(&self, args: &[String]) -> Result<(i32, String), String> {
        let mut child = Command::new(&self.command)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("cannot run keep-awake provider {}: {e}", self.command.display()))?;
        let (mut out, mut err) = (child.stdout.take(), child.stderr.take());
        let reader = |mut r: Box<dyn Read + Send>| {
            std::thread::spawn(move || {
                let mut s = String::new();
                let _ = r.read_to_string(&mut s);
                s
            })
        };
        let (to, te) = (out.take().map(|o| reader(Box::new(o))), err.take().map(|e| reader(Box::new(e))));
        let deadline = Instant::now() + self.timeout;
        let status = loop {
            match child.try_wait() {
                Ok(Some(s)) => break s,
                Ok(None) if Instant::now() >= deadline => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(format!("keep-awake provider timed out after {} s", self.timeout.as_secs()));
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(20)),
                Err(e) => return Err(format!("keep-awake provider: {e}")),
            }
        };
        let stdout = to.map(|h| h.join().unwrap_or_default()).unwrap_or_default();
        let stderr = te.map(|h| h.join().unwrap_or_default()).unwrap_or_default();
        let text = if status.success() {
            stdout
        } else {
            format!("{}{}", stderr.trim(), if stderr.trim().is_empty() { stdout } else { String::new() })
        };
        Ok((status.code().unwrap_or(-1), text))
    }

    fn fail(code: i32, text: &str) -> String {
        let why = match code {
            1 => "provider failure (caffeinate, state or lock)",
            2 => "refused (usage error, duration over the provider's maximum, or a dead --pid)",
            _ => "failed",
        };
        format!("keep-awake provider exit {code}: {why}: {}", text.trim())
    }
}

/// `hold corral:x until 2026-11-22 18:00:00 (pid 123)` -> the pid, if printed.
fn parse_hold_line(owner: &str, text: &str) -> HoldInfo {
    let pid = text.rsplit_once("(pid ").and_then(|(_, r)| r.split(')').next()).and_then(|n| n.trim().parse().ok());
    HoldInfo { owner: owner.into(), until: None, pid, bound_pid: None, display: false }
}

pub fn parse_holds(json: &str) -> Result<Vec<HoldInfo>, String> {
    let v: serde_json::Value = serde_json::from_str(json).map_err(|e| format!("holds --json: {e}"))?;
    let arr = v.as_array().ok_or("holds --json: not an array")?;
    let mut out = Vec::new();
    for h in arr {
        let Some(owner) = h.get("owner").and_then(|o| o.as_str()) else { continue };
        let num = |k: &str| h.get(k).and_then(|x| x.as_f64()).map(|x| x as i64);
        out.push(HoldInfo {
            owner: owner.to_string(),
            until: num("until"),
            pid: num("pid").and_then(|p| u32::try_from(p).ok()),
            bound_pid: num("bound_pid").and_then(|p| u32::try_from(p).ok()),
            display: h.get("display").is_some_and(|d| d.as_bool() == Some(true) || d.as_i64() == Some(1)),
        });
    }
    Ok(out)
}

impl KeepAwake for HoldCli {
    fn name(&self) -> &str {
        "hold-cli"
    }

    fn probe(&self) -> Result<(), String> {
        // `holds --json` is read-only and starts nothing: available means it runs and prints a JSON list.
        let (code, text) = self.run(&["holds".into(), "--json".into()])?;
        if code != 0 {
            return Err(Self::fail(code, &text));
        }
        parse_holds(&text).map(|_| ())
    }

    fn hold(&self, owner: &str, dur: Duration, bound_pid: Option<u32>, display: bool) -> Result<HoldInfo, String> {
        valid_owner(owner)?;
        let mut args: Vec<String> =
            vec!["hold".into(), "--for".into(), format!("{}s", dur.as_secs().max(1)), "--owner".into(), owner.into()];
        if let Some(p) = bound_pid {
            args.extend(["--pid".into(), p.to_string()]);
        }
        if display {
            args.push("--display".into());
        }
        let (code, text) = self.run(&args)?;
        if code != 0 {
            return Err(Self::fail(code, &text));
        }
        let mut info = parse_hold_line(owner, &text);
        info.bound_pid = bound_pid;
        info.display = display;
        Ok(info)
    }

    fn release(&self, owner: &str) -> Result<(), String> {
        valid_owner(owner)?;
        // Never `release --all`: only the one named hold.
        let (code, text) = self.run(&["release".into(), "--owner".into(), owner.into()])?;
        if code == 0 { Ok(()) } else { Err(Self::fail(code, &text)) }
    }

    fn list(&self) -> Result<Vec<HoldInfo>, String> {
        let (code, text) = self.run(&["holds".into(), "--json".into()])?;
        if code != 0 {
            return Err(Self::fail(code, &text));
        }
        // The provider lists every owner's holds: keep only ours.
        Ok(parse_holds(&text)?.into_iter().filter(|h| h.owner.starts_with(OWNER_PREFIX)).collect())
    }
}

/// Build the configured provider (`[guard.keepawake]`). No section = no provider. Returns the provider and whether
/// heavy work requires it to work (R-AWAKE-03).
/// A provider and whether heavy work requires it.
pub type Provider = (Arc<dyn KeepAwake>, bool);

pub fn from_config(cfg: Option<&corral_proto::guardcfg::KeepAwakeCfg>) -> Result<Option<Provider>, String> {
    let Some(c) = cfg else { return Ok(None) };
    for w in &c.required_for {
        if w != "heavy" {
            return Err(format!("guard.keepawake.required_for: {w:?} is not \"heavy\""));
        }
    }
    let required = !c.required_for.is_empty();
    let p: Arc<dyn KeepAwake> = match c.provider.as_deref().unwrap_or("hold-cli") {
        "hold-cli" => Arc::new(HoldCli::new(c.command.clone().unwrap_or_else(|| "awake".into()))),
        "none" => Arc::new(NoKeepAwake),
        o @ ("caffeinate" | "systemd-inhibit") => {
            return Err(format!("guard.keepawake.provider {o:?} is not built yet; use \"hold-cli\" or \"none\""));
        }
        o => return Err(format!("guard.keepawake.provider: {o:?} is not hold-cli or none")),
    };
    Ok(Some((p, required)))
}

/// A probe result cached for a few seconds: admission asks often, the provider is an external command.
pub struct CachedProbe {
    provider: Arc<dyn KeepAwake>,
    ttl: Duration,
    last: Mutex<Option<(Instant, Result<(), String>)>>,
}

impl CachedProbe {
    pub fn new(provider: Arc<dyn KeepAwake>, ttl: Duration) -> CachedProbe {
        CachedProbe { provider, ttl, last: Mutex::new(None) }
    }
    pub fn check(&self) -> Result<(), String> {
        let mut l = self.last.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((t, r)) = &*l
            && t.elapsed() < self.ttl
        {
            return r.clone();
        }
        let r = self.provider.probe();
        *l = Some((Instant::now(), r.clone()));
        r
    }
}

/// For tests and always-on servers: `none` provider (no holds, always available) and a recording fake.
pub struct NoKeepAwake;

impl KeepAwake for NoKeepAwake {
    fn name(&self) -> &str {
        "none"
    }
    fn probe(&self) -> Result<(), String> {
        Ok(())
    }
    fn hold(&self, owner: &str, _dur: Duration, bound_pid: Option<u32>, display: bool) -> Result<HoldInfo, String> {
        Ok(HoldInfo { owner: owner.into(), until: None, pid: None, bound_pid, display })
    }
    fn release(&self, _owner: &str) -> Result<(), String> {
        Ok(())
    }
    fn list(&self) -> Result<Vec<HoldInfo>, String> {
        Ok(vec![])
    }
}

/// One call a [`FakeKeepAwake`] saw.
#[derive(Debug, Clone, PartialEq)]
pub enum Call {
    Hold { owner: String, secs: u64, bound_pid: Option<u32>, display: bool },
    Release(String),
}

/// A recording provider (R-AWAKE-01 acceptance test): it can be made to fail.
#[derive(Default)]
pub struct FakeKeepAwake {
    pub calls: Mutex<Vec<Call>>,
    pub broken: Mutex<Option<String>>,
}

impl FakeKeepAwake {
    pub fn new() -> Arc<FakeKeepAwake> {
        Arc::new(FakeKeepAwake::default())
    }
    pub fn break_it(&self, why: &str) {
        *self.broken.lock().unwrap_or_else(|e| e.into_inner()) = Some(why.into());
    }
    pub fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
    fn check(&self) -> Result<(), String> {
        match &*self.broken.lock().unwrap_or_else(|e| e.into_inner()) {
            Some(w) => Err(w.clone()),
            None => Ok(()),
        }
    }
}

impl KeepAwake for FakeKeepAwake {
    fn name(&self) -> &str {
        "fake"
    }
    fn probe(&self) -> Result<(), String> {
        self.check()
    }
    fn hold(&self, owner: &str, dur: Duration, bound_pid: Option<u32>, display: bool) -> Result<HoldInfo, String> {
        self.check()?;
        self.calls.lock().unwrap_or_else(|e| e.into_inner()).push(Call::Hold {
            owner: owner.into(),
            secs: dur.as_secs(),
            bound_pid,
            display,
        });
        Ok(HoldInfo { owner: owner.into(), until: None, pid: None, bound_pid, display })
    }
    fn release(&self, owner: &str) -> Result<(), String> {
        self.calls.lock().unwrap_or_else(|e| e.into_inner()).push(Call::Release(owner.into()));
        Ok(())
    }
    fn list(&self) -> Result<Vec<HoldInfo>, String> {
        self.check()?;
        Ok(vec![])
    }
}

/// A hold kept alive for one unit: taken now, renewed every [`RENEW_EVERY`] by a background task, released by
/// [`HoldGuard::release`] and on drop (every exit path, R-AWAKE-01). The renewal starts the new hold before the old
/// one ends because renewing *is* repeating `hold` (the provider extends the same name without a gap).
pub struct HoldGuard {
    provider: Arc<dyn KeepAwake>,
    owner: String,
    task: Option<tokio::task::JoinHandle<()>>,
    released: bool,
}

impl HoldGuard {
    /// Take the hold. An `Err` means the unit has no hold (the caller decides whether that is fatal: R-AWAKE-03).
    pub async fn take(
        provider: Arc<dyn KeepAwake>,
        owner: String,
        timeout: Duration,
        bound_pid: Option<u32>,
        display: bool,
    ) -> Result<HoldGuard, String> {
        let window = hold_window(timeout);
        let (p, o) = (provider.clone(), owner.clone());
        tokio::task::spawn_blocking(move || p.hold(&o, window, bound_pid, display)).await.map_err(|e| e.to_string())??;
        let (p, o) = (provider.clone(), owner.clone());
        let task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(RENEW_EVERY).await;
                let (p, o) = (p.clone(), o.clone());
                if let Ok(Err(e)) = tokio::task::spawn_blocking(move || p.hold(&o, window, bound_pid, display)).await {
                    tracing::warn!("keep-awake renewal failed: {e}");
                }
            }
        });
        Ok(HoldGuard { provider, owner, task: Some(task), released: false })
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Stop renewing and release the hold. Idempotent.
    pub fn release(&mut self) {
        if let Some(t) = self.task.take() {
            t.abort();
        }
        if !self.released {
            self.released = true;
            if let Err(e) = self.provider.release(&self.owner) {
                tracing::warn!("keep-awake release of {} failed (the hold expires by itself): {e}", self.owner);
            }
        }
    }
}

impl Drop for HoldGuard {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// A fake provider script: records every call line in `<dir>/calls`, prints the contract's answers, and lists
    /// one foreign hold next to ours. It touches nothing but its own directory.
    fn script(dir: &std::path::Path, body: &str) -> PathBuf {
        let p = dir.join("fake-awake");
        std::fs::write(&p, format!("#!/bin/sh\nD={}\necho \"$@\" >> \"$D/calls\"\n{body}\n", dir.display())).unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
        p
    }

    fn tmp(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("cw-ka-{tag}-{}", corral_proto::util::random_b32(6)));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    const OK_BODY: &str = r#"case "$1" in
hold) echo "hold $5 until 2026-11-22 18:00:00 (pid 4242)";;
release) exit 0;;
holds) echo '[{"owner":"someone-else","until":1,"pid":9,"started":0,"display":0,"bound_pid":null},{"owner":"corral:j-1:1","until":1790000000,"pid":4242,"started":1789990000,"display":1,"bound_pid":77}]';;
esac"#;

    /// R-AWAKE-01: hold, renew (a repeated hold) and release are the provider's named-hold calls, nothing else.
    #[test]
    fn hold_renew_release_use_the_named_contract() {
        let d = tmp("ok");
        let k = HoldCli::new(script(&d, OK_BODY));
        let h = k.hold("corral:j-1:1", Duration::from_secs(1800), Some(77), true).unwrap();
        assert_eq!((h.pid, h.bound_pid, h.display), (Some(4242), Some(77), true));
        k.hold("corral:j-1:1", Duration::from_secs(1800), None, false).unwrap();
        k.release("corral:j-1:1").unwrap();
        let calls = std::fs::read_to_string(d.join("calls")).unwrap();
        assert_eq!(
            calls.lines().collect::<Vec<_>>(),
            vec![
                "hold --for 1800s --owner corral:j-1:1 --pid 77 --display",
                "hold --for 1800s --owner corral:j-1:1",
                "release --owner corral:j-1:1"
            ]
        );
        assert!(!calls.contains("--all"), "release --all is never used");
    }

    /// `holds --json` lists every owner's holds; corral keeps only its own.
    #[test]
    fn list_filters_by_the_corral_prefix() {
        let d = tmp("list");
        let k = HoldCli::new(script(&d, OK_BODY));
        let v = k.list().unwrap();
        assert_eq!(v.len(), 1);
        assert_eq!((v[0].owner.as_str(), v[0].until, v[0].bound_pid, v[0].display), ("corral:j-1:1", Some(1790000000), Some(77), true));
        assert!(k.probe().is_ok());
    }

    /// Provider exit codes: 2 (usage, over max, dead pid) and 1 (caffeinate/state/lock) are errors with the output.
    #[test]
    fn provider_failures_are_errors_with_their_meaning() {
        let d = tmp("fail");
        let k = HoldCli::new(script(&d, "echo 'awake: --pid 5 is not running' >&2; exit 2"));
        let e = k.hold("corral:j-1:1", Duration::from_secs(60), Some(5), false).unwrap_err();
        assert!(e.contains("exit 2") && e.contains("not running"), "{e}");
        let k = HoldCli::new(script(&tmp("fail1"), "echo 'no lock' >&2; exit 1"));
        assert!(k.probe().unwrap_err().contains("exit 1"));
        // a command that does not exist is unavailable, not a panic
        assert!(HoldCli::new("/nonexistent/awake-provider").probe().is_err());
    }

    /// Owners that are not ours, or malformed, never reach the provider.
    #[test]
    fn foreign_owner_names_are_refused_before_calling_the_provider() {
        let d = tmp("own");
        let k = HoldCli::new(script(&d, OK_BODY));
        for bad in ["someone-else", "corral:", "corral:a b", "corral:a\nb"] {
            assert!(k.hold(bad, Duration::from_secs(60), None, false).is_err(), "{bad:?}");
            assert!(k.release(bad).is_err(), "{bad:?}");
        }
        assert!(!d.join("calls").exists(), "the provider was never called");
    }

    /// A hung provider is abandoned at the deadline instead of blocking the worker.
    #[test]
    fn a_hung_provider_times_out() {
        let d = tmp("hang");
        let k = HoldCli::new(script(&d, "sleep 30")).with_timeout(Duration::from_millis(300));
        let t = Instant::now();
        assert!(k.probe().unwrap_err().contains("timed out"));
        assert!(t.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn window_and_names() {
        assert_eq!(hold_window(Duration::from_secs(30)), Duration::from_secs(630));
        assert_eq!(hold_window(Duration::from_secs(86_400)), Duration::from_secs(7_800), "capped at 2 h + 10 min");
        assert_eq!(job_owner("j-1", 2), "corral:j-1:2");
        assert_eq!(run_owner("build"), "corral:run:build");
    }

    /// The guard takes the hold, renews on its own task and releases on every exit path, drop included.
    #[tokio::test]
    async fn hold_guard_releases_on_drop() {
        let fake = FakeKeepAwake::new();
        let g = HoldGuard::take(fake.clone(), "corral:j-1:1".into(), Duration::from_secs(60), Some(5), false).await.unwrap();
        assert_eq!(fake.calls(), vec![Call::Hold { owner: "corral:j-1:1".into(), secs: 660, bound_pid: Some(5), display: false }]);
        drop(g);
        assert_eq!(fake.calls().last(), Some(&Call::Release("corral:j-1:1".into())));
        fake.break_it("provider gone");
        assert!(HoldGuard::take(fake.clone(), "corral:j-2:1".into(), Duration::from_secs(1), None, false).await.is_err());
    }
}
