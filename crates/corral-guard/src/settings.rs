//! Resolved guard settings: defaults < configuration file (GUARDS.md 9).

use crate::engine::HookRef;
use crate::levels::LevelCfg;
use crate::procs::{Matcher, Select};
use corral_proto::config::{Config, expand};
use corral_proto::guardcfg::GuardCfg;
use corral_proto::util::{parse_duration_ms, parse_size_mb};
use corral_worker::lock::Takeover;
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct LockSettings {
    pub path: PathBuf,
    pub ttl_s: u64,
    pub takeover: Takeover,
    /// Environment variable of a legacy lock's own re-entry token (flock kind, M4).
    pub reentry_env: Option<String>,
}

#[derive(Debug, Clone)]
pub struct HookSettings {
    pub name: String,
    pub on: Vec<String>,
    pub cmd: Vec<String>,
    pub timeout_ms: u64,
}

#[derive(Clone)]
pub struct GuardSettings {
    pub host: String,
    pub macos: bool,
    pub observe: bool,
    pub sample_ms: u64,
    pub settle_ms: u64,
    pub kill_grace_ms: u64,
    pub budget_slack: f64,
    pub heavy_slots: u32,
    pub notify_after_ms: u64,
    pub pause_max_ms: u64,
    pub pause_max_thermal_ms: u64,
    pub clear_samples: u32,
    pub state_dir: PathBuf,
    pub socket: PathBuf,
    pub levels: LevelCfg,
    pub matchers: Vec<Matcher>,
    pub never: Vec<Select>,
    pub hooks: Vec<HookSettings>,
    pub locks: BTreeMap<String, LockSettings>,
    pub disk_path: PathBuf,
    pub min_disk_mb: u64,
    pub admit_max_c: f64,
    pub sensor: Option<(String, String)>,
    pub zone: Option<PathBuf>,
}

fn is_root() -> bool {
    rustix::process::getuid().is_root()
}

pub fn default_state_dir() -> PathBuf {
    if is_root() {
        return "/var/lib/corral/guard".into();
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    let base = std::env::var("XDG_STATE_HOME").unwrap_or_else(|_| format!("{home}/.local/state"));
    PathBuf::from(base).join("corral/guard")
}

/// `/run/corral/guard.sock` for the system service; else under the user's runtime directory; else the state dir.
pub fn default_socket(state_dir: &std::path::Path) -> PathBuf {
    if is_root() {
        return "/run/corral/guard.sock".into();
    }
    match std::env::var("XDG_RUNTIME_DIR") {
        Ok(d) if !d.is_empty() => PathBuf::from(d).join("corral/guard.sock"),
        _ => state_dir.join("guard.sock"),
    }
}

impl GuardSettings {
    pub fn from_config(cfg: &Config) -> Result<GuardSettings, String> {
        Self::resolve(cfg, cfg!(target_os = "macos"))
    }

    pub fn resolve(cfg: &Config, macos: bool) -> Result<GuardSettings, String> {
        let g: GuardCfg = cfg.guard.clone().unwrap_or_default();
        match g.mode.as_deref().unwrap_or("observe") {
            "observe" => {}
            "enforce" => return Err("guard.mode = \"enforce\" is not available in this build (milestone M3); use \"observe\"".into()),
            o => return Err(format!("guard.mode: {o:?} is not observe or enforce")),
        }
        let dur = |k: &str, v: &Option<String>, d: u64| -> Result<u64, String> {
            v.as_deref().map(|s| parse_duration_ms(s).map_err(|e| format!("guard.{k}: {e}"))).transpose().map(|o| o.unwrap_or(d))
        };
        let state_dir = g.state_dir.as_deref().map(|s| PathBuf::from(expand(s))).unwrap_or_else(default_state_dir);
        let socket = g.socket.as_deref().map(|s| PathBuf::from(expand(s))).unwrap_or_else(|| default_socket(&state_dir));
        let over = g.levels.as_ref().and_then(|l| if macos { l.macos.as_ref() } else { l.linux.as_ref() });
        let levels = LevelCfg::resolve(macos, over).map_err(|e| format!("guard.levels: {e}"))?;
        let matchers = g.matchers.iter().map(Matcher::compile).collect::<Result<Vec<_>, _>>()?;
        let mut never = Vec::new();
        for n in &g.never {
            never.push(Select::compile(&n.select).map_err(|e| format!("never[{}].select: {e}", n.name))?);
        }
        let mut hooks = Vec::new();
        for h in &g.restore_hooks {
            if h.cmd.is_empty() {
                return Err(format!("restore_hooks[{}].cmd is empty", h.name));
            }
            for on in &h.on {
                if !matches!(on.as_str(), "shed" | "critical" | "stop") {
                    return Err(format!("restore_hooks[{}].on: {on:?} is not shed, critical or stop", h.name));
                }
            }
            hooks.push(HookSettings {
                name: h.name.clone(),
                on: h.on.clone(),
                cmd: h.cmd.clone(),
                timeout_ms: dur("restore_hooks.timeout", &h.timeout, 30_000)?,
            });
        }
        let mut locks = BTreeMap::new();
        for (name, l) in &g.locks {
            match l.kind.as_deref().unwrap_or("lease") {
                "lease" => {}
                "flock" => return Err(format!("guard.locks.{name}: kind \"flock\" arrives with milestone M4 (measurement queue interop)")),
                o => return Err(format!("guard.locks.{name}.kind: {o:?} is not lease or flock")),
            }
            let takeover = match l.takeover.as_deref().unwrap_or("corral") {
                "corral" => Takeover::Corral,
                "any" => Takeover::Any,
                "never" => Takeover::Never,
                o => return Err(format!("guard.locks.{name}.takeover: {o:?} is not corral, any or never")),
            };
            locks.insert(
                name.clone(),
                LockSettings {
                    path: PathBuf::from(expand(&l.path)),
                    ttl_s: dur(&format!("locks.{name}.ttl"), &l.ttl, 300_000)? / 1000,
                    takeover,
                    reentry_env: l.reentry_env.clone(),
                },
            );
        }
        let sensor = over.and_then(|o| o.temp_sensor.as_ref());
        Ok(GuardSettings {
            host: cfg.host.clone().unwrap_or_else(|| "host".into()),
            macos,
            observe: true,
            sample_ms: dur("sample", &g.sample, 2_000)?.max(100),
            settle_ms: dur("settle", &g.settle, 8_000)?,
            kill_grace_ms: dur("kill_grace", &g.kill_grace, 8_000)?,
            budget_slack: g.budget_slack.unwrap_or(1.1),
            heavy_slots: g.heavy_slots.unwrap_or(1),
            notify_after_ms: dur("notify_after", &g.notify_after, 30 * 60_000)?,
            pause_max_ms: dur("pause_max", &g.pause_max, 60_000)?,
            pause_max_thermal_ms: dur("pause_max_thermal", &g.pause_max_thermal, 10 * 60_000)?,
            clear_samples: g.clear_samples.unwrap_or(3).max(1),
            disk_path: g.disk_path.as_deref().map(|s| PathBuf::from(expand(s))).unwrap_or_else(|| state_dir.clone()),
            min_disk_mb: g
                .min_disk
                .as_deref()
                .map(parse_size_mb)
                .transpose()
                .map_err(|e| format!("guard.min_disk: {e}"))?
                .unwrap_or(15 * 1024),
            admit_max_c: g.admit_max_c.unwrap_or(85.0),
            sensor: sensor.and_then(|s| Some((s.hwmon.clone()?, s.label.clone()?))),
            zone: sensor.and_then(|s| s.zone.as_ref()).map(|z| PathBuf::from(format!("/sys/class/thermal/{z}/temp"))),
            state_dir,
            socket,
            levels,
            matchers,
            never,
            hooks,
            locks,
        })
    }

    pub fn hook_refs(&self) -> Vec<HookRef> {
        self.hooks.iter().map(|h| HookRef { name: h.name.clone(), on: h.on.clone() }).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(extra: &str) -> Config {
        Config::parse(&format!("version = 1\nhost = \"laptop\"\n{extra}")).unwrap()
    }

    #[test]
    fn defaults_and_the_spec_example() {
        let s = GuardSettings::resolve(&cfg(""), true).unwrap();
        assert_eq!((s.sample_ms, s.settle_ms, s.heavy_slots, s.clear_samples), (2_000, 8_000, 1, 3));
        let c = cfg(r#"
[guard]
mode = "observe"
[[guard.matchers]]
name = "corral-jobs"
select = { registered = "corral:*" }
tier = 0
pausable = true
[[guard.matchers]]
name = "llm-servers"
select = { cmdline = 'llama-server|llama-cli' }
tier = 1
[[guard.never]]
name = "personal-emulator"
select = { cmdline = '-avd personal|@personal' }
[[guard.restore_hooks]]
name = "perf-restore"
on = ["critical", "stop"]
cmd = ["/usr/local/libexec/restore"]
timeout = "30s"
[guard.locks.emulator]
kind = "lease"
path = "/var/tmp/locks/emulator.lock"
ttl = "300s"
takeover = "any"
[guard.levels.macos]
shed = { free_pct_at_most = 12 }
"#);
        let s = GuardSettings::resolve(&c, true).unwrap();
        assert_eq!(s.matchers.len(), 2);
        assert_eq!(s.never.len(), 1);
        assert_eq!(s.hooks[0].timeout_ms, 30_000);
        assert_eq!(s.locks["emulator"].takeover, Takeover::Any);
        assert_eq!(s.levels.shed.free_at_most, Some(12.0));
    }

    #[test]
    fn enforce_is_refused_until_m3_and_bad_values_name_their_key() {
        let e = GuardSettings::resolve(&cfg("[guard]\nmode = \"enforce\"\n"), true).err().unwrap();
        assert!(e.contains("M3"), "{e}");
        let e = GuardSettings::resolve(&cfg("[guard]\nsample = \"fast\"\n"), true).err().unwrap();
        assert!(e.contains("guard.sample"), "{e}");
        let e = GuardSettings::resolve(&cfg("[[guard.matchers]]\nname = \"x\"\nselect = {}\ntier = 1\n"), true).err().unwrap();
        assert!(e.contains("matchers[x].select"), "{e}");
        let e =
            GuardSettings::resolve(&cfg("[[guard.matchers]]\nname = \"x\"\nselect = { cmdline = \"(\" }\ntier = 1\n"), true).err().unwrap();
        assert!(e.contains("cmdline"), "{e}");
        assert!(Config::parse("version = 1\n[guard]\nbogus = 1\n").is_err(), "unknown keys are errors");
        let e = GuardSettings::resolve(&cfg("[guard.locks.d]\nkind = \"flock\"\npath = \"/x\"\n"), true).err().unwrap();
        assert!(e.contains("M4"), "{e}");
    }
}
