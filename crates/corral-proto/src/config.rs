//! One configuration file per host (SPEC.md section 9, R-OPS-04). Unknown keys are errors; every error names the key.

use crate::util::{parse_duration_ms, parse_size_mb};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct ConfigError {
    pub key: String,
    pub message: String,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.key.is_empty() { write!(f, "config: {}", self.message) } else { write!(f, "config: {}: {}", self.key, self.message) }
    }
}

impl std::error::Error for ConfigError {}

fn err(key: &str, message: impl Into<String>) -> ConfigError {
    ConfigError { key: key.to_string(), message: message.into() }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub version: Option<u32>,
    pub host: Option<String>,
    pub client: Option<ClientCfg>,
    pub scheduler: Option<SchedulerCfg>,
    pub worker: Option<WorkerCfg>,
    pub phase2: Option<Phase2Cfg>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Phase2Cfg {
    #[serde(default)]
    pub cloud: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientCfg {
    pub nats_url: Option<String>,
    pub http_url: Option<String>,
    pub identity: Option<String>,
    pub nkey_seed_file: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Retention {
    pub age: Option<String>,
    pub cap: Option<String>,
    pub keep_flagged: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    #[serde(default)]
    pub require_grant: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerCfg {
    pub data_dir: Option<String>,
    /// Addresses the HTTP API binds. Default: loopback only.
    pub listen: Option<Vec<String>>,
    pub http_port: Option<u16>,
    #[serde(default)]
    pub allow_public_bind: bool,
    pub lease_ttl: Option<String>,
    pub orphan_grace: Option<String>,
    pub claim_deadline: Option<String>,
    pub max_attempts: Option<u32>,
    pub max_timeout: Option<String>,
    /// Cap for one uploaded snapshot (the CLI also enforces `snapshot_max_mb`).
    pub input_max_mb: Option<u64>,
    pub retention: Option<Retention>,
    pub policy: Option<Policy>,
    /// File holding the HMAC key for HTTP capability tokens (created with mode 0600 when absent).
    pub key_file: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlotsCfg {
    pub heavy: Option<u32>,
    pub light: Option<u32>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeavyLockCfg {
    pub path: Option<String>,
    pub ttl: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerCfg {
    #[serde(default)]
    pub caps: Vec<String>,
    pub slots: Option<SlotsCfg>,
    pub admissible_ram_cap: Option<String>,
    /// Memory that must stay available after a job's declared RAM (admission floor). Default: 20% of RAM.
    pub admit_floor: Option<String>,
    /// RAM a light job that declares none is admitted and watched against (default 512M); never unlimited.
    pub light_ram_default: Option<String>,
    pub work_dir: Option<String>,
    pub runtime: Option<String>,
    pub cost_tier: Option<u32>,
    pub heavy_lock: Option<HeavyLockCfg>,
    pub kill_grace: Option<String>,
    pub keep_failed_runs: Option<String>,
    pub log_spool_max: Option<String>,
    /// Extra environment variable names the worker passes to jobs from its own environment (default: none).
    #[serde(default)]
    pub env_passthrough: Vec<String>,
    /// `PATH` given to jobs.
    pub job_path: Option<String>,
}

/// Where the config came from, for `config check`.
#[derive(Debug, Clone)]
pub struct Loaded {
    pub config: Config,
    pub path: Option<PathBuf>,
}

pub fn default_paths() -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Ok(p) = std::env::var("CORRAL_CONFIG") {
        v.push(PathBuf::from(p));
        return v;
    }
    if let Ok(x) = std::env::var("XDG_CONFIG_HOME") {
        v.push(Path::new(&x).join("corral/corral.toml"));
    } else if let Ok(h) = std::env::var("HOME") {
        v.push(Path::new(&h).join(".config/corral/corral.toml"));
    }
    v.push(PathBuf::from("/etc/corral/corral.toml"));
    v
}

/// Expand `$HOME`, `$XDG_CONFIG_HOME` and a leading `~` in a configured path.
pub fn expand(s: &str) -> String {
    let home = std::env::var("HOME").unwrap_or_default();
    let xdg = std::env::var("XDG_CONFIG_HOME").unwrap_or_else(|_| format!("{home}/.config"));
    let s = if let Some(rest) = s.strip_prefix("~/") { format!("{home}/{rest}") } else { s.to_string() };
    s.replace("$XDG_CONFIG_HOME", &xdg).replace("$HOME", &home)
}

impl Config {
    pub fn parse(text: &str) -> Result<Config, ConfigError> {
        let cfg: Config = toml::from_str(text).map_err(|e| err("", e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn load(path: &Path) -> Result<Config, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|e| err("", format!("{}: {e}", path.display())))?;
        Self::parse(&text)
    }

    /// Load the first existing default file, or an empty config (client settings may come from the environment).
    pub fn load_default() -> Result<Loaded, ConfigError> {
        for p in default_paths() {
            if p.exists() {
                return Ok(Loaded { config: Self::load(&p)?, path: Some(p) });
            }
        }
        Ok(Loaded { config: Config::default(), path: None })
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        match self.version {
            Some(1) => {}
            Some(v) => {
                return Err(err("version", format!("unsupported version {v} (this build reads 1)")));
            }
            None => return Err(err("version", "required (set version = 1)")),
        }
        if let Some(h) = &self.host
            && !crate::ident::valid_host(h)
        {
            return Err(err("host", format!("{h:?} is not a valid host name (letters, digits, - and _)")));
        }
        if let Some(c) = &self.client {
            if let Some(i) = &c.identity
                && crate::ident::Identity::parse(i).is_none()
            {
                return Err(err("client.identity", format!("{i:?} is not scheduler, owner, worker:<host> or agent:<host>")));
            }
        }
        if let Some(s) = &self.scheduler {
            let dur = |key: &str, v: &Option<String>| -> Result<(), ConfigError> {
                if let Some(v) = v {
                    parse_duration_ms(v).map_err(|m| err(key, m))?;
                }
                Ok(())
            };
            dur("scheduler.lease_ttl", &s.lease_ttl)?;
            dur("scheduler.orphan_grace", &s.orphan_grace)?;
            dur("scheduler.claim_deadline", &s.claim_deadline)?;
            dur("scheduler.max_timeout", &s.max_timeout)?;
            if let Some(r) = &s.retention {
                dur("scheduler.retention.age", &r.age)?;
                dur("scheduler.retention.keep_flagged", &r.keep_flagged)?;
                if let Some(c) = &r.cap {
                    parse_size_mb(c).map_err(|m| err("scheduler.retention.cap", m))?;
                }
            }
            for (i, a) in s.listen.iter().flatten().enumerate() {
                if matches!(a.as_str(), "0.0.0.0" | "::" | "[::]") && !s.allow_public_bind {
                    return Err(err(
                        &format!("scheduler.listen[{i}]"),
                        format!("{a} binds every interface; list a private address or set allow_public_bind = true"),
                    ));
                }
            }
            if let Some(p) = &s.policy {
                for a in &p.require_grant {
                    const KNOWN: [&str; 7] =
                        ["exclusive", "priority_high", "cancel_other", "resubmit_killed", "admin", "lock_none", "cost_tier"];
                    if !KNOWN.contains(&a.as_str()) {
                        return Err(err("scheduler.policy.require_grant", format!("unknown action {a:?}")));
                    }
                }
            }
        }
        if let Some(w) = &self.worker {
            for (key, v) in [("worker.kill_grace", &w.kill_grace), ("worker.keep_failed_runs", &w.keep_failed_runs)] {
                if let Some(v) = v {
                    parse_duration_ms(v).map_err(|m| err(key, m))?;
                }
            }
            for (key, v) in [
                ("worker.admissible_ram_cap", &w.admissible_ram_cap),
                ("worker.admit_floor", &w.admit_floor),
                ("worker.light_ram_default", &w.light_ram_default),
                ("worker.log_spool_max", &w.log_spool_max),
            ] {
                if let Some(v) = v {
                    parse_size_mb(v).map_err(|m| err(key, m))?;
                }
            }
            if let Some(r) = &w.runtime
                && r != "native"
            {
                return Err(err("worker.runtime", format!("unknown runtime {r:?} (v1 ships only \"native\")")));
            }
            if w.cost_tier.unwrap_or(0) > 0 && !self.phase2.as_ref().is_some_and(|p| p.cloud) {
                return Err(err("worker.cost_tier", "non-zero cost tiers need phase2.cloud = true (not available in v1)"));
            }
            if let Some(l) = &w.heavy_lock
                && let Some(t) = &l.ttl
            {
                parse_duration_ms(t).map_err(|m| err("worker.heavy_lock.ttl", m))?;
            }
        }
        Ok(())
    }
}

/// Resolved client settings: defaults < file < `CORRAL_*` environment < (caller applies flags).
#[derive(Debug, Clone)]
pub struct ClientResolved {
    pub nats_url: String,
    pub http_url: Option<String>,
    pub identity: String,
    pub nkey_seed_file: Option<PathBuf>,
    /// For `config check`: which source set each key.
    pub sources: BTreeMap<&'static str, &'static str>,
}

impl ClientCfg {
    pub fn resolve(file: Option<&ClientCfg>, env: &dyn Fn(&str) -> Option<String>) -> ClientResolved {
        let mut sources = BTreeMap::new();
        let mut pick = |key: &'static str, envname: &str, filev: Option<&String>, default: Option<&str>| -> Option<String> {
            if let Some(v) = env(envname) {
                sources.insert(key, "env");
                Some(v)
            } else if let Some(v) = filev {
                sources.insert(key, "file");
                Some(v.clone())
            } else if let Some(d) = default {
                sources.insert(key, "default");
                Some(d.to_string())
            } else {
                None
            }
        };
        let nats_url =
            pick("nats_url", "CORRAL_NATS_URL", file.and_then(|f| f.nats_url.as_ref()), Some("nats://127.0.0.1:4222")).unwrap_or_default();
        let http_url = pick("http_url", "CORRAL_HTTP_URL", file.and_then(|f| f.http_url.as_ref()), None);
        let identity = pick("identity", "CORRAL_IDENTITY", file.and_then(|f| f.identity.as_ref()), Some("owner")).unwrap_or_default();
        let seed = pick("nkey_seed_file", "CORRAL_NKEY_SEED_FILE", file.and_then(|f| f.nkey_seed_file.as_ref()), None);
        ClientResolved { nats_url, http_url, identity, nkey_seed_file: seed.map(|s| PathBuf::from(expand(&s))), sources }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key_of(text: &str) -> String {
        Config::parse(text).unwrap_err().to_string()
    }

    /// U-16 / R-OPS-04
    #[test]
    fn unknown_keys_and_errors_name_the_key() {
        assert!(key_of("version = 1\nbogus = 2\n").contains("bogus"));
        assert!(key_of("version = 1\n[worker]\nslotz = {}\n").contains("slotz"));
        assert!(key_of("[worker]\n").contains("version"));
        assert!(key_of("version = 1\n[scheduler]\nlease_ttl = \"90\"\n").contains("scheduler.lease_ttl"));
        assert!(key_of("version = 1\n[worker]\nadmit_floor = \"lots\"\n").contains("worker.admit_floor"));
        assert!(key_of("version = 1\nhost = \"a.b\"\n").contains("host"));
    }

    /// R-SEC-06
    #[test]
    fn public_bind_is_refused_unless_allowed() {
        let bad = "version = 1\n[scheduler]\nlisten = [\"127.0.0.1\", \"0.0.0.0\"]\n";
        assert!(key_of(bad).contains("scheduler.listen[1]"));
        let ok = "version = 1\n[scheduler]\nallow_public_bind = true\nlisten = [\"0.0.0.0\"]\n";
        assert!(Config::parse(ok).is_ok());
    }

    /// R-SCHED-04
    #[test]
    fn paid_tier_needs_the_phase2_gate() {
        let t = "version = 1\n[worker]\ncost_tier = 1\n";
        assert!(key_of(t).contains("worker.cost_tier"));
        let gated = "version = 1\n[phase2]\ncloud = true\n[worker]\ncost_tier = 1\n";
        assert!(Config::parse(gated).is_ok());
    }

    #[test]
    fn full_example_parses() {
        let text = r#"
version = 1
host = "server"
[client]
nats_url = "nats://server.example.ts.net:4222"
identity = "agent:server"
[scheduler]
data_dir = "/var/lib/corral"
listen = ["127.0.0.1", "192.0.2.10"]
retention = { age = "14d", cap = "30G", keep_flagged = "90d" }
policy.require_grant = []
[worker]
caps = ["linux", "x86_64"]
slots = { heavy = 1, light = 2 }
admissible_ram_cap = "12G"
runtime = "native"
"#;
        assert!(Config::parse(text).is_ok());
    }

    #[test]
    fn client_precedence_env_over_file_over_default() {
        let file = ClientCfg { nats_url: Some("nats://file:1".into()), identity: Some("agent:h".into()), ..Default::default() };
        let env = |k: &str| (k == "CORRAL_NATS_URL").then(|| "nats://env:2".to_string());
        let r = ClientCfg::resolve(Some(&file), &env);
        assert_eq!(r.nats_url, "nats://env:2");
        assert_eq!(r.sources["nats_url"], "env");
        assert_eq!(r.identity, "agent:h");
        assert_eq!(r.sources["identity"], "file");
        let d = ClientCfg::resolve(None, &|_| None);
        assert_eq!(d.sources["identity"], "default");
    }
}
