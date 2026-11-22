//! Resolved worker settings (defaults < config file).

use corral_proto::config::{Config, expand};
use corral_proto::util::{parse_duration_ms, parse_size_mb};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct WorkerSettings {
    pub host: String,
    pub caps: Vec<String>,
    pub heavy_slots: u32,
    pub light_slots: u32,
    /// Never claim more than this, whatever is free.
    pub ram_cap_mb: Option<u64>,
    /// Memory that must stay available after a job's declared RAM. `None` = 20% of total RAM.
    pub admit_floor_mb: Option<u64>,
    /// RAM a light job without `ram_mb` is admitted and watched against: no job runs unlimited.
    pub light_ram_default_mb: u64,
    pub work_dir: PathBuf,
    pub http_url: String,
    pub kill_grace_ms: u64,
    pub hb_interval_ms: u64,
    pub presence_interval_ms: u64,
    pub claim_retry_ms: u64,
    pub keep_failed_ms: u64,
    pub log_spool_max: u64,
    pub lock_path: PathBuf,
    pub lock_ttl_s: u64,
    pub job_path: String,
    pub env_passthrough: Vec<String>,
    /// A job whose footprint exceeds `ram * budget_slack` is killed (`ram_exceeded`).
    pub budget_slack: f64,
    pub version: String,
}

impl WorkerSettings {
    pub fn for_dir(host: &str, work_dir: impl Into<PathBuf>, http_url: &str) -> WorkerSettings {
        let work_dir = work_dir.into();
        WorkerSettings {
            host: host.into(),
            caps: auto_caps(),
            heavy_slots: 1,
            light_slots: 2,
            ram_cap_mb: None,
            admit_floor_mb: None,
            light_ram_default_mb: 512,
            lock_path: work_dir.join("locks/heavy.lock"),
            work_dir,
            http_url: http_url.into(),
            kill_grace_ms: 10_000,
            hb_interval_ms: 10_000,
            presence_interval_ms: 15_000,
            claim_retry_ms: 60_000,
            keep_failed_ms: 24 * 3_600_000,
            log_spool_max: 512 * 1024 * 1024,
            lock_ttl_s: 300,
            job_path: std::env::var("PATH").unwrap_or_else(|_| "/usr/local/bin:/usr/bin:/bin".into()),
            env_passthrough: vec![],
            budget_slack: 1.1,
            version: env!("CARGO_PKG_VERSION").into(),
        }
    }

    pub fn from_config(cfg: &Config, http_url: Option<String>) -> Result<WorkerSettings, String> {
        let host = cfg.host.clone().ok_or("host is required for a worker (set host = \"...\")")?;
        let w = cfg.worker.clone().unwrap_or_default();
        let work_dir = w.work_dir.as_deref().map(expand).unwrap_or_else(|| "./corral-work".into());
        let http =
            http_url.or_else(|| cfg.client.as_ref().and_then(|c| c.http_url.clone())).ok_or("client.http_url is required for a worker")?;
        let mut s = WorkerSettings::for_dir(&host, work_dir, &http);
        let mut caps = w.caps.clone();
        for c in auto_caps() {
            if !caps.contains(&c) {
                caps.push(c);
            }
        }
        s.caps = caps;
        if let Some(sl) = &w.slots {
            s.heavy_slots = sl.heavy.unwrap_or(s.heavy_slots);
            s.light_slots = sl.light.unwrap_or(s.light_slots);
        }
        s.ram_cap_mb = w.admissible_ram_cap.as_deref().and_then(|v| parse_size_mb(v).ok());
        s.admit_floor_mb = w.admit_floor.as_deref().and_then(|v| parse_size_mb(v).ok());
        if let Some(m) = w.light_ram_default.as_deref().and_then(|v| parse_size_mb(v).ok()).filter(|m| *m > 0) {
            s.light_ram_default_mb = m;
        }
        let dur = |v: &Option<String>, d: u64| v.as_deref().and_then(|x| parse_duration_ms(x).ok()).unwrap_or(d);
        s.kill_grace_ms = dur(&w.kill_grace, s.kill_grace_ms);
        s.keep_failed_ms = dur(&w.keep_failed_runs, s.keep_failed_ms);
        if let Some(m) = w.log_spool_max.as_deref().and_then(|v| parse_size_mb(v).ok()) {
            s.log_spool_max = m * 1024 * 1024;
        }
        if let Some(l) = &w.heavy_lock {
            if let Some(p) = &l.path {
                s.lock_path = PathBuf::from(expand(p));
            }
            if let Some(t) = &l.ttl {
                s.lock_ttl_s = parse_duration_ms(t).map(|ms| ms / 1000).unwrap_or(s.lock_ttl_s);
            }
        } else {
            s.lock_path = s.work_dir.join("locks/heavy.lock");
        }
        if let Some(p) = &w.job_path {
            s.job_path = p.clone();
        }
        s.env_passthrough = w.env_passthrough.clone();
        Ok(s)
    }
}

/// Capabilities every worker of this platform offers without configuration.
pub fn auto_caps() -> Vec<String> {
    let mut v = vec![std::env::consts::OS.to_string()];
    v.push(match std::env::consts::ARCH {
        "aarch64" => "arm64".to_string(),
        a => a.to_string(),
    });
    v
}
