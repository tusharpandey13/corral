//! Resolved scheduler settings (defaults < config file).

use corral_proto::config::{Config, expand};
use corral_proto::util::{parse_duration_ms, parse_size_mb};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Settings {
    pub data_dir: PathBuf,
    pub listen: Vec<String>,
    pub http_port: u16,
    pub lease_ttl_ms: i64,
    pub orphan_grace_ms: i64,
    pub claim_deadline_ms: i64,
    pub max_timeout_s: u64,
    pub input_max_mb: u64,
    pub retention_age_ms: i64,
    pub retention_cap_mb: u64,
    pub keep_flagged_ms: i64,
    pub require_grant: Vec<String>,
    pub key_file: Option<PathBuf>,
    /// A worker whose presence is older than this is offline.
    pub presence_ttl_ms: i64,
    pub upload_ttl_ms: i64,
    /// How often a worker with nothing to do asks again.
    pub retry_after_s: u64,
    /// A log sequence gap is given up on (recorded in the archive) after this many redeliveries, about one a second.
    pub sink_hold_max: u64,
}

impl Settings {
    pub fn for_dir(data_dir: impl Into<PathBuf>) -> Settings {
        Settings {
            data_dir: data_dir.into(),
            listen: vec!["127.0.0.1".into()],
            http_port: 4280,
            lease_ttl_ms: 90_000,
            orphan_grace_ms: 15 * 60_000,
            claim_deadline_ms: 120_000,
            max_timeout_s: 48 * 3600,
            input_max_mb: 1024,
            retention_age_ms: 14 * 86_400_000,
            retention_cap_mb: 30 * 1024,
            keep_flagged_ms: 90 * 86_400_000,
            require_grant: vec![],
            key_file: None,
            presence_ttl_ms: 60_000,
            upload_ttl_ms: 3_600_000,
            retry_after_s: 60,
            sink_hold_max: 30,
        }
    }

    pub fn from_config(cfg: &Config) -> Settings {
        let s = cfg.scheduler.clone().unwrap_or_default();
        let dir = s.data_dir.as_deref().map(expand).unwrap_or_else(|| "./corral-data".into());
        let mut out = Settings::for_dir(dir);
        let dur = |v: &Option<String>, d: i64| v.as_deref().and_then(|x| parse_duration_ms(x).ok()).map(|x| x as i64).unwrap_or(d);
        out.lease_ttl_ms = dur(&s.lease_ttl, out.lease_ttl_ms);
        out.orphan_grace_ms = dur(&s.orphan_grace, out.orphan_grace_ms);
        out.claim_deadline_ms = dur(&s.claim_deadline, out.claim_deadline_ms);
        if let Some(m) = &s.max_timeout
            && let Ok(ms) = parse_duration_ms(m)
        {
            out.max_timeout_s = ms / 1000;
        }
        if let Some(l) = s.listen {
            out.listen = l;
        }
        if let Some(p) = s.http_port {
            out.http_port = p;
        }
        if let Some(m) = s.input_max_mb {
            out.input_max_mb = m;
        }
        if let Some(r) = &s.retention {
            out.retention_age_ms = dur(&r.age, out.retention_age_ms);
            out.keep_flagged_ms = dur(&r.keep_flagged, out.keep_flagged_ms);
            if let Some(c) = r.cap.as_deref().and_then(|c| parse_size_mb(c).ok()) {
                out.retention_cap_mb = c;
            }
        }
        if let Some(p) = &s.policy {
            out.require_grant = p.require_grant.clone();
        }
        out.key_file = s.key_file.as_deref().map(|k| PathBuf::from(expand(k)));
        out
    }
}
