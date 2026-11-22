//! The `[guard]` section of the configuration file (GUARDS.md section 9). Unknown keys are errors.

use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GuardCfg {
    /// `observe` (log decisions, signal nothing) or `enforce` (M3).
    pub mode: Option<String>,
    pub sample: Option<String>,
    pub kill_grace: Option<String>,
    pub settle: Option<String>,
    pub budget_slack: Option<f64>,
    pub heavy_slots: Option<u32>,
    pub notify_after: Option<String>,
    pub state_dir: Option<String>,
    /// Unix socket path (default: the runtime directory, GUARDS.md section 6).
    pub socket: Option<String>,
    /// Longest a paused registration stays paused before it would be killed (memory 60 s).
    pub pause_max: Option<String>,
    pub pause_max_thermal: Option<String>,
    pub clear_samples: Option<u32>,
    pub levels: Option<LevelsCfg>,
    #[serde(default)]
    pub matchers: Vec<MatcherCfg>,
    #[serde(default)]
    pub never: Vec<NeverCfg>,
    #[serde(default)]
    pub restore_hooks: Vec<HookCfg>,
    #[serde(default)]
    pub locks: BTreeMap<String, LockCfg>,
    pub keepawake: Option<KeepAwakeCfg>,
    /// Directory the `disk_free` check measures (default: the guard's state directory).
    pub disk_path: Option<String>,
    pub min_disk: Option<String>,
    pub admit_max_c: Option<f64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LevelsCfg {
    pub macos: Option<LevelSet>,
    pub linux: Option<LevelSet>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LevelSet {
    pub closed: Option<LevelCond>,
    pub pressure: Option<LevelCond>,
    pub shed: Option<LevelCond>,
    pub critical: Option<LevelCond>,
    pub admit_floor_free_pct: Option<f64>,
    /// `20%` of RAM or a size.
    pub admit_floor: Option<String>,
    pub temp_sensor: Option<TempSensor>,
}

/// One level's condition: any listed sub-condition holds. A key that is not listed does not take part.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LevelCond {
    pub free_pct_below: Option<f64>,
    pub free_pct_at_most: Option<f64>,
    pub swap_above: Option<String>,
    pub swap_at_least: Option<String>,
    pub pressure_at_least: Option<u32>,
    pub available_below: Option<String>,
    pub psi_full_avg10_above: Option<f64>,
    pub temp_at_least: Option<f64>,
    pub for_samples: Option<u32>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TempSensor {
    pub hwmon: Option<String>,
    pub label: Option<String>,
    pub zone: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectCfg {
    pub registered: Option<String>,
    pub cmdline: Option<String>,
    pub exclude_cmdline: Option<String>,
    pub exe: Option<String>,
    pub cwd_prefix: Option<String>,
    pub user: Option<String>,
    pub cgroup_glob: Option<String>,
    pub parent_cmdline: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtectCfg {
    pub resolve_cmd: Vec<String>,
    pub ttl: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatcherCfg {
    pub name: String,
    pub select: SelectCfg,
    pub tier: u32,
    pub heavy: Option<bool>,
    pub budget: Option<String>,
    #[serde(default)]
    pub pausable: bool,
    pub kill: Option<String>,
    pub unit: Option<String>,
    pub notify: Option<bool>,
    #[serde(default)]
    pub protected: bool,
    pub protect: Option<ProtectCfg>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NeverCfg {
    pub name: String,
    pub select: SelectCfg,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookCfg {
    pub name: String,
    pub on: Vec<String>,
    pub cmd: Vec<String>,
    pub timeout: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LockCfg {
    pub kind: Option<String>,
    pub path: String,
    pub ttl: Option<String>,
    /// `corral` (default), `any` or `never`.
    pub takeover: Option<String>,
    pub holder_file: Option<String>,
    pub reentry_env: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeepAwakeCfg {
    /// `hold-cli` (an external command implementing the hold contract) or `none` (always-on servers).
    pub provider: Option<String>,
    /// The `hold-cli` command; default `awake` on `PATH`. Service environments have a minimal `PATH`, so a deployment
    /// sets an absolute path here.
    pub command: Option<String>,
    /// Work classes that need a working provider: `["heavy"]` refuses heavy work with `keepawake_unavailable` when the
    /// provider probe fails (R-AWAKE-03). Default: none.
    #[serde(default)]
    pub required_for: Vec<String>,
}
