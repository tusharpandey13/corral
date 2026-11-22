//! Admission checks (GUARDS.md 4, 7): "may this start now, and if not, why". Pure over a context the guard fills in
//! from its latest sample, so every check is tested with fake values. Admission answers are real in observe mode
//! too (a caller may rely on them); only signals are withheld.

use crate::levels::{Level, LevelCfg};
use crate::procs::Unit;
use crate::sample::Sample;
use crate::state::StopState;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CheckResult {
    pub check: String,
    pub ok: bool,
    /// A short human string with the measured value and the threshold.
    pub reason: String,
    /// When the check started failing (unix ms), filled in by the guard.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since_ms: Option<i64>,
}

fn pass(check: &str, reason: impl Into<String>) -> CheckResult {
    CheckResult { check: check.into(), ok: true, reason: reason.into(), since_ms: None }
}
fn fail(check: &str, reason: impl Into<String>) -> CheckResult {
    CheckResult { check: check.into(), ok: false, reason: reason.into(), since_ms: None }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Class {
    Heavy,
    Light,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AdmitReq {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub ram_mb: u64,
    #[serde(default = "heavy")]
    pub class: Class,
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default)]
    pub reserve_s: u64,
    /// Re-check under a lock: evaluate as if this reservation were not holding RAM and a slot.
    #[serde(default)]
    pub reservation: Option<String>,
}

fn heavy() -> Class {
    Class::Heavy
}

#[derive(Debug, Clone, PartialEq)]
pub struct Reservation {
    pub token: String,
    pub name: String,
    pub ram_mb: u64,
    pub heavy: bool,
    pub until_ms: i64,
}

pub struct Ctx<'a> {
    pub macos: bool,
    pub sample: &'a Sample,
    pub level: Level,
    pub level_why: &'a str,
    pub levels: &'a LevelCfg,
    pub stop: Option<&'a StopState>,
    pub units: &'a [Unit],
    pub reservations: &'a [Reservation],
    pub heavy_slots: u32,
    pub disk: Result<u64, String>,
    pub min_disk_mb: u64,
    pub admit_max_c: f64,
    /// Stage-1 idle threshold of the `measurement` profile.
    pub idle_need_s: u64,
}

impl Ctx<'_> {
    fn reserved_mb(&self, except: Option<&str>) -> u64 {
        self.reservations.iter().filter(|r| Some(r.token.as_str()) != except).map(|r| r.ram_mb).sum()
    }
}

pub fn kill_switch_off(c: &Ctx<'_>) -> CheckResult {
    match c.stop {
        Some(s) => fail("kill_switch_off", s.describe()),
        None => pass("kill_switch_off", "kill switch off"),
    }
}

/// Level below `closed` (below `shed` for light work) and the projection `free - ram - reserved >= floor`.
pub fn memory(c: &Ctx<'_>, ram_mb: u64, light: bool, except: Option<&str>) -> CheckResult {
    let s = c.sample;
    let (Some(total), Some(avail), Some(free)) = (s.total_mb, s.avail_mb, s.free_pct) else {
        return fail("memory", format!("unknown (memory probe failed: {})", s.errors.join("; ")));
    };
    let limit = if light { Level::Shed } else { Level::Closed };
    if c.level >= limit {
        return fail("memory", format!("level {}: {} ({})", c.level.name(), c.level_why, s.brief()));
    }
    let reserved = c.reserved_mb(except);
    let need = ram_mb + reserved;
    if c.macos {
        let floor = if light { 20.0 } else { c.levels.admit_floor_free_pct };
        let projected = free - need as f64 * 100.0 / total as f64;
        if projected < floor {
            return fail(
                "memory",
                format!(
                    "projected free {projected:.0}% after {need}MB (this {ram_mb}MB + reserved {reserved}MB) < {floor}% (free now {free:.0}%)"
                ),
            );
        }
        pass("memory", format!("projected free {projected:.0}% >= {floor}%"))
    } else {
        let floor = c.levels.admit_floor_mb.unwrap_or(total / 5);
        let projected = avail.saturating_sub(need);
        if avail < need + floor {
            return fail(
                "memory",
                format!("MemAvailable {avail}MB - {need}MB (this {ram_mb}MB + reserved {reserved}MB) leaves {projected}MB < {floor}MB"),
            );
        }
        pass("memory", format!("projected available {projected}MB >= {floor}MB"))
    }
}

pub fn heavy_slot(c: &Ctx<'_>, except: Option<&str>) -> CheckResult {
    let running: Vec<&Unit> = c.units.iter().filter(|u| u.heavy && !u.has_never).collect();
    let reserved = c.reservations.iter().filter(|r| r.heavy && Some(r.token.as_str()) != except).count();
    let used = running.len() + reserved;
    if used as u32 >= c.heavy_slots {
        let who: Vec<String> = running
            .iter()
            .map(|u| format!("{} tier {}", u.comm, u.tier))
            .chain(c.reservations.iter().filter(|r| r.heavy && Some(r.token.as_str()) != except).map(|r| format!("reserved by {}", r.name)))
            .collect();
        return fail("heavy_slot", format!("{used} of {} heavy slots in use ({})", c.heavy_slots, who.join(", ")));
    }
    pass("heavy_slot", format!("{used} of {} heavy slots in use", c.heavy_slots))
}

pub fn thermal_ok(c: &Ctx<'_>) -> CheckResult {
    let s = c.sample;
    if let Some(st) = &s.thermal_state {
        return if matches!(st.as_str(), "serious" | "critical") {
            fail("thermal_ok", format!("thermal state {st}"))
        } else {
            pass("thermal_ok", format!("thermal state {st}"))
        };
    }
    match s.temp_c {
        Some(t) if t >= c.admit_max_c => fail("thermal_ok", format!("{t:.0}C >= {}C", c.admit_max_c)),
        Some(t) => pass("thermal_ok", format!("{t:.0}C < {}C", c.admit_max_c)),
        None => pass("thermal_ok", "unknown (no thermal reading)"),
    }
}

pub fn disk_free(c: &Ctx<'_>) -> CheckResult {
    match &c.disk {
        Ok(free) if *free >= c.min_disk_mb => pass("disk_free", format!("{free}MB free >= {}MB", c.min_disk_mb)),
        Ok(free) => fail("disk_free", format!("{free}MB free < {}MB", c.min_disk_mb)),
        Err(e) => fail("disk_free", format!("unknown ({e})")),
    }
}

pub fn ac_power(c: &Ctx<'_>) -> CheckResult {
    match c.sample.on_ac {
        Some(false) => fail("ac_power", "on battery"),
        Some(true) => pass("ac_power", "on AC power"),
        None => pass("ac_power", "unknown or no battery"),
    }
}

/// Stage 1 of the idle check; stage 2 and the synthetic-input exemption arrive with M4 (R-ADM-07).
pub fn user_idle(c: &Ctx<'_>) -> CheckResult {
    match c.sample.idle_s {
        Some(i) if i >= c.idle_need_s => pass("user_idle", format!("idle {i}s >= {}s", c.idle_need_s)),
        Some(i) => fail("user_idle", format!("idle {i}s (need {}s)", c.idle_need_s)),
        None => fail("user_idle", "unknown (idle probe failed)"),
    }
}

/// The checks of the `default` profile for a class, in order (the first failure is the headline).
pub fn default_profile(c: &Ctx<'_>, req: &AdmitReq) -> Vec<CheckResult> {
    let except = req.reservation.as_deref();
    match req.class {
        Class::Heavy => {
            vec![kill_switch_off(c), memory(c, req.ram_mb, false, except), heavy_slot(c, except), thermal_ok(c), disk_free(c), ac_power(c)]
        }
        Class::Light => vec![kill_switch_off(c), memory(c, req.ram_mb, true, except)],
    }
}

/// Every implemented check, for `corral guard checks` / status (profile `all`).
pub fn all_checks(c: &Ctx<'_>) -> Vec<CheckResult> {
    vec![kill_switch_off(c), memory(c, 0, false, None), heavy_slot(c, None), thermal_ok(c), disk_free(c), ac_power(c), user_idle(c)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::tests::unit;
    use crate::levels::LevelCfg;

    struct World {
        sample: Sample,
        level: Level,
        levels: LevelCfg,
        units: Vec<Unit>,
        res: Vec<Reservation>,
        stop: Option<StopState>,
        disk: Result<u64, String>,
    }
    impl World {
        fn mac(free: f64) -> World {
            World {
                sample: Sample {
                    free_pct: Some(free),
                    total_mb: Some(16_000),
                    avail_mb: Some((160.0 * free) as u64),
                    swap_mb: Some(0),
                    pressure: Some(1),
                    on_ac: Some(true),
                    ..Sample::default()
                },
                level: Level::Ok,
                levels: LevelCfg::resolve(true, None).unwrap(),
                units: vec![],
                res: vec![],
                stop: None,
                disk: Ok(100_000),
            }
        }
        fn ctx(&self, macos: bool) -> Ctx<'_> {
            Ctx {
                macos,
                sample: &self.sample,
                level: self.level,
                level_why: "test",
                levels: &self.levels,
                stop: self.stop.as_ref(),
                units: &self.units,
                reservations: &self.res,
                heavy_slots: 1,
                disk: self.disk.clone(),
                min_disk_mb: 15_360,
                admit_max_c: 85.0,
                idle_need_s: 300,
            }
        }
    }
    fn req(ram: u64) -> AdmitReq {
        AdmitReq { name: Some("t".into()), ram_mb: ram, class: Class::Heavy, profile: None, reserve_s: 0, reservation: None }
    }
    fn failing(r: &[CheckResult]) -> Vec<&str> {
        r.iter().filter(|c| !c.ok).map(|c| c.check.as_str()).collect()
    }

    /// R-ADM-02: the projection admits and refuses exactly at the boundary (23% floor on macOS).
    #[test]
    fn projection_boundary_macos() {
        // 16000 MB total: 1600 MB is exactly 10 points. free 33 - 10 = 23 -> ok; free 32.9 -> refused.
        let w = World::mac(33.0);
        assert!(failing(&default_profile(&w.ctx(true), &req(1600))).is_empty());
        let w = World::mac(32.9);
        let r = default_profile(&w.ctx(true), &req(1600));
        assert_eq!(failing(&r), vec!["memory"], "{r:?}");
        assert!(r[1].reason.contains("projected free"), "{}", r[1].reason);
    }

    #[test]
    fn projection_boundary_linux_default_floor_is_20_percent() {
        let mut w = World::mac(0.0);
        w.sample = Sample::mem(0, 10_000, 6_000, 0); // floor = 2000
        w.levels = LevelCfg::resolve(false, None).unwrap();
        assert!(failing(&default_profile(&w.ctx(false), &req(4000))).is_empty(), "6000 - 4000 = 2000 >= 2000");
        assert_eq!(failing(&default_profile(&w.ctx(false), &req(4001))), vec!["memory"]);
    }

    #[test]
    fn a_closed_level_refuses_with_its_reason_first() {
        let mut w = World::mac(35.0);
        w.level = Level::Closed;
        let r = default_profile(&w.ctx(true), &req(0));
        assert_eq!(r[1].check, "memory");
        assert!(!r[1].ok && r[1].reason.contains("level closed"), "{r:?}");
    }

    #[test]
    fn kill_switch_wins_and_names_who() {
        let mut w = World::mac(80.0);
        w.stop = Some(StopState { since_ms: 0, by: "owner".into(), reason: "drill".into() });
        let r = default_profile(&w.ctx(true), &req(100));
        assert_eq!(failing(&r), vec!["kill_switch_off"]);
        assert!(r[0].reason.contains("by owner: drill"));
    }

    /// Counting heavy slots: live matches, reservations; never-matched processes are not heavy work.
    #[test]
    fn heavy_slot_counts_units_and_reservations() {
        let mut w = World::mac(80.0);
        assert!(heavy_slot(&w.ctx(true), None).ok);
        w.units = vec![unit("g10", 1, 100)];
        let r = heavy_slot(&w.ctx(true), None);
        assert!(!r.ok && r.reason.contains("1 of 1"), "{r:?}");
        w.units[0].has_never = true;
        assert!(heavy_slot(&w.ctx(true), None).ok, "a never-matched unit (the owner's own work) is not counted");
        w.units.clear();
        w.res = vec![Reservation { token: "tok".into(), name: "other".into(), ram_mb: 10, heavy: true, until_ms: 1 }];
        assert!(!heavy_slot(&w.ctx(true), None).ok);
        assert!(heavy_slot(&w.ctx(true), Some("tok")).ok, "the re-check under a lock ignores the caller's own reservation");
        let mut l = req(10);
        l.class = Class::Light;
        assert_eq!(default_profile(&w.ctx(true), &l).len(), 2, "light work: kill switch and memory only");
    }

    #[test]
    fn thermal_disk_and_power() {
        let mut w = World::mac(80.0);
        w.sample.thermal_state = Some("serious".into());
        assert!(!thermal_ok(&w.ctx(true)).ok);
        w.sample.thermal_state = None;
        w.sample.temp_c = Some(85.0);
        assert!(!thermal_ok(&w.ctx(false)).ok);
        w.sample.temp_c = None;
        assert!(thermal_ok(&w.ctx(false)).ok, "on_unknown = pass");
        w.disk = Err("statvfs failed".into());
        assert!(!disk_free(&w.ctx(true)).ok, "on_unknown = fail");
        w.disk = Ok(1000);
        assert!(disk_free(&w.ctx(true)).reason.contains("< 15360MB"));
        w.sample.on_ac = Some(false);
        assert!(!ac_power(&w.ctx(true)).ok);
        w.sample.on_ac = None;
        assert!(ac_power(&w.ctx(true)).ok, "a desktop has no battery");
        w.sample.idle_s = Some(40);
        assert_eq!(user_idle(&w.ctx(true)).reason, "idle 40s (need 300s)");
        w.sample.idle_s = None;
        assert!(!user_idle(&w.ctx(true)).ok);
    }

    #[test]
    fn unknown_memory_fails_closed() {
        let mut w = World::mac(80.0);
        w.sample = Sample { errors: vec!["memory: probe failed".into()], ..Sample::default() };
        let r = memory(&w.ctx(true), 100, false, None);
        assert!(!r.ok && r.reason.contains("unknown"));
    }
}
