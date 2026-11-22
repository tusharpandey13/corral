//! Levels from samples (GUARDS.md 2.2): the highest level whose condition has held for `for_samples` samples acts;
//! a level clears only after `clear_samples` samples below it (hysteresis).

use crate::sample::Sample;
use corral_proto::guardcfg::{LevelCond, LevelSet};
use corral_proto::util::parse_size_mb;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    Ok,
    Closed,
    Pressure,
    Shed,
    Critical,
    /// The kill switch (not computed from samples).
    Stop,
}

impl Level {
    pub fn name(self) -> &'static str {
        match self {
            Level::Ok => "ok",
            Level::Closed => "closed",
            Level::Pressure => "pressure",
            Level::Shed => "shed",
            Level::Critical => "critical",
            Level::Stop => "stop",
        }
    }
    pub fn number(self) -> u8 {
        self as u8
    }
}

/// One level's resolved condition. Every key that is set is an alternative (any of them holds the level).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Cond {
    pub free_below: Option<f64>,
    pub free_at_most: Option<f64>,
    pub swap_above_mb: Option<u64>,
    pub swap_at_least_mb: Option<u64>,
    pub pressure_at_least: Option<u32>,
    pub avail_below_mb: Option<u64>,
    pub psi_full_above: Option<f64>,
    pub temp_at_least: Option<f64>,
    pub for_samples: u32,
}

/// What made a condition hold.
#[derive(Debug, Clone, PartialEq)]
pub struct Hit {
    pub thermal: bool,
    pub why: String,
}

impl Cond {
    pub fn from_cfg(c: &LevelCond, default_for: u32) -> Result<Cond, String> {
        let size = |k: &str, v: &Option<String>| v.as_deref().map(|s| parse_size_mb(s).map_err(|e| format!("{k}: {e}"))).transpose();
        Ok(Cond {
            free_below: c.free_pct_below,
            free_at_most: c.free_pct_at_most,
            swap_above_mb: size("swap_above", &c.swap_above)?,
            swap_at_least_mb: size("swap_at_least", &c.swap_at_least)?,
            pressure_at_least: c.pressure_at_least,
            avail_below_mb: size("available_below", &c.available_below)?,
            psi_full_above: c.psi_full_avg10_above,
            temp_at_least: c.temp_at_least,
            for_samples: c.for_samples.unwrap_or(default_for).max(1),
        })
    }

    /// Does any sub-condition hold? A reading that is unknown never holds one.
    pub fn eval(&self, s: &Sample) -> Option<Hit> {
        let mut why = Vec::new();
        let mut thermal = false;
        let mut mem = false;
        if let (Some(lim), Some(f)) = (self.free_below, s.free_pct)
            && f < lim
        {
            why.push(format!("free {f:.0}% < {lim}%"));
            mem = true;
        }
        if let (Some(lim), Some(f)) = (self.free_at_most, s.free_pct)
            && f <= lim
        {
            why.push(format!("free {f:.0}% <= {lim}%"));
            mem = true;
        }
        if let (Some(lim), Some(w)) = (self.swap_above_mb, s.swap_mb)
            && w > lim
        {
            why.push(format!("swap {w}MB > {lim}MB"));
            mem = true;
        }
        if let (Some(lim), Some(w)) = (self.swap_at_least_mb, s.swap_mb)
            && w >= lim
        {
            why.push(format!("swap {w}MB >= {lim}MB"));
            mem = true;
        }
        if let (Some(lim), Some(p)) = (self.pressure_at_least, s.pressure)
            && p >= lim
        {
            why.push(format!("pressure {p} >= {lim}"));
            mem = true;
        }
        if let (Some(lim), Some(a)) = (self.avail_below_mb, s.avail_mb)
            && a < lim
        {
            why.push(format!("available {a}MB < {lim}MB"));
            mem = true;
        }
        if let (Some(lim), Some(p)) = (self.psi_full_above, s.psi_full_avg10)
            && p > lim
        {
            why.push(format!("psi full avg10 {p:.1} > {lim}"));
            mem = true;
        }
        if let (Some(lim), Some(t)) = (self.temp_at_least, s.temp_c)
            && t >= lim
        {
            why.push(format!("temp {t:.0}C >= {lim}C"));
            thermal = true;
        }
        (!why.is_empty()).then(|| Hit { thermal: thermal && !mem, why: why.join(", ") })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LevelCfg {
    pub closed: Cond,
    pub pressure: Cond,
    pub shed: Cond,
    pub critical: Cond,
    /// macOS: a unit is admitted only if `free% - ram/total*100` stays at least this (default 23).
    pub admit_floor_free_pct: f64,
    /// Linux: `MemAvailable - ram` must stay at least this many MB; `None` = 20% of RAM.
    pub admit_floor_mb: Option<u64>,
    /// Resume paused work when free memory is back above this (macOS %) or available above this (Linux MB).
    pub resume_free_pct: f64,
    pub resume_avail_mb: u64,
}

fn c(f: impl FnOnce(&mut LevelCond)) -> LevelCond {
    let mut x = LevelCond::default();
    f(&mut x);
    x
}

impl LevelCfg {
    /// The defaults of GUARDS.md 2.2 (the legacy scripts' numbers), overridden by the config file.
    pub fn resolve(macos: bool, over: Option<&LevelSet>) -> Result<LevelCfg, String> {
        let pick = |own: Option<&LevelCond>, def: LevelCond| own.cloned().unwrap_or(def);
        let o = over.cloned().unwrap_or_default();
        let (closed, pressure, shed, critical) = if macos {
            (
                c(|x| {
                    x.free_pct_below = Some(40.0);
                    x.swap_above = Some("4G".into());
                    x.pressure_at_least = Some(2);
                }),
                c(|x| {
                    x.free_pct_at_most = Some(22.0);
                    x.pressure_at_least = Some(2);
                    x.swap_at_least = Some("6G".into());
                    x.for_samples = Some(3);
                }),
                // `pressure_at_least = 4` is the legacy script's own trigger (kernel level "critical").
                c(|x| {
                    x.free_pct_at_most = Some(20.0);
                    x.pressure_at_least = Some(4);
                }),
                c(|x| {
                    x.free_pct_at_most = Some(15.0);
                    x.swap_at_least = Some("7G".into());
                }),
            )
        } else {
            (
                c(|x| {
                    x.available_below = Some("5G".into());
                    x.temp_at_least = Some(85.0);
                }),
                c(|x| {
                    x.available_below = Some("2304M".into());
                    x.psi_full_avg10_above = Some(20.0);
                    x.temp_at_least = Some(92.0);
                    x.for_samples = Some(3);
                }),
                c(|x| {
                    x.available_below = Some("2G".into());
                    x.temp_at_least = Some(95.0);
                }),
                c(|x| {
                    x.available_below = Some("768M".into());
                    x.temp_at_least = Some(99.0);
                }),
            )
        };
        let admit_floor_mb = match o.admit_floor.as_deref() {
            None | Some("20%") => None,
            Some(s) => Some(parse_size_mb(s).map_err(|e| format!("admit_floor: {e}"))?),
        };
        Ok(LevelCfg {
            closed: Cond::from_cfg(&pick(o.closed.as_ref(), closed), 1)?,
            pressure: Cond::from_cfg(&pick(o.pressure.as_ref(), pressure), 3)?,
            shed: Cond::from_cfg(&pick(o.shed.as_ref(), shed), 1)?,
            critical: Cond::from_cfg(&pick(o.critical.as_ref(), critical), 1)?,
            admit_floor_free_pct: o.admit_floor_free_pct.unwrap_or(23.0),
            admit_floor_mb,
            resume_free_pct: 30.0,
            resume_avail_mb: 3072,
        })
    }

    fn conds(&self) -> [&Cond; 4] {
        [&self.closed, &self.pressure, &self.shed, &self.critical]
    }
}

/// The level after one sample, and why.
#[derive(Debug, Clone, PartialEq)]
pub struct LevelInfo {
    pub level: Level,
    /// What this sample alone says (conditions and `for_samples`, no hysteresis). Actions that must stop "as soon as
    /// the level clears" (the shed ladder) look at this, so the lag of the hysteresis never causes another kill.
    pub raw: Level,
    /// Did a thermal condition (and no memory condition) cause it?
    pub thermal: bool,
    pub why: String,
}

pub struct LevelEval {
    cfg: LevelCfg,
    clear_samples: u32,
    streak: [u32; 4],
    cur: Level,
    below: u32,
    last: Option<Hit>,
}

const ORDER: [Level; 4] = [Level::Closed, Level::Pressure, Level::Shed, Level::Critical];

impl LevelEval {
    pub fn new(cfg: LevelCfg, clear_samples: u32) -> LevelEval {
        LevelEval { cfg, clear_samples: clear_samples.max(1), streak: [0; 4], cur: Level::Ok, below: 0, last: None }
    }

    pub fn cfg(&self) -> &LevelCfg {
        &self.cfg
    }

    pub fn level(&self) -> Level {
        self.cur
    }

    pub fn update(&mut self, s: &Sample) -> LevelInfo {
        let mut raw = Level::Ok;
        let mut raw_hit: Option<Hit> = None;
        let mut hits: [Option<Hit>; 4] = [None, None, None, None];
        for (i, cond) in self.cfg.conds().iter().enumerate() {
            hits[i] = cond.eval(s);
            self.streak[i] = if hits[i].is_some() { self.streak[i] + 1 } else { 0 };
            if self.streak[i] >= cond.for_samples {
                raw = ORDER[i];
                raw_hit = hits[i].clone();
            }
        }
        if raw >= self.cur {
            if raw_hit.is_some() {
                self.last = raw_hit;
            }
            self.cur = raw;
            self.below = 0;
        } else {
            self.below += 1;
            if self.below >= self.clear_samples {
                self.cur = raw;
                self.below = 0;
                self.last = raw_hit;
            } else if let Some(h) = hits[self.cur.number() as usize - 1].clone() {
                self.last = Some(h); // still holding, just not for long enough to count as cleared
            }
        }
        if self.cur == Level::Ok {
            self.last = None;
        }
        let (thermal, why) = self.last.as_ref().map(|h| (h.thermal, h.why.clone())).unwrap_or((false, String::new()));
        LevelInfo { level: self.cur, raw, thermal, why }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mac_sample(free: f64) -> Sample {
        let total = 16_000u64;
        Sample {
            free_pct: Some(free),
            total_mb: Some(total),
            avail_mb: Some((total as f64 * free / 100.0) as u64),
            swap_mb: Some(0),
            pressure: Some(1),
            ..Sample::default()
        }
    }

    fn eval() -> LevelEval {
        LevelEval::new(LevelCfg::resolve(true, None).unwrap(), 3)
    }

    /// U-04: each level from injected sequences.
    #[test]
    fn macos_levels_in_order() {
        let mut e = eval();
        assert_eq!(e.update(&mac_sample(60.0)).level, Level::Ok);
        assert_eq!(e.update(&mac_sample(39.0)).level, Level::Closed);
        // pressure needs three samples at <= 22%
        assert_eq!(e.update(&mac_sample(21.5)).level, Level::Closed);
        assert_eq!(e.update(&mac_sample(21.5)).level, Level::Closed);
        assert_eq!(e.update(&mac_sample(21.5)).level, Level::Pressure);
        assert_eq!(e.update(&mac_sample(20.0)).level, Level::Shed);
        assert_eq!(e.update(&mac_sample(15.0)).level, Level::Critical);
    }

    /// R-GUARD-09: crossing the ceiling is acted on at the very next sample (<= 2 s), well inside the 2-sample bound.
    #[test]
    fn shed_acts_at_the_first_sample_over_the_ceiling() {
        let mut e = eval();
        assert_eq!(e.update(&mac_sample(30.0)).level, Level::Closed);
        let second = e.update(&mac_sample(19.0));
        assert_eq!(second.level, Level::Shed);
        assert!(second.why.contains("free 19% <= 20%"), "{}", second.why);
    }

    /// U-04 hysteresis: a level clears only after `clear_samples` samples below it.
    #[test]
    fn levels_clear_after_three_samples() {
        let mut e = eval();
        e.update(&mac_sample(19.0));
        assert_eq!(e.level(), Level::Shed);
        assert_eq!(e.update(&mac_sample(50.0)).level, Level::Shed);
        assert_eq!(e.update(&mac_sample(50.0)).level, Level::Shed);
        assert_eq!(e.update(&mac_sample(50.0)).level, Level::Ok);
        // a relapse during clearing restarts the count
        e.update(&mac_sample(19.0));
        e.update(&mac_sample(50.0));
        e.update(&mac_sample(19.0));
        assert_eq!(e.update(&mac_sample(50.0)).level, Level::Shed);
        assert_eq!(e.update(&mac_sample(50.0)).level, Level::Shed);
        assert_eq!(e.update(&mac_sample(50.0)).level, Level::Ok);
    }

    #[test]
    fn swap_and_pressure_conditions() {
        let mut e = eval();
        let mut s = mac_sample(60.0);
        s.swap_mb = Some(7168);
        assert_eq!(e.update(&s).level, Level::Critical, "swap >= 7G is critical");
        let mut e = eval();
        let mut s = mac_sample(60.0);
        s.pressure = Some(4);
        assert_eq!(e.update(&s).level, Level::Shed, "kernel pressure critical acts, as the legacy script does");
        let mut e = eval();
        s.pressure = Some(2);
        assert_eq!(e.update(&s).level, Level::Closed);
    }

    #[test]
    fn linux_levels_and_thermal_reason() {
        let mut e = LevelEval::new(LevelCfg::resolve(false, None).unwrap(), 3);
        let s = |avail: u64, temp: f64| Sample { avail_mb: Some(avail), total_mb: Some(16_000), temp_c: Some(temp), ..Sample::default() };
        assert_eq!(e.update(&s(8000, 60.0)).level, Level::Ok);
        assert_eq!(e.update(&s(4000, 60.0)).level, Level::Closed);
        let hot = e.update(&s(8000, 95.0));
        assert_eq!((hot.level, hot.thermal), (Level::Shed, true));
        let mem = e.update(&s(700, 60.0));
        assert_eq!((mem.level, mem.thermal), (Level::Critical, false));
    }

    #[test]
    fn unknown_readings_never_trigger_a_level() {
        let mut e = eval();
        assert_eq!(e.update(&Sample::default()).level, Level::Ok);
    }

    #[test]
    fn config_overrides_defaults() {
        let set = LevelSet {
            shed: Some(LevelCond { free_pct_at_most: Some(10.0), ..LevelCond::default() }),
            critical: Some(LevelCond { free_pct_at_most: Some(5.0), ..LevelCond::default() }),
            ..LevelSet::default()
        };
        let mut e = LevelEval::new(LevelCfg::resolve(true, Some(&set)).unwrap(), 3);
        assert_eq!(e.update(&mac_sample(19.0)).level, Level::Closed, "19% is no longer shed");
        assert_eq!(e.update(&mac_sample(9.0)).level, Level::Shed);
    }
}
