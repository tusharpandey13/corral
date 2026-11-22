//! The decision engine (GUARDS.md 2-3). One call per sample: level from the sample, then what the guard would do to
//! the units it watches. It only *plans*: the result is a list of decisions. Nothing here (or anywhere in this
//! crate) sends a signal, so observe mode cannot change a process (R-GUARD-10; `tests/no_signals.rs`).
//!
//! Because observe mode does not end anything, the engine keeps a *virtual* state: a unit it has decided to kill is
//! treated as gone for the rest of the episode, so the next tier's decision comes after `settle` exactly as it would
//! if the kill had worked and the level had not dropped. The `detail` of such a decision says so.

use crate::decisions::Decision;
use crate::levels::{Level, LevelEval};
use crate::procs::Unit;
use crate::sample::Sample;
use crate::state::StopState;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct HookRef {
    pub name: String,
    pub on: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct EngineCfg {
    pub macos: bool,
    pub settle_ms: i64,
    pub pause_max_ms: i64,
    pub pause_max_thermal_ms: i64,
    pub budget_slack: f64,
    pub hooks: Vec<HookRef>,
    /// Enforce mode: kills are really sent, so a unit that survives them is tried again after `retry_ms`.
    pub enforce: bool,
    pub retry_ms: i64,
}

pub struct Engine {
    cfg: EngineCfg,
    eval: LevelEval,
    level: Level,
    shed_wait_until: i64,
    /// Units the guard has decided to kill: key -> (episodic, since). Episodic ones are forgotten when the episode ends.
    dead: HashMap<String, bool>,
    dead_at: HashMap<String, i64>,
    paused: HashMap<String, (i64, bool)>,
    noted: HashSet<String>,
    hooks_ran: HashSet<String>,
    why: String,
}

pub struct Tick<'a> {
    pub now_ms: i64,
    pub sample: &'a Sample,
    /// `None` = the process table could not be read.
    pub units: Option<&'a [Unit]>,
    pub stop: Option<&'a StopState>,
}

fn reason_of(thermal: bool) -> &'static str {
    if thermal { "thermal" } else { "system_memory" }
}

impl Engine {
    pub fn new(cfg: EngineCfg, eval: LevelEval) -> Engine {
        Engine {
            cfg,
            eval,
            level: Level::Ok,
            shed_wait_until: 0,
            dead: HashMap::new(),
            dead_at: HashMap::new(),
            paused: HashMap::new(),
            noted: HashSet::new(),
            hooks_ran: HashSet::new(),
            why: String::new(),
        }
    }

    /// The level of the last tick, the kill switch included.
    pub fn level(&self) -> Level {
        self.level
    }

    /// The level the samples alone say (no kill switch), always current with the state of the last sample.
    pub fn sample_level(&self) -> Level {
        self.eval.level()
    }

    /// What holds the sample level (empty at `ok`).
    pub fn why(&self) -> &str {
        &self.why
    }

    pub fn levels(&self) -> &crate::levels::LevelCfg {
        self.eval.cfg()
    }

    pub fn paused_keys(&self) -> Vec<String> {
        let mut v: Vec<String> = self.paused.keys().cloned().collect();
        v.sort();
        v
    }

    fn unit_decision(&self, now: i64, kind: &str, u: &Unit, reason: &str, detail: String) -> Decision {
        let mut d = Decision::new(now, kind, self.level.name(), reason, detail);
        d.tier = Some(u.tier);
        d.matcher = Some(u.matcher.clone());
        d.unit = Some(u.reg_name.clone().unwrap_or_else(|| u.key.clone()));
        d.pids = u.pids.clone();
        d.comm = Some(u.comm.clone());
        d.key = Some(u.key.clone());
        d
    }

    fn note_once(&mut self, key: String, now: i64, kind: &str, reason: &str, detail: String, out: &mut Vec<Decision>) {
        if self.noted.insert(key) {
            out.push(Decision::new(now, kind, self.level.name(), reason, detail));
        }
    }

    fn hooks(&mut self, now: i64, on: &str, out: &mut Vec<Decision>) {
        for h in self.cfg.hooks.iter().filter(|h| h.on.iter().any(|x| x == on)) {
            if self.hooks_ran.insert(format!("{on}:{}", h.name)) || on == "shed" {
                let mut d = Decision::new(now, "would_hook", on, "restore_hook", format!("run restore hook {:?} (after {on})", h.name));
                d.key = Some(h.name.clone());
                out.push(d);
            }
        }
    }

    fn mark_dead(&mut self, key: &str, episodic: bool, now: i64) {
        self.dead.insert(key.to_string(), episodic);
        self.dead_at.insert(key.to_string(), now);
    }

    fn resume_ok(&self, s: &Sample) -> bool {
        let c = self.eval.cfg();
        // A reading that is missing is not pressure: the caller already requires the level to be below `pressure`, and a
        // unit must not be killed at `pause_max` only because a probe could not be read (note 43).
        if self.cfg.macos {
            s.free_pct.is_none_or(|f| f >= c.resume_free_pct) && s.pressure.is_none_or(|p| p <= 1)
        } else {
            s.avail_mb.is_none_or(|a| a >= c.resume_avail_mb)
        }
    }

    pub fn tick(&mut self, t: Tick<'_>) -> Vec<Decision> {
        let mut out = Vec::new();
        let now = t.now_ms;
        let info = self.eval.update(t.sample);
        let (level, thermal, why) = match t.stop {
            Some(s) => (Level::Stop, false, s.describe()),
            None => (info.level, info.thermal, info.why.clone()),
        };
        // Kills and pauses follow what this very sample says: when memory has recovered the ladder stops at once, even
        // though the reported level clears only after `clear_samples` (GUARDS.md 2.2: "stop as soon as the level
        // drops below shed").
        let acting = if t.stop.is_some() { Level::Stop } else { info.raw };
        let prev = self.level;
        self.level = level;
        self.why = if info.level == Level::Ok { String::new() } else { info.why.clone() };
        if level != prev {
            out.push(Decision::new(
                now,
                "level",
                level.name(),
                if level > prev { "level_up" } else { "level_down" },
                format!("{} -> {}: {} ({})", prev.name(), level.name(), why, t.sample.brief()),
            ));
        }
        if level < Level::Shed {
            // the episode is over: forget episodic decisions and one-shot notes
            self.dead.retain(|_, episodic| !*episodic);
            self.hooks_ran.clear();
            self.shed_wait_until = 0;
        }
        if level < Level::Pressure {
            self.noted.clear();
        }
        let ctx = format!("level={} {} [{}]", level.name(), why, t.sample.brief());
        let Some(units) = t.units else {
            if level >= Level::Pressure {
                self.note_once(
                    "table".into(),
                    now,
                    "skipped",
                    "process_table_unreadable",
                    "no decision on units: the process table could not be read".into(),
                    &mut out,
                );
            }
            return out;
        };
        let live: HashSet<&str> = units.iter().map(|u| u.key.as_str()).collect();
        if self.cfg.enforce {
            // a unit still alive long after its kill was sent is tried again (the kill did not take)
            let retry = self.cfg.retry_ms;
            let stale: Vec<String> =
                self.dead_at.iter().filter(|(k, at)| now - **at > retry && live.contains(k.as_str())).map(|(k, _)| k.clone()).collect();
            for k in stale {
                self.dead.remove(&k);
                self.dead_at.remove(&k);
            }
        }
        self.dead.retain(|k, _| live.contains(k.as_str()));
        self.dead_at.retain(|k, _| self.dead.contains_key(k));
        self.paused.retain(|k, _| live.contains(k.as_str()));

        // per-registration budgets, whatever the level (R-GUARD-06)
        let over: Vec<&Unit> = units.iter().filter(|u| !u.has_never && !self.dead.contains_key(&u.key)).collect();
        for u in over {
            if let Some(b) = u.budget_mb
                && u.footprint_mb as f64 > b as f64 * self.cfg.budget_slack
            {
                out.push(self.unit_decision(
                    now,
                    "would_kill",
                    u,
                    "ram_exceeded",
                    format!("footprint {}MB > budget {b}MB x {}", u.footprint_mb, self.cfg.budget_slack),
                ));
                self.mark_dead(&u.key, false, now);
                self.paused.remove(&u.key);
            }
        }
        for u in units.iter().filter(|u| u.has_never) {
            if level >= Level::Shed {
                self.note_once(
                    format!("never:{}", u.key),
                    now,
                    "skipped",
                    "never",
                    format!("{} contains a never process; it is not signalled", u.matcher),
                    &mut out,
                );
            }
        }
        let candidates = |me: &Engine| -> Vec<&Unit> { units.iter().filter(|u| !u.has_never && !me.dead.contains_key(&u.key)).collect() };

        match acting {
            Level::Stop | Level::Critical => {
                let reason = if acting == Level::Stop { "kill_switch" } else { reason_of(thermal) };
                let victims: Vec<&Unit> = candidates(self);
                if acting == Level::Critical || !victims.is_empty() {
                    self.hooks(now, acting.name(), &mut out);
                }
                for u in victims {
                    out.push(self.unit_decision(
                        now,
                        "would_kill",
                        u,
                        reason,
                        format!("{ctx}; every watched unit goes, protected included"),
                    ));
                    self.mark_dead(&u.key, true, now);
                    self.paused.remove(&u.key);
                }
            }
            Level::Shed => {
                if now >= self.shed_wait_until {
                    let pool: Vec<&Unit> = candidates(self);
                    let mut skipped = Vec::new();
                    let eligible: Vec<&Unit> = pool
                        .into_iter()
                        .filter(|u| {
                            if u.protected {
                                return false;
                            }
                            if u.protect_unresolved {
                                skipped.push(u.matcher.clone());
                                return false;
                            }
                            true
                        })
                        .collect();
                    for m in skipped {
                        // R-GUARD-03: a protected set that cannot be resolved means shed kills nothing from this matcher
                        self.note_once(
                            format!("unresolved:{m}"),
                            now,
                            "skipped",
                            "protected_set_unresolved",
                            format!("shed skipped: protected set unresolved for matcher {m}"),
                            &mut out,
                        );
                    }
                    if let Some(tier) = eligible.iter().map(|u| u.tier).min() {
                        let reason = reason_of(thermal);
                        for u in eligible.iter().filter(|u| u.tier == tier) {
                            let assumed = if self.shed_wait_until > 0 && !self.cfg.enforce {
                                "; assuming the previous tier's kill took effect (observe mode cannot confirm it)"
                            } else {
                                ""
                            };
                            out.push(self.unit_decision(
                                now,
                                "would_kill",
                                u,
                                reason,
                                format!("{ctx}; tier {tier} of the shed ladder{assumed}"),
                            ));
                            self.mark_dead(&u.key, true, now);
                            self.paused.remove(&u.key);
                        }
                        self.shed_wait_until = now + self.cfg.settle_ms;
                        self.hooks(now, "shed", &mut out);
                    }
                }
            }
            Level::Pressure => {
                let to_pause: Vec<&Unit> = candidates(self)
                    .into_iter()
                    // a protected (or unresolved-protected) unit dies only at critical or stop: it is not paused either, or
                    // pause_max would turn the pause into a kill at pressure level (R-GUARD-03)
                    .filter(|u| u.pausable && !u.protected && !u.protect_unresolved && !self.paused.contains_key(&u.key))
                    .collect();
                for u in to_pause {
                    out.push(self.unit_decision(
                        now,
                        "would_pause",
                        u,
                        reason_of(thermal),
                        format!("{ctx}; pausable unit paused instead of killed"),
                    ));
                    self.paused.insert(u.key.clone(), (now, thermal));
                }
            }
            Level::Closed | Level::Ok => {}
        }

        // paused units: resume when clear, or escalate to a kill after pause_max
        let mut resumed = Vec::new();
        let mut escalated = Vec::new();
        for (key, (since, th)) in &self.paused {
            let max = if *th { self.cfg.pause_max_thermal_ms } else { self.cfg.pause_max_ms };
            if level < Level::Pressure && self.resume_ok(t.sample) {
                resumed.push(key.clone());
            } else if now - since > max {
                escalated.push((key.clone(), *th));
            }
        }
        for key in resumed {
            if let Some(u) = units.iter().find(|u| u.key == key) {
                out.push(self.unit_decision(now, "would_resume", u, "memory_clear", format!("{ctx}; resumed")));
            }
            self.paused.remove(&key);
        }
        for (key, th) in escalated {
            if let Some(u) = units.iter().find(|u| u.key == key && (u.protected || u.protect_unresolved) && !u.has_never) {
                // protection can appear after the pause (a resolver answered late): continue it, never kill it here
                out.push(self.unit_decision(
                    now,
                    "would_resume",
                    u,
                    "protected",
                    format!("{ctx}; paused longer than pause_max but protected: resumed, not killed"),
                ));
            } else if let Some(u) = units.iter().find(|u| u.key == key && !u.has_never) {
                out.push(self.unit_decision(
                    now,
                    "would_kill",
                    u,
                    reason_of(th),
                    format!("{ctx}; paused longer than pause_max, escalated to a kill"),
                ));
                self.mark_dead(&key, true, now);
            }
            self.paused.remove(&key);
        }
        out
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::levels::{LevelCfg, LevelEval};
    use crate::procs::KillKind;

    pub fn unit(key: &str, tier: u32, footprint_mb: u64) -> Unit {
        Unit {
            key: key.into(),
            matcher: format!("m{tier}"),
            tier,
            heavy: true,
            pausable: false,
            kill: KillKind::Group,
            budget_mb: None,
            notify: true,
            protected: false,
            protect_unresolved: false,
            has_never: false,
            reg_id: None,
            reg_name: None,
            leader: key.trim_start_matches(|c: char| !c.is_ascii_digit()).parse().unwrap_or(1),
            pids: vec![key.trim_start_matches(|c: char| !c.is_ascii_digit()).parse().unwrap_or(1)],
            starts: vec![],
            pgid: None,
            comm: "prog".into(),
            footprint_mb,
        }
    }

    fn engine() -> Engine {
        Engine::new(
            EngineCfg {
                macos: true,
                settle_ms: 8_000,
                pause_max_ms: 60_000,
                pause_max_thermal_ms: 600_000,
                budget_slack: 1.1,
                hooks: vec![HookRef { name: "restore".into(), on: vec!["critical".into(), "stop".into()] }],
                enforce: false,
                retry_ms: 20_000,
            },
            LevelEval::new(LevelCfg::resolve(true, None).unwrap(), 3),
        )
    }

    fn free(pct: f64) -> Sample {
        Sample {
            free_pct: Some(pct),
            total_mb: Some(16_000),
            avail_mb: Some((160.0 * pct) as u64),
            swap_mb: Some(0),
            pressure: Some(1),
            ..Sample::default()
        }
    }

    fn kills(d: &[Decision]) -> Vec<(u32, String)> {
        d.iter().filter(|d| d.kind == "would_kill").map(|d| (d.tier.unwrap(), d.reason.clone())).collect()
    }

    /// R-GUARD-02 / U-04: tier by tier, lowest first, with a settle period between tiers; the ladder stops as soon as
    /// the level clears (tier 1 is never touched when tier 0 was enough).
    #[test]
    fn shed_goes_one_tier_at_a_time_and_stops_when_clear() {
        let units = vec![unit("g10", 0, 100), unit("g11", 1, 100), unit("g12", 2, 100)];
        let mut e = engine();
        let d = e.tick(Tick { now_ms: 0, sample: &free(19.0), units: Some(&units), stop: None });
        assert_eq!(kills(&d), vec![(0, "system_memory".into())]);
        // inside the settle window nothing more happens, even though the level still holds
        assert!(kills(&e.tick(Tick { now_ms: 2_000, sample: &free(19.0), units: Some(&units), stop: None })).is_empty());
        // memory recovered after tier 0 went: tier 1 is untouched
        let after = vec![unit("g11", 1, 100), unit("g12", 2, 100)];
        for i in 0..4 {
            let d = e.tick(Tick { now_ms: 10_000 + i * 2_000, sample: &free(35.0), units: Some(&after), stop: None });
            assert!(kills(&d).is_empty(), "{d:?}");
        }
    }

    #[test]
    fn shed_continues_with_the_next_tier_after_settle_if_the_level_holds() {
        let units = vec![unit("g10", 0, 100), unit("g11", 1, 100), unit("g12", 2, 100)];
        let mut e = engine();
        let mut seen = Vec::new();
        for i in 0..10 {
            let d = e.tick(Tick { now_ms: i * 2_000, sample: &free(19.0), units: Some(&units), stop: None });
            seen.extend(kills(&d).into_iter().map(|(t, _)| (i * 2_000, t)));
        }
        assert_eq!(seen, vec![(0, 0), (8_000, 1), (16_000, 2)]);
        let d = e.tick(Tick { now_ms: 18_000, sample: &free(19.0), units: Some(&units), stop: None });
        assert!(d.is_empty(), "nothing left, nothing logged twice: {d:?}");
    }

    /// R-GUARD-03 / U-05: never and protected.
    #[test]
    fn never_is_never_touched_protected_only_at_critical() {
        let mut never = unit("g10", 0, 100);
        never.has_never = true;
        let mut prot = unit("g11", 1, 100);
        prot.protected = true;
        let plain = unit("g12", 2, 100);
        let units = vec![never, prot, plain];
        let mut e = engine();
        let d = e.tick(Tick { now_ms: 0, sample: &free(19.0), units: Some(&units), stop: None });
        assert_eq!(kills(&d), vec![(2, "system_memory".into())], "the protected tier 1 is skipped, tier 2 goes");
        let d = e.tick(Tick { now_ms: 2_000, sample: &free(10.0), units: Some(&units), stop: None });
        let k: Vec<u32> = d.iter().filter(|d| d.kind == "would_kill").flat_map(|d| d.pids.clone()).collect();
        assert_eq!(k, vec![11], "critical takes the protected unit too, never the never unit");
        assert!(d.iter().any(|d| d.kind == "would_hook"), "restore hook planned at critical");
    }

    /// R-GUARD-03: an unresolvable protected set means shed kills nothing from that matcher; critical still applies.
    #[test]
    fn unresolved_protected_set_fails_safe() {
        let mut a = unit("g10", 1, 100);
        a.protect_unresolved = true;
        let units = vec![a];
        let mut e = engine();
        let d = e.tick(Tick { now_ms: 0, sample: &free(19.0), units: Some(&units), stop: None });
        assert!(kills(&d).is_empty());
        assert!(d.iter().any(|d| d.kind == "skipped" && d.detail.contains("protected set unresolved")), "{d:?}");
        let d = e.tick(Tick { now_ms: 2_000, sample: &free(19.0), units: Some(&units), stop: None });
        assert!(d.is_empty(), "the note is logged once per episode");
        let d = e.tick(Tick { now_ms: 4_000, sample: &free(10.0), units: Some(&units), stop: None });
        assert_eq!(kills(&d).len(), 1, "critical still applies");
    }

    /// R-GUARD-06 / U-07: a unit over budget x slack, at any level.
    #[test]
    fn budget_exceeded_is_decided_at_any_level_once() {
        let mut u = unit("g10", 0, 56);
        u.budget_mb = Some(50);
        let mut units = vec![u];
        let mut e = engine();
        units[0].footprint_mb = 54;
        assert!(kills(&e.tick(Tick { now_ms: 0, sample: &free(60.0), units: Some(&units), stop: None })).is_empty(), "54 <= 50 x 1.1 = 55");
        units[0].footprint_mb = 60;
        let d = e.tick(Tick { now_ms: 2_000, sample: &free(60.0), units: Some(&units), stop: None });
        assert_eq!(kills(&d), vec![(0, "ram_exceeded".into())]);
        assert!(e.tick(Tick { now_ms: 4_000, sample: &free(60.0), units: Some(&units), stop: None }).is_empty(), "not repeated");
    }

    /// pressure pauses pausable units; clear resumes; staying paused past pause_max escalates to a kill.
    #[test]
    fn pause_resume_and_escalation() {
        let mut u = unit("g10", 0, 100);
        u.pausable = true;
        let units = vec![u];
        let mut e = engine();
        let at = |e: &mut Engine, ms: i64, f: f64| e.tick(Tick { now_ms: ms, sample: &free(f), units: Some(&units), stop: None });
        at(&mut e, 0, 21.0);
        at(&mut e, 2_000, 21.0);
        let d = at(&mut e, 4_000, 21.0);
        assert_eq!(d.iter().filter(|d| d.kind == "would_pause").count(), 1, "{d:?}");
        // recovers: resume after the level clears
        at(&mut e, 6_000, 40.0);
        at(&mut e, 8_000, 40.0);
        let d = at(&mut e, 10_000, 40.0);
        assert_eq!(d.iter().filter(|d| d.kind == "would_resume").count(), 1, "{d:?}");

        // pressure persists beyond pause_max: kill with the memory reason
        let mut e = engine();
        for i in 0..3 {
            at(&mut e, i * 2_000, 21.0);
        }
        let d = at(&mut e, 70_000, 21.0);
        assert_eq!(kills(&d), vec![(0, "system_memory".into())], "{d:?}");
    }

    /// R-GUARD-05: the kill switch overrides the level, kills everything non-never every sample that new matches
    /// appear, and plans the restore hooks.
    #[test]
    fn kill_switch_plans_everything_and_new_matches_each_sample() {
        let stop = StopState { since_ms: 0, by: "owner".into(), reason: "test".into() };
        let mut prot = unit("g11", 1, 10);
        prot.protected = true;
        let mut units = vec![unit("g10", 0, 10), prot];
        let mut e = engine();
        let d = e.tick(Tick { now_ms: 0, sample: &free(80.0), units: Some(&units), stop: Some(&stop) });
        assert_eq!(kills(&d).len(), 2);
        assert!(kills(&d).iter().all(|(_, r)| r == "kill_switch"));
        assert!(d.iter().any(|d| d.kind == "would_hook"));
        assert!(e.tick(Tick { now_ms: 2_000, sample: &free(80.0), units: Some(&units), stop: Some(&stop) }).is_empty());
        units.push(unit("g13", 3, 10));
        let d = e.tick(Tick { now_ms: 4_000, sample: &free(80.0), units: Some(&units), stop: Some(&stop) });
        assert_eq!(kills(&d), vec![(3, "kill_switch".into())]);
    }

    fn kinds(d: &[Decision], k: &str) -> usize {
        d.iter().filter(|d| d.kind == k).count()
    }

    fn pausable(key: &str) -> Unit {
        let mut u = unit(key, 0, 100);
        u.pausable = true;
        u
    }

    /// M1 / R-GUARD-03 (note 43): a protected or unresolved-protected unit dies only at critical or stop; pause_max
    /// must not turn a pressure-level pause into a kill, so it is not paused at all.
    #[test]
    fn a_protected_pausable_unit_is_neither_paused_nor_killed_at_pressure() {
        for unresolved in [false, true] {
            let mut u = pausable("g10");
            u.protected = !unresolved;
            u.protect_unresolved = unresolved;
            let units = vec![u];
            let mut e = engine();
            let mut all = vec![];
            for i in 0..3 {
                all.extend(e.tick(Tick { now_ms: i * 2000, sample: &free(21.0), units: Some(&units), stop: None }));
            }
            all.extend(e.tick(Tick { now_ms: 70_000, sample: &free(21.0), units: Some(&units), stop: None }));
            assert_eq!((kinds(&all, "would_pause"), kinds(&all, "would_kill")), (0, 0), "{all:?}");
        }
    }

    /// M1: protection that appears after the pause (a resolver answered late) resumes the unit at pause_max.
    #[test]
    fn protection_appearing_after_a_pause_resumes_instead_of_escalating() {
        let mut e = engine();
        let units = vec![pausable("g10")];
        let mut all = vec![];
        for i in 0..3 {
            all.extend(e.tick(Tick { now_ms: i * 2000, sample: &free(21.0), units: Some(&units), stop: None }));
        }
        assert_eq!(kinds(&all, "would_pause"), 1);
        let mut u = pausable("g10");
        u.protected = true;
        let d = e.tick(Tick { now_ms: 70_000, sample: &free(21.0), units: Some(&[u]), stop: None });
        assert_eq!((kinds(&d, "would_kill"), kinds(&d, "would_resume")), (0, 1), "{d:?}");
    }

    /// M5 / note 43: an unreadable free-memory reading below pressure resumes the unit; it is not killed at pause_max
    /// for want of a number.
    #[test]
    fn an_unknown_reading_below_pressure_resumes_a_paused_unit() {
        let units = vec![pausable("g10")];
        let mut e = engine();
        let mut all = vec![];
        for i in 0..3 {
            all.extend(e.tick(Tick { now_ms: i * 2000, sample: &free(21.0), units: Some(&units), stop: None }));
        }
        assert_eq!(kinds(&all, "would_pause"), 1);
        let unknown = Sample { free_pct: None, total_mb: Some(16_000), pressure: Some(1), ..Sample::default() };
        for i in 3..40 {
            all.extend(e.tick(Tick { now_ms: i * 2000, sample: &unknown, units: Some(&units), stop: None }));
        }
        assert_eq!((kinds(&all, "would_resume"), kinds(&all, "would_kill")), (1, 0), "{all:?}");
    }

    /// Note 42: a unit that survives a real kill is tried again after retry_ms, not before; in observe mode it is not.
    #[test]
    fn enforce_retries_a_unit_that_survived_its_kill_after_retry_ms() {
        let mut e = engine();
        e.cfg.enforce = true;
        let units = vec![unit("g10", 0, 100)];
        let k = |e: &mut Engine, ms| kills(&e.tick(Tick { now_ms: ms, sample: &free(19.0), units: Some(&units), stop: None })).len();
        assert_eq!(k(&mut e, 0), 1);
        assert_eq!(k(&mut e, 10_000), 0, "inside retry_ms");
        assert_eq!(k(&mut e, 22_000), 1, "still alive after retry_ms: the kill did not take");
        let mut o = engine();
        o.cfg.enforce = false;
        assert_eq!(kills(&o.tick(Tick { now_ms: 0, sample: &free(19.0), units: Some(&units), stop: None })).len(), 1);
        assert_eq!(kills(&o.tick(Tick { now_ms: 22_000, sample: &free(19.0), units: Some(&units), stop: None })).len(), 0);
    }

    #[test]
    fn an_unreadable_process_table_is_reported_not_guessed() {
        let mut e = engine();
        let d = e.tick(Tick { now_ms: 0, sample: &free(10.0), units: None, stop: None });
        assert!(d.iter().any(|d| d.kind == "skipped" && d.reason == "process_table_unreadable"));
    }

    #[test]
    fn thermal_levels_use_the_thermal_reason() {
        let mut e = Engine::new(
            EngineCfg {
                macos: false,
                settle_ms: 8_000,
                pause_max_ms: 60_000,
                pause_max_thermal_ms: 600_000,
                budget_slack: 1.1,
                hooks: vec![],
                enforce: false,
                retry_ms: 20_000,
            },
            LevelEval::new(LevelCfg::resolve(false, None).unwrap(), 3),
        );
        let units = vec![unit("g10", 0, 10)];
        let s = Sample { avail_mb: Some(9000), total_mb: Some(16_000), temp_c: Some(96.0), ..Sample::default() };
        let d = e.tick(Tick { now_ms: 0, sample: &s, units: Some(&units), stop: None });
        assert_eq!(kills(&d), vec![(0, "thermal".into())]);
    }
}
