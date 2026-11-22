//! The guard core: samples, the engine, registrations, reservations, admission, the kill switch and the request
//! handler behind the socket. In observe mode (the only mode of this build) it decides and logs; it never signals.

use crate::admit::{self, AdmitReq, CheckResult, Class, Ctx, Reservation};
use crate::decisions::{Decision, DecisionLog};
use crate::enforce::{Executor, RealHooks, RealSys};
use crate::engine::{Engine, EngineCfg, Tick};
use crate::levels::{Level, LevelEval};
use crate::procs::{ProcInfo, ProcSource, Protect, Rules, Unit, build_units};
use crate::sample::{DiskProbe, Probes, Sample, run_cmd_owned};
use crate::settings::GuardSettings;
use crate::state::{Registration, StateDir, StopState};
use corral_proto::util::{random_b32, rfc3339};
use corral_worker::keepawake::{CachedProbe, KeepAwake};
use corral_worker::lock::{FileLock, LockStatus, LockStore, SystemWall};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::broadcast;

pub trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
}

pub struct SystemClock;
impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
    }
}

/// Resolves a matcher's `protect.resolve_cmd` into ids (GUARDS.md 3.2). Read-only by contract.
pub trait ProtectResolver: Send + Sync {
    fn resolve(&self, cmd: &[String]) -> Protect;
}

pub struct CmdResolver;
impl ProtectResolver for CmdResolver {
    fn resolve(&self, cmd: &[String]) -> Protect {
        let (prog, args) = cmd.split_first().ok_or("empty resolve_cmd")?;
        let out = run_cmd_owned(prog, args, std::time::Duration::from_secs(5))?;
        Ok(out.lines().map(|l| l.trim().to_string()).filter(|l| !l.is_empty()).collect())
    }
}

pub struct Deps {
    pub probes: Probes,
    pub procs: Box<dyn ProcSource>,
    pub disk: Box<dyn DiskProbe>,
    pub resolver: Box<dyn ProtectResolver>,
    pub clock: Arc<dyn Clock>,
}

#[derive(Default)]
struct Metrics {
    actions: BTreeMap<(String, String, String), u64>,
    admits: BTreeMap<String, u64>,
    samples: u64,
}

struct Inner {
    engine: Engine,
    sample: Sample,
    units: Vec<Unit>,
    table_ok: bool,
    regs: Vec<Registration>,
    reservations: Vec<Reservation>,
    log: DecisionLog,
    fail_since: HashMap<String, i64>,
    protect_cache: HashMap<String, (i64, Protect)>,
    metrics: Metrics,
    started_ms: i64,
}

pub struct Guard {
    pub settings: GuardSettings,
    state: StateDir,
    probes: Probes,
    procs: Box<dyn ProcSource>,
    disk: Box<dyn DiskProbe>,
    resolver: Box<dyn ProtectResolver>,
    clock: Arc<dyn Clock>,
    /// Enforce mode only: the one place that signals (`enforce.rs`).
    exec: Option<Executor>,
    /// The keep-awake provider, and whether heavy work requires it (R-AWAKE-03).
    keepawake: Option<(Arc<dyn KeepAwake>, bool, CachedProbe)>,
    inner: Mutex<Inner>,
    events: broadcast::Sender<String>,
    /// Held for a whole decision round and by `shutdown`; `true` once shut down. A round that was in flight when the
    /// server stopped cannot STOP anything after shutdown has continued what was paused (L7).
    rounds: Mutex<bool>,
}

fn err(msg: impl Into<String>) -> Value {
    json!({ "v": 1, "ok": false, "error": msg.into() })
}

impl Guard {
    pub fn new(settings: GuardSettings, deps: Deps) -> Result<Guard, String> {
        let state = StateDir::open(&settings.state_dir).map_err(|e| format!("state dir {}: {e}", settings.state_dir.display()))?;
        let eval = LevelEval::new(settings.levels.clone(), settings.clear_samples);
        let engine = Engine::new(
            EngineCfg {
                macos: settings.macos,
                settle_ms: settings.settle_ms as i64,
                pause_max_ms: settings.pause_max_ms as i64,
                pause_max_thermal_ms: settings.pause_max_thermal_ms as i64,
                budget_slack: settings.budget_slack,
                hooks: settings.hook_refs(),
                enforce: !settings.observe,
                retry_ms: (settings.kill_grace_ms + settings.settle_ms) as i64 + 4_000,
            },
            eval,
        );
        let mode = if settings.observe { "observe" } else { "enforce" };
        let log = DecisionLog::open(state.path(), &settings.host, mode);
        let now = deps.clock.now_ms();
        let regs = state.load_regs();
        let (events, _) = broadcast::channel(256);
        let exec = (!settings.observe).then(|| {
            Executor::new(
                Arc::new(RealSys::default()),
                Arc::new(RealHooks),
                std::time::Duration::from_millis(settings.kill_grace_ms),
                state.path(),
            )
        });
        if let Some(x) = &exec {
            // processes a previous run of this guard left stopped are continued
            let n = x.resume_all_paused();
            if n > 0 {
                tracing::warn!("resumed {n} process(es) a previous guard run had paused");
            }
        }
        let keepawake = corral_worker::keepawake::from_config(settings.keepawake.as_ref())?
            .map(|(p, req)| (p.clone(), req, CachedProbe::new(p, std::time::Duration::from_secs(15))));
        Ok(Guard {
            exec,
            keepawake,
            inner: Mutex::new(Inner {
                engine,
                sample: Sample::default(),
                units: vec![],
                table_ok: false,
                regs,
                reservations: vec![],
                log,
                fail_since: HashMap::new(),
                protect_cache: HashMap::new(),
                metrics: Metrics::default(),
                started_ms: now,
            }),
            settings,
            state,
            probes: deps.probes,
            procs: deps.procs,
            disk: deps.disk,
            resolver: deps.resolver,
            clock: deps.clock,
            events,
            rounds: Mutex::new(false),
        })
    }

    /// Replace the keep-awake provider (tests).
    pub fn with_keepawake(mut self, p: Arc<dyn KeepAwake>, required: bool) -> Guard {
        let probe = CachedProbe::new(p.clone(), std::time::Duration::from_millis(1));
        self.keepawake = Some((p, required, probe));
        self
    }

    /// `keepawake_ok` (GUARDS.md 7.1): the provider probe; `required` makes a failure refuse heavy work.
    fn keepawake_check(&self) -> Option<(CheckResult, bool)> {
        let (_, required, probe) = self.keepawake.as_ref()?;
        Some(match probe.check() {
            Ok(()) => (
                CheckResult { check: "keepawake_ok".into(), ok: true, reason: "keep-awake provider works".into(), since_ms: None },
                *required,
            ),
            Err(e) => (
                CheckResult {
                    check: "keepawake_unavailable".into(),
                    ok: !*required,
                    reason: format!("keep-awake provider unavailable: {e}"),
                    since_ms: None,
                },
                *required,
            ),
        })
    }

    fn hold_op(&self, op: &str, req: &Value) -> Value {
        let Some((p, _, _)) = &self.keepawake else {
            return err("no keep-awake provider is configured ([guard.keepawake])");
        };
        let owner = req.get("owner").and_then(Value::as_str).unwrap_or("");
        let res: Result<Value, String> = match op {
            "hold" => {
                let for_s = req.get("for_s").and_then(Value::as_u64).unwrap_or(0);
                let bound = req.get("bound_pid").and_then(Value::as_u64).map(|p| p as u32);
                let display = req.get("display").and_then(Value::as_bool).unwrap_or(false);
                p.hold(owner, std::time::Duration::from_secs(for_s.max(1)), bound, display)
                    .map(|h| json!({ "v": 1, "ok": true, "owner": h.owner, "pid": h.pid, "for_s": for_s }))
            }
            "release_hold" => p.release(owner).map(|()| json!({ "v": 1, "ok": true })),
            _ => p.list().map(|l| {
                json!({ "v": 1, "ok": true, "holds": l.iter().map(|h| json!({ "owner": h.owner, "until": h.until, "pid": h.pid, "bound_pid": h.bound_pid, "display": h.display })).collect::<Vec<_>>() })
            }),
        };
        res.unwrap_or_else(err)
    }

    /// Replace the executor (tests inject fake system calls). Enforce mode only.
    pub fn with_executor(mut self, e: Executor) -> Guard {
        self.exec = (!self.settings.observe).then_some(e);
        self
    }

    /// Orderly shutdown: continue anything this guard paused.
    pub fn shutdown(&self) {
        // waits for a round in flight, and stops later rounds from acting
        let mut down = self.rounds.lock().unwrap_or_else(|e| e.into_inner());
        *down = true;
        if let Some(x) = &self.exec {
            x.resume_all_paused();
        }
    }

    /// Kinds in enforce mode lose the `would_` prefix (decisions.rs).
    fn relabel(&self, mut d: Decision) -> Decision {
        if self.exec.is_some() {
            d.kind = match d.kind.as_str() {
                "would_kill" => "kill",
                "would_pause" => "pause",
                "would_resume" => "resume_unit",
                "would_hook" => "hook",
                o => o,
            }
            .into();
        }
        d
    }

    fn drain(&self, g: &mut Inner) {
        if let Some(x) = &self.exec {
            let out: Vec<Decision> = std::mem::take(&mut *x.outbox().lock().unwrap_or_else(|e| e.into_inner()));
            for mut d in out {
                if d.t_ms == 0 {
                    d.t_ms = self.clock.now_ms();
                }
                self.record(g, d);
            }
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<String> {
        self.events.subscribe()
    }

    pub fn mode(&self) -> &'static str {
        if self.settings.observe { "observe" } else { "enforce" }
    }

    /// Read the probes and the process table, then decide. Blocking (runs helper programs): call from a blocking
    /// thread.
    pub fn tick(&self) {
        let now = self.clock.now_ms();
        let sample = self.probes.sample(now);
        let table = self.procs.snapshot();
        let mut protect = HashMap::new();
        for m in self.settings.matchers.iter().filter(|m| m.protect_cmd.is_some()) {
            let cached = self.lock().protect_cache.get(&m.name).filter(|(t, _)| now - t < m.protect_ttl_ms as i64).map(|(_, p)| p.clone());
            let p = cached.unwrap_or_else(|| {
                let p = self.resolver.resolve(m.protect_cmd.as_deref().unwrap_or(&[]));
                self.lock().protect_cache.insert(m.name.clone(), (now, p.clone()));
                p
            });
            protect.insert(m.name.clone(), p);
        }
        self.tick_with(sample, table, protect);
    }

    /// One decision round from already-read inputs (tests feed these directly).
    pub fn tick_with(&self, sample: Sample, table: Option<Vec<ProcInfo>>, protect: HashMap<String, Protect>) {
        let down = self.rounds.lock().unwrap_or_else(|e| e.into_inner());
        if *down {
            return;
        }
        let now = sample.t_ms;
        let stop = self.state.stop();
        let mut g = self.lock();
        self.drain(&mut g);
        g.metrics.samples += 1;
        g.reservations.retain(|r| r.until_ms > now);
        if let Some(t) = &table {
            let before = g.regs.len();
            g.regs.retain(|r| r.since_ms >= now || t.iter().any(|p| p.pid == r.pid && p.start == r.pid_start));
            if g.regs.len() != before {
                let _ = self.state.save_regs(&g.regs);
            }
        }
        let regs = g.regs.clone();
        let units = table.as_ref().map(|t| {
            build_units(
                t,
                &*self.procs,
                &Rules { matchers: &self.settings.matchers, never: &self.settings.never, protect: &protect, me: std::process::id() },
                &regs,
            )
        });
        let decisions = g.engine.tick(Tick { now_ms: now, sample: &sample, units: units.as_deref(), stop: stop.as_ref() });
        g.table_ok = units.is_some();
        g.units = units.unwrap_or_default();
        g.sample = sample;
        // a claimed reservation ends once a sample shows its unit (the unit counts for itself from then on)
        let seen: Vec<String> = g.units.iter().filter_map(|u| u.reg_id.clone()).collect();
        g.reservations.retain(|r| r.claimed_by.as_ref().is_none_or(|id| !seen.contains(id)));
        if let Some(x) = &self.exec {
            x.set_level(g.engine.level().name());
        }
        // Record (and broadcast) first, then act: a worker hears why its job is about to die before it dies.
        let mut todo: Vec<(Decision, Option<Unit>)> = Vec::new();
        for d in decisions {
            let d = self.record(&mut g, self.relabel(d));
            let u = d.key.as_ref().and_then(|k| g.units.iter().find(|u| &u.key == k).cloned());
            todo.push((d, u));
        }
        let paused_keys = g.engine.paused_keys();
        drop(g);
        let Some(x) = &self.exec else { return };
        for (d, u) in todo {
            match (d.kind.as_str(), u) {
                ("kill", Some(u)) => x.kill(now, &u, &d.reason),
                ("pause", Some(u)) => x.pause(now, &u),
                ("resume_unit", Some(u)) => x.resume(&u),
                ("hook", _) => {
                    if let Some(h) = self.settings.hooks.iter().find(|h| Some(&h.name) == d.key.as_ref()) {
                        x.hook(now, h);
                    }
                }
                _ => {}
            }
        }
        // anything recorded as stopped that the engine no longer holds as paused is continued now (note 43)
        x.reconcile(&paused_keys);
    }

    fn record(&self, g: &mut Inner, d: Decision) -> Decision {
        let d = g.log.record(d);
        if d.is_action() {
            *g.metrics.actions.entry((d.kind.clone(), d.tier.map(|t| t.to_string()).unwrap_or_default(), d.reason.clone())).or_default() +=
                1;
        }
        let _ = self.events.send(serde_json::to_string(&d).expect("json"));
        d
    }

    fn annotate(g: &mut Inner, now: i64, results: &mut [CheckResult]) {
        for r in results {
            if r.ok {
                g.fail_since.remove(&r.check);
            } else {
                r.since_ms = Some(*g.fail_since.entry(r.check.clone()).or_insert(now));
            }
        }
    }

    fn with_ctx<T>(&self, g: &Inner, now: i64, f: impl FnOnce(&Ctx<'_>) -> T) -> T {
        let stop = self.state.stop();
        let why = g.engine.why().to_string();
        let ctx = Ctx {
            macos: self.settings.macos,
            sample: &g.sample,
            level: if stop.is_some() { Level::Stop } else { g.engine.sample_level() },
            level_why: &why,
            levels: g.engine.levels(),
            stop: stop.as_ref(),
            units: &g.units,
            reservations: &g.reservations,
            heavy_slots: self.settings.heavy_slots,
            disk: self.disk.free_mb(&self.settings.disk_path),
            min_disk_mb: self.settings.min_disk_mb,
            admit_max_c: self.settings.admit_max_c,
            idle_need_s: 300,
        };
        let _ = now;
        f(&ctx)
    }

    fn admit(&self, req: AdmitReq) -> Value {
        match req.profile.as_deref() {
            None | Some("default") => {}
            Some(p) => {
                return err(format!(
                    "profile {p:?} is not available in this build (only \"default\"; measurement and gui arrive with milestone M4)"
                ));
            }
        }
        let now = self.clock.now_ms();
        let mut g = self.lock();
        g.reservations.retain(|r| r.until_ms > now);
        let mut results = self.with_ctx(&g, now, |c| admit::default_profile(c, &req));
        if req.class == Class::Heavy
            && let Some((r, _)) = self.keepawake_check()
            && !r.ok
        {
            results.push(r);
        }
        Self::annotate(&mut g, now, &mut results);
        let failing: Vec<&CheckResult> = results.iter().filter(|r| !r.ok).collect();
        let ok = failing.is_empty();
        let mut reply = json!({ "v": 1, "ok": ok, "mode": self.mode(), "reasons": failing });
        if ok && req.reserve_s > 0 {
            let token = random_b32(16);
            let until = now + (req.reserve_s as i64) * 1000;
            g.reservations.push(Reservation {
                token: token.clone(),
                name: req.name.clone().unwrap_or_default(),
                ram_mb: req.ram_mb,
                heavy: req.class == Class::Heavy,
                until_ms: until,
                claimed_by: None,
            });
            reply["reservation"] = json!(token);
            reply["until_ms"] = json!(until);
            reply["until"] = json!(rfc3339(until));
        }
        let name = req.name.clone().unwrap_or_else(|| "?".into());
        let (reason, detail) = match failing.first() {
            Some(f) => (f.check.clone(), format!("{name}: {} ({} MB, class {:?}); {}", f.reason, req.ram_mb, req.class, g.sample.brief())),
            None => ("admitted".into(), format!("{name}: {} MB, class {:?}; {}", req.ram_mb, req.class, g.sample.brief())),
        };
        let mut d = Decision::new(now, "admit", g.engine.level().name(), reason, detail);
        d.ok = Some(ok);
        *g.metrics.admits.entry(if ok { "ok" } else { "refused" }.into()).or_default() += 1;
        self.record(&mut g, d);
        reply
    }

    fn checks(&self, profile: Option<&str>) -> Value {
        let now = self.clock.now_ms();
        let mut g = self.lock();
        let mut results = match profile {
            None | Some("all") => self.with_ctx(&g, now, admit::all_checks),
            Some("default") => self.with_ctx(&g, now, |c| {
                admit::default_profile(
                    c,
                    &AdmitReq { name: None, ram_mb: 0, class: Class::Heavy, profile: None, reserve_s: 0, reservation: None },
                )
            }),
            Some(p) => return err(format!("profile {p:?} is not available in this build (default or all)")),
        };
        if let Some((r, _)) = self.keepawake_check() {
            results.push(r);
        }
        for (name, l) in &self.settings.locks {
            let lock = FileLock::new(l.path.clone(), Arc::new(SystemWall));
            let holder = lock.holder();
            results.push(CheckResult {
                check: format!("lock_free:{name}"),
                ok: holder.is_none(),
                reason: holder.unwrap_or_else(|| "lock free".into()),
                since_ms: None,
            });
        }
        Self::annotate(&mut g, now, &mut results);
        json!({ "v": 1, "ok": results.iter().all(|r| r.ok), "results": results })
    }

    fn register(&self, v: &Value) -> Value {
        let now = self.clock.now_ms();
        let num = |k: &str| v.get(k).and_then(Value::as_u64);
        let (Some(name), Some(pid), Some(pgid), Some(start)) =
            (v.get("name").and_then(Value::as_str), num("pid"), num("pgid"), v.get("pid_start").and_then(Value::as_str))
        else {
            return err("register needs name, pid, pgid and pid_start");
        };
        let mut g = self.lock();
        // idempotent by (pgid, pid_start): a worker that reconnects re-sends its live registrations
        if let Some(r) = g.regs.iter().find(|r| r.pgid == pgid as u32 && r.pid_start == start) {
            return json!({ "v": 1, "ok": true, "id": r.id });
        }
        let r = Registration {
            id: random_b32(10),
            name: name.to_string(),
            pid: pid as u32,
            pgid: pgid as u32,
            pid_start: start.to_string(),
            tier: num("tier").map(|t| t as u32),
            budget_mb: num("budget_mb"),
            pausable: v.get("pausable").and_then(Value::as_bool).unwrap_or(false),
            heavy: v.get("heavy").and_then(Value::as_bool).unwrap_or(true),
            since_ms: now,
        };
        let id = r.id.clone();
        g.regs.push(r);
        if let Err(e) = self.state.save_regs(&g.regs) {
            // not registered means not registered: the reservation stays with the caller, who releases it (L1)
            g.regs.retain(|r| r.id != id);
            return err(format!("cannot persist registrations: {e}"));
        }
        // The reservation is not dropped now: until a sample has seen the unit (and its memory), it still counts for
        // its slot and its RAM, or a second heavy job is admitted into the gap (M2).
        if let Some(tok) = v.get("reservation").and_then(Value::as_str) {
            for r in g.reservations.iter_mut().filter(|r| r.token == tok) {
                r.claimed_by = Some(id.clone());
            }
        }
        json!({ "v": 1, "ok": true, "id": id })
    }

    fn unregister(&self, id: &str) -> Value {
        let mut g = self.lock();
        g.regs.retain(|r| r.id != id);
        g.reservations.retain(|r| r.claimed_by.as_deref() != Some(id));
        let _ = self.state.save_regs(&g.regs);
        json!({ "v": 1, "ok": true })
    }

    fn stop(&self, by: &str, reason: &str) -> Value {
        let now = self.clock.now_ms();
        let s = StopState { since_ms: now, by: by.to_string(), reason: reason.to_string() };
        if let Err(e) = self.state.set_stop(&s) {
            return err(format!("cannot write the kill switch: {e}"));
        }
        let mut g = self.lock();
        let d = Decision::new(
            now,
            "stop",
            g.engine.level().name(),
            "kill_switch",
            if self.settings.observe {
                format!(
                    "kill switch on by {by}: {reason}; admission refused. Observe mode: nothing is signalled, the next samples log what would be killed"
                )
            } else {
                format!("kill switch on by {by}: {reason}; admission refused; every watched non-never unit is being stopped")
            },
        );
        self.record(&mut g, d);
        json!({ "v": 1, "ok": true, "mode": self.mode(), "observed": self.settings.observe, "note": if self.settings.observe { "observe mode: state recorded, no process is signalled" } else { "" } })
    }

    fn resume(&self, by: &str) -> Value {
        let now = self.clock.now_ms();
        if let Err(e) = self.state.clear_stop() {
            return err(format!("cannot clear the kill switch: {e}"));
        }
        let mut g = self.lock();
        let d = Decision::new(now, "resume", g.engine.level().name(), "kill_switch_off", format!("kill switch cleared by {by}"));
        self.record(&mut g, d);
        json!({ "v": 1, "ok": true })
    }

    fn lock_status(&self, name: Option<&str>) -> Value {
        let mut out = serde_json::Map::new();
        for (n, l) in self.settings.locks.iter().filter(|(n, _)| name.is_none_or(|w| w == n.as_str())) {
            let lock = FileLock::new(l.path.clone(), Arc::new(SystemWall));
            let v = match lock.status() {
                LockStatus::Free => json!({ "state": "free" }),
                LockStatus::Unreadable => json!({ "state": "unreadable" }),
                LockStatus::Held { file, stale } => json!({ "state": if stale { "stale" } else { "held" }, "holder": file }),
            };
            out.insert(n.clone(), v);
        }
        Value::Object(out)
    }

    fn status(&self) -> Value {
        let now = self.clock.now_ms();
        let g = self.lock();
        let stop = self.state.stop();
        let level = if stop.is_some() { Level::Stop } else { g.engine.sample_level() };
        let units: Vec<Value> = g
            .units
            .iter()
            .map(|u| json!({ "key": u.key, "matcher": u.matcher, "tier": u.tier, "pids": u.pids, "comm": u.comm, "footprint_mb": u.footprint_mb, "heavy": u.heavy, "protected": u.protected, "never": u.has_never }))
            .collect();
        json!({
            "v": 1, "ok": true, "mode": self.mode(), "host": self.settings.host, "pid": std::process::id(),
            "level": level.name(), "why": g.engine.why(), "sample": g.sample, "table_ok": g.table_ok,
            "kill_switch": stop, "uptime_s": (now - g.started_ms) / 1000,
            "registrations": g.regs, "reservations": g.reservations.iter().map(|r| json!({ "name": r.name, "ram_mb": r.ram_mb, "heavy": r.heavy, "until_ms": r.until_ms })).collect::<Vec<_>>(),
            "units": units, "paused": g.engine.paused_keys(), "locks": self.lock_status(None),
            "last_actions": g.log.last(10, true), "decision_log_errors": g.log.write_errors,
        })
    }

    fn metrics(&self) -> String {
        let g = self.lock();
        let level = if self.state.stop().is_some() { Level::Stop } else { g.engine.sample_level() };
        let mut o = String::new();
        let mut gauge = |name: &str, v: Option<f64>| {
            if let Some(v) = v {
                o.push_str(&format!("{name} {v}\n"));
            }
        };
        gauge("corral_guard_level", Some(level.number() as f64));
        gauge("corral_guard_free_pct", g.sample.free_pct);
        gauge("corral_guard_swap_bytes", g.sample.swap_mb.map(|m| m as f64 * 1048576.0));
        gauge("corral_guard_temp_celsius", g.sample.temp_c);
        gauge("corral_guard_samples_total", Some(g.metrics.samples as f64));
        for ((action, tier, reason), n) in &g.metrics.actions {
            o.push_str(&format!("corral_guard_actions_total{{action=\"{action}\",tier=\"{tier}\",reason=\"{reason}\"}} {n}\n"));
        }
        for (result, n) in &g.metrics.admits {
            o.push_str(&format!("corral_guard_admits_total{{result=\"{result}\"}} {n}\n"));
        }
        o
    }

    /// One request, one reply. `subscribe` is handled by the server (it streams).
    pub fn handle(&self, req: &Value) -> Value {
        if req.get("v").and_then(Value::as_u64) != Some(1) {
            return err("unsupported or missing protocol version (this guard speaks v 1)");
        }
        let s = |k: &str| req.get(k).and_then(Value::as_str);
        match s("op").unwrap_or("") {
            "admit" => match serde_json::from_value::<AdmitReq>(req.clone()) {
                Ok(r) => self.admit(r),
                Err(e) => err(format!("bad admit request: {e}")),
            },
            "checks" => self.checks(s("profile")),
            "register" => self.register(req),
            "unregister" => self.unregister(s("id").unwrap_or("")),
            "unreserve" => {
                self.lock().reservations.retain(|r| Some(r.token.as_str()) != s("token"));
                json!({ "v": 1, "ok": true })
            }
            "status" => {
                self.drain(&mut self.lock());
                self.status()
            }
            "explain" => {
                let n = req.get("n").and_then(Value::as_u64).unwrap_or(20) as usize;
                json!({ "v": 1, "ok": true, "now_ms": self.clock.now_ms(), "decisions": self.lock().log.last(n, false) })
            }
            "stop" => {
                let r = self.stop(s("by").unwrap_or("cli"), s("reason").unwrap_or("no reason given"));
                if self.exec.is_some() && r["ok"] == json!(true) {
                    self.tick(); // the kill switch acts now, not at the next sample
                }
                r
            }
            "resume" => self.resume(s("by").unwrap_or("cli")),
            "metrics" => json!({ "v": 1, "ok": true, "text": self.metrics() }),
            "lock.status" => json!({ "v": 1, "ok": true, "locks": self.lock_status(s("lock")) }),
            "lock.acquire" | "lock.renew" | "lock.release" => {
                err("unavailable: take host locks with `corral lock` (the lease-file protocol); guard-held locks arrive with milestone M3")
            }
            op @ ("hold" | "release_hold" | "holds") => self.hold_op(op, req),
            "subscribe" => err("subscribe is a streaming request"),
            other => err(format!("unknown op {other:?}")),
        }
    }
}
