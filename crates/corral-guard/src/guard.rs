//! The guard core: samples, the engine, registrations, reservations, admission, the kill switch and the request
//! handler behind the socket. In observe mode (the only mode of this build) it decides and logs; it never signals.

use crate::admit::{self, AdmitReq, CheckResult, Class, Ctx, Reservation};
use crate::decisions::{Decision, DecisionLog};
use crate::engine::{Engine, EngineCfg, Tick};
use crate::levels::{Level, LevelEval};
use crate::procs::{ProcInfo, ProcSource, Protect, Rules, Unit, build_units};
use crate::sample::{DiskProbe, Probes, Sample, run_cmd_owned};
use crate::settings::GuardSettings;
use crate::state::{Registration, StateDir, StopState};
use corral_proto::util::{random_b32, rfc3339};
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
    inner: Mutex<Inner>,
    events: broadcast::Sender<String>,
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
            },
            eval,
        );
        let mode = if settings.observe { "observe" } else { "enforce" };
        let log = DecisionLog::open(state.path(), &settings.host, mode);
        let now = deps.clock.now_ms();
        let regs = state.load_regs();
        let (events, _) = broadcast::channel(256);
        Ok(Guard {
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
        })
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
        let now = sample.t_ms;
        let stop = self.state.stop();
        let mut g = self.lock();
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
        for d in decisions {
            self.record(&mut g, d);
        }
    }

    fn record(&self, g: &mut Inner, d: Decision) {
        let d = g.log.record(d);
        if d.is_action() {
            *g.metrics.actions.entry((d.kind.clone(), d.tier.map(|t| t.to_string()).unwrap_or_default(), d.reason.clone())).or_default() +=
                1;
        }
        let _ = self.events.send(serde_json::to_string(&d).expect("json"));
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
        if let Some(tok) = v.get("reservation").and_then(Value::as_str) {
            g.reservations.retain(|r| r.token != tok);
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
            return err(format!("cannot persist registrations: {e}"));
        }
        json!({ "v": 1, "ok": true, "id": id })
    }

    fn unregister(&self, id: &str) -> Value {
        let mut g = self.lock();
        g.regs.retain(|r| r.id != id);
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
            format!(
                "kill switch on by {by}: {reason}; admission refused. Observe mode: nothing is signalled, the next samples log what would be killed"
            ),
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
            "status" => self.status(),
            "explain" => {
                let n = req.get("n").and_then(Value::as_u64).unwrap_or(20) as usize;
                json!({ "v": 1, "ok": true, "decisions": self.lock().log.last(n, false) })
            }
            "stop" => self.stop(s("by").unwrap_or("cli"), s("reason").unwrap_or("no reason given")),
            "resume" => self.resume(s("by").unwrap_or("cli")),
            "metrics" => json!({ "v": 1, "ok": true, "text": self.metrics() }),
            "lock.status" => json!({ "v": 1, "ok": true, "locks": self.lock_status(s("lock")) }),
            "lock.acquire" | "lock.renew" | "lock.release" => {
                err("unavailable: take host locks with `corral lock` (the lease-file protocol); guard-held locks arrive with milestone M3")
            }
            "hold" | "release_hold" | "holds" => err("unavailable: keep-awake holds arrive with milestone M3"),
            "subscribe" => err("subscribe is a streaming request"),
            other => err(format!("unknown op {other:?}")),
        }
    }
}
