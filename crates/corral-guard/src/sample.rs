//! Host probes behind traits (GUARDS.md 2.1, AGENTS.md "Tests"): memory, swap, memory pressure (macOS level, Linux
//! PSI), temperature, idle time and power source. Unit tests inject values through the traits; the parsers of every
//! real probe are pure functions with their own tests, so both operating systems' parsers run on every host.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// Everything the guard knows about the host at one instant. `None` = the probe failed or does not exist here; the
/// level logic never acts on an unknown, and admission treats it per check (`on_unknown`, GUARDS.md 7.1).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    pub t_ms: i64,
    pub total_mb: Option<u64>,
    pub avail_mb: Option<u64>,
    pub free_pct: Option<f64>,
    pub swap_mb: Option<u64>,
    /// macOS `kern.memorystatus_vm_pressure_level`: 1 normal, 2 warn, 4 critical.
    pub pressure: Option<u32>,
    /// Linux PSI `full avg10`.
    pub psi_full_avg10: Option<f64>,
    pub temp_c: Option<f64>,
    /// macOS thermal state: nominal, fair, serious, critical.
    pub thermal_state: Option<String>,
    pub idle_s: Option<u64>,
    /// `Some(false)` = on battery; `None` = no battery (desktop) or unknown.
    pub on_ac: Option<bool>,
    /// Probes that failed in this sample (name: reason). Logged, never silently dropped.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
}

impl Sample {
    /// A sample with the memory numbers a test cares about.
    pub fn mem(t_ms: i64, total_mb: u64, avail_mb: u64, swap_mb: u64) -> Sample {
        Sample {
            t_ms,
            total_mb: Some(total_mb),
            avail_mb: Some(avail_mb),
            free_pct: Some(avail_mb as f64 * 100.0 / total_mb as f64),
            swap_mb: Some(swap_mb),
            ..Sample::default()
        }
    }

    /// A short `free=19% swap=5100MB ...` text for log lines and reasons.
    pub fn brief(&self) -> String {
        let mut v = Vec::new();
        if let Some(f) = self.free_pct {
            v.push(format!("free={f:.0}%"));
        }
        if let Some(a) = self.avail_mb {
            v.push(format!("avail={a}MB"));
        }
        if let Some(s) = self.swap_mb {
            v.push(format!("swap={s}MB"));
        }
        if let Some(p) = self.pressure {
            v.push(format!("pressure={p}"));
        }
        if let Some(p) = self.psi_full_avg10 {
            v.push(format!("psi_full={p:.1}"));
        }
        if let Some(t) = self.temp_c {
            v.push(format!("temp={t:.0}C"));
        }
        if let Some(t) = &self.thermal_state {
            v.push(format!("thermal={t}"));
        }
        v.join(" ")
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MemReading {
    pub total_mb: u64,
    pub avail_mb: u64,
    pub swap_mb: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct PressureReading {
    pub level: Option<u32>,
    pub psi_full_avg10: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ThermalReading {
    pub temp_c: Option<f64>,
    pub state: Option<String>,
}

pub trait MemoryProbe: Send + Sync {
    fn read(&self) -> Result<MemReading, String>;
}
pub trait PressureProbe: Send + Sync {
    fn read(&self) -> Result<PressureReading, String>;
}
pub trait ThermalProbe: Send + Sync {
    fn read(&self) -> Result<ThermalReading, String>;
}
pub trait IdleProbe: Send + Sync {
    fn idle_s(&self) -> Result<u64, String>;
}
pub trait PowerProbe: Send + Sync {
    /// `Some(true)` on AC, `Some(false)` on battery, `None` when the host has no battery.
    fn on_ac(&self) -> Result<Option<bool>, String>;
}
pub trait DiskProbe: Send + Sync {
    fn free_mb(&self, path: &Path) -> Result<u64, String>;
}

// ---------------------------------------------------------------------------------------------------------------
// Running helper programs (sysctl, pmset, ioreg, ps, ...) with a deadline. A helper that hangs is abandoned, not
// signalled: the guard crate never sends signals (R-GUARD-10), and at most `MAX_STUCK` abandoned helpers are
// tolerated before probes report "helpers stuck" instead of starting more.

static STUCK: AtomicUsize = AtomicUsize::new(0);
const MAX_STUCK: usize = 4;

pub fn run_cmd(prog: &str, args: &[&str], timeout: Duration) -> Result<String, String> {
    run_cmd_owned(prog, &args.iter().map(|a| a.to_string()).collect::<Vec<_>>(), timeout)
}

pub fn run_cmd_owned(prog: &str, args: &[String], timeout: Duration) -> Result<String, String> {
    if STUCK.load(Ordering::SeqCst) >= MAX_STUCK {
        return Err(format!("{prog}: {MAX_STUCK} earlier helper commands are stuck"));
    }
    let (tx, rx) = std::sync::mpsc::channel();
    let (prog_s, args_v) = (prog.to_string(), args.to_vec());
    STUCK.fetch_add(1, Ordering::SeqCst);
    std::thread::spawn(move || {
        let r = std::process::Command::new(&prog_s).args(&args_v).env("LC_ALL", "C").stdin(std::process::Stdio::null()).output();
        STUCK.fetch_sub(1, Ordering::SeqCst); // finished, whether or not anyone still waits
        let _ = tx.send(r);
    });
    match rx.recv_timeout(timeout) {
        Ok(Ok(o)) if o.status.success() => Ok(String::from_utf8_lossy(&o.stdout).to_string()),
        Ok(Ok(o)) => Err(format!("{prog} exited with {}", o.status)),
        Ok(Err(e)) => Err(format!("{prog}: {e}")),
        Err(_) => Err(format!("{prog} timed out after {}s", timeout.as_secs())),
    }
}

const HELPER_TIMEOUT: Duration = Duration::from_secs(3);

// ---------------------------------------------------------------------------------------------------------------
// Parsers (pure).

/// `/proc/meminfo` -> total, available, swap used (MB).
pub fn parse_meminfo(text: &str) -> Result<MemReading, String> {
    let field = |name: &str| -> Option<u64> {
        text.lines().find_map(|l| {
            let rest = l.strip_prefix(name)?.strip_prefix(':')?;
            rest.split_whitespace().next()?.parse::<u64>().ok()
        })
    };
    let total = field("MemTotal").ok_or("MemTotal missing")?;
    let avail = field("MemAvailable").ok_or("MemAvailable missing")?;
    let swap = field("SwapTotal").unwrap_or(0).saturating_sub(field("SwapFree").unwrap_or(0));
    Ok(MemReading { total_mb: total / 1024, avail_mb: avail / 1024, swap_mb: swap / 1024 })
}

/// `vm.swapusage`: `total = 3072.00M  used = 1971.50M  free = 1100.50M  (encrypted)` -> used MB.
pub fn parse_swapusage(text: &str) -> Result<u64, String> {
    let rest = text.split("used =").nth(1).ok_or_else(|| format!("no 'used =' in {text:?}"))?;
    let tok = rest.split_whitespace().next().ok_or("empty swap used")?;
    let (num, unit) = tok.split_at(tok.len().saturating_sub(1));
    let n: f64 = num.parse().map_err(|_| format!("bad swap number {tok:?}"))?;
    let mult = match unit {
        "M" => 1.0,
        "G" => 1024.0,
        "K" => 1.0 / 1024.0,
        _ => return Err(format!("bad swap unit in {tok:?}")),
    };
    Ok((n * mult) as u64)
}

/// `/proc/pressure/memory`: `some avg10=0.00 ...` / `full avg10=1.50 ...` -> the `full` avg10.
pub fn parse_psi_full(text: &str) -> Result<f64, String> {
    let line = text.lines().find(|l| l.starts_with("full ")).ok_or("no 'full' line in PSI")?;
    let v = line.split_whitespace().find_map(|t| t.strip_prefix("avg10=")).ok_or("no avg10 in PSI")?;
    v.parse().map_err(|_| format!("bad PSI value {v:?}"))
}

/// `pmset -g therm`: `CPU_Speed_Limit = 100` -> a coarse state. 100 nominal, >= 80 fair, >= 50 serious, below critical.
pub fn parse_pmset_therm(text: &str) -> ThermalReading {
    let limit = text.lines().find_map(|l| {
        let (k, v) = l.split_once('=')?;
        (k.trim() == "CPU_Speed_Limit").then(|| v.trim().parse::<u32>().ok()).flatten()
    });
    let state = match limit {
        None => "nominal",
        Some(100..) => "nominal",
        Some(80..=99) => "fair",
        Some(50..=79) => "serious",
        Some(_) => "critical",
    };
    ThermalReading { temp_c: None, state: Some(state.into()) }
}

/// `ioreg -c IOHIDSystem`: `"HIDIdleTime" = 123456789` (nanoseconds) -> seconds.
pub fn parse_hid_idle(text: &str) -> Result<u64, String> {
    let l = text.lines().find(|l| l.contains("\"HIDIdleTime\"")).ok_or("no HIDIdleTime in ioreg output")?;
    let v = l.split('=').nth(1).ok_or("malformed HIDIdleTime line")?.trim();
    let ns: u64 = v.parse().map_err(|_| format!("bad HIDIdleTime {v:?}"))?;
    Ok(ns / 1_000_000_000)
}

/// `loginctl show-session -p IdleHint -p IdleSinceHint`: idle seconds, 0 when the session is not idle.
pub fn parse_logind_idle(text: &str, now_us: u64) -> Result<u64, String> {
    let get = |k: &str| text.lines().find_map(|l| l.strip_prefix(k)?.strip_prefix('='));
    match get("IdleHint") {
        Some("no") => Ok(0),
        Some("yes") => {
            let since: u64 = get("IdleSinceHint").and_then(|v| v.parse().ok()).ok_or("IdleSinceHint missing")?;
            if since == 0 {
                return Err("IdleSinceHint is 0".into());
            }
            Ok(now_us.saturating_sub(since) / 1_000_000)
        }
        _ => Err("no IdleHint (headless host?)".into()),
    }
}

/// `pmset -g batt`: `Now drawing from 'AC Power'` / `'Battery Power'`; no battery line -> `None`.
pub fn parse_pmset_batt(text: &str) -> Option<bool> {
    let first = text.lines().next()?;
    let has_battery = text.contains("InternalBattery");
    if !has_battery {
        return None;
    }
    if first.contains("AC Power") {
        Some(true)
    } else if first.contains("Battery Power") {
        Some(false)
    } else {
        None
    }
}

/// `/sys/class/power_supply/*`: a mains supply online -> AC; a battery with no mains online -> battery; neither -> `None`.
pub fn read_power_supply(root: &Path) -> Result<Option<bool>, String> {
    let rd = std::fs::read_dir(root).map_err(|e| format!("{}: {e}", root.display()))?;
    let (mut battery, mut mains_online) = (false, false);
    for e in rd.flatten() {
        let ty = std::fs::read_to_string(e.path().join("type")).unwrap_or_default();
        match ty.trim() {
            "Battery" => battery = true,
            "Mains" | "USB" | "USB_C" | "USB_PD" => {
                if std::fs::read_to_string(e.path().join("online")).map(|v| v.trim() == "1").unwrap_or(false) {
                    mains_online = true;
                }
            }
            _ => {}
        }
    }
    Ok(if !battery { None } else { Some(mains_online) })
}

/// Find a hwmon temperature input (millidegrees) by chip name and label under `root` (`/sys/class/hwmon`).
pub fn read_hwmon_temp(root: &Path, chip: &str, label: &str) -> Result<f64, String> {
    let rd = std::fs::read_dir(root).map_err(|e| format!("{}: {e}", root.display()))?;
    for e in rd.flatten() {
        let dir = e.path();
        if std::fs::read_to_string(dir.join("name")).map(|n| n.trim() == chip).unwrap_or(false) {
            for i in 1..=32 {
                let l = std::fs::read_to_string(dir.join(format!("temp{i}_label"))).unwrap_or_default();
                if l.trim() == label {
                    let raw = std::fs::read_to_string(dir.join(format!("temp{i}_input"))).map_err(|e| e.to_string())?;
                    let milli: f64 = raw.trim().parse().map_err(|_| format!("bad temperature {raw:?}"))?;
                    return Ok(milli / 1000.0);
                }
            }
        }
    }
    Err(format!("no hwmon sensor {chip:?} / {label:?} under {}", root.display()))
}

// ---------------------------------------------------------------------------------------------------------------
// Real probes.

pub struct ProcMemory;
impl MemoryProbe for ProcMemory {
    fn read(&self) -> Result<MemReading, String> {
        parse_meminfo(&std::fs::read_to_string("/proc/meminfo").map_err(|e| format!("/proc/meminfo: {e}"))?)
    }
}

/// macOS: `kern.memorystatus_level` (what jetsam uses), `hw.memsize`, `vm.swapusage`.
pub struct SysctlMemory;
impl MemoryProbe for SysctlMemory {
    fn read(&self) -> Result<MemReading, String> {
        let level: f64 =
            run_cmd("sysctl", &["-n", "kern.memorystatus_level"], HELPER_TIMEOUT)?.trim().parse().map_err(|_| "bad memorystatus_level")?;
        let bytes: u64 = run_cmd("sysctl", &["-n", "hw.memsize"], HELPER_TIMEOUT)?.trim().parse().map_err(|_| "bad hw.memsize")?;
        let swap = parse_swapusage(&run_cmd("sysctl", &["-n", "vm.swapusage"], HELPER_TIMEOUT)?)?;
        let total_mb = bytes / 1024 / 1024;
        Ok(MemReading { total_mb, avail_mb: (total_mb as f64 * level / 100.0) as u64, swap_mb: swap })
    }
}

pub struct PsiPressure;
impl PressureProbe for PsiPressure {
    fn read(&self) -> Result<PressureReading, String> {
        let t = std::fs::read_to_string("/proc/pressure/memory").map_err(|e| format!("/proc/pressure/memory: {e}"))?;
        Ok(PressureReading { level: None, psi_full_avg10: Some(parse_psi_full(&t)?) })
    }
}

pub struct SysctlPressure;
impl PressureProbe for SysctlPressure {
    fn read(&self) -> Result<PressureReading, String> {
        let l: u32 = run_cmd("sysctl", &["-n", "kern.memorystatus_vm_pressure_level"], HELPER_TIMEOUT)?
            .trim()
            .parse()
            .map_err(|_| "bad pressure level")?;
        Ok(PressureReading { level: Some(l), psi_full_avg10: None })
    }
}

/// Linux: a configured hwmon chip + label, `coretemp` / `Package id 0` by default.
pub struct HwmonThermal {
    pub root: PathBuf,
    pub chip: String,
    pub label: String,
}
impl HwmonThermal {
    pub fn default_sensor() -> HwmonThermal {
        HwmonThermal { root: "/sys/class/hwmon".into(), chip: "coretemp".into(), label: "Package id 0".into() }
    }
}
impl ThermalProbe for HwmonThermal {
    fn read(&self) -> Result<ThermalReading, String> {
        Ok(ThermalReading { temp_c: Some(read_hwmon_temp(&self.root, &self.chip, &self.label)?), state: None })
    }
}

/// Linux: one `/sys/class/thermal/thermal_zoneN/temp` file.
pub struct ZoneThermal(pub PathBuf);
impl ThermalProbe for ZoneThermal {
    fn read(&self) -> Result<ThermalReading, String> {
        let raw = std::fs::read_to_string(&self.0).map_err(|e| format!("{}: {e}", self.0.display()))?;
        let milli: f64 = raw.trim().parse().map_err(|_| format!("bad temperature {raw:?}"))?;
        Ok(ThermalReading { temp_c: Some(milli / 1000.0), state: None })
    }
}

pub struct PmsetThermal;
impl ThermalProbe for PmsetThermal {
    fn read(&self) -> Result<ThermalReading, String> {
        Ok(parse_pmset_therm(&run_cmd("pmset", &["-g", "therm"], HELPER_TIMEOUT)?))
    }
}

pub struct IoregIdle;
impl IdleProbe for IoregIdle {
    fn idle_s(&self) -> Result<u64, String> {
        parse_hid_idle(&run_cmd("ioreg", &["-c", "IOHIDSystem", "-d", "4"], HELPER_TIMEOUT)?)
    }
}

pub struct LogindIdle;
impl IdleProbe for LogindIdle {
    fn idle_s(&self) -> Result<u64, String> {
        let t = run_cmd("loginctl", &["show-session", "auto", "-p", "IdleHint", "-p", "IdleSinceHint"], HELPER_TIMEOUT)?;
        let now_us = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_micros() as u64).unwrap_or(0);
        parse_logind_idle(&t, now_us)
    }
}

pub struct PmsetPower;
impl PowerProbe for PmsetPower {
    fn on_ac(&self) -> Result<Option<bool>, String> {
        Ok(parse_pmset_batt(&run_cmd("pmset", &["-g", "batt"], HELPER_TIMEOUT)?))
    }
}

pub struct SysfsPower(pub PathBuf);
impl PowerProbe for SysfsPower {
    fn on_ac(&self) -> Result<Option<bool>, String> {
        read_power_supply(&self.0)
    }
}

pub struct StatvfsDisk;
impl DiskProbe for StatvfsDisk {
    fn free_mb(&self, path: &Path) -> Result<u64, String> {
        let s = rustix::fs::statvfs(path).map_err(|e| format!("statvfs {}: {e}", path.display()))?;
        Ok(s.f_bavail.saturating_mul(s.f_frsize) / 1024 / 1024)
    }
}

// ---------------------------------------------------------------------------------------------------------------

/// All probes of a host, with the slower ones (thermal, idle, power) refreshed every `slow_every_ms` (5 s).
pub struct Probes {
    pub memory: Box<dyn MemoryProbe>,
    pub pressure: Box<dyn PressureProbe>,
    pub thermal: Box<dyn ThermalProbe>,
    pub idle: Box<dyn IdleProbe>,
    pub power: Box<dyn PowerProbe>,
    pub slow_every_ms: i64,
    slow: Mutex<Option<(i64, Sample)>>,
}

impl Probes {
    pub fn new(
        memory: Box<dyn MemoryProbe>,
        pressure: Box<dyn PressureProbe>,
        thermal: Box<dyn ThermalProbe>,
        idle: Box<dyn IdleProbe>,
        power: Box<dyn PowerProbe>,
    ) -> Probes {
        Probes { memory, pressure, thermal, idle, power, slow_every_ms: 5_000, slow: Mutex::new(None) }
    }

    /// The probes of this host's operating system.
    pub fn for_host(sensor: Option<HwmonThermal>, zone: Option<PathBuf>) -> Probes {
        if cfg!(target_os = "macos") {
            Probes::new(Box::new(SysctlMemory), Box::new(SysctlPressure), Box::new(PmsetThermal), Box::new(IoregIdle), Box::new(PmsetPower))
        } else {
            let thermal: Box<dyn ThermalProbe> = match (zone, sensor) {
                (Some(z), _) => Box::new(ZoneThermal(z)),
                (None, s) => Box::new(s.unwrap_or_else(HwmonThermal::default_sensor)),
            };
            Probes::new(
                Box::new(ProcMemory),
                Box::new(PsiPressure),
                thermal,
                Box::new(LogindIdle),
                Box::new(SysfsPower("/sys/class/power_supply".into())),
            )
        }
    }

    /// One sample. A probe that fails leaves its fields `None` and adds a line to `errors`.
    pub fn sample(&self, now_ms: i64) -> Sample {
        let mut s = Sample { t_ms: now_ms, ..Sample::default() };
        match self.memory.read() {
            Ok(m) => {
                s.total_mb = Some(m.total_mb);
                s.avail_mb = Some(m.avail_mb);
                s.swap_mb = Some(m.swap_mb);
                if m.total_mb > 0 {
                    s.free_pct = Some(m.avail_mb as f64 * 100.0 / m.total_mb as f64);
                }
            }
            Err(e) => s.errors.push(format!("memory: {e}")),
        }
        match self.pressure.read() {
            Ok(p) => {
                s.pressure = p.level;
                s.psi_full_avg10 = p.psi_full_avg10;
            }
            Err(e) => s.errors.push(format!("pressure: {e}")),
        }
        let mut slow = self.slow.lock().unwrap_or_else(|e| e.into_inner());
        let fresh = slow.as_ref().is_some_and(|(t, _)| now_ms - *t < self.slow_every_ms);
        if !fresh {
            let mut c = Sample::default();
            match self.thermal.read() {
                Ok(t) => {
                    c.temp_c = t.temp_c;
                    c.thermal_state = t.state;
                }
                Err(e) => c.errors.push(format!("thermal: {e}")),
            }
            match self.idle.idle_s() {
                Ok(i) => c.idle_s = Some(i),
                Err(e) => c.errors.push(format!("idle: {e}")),
            }
            match self.power.on_ac() {
                Ok(a) => c.on_ac = a,
                Err(e) => c.errors.push(format!("power: {e}")),
            }
            *slow = Some((now_ms, c));
        }
        if let Some((_, c)) = slow.as_ref() {
            s.temp_c = c.temp_c;
            s.thermal_state = c.thermal_state.clone();
            s.idle_s = c.idle_s;
            s.on_ac = c.on_ac;
            s.errors.extend(c.errors.iter().cloned());
        }
        s
    }
}

/// Scripted probes for tests: each reading is a shared, mutable cell.
pub mod fake {
    use super::*;
    use std::sync::Arc;

    #[derive(Default)]
    pub struct Cells {
        pub mem: Mutex<Option<MemReading>>,
        pub pressure: Mutex<PressureReading>,
        pub thermal: Mutex<ThermalReading>,
        pub idle: Mutex<Option<u64>>,
        pub ac: Mutex<Option<bool>>,
        pub disk_mb: Mutex<Option<u64>>,
    }
    pub struct F(pub Arc<Cells>);
    impl MemoryProbe for F {
        fn read(&self) -> Result<MemReading, String> {
            (*self.0.mem.lock().unwrap()).ok_or_else(|| "fake memory probe unset".into())
        }
    }
    impl PressureProbe for F {
        fn read(&self) -> Result<PressureReading, String> {
            Ok(*self.0.pressure.lock().unwrap())
        }
    }
    impl ThermalProbe for F {
        fn read(&self) -> Result<ThermalReading, String> {
            Ok(self.0.thermal.lock().unwrap().clone())
        }
    }
    impl IdleProbe for F {
        fn idle_s(&self) -> Result<u64, String> {
            (*self.0.idle.lock().unwrap()).ok_or_else(|| "idle unknown".into())
        }
    }
    impl PowerProbe for F {
        fn on_ac(&self) -> Result<Option<bool>, String> {
            Ok(*self.0.ac.lock().unwrap())
        }
    }
    impl DiskProbe for F {
        fn free_mb(&self, _p: &Path) -> Result<u64, String> {
            (*self.0.disk_mb.lock().unwrap()).ok_or_else(|| "disk unknown".into())
        }
    }

    pub fn probes(c: &Arc<Cells>) -> Probes {
        let mut p = Probes::new(
            Box::new(F(c.clone())),
            Box::new(F(c.clone())),
            Box::new(F(c.clone())),
            Box::new(F(c.clone())),
            Box::new(F(c.clone())),
        );
        p.slow_every_ms = 0;
        p
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meminfo_and_swap() {
        let m = parse_meminfo(
            "MemTotal:       16384000 kB\nMemFree: 1 kB\nMemAvailable:    4096000 kB\nSwapTotal: 8192000 kB\nSwapFree: 6144000 kB\n",
        )
        .unwrap();
        assert_eq!(m, MemReading { total_mb: 16000, avail_mb: 4000, swap_mb: 2000 });
        assert!(parse_meminfo("MemTotal: 1 kB\n").is_err());
    }

    #[test]
    fn macos_outputs() {
        assert_eq!(parse_swapusage("total = 3072.00M  used = 1971.50M  free = 1100.50M  (encrypted)").unwrap(), 1971);
        assert_eq!(parse_swapusage("total = 8.00G  used = 6.50G  free = 1.50G").unwrap(), 6656);
        assert!(parse_swapusage("garbage").is_err());
        assert_eq!(
            parse_pmset_therm("Note: No thermal warning level has been recorded\nCPU_Speed_Limit = 100\n").state.as_deref(),
            Some("nominal")
        );
        assert_eq!(parse_pmset_therm("CPU_Speed_Limit \t= 70\n").state.as_deref(), Some("serious"));
        assert_eq!(parse_pmset_therm("CPU_Speed_Limit = 30\n").state.as_deref(), Some("critical"));
        assert_eq!(parse_hid_idle("    | |   \"HIDIdleTime\" = 42000000000\n").unwrap(), 42);
        assert!(parse_hid_idle("nothing").is_err());
        let ac = "Now drawing from 'AC Power'\n -InternalBattery-0 (id=1)\t100%; charged; 0:00 remaining present: true\n";
        assert_eq!(parse_pmset_batt(ac), Some(true));
        assert_eq!(parse_pmset_batt(&ac.replace("AC Power", "Battery Power")), Some(false));
        assert_eq!(parse_pmset_batt("Now drawing from 'AC Power'\n"), None, "a desktop has no battery");
    }

    #[test]
    fn linux_outputs() {
        let psi = "some avg10=0.50 avg60=0.20 avg300=0.10 total=123\nfull avg10=21.75 avg60=1.00 avg300=0.50 total=456\n";
        assert_eq!(parse_psi_full(psi).unwrap(), 21.75);
        assert!(parse_psi_full("some avg10=1\n").is_err());
        assert_eq!(parse_logind_idle("IdleHint=no\nIdleSinceHint=0\n", 5_000_000).unwrap(), 0);
        assert_eq!(parse_logind_idle("IdleHint=yes\nIdleSinceHint=1000000\n", 61_000_000).unwrap(), 60);
        assert!(parse_logind_idle("", 1).is_err());
    }

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("corral-guard-{tag}-{}", corral_proto::util::random_b32(8)));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// Fake sysfs trees: hwmon label lookup and the AC / battery decision.
    #[test]
    fn sysfs_trees() {
        let root = tmpdir("hwmon");
        let other = root.join("hwmon0");
        let core = root.join("hwmon3");
        std::fs::create_dir_all(&other).unwrap();
        std::fs::create_dir_all(&core).unwrap();
        std::fs::write(other.join("name"), "acpitz\n").unwrap();
        std::fs::write(core.join("name"), "coretemp\n").unwrap();
        std::fs::write(core.join("temp1_label"), "Core 0\n").unwrap();
        std::fs::write(core.join("temp1_input"), "50000\n").unwrap();
        std::fs::write(core.join("temp2_label"), "Package id 0\n").unwrap();
        std::fs::write(core.join("temp2_input"), "87500\n").unwrap();
        assert_eq!(read_hwmon_temp(&root, "coretemp", "Package id 0").unwrap(), 87.5);
        assert!(read_hwmon_temp(&root, "coretemp", "Nope").is_err());

        let ps = tmpdir("power");
        let mk = |n: &str, ty: &str, online: Option<&str>| {
            std::fs::create_dir_all(ps.join(n)).unwrap();
            std::fs::write(ps.join(n).join("type"), format!("{ty}\n")).unwrap();
            if let Some(o) = online {
                std::fs::write(ps.join(n).join("online"), format!("{o}\n")).unwrap();
            }
        };
        mk("BAT0", "Battery", None);
        mk("AC", "Mains", Some("0"));
        assert_eq!(read_power_supply(&ps).unwrap(), Some(false));
        mk("AC", "Mains", Some("1"));
        assert_eq!(read_power_supply(&ps).unwrap(), Some(true));
        let desktop = tmpdir("desktop");
        assert_eq!(read_power_supply(&desktop).unwrap(), None);
    }

    #[test]
    fn failed_probes_are_reported_not_hidden() {
        let cells = std::sync::Arc::new(fake::Cells::default());
        let p = fake::probes(&cells);
        let s = p.sample(1000);
        assert!(s.free_pct.is_none());
        assert!(s.errors.iter().any(|e| e.starts_with("memory:")), "{:?}", s.errors);
        *cells.mem.lock().unwrap() = Some(MemReading { total_mb: 1000, avail_mb: 250, swap_mb: 10 });
        assert_eq!(p.sample(2000).free_pct, Some(25.0));
    }
}
