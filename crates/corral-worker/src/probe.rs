//! Host probes behind traits, so tests never touch real system state (AGENTS.md "Tests").

use std::collections::{HashMap, HashSet};
use std::process::Command;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemInfo {
    pub total_mb: u64,
    pub available_mb: u64,
}

pub trait MemoryProbe: Send + Sync {
    fn read(&self) -> Result<MemInfo, String>;
}

/// Linux `/proc/meminfo` (`MemAvailable`). Other platforms report "unknown", which fails closed.
pub struct ProcMeminfo;

impl MemoryProbe for ProcMeminfo {
    fn read(&self) -> Result<MemInfo, String> {
        let text = std::fs::read_to_string("/proc/meminfo").map_err(|e| format!("cannot read /proc/meminfo: {e}"))?;
        let field = |name: &str| -> Option<u64> {
            text.lines().find_map(|l| {
                let rest = l.strip_prefix(name)?.strip_prefix(':')?;
                rest.split_whitespace().next()?.parse::<u64>().ok()
            })
        };
        let total = field("MemTotal").ok_or("MemTotal missing")?;
        let avail = field("MemAvailable").ok_or("MemAvailable missing")?;
        Ok(MemInfo { total_mb: total / 1024, available_mb: avail / 1024 })
    }
}

/// macOS: `kern.memorystatus_level` is the free-memory percentage jetsam itself uses (GUARDS.md 2.1), so the
/// available memory is that share of `hw.memsize`. Unreadable or out-of-range values are errors (fail closed).
pub struct MacMemory;

pub fn mac_mem_from(memsize: &str, level: &str) -> Result<MemInfo, String> {
    let bytes: u64 = memsize.trim().parse().map_err(|_| format!("hw.memsize: {:?} is not a number", memsize.trim()))?;
    let pct: u64 = level.trim().parse().map_err(|_| format!("kern.memorystatus_level: {:?} is not a number", level.trim()))?;
    if bytes == 0 || pct > 100 {
        return Err(format!("implausible memory reading (memsize {bytes}, free level {pct}%)"));
    }
    let total_mb = bytes / 1_048_576;
    Ok(MemInfo { total_mb, available_mb: total_mb * pct / 100 })
}

fn sysctl(name: &str) -> Result<String, String> {
    let o = Command::new("sysctl").args(["-n", name]).output().map_err(|e| format!("sysctl {name}: {e}"))?;
    if !o.status.success() {
        return Err(format!("sysctl {name} failed"));
    }
    Ok(String::from_utf8_lossy(&o.stdout).to_string())
}

impl MemoryProbe for MacMemory {
    fn read(&self) -> Result<MemInfo, String> {
        mac_mem_from(&sysctl("hw.memsize")?, &sysctl("kern.memorystatus_level")?)
    }
}

/// The memory probe of this host: macOS `sysctl`, else `/proc/meminfo`.
pub fn host_memory() -> std::sync::Arc<dyn MemoryProbe> {
    if cfg!(target_os = "macos") { std::sync::Arc::new(MacMemory) } else { std::sync::Arc::new(ProcMeminfo) }
}

/// One row of `ps`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proc {
    pub pid: u32,
    pub ppid: u32,
    pub pgid: u32,
    pub rss_kb: u64,
    /// Process start time as printed by `ps` (identifies a pid across reuse).
    pub start: String,
}

pub trait ProcessTable: Send + Sync {
    /// Every process, or `None` when the table could not be read. Callers that guard memory fail closed on `None`:
    /// an unreadable table is not an empty one.
    fn snapshot(&self) -> Option<Vec<Proc>>;

    /// Does the process carry every one of these `KEY=VALUE` environment entries? Linux reads `/proc/<pid>/environ`
    /// (readable for the same user); elsewhere the answer is always `false`. A job's descendants inherit corral's
    /// `CORRAL_JOB_ID` / `CORRAL_ATTEMPT`, so this finds processes that left the group and were reparented to init.
    fn has_env(&self, _pid: u32, _entries: &[String]) -> bool {
        false
    }
}

/// `ps -axo pid=,ppid=,pgid=,rss=,lstart=` (same on Linux and macOS).
pub struct PsTable;

impl ProcessTable for PsTable {
    fn snapshot(&self) -> Option<Vec<Proc>> {
        let out = Command::new("ps").args(["-axo", "pid=,ppid=,pgid=,rss=,lstart="]).env("LC_ALL", "C").output().ok()?;
        if !out.status.success() {
            return None;
        }
        let procs = parse_ps(&String::from_utf8_lossy(&out.stdout));
        // This very process is always in the table: an empty parse means `ps` printed something unusable.
        (!procs.is_empty()).then_some(procs)
    }

    fn has_env(&self, pid: u32, entries: &[String]) -> bool {
        let Ok(raw) = std::fs::read(format!("/proc/{pid}/environ")) else { return false };
        let have: HashSet<&[u8]> = raw.split(|b| *b == 0).collect();
        entries.iter().all(|e| have.contains(e.as_bytes()))
    }
}

pub fn parse_ps(text: &str) -> Vec<Proc> {
    text.lines()
        .filter_map(|l| {
            let mut it = l.split_whitespace();
            let pid = it.next()?.parse().ok()?;
            let ppid = it.next()?.parse().ok()?;
            let pgid = it.next()?.parse().ok()?;
            let rss_kb = it.next()?.parse().ok()?;
            let start = it.collect::<Vec<_>>().join(" ");
            Some(Proc { pid, ppid, pgid, rss_kb, start })
        })
        .collect()
}

/// The start time of one process as `ps` prints it (empty if it is gone). Used to recognise our own pid later.
pub fn start_time_of(pid: u32) -> String {
    Command::new("ps")
        .args(["-o", "lstart=", "-p", &pid.to_string()])
        .env("LC_ALL", "C")
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).split_whitespace().collect::<Vec<_>>().join(" "))
        .unwrap_or_default()
}

/// Every process in the unit: the group of `pgid` plus all descendants of `root` (children that `setsid`).
pub fn unit_members(table: &[Proc], pgid: u32, root: u32) -> Vec<Proc> {
    let mut members: HashSet<u32> = table.iter().filter(|p| p.pgid == pgid).map(|p| p.pid).collect();
    let children: HashMap<u32, Vec<u32>> = table.iter().fold(HashMap::new(), |mut m, p| {
        m.entry(p.ppid).or_default().push(p.pid);
        m
    });
    let mut stack: Vec<u32> = members.iter().copied().chain(std::iter::once(root)).collect();
    while let Some(pid) = stack.pop() {
        for c in children.get(&pid).into_iter().flatten() {
            if members.insert(*c) {
                stack.push(*c);
            }
        }
    }
    table.iter().filter(|p| members.contains(&p.pid) && p.pid != 0 && p.pid != 1).cloned().collect()
}

/// `unit_members` plus every other process that carries the attempt's environment tag (Linux): a double-forked,
/// `setsid` descendant that was reparented to init is still found.
pub fn unit_members_tagged(table: &dyn ProcessTable, snap: &[Proc], pgid: u32, root: u32, tag: &[String]) -> Vec<Proc> {
    let mut members = unit_members(snap, pgid, root);
    if tag.is_empty() {
        return members;
    }
    let known: HashSet<u32> = members.iter().map(|p| p.pid).collect();
    let me = std::process::id();
    for p in snap {
        if p.pid > 1 && p.pid != me && !known.contains(&p.pid) && table.has_env(p.pid, tag) {
            members.push(p.clone());
        }
    }
    members
}

/// A sortable key for a `ps` start time such as `Mon Nov 22 10:00:00 2026` (one-second resolution).
pub fn start_key(lstart: &str) -> Option<u64> {
    let t: Vec<&str> = lstart.split_whitespace().collect();
    let [_dow, mon, day, hms, year] = t.as_slice() else { return None };
    const MONTHS: [&str; 12] = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let m = MONTHS.iter().position(|x| x == mon)? as u64 + 1;
    let (d, y): (u64, u64) = (day.parse().ok()?, year.parse().ok()?);
    let mut hp = hms.split(':').map(|x| x.parse::<u64>());
    let (h, mi, se) = (hp.next()?.ok()?, hp.next()?.ok()?, hp.next()?.ok()?);
    Some((((y * 13 + m) * 32 + d) * 86_400) + h * 3600 + mi * 60 + se)
}

/// What a previous worker process recorded about an attempt.
pub struct AttemptKey<'a> {
    pub job: &'a str,
    pub attempt: u32,
    pub pgid: u32,
    /// Start time of the leader as `ps` printed it when the attempt started (empty = unknown).
    pub leader_start: &'a str,
}

/// Processes of an attempt that are still alive, whether or not its leader is: members of the recorded process
/// group that started no earlier than the leader (so a stranger that reused the number is excluded), and, on Linux,
/// any process carrying the attempt's `CORRAL_JOB_ID`/`CORRAL_ATTEMPT` environment. Never includes this process.
pub fn attempt_survivors(table: &dyn ProcessTable, k: &AttemptKey<'_>) -> Vec<Proc> {
    let Some(snap) = table.snapshot() else { return vec![] };
    let tag = [format!("CORRAL_JOB_ID={}", k.job), format!("CORRAL_ATTEMPT={}", k.attempt)];
    let leader = start_key(k.leader_start);
    let me = std::process::id();
    snap.into_iter()
        .filter(|p| p.pid > 1 && p.pid != me)
        .filter(|p| {
            let in_group = p.pgid == k.pgid && leader.zip(start_key(&p.start)).is_some_and(|(l, s)| s >= l);
            in_group || table.has_env(p.pid, &tag)
        })
        .collect()
}

/// Footprint of a unit in MB (RSS sum; shared pages are counted per process, which errs on the safe side).
pub fn footprint_mb(members: &[Proc]) -> u64 {
    members.iter().map(|p| p.rss_kb).sum::<u64>() / 1024
}

#[cfg(test)]
mod tests {
    use super::*;

    /// M3: the macOS probe turns jetsam's free percentage into available MB and refuses nonsense (fail closed).
    #[test]
    fn mac_memory_reading() {
        let m = mac_mem_from("17179869184\n", "37\n").unwrap();
        assert_eq!((m.total_mb, m.available_mb), (16384, 6062));
        for bad in [("x", "37"), ("17179869184", ""), ("17179869184", "101"), ("0", "10")] {
            assert!(mac_mem_from(bad.0, bad.1).is_err(), "{bad:?}");
        }
    }

    /// The real probes answer on this host (read-only calls).
    #[test]
    fn host_probes_answer_here() {
        let m = host_memory().read().expect("memory probe");
        assert!(m.total_mb > 0 && m.available_mb <= m.total_mb);
    }

    struct Tagged(Vec<Proc>, Vec<u32>);
    impl ProcessTable for Tagged {
        fn snapshot(&self) -> Option<Vec<Proc>> {
            Some(self.0.clone())
        }
        fn has_env(&self, pid: u32, _e: &[String]) -> bool {
            self.1.contains(&pid)
        }
    }

    fn proc(pid: u32, ppid: u32, pgid: u32, start: &str) -> Proc {
        Proc { pid, ppid, pgid, rss_kb: 1024, start: start.into() }
    }

    #[test]
    fn start_keys_order_like_the_calendar() {
        let k = |s| start_key(s).unwrap();
        assert!(k("Mon Nov 22 10:00:00 2026") < k("Mon Nov 22 10:00:01 2026"));
        assert!(k("Mon Nov  2 23:59:59 2026") < k("Mon Nov 22 00:00:00 2026"));
        assert!(k("Sun Dec 31 23:59:59 2026") < k("Fri Jan  1 00:00:00 2027"));
        assert!(start_key("garbage").is_none());
    }

    /// R-EXEC-01 / IT-25 (M1 review): with the leader gone, group members and tagged escapees are survivors; a
    /// stranger that merely shares the group number but started before the leader, and unrelated processes, are not.
    #[test]
    fn survivors_are_found_without_a_leader() {
        let t = Tagged(
            vec![
                proc(500, 1, 400, "Mon Nov 22 10:00:05 2026"), // member of the dead leader's group
                proc(501, 1, 501, "Mon Nov 22 10:00:06 2026"), // double-forked setsid escapee, tagged
                proc(502, 1, 400, "Mon Nov 22 09:00:00 2026"), // reused group number, older than the leader: stranger
                proc(503, 1, 503, "Mon Nov 22 10:00:07 2026"), // unrelated
            ],
            vec![501],
        );
        let k = AttemptKey { job: "j-1", attempt: 1, pgid: 400, leader_start: "Mon Nov 22 10:00:00 2026" };
        let pids: Vec<u32> = attempt_survivors(&t, &k).iter().map(|p| p.pid).collect();
        assert_eq!(pids, vec![500, 501]);
        let none = AttemptKey { leader_start: "", ..k };
        let pids: Vec<u32> = attempt_survivors(&t, &none).iter().map(|p| p.pid).collect();
        assert_eq!(pids, vec![501], "without a recorded start the group rule is not trusted");
    }

    /// An escaped (`setsid`, reparented to init) descendant counts towards the footprint when it carries the tag.
    #[test]
    fn tagged_escapees_count_in_the_unit() {
        let t = Tagged(vec![proc(10, 1, 10, "a"), proc(12, 1, 12, "b"), proc(13, 1, 13, "c")], vec![12]);
        let snap = t.snapshot().unwrap();
        let m = unit_members_tagged(&t, &snap, 10, 10, &["CORRAL_JOB_ID=j-1".into()]);
        assert_eq!(m.iter().map(|p| p.pid).collect::<Vec<_>>(), vec![10, 12]);
    }

    #[test]
    fn ps_parsing_and_unit_membership() {
        let text = "  10     1    10  1000 Mon Nov 22 10:00:00 2026\n  11    10    10  2048 Mon Nov 22 10:00:01 2026\n  12    11    12  4096 Mon Nov 22 10:00:02 2026\n  99     1    99  9999 Mon Nov 22 09:00:00 2026\nbad line\n";
        let t = parse_ps(text);
        assert_eq!(t.len(), 4);
        assert_eq!(t[0].start, "Mon Nov 22 10:00:00 2026");
        let m = unit_members(&t, 10, 10);
        let pids: Vec<u32> = m.iter().map(|p| p.pid).collect();
        assert_eq!(pids, vec![10, 11, 12], "pid 12 called setsid (pgid 12) but is a descendant");
        assert_eq!(footprint_mb(&m), (1000 + 2048 + 4096) / 1024);
        assert!(!pids.contains(&99), "an unrelated process is never part of the unit");
    }
}
