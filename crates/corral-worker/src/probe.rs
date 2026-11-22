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
        let text = std::fs::read_to_string("/proc/meminfo")
            .map_err(|e| format!("cannot read /proc/meminfo: {e} (the macOS memory probe arrives with milestone M3)"))?;
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
    fn snapshot(&self) -> Vec<Proc>;
}

/// `ps -axo pid=,ppid=,pgid=,rss=,lstart=` (same on Linux and macOS).
pub struct PsTable;

impl ProcessTable for PsTable {
    fn snapshot(&self) -> Vec<Proc> {
        let out = Command::new("ps").args(["-axo", "pid=,ppid=,pgid=,rss=,lstart="]).env("LC_ALL", "C").output();
        let Ok(out) = out else { return vec![] };
        parse_ps(&String::from_utf8_lossy(&out.stdout))
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

/// Footprint of a unit in MB (RSS sum; shared pages are counted per process, which errs on the safe side).
pub fn footprint_mb(members: &[Proc]) -> u64 {
    members.iter().map(|p| p.rss_kb).sum::<u64>() / 1024
}

#[cfg(test)]
mod tests {
    use super::*;

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
