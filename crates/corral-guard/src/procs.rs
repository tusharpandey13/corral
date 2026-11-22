//! Watched processes (GUARDS.md 3): the process table, matchers, `never` rules, protected sets and the units the
//! guard would act on. Everything here only reads; nothing in this crate sends a signal.

use crate::sample::run_cmd;
use crate::state::Registration;
use corral_proto::guardcfg::{MatcherCfg, SelectCfg};
use corral_proto::util::parse_size_mb;
use corral_worker::probe::{Proc, unit_members};
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
pub struct ProcInfo {
    pub pid: u32,
    pub ppid: u32,
    pub pgid: u32,
    pub rss_kb: u64,
    pub user: String,
    /// `ps` start time (identifies the pid across reuse).
    pub start: String,
    pub cmdline: String,
}

impl ProcInfo {
    /// The program's base name (`llama-server`), the only part of a command line the guard ever logs.
    pub fn comm(&self) -> String {
        let argv0 = self.cmdline.split_whitespace().next().unwrap_or("");
        argv0.rsplit('/').next().unwrap_or("").to_string()
    }
    fn as_proc(&self) -> Proc {
        Proc { pid: self.pid, ppid: self.ppid, pgid: self.pgid, rss_kb: self.rss_kb, start: self.start.clone() }
    }
}

pub trait ProcSource: Send + Sync {
    /// Every process, or `None` when the table cannot be read (an unreadable table is not an empty one).
    fn snapshot(&self) -> Option<Vec<ProcInfo>>;
    fn cwd(&self, pid: u32) -> Option<String>;
    fn cgroup(&self, pid: u32) -> Option<String>;
    fn exe(&self, pid: u32) -> Option<String>;
}

fn token(s: &str) -> Option<(&str, &str)> {
    let s = s.trim_start();
    if s.is_empty() {
        return None;
    }
    let end = s.find(char::is_whitespace).unwrap_or(s.len());
    Some((&s[..end], &s[end..]))
}

/// `ps -axww -o pid=,ppid=,pgid=,rss=,user=,lstart=,command=` (same on Linux and macOS).
pub fn parse_ps_full(text: &str) -> Vec<ProcInfo> {
    text.lines()
        .filter_map(|l| {
            let (pid, r) = token(l)?;
            let (ppid, r) = token(r)?;
            let (pgid, r) = token(r)?;
            let (rss, r) = token(r)?;
            let (user, mut r) = token(r)?;
            let mut start = Vec::new();
            for _ in 0..5 {
                let (t, rest) = token(r)?;
                start.push(t);
                r = rest;
            }
            Some(ProcInfo {
                pid: pid.parse().ok()?,
                ppid: ppid.parse().ok()?,
                pgid: pgid.parse().ok()?,
                rss_kb: rss.parse().ok()?,
                user: user.to_string(),
                start: start.join(" "),
                cmdline: r.trim().to_string(),
            })
        })
        .collect()
}

pub struct PsSource;

impl ProcSource for PsSource {
    fn snapshot(&self) -> Option<Vec<ProcInfo>> {
        let out = run_cmd("ps", &["-axww", "-o", "pid=,ppid=,pgid=,rss=,user=,lstart=,command="], Duration::from_secs(5)).ok()?;
        let v = parse_ps_full(&out);
        (!v.is_empty()).then_some(v)
    }
    fn cwd(&self, pid: u32) -> Option<String> {
        if cfg!(target_os = "linux") {
            return std::fs::read_link(format!("/proc/{pid}/cwd")).ok().map(|p| p.to_string_lossy().to_string());
        }
        let out = run_cmd("lsof", &["-a", "-d", "cwd", "-Fn", "-p", &pid.to_string()], Duration::from_secs(3)).ok()?;
        out.lines().find_map(|l| l.strip_prefix('n').map(str::to_string))
    }
    fn cgroup(&self, pid: u32) -> Option<String> {
        let t = std::fs::read_to_string(format!("/proc/{pid}/cgroup")).ok()?;
        t.lines().find_map(|l| l.strip_prefix("0::").map(|p| p.trim_start_matches('/').to_string()))
    }
    fn exe(&self, pid: u32) -> Option<String> {
        std::fs::read_link(format!("/proc/{pid}/exe")).ok().map(|p| p.to_string_lossy().to_string())
    }
}

/// `*` and `?` glob.
pub fn glob_match(pat: &str, s: &str) -> bool {
    let (p, t): (Vec<char>, Vec<char>) = (pat.chars().collect(), s.chars().collect());
    let (mut pi, mut ti, mut star, mut mark) = (0, 0, None, 0);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(sp) = star {
            pi = sp + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

/// All fields must match. An empty select is refused: it would match every process.
#[derive(Debug, Clone)]
pub struct Select {
    registered: Option<String>,
    cmdline: Option<Regex>,
    exclude_cmdline: Option<Regex>,
    exe: Option<Regex>,
    cwd_prefix: Option<String>,
    user: Option<String>,
    cgroup_glob: Option<String>,
    parent_cmdline: Option<Regex>,
}

fn re(key: &str, v: &Option<String>) -> Result<Option<Regex>, String> {
    v.as_deref().map(|s| Regex::new(s).map_err(|e| format!("{key}: {e}"))).transpose()
}

impl Select {
    pub fn compile(c: &SelectCfg) -> Result<Select, String> {
        let s = Select {
            registered: c.registered.clone(),
            cmdline: re("cmdline", &c.cmdline)?,
            exclude_cmdline: re("exclude_cmdline", &c.exclude_cmdline)?,
            exe: re("exe", &c.exe)?,
            cwd_prefix: c.cwd_prefix.clone(),
            user: c.user.clone(),
            cgroup_glob: c.cgroup_glob.clone(),
            parent_cmdline: re("parent_cmdline", &c.parent_cmdline)?,
        };
        let positive = s.registered.is_some()
            || s.cmdline.is_some()
            || s.exe.is_some()
            || s.cwd_prefix.is_some()
            || s.user.is_some()
            || s.cgroup_glob.is_some()
            || s.parent_cmdline.is_some();
        if !positive {
            return Err("select needs at least one of registered, cmdline, exe, cwd_prefix, user, cgroup_glob, parent_cmdline".into());
        }
        Ok(s)
    }

    pub fn wants_cwd(&self) -> bool {
        self.cwd_prefix.is_some()
    }

    /// Cheap fields first, so `cwd` (an `lsof` call on macOS) is read only for processes that pass the rest.
    fn matches(&self, p: &ProcInfo, cx: &Cx<'_>) -> bool {
        if let Some(pat) = &self.registered
            && !cx.reg_of.get(&p.pid).is_some_and(|name| glob_match(pat, name))
        {
            return false;
        }
        if let Some(r) = &self.cmdline
            && !r.is_match(&p.cmdline)
        {
            return false;
        }
        if let Some(r) = &self.exclude_cmdline
            && r.is_match(&p.cmdline)
        {
            return false;
        }
        if let Some(u) = &self.user
            && &p.user != u
        {
            return false;
        }
        if let Some(r) = &self.exe {
            let exe = cx.src.exe(p.pid).unwrap_or_else(|| p.cmdline.split_whitespace().next().unwrap_or("").to_string());
            if !r.is_match(&exe) {
                return false;
            }
        }
        if let Some(r) = &self.parent_cmdline {
            let (mut cur, mut hops, mut found) = (p.ppid, 0, false);
            while let Some(a) = cx.by_pid.get(&cur) {
                if r.is_match(&a.cmdline) {
                    found = true;
                    break;
                }
                hops += 1;
                if a.ppid == cur || a.ppid == 0 || hops > 64 {
                    break;
                }
                cur = a.ppid;
            }
            if !found {
                return false;
            }
        }
        if let Some(g) = &self.cgroup_glob
            && !cx.src.cgroup(p.pid).is_some_and(|c| glob_match(g, &c))
        {
            return false;
        }
        if let Some(prefix) = &self.cwd_prefix
            && !cx.src.cwd(p.pid).is_some_and(|c| c.starts_with(prefix.as_str()))
        {
            return false;
        }
        true
    }
}

struct Cx<'a> {
    src: &'a dyn ProcSource,
    by_pid: HashMap<u32, &'a ProcInfo>,
    reg_of: HashMap<u32, &'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillKind {
    Group,
    Process,
    Cgroup,
    Hook,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnitKind {
    Group,
    Cgroup,
    Process,
}

#[derive(Debug, Clone)]
pub struct Matcher {
    pub name: String,
    pub select: Select,
    pub tier: u32,
    pub heavy: bool,
    pub budget_mb: Option<u64>,
    pub pausable: bool,
    pub kill: KillKind,
    pub unit: UnitKind,
    pub notify: bool,
    pub protected: bool,
    /// Command whose output lists the ids (pids, cgroup names, container ids) to protect.
    pub protect_cmd: Option<Vec<String>>,
    pub protect_ttl_ms: u64,
}

impl Matcher {
    pub fn compile(c: &MatcherCfg) -> Result<Matcher, String> {
        let e = |k: &str, m: String| format!("matchers[{}].{k}: {m}", c.name);
        let kill = match c.kill.as_deref().unwrap_or("group") {
            "group" => KillKind::Group,
            "process" => KillKind::Process,
            "cgroup" => KillKind::Cgroup,
            "hook" => KillKind::Hook,
            o => return Err(e("kill", format!("{o:?} is not group, process, cgroup or hook"))),
        };
        let unit = match c.unit.as_deref().unwrap_or("group") {
            "group" => UnitKind::Group,
            "cgroup" => UnitKind::Cgroup,
            "process" => UnitKind::Process,
            o => return Err(e("unit", format!("{o:?} is not group, cgroup or process"))),
        };
        Ok(Matcher {
            name: c.name.clone(),
            select: Select::compile(&c.select).map_err(|m| e("select", m))?,
            tier: c.tier,
            heavy: c.heavy.unwrap_or(true),
            budget_mb: c.budget.as_deref().map(parse_size_mb).transpose().map_err(|m| e("budget", m))?,
            pausable: c.pausable,
            kill,
            unit,
            notify: c.notify.unwrap_or(true),
            protected: c.protected,
            protect_cmd: c.protect.as_ref().map(|p| p.resolve_cmd.clone()),
            protect_ttl_ms: c
                .protect
                .as_ref()
                .and_then(|p| p.ttl.as_deref())
                .map(corral_proto::util::parse_duration_ms)
                .transpose()
                .map_err(|m| e("protect.ttl", m))?
                .unwrap_or(60_000),
        })
    }
}

/// A set of ids from a matcher's `protect` command: `Err` when it could not be resolved (fail safe: the matcher's
/// units are not shed, R-GUARD-03).
pub type Protect = Result<Vec<String>, String>;

/// A unit the guard watches: what a budget, a pause or a kill would apply to.
#[derive(Debug, Clone)]
pub struct Unit {
    pub key: String,
    pub matcher: String,
    pub tier: u32,
    pub heavy: bool,
    pub pausable: bool,
    pub kill: KillKind,
    pub budget_mb: Option<u64>,
    pub notify: bool,
    pub protected: bool,
    /// The matcher has a `protect` block that could not be resolved.
    pub protect_unresolved: bool,
    /// A member matches a `never` rule or a built-in one: the unit is never signalled.
    pub has_never: bool,
    pub reg_id: Option<String>,
    pub reg_name: Option<String>,
    pub leader: u32,
    pub pids: Vec<u32>,
    /// `ps` start time of each pid in `pids` (same order): what a signal re-verifies (pid reuse, R-GUARD-04).
    pub starts: Vec<String>,
    /// The process group to signal as a whole: set only when the unit *is* a group led by `leader` (pgid == pid).
    pub pgid: Option<u32>,
    pub comm: String,
    pub footprint_mb: u64,
}

pub struct Rules<'a> {
    pub matchers: &'a [Matcher],
    pub never: &'a [Select],
    /// Per matcher name: resolved protect ids.
    pub protect: &'a HashMap<String, Protect>,
    /// The guard's own pid; it and its ancestors are never touched.
    pub me: u32,
}

const BUILTIN_NEVER: [&str; 6] = ["corral", "nats-server", "launchd", "systemd", "loginwindow", "WindowServer"];

fn descendants(children: &HashMap<u32, Vec<u32>>, root: u32) -> Vec<u32> {
    let mut seen = vec![root];
    let mut i = 0;
    while i < seen.len() {
        for c in children.get(&seen[i]).into_iter().flatten() {
            if !seen.contains(c) {
                seen.push(*c);
            }
        }
        i += 1;
    }
    seen
}

/// Resolve the units of one sample. A matcher with no matching process yields nothing.
pub fn build_units(table: &[ProcInfo], src: &dyn ProcSource, rules: &Rules<'_>, regs: &[Registration]) -> Vec<Unit> {
    let by_pid: HashMap<u32, &ProcInfo> = table.iter().map(|p| (p.pid, p)).collect();
    let mut children: HashMap<u32, Vec<u32>> = HashMap::new();
    for p in table {
        children.entry(p.ppid).or_default().push(p.pid);
    }
    // never: built-in rules plus configured ones; the guard and everything above it
    let mut ancestors = HashSet::new();
    let mut cur = rules.me;
    while let Some(p) = by_pid.get(&cur) {
        if !ancestors.insert(cur) || p.ppid == 0 || p.ppid == cur {
            break;
        }
        cur = p.ppid;
    }
    let procs: Vec<Proc> = table.iter().map(ProcInfo::as_proc).collect();
    // registrations: live ones only (leader still has the recorded start time)
    let mut reg_of: HashMap<u32, &str> = HashMap::new();
    let mut reg_members: Vec<(&Registration, Vec<u32>)> = Vec::new();
    for r in regs {
        if by_pid.get(&r.pid).is_none_or(|p| p.start != r.pid_start) {
            continue;
        }
        let members: Vec<u32> = unit_members(&procs, r.pgid, r.pid).iter().map(|p| p.pid).collect();
        for m in &members {
            reg_of.entry(*m).or_insert(&r.name);
        }
        reg_members.push((r, members));
    }
    let cx = Cx { src, by_pid, reg_of };
    let is_never = |p: &ProcInfo| -> bool {
        p.pid <= 1
            || p.pid == rules.me
            || ancestors.contains(&p.pid)
            || BUILTIN_NEVER.contains(&p.comm().as_str())
            || rules.never.iter().any(|s| s.matches(p, &cx))
    };
    let mut never: HashSet<u32> = HashSet::new();
    let mut unit_for = |matcher: &Matcher, key: String, mut pids: Vec<u32>, leader: u32, reg: Option<&Registration>, group: bool| -> Unit {
        pids.sort_unstable();
        pids.dedup();
        let leader_p = cx.by_pid.get(&leader);
        let starts: Vec<String> = pids.iter().map(|p| cx.by_pid.get(p).map(|x| x.start.clone()).unwrap_or_default()).collect();
        let pgid = (group && leader_p.is_some_and(|l| l.pgid == leader)).then_some(leader);
        let footprint_kb: u64 = pids.iter().filter_map(|p| cx.by_pid.get(p)).map(|p| p.rss_kb).sum();
        let ids = rules.protect.get(&matcher.name);
        let protected_by_id = match ids {
            Some(Ok(ids)) => pids.iter().any(|pid| {
                let cg = cx.src.cgroup(*pid).unwrap_or_default();
                ids.iter().any(|id| {
                    !id.is_empty()
                        && (*id == pid.to_string()
                            || cg.contains(id.as_str())
                            || cx.by_pid.get(pid).is_some_and(|p| p.cmdline.contains(id.as_str())))
                })
            }),
            _ => false,
        };
        let unresolved = matcher.protect_cmd.is_some() && !matches!(ids, Some(Ok(v)) if !v.is_empty());
        let has_never = pids.iter().any(|p| {
            let hit = cx.by_pid.get(p).is_some_and(|pi| is_never(pi));
            if hit {
                never.insert(*p);
            }
            hit
        });
        Unit {
            key,
            matcher: matcher.name.clone(),
            tier: reg.and_then(|r| r.tier).unwrap_or(matcher.tier),
            heavy: reg.map(|r| r.heavy).unwrap_or(matcher.heavy),
            pausable: reg.map(|r| r.pausable).unwrap_or(matcher.pausable),
            kill: matcher.kill,
            budget_mb: reg.and_then(|r| r.budget_mb).or(matcher.budget_mb),
            notify: matcher.notify,
            protected: matcher.protected || protected_by_id,
            protect_unresolved: unresolved,
            has_never,
            reg_id: reg.map(|r| r.id.clone()),
            reg_name: reg.map(|r| r.name.clone()),
            leader,
            pids,
            starts,
            pgid,
            comm: leader_p.map(|p| p.comm()).unwrap_or_default(),
            footprint_mb: footprint_kb / 1024,
        }
    };

    let implicit = Matcher {
        name: "registered".into(),
        select: Select {
            registered: Some("*".into()),
            cmdline: None,
            exclude_cmdline: None,
            exe: None,
            cwd_prefix: None,
            user: None,
            cgroup_glob: None,
            parent_cmdline: None,
        },
        tier: 1,
        heavy: true,
        budget_mb: None,
        pausable: false,
        kill: KillKind::Group,
        unit: UnitKind::Group,
        notify: true,
        protected: false,
        protect_cmd: None,
        protect_ttl_ms: 0,
    };
    let mut units = Vec::new();
    let mut claimed: HashSet<u32> = HashSet::new();
    for (r, members) in &reg_members {
        let leader = cx.by_pid[&r.pid];
        let m = rules.matchers.iter().find(|m| m.select.matches(leader, &cx)).unwrap_or(&implicit);
        claimed.extend(members.iter().copied());
        units.push(unit_for(m, format!("reg:{}", r.id), members.clone(), r.pid, Some(r), r.pgid == r.pid));
    }
    // unregistered processes: first matching matcher wins
    let mut matched: Vec<(u32, usize)> = Vec::new();
    for p in table.iter().filter(|p| !claimed.contains(&p.pid) && p.pid > 1 && p.pid != rules.me) {
        if let Some(i) = rules.matchers.iter().position(|m| m.select.matches(p, &cx)) {
            matched.push((p.pid, i));
        }
    }
    matched.sort_unstable();
    let matched_by: HashMap<u32, usize> = matched.iter().copied().collect();
    for (pid, mi) in &matched {
        if claimed.contains(pid) {
            continue;
        }
        let m = &rules.matchers[*mi];
        let p = cx.by_pid[pid];
        let (key, members, leader, group) = match m.unit {
            UnitKind::Process => (format!("p:{pid}@{}", p.start), vec![*pid], *pid, false),
            UnitKind::Cgroup => match src.cgroup(*pid) {
                Some(cg) => {
                    let all: Vec<u32> = matched
                        .iter()
                        .filter(|(q, qi)| qi == mi && src.cgroup(*q).as_deref() == Some(cg.as_str()))
                        .map(|(q, _)| *q)
                        .collect();
                    (format!("c:{cg}"), all, *pid, false)
                }
                None => (format!("p:{pid}@{}", p.start), vec![*pid], *pid, false),
            },
            UnitKind::Group => {
                // The whole group only when its leader is itself matched: a matched process that merely shares a
                // group with something else (an interactive shell's job) is acted on alone, with its descendants.
                let leader_matched = matched_by.get(&p.pgid) == Some(mi) && cx.by_pid.contains_key(&p.pgid);
                if leader_matched {
                    let l = cx.by_pid[&p.pgid];
                    (format!("g:{}@{}", l.pid, l.start), unit_members(&procs, p.pgid, p.pgid).iter().map(|x| x.pid).collect(), l.pid, true)
                } else {
                    (format!("p:{pid}@{}", p.start), descendants(&children, *pid), *pid, false)
                }
            }
        };
        if members.iter().any(|m| claimed.contains(m)) && !members.contains(pid) {
            continue;
        }
        claimed.extend(members.iter().copied());
        units.push(unit_for(m, key, members, leader, None, group));
    }
    units.sort_by(|a, b| (a.tier, &a.key).cmp(&(b.tier, &b.key)));
    units
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use corral_proto::guardcfg::SelectCfg;
    use std::collections::BTreeMap;

    #[derive(Default)]
    pub struct FakeProcs {
        pub table: Vec<ProcInfo>,
        pub cwd: BTreeMap<u32, String>,
        pub cgroup: BTreeMap<u32, String>,
    }
    impl ProcSource for FakeProcs {
        fn snapshot(&self) -> Option<Vec<ProcInfo>> {
            Some(self.table.clone())
        }
        fn cwd(&self, pid: u32) -> Option<String> {
            self.cwd.get(&pid).cloned()
        }
        fn cgroup(&self, pid: u32) -> Option<String> {
            self.cgroup.get(&pid).cloned()
        }
        fn exe(&self, _pid: u32) -> Option<String> {
            None
        }
    }

    pub fn p(pid: u32, ppid: u32, pgid: u32, rss_mb: u64, cmd: &str) -> ProcInfo {
        ProcInfo { pid, ppid, pgid, rss_kb: rss_mb * 1024, user: "u".into(), start: format!("start-{pid}"), cmdline: cmd.into() }
    }

    pub fn matcher(name: &str, tier: u32, select: SelectCfg) -> Matcher {
        Matcher::compile(&MatcherCfg {
            name: name.into(),
            select,
            tier,
            heavy: None,
            budget: None,
            pausable: false,
            kill: None,
            unit: None,
            notify: None,
            protected: false,
            protect: None,
        })
        .unwrap()
    }

    pub fn sel_cmd(r: &str) -> SelectCfg {
        SelectCfg { cmdline: Some(r.into()), ..SelectCfg::default() }
    }

    fn rules<'a>(m: &'a [Matcher], never: &'a [Select], protect: &'a HashMap<String, Protect>) -> Rules<'a> {
        Rules { matchers: m, never, protect, me: 9999 }
    }

    #[test]
    fn ps_line_parsing_keeps_the_command_intact() {
        let t = "  10     1    10  1000 alice Mon Nov  2 10:00:00 2026 /usr/bin/python -m unittest  -v\n  bad\n";
        let v = parse_ps_full(t);
        assert_eq!(v.len(), 1);
        assert_eq!((v[0].pid, v[0].rss_kb, v[0].user.as_str()), (10, 1000, "alice"));
        assert_eq!(v[0].start, "Mon Nov 2 10:00:00 2026");
        assert_eq!(v[0].cmdline, "/usr/bin/python -m unittest  -v");
        assert_eq!(v[0].comm(), "python");
    }

    #[test]
    fn globs() {
        assert!(glob_match("corral:*", "corral:j-1"));
        assert!(!glob_match("corral:*", "run:x"));
        assert!(glob_match("*", ""));
        assert!(glob_match("system.slice/docker-*.scope", "system.slice/docker-ab12.scope"));
        assert!(glob_match("a?c", "abc") && !glob_match("a?c", "ac"));
    }

    #[test]
    fn empty_select_is_refused() {
        assert!(Select::compile(&SelectCfg::default()).is_err());
        assert!(
            Select::compile(&SelectCfg { exclude_cmdline: Some("x".into()), ..SelectCfg::default() }).is_err(),
            "exclusion alone selects nothing"
        );
    }

    /// The guard's own process, its ancestors, and the built-in never set are not signalled by any unit.
    #[test]
    fn never_rules_mark_units_unsignallable() {
        let procs = FakeProcs {
            table: vec![
                p(1, 0, 1, 1, "/sbin/init"),
                p(20, 1, 20, 500, "llama-server --model a"),
                p(21, 1, 21, 500, "llama-server --model b -avd personal"),
                p(30, 1, 30, 10, "corral guard run llama-server"),
            ],
            ..FakeProcs::default()
        };
        let m = vec![matcher("llm", 1, sel_cmd("llama-server"))];
        let never = vec![Select::compile(&sel_cmd("-avd personal")).unwrap()];
        let protect = HashMap::new();
        let units = build_units(&procs.table, &procs, &rules(&m, &never, &protect), &[]);
        let by: HashMap<u32, &Unit> = units.iter().map(|u| (u.leader, u)).collect();
        assert!(!by[&20].has_never);
        assert!(by[&21].has_never, "a configured never rule wins over the matcher");
        assert!(by[&30].has_never, "the corral binary itself is built-in never, whatever its arguments");
    }

    #[test]
    fn exclude_cmdline_and_cwd_prefix() {
        let mut procs = FakeProcs {
            table: vec![
                p(40, 1, 40, 100, "python -m unittest"),
                p(41, 1, 41, 100, "python -m unittest"),
                p(42, 1, 42, 100, "python -m unittest personal"),
            ],
            ..FakeProcs::default()
        };
        procs.cwd.insert(40, "/work/app/x".into());
        procs.cwd.insert(41, "/elsewhere".into());
        procs.cwd.insert(42, "/work/app/y".into());
        let m = vec![matcher(
            "tests",
            3,
            SelectCfg {
                cmdline: Some("python.*-m unittest".into()),
                exclude_cmdline: Some("personal".into()),
                cwd_prefix: Some("/work/app".into()),
                ..SelectCfg::default()
            },
        )];
        let protect = HashMap::new();
        let units = build_units(&procs.table, &procs, &rules(&m, &[], &protect), &[]);
        assert_eq!(units.iter().map(|u| u.leader).collect::<Vec<_>>(), vec![40]);
    }

    /// A matched process that only shares a group with something else is acted on alone: the unit is the group
    /// only when the group's leader is matched (so an interactive shell is never part of a kill).
    #[test]
    fn group_unit_needs_a_matched_leader() {
        let procs = FakeProcs {
            table: vec![
                p(100, 1, 100, 10, "bash -l"),              // the user's shell, group leader
                p(101, 100, 100, 700, "llama-server -m x"), // matched, but the leader is not
                p(102, 101, 100, 50, "helper"),
                p(200, 1, 200, 600, "llama-server -m y"), // matched leader
                p(201, 200, 200, 50, "worker child"),
            ],
            ..FakeProcs::default()
        };
        let m = vec![matcher("llm", 1, sel_cmd("llama-server"))];
        let protect = HashMap::new();
        let units = build_units(&procs.table, &procs, &rules(&m, &[], &protect), &[]);
        let by: HashMap<u32, &Unit> = units.iter().map(|u| (u.leader, u)).collect();
        assert_eq!(by[&101].pids, vec![101, 102], "the shell (100) is not in the unit");
        assert_eq!(by[&200].pids, vec![200, 201]);
        assert_eq!(by[&200].footprint_mb, 650);
    }

    #[test]
    fn registrations_validate_by_start_time_and_use_the_registered_select() {
        let procs = FakeProcs { table: vec![p(300, 1, 300, 20, "sleep 100"), p(301, 300, 300, 30, "child")], ..FakeProcs::default() };
        let m = vec![matcher("corral-jobs", 0, SelectCfg { registered: Some("corral:*".into()), ..SelectCfg::default() })];
        let reg = |start: &str, name: &str| Registration {
            id: "r1".into(),
            name: name.into(),
            pid: 300,
            pgid: 300,
            pid_start: start.into(),
            tier: None,
            budget_mb: Some(40),
            pausable: true,
            heavy: true,
            since_ms: 0,
        };
        let protect = HashMap::new();
        let u = build_units(&procs.table, &procs, &rules(&m, &[], &protect), &[reg("start-300", "corral:j-1")]);
        assert_eq!(u.len(), 1);
        assert_eq!((u[0].tier, u[0].matcher.as_str(), u[0].budget_mb, u[0].pausable), (0, "corral-jobs", Some(40), true));
        assert_eq!(u[0].pids, vec![300, 301]);
        // pid reuse: the recorded start time differs, so this is not our unit
        assert!(build_units(&procs.table, &procs, &rules(&m, &[], &protect), &[reg("another-start", "corral:j-1")]).is_empty());
        // a registration no matcher selects still counts, under the implicit tier 1
        let u = build_units(&procs.table, &procs, &rules(&m, &[], &protect), &[reg("start-300", "run:x")]);
        assert_eq!((u[0].tier, u[0].matcher.as_str()), (1, "registered"));
    }

    #[test]
    fn protect_ids_by_pid_cgroup_and_cmdline_and_unresolved() {
        let mut procs = FakeProcs {
            table: vec![
                p(400, 1, 400, 10, "shim -id abc123 x"),
                p(401, 1, 401, 10, "shim -id def456 x"),
                p(402, 1, 402, 10, "shim -id zzz999 x"),
                p(403, 1, 403, 10, "shim -id fff000 x"),
            ],
            ..FakeProcs::default()
        };
        procs.cgroup.insert(402, "system.slice/docker-other.scope".into());
        procs.cgroup.insert(403, "system.slice/docker-fff000.scope".into());
        let mut mc = MatcherCfg {
            name: "containers".into(),
            select: sel_cmd("^shim"),
            tier: 2,
            heavy: None,
            budget: None,
            pausable: false,
            kill: None,
            unit: Some("process".into()),
            notify: None,
            protected: false,
            protect: Some(corral_proto::guardcfg::ProtectCfg { resolve_cmd: vec!["x".into()], ttl: None }),
        };
        let m = vec![Matcher::compile(&mc).unwrap()];
        let mut protect: HashMap<String, Protect> = HashMap::new();
        protect.insert("containers".into(), Ok(vec!["abc123".into(), "def456".into()]));
        protect.insert("containers".into(), Ok(vec!["abc123".into(), "def456".into(), "fff000".into()]));
        let units = build_units(&procs.table, &procs, &rules(&m, &[], &protect), &[]);
        let by: HashMap<u32, &Unit> = units.iter().map(|u| (u.leader, u)).collect();
        assert!(by[&400].protected && by[&401].protected && by[&403].protected, "by cmdline id and by cgroup");
        assert!(!by[&402].protected);
        assert!(!by[&402].protect_unresolved);
        protect.insert("containers".into(), Err("docker not running".into()));
        let units = build_units(&procs.table, &procs, &rules(&m, &[], &protect), &[]);
        assert!(units.iter().all(|u| u.protect_unresolved));
        protect.insert("containers".into(), Ok(vec![]));
        assert!(
            build_units(&procs.table, &procs, &rules(&m, &[], &protect), &[]).iter().all(|u| u.protect_unresolved),
            "an empty list is unresolved too"
        );
        mc.protect = None;
        assert!(Matcher::compile(&mc).unwrap().protect_cmd.is_none());
    }
}
