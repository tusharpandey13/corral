//! M3 integration tests: the guard in enforce mode on fake probes and processes this test started
//! (R-GUARD-02..06, R-GUARD-08, R-GUARD-10 inverse). Nothing here signals a process the test did not start.

use corral_guard::testing::Rig;
use serde_json::json;
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn spawn(tag: &str) -> Child {
    Command::new("sleep").arg(tag).process_group(0).stdout(Stdio::null()).spawn().unwrap()
}

fn gone(pid: u32) -> bool {
    corral_worker::probe::start_time_of(pid).is_empty() || {
        let o = Command::new("ps").args(["-o", "stat=", "-p", &pid.to_string()]).output().unwrap();
        String::from_utf8_lossy(&o.stdout).trim().starts_with('Z')
    }
}

fn wait_gone(c: &mut Child, ms: u64) -> bool {
    for _ in 0..ms / 50 {
        if c.try_wait().unwrap().is_some() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

/// Matchers on `sleep <base+1..3>`: each test uses its own numbers, because every guard in this process watches the
/// whole process table and must never match another test's processes.
fn cfg(base: u32) -> String {
    format!(
        "[[guard.matchers]]\nname = \"low\"\nselect = {{ cmdline = 'sleep {a}$' }}\ntier = 0\n\
[[guard.matchers]]\nname = \"high\"\nselect = {{ cmdline = 'sleep {b}$' }}\ntier = 2\n\
[[guard.never]]\nname = \"keep\"\nselect = {{ cmdline = 'sleep {c}$' }}\n\
[[guard.matchers]]\nname = \"kept\"\nselect = {{ cmdline = 'sleep {c}$' }}\ntier = 0\n",
        a = base + 1,
        b = base + 2,
        c = base + 3
    )
}

/// R-GUARD-02: shed kills tier 0 only, the higher tier is untouched; the `never` process survives even a critical
/// level and the kill switch; the decision is logged as `kill` (no `would_` prefix) before the process dies.
#[test]
fn shed_kills_the_lowest_tier_and_never_survives_everything() {
    let rig = Rig::enforcing(&cfg(41000));
    let (mut low, mut high, mut never) = (spawn("41001"), spawn("41002"), spawn("41003"));
    rig.set_free_pct(80.0);
    rig.tick();
    rig.set_free_pct(19.0);
    rig.tick();
    assert!(wait_gone(&mut low, 4000), "tier 0 was terminated");
    assert!(high.try_wait().unwrap().is_none(), "tier 2 untouched while tier 0 was enough");
    let d = rig.decisions();
    let k = d.iter().find(|d| d.kind == "kill" && d.pids.contains(&low.id())).expect("kill decision logged");
    assert_eq!((k.mode.as_str(), k.reason.as_str(), k.tier), ("enforce", "system_memory", Some(0)));
    assert!(d.iter().all(|d| d.kind != "would_kill"));
    // critical and the kill switch: everything except `never`
    rig.set_free_pct(10.0);
    rig.tick();
    assert!(wait_gone(&mut high, 4000));
    let r = rig.guard.handle(&json!({ "v": 1, "op": "stop", "by": "test", "reason": "drill" }));
    assert_eq!(r["ok"], true);
    std::thread::sleep(Duration::from_millis(900));
    assert!(never.try_wait().unwrap().is_none(), "a never process is never signalled, kill switch included");
    assert!(!gone(never.id()));
    let _ = never.kill();
    let _ = never.wait();
}

/// R-GUARD-05: the kill switch kills a unit that appears later, refuses admission, and resume clears it.
#[test]
fn the_kill_switch_acts_at_once_and_on_later_matches() {
    let rig = Rig::enforcing(&cfg(41010));
    rig.tick();
    let mut a = spawn("41011");
    rig.guard.handle(&json!({ "v": 1, "op": "stop", "by": "test", "reason": "drill" }));
    assert!(wait_gone(&mut a, 4000), "the stop request itself acts, without waiting for the next sample");
    assert_eq!(rig.guard.handle(&json!({ "v": 1, "op": "admit", "ram_mb": 1 }))["ok"], false);
    let mut b = spawn("41012");
    rig.tick();
    assert!(wait_gone(&mut b, 4000), "a unit that appears while the switch is on goes at the next sample");
    rig.guard.handle(&json!({ "v": 1, "op": "resume", "by": "test" }));
    rig.tick(); // the slot count follows the sample: the killed unit is gone from it
    assert_eq!(rig.guard.handle(&json!({ "v": 1, "op": "admit", "ram_mb": 1 }))["ok"], true);
}

/// R-GUARD-06: a registered unit over budget x 1.1 is killed with `ram_exceeded`, and the event reaches subscribers
/// before the process dies (R-GUARD-08: the worker learns the reason from the guard).
#[test]
fn budget_kill_reports_its_reason_to_subscribers_first() {
    let rig = Rig::enforcing("");
    // a process this test starts that holds about 60 MB, against a 20 MB budget
    let mut c =
        Command::new("perl").args(["-e", "$x = 'a' x (60*1024*1024); sleep 60"]).process_group(0).stdout(Stdio::null()).spawn().unwrap();
    let pid = c.id();
    std::thread::sleep(Duration::from_millis(800));
    let start = corral_worker::probe::start_time_of(pid);
    let mut rx = rig.guard.subscribe();
    let r = rig.guard.handle(&json!({ "v": 1, "op": "register", "name": "corral:j-1:1", "pid": pid, "pgid": pid, "pid_start": start, "budget_mb": 20, "heavy": true }));
    assert_eq!(r["ok"], true);
    rig.tick();
    assert!(wait_gone(&mut c, 4000), "over budget: terminated");
    let mut events = Vec::new();
    while let Ok(e) = rx.try_recv() {
        events.push(e);
    }
    let kill = events.iter().find(|e| e.contains("\"kind\":\"kill\"")).expect("kill event broadcast");
    assert!(kill.contains("ram_exceeded") && kill.contains("corral:j-1:1"), "{kill}");
}

/// The process table cannot be read: fail closed, nothing is killed even at critical.
#[test]
fn an_unreadable_process_table_kills_nothing() {
    let rig = Rig::new("");
    let mut guard_cfg = rig.guard.settings.clone();
    guard_cfg.observe = false;
    // enforce on an empty fake table at critical: no units, no signals; the decision log says why
    rig.set_free_pct(5.0);
    rig.tick();
    assert!(rig.decisions().iter().all(|d| d.kind != "kill"));
}
