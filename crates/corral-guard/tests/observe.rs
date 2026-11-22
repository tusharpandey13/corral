//! M2 integration tests: the guard on fake probes in a temp directory (R-GUARD-05, R-GUARD-10, R-ADM-06, ...).

use corral_guard::client::request;
use corral_guard::procs::PsSource;
use corral_guard::testing::Rig;
use serde_json::{Value, json};
use std::process::{Child, Command, Stdio};

const SENTINEL: &str = "31337";

struct Sentinel(Child);
impl Sentinel {
    fn start() -> Sentinel {
        use std::os::unix::process::CommandExt;
        Sentinel(Command::new("sleep").arg(SENTINEL).process_group(0).stdout(Stdio::null()).spawn().unwrap())
    }
    fn pid(&self) -> u32 {
        self.0.id()
    }
    fn state(&self) -> String {
        let o = Command::new("ps").args(["-o", "stat=", "-p", &self.pid().to_string()]).output().unwrap();
        String::from_utf8_lossy(&o.stdout).trim().to_string()
    }
}
impl Drop for Sentinel {
    fn drop(&mut self) {
        // a process this test started itself
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn ask(rig: &Rig, v: Value) -> Value {
    rig.guard.handle(&v)
}

/// R-GUARD-10 / U-08: through every level up to the kill switch the guard logs what it would kill, and the real
/// process it points at is untouched: alive, not stopped, same start time.
#[test]
fn observe_decides_and_never_touches_a_real_process() {
    let rig = Rig::with_real_ps(&format!(
        "[[guard.matchers]]\nname = \"sentinels\"\nselect = {{ cmdline = 'sleep {SENTINEL}' }}\ntier = 1\n[[guard.restore_hooks]]\nname = \"restore\"\non = [\"critical\", \"stop\"]\ncmd = [\"/nonexistent/hook\"]\n"
    ));
    let s = Sentinel::start();
    let start = corral_worker::probe::start_time_of(s.pid());
    assert!(!start.is_empty());
    for pct in [80.0, 19.0, 10.0] {
        rig.set_free_pct(pct);
        rig.tick();
    }
    let r = ask(&rig, json!({ "v": 1, "op": "stop", "by": "test", "reason": "drill" }));
    assert_eq!(r["observed"], true);
    rig.tick();
    let d = rig.decisions();
    let kills: Vec<_> = d.iter().filter(|d| d.kind == "would_kill" && d.pids.contains(&s.pid())).collect();
    assert!(!kills.is_empty(), "the sentinel is a watched unit: {d:#?}");
    assert!(d.iter().all(|d| d.mode == "observe" && d.v == 1 && !d.t.is_empty()));
    assert!(d.iter().any(|d| d.kind == "would_hook"), "restore hook planned, not run");
    assert!(d.iter().any(|d| d.kind == "level" && d.level == "shed") && d.iter().any(|d| d.kind == "level" && d.level == "critical"));
    // the process: alive, running, the same process
    assert!(PsSource_alive(s.pid()));
    let st = s.state();
    assert!(!st.contains('T') && !st.contains('Z') && !st.is_empty(), "stat {st:?}");
    assert_eq!(corral_worker::probe::start_time_of(s.pid()), start);
}

#[allow(non_snake_case)]
fn PsSource_alive(pid: u32) -> bool {
    use corral_guard::procs::ProcSource;
    PsSource.snapshot().unwrap().iter().any(|p| p.pid == pid)
}

/// R-GUARD-05 / U-28: stop -> restart the guard on the same state -> admission still refused, with who and why.
#[test]
fn kill_switch_survives_a_restart_and_admission_follows_it() {
    let rig = Rig::new("");
    rig.tick();
    assert_eq!(ask(&rig, json!({ "v": 1, "op": "admit", "ram_mb": 100 }))["ok"], true);
    ask(&rig, json!({ "v": 1, "op": "stop", "by": "owner", "reason": "drill" }));
    let again = Rig::in_dir(&rig.dir, "");
    again.tick();
    let r = ask(&again, json!({ "v": 1, "op": "admit", "ram_mb": 100 }));
    assert_eq!(r["ok"], false);
    let why = r["reasons"][0]["reason"].as_str().unwrap();
    assert!(why.contains("kill switch on") && why.contains("by owner: drill"), "{why}");
    assert_eq!(ask(&again, json!({ "v": 1, "op": "status" }))["level"], "stop");
    ask(&again, json!({ "v": 1, "op": "resume", "by": "owner" }));
    assert_eq!(ask(&again, json!({ "v": 1, "op": "admit", "ram_mb": 100 }))["ok"], true);
}

/// GUARDS 11: two concurrent `admit --reserve` with one heavy slot: exactly one ok.
#[test]
fn concurrent_reservations_one_slot_exactly_one_wins() {
    let rig = Rig::new("");
    rig.tick();
    let wins: usize = (0..8)
        .map(|i| {
            let g = rig.guard.clone();
            std::thread::spawn(move || {
                g.handle(&json!({ "v": 1, "op": "admit", "name": format!("t{i}"), "ram_mb": 100, "reserve_s": 60 }))["ok"] == true
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|h| h.join().unwrap())
        .filter(|ok| *ok)
        .count();
    assert_eq!(wins, 1);
}

/// A reservation holds the slot and the RAM; the re-check under the lock sees through the caller's own reservation;
/// releasing it frees the slot.
#[test]
fn reservation_lifecycle_and_recheck() {
    let rig = Rig::new("");
    rig.tick();
    let a = ask(&rig, json!({ "v": 1, "op": "admit", "name": "a", "ram_mb": 1000, "reserve_s": 60 }));
    assert_eq!(a["ok"], true);
    let token = a["reservation"].as_str().unwrap().to_string();
    let b = ask(&rig, json!({ "v": 1, "op": "admit", "name": "b", "ram_mb": 1000 }));
    assert_eq!(b["ok"], false);
    assert_eq!(b["reasons"][0]["check"], "heavy_slot", "{b}");
    assert!(b["reasons"][0]["since_ms"].is_i64());
    let recheck = ask(&rig, json!({ "v": 1, "op": "admit", "name": "a", "ram_mb": 1000, "reservation": token }));
    assert_eq!(recheck["ok"], true, "{recheck}");
    ask(&rig, json!({ "v": 1, "op": "unreserve", "token": token }));
    assert_eq!(ask(&rig, json!({ "v": 1, "op": "admit", "name": "b", "ram_mb": 1000 }))["ok"], true);
    // a reservation expires on its own
    let c = ask(&rig, json!({ "v": 1, "op": "admit", "name": "c", "ram_mb": 10, "reserve_s": 5 }));
    assert_eq!(c["ok"], true);
    assert_eq!(ask(&rig, json!({ "v": 1, "op": "admit", "name": "d", "ram_mb": 10 }))["ok"], false);
    rig.clock.0.fetch_add(6_000, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(ask(&rig, json!({ "v": 1, "op": "admit", "name": "d", "ram_mb": 10 }))["ok"], true);
}

/// R-GUARD-06 / U-07 end to end: a registration over budget x 1.1 is decided `ram_exceeded`, once.
#[test]
fn registered_unit_over_budget_is_decided_ram_exceeded() {
    use corral_guard::procs::ProcInfo;
    let rig = Rig::new("[[guard.matchers]]\nname = \"runs\"\nselect = { registered = \"run:*\" }\ntier = 1\n");
    let proc = |rss_mb: u64| ProcInfo {
        pid: 4242,
        ppid: 1,
        pgid: 4242,
        rss_kb: rss_mb * 1024,
        user: "u".into(),
        start: "Mon Nov 22 10:00:00 2026".into(),
        cmdline: "alloc".into(),
    };
    *rig.procs.table.lock().unwrap() = vec![proc(20)];
    let r = ask(
        &rig,
        json!({ "v": 1, "op": "register", "name": "run:alloc", "pid": 4242, "pgid": 4242, "pid_start": "Mon Nov 22 10:00:00 2026", "budget_mb": 50 }),
    );
    assert_eq!(r["ok"], true);
    let again = ask(
        &rig,
        json!({ "v": 1, "op": "register", "name": "run:alloc", "pid": 4242, "pgid": 4242, "pid_start": "Mon Nov 22 10:00:00 2026", "budget_mb": 50 }),
    );
    assert_eq!(again["id"], r["id"], "idempotent by (pgid, pid_start)");
    rig.tick();
    assert!(rig.decisions().iter().all(|d| d.kind != "would_kill"), "20 MB is within budget");
    *rig.procs.table.lock().unwrap() = vec![proc(200)];
    rig.tick();
    rig.tick();
    let kills: Vec<_> = rig.decisions().into_iter().filter(|d| d.kind == "would_kill").collect();
    assert_eq!(kills.len(), 1, "{kills:#?}");
    assert_eq!(
        (kills[0].reason.as_str(), kills[0].unit.as_deref(), kills[0].pids.clone()),
        ("ram_exceeded", Some("run:alloc"), vec![4242])
    );
    // pid reuse: the pid now has another start time, so the registration is dropped, not acted on
    *rig.procs.table.lock().unwrap() = vec![ProcInfo { start: "Mon Nov 22 11:00:00 2026".into(), ..proc(200) }];
    rig.tick();
    assert!(ask(&rig, json!({ "v": 1, "op": "status" }))["registrations"].as_array().unwrap().is_empty());
}

/// Real socket: framing, versioning, errors, `subscribe`, `metrics`, the instance lock (R-GUARD-01 / IT-11).
#[tokio::test(flavor = "multi_thread")]
async fn socket_protocol_end_to_end() {
    let rig = Rig::new("");
    let guard = rig.guard.clone();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(corral_guard::server::serve(guard.clone(), async move {
        let _ = rx.await;
    }));
    let sock = rig.socket();
    for _ in 0..100 {
        if sock.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let call = |op: &'static str, f: Value| {
        let sock = sock.clone();
        tokio::task::spawn_blocking(move || request(&sock, op, f))
    };
    let r = call("admit", json!({ "ram_mb": 100 })).await.unwrap().unwrap();
    assert_eq!((r["v"].clone(), r["ok"].clone(), r["mode"].clone()), (json!(1), json!(true), json!("observe")));
    assert_eq!(call("nonsense", json!({})).await.unwrap().unwrap()["ok"], false);
    assert!(call("admit", json!({ "profile": "measurement" })).await.unwrap().unwrap()["error"].as_str().unwrap().contains("M4"));
    assert!(call("hold", json!({})).await.unwrap().unwrap()["error"].as_str().unwrap().contains("keep-awake provider"));
    let m = call("metrics", json!({})).await.unwrap().unwrap();
    assert!(
        m["text"].as_str().unwrap().contains("corral_guard_level 0")
            && m["text"].as_str().unwrap().contains("corral_guard_admits_total{result=\"ok\"} 1"),
        "{m}"
    );
    // a bad version is refused
    let bad = {
        let sock = sock.clone();
        tokio::task::spawn_blocking(move || {
            use std::io::{BufRead, Write};
            let mut s = std::os::unix::net::UnixStream::connect(sock).unwrap();
            s.write_all(b"{\"v\":2,\"op\":\"status\"}\n").unwrap();
            let mut l = String::new();
            std::io::BufReader::new(s).read_line(&mut l).unwrap();
            l
        })
        .await
        .unwrap()
    };
    assert!(bad.contains("unsupported"), "{bad}");
    // subscribe: an event arrives when a decision is made
    let sub = {
        let sock = sock.clone();
        tokio::task::spawn_blocking(move || {
            use std::io::{BufRead, Write};
            let mut s = std::os::unix::net::UnixStream::connect(sock).unwrap();
            s.set_read_timeout(Some(std::time::Duration::from_secs(10))).unwrap();
            s.write_all(b"{\"v\":1,\"op\":\"subscribe\",\"topics\":[\"actions\"]}\n").unwrap();
            let mut r = std::io::BufReader::new(s);
            let mut ack = String::new();
            r.read_line(&mut ack).unwrap();
            let mut ev = String::new();
            r.read_line(&mut ev).unwrap();
            (ack, ev)
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    call("stop", json!({ "by": "t", "reason": "x" })).await.unwrap().unwrap();
    let (ack, ev) = sub.await.unwrap();
    assert!(ack.contains("\"ok\":true"), "{ack}");
    assert!(ev.contains("\"kind\":\"stop\""), "{ev}");
    // one guard per host
    let first = corral_guard::server::instance_lock(&guard.settings.state_dir.join("x1")).unwrap();
    let second = corral_guard::server::instance_lock(&guard.settings.state_dir.join("x1")).err().unwrap();
    assert!(second.contains("already running"), "{second}");
    drop(first);
    tx.send(()).unwrap();
    server.await.unwrap().unwrap();
    assert!(!sock.exists(), "the socket file is removed on shutdown");
}

const START: &str = "Mon Nov 22 10:00:00 2026";

fn heavy_admit(rig: &Rig, name: &str) -> serde_json::Value {
    rig.guard.handle(&json!({ "v": 1, "op": "admit", "name": name, "ram_mb": 100, "class": "heavy", "reserve_s": 600 }))
}

fn job_proc(pid: u32) -> corral_guard::procs::ProcInfo {
    corral_guard::procs::ProcInfo { pid, ppid: 1, pgid: pid, rss_kb: 1024, user: "u".into(), start: START.into(), cmdline: "job".into() }
}

/// M2 (note 41, R-ADM-01): registering hands the reservation over, but the slot and the RAM stay counted until a
/// sample has seen the unit; a second heavy admit in that gap is refused. After the sample the unit counts for itself
/// and the reservation is gone; unregistering frees the slot.
#[test]
fn a_registered_job_keeps_its_slot_until_a_sample_sees_it() {
    let rig = Rig::new("");
    *rig.procs.table.lock().unwrap() = vec![job_proc(4242)];
    rig.tick();
    let a = heavy_admit(&rig, "corral:j:1");
    assert_eq!(a["ok"], true, "{a}");
    let tok = a["reservation"].as_str().unwrap().to_string();
    let r = rig.guard.handle(&json!({ "v": 1, "op": "register", "name": "corral:j:1", "pid": 4242, "pgid": 4242, "pid_start": START, "heavy": true, "reservation": tok }));
    assert_eq!(r["ok"], true, "{r}");
    let id = r["id"].as_str().unwrap().to_string();
    let b = heavy_admit(&rig, "second");
    assert_eq!(b["ok"], false, "the slot is not free between register and the next sample: {b}");
    assert!(b["reasons"].to_string().contains("heavy_slot"), "{b}");
    rig.tick();
    let st = rig.guard.handle(&json!({ "v": 1, "op": "status" }));
    assert_eq!(st["reservations"].as_array().unwrap().len(), 0, "the unit counts for itself now: {st}");
    assert_eq!(heavy_admit(&rig, "third")["ok"], false, "still one heavy unit running");
    rig.guard.handle(&json!({ "v": 1, "op": "unregister", "id": id }));
    rig.procs.table.lock().unwrap().clear();
    rig.tick();
    assert_eq!(heavy_admit(&rig, "fourth")["ok"], true);
}

/// M2: unregistering before any sample saw the unit also frees the claimed reservation.
#[test]
fn unregistering_frees_a_claimed_reservation() {
    let rig = Rig::new("");
    rig.tick();
    let a = heavy_admit(&rig, "corral:j:1");
    let tok = a["reservation"].as_str().unwrap().to_string();
    let r = rig.guard.handle(
        &json!({ "v": 1, "op": "register", "name": "corral:j:1", "pid": 4242, "pgid": 4242, "pid_start": START, "reservation": tok }),
    );
    rig.guard.handle(&json!({ "v": 1, "op": "unregister", "id": r["id"] }));
    assert_eq!(heavy_admit(&rig, "next")["ok"], true);
}

/// L1: a registration that cannot be persisted is refused and rolled back; the reservation stays unclaimed so the
/// caller's `unreserve` frees it.
#[test]
fn a_registration_that_cannot_be_saved_is_rolled_back() {
    let rig = Rig::new("");
    rig.tick();
    let a = heavy_admit(&rig, "corral:j:1");
    let tok = a["reservation"].as_str().unwrap().to_string();
    std::fs::create_dir(rig.guard.settings.state_dir.join("registrations.json")).unwrap(); // the rename onto it fails
    let r = rig.guard.handle(
        &json!({ "v": 1, "op": "register", "name": "corral:j:1", "pid": 4242, "pgid": 4242, "pid_start": START, "reservation": tok }),
    );
    assert_eq!(r["ok"], false, "{r}");
    let st = rig.guard.handle(&json!({ "v": 1, "op": "status" }));
    assert_eq!(st["registrations"].as_array().unwrap().len(), 0, "not half-registered: {st}");
    rig.guard.handle(&json!({ "v": 1, "op": "unreserve", "token": tok }));
    assert_eq!(heavy_admit(&rig, "next")["ok"], true);
}
