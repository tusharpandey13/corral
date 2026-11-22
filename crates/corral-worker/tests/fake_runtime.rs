//! Worker driven by the `FakeRuntime`, talking to a real scheduler `Service` through an in-process link (no NATS).
//! Proves the worker's job semantics: U-23 finish classification end to end, U-24 `pause_on_exit`, U-26.

use corral_proto::api::*;
use corral_proto::envelope::{ApiError, decode_reply, encode_request};
use corral_proto::ident::Identity;
use corral_proto::job::{Class, JobSpec, JobState, JobView};
use corral_sched::FakeClock;
use corral_sched::service::Service;
use corral_sched::settings::Settings;
use corral_sched::store::Store;
use corral_worker::admission::{AdmitConfig, Admitter};
use corral_worker::link::{LogHeaders, SchedLink};
use corral_worker::lock::FakeLock;
use corral_worker::native::NativeRuntime;
use corral_worker::runtime::Runtime;
use corral_worker::settings::WorkerSettings;
use corral_worker::testing::{Behavior, FakeMemory, FakeRuntime};
use corral_worker::worker::Worker;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const T0: i64 = 1_795_000_000_000;

struct Loopback {
    svc: Arc<Service>,
    host: String,
    logs: Mutex<Vec<(String, u64)>>,
}

impl Loopback {
    fn call<B: serde::Serialize, R: serde::de::DeserializeOwned>(&self, subject: &str, body: &B) -> Result<R, ApiError> {
        let bytes = encode_request(&Identity::Worker(self.host.clone()), &corral_proto::util::new_req_id(0), body);
        decode_reply(&self.svc.handle(subject, &bytes))
    }
}

impl SchedLink for Loopback {
    async fn claim(&self, req: &ClaimReq) -> Result<ClaimReply, ApiError> {
        self.call(&format!("corral.work.{}.claim", self.host), req)
    }
    async fn heartbeat(&self, job: &str, hb: &HbReq) -> Result<HbReply, ApiError> {
        self.call(&format!("corral.work.{}.{job}.hb", self.host), hb)
    }
    async fn event(&self, job: &str, ev: &EventReq) -> Result<EventReply, ApiError> {
        self.call(&format!("corral.work.{}.{job}.event", self.host), ev)
    }
    async fn publish_log(&self, stream: &str, _job: &str, h: &LogHeaders, _p: Vec<u8>) -> Result<(), String> {
        self.logs.lock().unwrap().push((stream.to_string(), h.seq));
        assert_eq!(h.stream, stream);
        Ok(())
    }
    async fn publish_progress(&self, _job: &str, _m: &ProgressMsg) {}
    async fn put_presence(&self, _p: Vec<u8>) -> Result<(), String> {
        Ok(())
    }
}

struct Env {
    svc: Arc<Service>,
    clock: Arc<FakeClock>,
    worker: Arc<Worker<Loopback, FakeRuntime>>,
    rt: Arc<FakeRuntime>,
    mem: Arc<FakeMemory>,
}

fn tmpdir() -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("corral-wt-{}", corral_proto::util::random_b32(8)));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn env(behavior: impl Fn(&JobSpec) -> Behavior + Send + Sync + 'static) -> Env {
    let clock = Arc::new(FakeClock::new(T0));
    let (svc, _rx) = Service::new(Settings::for_dir(tmpdir()), Store::open_memory(), clock.clone(), b"k".to_vec());
    std::mem::forget(_rx); // keep the outbox open; nothing consumes it in these tests
    let rt = FakeRuntime::new(behavior);
    let mem = FakeMemory::constant(16_000, 12_000);
    let adm = Arc::new(Admitter::new(
        AdmitConfig { heavy_slots: 1, light_slots: 2, ram_cap_mb: None, admit_floor_mb: Some(1000), lock_ttl_s: 300 },
        mem.clone(),
        Arc::new(FakeLock::default()),
    ));
    let mut st = WorkerSettings::for_dir("w1", tmpdir(), "http://127.0.0.1:1");
    st.hb_interval_ms = 50;
    st.kill_grace_ms = 200;
    st.claim_retry_ms = 100;
    let link = Arc::new(Loopback { svc: svc.clone(), host: "w1".into(), logs: Mutex::new(vec![]) });
    let worker = Worker::new(link, rt.clone(), adm, Arc::new(st));
    worker.spawn_loops();
    Env { svc, clock, worker, rt, mem }
}

fn agent() -> Identity {
    Identity::Agent("laptop".into())
}

impl Env {
    fn submit(&self, spec: &JobSpec) -> String {
        let bytes = encode_request(&agent(), &corral_proto::util::new_req_id(0), spec);
        let r: SubmitReply = decode_reply(&self.svc.handle("corral.api.agent-laptop.submit", &bytes)).expect("submit");
        self.clock.advance_ms(1);
        self.worker.claim_notify.notify_one();
        r.id
    }

    fn job(&self, id: &str) -> JobView {
        let bytes = encode_request(&agent(), &corral_proto::util::new_req_id(0), &GetReq { id: id.into() });
        decode_reply(&self.svc.handle("corral.api.agent-laptop.get", &bytes)).expect("get")
    }

    async fn until(&self, id: &str, want: &[JobState]) -> JobView {
        for _ in 0..200 {
            let j = self.job(id);
            if want.contains(&j.state) {
                return j;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("job {id} never reached {want:?}; now {:?}", self.job(id).state);
    }
}

fn heavy(ram: u64) -> JobSpec {
    let mut s = JobSpec::simple(&["fake"]);
    s.class = Class::Heavy;
    s.ram_mb = Some(ram);
    s
}

/// U-26 / R-RT-01: a fake runtime drives the worker end to end.
#[tokio::test]
async fn fake_runtime_runs_a_job_to_success() {
    let e = env(|_| Behavior::Exit { code: 0, after_ms: 50, stdout: "hi\n".into() });
    let id = e.submit(&heavy(100));
    let j = e.until(&id, &[JobState::Succeeded]).await;
    assert_eq!(j.attempts, 1);
    assert_eq!(j.log_final.get("out"), Some(&1), "the final log sequence is reported with `finished`");
}

/// R-RT-02: the native runtime reports cost tier 0.
#[test]
fn native_runtime_is_tier_zero() {
    let rt = NativeRuntime::new(tmpdir(), "/usr/bin:/bin".into(), vec![]);
    assert_eq!(rt.describe().cost_tier, 0);
    assert_eq!(rt.describe().name, "native");
}

/// U-23 / R-QUEUE-04: an unexplained signal ends `killed:external_signal`; the job is never requeued.
#[tokio::test]
async fn external_signal_is_killed_and_final() {
    let e = env(|_| Behavior::ExternalSignal { after_ms: 100 });
    let id = e.submit(&heavy(100));
    let j = e.until(&id, &[JobState::Killed]).await;
    assert_eq!(j.reason.as_deref(), Some("external_signal"));
    e.clock.advance_s(3600);
    e.svc.tick().unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let j = e.job(&id);
    assert_eq!((j.state, j.attempts), (JobState::Killed, 1), "killed is final: no new attempt");
}

/// R-EXEC-02: the worker enforces `timeout` (TERM first); the job ends `killed:timeout`.
#[tokio::test]
async fn timeout_kills_with_term_first() {
    let e = env(|_| Behavior::Hang);
    let mut s = heavy(100);
    s.timeout_s = 1;
    let id = e.submit(&s);
    let j = e.until(&id, &[JobState::Killed]).await;
    assert_eq!(j.reason.as_deref(), Some("timeout"));
    assert_eq!(e.rt.signals.lock().unwrap().first(), Some(&corral_worker::runtime::UnitSignal::Term));
}

/// A job that ignores TERM is KILLed after `kill_grace` (R-EXEC-01); here the fake dies on TERM, so no KILL.
#[tokio::test]
async fn cancel_kills_and_reports_cancelled() {
    let e = env(|_| Behavior::Hang);
    let id = e.submit(&heavy(100));
    e.until(&id, &[JobState::Running]).await;
    let bytes = encode_request(&agent(), "c1", &CancelReq { id: id.clone(), reason: Some("test".into()) });
    let _: CancelReply = decode_reply(&e.svc.handle("corral.api.agent-laptop.cancel", &bytes)).unwrap();
    // The heartbeat reply carries the cancel flag (covers a lost push).
    let j = e.until(&id, &[JobState::Cancelled]).await;
    assert_eq!(j.attempts, 1);
}

/// R-QUEUE-04 / G1: exceeding the declared RAM by the slack kills the job with `ram_exceeded`.
#[tokio::test]
async fn budget_overrun_is_killed_ram_exceeded() {
    let e = env(|_| Behavior::Grow { footprint_mb: 300 });
    let id = e.submit(&heavy(100));
    let j = e.until(&id, &[JobState::Killed]).await;
    assert_eq!(j.reason.as_deref(), Some("ram_exceeded"));
    assert_eq!(j.attempts, 1);
}

/// U-24 / R-EXEC-06: an exit code in `pause_on_exit` pauses the worker with the reason; nothing else is claimed.
#[tokio::test]
async fn pause_on_exit_pauses_the_worker() {
    let e = env(|_| Behavior::Exit { code: 4, after_ms: 50, stdout: String::new() });
    let mut s = heavy(100);
    s.pause_on_exit = vec![4];
    let first = e.submit(&s);
    e.until(&first, &[JobState::Failed]).await;
    assert!(
        e.worker.adm.paused().is_some_and(|r| r.contains("exit with code 4") || r.contains("exited with code 4")),
        "{:?}",
        e.worker.adm.paused()
    );
    let second = e.submit(&heavy(100));
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(e.job(&second).state, JobState::Queued, "a paused worker claims nothing");
    e.worker.handle_ctl(&Ctl { v: 1, action: "resume".into(), job: None, lease_token: None, by: None, reason: None });
    e.until(&second, &[JobState::Failed]).await;
}

/// R-ADM-01 / R-ADM-03 end to end: memory that is too low at start declines; the job stays queued, attempts 0.
#[tokio::test]
async fn memory_decline_keeps_the_job_queued_with_a_reason() {
    let e = env(|_| Behavior::Exit { code: 0, after_ms: 10, stdout: String::new() });
    e.mem.set(1_500); // 1500 - 900 = 600 MB left, below the 1000 MB admit floor
    let id = e.submit(&heavy(900));
    tokio::time::sleep(Duration::from_millis(600)).await;
    let j = e.job(&id);
    assert_eq!(j.state, JobState::Queued);
    assert_eq!(j.attempts, 0);
    assert!(j.waiting.iter().any(|w| w.text.contains("memory")), "{:?}", j.waiting);
    e.mem.set(12_000);
    e.worker.claim_notify.notify_one();
    e.until(&id, &[JobState::Succeeded]).await;
}

fn light_undeclared() -> JobSpec {
    let mut s = JobSpec::simple(&["fake"]);
    s.class = Class::Light;
    s.ram_mb = None;
    s
}

/// R-QUEUE-04 / R-ADM-02 / G1 (M1 review): a light job that declares no RAM is never unlimited: it is admitted
/// against `light_ram_default` and killed `ram_exceeded` past that default times the slack.
#[tokio::test]
async fn light_job_without_ram_is_killed_past_the_default_budget() {
    let e = env(|_| Behavior::Grow { footprint_mb: 2_000 });
    let id = e.submit(&light_undeclared());
    let j = e.until(&id, &[JobState::Killed]).await;
    assert_eq!(j.reason.as_deref(), Some("ram_exceeded"));
}

#[tokio::test]
async fn light_job_without_ram_is_admitted_against_the_default() {
    let e = env(|_| Behavior::Exit { code: 0, after_ms: 10, stdout: String::new() });
    e.mem.set(1_200); // 1200 - 512 (default) = 688 MB left, below the 1000 MB admit floor
    let id = e.submit(&light_undeclared());
    tokio::time::sleep(Duration::from_millis(600)).await;
    let j = e.job(&id);
    assert_eq!((j.state, j.attempts), (JobState::Queued, 0), "a zero reservation would have admitted it");
    assert!(j.waiting.iter().any(|w| w.text.contains("memory")), "{:?}", j.waiting);
    e.mem.set(12_000);
    e.worker.claim_notify.notify_one();
    e.until(&id, &[JobState::Succeeded]).await;
}

/// R-EXEC-01 (M1 review test gap): a job that ignores TERM is KILLed after `kill_grace`, and only then.
#[tokio::test]
async fn term_ignoring_job_is_killed_after_the_grace() {
    use corral_worker::runtime::UnitSignal;
    let e = env(|_| Behavior::IgnoreTerm);
    let mut s = heavy(100);
    s.timeout_s = 1;
    let id = e.submit(&s);
    let j = e.until(&id, &[JobState::Killed]).await;
    assert_eq!(j.reason.as_deref(), Some("timeout"));
    let sigs = e.rt.signals.lock().unwrap().clone();
    assert_eq!(sigs, vec![UnitSignal::Term, UnitSignal::Kill], "TERM first, KILL once after the grace");
}

/// G1 / R-QUEUE-04 (M1 review): a footprint that cannot be measured (process table unreadable) is not "zero": the
/// job is stopped `ram_exceeded` after a few samples instead of running unwatched.
#[tokio::test]
async fn unmeasurable_footprint_fails_closed() {
    let e = env(|_| Behavior::Unmeasurable);
    let id = e.submit(&heavy(100));
    let j = e.until(&id, &[JobState::Killed]).await;
    assert_eq!(j.reason.as_deref(), Some("ram_exceeded"));
}
