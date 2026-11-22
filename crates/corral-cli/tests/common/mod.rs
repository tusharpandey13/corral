//! Integration harness: a real `nats-server` in a temp dir with generated nkeys and per-identity permissions, the
//! scheduler and workers in-process (workers with fake capabilities: fake memory, real native runtime).

#![allow(dead_code)]

use corral_cli::client::Client;
use corral_proto::bus::Bus;
use corral_proto::ident::Identity;
use corral_proto::perms::nats_server_conf;
use corral_sched::SystemClock;
use corral_sched::nats::{NatsConnect, Running};
use corral_sched::settings::Settings;
use corral_worker::lock::{FileLock, SystemWall};
use corral_worker::native::NativeRuntime;
use corral_worker::run::start;
use corral_worker::settings::WorkerSettings;
use corral_worker::testing::FakeMemory;
use corral_worker::worker::Worker;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub fn tmpdir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("corral-it-{tag}-{}", corral_proto::util::random_b32(8)));
    std::fs::create_dir_all(&d).unwrap();
    d
}

pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

pub struct Nats {
    pub dir: PathBuf,
    pub port: u16,
    pub url: String,
    pub seeds: HashMap<String, String>,
    conf: PathBuf,
    child: Option<Child>,
}

fn nats_binary() -> String {
    std::env::var("NATS_SERVER").unwrap_or_else(|_| "nats-server".into())
}

impl Nats {
    /// Start a server with these identities (scheduler, owner, agent:laptop, agent:other, worker:w1, worker:w2 by default).
    pub fn start(extra: &[Identity]) -> Nats {
        let dir = tmpdir("nats");
        let port = free_port();
        let mut ids = vec![
            Identity::Scheduler,
            Identity::Owner,
            Identity::Agent("laptop".into()),
            Identity::Agent("other".into()),
            Identity::Worker("w1".into()),
            Identity::Worker("w2".into()),
        ];
        ids.extend(extra.iter().cloned());
        let mut users = Vec::new();
        let mut seeds = HashMap::new();
        for id in ids {
            let kp = nkeys::KeyPair::new_user();
            seeds.insert(id.to_string(), kp.seed().unwrap());
            users.push((id, kp.public_key()));
        }
        let conf = dir.join("nats.conf");
        std::fs::write(&conf, nats_server_conf(&format!("127.0.0.1:{port}"), &dir.join("js").to_string_lossy(), &users)).unwrap();
        let mut n = Nats { dir, port, url: format!("nats://127.0.0.1:{port}"), seeds, conf, child: None };
        n.spawn();
        n
    }

    fn spawn(&mut self) {
        let log = std::fs::File::create(self.dir.join("nats.log")).unwrap_or_else(|_| panic!("log"));
        let child = Command::new(nats_binary())
            .args(["-c", &self.conf.to_string_lossy()])
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap_or_else(|e| panic!("cannot start nats-server ({e}); install it (brew install nats-server) or set NATS_SERVER"));
        self.child = Some(child);
        let deadline = Instant::now() + Duration::from_secs(10);
        while std::net::TcpStream::connect(("127.0.0.1", self.port)).is_err() {
            assert!(Instant::now() < deadline, "nats-server did not start; see {}", self.dir.join("nats.log").display());
            std::thread::sleep(Duration::from_millis(50));
        }
        std::thread::sleep(Duration::from_millis(300));
    }

    /// Stop and start the server again on the same port and store (a NATS restart).
    pub fn restart(&mut self, down_for: Duration) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
        std::thread::sleep(down_for);
        self.spawn();
    }

    pub fn seed(&self, id: &str) -> String {
        self.seeds[id].clone()
    }

    pub async fn bus(&self, id: &str) -> Bus {
        Bus::connect(&self.url, Identity::parse(id).unwrap(), Some(self.seed(id)), true, None).await.unwrap()
    }
}

impl Drop for Nats {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

pub struct Sched {
    pub running: Running,
    pub http: String,
    pub data_dir: PathBuf,
}

pub async fn start_scheduler(nats: &Nats, data_dir: &Path, tweak: impl FnOnce(&mut Settings)) -> Sched {
    let mut st = Settings::for_dir(data_dir);
    st.http_port = 0;
    tweak(&mut st);
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let running = corral_sched::nats::run(
        st,
        Arc::new(SystemClock),
        NatsConnect { url: nats.url.clone(), seed: Some(nats.seed("scheduler")) },
        vec![l],
    )
    .await
    .unwrap();
    let http = format!("http://{}", running.http_addrs[0]);
    Sched { running, http, data_dir: data_dir.to_path_buf() }
}

impl Sched {
    /// Stop the scheduler without any orderly shutdown (what `kill -9` leaves behind: only committed SQLite state).
    pub fn kill(self) {
        self.running.abort();
        drop(self);
    }
}

pub struct W {
    pub worker: Arc<Worker<corral_worker::link::NatsLink, NativeRuntime>>,
    pub mem: Arc<FakeMemory>,
    pub work_dir: PathBuf,
}

/// A worker with fake memory and the real native runtime.
pub async fn start_worker(nats: &Nats, host: &str, http: &str, work_dir: &Path, tweak: impl FnOnce(&mut WorkerSettings)) -> W {
    let mut st = WorkerSettings::for_dir(host, work_dir, http);
    st.hb_interval_ms = 300;
    st.presence_interval_ms = 1000;
    st.claim_retry_ms = 500;
    st.kill_grace_ms = 500;
    st.admit_floor_mb = Some(1000);
    tweak(&mut st);
    let mem = FakeMemory::constant(64_000, 32_000);
    let bus = nats.bus(&format!("worker:{host}")).await;
    let rt = Arc::new(NativeRuntime::new(st.work_dir.clone(), st.job_path.clone(), st.env_passthrough.clone()));
    let lock = Arc::new(FileLock::new(st.lock_path.clone(), Arc::new(SystemWall)));
    let worker = start(st.clone(), bus, mem.clone(), lock, rt).await.unwrap();
    W { worker, mem, work_dir: work_dir.to_path_buf() }
}

pub async fn client(nats: &Nats, id: &str, http: &str) -> Client {
    Client::connect(&nats.url, Identity::parse(id).unwrap(), Some(nats.seed(id)), Some(http.to_string())).await.unwrap()
}

pub fn spec(cmd: &[&str]) -> corral_proto::job::JobSpec {
    let mut s = corral_proto::job::JobSpec::simple(cmd);
    s.class = corral_proto::job::Class::Heavy;
    s.ram_mb = Some(64);
    s
}

pub async fn until<F, Fut>(what: &str, secs: u64, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if f().await {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Processes whose command line contains `needle` (a test-owned marker such as `sleep 31337`).
pub fn procs_matching(needle: &str) -> Vec<u32> {
    let out = Command::new("ps").args(["-axo", "pid=,command="]).output().unwrap();
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| l.contains(needle) && !l.contains("ps -axo"))
        .filter_map(|l| l.split_whitespace().next()?.parse().ok())
        .collect()
}

pub fn have(cmd: &str) -> bool {
    Command::new(cmd).arg("--version").output().is_ok()
}
