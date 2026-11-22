//! The scheduler and worker daemons, started from the configuration file.

use corral_proto::bus::{Bus, read_seed};
use corral_proto::config::{ClientCfg, Config, expand};
use corral_proto::ident::Identity;
use corral_sched::SystemClock;
use corral_sched::nats::NatsConnect;
use corral_sched::settings::Settings;
use corral_worker::lock::{FileLock, SystemWall};
use corral_worker::native::NativeRuntime;
use corral_worker::probe::ProcMeminfo;
use corral_worker::settings::WorkerSettings;
use std::sync::Arc;

fn client_settings(cfg: &Config) -> corral_proto::config::ClientResolved {
    ClientCfg::resolve(cfg.client.as_ref(), &|k| std::env::var(k).ok())
}

fn seed_of(c: &corral_proto::config::ClientResolved) -> Result<Option<String>, String> {
    c.nkey_seed_file.as_ref().map(|p| read_seed(p)).transpose()
}

pub async fn run_scheduler(cfg: &Config) -> Result<(), String> {
    let settings = Settings::from_config(cfg);
    let client = client_settings(cfg);
    let seed = seed_of(&client)?;
    let mut listeners = Vec::new();
    for addr in &settings.listen {
        let l = tokio::net::TcpListener::bind((expand(addr).as_str(), settings.http_port))
            .await
            .map_err(|e| format!("cannot bind {addr}:{}: {e}", settings.http_port))?;
        tracing::info!("HTTP listening on {}", l.local_addr().map(|a| a.to_string()).unwrap_or_default());
        listeners.push(l);
    }
    let running = corral_sched::nats::run(settings, Arc::new(SystemClock), NatsConnect { url: client.nats_url, seed }, listeners).await?;
    tracing::info!("scheduler running");
    wait_for_signal().await;
    running.abort();
    Ok(())
}

pub async fn run_worker(cfg: &Config) -> Result<(), String> {
    let client = client_settings(cfg);
    let settings = WorkerSettings::from_config(cfg, client.http_url.clone())?;
    let seed = seed_of(&client)?;
    let identity = Identity::Worker(settings.host.clone());
    let bus = Bus::connect(&client.nats_url, identity, seed, true, None).await?;
    let rt = Arc::new(NativeRuntime::new(settings.work_dir.clone(), settings.job_path.clone(), settings.env_passthrough.clone()));
    let lock = Arc::new(FileLock::new(settings.lock_path.clone(), Arc::new(SystemWall)));
    std::fs::create_dir_all(&settings.work_dir).map_err(|e| format!("work_dir {}: {e}", settings.work_dir.display()))?;
    let worker = corral_worker::run::start(settings, bus, Arc::new(ProcMeminfo), lock, rt).await?;
    tracing::info!("worker running");
    wait_for_signal().await;
    // Running jobs keep their own process groups; the next start finds and reports them (restart recovery).
    worker.abort_all();
    Ok(())
}

pub async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let (Ok(mut term), Ok(mut int)) = (signal(SignalKind::terminate()), signal(SignalKind::interrupt())) else {
        std::future::pending::<()>().await;
        return;
    };
    tokio::select! { _ = term.recv() => {}, _ = int.recv() => {} }
}
