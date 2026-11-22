//! Start a worker on the bus: recovery, subscriptions, loops.

use crate::admission::{AdmitConfig, Admitter};
use crate::link::NatsLink;
use crate::lock::LockStore;
use crate::probe::MemoryProbe;
use crate::runtime::Runtime;
use crate::settings::WorkerSettings;
use crate::worker::Worker;
use corral_proto::api::Ctl;
use corral_proto::bus::Bus;
use corral_proto::subjects;
use futures::StreamExt;
use std::sync::Arc;

pub async fn start<R: Runtime>(
    settings: WorkerSettings,
    bus: Bus,
    mem: Arc<dyn MemoryProbe>,
    lock: Arc<dyn LockStore>,
    rt: Arc<R>,
) -> Result<Arc<Worker<NatsLink, R>>, String> {
    let adm = Arc::new(Admitter::new(
        AdmitConfig {
            heavy_slots: settings.heavy_slots,
            light_slots: settings.light_slots,
            ram_cap_mb: settings.ram_cap_mb,
            admit_floor_mb: settings.admit_floor_mb,
            lock_ttl_s: settings.lock_ttl_s,
        },
        mem,
        lock,
    ));
    let host = settings.host.clone();
    let link = Arc::new(NatsLink { bus: bus.clone(), host: host.clone() });
    let worker = Worker::new(link, rt, adm, Arc::new(settings));

    let mut ctl = bus.client.subscribe(subjects::ctl(&host)).await.map_err(|e| e.to_string())?;
    let mut wake = bus.client.subscribe(subjects::wake(&host)).await.map_err(|e| e.to_string())?;
    worker.sweep_runs();
    worker.recover().await;
    {
        let w = worker.clone();
        worker.spawn_task(tokio::spawn(async move {
            while let Some(m) = ctl.next().await {
                if let Ok(c) = serde_json::from_slice::<Ctl>(&m.payload) {
                    w.handle_ctl(&c);
                }
            }
        }));
    }
    {
        let w = worker.clone();
        worker.spawn_task(tokio::spawn(async move {
            while wake.next().await.is_some() {
                w.claim_notify.notify_one();
            }
        }));
    }
    worker.spawn_loops();
    worker.watch_guard();
    Ok(worker)
}
