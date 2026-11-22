//! NATS wiring: subscriptions, JetStream streams, the event publisher, the log sink, presence and the tick loop.

use crate::http;
use crate::service::{Out, Service};
use crate::settings::Settings;
use crate::store::{SinkDecision, Store};
use crate::{Clock, gc};
use async_nats::jetstream::consumer::{AckPolicy, pull};
use async_nats::jetstream::{self, kv, stream};
use corral_proto::api::{Presence, ProgressMsg};
use corral_proto::bus::Bus;
use corral_proto::ident::Identity;
use corral_proto::subjects::{self, KV_WORKERS, STREAM_EVENTS, STREAM_LOGS};
use futures::StreamExt;
use std::net::SocketAddr;
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

pub struct NatsConnect {
    pub url: String,
    pub seed: Option<String>,
}

pub struct Running {
    pub service: Arc<Service>,
    pub http_addrs: Vec<SocketAddr>,
    pub bus: Bus,
    handles: Vec<JoinHandle<()>>,
}

impl Running {
    /// Stop every task (the equivalent of killing the process for crash tests: SQLite state stays as committed).
    pub fn abort(&self) {
        for h in &self.handles {
            h.abort();
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        self.abort();
    }
}

fn load_key(settings: &Settings) -> std::io::Result<Vec<u8>> {
    let path = settings.key_file.clone().unwrap_or_else(|| settings.data_dir.join("http.key"));
    if let Ok(k) = std::fs::read(&path) {
        if !k.is_empty() {
            return Ok(k);
        }
    }
    let key = corral_proto::util::random_bytes::<32>().to_vec();
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p)?;
    }
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut f = std::fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(&path)?;
        f.write_all(&key)?;
    }
    Ok(key)
}

pub async fn ensure_streams(js: &jetstream::Context) -> Result<kv::Store, String> {
    let e = |what: &str, err: &dyn std::fmt::Display| format!("{what}: {err}");
    js.get_or_create_stream(stream::Config {
        name: STREAM_LOGS.into(),
        subjects: vec!["corral.log.>".into()],
        max_age: Duration::from_secs(7 * 86_400),
        max_bytes: 4 * 1024 * 1024 * 1024,
        max_message_size: 1024 * 1024,
        discard: stream::DiscardPolicy::Old,
        duplicate_window: Duration::from_secs(120),
        ..Default::default()
    })
    .await
    .map_err(|x| e("create CORRAL_LOGS", &x))?;
    js.get_or_create_stream(stream::Config {
        name: STREAM_EVENTS.into(),
        subjects: vec!["corral.event.>".into(), "corral.progress.>".into()],
        max_age: Duration::from_secs(30 * 86_400),
        max_messages_per_subject: 200,
        ..Default::default()
    })
    .await
    .map_err(|x| e("create CORRAL_EVENTS", &x))?;
    match js.get_key_value(KV_WORKERS).await {
        Ok(s) => Ok(s),
        Err(_) => js
            .create_key_value(kv::Config { bucket: KV_WORKERS.into(), history: 1, max_age: Duration::from_secs(60), ..Default::default() })
            .await
            .map_err(|x| e("create KV corral_workers", &x)),
    }
}

/// Start the scheduler: open the store, connect, create streams, spawn every task.
pub async fn run(
    settings: Settings,
    clock: Arc<dyn Clock>,
    nats: NatsConnect,
    http_listeners: Vec<tokio::net::TcpListener>,
) -> Result<Running, String> {
    std::fs::create_dir_all(settings.data_dir.join("jobs")).map_err(|e| e.to_string())?;
    std::fs::create_dir_all(settings.data_dir.join("inputs")).map_err(|e| e.to_string())?;
    let mut store = Store::open(&settings.data_dir.join("corral.db")).map_err(|e| format!("{e:?}"))?;
    let downtime = store.start_up(clock.now_ms()).map_err(|e| format!("{e:?}"))?;
    if downtime > 0 {
        tracing::info!("scheduler was down {downtime} ms; live leases extended by that much");
    }
    let key = load_key(&settings).map_err(|e| format!("http key: {e}"))?;
    let (svc, mut rx) = Service::new(settings, store, clock, key);

    let flag = svc.clone();
    let bus = Bus::connect(&nats.url, Identity::Scheduler, nats.seed, true, Some(Arc::new(move |ok| flag.set_nats_ok(ok)))).await?;
    let mut handles: Vec<JoinHandle<()>> = Vec::new();

    // JetStream objects may not be creatable until NATS is up; wait for it.
    let kv_store = loop {
        match ensure_streams(&bus.js).await {
            Ok(k) => break k,
            Err(e) => {
                tracing::warn!("waiting for JetStream: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    };
    svc.set_nats_ok(true);

    // API and worker requests.
    for pattern in [subjects::API_ALL, subjects::WORK_ALL] {
        let mut sub = bus.client.subscribe(pattern).await.map_err(|e| e.to_string())?;
        let svc = svc.clone();
        let client = bus.client.clone();
        handles.push(tokio::spawn(async move {
            while let Some(msg) = sub.next().await {
                let reply = svc.handle(msg.subject.as_str(), &msg.payload);
                if let Some(r) = msg.reply {
                    // Never answer outside the caller's own inbox space (R-SEC-02): a hostile reply subject such as a
                    // JetStream API subject must not turn the scheduler into a confused deputy.
                    if subjects::reply_allowed(msg.subject.as_str(), r.as_str()) {
                        let _ = client.publish(r, reply.into()).await;
                    } else {
                        tracing::warn!("ignored a request on {} with a foreign reply subject", msg.subject);
                    }
                }
            }
        }));
    }

    // Outbox: events must reach JetStream in order, so a failed publish is retried before the next one.
    {
        let bus = bus.clone();
        handles.push(tokio::spawn(async move {
            while let Some(out) = rx.recv().await {
                match out {
                    Out::Follow { job, deliver } => {
                        let js = bus.js.clone();
                        tokio::spawn(async move {
                            if let Err(e) = create_follow_consumers(&js, &job, &deliver).await {
                                tracing::warn!(%job, "cannot create follow consumers: {e}");
                            }
                        });
                    }
                    Out::Core { subject, payload } => {
                        let _ = bus.client.publish(subject, payload.into()).await;
                    }
                    Out::Js { subject, payload } => {
                        let mut delay = Duration::from_millis(200);
                        loop {
                            match bus.js.publish(subject.clone(), payload.clone().into()).await {
                                Ok(ack) => {
                                    if ack.await.is_ok() {
                                        break;
                                    }
                                }
                                Err(e) => tracing::warn!("event publish failed, retrying: {e}"),
                            }
                            tokio::time::sleep(delay).await;
                            delay = (delay * 2).min(Duration::from_secs(5));
                        }
                    }
                }
            }
        }));
    }

    // Periodic work.
    {
        let svc = svc.clone();
        handles.push(tokio::spawn(async move {
            let mut last_gc = svc.now();
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if let Err(e) = svc.tick() {
                    tracing::error!("tick failed: {e}");
                }
                let now = svc.now();
                if now - last_gc >= 3_600_000 {
                    last_gc = now;
                    if let Err(e) = gc::run(&svc, now) {
                        tracing::error!("retention failed: {e}");
                    }
                }
            }
        }));
    }

    // Presence: watch the KV bucket.
    {
        let svc = svc.clone();
        handles.push(tokio::spawn(async move {
            loop {
                match kv_store.watch_all().await {
                    Ok(mut w) => {
                        while let Some(entry) = w.next().await {
                            let Ok(entry) = entry else { break };
                            if matches!(entry.operation, kv::Operation::Put)
                                && let Ok(p) = serde_json::from_slice::<Presence>(&entry.value)
                            {
                                // The key is the host; the payload's `from` must agree (R-SEC-03).
                                if entry.key == p.host && p.from == format!("worker:{}", p.host) {
                                    svc.presence_update(&entry.key, Some(p));
                                }
                            }
                        }
                    }
                    Err(e) => tracing::warn!("presence watch failed: {e}"),
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }));
    }

    // Progress.
    {
        let mut sub = bus.client.subscribe("corral.progress.>").await.map_err(|e| e.to_string())?;
        let svc = svc.clone();
        handles.push(tokio::spawn(async move {
            while let Some(msg) = sub.next().await {
                let parts: Vec<&str> = msg.subject.as_str().split('.').collect();
                // corral.progress.<worker>.<id>: the worker in the subject must be the one that holds the job.
                if let ([_, _, worker, id], Ok(p)) = (&parts[..], serde_json::from_slice::<ProgressMsg>(&msg.payload)) {
                    svc.progress(worker, id, p.pct);
                }
            }
        }));
    }

    // Log sink (R-IO-03).
    {
        let svc = svc.clone();
        let js = bus.js.clone();
        handles.push(tokio::spawn(async move {
            loop {
                if let Err(e) = sink_loop(&svc, &js).await {
                    tracing::warn!("log sink restarting: {e}");
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }));
    }

    // HTTP.
    let mut http_addrs = Vec::new();
    for l in http_listeners {
        http_addrs.push(l.local_addr().map_err(|e| e.to_string())?);
        let app = http::router(svc.clone());
        handles.push(tokio::spawn(async move {
            let _ = axum::serve(l, app).await;
        }));
    }

    Ok(Running { service: svc, http_addrs, bus, handles })
}

/// Three ephemeral push consumers (logs, job events, progress) that deliver into the follower's inbox space. They
/// delete themselves once nobody listens any more (`inactive_threshold`).
async fn create_follow_consumers(js: &jetstream::Context, job: &str, deliver: &str) -> Result<(), String> {
    use async_nats::jetstream::consumer::{DeliverPolicy, push};
    for (stream, filter, suffix) in [
        (STREAM_LOGS, subjects::log_filter(job), "log"),
        (STREAM_EVENTS, subjects::job_event(job), "ev"),
        (STREAM_EVENTS, subjects::progress_filter(job), "prog"),
    ] {
        let s = js.get_stream(stream).await.map_err(|e| e.to_string())?;
        s.create_consumer(push::Config {
            deliver_subject: format!("{deliver}.{suffix}"),
            filter_subject: filter,
            ack_policy: AckPolicy::None,
            deliver_policy: DeliverPolicy::All,
            inactive_threshold: Duration::from_secs(30),
            ..Default::default()
        })
        .await
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

async fn sink_loop(svc: &Arc<Service>, js: &jetstream::Context) -> Result<(), String> {
    let stream = js.get_stream(STREAM_LOGS).await.map_err(|e| e.to_string())?;
    let consumer = stream
        .get_or_create_consumer(
            "sink",
            pull::Config {
                durable_name: Some("sink".into()),
                ack_policy: AckPolicy::Explicit,
                ack_wait: Duration::from_secs(30),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| e.to_string())?;
    let mut msgs = consumer.messages().await.map_err(|e| e.to_string())?;
    while let Some(m) = msgs.next().await {
        let m = m.map_err(|e| e.to_string())?;
        let hdr = |k: &str| m.headers.as_ref().and_then(|h| h.get(k)).map(|v| v.as_str().to_string());
        let parsed = subjects::parse_log(m.subject.as_str()).map(|(w, id, s)| (w.to_string(), id.to_string(), s.to_string()));
        let (Some((worker, id, stream_name)), Some(seq), Some(attempt)) =
            (parsed, hdr("Corral-Seq").and_then(|v| v.parse::<u64>().ok()), hdr("Corral-Attempt").and_then(|v| v.parse::<u32>().ok()))
        else {
            let _ = m.ack().await;
            continue;
        };
        let decision = svc.db().sink_decide(&id, attempt, &stream_name, seq, &worker).map_err(|e| format!("{e:?}"))?;
        // A chunk to append: (offset, bytes). A gap that never fills is recorded and skipped after `sink_hold_max`
        // redeliveries (about that many seconds), so one lost chunk cannot stall the archive of every job.
        let write = match decision {
            SinkDecision::Write { offset } => Some((offset, m.payload.to_vec())),
            SinkDecision::Hold { last, offset } if m.info().is_ok_and(|i| i.delivered as u64 >= svc.settings.sink_hold_max) => {
                tracing::warn!(job = %id, stream = %stream_name, "log chunks {}..{} never arrived; recording the gap", last + 1, seq - 1);
                let mut bytes = crate::store::sink_gap_note(last, seq);
                bytes.extend_from_slice(&m.payload);
                Some((offset, bytes))
            }
            SinkDecision::Hold { .. } => {
                let _ = m.ack_with(jetstream::AckKind::Nak(Some(Duration::from_secs(1)))).await;
                None
            }
            SinkDecision::Duplicate | SinkDecision::Drop => {
                let _ = m.ack().await;
                None
            }
        };
        if let Some((offset, bytes)) = write {
            let dir = svc.jobs_dir().join(&id);
            std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
            let f = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(dir.join(format!("{attempt}.{stream_name}.log")))
                .map_err(|e| e.to_string())?;
            f.write_all_at(&bytes, offset).map_err(|e| e.to_string())?;
            f.sync_data().map_err(|e| e.to_string())?;
            svc.db().sink_commit(&id, attempt, &stream_name, seq, offset + bytes.len() as u64).map_err(|e| format!("{e:?}"))?;
            let _ = m.ack().await;
        }
    }
    Ok(())
}
