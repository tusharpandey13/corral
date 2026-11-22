//! The worker's connection to the scheduler, behind a trait so the run loop is testable without a network.

use async_nats::HeaderMap;
use corral_proto::api::{ClaimReply, ClaimReq, EventReply, EventReq, HbReply, HbReq, ProgressMsg};
use corral_proto::bus::Bus;
use corral_proto::envelope::ApiError;
use corral_proto::subjects;
use std::future::Future;

/// Headers carried by every log chunk (PROTOCOL.md section 8). There is deliberately no lease token: followers can
/// read these headers, and a lease token must never be readable by anyone but the worker and the scheduler.
#[derive(Debug, Clone)]
pub struct LogHeaders {
    pub msg_id: String,
    pub seq: u64,
    pub attempt: u32,
    /// `out`, `err` or `sys`: followers get logs through a push consumer, which delivers to a private subject, so
    /// the stream name travels in a header too.
    pub stream: String,
}

pub trait SchedLink: Send + Sync + 'static {
    fn claim(&self, req: &ClaimReq) -> impl Future<Output = Result<ClaimReply, ApiError>> + Send;
    fn heartbeat(&self, job: &str, hb: &HbReq) -> impl Future<Output = Result<HbReply, ApiError>> + Send;
    /// Attempt events are retried (same `req_id`) while the scheduler is unreachable.
    fn event(&self, job: &str, ev: &EventReq) -> impl Future<Output = Result<EventReply, ApiError>> + Send;
    /// Publish one log chunk and wait for the stream's ack.
    fn publish_log(&self, stream: &str, job: &str, h: &LogHeaders, payload: Vec<u8>) -> impl Future<Output = Result<(), String>> + Send;
    fn publish_progress(&self, job: &str, msg: &ProgressMsg) -> impl Future<Output = ()> + Send;
    /// Put the presence value (KV `corral_workers.<host>`).
    fn put_presence(&self, payload: Vec<u8>) -> impl Future<Output = Result<(), String>> + Send;
}

pub struct NatsLink {
    pub bus: Bus,
    pub host: String,
}

impl SchedLink for NatsLink {
    async fn claim(&self, req: &ClaimReq) -> Result<ClaimReply, ApiError> {
        // Same req_id on each retry: the scheduler answers a repeated claim with the original reply, so a lost
        // reply cannot strand a claimed job until the claim deadline.
        self.bus.call_retry(&subjects::claim(&self.host), req, 3).await
    }

    async fn heartbeat(&self, job: &str, hb: &HbReq) -> Result<HbReply, ApiError> {
        self.bus.call(&subjects::heartbeat(&self.host, job), hb).await
    }

    async fn event(&self, job: &str, ev: &EventReq) -> Result<EventReply, ApiError> {
        self.bus.call_retry(&subjects::attempt_event(&self.host, job), ev, 4).await
    }

    async fn publish_log(&self, stream: &str, job: &str, h: &LogHeaders, payload: Vec<u8>) -> Result<(), String> {
        let mut headers = HeaderMap::new();
        headers.insert("Nats-Msg-Id", h.msg_id.as_str());
        headers.insert("Corral-Seq", h.seq.to_string().as_str());
        headers.insert("Corral-Attempt", h.attempt.to_string().as_str());
        headers.insert("Corral-Stream", h.stream.as_str());
        let ack = self
            .bus
            .js
            .publish_with_headers(subjects::log(&self.host, job, stream), headers, payload.into())
            .await
            .map_err(|e| e.to_string())?;
        ack.await.map(|_| ()).map_err(|e| e.to_string())
    }

    async fn publish_progress(&self, job: &str, msg: &ProgressMsg) {
        let _ = self.bus.client.publish(subjects::progress(&self.host, job), serde_json::to_vec(msg).expect("json").into()).await;
    }

    async fn put_presence(&self, payload: Vec<u8>) -> Result<(), String> {
        let ack =
            self.bus.js.publish(format!("$KV.{}.{}", subjects::KV_WORKERS, self.host), payload.into()).await.map_err(|e| e.to_string())?;
        ack.await.map(|_| ()).map_err(|e| e.to_string())
    }
}
