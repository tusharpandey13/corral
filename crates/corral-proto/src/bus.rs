//! Client side of the bus: connect with an nkey and a private inbox prefix, send enveloped requests with retries
//! that keep the same `req_id` (the scheduler answers a retried request with its original reply).

use crate::envelope::{ApiError, ErrorCode, decode_reply, encode_request};
use crate::ident::Identity;
use crate::util::new_req_id;
use async_nats::jetstream;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::time::Duration;

#[derive(Clone)]
pub struct Bus {
    pub client: async_nats::Client,
    pub js: jetstream::Context,
    pub identity: Identity,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

/// Marks an error as "could not reach the scheduler" (CLI exit code 5).
pub fn unreachable_error(msg: impl Into<String>) -> ApiError {
    ApiError::new(ErrorCode::Unavailable, msg).with("unreachable", serde_json::json!(true))
}

pub fn is_unreachable(e: &ApiError) -> bool {
    e.extra.get("unreachable").and_then(|v| v.as_bool()).unwrap_or(false)
}

impl Bus {
    /// Connect. With `retry`, the first connection is retried in the background (daemons); without it a refused
    /// connection is an immediate error (CLI).
    pub async fn connect(
        url: &str,
        identity: Identity,
        seed: Option<String>,
        retry: bool,
        on_state: Option<std::sync::Arc<dyn Fn(bool) + Send + Sync>>,
    ) -> Result<Bus, String> {
        let mut opts = match seed {
            Some(s) => async_nats::ConnectOptions::with_nkey(s),
            None => async_nats::ConnectOptions::new(),
        };
        opts = opts.custom_inbox_prefix(identity.inbox_prefix()).name(identity.to_string()).request_timeout(Some(Duration::from_secs(10)));
        if retry {
            opts = opts.retry_on_initial_connect();
        }
        if let Some(cb) = on_state {
            opts = opts.event_callback(move |ev| {
                let cb = cb.clone();
                async move {
                    match ev {
                        async_nats::Event::Connected => cb(true),
                        async_nats::Event::Disconnected => cb(false),
                        _ => {}
                    }
                }
            });
        }
        let client = opts.connect(url).await.map_err(|e| format!("cannot connect to {url}: {e}"))?;
        let js = jetstream::new(client.clone());
        Ok(Bus { client, js, identity })
    }

    /// One request/reply with a fresh `req_id`.
    pub async fn call<B: Serialize, R: DeserializeOwned>(&self, subject: &str, body: &B) -> Result<R, ApiError> {
        self.call_with_id(subject, body, &new_req_id(now_ms())).await
    }

    pub async fn call_with_id<B: Serialize, R: DeserializeOwned>(&self, subject: &str, body: &B, req_id: &str) -> Result<R, ApiError> {
        let payload = encode_request(&self.identity, req_id, body);
        match self.client.request(subject.to_string(), payload.into()).await {
            Ok(msg) => decode_reply(&msg.payload),
            Err(e) => Err(unreachable_error(format!("no answer from the scheduler on {subject}: {e}"))),
        }
    }

    /// Retry unreachable errors with backoff, keeping one `req_id` so the call is idempotent.
    pub async fn call_retry<B: Serialize, R: DeserializeOwned>(&self, subject: &str, body: &B, attempts: u32) -> Result<R, ApiError> {
        let req_id = new_req_id(now_ms());
        let mut delay = Duration::from_millis(500);
        let mut last = None;
        for _ in 0..attempts.max(1) {
            match self.call_with_id(subject, body, &req_id).await {
                Err(e) if is_unreachable(&e) => {
                    last = Some(e);
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(15));
                }
                other => return other,
            }
        }
        Err(last.unwrap_or_else(|| unreachable_error("scheduler unreachable")))
    }
}

/// Read an nkey seed file (one line).
pub fn read_seed(path: &std::path::Path) -> Result<String, String> {
    std::fs::read_to_string(path).map(|s| s.trim().to_string()).map_err(|e| format!("cannot read nkey seed {}: {e}", path.display()))
}
