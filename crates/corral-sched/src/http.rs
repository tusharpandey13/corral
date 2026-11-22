//! HTTP API for bulk bytes (PROTOCOL.md section 10). Control stays on NATS.

use crate::gc;
use crate::service::Service;
use axum::Router;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use corral_proto::job::JobState;
use corral_proto::token::{self, Token, TokenError};
use futures::StreamExt;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

type S = Arc<Service>;

pub fn router(svc: S) -> Router {
    Router::new()
        .route("/v1/inputs/{upload}", put(put_input).get(get_input))
        .route("/v1/jobs/{id}/{attempt}/artifacts", put(put_artifacts))
        .route("/v1/jobs/{id}/artifacts", get(get_artifacts))
        .route("/v1/jobs/{id}/logs/{attempt}/{stream}", get(get_log))
        .route("/v1/workers/{w}/fresh", get(fresh))
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .with_state(svc)
}

fn status(code: StatusCode, msg: &str) -> Response {
    (code, format!("{msg}\n")).into_response()
}

/// Verify the bearer token for `(job, verb)`. 401 for bad or expired tokens, 403 for a token of another scope.
#[allow(clippy::result_large_err)]
fn authorize(svc: &Service, headers: &HeaderMap, job: &str, verb: &str) -> Result<Token, Response> {
    let raw = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer ")).unwrap_or("");
    token::verify_scope(svc.key(), raw, svc.now() / 1000, job, verb).map_err(|e| match e {
        TokenError::WrongScope => status(StatusCode::FORBIDDEN, &e.to_string()),
        _ => status(StatusCode::UNAUTHORIZED, &e.to_string()),
    })
}

/// Stream a request body to `path`, hashing it and enforcing `cap` bytes. Returns (bytes, sha256 hex) or a response.
async fn receive(body: Body, path: &std::path::Path, cap: u64) -> Result<(u64, String), Response> {
    if let Some(p) = path.parent() {
        let _ = tokio::fs::create_dir_all(p).await;
    }
    let tmp = path.with_extension("part");
    let mut f = tokio::fs::File::create(&tmp).await.map_err(|e| status(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()))?;
    let mut hasher = Sha256::new();
    let mut total = 0u64;
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let c = match chunk {
            Ok(c) => c,
            Err(e) => {
                let _ = tokio::fs::remove_file(&tmp).await;
                return Err(status(StatusCode::BAD_REQUEST, &e.to_string()));
            }
        };
        total += c.len() as u64;
        if total > cap {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(status(StatusCode::PAYLOAD_TOO_LARGE, "over the size cap"));
        }
        hasher.update(&c);
        if let Err(e) = f.write_all(&c).await {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(status(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()));
        }
    }
    let _ = f.flush().await;
    let _ = f.sync_all().await;
    drop(f);
    tokio::fs::rename(&tmp, path).await.map_err(|e| status(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()))?;
    Ok((total, corral_proto::util::hex(&hasher.finalize())))
}

fn too_big(headers: &HeaderMap, cap: u64) -> bool {
    headers.get(header::CONTENT_LENGTH).and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok()).is_some_and(|n| n > cap)
}

async fn put_input(State(svc): State<S>, Path(upload): Path<String>, headers: HeaderMap, body: Body) -> Response {
    if let Err(r) = authorize(&svc, &headers, &upload, "input:put") {
        return r;
    }
    let job = match svc.db().job_for_upload(&upload) {
        Ok(Some(j)) => j,
        _ => return status(StatusCode::NOT_FOUND, "unknown upload"),
    };
    if job.state != JobState::Uploading {
        return status(StatusCode::CONFLICT, "upload already completed or job is no longer waiting for it");
    }
    let corral_proto::job::Input::Snapshot { sha256: want, bytes: want_bytes, .. } = &job.spec.input else {
        return status(StatusCode::BAD_REQUEST, "job has no snapshot input");
    };
    let cap = svc.settings.input_max_mb * 1024 * 1024;
    if too_big(&headers, cap) {
        return status(StatusCode::PAYLOAD_TOO_LARGE, "over the size cap");
    }
    let header_sha = headers.get("x-corral-sha256").and_then(|v| v.to_str().ok()).unwrap_or("");
    if !header_sha.is_empty() && !header_sha.eq_ignore_ascii_case(want) {
        return status(StatusCode::BAD_REQUEST, "X-Corral-Sha256 differs from the declared snapshot hash");
    }
    let path = svc.inputs_dir().join(&upload);
    let (n, got) = match receive(body, &path, cap).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    if !got.eq_ignore_ascii_case(want) || n != *want_bytes {
        let _ = tokio::fs::remove_file(&path).await;
        return status(StatusCode::BAD_REQUEST, "uploaded bytes do not match the declared sha256 and size");
    }
    let now = svc.now();
    match svc.db().upload_verified(&upload, now) {
        Ok(fx) => {
            svc.emit(fx);
            status(StatusCode::OK, "ok")
        }
        Err(_) => status(StatusCode::INTERNAL_SERVER_ERROR, "could not record the upload"),
    }
}

/// Serve a file, honouring a single `Range: bytes=a-b` header.
async fn serve_file(path: &std::path::Path, headers: &HeaderMap) -> Response {
    let Ok(mut f) = tokio::fs::File::open(path).await else {
        return status(StatusCode::NOT_FOUND, "not found");
    };
    let len = f.metadata().await.map(|m| m.len()).unwrap_or(0);
    let range = headers.get(header::RANGE).and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("bytes=")).and_then(|v| {
        let (a, b) = v.split_once('-')?;
        let start: u64 = a.parse().ok()?;
        let end: u64 = if b.is_empty() { len.saturating_sub(1) } else { b.parse().ok()? };
        (start <= end && start < len).then_some((start, end.min(len.saturating_sub(1))))
    });
    let (start, end, code) = match range {
        Some((s, e)) => (s, e, StatusCode::PARTIAL_CONTENT),
        None => (0, len.saturating_sub(1), StatusCode::OK),
    };
    if f.seek(std::io::SeekFrom::Start(start)).await.is_err() {
        return status(StatusCode::INTERNAL_SERVER_ERROR, "seek failed");
    }
    let remaining = if len == 0 { 0 } else { end - start + 1 };
    let stream = futures::stream::unfold((f, remaining), |(mut f, left)| async move {
        if left == 0 {
            return None;
        }
        let mut buf = vec![0u8; left.min(64 * 1024) as usize];
        match f.read(&mut buf).await {
            Ok(0) => None,
            Ok(n) => {
                buf.truncate(n);
                Some((Ok::<_, std::io::Error>(bytes::Bytes::from(buf)), (f, left - n as u64)))
            }
            Err(e) => Some((Err(e), (f, 0))),
        }
    });
    let mut resp = Response::new(Body::from_stream(stream));
    *resp.status_mut() = code;
    resp.headers_mut().insert(header::CONTENT_LENGTH, remaining.into());
    if code == StatusCode::PARTIAL_CONTENT {
        resp.headers_mut().insert(header::CONTENT_RANGE, format!("bytes {start}-{end}/{len}").parse().expect("header"));
    }
    resp
}

async fn get_input(State(svc): State<S>, Path(upload): Path<String>, headers: HeaderMap) -> Response {
    let t = match authorize(&svc, &headers, &upload, "input:get") {
        Ok(t) => t,
        Err(r) => return r,
    };
    // Bound to the leased attempt: a revoked attempt's token no longer downloads.
    if !t.lease.as_deref().is_some_and(|l| svc.db().live_token(l)) {
        return status(StatusCode::FORBIDDEN, "the lease for this download is no longer current");
    }
    serve_file(&svc.inputs_dir().join(&upload), &headers).await
}

async fn put_artifacts(State(svc): State<S>, Path((id, attempt)): Path<(String, u32)>, headers: HeaderMap, body: Body) -> Response {
    let t = match authorize(&svc, &headers, &id, "artifacts:put") {
        Ok(t) => t,
        Err(r) => return r,
    };
    let job = match svc.db().get(&id) {
        Ok(Some(j)) => j,
        _ => return status(StatusCode::NOT_FOUND, "unknown job"),
    };
    if job.attempts != attempt || job.token.is_none() || job.token != t.lease {
        return status(StatusCode::FORBIDDEN, "the lease for this upload is no longer current");
    }
    let cap = job.spec.artifacts_max_mb * 1024 * 1024;
    let refuse = |svc: &Service| {
        let _ = svc.db().set_artifacts_state(&id, "refused");
        svc.db().audit_write(svc.now(), "scheduler", "", Some(&id), "artifacts_refused", &format!("over {} MB", job.spec.artifacts_max_mb));
        status(StatusCode::PAYLOAD_TOO_LARGE, "artifacts are over the per-job cap")
    };
    if too_big(&headers, cap) {
        return refuse(&svc);
    }
    let path = svc.jobs_dir().join(&id).join(format!("artifacts-{attempt}.tar.zst"));
    match receive(body, &path, cap).await {
        Ok((n, sha)) => {
            let _ = svc.db().set_artifacts_state(&id, "uploaded");
            let mut out = Response::new(Body::from(format!("{{\"bytes\":{n},\"sha256\":\"{sha}\"}}\n")));
            *out.status_mut() = StatusCode::OK;
            out
        }
        Err(r) if r.status() == StatusCode::PAYLOAD_TOO_LARGE => refuse(&svc),
        Err(r) => r,
    }
}

async fn get_artifacts(
    State(svc): State<S>,
    Path(id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    if let Err(r) = authorize(&svc, &headers, &id, "artifacts:get") {
        return r;
    }
    let attempt = match q.get("attempt").and_then(|a| a.parse::<u32>().ok()) {
        Some(a) => a,
        None => match svc.db().get(&id) {
            Ok(Some(j)) => j.attempts,
            _ => return status(StatusCode::NOT_FOUND, "unknown job"),
        },
    };
    serve_file(&svc.jobs_dir().join(&id).join(format!("artifacts-{attempt}.tar.zst")), &headers).await
}

async fn get_log(State(svc): State<S>, Path((id, attempt, stream)): Path<(String, u32, String)>, headers: HeaderMap) -> Response {
    if let Err(r) = authorize(&svc, &headers, &id, "log:get") {
        return r;
    }
    if !matches!(stream.as_str(), "out" | "err" | "sys") {
        return status(StatusCode::BAD_REQUEST, "stream is out, err or sys");
    }
    serve_file(&svc.jobs_dir().join(&id).join(format!("{attempt}.{stream}.log")), &headers).await
}

async fn fresh(State(svc): State<S>, Path(w): Path<String>) -> Response {
    if svc.worker_fresh(&w) { status(StatusCode::OK, "fresh") } else { status(StatusCode::SERVICE_UNAVAILABLE, "stale or unknown") }
}

async fn healthz(State(svc): State<S>) -> Response {
    let now = svc.now();
    let db = svc.db().writable(now);
    let nats = svc.nats_ok();
    let body = format!("{{\"sqlite\":{db},\"nats\":{nats}}}\n");
    let code = if db && nats { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    (code, body).into_response()
}

async fn metrics(State(svc): State<S>) -> Response {
    let now = svc.now();
    let mut out = String::new();
    let c = svc.counters();
    out.push_str("# TYPE corral_jobs gauge\n");
    for (state, n) in svc.db().counts_by_state().unwrap_or_default() {
        out.push_str(&format!("corral_jobs{{state=\"{state}\"}} {n}\n"));
    }
    out.push_str("# TYPE corral_claims_total counter\n");
    for (k, v) in &c.claims {
        out.push_str(&format!("corral_claims_total{{result=\"{k}\"}} {v}\n"));
    }
    out.push_str("# TYPE corral_declines_total counter\n");
    for (k, v) in &c.declines {
        out.push_str(&format!("corral_declines_total{{reason=\"{k}\"}} {v}\n"));
    }
    out.push_str("# TYPE corral_kills_total counter\n");
    for (k, v) in &c.kills {
        out.push_str(&format!("corral_kills_total{{reason=\"{k}\"}} {v}\n"));
    }
    out.push_str(&format!("# TYPE corral_lease_expiries_total counter\ncorral_lease_expiries_total {}\n", c.lease_expiries));
    out.push_str(&format!("# TYPE corral_lost_total counter\ncorral_lost_total {}\n", c.lost));
    out.push_str("# TYPE corral_worker_up gauge\n");
    for (host, p, seen) in svc.presence_snapshot() {
        let up = (now - seen < svc.settings.presence_ttl_ms) as u8;
        out.push_str(&format!("corral_worker_up{{worker=\"{host}\"}} {up}\n"));
        if let Some(p) = p {
            out.push_str(&format!(
                "corral_worker_admissible_ram_bytes{{worker=\"{host}\"}} {}\n",
                p.admission.admissible_ram_mb * 1024 * 1024
            ));
        }
    }
    out.push_str(&format!("# TYPE corral_storage_bytes gauge\ncorral_storage_bytes {}\n", gc::storage_bytes(&svc)));
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], out).into_response()
}
