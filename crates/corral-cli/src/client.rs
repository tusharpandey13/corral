//! Calls to the scheduler from the CLI (and from tests).

use crate::snapshot::Snapshot;
use corral_proto::api::*;
use corral_proto::bus::{Bus, unreachable_error};
use corral_proto::envelope::{ApiError, ErrorCode};
use corral_proto::ident::Identity;
use corral_proto::job::{JobSpec, JobView};
use corral_proto::subjects;
use corral_worker::input::{http_download, http_put_file};
use std::path::Path;

pub struct Client {
    pub bus: Bus,
    pub http_url: Option<String>,
}

impl Client {
    pub async fn connect(url: &str, identity: Identity, seed: Option<String>, http_url: Option<String>) -> Result<Client, ApiError> {
        let bus = Bus::connect(url, identity, seed, false, None).await.map_err(|e| {
            let lower = e.to_lowercase();
            if lower.contains("authorization") || lower.contains("authentication") || lower.contains("permission") {
                ApiError::forbidden(format!("{e} (check the nkey seed and identity)"))
            } else {
                unreachable_error(e)
            }
        })?;
        Ok(Client { bus, http_url })
    }

    pub fn subject(&self, verb: &str) -> String {
        subjects::api(&self.bus.identity.slug(), verb)
    }

    fn http(&self) -> Result<&str, ApiError> {
        self.http_url.as_deref().ok_or_else(|| ApiError::bad("no http_url configured (client.http_url or --http-url)"))
    }

    /// Submit; a snapshot input is uploaded over HTTP before the job becomes claimable.
    pub async fn submit(&self, spec: &JobSpec, snapshot: Option<&Snapshot>) -> Result<SubmitReply, ApiError> {
        let reply: SubmitReply = self.bus.call_retry(&self.subject("submit"), spec, 3).await?;
        if let (Some(snap), Some(url), Some(tok)) = (snapshot, &reply.upload_url, &reply.upload_token) {
            let http = self.http()?.to_string();
            let (url, tok, path) = (url.clone(), tok.clone(), snap.path.clone());
            let status = tokio::task::spawn_blocking(move || http_put_file(&http, &url, &tok, &path))
                .await
                .map_err(|e| ApiError::bad(e.to_string()))?
                .map_err(|e| unreachable_error(format!("snapshot upload failed: {e}")))?;
            if status != 200 {
                let code = if status == 413 { ErrorCode::TooLarge } else { ErrorCode::BadRequest };
                return Err(ApiError::new(code, format!("snapshot upload was refused (HTTP {status})")));
            }
        }
        Ok(reply)
    }

    pub async fn get(&self, id: &str) -> Result<JobView, ApiError> {
        self.bus.call_retry(&self.subject("get"), &GetReq { id: id.into() }, 3).await
    }

    pub async fn list(&self, all: bool) -> Result<Vec<JobView>, ApiError> {
        let r: ListReply = self.bus.call_retry(&self.subject("list"), &ListReq { all, state: None, limit: Some(100) }, 3).await?;
        Ok(r.jobs)
    }

    pub async fn workers(&self) -> Result<WorkersReply, ApiError> {
        self.bus.call_retry(&self.subject("workers"), &serde_json::json!({}), 3).await
    }

    pub async fn cancel(&self, id: &str, reason: Option<String>) -> Result<CancelReply, ApiError> {
        self.bus.call_retry(&self.subject("cancel"), &CancelReq { id: id.into(), reason }, 3).await
    }

    pub async fn resubmit(&self, id: &str) -> Result<SubmitReply, ApiError> {
        self.bus.call_retry(&self.subject("resubmit"), &ResubmitReq { id: id.into() }, 3).await
    }

    pub async fn admin(&self, verb: &str, req: &AdminReq) -> Result<AdminReply, ApiError> {
        self.bus.call_retry(&self.subject(&format!("admin.{verb}")), req, 3).await
    }

    /// Download and unpack a job's artifacts into `dir`. Returns the reply (state, attempt).
    pub async fn artifacts(&self, id: &str, dir: &Path) -> Result<ArtifactsReply, ApiError> {
        let r: ArtifactsReply = self.bus.call_retry(&self.subject("artifacts"), &ArtifactsReq { id: id.into(), attempt: None }, 3).await?;
        if r.state != "uploaded" {
            return Err(ApiError::not_found(format!("job {id} has no artifacts (state: {})", r.state)));
        }
        let http = self.http()?.to_string();
        let (url, tok, dir) = (r.url.clone(), r.token.clone(), dir.to_path_buf());
        tokio::task::spawn_blocking(move || -> Result<(), String> {
            std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
            let archive = dir.join(format!(".corral-artifacts-{}.tar.zst", corral_proto::util::random_b32(6)));
            http_download(&http, &url, &tok, &archive)?;
            let f = std::fs::File::open(&archive).map_err(|e| e.to_string())?;
            tar::Archive::new(zstd::Decoder::new(f).map_err(|e| e.to_string())?).unpack(&dir).map_err(|e| e.to_string())?;
            let _ = std::fs::remove_file(&archive);
            Ok(())
        })
        .await
        .map_err(|e| ApiError::bad(e.to_string()))?
        .map_err(ApiError::bad)?;
        Ok(r)
    }
}
