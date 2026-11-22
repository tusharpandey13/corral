//! Retention (R-IO-05): finished jobs older than `retention.age` go first, then oldest first until the data is under
//! `retention.cap`. Queued, running and `keep`-flagged jobs (up to `keep_flagged`) are never deleted.

use crate::service::Service;
use std::io;
use std::path::Path;

fn dir_size(p: &Path) -> u64 {
    let Ok(rd) = std::fs::read_dir(p) else {
        return std::fs::metadata(p).map(|m| m.len()).unwrap_or(0);
    };
    rd.flatten()
        .map(|e| match e.metadata() {
            Ok(m) if m.is_dir() => dir_size(&e.path()),
            Ok(m) => m.len(),
            Err(_) => 0,
        })
        .sum()
}

pub fn storage_bytes(svc: &Service) -> u64 {
    dir_size(&svc.jobs_dir()) + dir_size(&svc.inputs_dir())
}

/// Returns the number of jobs deleted.
pub fn run(svc: &Service, now: i64) -> io::Result<usize> {
    let st = &svc.settings;
    let finished = svc.db().finished_jobs().map_err(|e| io::Error::other(format!("{e:?}")))?;
    let mut deleted = 0;
    let cap = st.retention_cap_mb * 1024 * 1024;
    let remove = |id: &str, upload: &Option<String>| -> io::Result<()> {
        let _ = std::fs::remove_dir_all(svc.jobs_dir().join(id));
        svc.db().delete_job(id).map_err(|e| io::Error::other(format!("{e:?}")))?;
        if let Some(u) = upload
            && !svc.db().upload_in_use(u).unwrap_or(true)
            && svc.db().job_for_upload(u).ok().flatten().is_none()
        {
            let _ = std::fs::remove_file(svc.inputs_dir().join(u));
        }
        Ok(())
    };
    let mut remaining = Vec::new();
    for (id, finished_at, keep, upload) in finished {
        let age = now - finished_at;
        let protected = keep && age < st.keep_flagged_ms;
        if age >= st.retention_age_ms && !protected {
            remove(&id, &upload)?;
            deleted += 1;
        } else if !protected {
            remaining.push((id, upload));
        }
    }
    // Still over the size cap: oldest unprotected first.
    for (id, upload) in remaining {
        if storage_bytes(svc) <= cap {
            break;
        }
        remove(&id, &upload)?;
        deleted += 1;
    }
    Ok(deleted)
}
