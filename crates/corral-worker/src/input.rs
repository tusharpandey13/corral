//! Job inputs for the native runtime (RUNTIMES.md section 2): git mirror cache + worktree, snapshot download and
//! unpack, or an empty directory.

use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;

pub fn git(args: &[&str], cwd: Option<&Path>) -> Result<String, String> {
    let mut c = Command::new("git");
    c.args(args).env("GIT_TERMINAL_PROMPT", "0").env("LC_ALL", "C");
    if let Some(d) = cwd {
        c.current_dir(d);
    }
    let out = c.output().map_err(|e| format!("cannot run git: {e}"))?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    } else {
        Err(format!("git {}: {}", args.join(" "), String::from_utf8_lossy(&out.stderr).trim()))
    }
}

fn mirror_path(work_dir: &Path, repo: &str) -> PathBuf {
    let h = corral_proto::util::hex(&Sha256::digest(repo.as_bytes()));
    work_dir.join("mirrors").join(&h[..16])
}

/// Make `<run>/src` a detached worktree of `repo` at `sha`, through a per-repo mirror cache.
pub fn checkout_git(work_dir: &Path, repo: &str, sha: &str, dest: &Path) -> Result<(), String> {
    let mirror = mirror_path(work_dir, repo);
    if !mirror.join("HEAD").exists() {
        std::fs::create_dir_all(mirror.parent().expect("parent")).map_err(|e| e.to_string())?;
        git(&["clone", "--mirror", "--quiet", repo, &mirror.to_string_lossy()], None)?;
    } else {
        git(&["fetch", "--quiet", "--prune", "origin"], Some(&mirror))?;
    }
    if git(&["cat-file", "-e", &format!("{sha}^{{commit}}")], Some(&mirror)).is_err() {
        // A commit that is not on any branch (e.g. a detached or remote-only sha).
        git(&["fetch", "--quiet", "origin", sha], Some(&mirror))?;
    }
    git(&["worktree", "add", "--detach", "--force", &dest.to_string_lossy(), sha], Some(&mirror))?;
    Ok(())
}

pub fn remove_worktree(work_dir: &Path, repo: &str, dest: &Path) {
    let mirror = mirror_path(work_dir, repo);
    let _ = git(&["worktree", "remove", "--force", &dest.to_string_lossy()], Some(&mirror));
    let _ = git(&["worktree", "prune"], Some(&mirror));
}

/// Download `url` (relative to `http_url`) with a bearer token into `dest`; returns the sha256 of the bytes.
pub fn http_download(http_url: &str, path: &str, token: &str, dest: &Path) -> Result<String, String> {
    let url = format!("{}{}", http_url.trim_end_matches('/'), path);
    let resp = ureq::get(&url).header("Authorization", format!("Bearer {token}")).call().map_err(|e| format!("GET {path}: {e}"))?;
    let mut reader = resp.into_body().into_reader();
    let mut f = std::fs::File::create(dest).map_err(|e| e.to_string())?;
    let mut h = Sha256::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = reader.read(&mut buf).map_err(|e| format!("download {path}: {e}"))?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
        std::io::Write::write_all(&mut f, &buf[..n]).map_err(|e| e.to_string())?;
    }
    Ok(corral_proto::util::hex(&h.finalize()))
}

/// Unpack a snapshot `tar.zst` into `dest`; the CLI's `.corral-snapshot/` bookkeeping directory is moved to `meta`.
pub fn unpack_snapshot(archive: &Path, dest: &Path, meta: &Path) -> Result<(), String> {
    let f = std::fs::File::open(archive).map_err(|e| e.to_string())?;
    let dec = zstd::Decoder::new(f).map_err(|e| e.to_string())?;
    std::fs::create_dir_all(dest).map_err(|e| e.to_string())?;
    // `unpack` refuses entries that would land outside `dest` (path traversal).
    tar::Archive::new(dec).unpack(dest).map_err(|e| format!("unpack snapshot: {e}"))?;
    let bookkeeping = dest.join(".corral-snapshot");
    if bookkeeping.exists() {
        std::fs::rename(&bookkeeping, meta).map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// `tar.zst` of `paths` (relative to `base`). Returns whether anything was packed.
pub fn pack(base: &Path, paths: &[String], dest: &Path) -> Result<bool, String> {
    let f = std::fs::File::create(dest).map_err(|e| e.to_string())?;
    let enc = zstd::Encoder::new(f, 3).map_err(|e| e.to_string())?;
    let mut b = tar::Builder::new(enc);
    b.follow_symlinks(false);
    let mut any = false;
    for p in paths {
        if p.starts_with('/') || p.split('/').any(|c| c == "..") {
            return Err(format!("artifact path {p:?} must be relative and stay inside the working directory"));
        }
        let full = base.join(p);
        let name = p.trim_end_matches('/');
        if full.is_dir() {
            b.append_dir_all(name, &full).map_err(|e| e.to_string())?;
            any = true;
        } else if full.exists() {
            b.append_path_with_name(&full, name).map_err(|e| e.to_string())?;
            any = true;
        }
    }
    let enc = b.into_inner().map_err(|e| e.to_string())?;
    enc.finish().map_err(|e| e.to_string())?;
    Ok(any)
}

/// PUT a file with a bearer token and its exact `Content-Length`. Returns the HTTP status.
pub fn http_put_file(http_url: &str, path: &str, token: &str, file: &Path) -> Result<u16, String> {
    let url = format!("{}{}", http_url.trim_end_matches('/'), path);
    let len = std::fs::metadata(file).map_err(|e| e.to_string())?.len();
    let mut f = std::fs::File::open(file).map_err(|e| e.to_string())?;
    let resp = ureq::put(&url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Length", len.to_string())
        .config()
        .http_status_as_error(false)
        .build()
        .send(ureq::SendBody::from_reader(&mut f))
        .map_err(|e| format!("PUT {path}: {e}"))?;
    Ok(resp.status().as_u16())
}
