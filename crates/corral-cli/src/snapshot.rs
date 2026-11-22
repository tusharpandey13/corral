//! Snapshot of the submitter's working tree (R-IO-01, R-IO-02): tracked plus untracked-not-ignored files,
//! including uncommitted changes, as a `tar.zst` with the diff against HEAD and the base commit recorded inside.

use corral_proto::util::hex;
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Debug)]
pub enum SnapError {
    /// Files matching the secret deny-list were found.
    Secrets(Vec<String>),
    TooBig {
        mb: u64,
        cap_mb: u64,
    },
    Git(String),
    Io(String),
}

impl std::fmt::Display for SnapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapError::Secrets(files) => write!(
                f,
                "refusing to snapshot files that look like secrets: {} (add --allow-secret-files to send them anyway)",
                files.join(", ")
            ),
            SnapError::TooBig { mb, cap_mb } => {
                write!(f, "the snapshot is {mb} MB, over the cap of {cap_mb} MB (snapshot_max_mb); trim the tree or use --repo/--ref")
            }
            SnapError::Git(m) | SnapError::Io(m) => write!(f, "{m}"),
        }
    }
}

pub struct Snapshot {
    pub path: PathBuf,
    pub sha256: String,
    pub bytes: u64,
    pub files: usize,
    pub base_sha: Option<String>,
    pub repo_hint: Option<String>,
}

/// Names refused unless `--allow-secret-files` (PROTOCOL.md 10).
pub fn is_secret_name(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path);
    let lower = name.to_ascii_lowercase();
    lower == ".env"
        || lower.starts_with(".env.")
        || lower.ends_with(".pem")
        || lower.ends_with(".key")
        || lower.starts_with("id_")
        || lower.contains("credentials")
}

fn git(root: &Path, args: &[&str]) -> Result<Vec<u8>, SnapError> {
    let out = Command::new("git")
        .args(args)
        .current_dir(root)
        .env("LC_ALL", "C")
        .output()
        .map_err(|e| SnapError::Git(format!("cannot run git: {e}")))?;
    if out.status.success() {
        Ok(out.stdout)
    } else {
        Err(SnapError::Git(format!("git {}: {}", args.join(" "), String::from_utf8_lossy(&out.stderr).trim())))
    }
}

pub fn repo_root(from: &Path) -> Result<PathBuf, SnapError> {
    let out = git(from, &["rev-parse", "--show-toplevel"])?;
    Ok(PathBuf::from(String::from_utf8_lossy(&out).trim()))
}

/// Build the snapshot of the repository containing `from`. `cap_mb` bounds the raw size of the files.
pub fn build(from: &Path, cap_mb: u64, allow_secrets: bool, out_dir: &Path) -> Result<Snapshot, SnapError> {
    let root = repo_root(from)?;
    let listing = git(&root, &["ls-files", "-co", "--exclude-standard", "-z"])?;
    let mut files: Vec<String> =
        listing.split(|b| *b == 0).filter(|p| !p.is_empty()).map(|p| String::from_utf8_lossy(p).into_owned()).collect();
    files.sort();
    files.dedup();
    // Tracked files deleted in the working tree, and submodule directories, are not part of the snapshot.
    files.retain(|f| root.join(f).symlink_metadata().map(|m| !m.is_dir()).unwrap_or(false));

    if !allow_secrets {
        let secrets: Vec<String> = files.iter().filter(|f| is_secret_name(f)).cloned().collect();
        if !secrets.is_empty() {
            return Err(SnapError::Secrets(secrets));
        }
    }
    let raw: u64 = files.iter().filter_map(|f| root.join(f).symlink_metadata().ok()).map(|m| m.len()).sum();
    if raw > cap_mb * 1024 * 1024 {
        return Err(SnapError::TooBig { mb: raw.div_ceil(1024 * 1024), cap_mb });
    }

    let base_sha = git(&root, &["rev-parse", "HEAD"]).ok().map(|o| String::from_utf8_lossy(&o).trim().to_string());
    let patch = git(&root, &["diff", "HEAD"]).unwrap_or_default();
    std::fs::create_dir_all(out_dir).map_err(|e| SnapError::Io(e.to_string()))?;
    let path = out_dir.join(format!("snapshot-{}.tar.zst", corral_proto::util::random_b32(8)));
    let file = std::fs::File::create(&path).map_err(|e| SnapError::Io(e.to_string()))?;
    let enc = zstd::Encoder::new(file, 3).map_err(|e| SnapError::Io(e.to_string()))?;
    let mut b = tar::Builder::new(enc);
    b.follow_symlinks(false);
    for f in &files {
        b.append_path_with_name(root.join(f), f).map_err(|e| SnapError::Io(format!("{f}: {e}")))?;
    }
    let meta = serde_json::json!({ "base_sha": base_sha, "files": files.len() }).to_string();
    for (name, data) in [(".corral-snapshot/code.patch", patch.as_slice()), (".corral-snapshot/meta.json", meta.as_bytes())] {
        let mut h = tar::Header::new_gnu();
        h.set_size(data.len() as u64);
        h.set_mode(0o644);
        h.set_mtime(0);
        b.append_data(&mut h, name, data).map_err(|e| SnapError::Io(e.to_string()))?;
    }
    b.into_inner().and_then(|e| e.finish()).map_err(|e| SnapError::Io(e.to_string()))?;

    let mut hasher = Sha256::new();
    let mut f = std::fs::File::open(&path).map_err(|e| SnapError::Io(e.to_string()))?;
    let mut buf = vec![0u8; 64 * 1024];
    let mut bytes = 0u64;
    loop {
        let n = f.read(&mut buf).map_err(|e| SnapError::Io(e.to_string()))?;
        if n == 0 {
            break;
        }
        bytes += n as u64;
        hasher.update(&buf[..n]);
    }
    let repo_hint = root.file_name().map(|n| n.to_string_lossy().into_owned());
    Ok(Snapshot { path, sha256: hex(&hasher.finalize()), bytes, files: files.len(), base_sha, repo_hint })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sh(dir: &Path, args: &[&str]) {
        let ok = Command::new("git")
            .args(["-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false"])
            .args(args)
            .current_dir(dir)
            .output()
            .unwrap()
            .status
            .success();
        assert!(ok, "git {args:?}");
    }

    fn repo() -> PathBuf {
        let d = std::env::temp_dir().join(format!("corral-snap-{}", corral_proto::util::random_b32(8)));
        std::fs::create_dir_all(&d).unwrap();
        sh(&d, &["init", "-q", "."]);
        std::fs::write(d.join(".gitignore"), "ignored.txt\ntarget/\n").unwrap();
        std::fs::write(d.join("tracked.txt"), "v1\n").unwrap();
        sh(&d, &["add", "."]);
        sh(&d, &["commit", "-q", "-m", "init"]);
        d
    }

    fn unpack(s: &Snapshot) -> PathBuf {
        let dest = std::env::temp_dir().join(format!("corral-unpack-{}", corral_proto::util::random_b32(8)));
        let f = std::fs::File::open(&s.path).unwrap();
        tar::Archive::new(zstd::Decoder::new(f).unwrap()).unpack(&dest).unwrap();
        dest
    }

    /// U-18 / R-IO-01: a dirty tracked file and a new untracked file are kept; an ignored file is excluded.
    #[test]
    fn dirty_and_untracked_kept_ignored_excluded() {
        let d = repo();
        std::fs::write(d.join("tracked.txt"), "v2 dirty\n").unwrap();
        std::fs::write(d.join("new.txt"), "new\n").unwrap();
        std::fs::write(d.join("ignored.txt"), "nope\n").unwrap();
        let s = build(&d, 10, false, &std::env::temp_dir()).unwrap();
        let out = unpack(&s);
        assert_eq!(std::fs::read_to_string(out.join("tracked.txt")).unwrap(), "v2 dirty\n");
        assert!(out.join("new.txt").exists());
        assert!(!out.join("ignored.txt").exists());
        assert!(std::fs::read_to_string(out.join(".corral-snapshot/code.patch")).unwrap().contains("+v2 dirty"));
        assert_eq!(s.base_sha.as_ref().map(|s| s.len()), Some(40));
        assert_eq!(s.sha256.len(), 64);
    }

    /// U-18 / R-IO-02: `.env` and friends are refused naming the file, unless allowed.
    #[test]
    fn secret_files_are_refused_by_name() {
        let d = repo();
        std::fs::write(d.join(".env"), "TOKEN=abc\n").unwrap();
        std::fs::create_dir_all(d.join("sub")).unwrap();
        std::fs::write(d.join("sub/server.pem"), "x").unwrap();
        let e = build(&d, 10, false, &std::env::temp_dir()).err().unwrap();
        let msg = e.to_string();
        assert!(msg.contains(".env") && msg.contains("sub/server.pem"), "{msg}");
        assert!(build(&d, 10, true, &std::env::temp_dir()).is_ok());
    }

    #[test]
    fn size_cap_is_enforced() {
        let d = repo();
        std::fs::write(d.join("big.bin"), vec![1u8; 2 * 1024 * 1024]).unwrap();
        match build(&d, 1, false, &std::env::temp_dir()) {
            Err(SnapError::TooBig { cap_mb: 1, .. }) => {}
            other => panic!("expected TooBig, got {:?}", other.err().map(|e| e.to_string())),
        }
    }

    #[test]
    fn deny_list_names() {
        for n in [".env", ".env.local", "a/b/id_rsa", "x.pem", "k.key", "aws-credentials.json"] {
            assert!(is_secret_name(n), "{n}");
        }
        for n in ["environment.rs", "keyboard.rs", "src/main.rs", "idea.md"] {
            assert!(!is_secret_name(n), "{n}");
        }
    }
}
