//! Turn `corral submit` arguments into a job spec, resolving git refs and building snapshots.

use crate::args::SubmitArgs;
use crate::snapshot::{self, Snapshot};
use corral_proto::job::{Class, Input, JobSpec};
use corral_proto::util::{parse_duration_ms, parse_size_mb};
use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

/// Caps that only one kind of host has imply "no fallback" (SPEC.md section 7).
const PINNING_CAPS: [&str; 3] = ["metal", "macos", "android"];

pub enum InputPlan {
    None,
    Git { repo: String, git_ref: String, sha: String },
    Snapshot(Box<Snapshot>),
}

/// `CMD...`: one argument is a shell string, several are an argv.
pub fn command_argv(cmd: &[String]) -> Vec<String> {
    if cmd.len() == 1 { vec!["bash".into(), "-lc".into(), cmd[0].clone()] } else { cmd.to_vec() }
}

fn kv(items: &[String], what: &str) -> Result<BTreeMap<String, String>, String> {
    items
        .iter()
        .map(|s| s.split_once('=').map(|(k, v)| (k.to_string(), v.to_string())).ok_or_else(|| format!("--{what} wants K=V, got {s:?}")))
        .collect()
}

/// Everything but the input (which needs git or a snapshot).
pub fn spec_from_args(a: &SubmitArgs) -> Result<JobSpec, String> {
    let class = if a.light { Class::Light } else { Class::Heavy };
    let ram_mb = match &a.ram {
        Some(r) => Some(parse_size_mb(r)?),
        None => None,
    };
    if class == Class::Heavy && ram_mb.is_none() {
        return Err("heavy jobs need --ram (declared memory, e.g. --ram 8G); use --light for small jobs".into());
    }
    let timeout_s = parse_duration_ms(&a.timeout)? / 1000;
    let pinned = a.caps.iter().any(|c| PINNING_CAPS.contains(&c.as_str()) || c.starts_with("tcc-"));
    let fallback_after_s = match a.fallback_after.as_deref() {
        Some("never") => Some(-1),
        Some(d) => Some((parse_duration_ms(d)? / 1000) as i64),
        None if pinned => Some(-1),
        None if a.prefer.is_some() => Some(1800),
        None => None,
    };
    let lock = if a.no_lock { "none".to_string() } else { a.lock.clone().unwrap_or_else(|| "auto".into()) };
    let mut labels = kv(&a.labels, "label")?;
    if a.keep {
        labels.insert("keep".into(), "true".into());
    }
    let mut spec = JobSpec::simple(&["true"]);
    spec.cmd = command_argv(&a.cmd);
    spec.cwd = a.cwd.clone();
    spec.env = kv(&a.env, "env")?;
    spec.caps = a.caps.clone();
    spec.prefer = a.prefer.clone();
    spec.fallback_after_s = fallback_after_s;
    spec.ram_mb = ram_mb;
    spec.class = class;
    spec.exclusive = a.exclusive;
    spec.profile = a.profile.clone();
    spec.lock = lock;
    spec.priority = a.priority;
    spec.timeout_s = timeout_s;
    spec.not_after = a.not_after.clone();
    spec.artifacts = a.artifacts.clone();
    spec.pause_on_exit = a.pause_on_exit.clone();
    spec.labels = labels;
    spec.notify = a.notify.clone();
    Ok(spec)
}

fn git_out(dir: &Path, args: &[&str]) -> Result<String, String> {
    let out = Command::new("git").args(args).current_dir(dir).output().map_err(|e| format!("cannot run git: {e}"))?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    } else {
        Err(format!("git {}: {}", args.join(" "), String::from_utf8_lossy(&out.stderr).trim()))
    }
}

/// Resolve a ref to a sha now (R-IO-01: the job runs exactly what was resolved at submit).
pub fn resolve_git(cwd: &Path, repo: Option<&str>, git_ref: &str) -> Result<(String, String), String> {
    let repo = match repo {
        Some(r) => r.to_string(),
        None => git_out(cwd, &["remote", "get-url", "origin"])
            .map_err(|_| "no --repo given and this directory has no `origin` remote".to_string())?,
    };
    // A ref that is already a full sha needs no lookup.
    if git_ref.len() == 40 && git_ref.chars().all(|c| c.is_ascii_hexdigit()) {
        return Ok((repo, git_ref.to_string()));
    }
    let listing = git_out(cwd, &["ls-remote", &repo, git_ref])?;
    let sha =
        listing.lines().next().and_then(|l| l.split_whitespace().next()).ok_or_else(|| format!("ref {git_ref:?} not found in {repo}"))?;
    Ok((repo, sha.to_string()))
}

/// Decide the input: explicit flags win; otherwise a snapshot inside a git work tree, else none.
pub fn plan_input(a: &SubmitArgs, cwd: &Path, scratch: &Path) -> Result<InputPlan, String> {
    if a.no_input {
        return Ok(InputPlan::None);
    }
    if a.repo.is_some() || a.git_ref.is_some() {
        let (repo, sha) = resolve_git(cwd, a.repo.as_deref(), a.git_ref.as_deref().unwrap_or("HEAD"))?;
        return Ok(InputPlan::Git { repo, git_ref: a.git_ref.clone().unwrap_or_else(|| "HEAD".into()), sha });
    }
    if a.snapshot || snapshot::repo_root(cwd).is_ok() {
        let s = snapshot::build(cwd, a.snapshot_max_mb, a.allow_secret_files, scratch).map_err(|e| e.to_string())?;
        return Ok(InputPlan::Snapshot(Box::new(s)));
    }
    Ok(InputPlan::None)
}

impl InputPlan {
    pub fn to_input(&self) -> Input {
        match self {
            InputPlan::None => Input::None,
            InputPlan::Git { repo, git_ref, sha } => Input::Git { repo: repo.clone(), git_ref: git_ref.clone(), sha: sha.clone() },
            InputPlan::Snapshot(s) => {
                Input::Snapshot { sha256: s.sha256.clone(), bytes: s.bytes, base_sha: s.base_sha.clone(), repo_hint: s.repo_hint.clone() }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(cmd: &[&str]) -> SubmitArgs {
        SubmitArgs {
            cmd: cmd.iter().map(|s| s.to_string()).collect(),
            timeout: "2h".into(),
            profile: "default".into(),
            priority: 50,
            cwd: ".".into(),
            notify: "on_fail".into(),
            snapshot_max_mb: 1024,
            ..Default::default()
        }
    }

    #[test]
    fn shell_string_is_wrapped_and_argv_is_kept() {
        assert_eq!(command_argv(&["make bench".into()]), vec!["bash", "-lc", "make bench"]);
        assert_eq!(command_argv(&["echo".into(), "hi".into()]), vec!["echo", "hi"]);
    }

    #[test]
    fn heavy_needs_ram_and_light_does_not() {
        assert!(spec_from_args(&args(&["true"])).unwrap_err().contains("--ram"));
        let mut a = args(&["true"]);
        a.light = true;
        assert_eq!(spec_from_args(&a).unwrap().class, Class::Light);
        let mut a = args(&["true"]);
        a.ram = Some("8G".into());
        assert_eq!(spec_from_args(&a).unwrap().ram_mb, Some(8192));
    }

    /// SPEC.md section 7: single-host caps pin the job; `--prefer` alone falls back after 30 minutes.
    #[test]
    fn fallback_defaults() {
        let mut a = args(&["true"]);
        a.ram = Some("1G".into());
        a.caps = vec!["metal".into()];
        assert_eq!(spec_from_args(&a).unwrap().fallback_after_s, Some(-1));
        let mut a = args(&["true"]);
        a.ram = Some("1G".into());
        a.prefer = Some("laptop".into());
        assert_eq!(spec_from_args(&a).unwrap().fallback_after_s, Some(1800));
        a.fallback_after = Some("never".into());
        assert_eq!(spec_from_args(&a).unwrap().fallback_after_s, Some(-1));
        a.fallback_after = Some("10m".into());
        assert_eq!(spec_from_args(&a).unwrap().fallback_after_s, Some(600));
        let mut a = args(&["true"]);
        a.ram = Some("1G".into());
        assert_eq!(spec_from_args(&a).unwrap().fallback_after_s, None);
    }

    #[test]
    fn env_labels_keep_and_no_lock() {
        let mut a = args(&["true"]);
        a.ram = Some("1G".into());
        a.env = vec!["A=1".into(), "B=x=y".into()];
        a.labels = vec!["project=demo".into()];
        a.keep = true;
        a.no_lock = true;
        let s = spec_from_args(&a).unwrap();
        assert_eq!(s.env["B"], "x=y");
        assert_eq!(s.labels["keep"], "true");
        assert_eq!(s.lock, "none");
        a.env = vec!["broken".into()];
        assert!(spec_from_args(&a).is_err());
    }
}
