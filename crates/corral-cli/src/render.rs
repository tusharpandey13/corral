//! Plain-text output for humans (the default); `--json` is handled by callers.

use corral_proto::api::WorkerRow;
use corral_proto::job::{JobState, JobView};
use corral_proto::util::parse_rfc3339_ms;

pub fn age(since: &str, now_ms: i64) -> String {
    let Ok(t) = parse_rfc3339_ms(since) else { return "?".into() };
    let s = ((now_ms - t) / 1000).max(0);
    match s {
        0..=89 => format!("{s}s"),
        90..=5399 => format!("{}m", s / 60),
        5400..=172_799 => format!("{}h", s / 3600),
        _ => format!("{}d", s / 86_400),
    }
}

pub fn ram(mb: Option<u64>) -> String {
    match mb {
        Some(m) if m >= 1024 && m % 1024 == 0 => format!("{}G", m / 1024),
        Some(m) => format!("{m}M"),
        None => "-".into(),
    }
}

/// One job, with the reasons it waits (SPEC.md section 7).
pub fn job_detail(j: &JobView, now_ms: i64) -> String {
    let mut s = String::new();
    let state = match (&j.state, &j.reason) {
        (JobState::Queued, _) => format!("queued {}", j.queued_at.as_deref().map(|q| age(q, now_ms)).unwrap_or_default()),
        (st, Some(r)) => format!("{} ({r})", st.as_str()),
        (st, None) => st.as_str().to_string(),
    };
    s.push_str(&format!("{}  {}  {:?} {}", j.id, state.trim(), j.spec.class, ram(j.spec.ram_mb)).to_lowercase());
    if !j.spec.caps.is_empty() {
        s.push_str(&format!("  caps {}", j.spec.caps.join(",")));
    }
    if let Some(p) = &j.spec.prefer {
        s.push_str(&format!("  prefer {p}"));
    }
    if let Some(w) = &j.worker {
        s.push_str(&format!("  on {w}"));
    }
    if j.attempts > 0 {
        s.push_str(&format!("  attempt {}", j.attempts));
    }
    if let Some(c) = j.exit_code {
        s.push_str(&format!("  exit {c}"));
    }
    if let Some(p) = j.progress {
        s.push_str(&format!("  progress {p}%"));
    }
    s.push('\n');
    if let Some(d) = &j.detail {
        s.push_str(&format!("  {d}\n"));
    }
    for w in &j.waiting {
        s.push_str(&format!("  {}: {}\n", w.worker, w.text));
    }
    s
}

pub fn job_row(j: &JobView, now_ms: i64) -> String {
    let when = j.queued_at.as_deref().or(Some(j.created_at.as_str())).map(|t| age(t, now_ms)).unwrap_or_default();
    let cmd = j.spec.cmd.join(" ");
    let cmd = if cmd.len() > 50 { format!("{}...", &cmd[..47]) } else { cmd };
    format!("{:<20} {:<10} {:>5} {:>6}  {}\n", j.id, j.state.as_str(), when, ram(j.spec.ram_mb), cmd)
}

pub fn worker_row(w: &WorkerRow) -> String {
    let mut s = format!("{:<14} {}", w.host, if w.online { "online" } else { "offline" });
    if let Some(p) = &w.presence {
        s.push_str(&format!(
            "  caps {}  heavy free {}  admissible {} MB  running {}",
            p.caps.join(","),
            p.slots.heavy_free,
            p.admission.admissible_ram_mb,
            p.running.len()
        ));
        for b in &p.admission.blocked {
            s.push_str(&format!("\n  blocked: {}: {}", b.check, b.reason));
        }
    }
    if let Some(f) = &w.flags {
        s.push_str(&format!("  [{f}]"));
    }
    s.push('\n');
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ages_and_sizes() {
        let t = corral_proto::util::rfc3339(1_000_000_000_000);
        assert_eq!(age(&t, 1_000_000_000_000 + 30_000), "30s");
        assert_eq!(age(&t, 1_000_000_000_000 + 14 * 60_000), "14m");
        assert_eq!(age(&t, 1_000_000_000_000 + 3 * 3_600_000), "3h");
        assert_eq!(ram(Some(8192)), "8G");
        assert_eq!(ram(Some(512)), "512M");
    }
}
