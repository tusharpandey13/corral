//! The comparison between the guard's observed decisions and a legacy guard's own log (GUARDS.md 10.3, phase G0:
//! "every legacy action has a matching corral decision, same tier, same moment +- 1 sample").
//!
//! A legacy log is plain text, one event per line: `YYYY-MM-DD HH:MM:SS <message>` in the host's local time. The
//! messages understood (generic shapes, not tied to a product):
//!
//! | legacy message | event | matches a corral decision |
//! |---|---|---|
//! | `kill tier N ... reason=R` | kill of tier N | `would_kill`, tier N |
//! | `stop_all reason=R` | kill of everything | `would_kill` at level `critical` or `stop` |
//! | `shed reason=R ...` (not `skipped`) | kill of the non-protected set | `would_kill` at level `shed` |
//! | `shed ... skipped: ...` | a withheld shed | `skipped` |
//! | `STOP by X` | kill switch on | `stop` |
//! | `resume` | kill switch off | `resume` |
//! | `admit NAME` | an admission granted | `admit` with `ok: true` |
//!
//! Lines that are none of these (start-up notes, `restored:`) are ignored. The decision log is `decisions.jsonl`
//! (`decisions.rs`).

use crate::decisions::Decision;
use corral_proto::util::parse_rfc3339_ms;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LegacyEvent {
    pub t_ms: i64,
    pub line: String,
    pub kind: LegacyKind,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LegacyKind {
    KillTier { tier: u32 },
    KillAll,
    Shed,
    ShedSkipped,
    Stop,
    Resume,
    Admit,
}

/// Parse a legacy log. `offset` is the log's UTC offset as `+01:00`. Returns the events and the count of lines that
/// had a timestamp but no recognised message (reported so a format change is noticed, not silently ignored).
pub fn parse_legacy(text: &str, offset: &str) -> Result<(Vec<LegacyEvent>, usize), String> {
    let mut events: Vec<LegacyEvent> = Vec::new();
    let mut unknown = 0;
    for line in text.lines() {
        let Some((date, rest)) = line.split_once(' ') else { continue };
        let Some((time, msg)) = rest.split_once(' ').or(Some((rest, ""))) else { continue };
        if date.len() != 10 || time.len() != 8 || !date.contains('-') || !time.contains(':') {
            continue;
        }
        let t_ms = parse_rfc3339_ms(&format!("{date}T{time}{offset}")).map_err(|e| format!("bad timestamp in {line:?}: {e}"))?;
        let msg = msg.trim();
        let kind = if let Some(r) = msg.strip_prefix("kill tier ") {
            r.split_whitespace().next().and_then(|n| n.trim_end_matches(':').parse().ok()).map(|tier| LegacyKind::KillTier { tier })
        } else if msg.starts_with("stop_all ") {
            Some(LegacyKind::KillAll)
        } else if msg.starts_with("shed ") && msg.contains("skipped") {
            Some(LegacyKind::ShedSkipped)
        } else if msg.starts_with("shed ") {
            Some(LegacyKind::Shed)
        } else if msg.starts_with("STOP") {
            Some(LegacyKind::Stop)
        } else if msg == "resume" || msg.starts_with("resume ") {
            Some(LegacyKind::Resume)
        } else if msg == "admit" || msg.starts_with("admit ") {
            Some(LegacyKind::Admit)
        } else if msg.starts_with("watch start") || msg.starts_with("restored:") || msg.starts_with("killed container") {
            continue;
        } else {
            None
        };
        match kind {
            // one `shed` line per container is one decision: collapse same-second repeats
            Some(k) if events.last().is_some_and(|e| e.kind == k && e.t_ms == t_ms && matches!(k, LegacyKind::Shed)) => {}
            Some(kind) => events.push(LegacyEvent { t_ms, line: line.trim().to_string(), kind }),
            None => unknown += 1,
        }
    }
    Ok((events, unknown))
}

#[derive(Debug, Serialize)]
pub struct Matched {
    pub legacy: LegacyEvent,
    /// Indexes into the corral decisions.
    pub corral: Vec<usize>,
    pub lag_ms: i64,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub matched: Vec<Matched>,
    pub legacy_only: Vec<LegacyEvent>,
    pub corral_only: Vec<Decision>,
    pub legacy_unrecognised_lines: usize,
    pub window_ms: i64,
}

impl Report {
    /// G0 exit criterion: every legacy action has a matching corral decision.
    pub fn clean(&self) -> bool {
        self.legacy_only.is_empty()
    }

    pub fn render(&self) -> String {
        let hms = |ms: i64| corral_proto::util::rfc3339(ms);
        let mut o = String::new();
        for m in &self.matched {
            o.push_str(&format!(
                "match      legacy [{}] -> corral {} decision(s), first {:+.1}s\n",
                m.legacy.line,
                m.corral.len(),
                m.lag_ms as f64 / 1000.0
            ));
        }
        for l in &self.legacy_only {
            o.push_str(&format!(
                "LEGACY-ONLY legacy [{}] at {}: no corral decision within +-{}s\n",
                l.line,
                hms(l.t_ms),
                self.window_ms / 1000
            ));
        }
        for c in &self.corral_only {
            o.push_str(&format!(
                "corral-only {} {} tier={:?} reason={} at {}: {}\n",
                c.kind,
                c.matcher.clone().unwrap_or_default(),
                c.tier,
                c.reason,
                c.t,
                c.detail
            ));
        }
        o.push_str(&format!(
            "summary: legacy actions {}, matched {}, legacy-only {}, corral-only {}, unrecognised legacy lines {}\n",
            self.matched.len() + self.legacy_only.len(),
            self.matched.len(),
            self.legacy_only.len(),
            self.corral_only.len(),
            self.legacy_unrecognised_lines
        ));
        o
    }
}

fn candidate(kind: &LegacyKind, d: &Decision) -> bool {
    match kind {
        LegacyKind::KillTier { tier } => matches!(d.kind.as_str(), "would_kill" | "kill") && d.tier == Some(*tier),
        LegacyKind::KillAll => matches!(d.kind.as_str(), "would_kill" | "kill") && matches!(d.level.as_str(), "critical" | "stop"),
        LegacyKind::Shed => matches!(d.kind.as_str(), "would_kill" | "kill") && d.level == "shed",
        LegacyKind::ShedSkipped => d.kind == "skipped",
        LegacyKind::Stop => d.kind == "stop",
        LegacyKind::Resume => d.kind == "resume",
        LegacyKind::Admit => d.kind == "admit" && d.ok == Some(true),
    }
}

/// Match every legacy event to the corral decisions of the same kind within `window_ms`. Several corral decisions
/// (one per unit) may answer one legacy event that killed a whole tier. Only decisions inside the legacy log's time
/// span are reported as corral-only.
pub fn compare(legacy: &[LegacyEvent], unknown_lines: usize, ours: &[Decision], window_ms: i64) -> Report {
    let mut used = vec![false; ours.len()];
    let (mut matched, mut legacy_only) = (Vec::new(), Vec::new());
    for l in legacy {
        let hits: Vec<usize> = ours
            .iter()
            .enumerate()
            .filter(|(_, d)| candidate(&l.kind, d) && (d.t_ms - l.t_ms).abs() <= window_ms)
            .map(|(i, _)| i)
            .collect();
        if hits.is_empty() {
            legacy_only.push(l.clone());
        } else {
            for i in &hits {
                used[*i] = true;
            }
            let lag = hits.iter().map(|i| ours[*i].t_ms - l.t_ms).min_by_key(|x| x.abs()).unwrap_or(0);
            matched.push(Matched { legacy: l.clone(), corral: hits, lag_ms: lag });
        }
    }
    let (lo, hi) = (legacy.iter().map(|l| l.t_ms).min(), legacy.iter().map(|l| l.t_ms).max());
    let corral_only = ours
        .iter()
        .enumerate()
        .filter(|(i, d)| {
            !used[*i] && matches!(d.kind.as_str(), "would_kill" | "kill" | "stop" | "resume" | "skipped")
                || (!used[*i] && d.kind == "admit" && d.ok == Some(true))
        })
        .map(|(_, d)| d)
        .filter(|d| match (lo, hi) {
            (Some(lo), Some(hi)) => d.t_ms >= lo - window_ms && d.t_ms <= hi + window_ms,
            _ => true,
        })
        .cloned()
        .collect();
    Report { matched, legacy_only, corral_only, legacy_unrecognised_lines: unknown_lines, window_ms }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LEGACY: &str = "\
2026-11-22 10:00:00 watch start
2026-11-22 10:00:05 kill tier 1 pids: 812  (level=14% pressure=1 swap=100MB) reason=memory_critical
2026-11-22 10:00:30 STOP by someone
2026-11-22 10:00:30 kill tier 2 pids: 900  (level=14% pressure=1 swap=100MB) reason=stop_switch
2026-11-22 10:01:00 resume
2026-11-22 10:02:00 admit build-1
2026-11-22 10:03:00 shed reason=memory_low avail=1800MB temp=60C killed container cgroup abc
2026-11-22 10:03:00 shed reason=memory_low avail=1800MB temp=60C killed container cgroup def
2026-11-22 10:04:00 stop_all reason=memory_critical avail=700MB temp=60C
2026-11-22 10:04:01 killed container cgroup abc
2026-11-22 10:05:00 shed memory_low skipped: prod ids unresolved
2026-11-22 10:06:00 something new the parser does not know
";

    fn d(t: &str, kind: &str, level: &str, tier: Option<u32>) -> Decision {
        let mut x = Decision::new(parse_rfc3339_ms(t).unwrap(), kind, level, "r", "detail");
        x.tier = tier;
        x
    }

    #[test]
    fn parses_the_generic_shapes() {
        let (ev, unknown) = parse_legacy(LEGACY, "+00:00").unwrap();
        let kinds: Vec<&LegacyKind> = ev.iter().map(|e| &e.kind).collect();
        assert_eq!(
            kinds,
            vec![
                &LegacyKind::KillTier { tier: 1 },
                &LegacyKind::Stop,
                &LegacyKind::KillTier { tier: 2 },
                &LegacyKind::Resume,
                &LegacyKind::Admit,
                &LegacyKind::Shed,
                &LegacyKind::KillAll,
                &LegacyKind::ShedSkipped
            ],
            "one shed event for two containers in the same second"
        );
        assert_eq!(unknown, 1);
        // a local-time log with an offset
        let (ev, _) = parse_legacy("2026-11-22 10:00:00 resume\n", "+01:00").unwrap();
        assert_eq!(corral_proto::util::rfc3339(ev[0].t_ms), "2026-11-22T09:00:00Z");
    }

    /// G0: matching by kind and tier within the window; extras on either side are reported.
    #[test]
    fn matches_within_the_window_and_reports_both_kinds_of_difference() {
        let (ev, unknown) = parse_legacy(LEGACY, "+00:00").unwrap();
        let ours = vec![
            d("2026-11-22T10:00:03Z", "would_kill", "shed", Some(1)),
            d("2026-11-22T10:00:03Z", "would_kill", "shed", Some(1)), // second unit of the same tier
            d("2026-11-22T10:00:31Z", "stop", "ok", None),
            d("2026-11-22T10:00:32Z", "would_kill", "stop", Some(2)),
            d("2026-11-22T10:01:00Z", "resume", "ok", None),
            // admit at 10:02: missing on purpose -> legacy-only
            d("2026-11-22T10:03:01Z", "would_kill", "shed", Some(0)),
            // 10:04 stop_all: ours is 60 s late -> legacy-only AND corral-only
            d("2026-11-22T10:05:00Z", "would_kill", "critical", Some(0)),
            d("2026-11-22T10:05:00Z", "skipped", "shed", None),
            d("2026-11-22T10:05:08Z", "would_kill", "shed", Some(3)), // corral-only extra
        ];
        let r = compare(&ev, unknown, &ours, 10_000);
        assert_eq!(r.matched.len(), 6, "{}", r.render());
        assert_eq!(r.matched[0].corral.len(), 2, "one legacy tier kill, two corral units");
        let lo: Vec<&LegacyKind> = r.legacy_only.iter().map(|l| &l.kind).collect();
        assert_eq!(lo, vec![&LegacyKind::Admit, &LegacyKind::KillAll]);
        assert_eq!(r.corral_only.len(), 2, "{}", r.render());
        assert!(!r.clean());
        let text = r.render();
        assert!(
            text.contains("LEGACY-ONLY") && text.contains("summary: legacy actions 8, matched 6, legacy-only 2, corral-only 2"),
            "{text}"
        );
    }

    #[test]
    fn a_clean_run_is_clean() {
        let (ev, u) = parse_legacy("2026-11-22 10:00:05 kill tier 1 pids: 1 reason=memory_critical\n", "+00:00").unwrap();
        let r = compare(&ev, u, &[d("2026-11-22T10:00:07Z", "would_kill", "shed", Some(1))], 10_000);
        assert!(r.clean() && r.corral_only.is_empty());
        let r = compare(&ev, u, &[d("2026-11-22T10:00:07Z", "would_kill", "shed", Some(2))], 10_000);
        assert!(!r.clean(), "a different tier is not a match");
    }
}
