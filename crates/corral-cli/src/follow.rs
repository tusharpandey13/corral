//! `corral wait` and `corral logs`: follow a job's logs, progress and state over JetStream (R-IO-04).

use crate::client::Client;
use corral_proto::api::{FollowReq, JobEvent, ProgressMsg};
use corral_proto::bus::unreachable_error;
use corral_proto::envelope::ApiError;
use corral_proto::job::{JobState, JobView};
use futures::StreamExt;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Where output goes (stdout/stderr in the CLI, memory in tests).
pub trait Sink: Send {
    fn out(&mut self, bytes: &[u8]);
    fn err(&mut self, bytes: &[u8]);
}

pub struct StdSink;

impl Sink for StdSink {
    fn out(&mut self, bytes: &[u8]) {
        use std::io::Write;
        let mut o = std::io::stdout().lock();
        let _ = o.write_all(bytes);
        let _ = o.flush();
    }
    fn err(&mut self, bytes: &[u8]) {
        use std::io::Write;
        let mut o = std::io::stderr().lock();
        let _ = o.write_all(bytes);
        let _ = o.flush();
    }
}

#[derive(Default)]
pub struct MemSink {
    pub out: Vec<u8>,
    pub err: Vec<u8>,
}

impl Sink for MemSink {
    fn out(&mut self, b: &[u8]) {
        self.out.extend_from_slice(b);
    }
    fn err(&mut self, b: &[u8]) {
        self.err.extend_from_slice(b);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FollowOpts {
    /// Keep following until the job ends (`wait`, `logs -f`); otherwise print what exists and return.
    pub follow: bool,
    /// Show corral's own notes (the `sys` stream) on stderr.
    pub sys: bool,
}

/// Exit code of `corral wait` for a finished job (R-IO-04): the job's own code, 125 when it did not exit by itself.
pub fn exit_code_for(j: &JobView) -> i32 {
    match j.state {
        JobState::Succeeded => 0,
        JobState::Failed => j.exit_code.filter(|c| *c != 0).unwrap_or(crate::exitcode::JOB_ENDED),
        _ => crate::exitcode::JOB_ENDED,
    }
}

pub fn final_line(j: &JobView) -> Option<String> {
    match j.state {
        JobState::Succeeded => None,
        JobState::Failed => {
            Some(format!("corral: {} failed (exit {})", j.id, j.exit_code.map(|c| c.to_string()).unwrap_or_else(|| "?".into())))
        }
        s => Some(format!(
            "corral: {} {}{}{}",
            j.id,
            s.as_str(),
            j.reason.as_ref().map(|r| format!(" ({r})")).unwrap_or_default(),
            j.detail.as_ref().map(|d| format!(": {d}")).unwrap_or_default()
        )),
    }
}

/// Subscribe to one stream's three delivery subjects, then ask the scheduler to start it. JetStream only delivers to
/// a push consumer's delivery subject while someone is subscribed to that exact subject (a wildcard does not
/// count), so each subject is subscribed on its own, before the request.
async fn start_stream(
    c: &Client,
    id: &str,
    base: &str,
    n: u32,
    subs: &mut futures::stream::SelectAll<async_nats::Subscriber>,
) -> Result<(), ApiError> {
    for kind in ["log", "ev", "prog"] {
        let s = c
            .bus
            .client
            .subscribe(format!("{base}.{n}.{kind}"))
            .await
            .map_err(|e| unreachable_error(format!("cannot follow {id}: {e}")))?;
        subs.push(s);
    }
    let _: serde_json::Value =
        c.bus.call_retry(&c.subject("follow"), &FollowReq { id: id.into(), deliver: format!("{base}.{n}") }, 3).await?;
    Ok(())
}

/// How long a follower waits without any message before it asks for a fresh stream (the scheduler's consumers
/// vanish when NATS restarts; sequence numbers make the replay idempotent). The wait doubles each time, so a job
/// that is simply silent costs one replay per few minutes at most.
const RESTART_AFTER: Duration = Duration::from_secs(10);
const RESTART_AFTER_MAX: Duration = Duration::from_secs(160);

/// Follow `id`. Returns the final job view when the job ended (or the current view when not following).
///
/// Logs, events and progress arrive through push consumers that the scheduler creates on the caller's behalf and
/// that deliver into the caller's private inbox space; the caller never holds JetStream consumer permissions
/// (R-SEC-02). The caller subscribes first, then asks, so nothing is delivered into the void.
pub async fn follow(c: &Client, id: &str, opts: FollowOpts, sink: &mut dyn Sink) -> Result<JobView, ApiError> {
    let first = c.get(id).await?;
    let base = format!("{}.f{}", c.bus.identity.inbox_prefix(), corral_proto::util::random_b32(10));
    let mut subs = futures::stream::SelectAll::new();
    let mut stream_n = 1u32;
    start_stream(c, id, &base, stream_n, &mut subs).await?;

    let mut cur_attempt = 0u32;
    let mut got: HashMap<(u32, String), u64> = HashMap::new();
    let mut ended: Option<(JobView, Instant)> = if first.state.is_terminal() { Some((first.clone(), Instant::now())) } else { None };
    let mut last_activity = Instant::now();
    let mut last_msg = Instant::now();
    let mut last_progress = 0u8;
    let mut restart_after = RESTART_AFTER;

    loop {
        tokio::select! {
            Some(m) = subs.next() => {
                last_activity = Instant::now();
                last_msg = Instant::now();
                restart_after = RESTART_AFTER;
                let subject = m.subject.as_str();
                // A push consumer delivers each message under its original subject.
                if subject.starts_with("corral.log.") {
                    let hdr = |k: &str| m.headers.as_ref().and_then(|h| h.get(k)).map(|v| v.as_str().to_string());
                    let (Some(attempt), Some(seq), Some(stream)) = (
                        hdr("Corral-Attempt").and_then(|v| v.parse::<u32>().ok()),
                        hdr("Corral-Seq").and_then(|v| v.parse::<u64>().ok()),
                        hdr("Corral-Stream"),
                    ) else { continue };
                    if attempt < cur_attempt {
                        continue; // an older attempt's output
                    }
                    if attempt > cur_attempt {
                        if cur_attempt > 0 {
                            sink.err(format!("corral: attempt {attempt} started (the earlier attempt was lost)\n").as_bytes());
                        }
                        cur_attempt = attempt;
                    }
                    if got.get(&(attempt, stream.clone())).is_some_and(|last| seq <= *last) {
                        continue; // a replay after a restarted stream
                    }
                    got.insert((attempt, stream.clone()), seq);
                    match stream.as_str() {
                        "out" => sink.out(&m.payload),
                        "err" => sink.err(&m.payload),
                        "sys" if opts.sys => sink.err(&m.payload),
                        _ => {}
                    }
                } else if subject.starts_with("corral.event.") {
                    if let Ok(ev) = serde_json::from_slice::<JobEvent>(&m.payload)
                        && JobState::parse(&ev.state).is_some_and(|s| s.is_terminal())
                        && ended.is_none()
                    {
                        let view = c.get(id).await?;
                        ended = Some((view, Instant::now()));
                    }
                } else if subject.starts_with("corral.progress.")
                    && let Ok(p) = serde_json::from_slice::<ProgressMsg>(&m.payload)
                    && p.pct != last_progress
                {
                    last_progress = p.pct;
                    sink.err(format!("corral: progress {}% {}\n", p.pct, p.text).as_bytes());
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
        if let Some((view, since)) = &ended {
            // Done when every stream the worker reported has been seen up to its final sequence; a short grace
            // covers a job whose logs expired or never existed.
            let complete = view.log_final.iter().all(|(s, n)| *n == 0 || got.get(&(view.attempts, s.clone())).copied().unwrap_or(0) >= *n);
            if complete || since.elapsed() > Duration::from_secs(5) {
                return Ok(view.clone());
            }
        } else if !opts.follow && last_activity.elapsed() > Duration::from_millis(1200) {
            return c.get(id).await;
        } else if opts.follow && last_msg.elapsed() > restart_after {
            restart_after = (restart_after * 2).min(RESTART_AFTER_MAX);
            // Quiet for a long time: the job may just be silent, or the scheduler's consumers may be gone (NATS
            // restart). Ask again; replayed messages are skipped by sequence.
            last_msg = Instant::now();
            stream_n += 1;
            if let Err(e) = start_stream(c, id, &base, stream_n, &mut subs).await {
                tracing::debug!("could not restart the stream: {e}");
            }
            let view = c.get(id).await?;
            if view.state.is_terminal() && ended.is_none() {
                ended = Some((view, Instant::now()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use corral_proto::job::JobSpec;

    fn view(state: JobState, code: Option<i32>) -> JobView {
        JobView {
            id: "j-1".into(),
            state,
            kind: "command".into(),
            spec: JobSpec::simple(&["true"]),
            submitter: "agent:laptop".into(),
            attempts: 1,
            worker: None,
            reason: None,
            exit_code: code,
            detail: None,
            created_at: "2026-11-22T00:00:00Z".into(),
            queued_at: None,
            started_at: None,
            finished_at: None,
            waiting: vec![],
            cancel_requested: false,
            artifacts_state: None,
            log_final: Default::default(),
            retry_of: None,
            progress: None,
        }
    }

    /// U-21 / R-IO-04: `wait` exits with the job's code, and 125 for killed, lost, cancelled and expired.
    #[test]
    fn wait_exit_codes() {
        assert_eq!(exit_code_for(&view(JobState::Succeeded, Some(0))), 0);
        assert_eq!(exit_code_for(&view(JobState::Failed, Some(3))), 3);
        assert_eq!(exit_code_for(&view(JobState::Failed, None)), 125, "a failure without an exit code (input) is not 0");
        for s in [JobState::Killed, JobState::Lost, JobState::Cancelled, JobState::Expired] {
            assert_eq!(exit_code_for(&view(s, None)), 125, "{s:?}");
        }
        assert!(final_line(&view(JobState::Succeeded, Some(0))).is_none());
        assert!(final_line(&view(JobState::Killed, None)).unwrap().contains("killed"));
    }
}
