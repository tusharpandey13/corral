//! NATS subjects (PROTOCOL.md section 3). All start with `corral.`.

pub const STREAM_LOGS: &str = "CORRAL_LOGS";
pub const STREAM_EVENTS: &str = "CORRAL_EVENTS";
pub const KV_WORKERS: &str = "corral_workers";

pub const API_ALL: &str = "corral.api.>";
pub const WORK_ALL: &str = "corral.work.>";

pub fn api(who_slug: &str, verb: &str) -> String {
    format!("corral.api.{who_slug}.{verb}")
}
pub fn claim(w: &str) -> String {
    format!("corral.work.{w}.claim")
}
pub fn heartbeat(w: &str, id: &str) -> String {
    format!("corral.work.{w}.{id}.hb")
}
pub fn attempt_event(w: &str, id: &str) -> String {
    format!("corral.work.{w}.{id}.event")
}
pub fn ctl(w: &str) -> String {
    format!("corral.ctl.{w}")
}
pub fn wake(w: &str) -> String {
    format!("corral.wake.{w}")
}
pub fn log(w: &str, id: &str, stream: &str) -> String {
    format!("corral.log.{w}.{id}.{stream}")
}
pub fn progress(w: &str, id: &str) -> String {
    format!("corral.progress.{w}.{id}")
}
pub fn job_event(id: &str) -> String {
    format!("corral.event.{id}")
}
/// Filter for every log message of one job, any worker, any stream.
pub fn log_filter(id: &str) -> String {
    format!("corral.log.*.{id}.>")
}
pub fn progress_filter(id: &str) -> String {
    format!("corral.progress.*.{id}")
}

/// A parsed `corral.api.<who>.<verb>` subject.
#[derive(Debug, PartialEq, Eq)]
pub struct ApiSubject<'a> {
    pub who: &'a str,
    pub verb: &'a str,
}

pub fn parse_api(subject: &str) -> Option<ApiSubject<'_>> {
    let rest = subject.strip_prefix("corral.api.")?;
    let (who, verb) = rest.split_once('.')?;
    (!who.is_empty() && !verb.is_empty()).then_some(ApiSubject { who, verb })
}

/// A parsed `corral.work.<w>...` subject.
#[derive(Debug, PartialEq, Eq)]
pub enum WorkSubject<'a> {
    Claim { worker: &'a str },
    Heartbeat { worker: &'a str, job: &'a str },
    Event { worker: &'a str, job: &'a str },
}

pub fn parse_work(subject: &str) -> Option<WorkSubject<'_>> {
    let rest = subject.strip_prefix("corral.work.")?;
    let parts: Vec<&str> = rest.split('.').collect();
    match parts.as_slice() {
        [w, "claim"] => Some(WorkSubject::Claim { worker: w }),
        [w, id, "hb"] => Some(WorkSubject::Heartbeat { worker: w, job: id }),
        [w, id, "event"] => Some(WorkSubject::Event { worker: w, job: id }),
        _ => None,
    }
}

/// A parsed `corral.log.<w>.<id>.<stream>` subject.
pub fn parse_log(subject: &str) -> Option<(&str, &str, &str)> {
    let rest = subject.strip_prefix("corral.log.")?;
    let parts: Vec<&str> = rest.split('.').collect();
    match parts.as_slice() {
        [w, id, s] if matches!(*s, "out" | "err" | "sys") => Some((w, id, s)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses() {
        assert_eq!(
            parse_api("corral.api.agent-laptop.submit"),
            Some(ApiSubject {
                who: "agent-laptop",
                verb: "submit"
            })
        );
        assert_eq!(parse_api("corral.api.x"), None);
        assert_eq!(
            parse_work("corral.work.w.claim"),
            Some(WorkSubject::Claim { worker: "w" })
        );
        assert_eq!(
            parse_work("corral.work.w.j-1.hb"),
            Some(WorkSubject::Heartbeat {
                worker: "w",
                job: "j-1"
            })
        );
        assert_eq!(parse_work("corral.work.w.j-1.nope"), None);
        assert_eq!(parse_log("corral.log.w.j-1.out"), Some(("w", "j-1", "out")));
        assert_eq!(parse_log("corral.log.w.j-1.bad"), None);
    }
}
