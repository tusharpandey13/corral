//! The job state machine, exactly the table of PROTOCOL.md section 5 (R-QUEUE-03).

use crate::job::JobState::{self, *};

/// What can happen to a job. `Finished*` and `Cancel` are applied when the worker reports (or, for queued jobs,
/// immediately).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    UploadVerified,
    UploadFailed,
    Claim,
    Declined,
    Started,
    /// `finished{outcome: exited}` with exit code 0.
    ExitedOk,
    /// `finished{outcome: exited}` with a non-zero exit code.
    ExitedFailed,
    /// `finished{outcome: killed}` for a reason other than `cancelled`, or the scheduler's timeout backstop.
    Killed,
    Cancel,
    LeaseExpired,
    /// A valid heartbeat with the current token.
    Heartbeat,
    /// Orphan grace over and the job may be retried.
    GraceOverRequeue,
    /// Orphan grace over, no retry left.
    GraceOverLost,
    /// `finished{outcome: lost_local}` and the job may be retried.
    LostLocalRequeue,
    LostLocalFinal,
    NotAfterPassed,
}

impl Event {
    pub const ALL: [Event; 16] = [
        Event::UploadVerified,
        Event::UploadFailed,
        Event::Claim,
        Event::Declined,
        Event::Started,
        Event::ExitedOk,
        Event::ExitedFailed,
        Event::Killed,
        Event::Cancel,
        Event::LeaseExpired,
        Event::Heartbeat,
        Event::GraceOverRequeue,
        Event::GraceOverLost,
        Event::LostLocalRequeue,
        Event::LostLocalFinal,
        Event::NotAfterPassed,
    ];
}

#[derive(Debug, PartialEq, Eq)]
pub struct IllegalTransition {
    pub from: JobState,
    pub event: Event,
}

impl std::fmt::Display for IllegalTransition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "illegal transition: {:?} in state {}",
            self.event,
            self.from.as_str()
        )
    }
}

/// The only place transitions are defined.
pub fn next(from: JobState, ev: Event) -> Result<JobState, IllegalTransition> {
    use Event::*;
    let to = match (from, ev) {
        (Uploading, UploadVerified) => Queued,
        (Uploading, UploadFailed) => Failed,
        (Queued, Claim) => Claimed,
        (Claimed, Declined) => Queued,
        (Claimed, Started) => Running,
        (Claimed | Running, ExitedOk) => Succeeded,
        (Claimed | Running, ExitedFailed) => Failed,
        (Claimed | Running, Killed) => JobState::Killed,
        (Uploading | Queued | Claimed | Running | Orphaned, Cancel) => Cancelled,
        (Claimed | Running, LeaseExpired) => Orphaned,
        (Orphaned, Heartbeat) => Running,
        (Orphaned, GraceOverRequeue) => Queued,
        (Orphaned, GraceOverLost) => Lost,
        (Claimed | Running, LostLocalRequeue) => Queued,
        (Claimed | Running, LostLocalFinal) => Lost,
        (Queued, NotAfterPassed) => Expired,
        (from, event) => return Err(IllegalTransition { from, event }),
    };
    Ok(to)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// U-01 / R-QUEUE-03: table test over every (state, event) pair.
    #[test]
    fn every_pair_matches_the_protocol_table() {
        let legal: &[(JobState, Event, JobState)] = &[
            (Uploading, Event::UploadVerified, Queued),
            (Uploading, Event::UploadFailed, Failed),
            (Queued, Event::Claim, Claimed),
            (Claimed, Event::Declined, Queued),
            (Claimed, Event::Started, Running),
            (Claimed, Event::ExitedOk, Succeeded),
            (Running, Event::ExitedOk, Succeeded),
            (Claimed, Event::ExitedFailed, Failed),
            (Running, Event::ExitedFailed, Failed),
            (Claimed, Event::Killed, JobState::Killed),
            (Running, Event::Killed, JobState::Killed),
            (Uploading, Event::Cancel, Cancelled),
            (Queued, Event::Cancel, Cancelled),
            (Claimed, Event::Cancel, Cancelled),
            (Running, Event::Cancel, Cancelled),
            (Orphaned, Event::Cancel, Cancelled),
            (Claimed, Event::LeaseExpired, Orphaned),
            (Running, Event::LeaseExpired, Orphaned),
            (Orphaned, Event::Heartbeat, Running),
            (Orphaned, Event::GraceOverRequeue, Queued),
            (Orphaned, Event::GraceOverLost, Lost),
            (Claimed, Event::LostLocalRequeue, Queued),
            (Running, Event::LostLocalRequeue, Queued),
            (Claimed, Event::LostLocalFinal, Lost),
            (Running, Event::LostLocalFinal, Lost),
            (Queued, Event::NotAfterPassed, Expired),
        ];
        let mut checked = 0;
        for st in JobState::ALL {
            for ev in Event::ALL {
                let expected = legal
                    .iter()
                    .find(|(s, e, _)| *s == st && *e == ev)
                    .map(|(_, _, t)| *t);
                let got = next(st, ev);
                match expected {
                    Some(t) => assert_eq!(got, Ok(t), "{st:?} + {ev:?}"),
                    None => assert_eq!(
                        got,
                        Err(IllegalTransition {
                            from: st,
                            event: ev
                        }),
                        "{st:?} + {ev:?}"
                    ),
                }
                checked += 1;
            }
        }
        assert_eq!(checked, 11 * 16);
    }

    /// R-QUEUE-04: nothing leaves `killed` (a killed job is final), nor any other terminal state.
    #[test]
    fn terminal_states_are_final() {
        for st in JobState::ALL.into_iter().filter(|s| s.is_terminal()) {
            for ev in Event::ALL {
                assert!(next(st, ev).is_err(), "{st:?} must not accept {ev:?}");
            }
        }
    }
}
