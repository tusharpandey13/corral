//! Log publishing (PROTOCOL.md section 8). The child's stdout and stderr go to local files first; they are the
//! source of truth and the spool. A publisher tails a file and publishes chunks (at most 64 KiB) with sequence
//! numbers, waiting for each ack, and resumes from the last acked sequence after any failure.

use crate::link::{LogHeaders, SchedLink};
use corral_proto::progress::parse_progress;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, watch};

pub const CHUNK: usize = 64 * 1024;

pub struct LogPublisher<L: SchedLink> {
    pub link: Arc<L>,
    pub job: String,
    pub attempt: u32,
    pub stream: &'static str,
    pub path: PathBuf,
    /// Unsent bytes beyond this are dropped and replaced by one note (R-IO-03, `log_spool_max`).
    pub spool_max: u64,
    /// Progress lines found in the output (`##corral progress=..`).
    pub progress: Option<mpsc::UnboundedSender<(u8, String)>>,
    /// Highest acked sequence, readable by the heartbeat.
    pub seq_out: Arc<AtomicU64>,
    pub poll: Duration,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PubResult {
    pub last_seq: u64,
    pub dropped: u64,
}

impl<L: SchedLink> LogPublisher<L> {
    async fn send(&self, seq: u64, payload: Vec<u8>) {
        let h = LogHeaders {
            msg_id: format!("{}/{}/{}/{}", self.job, self.attempt, self.stream, seq),
            seq,
            attempt: self.attempt,
            stream: self.stream.to_string(),
        };
        let mut delay = Duration::from_millis(200);
        // The same chunk with the same Nats-Msg-Id is retried until acked: nothing is lost while disconnected.
        while let Err(e) = self.link.publish_log(self.stream, &self.job, &h, payload.clone()).await {
            tracing::debug!(job = %self.job, stream = self.stream, "log publish failed, retrying: {e}");
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(5));
        }
        self.seq_out.store(seq, Ordering::SeqCst);
    }

    /// Run until `done` is set and the file has been published to its end.
    pub async fn run(self, mut done: watch::Receiver<bool>) -> PubResult {
        let mut offset = 0u64;
        let mut seq = 0u64;
        let mut dropped = 0u64;
        let mut partial: Vec<u8> = Vec::new();
        loop {
            let finished = *done.borrow();
            let len = std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0);
            if len.saturating_sub(offset) > self.spool_max {
                let skip = len - offset - self.spool_max;
                offset += skip;
                dropped += skip;
                seq += 1;
                self.send(seq, format!("[corral] {skip} bytes dropped\n").into_bytes()).await;
            }
            if len > offset {
                let want = ((len - offset) as usize).min(CHUNK);
                let mut buf = vec![0u8; want];
                let n = std::fs::File::open(&self.path)
                    .and_then(|mut f| {
                        f.seek(SeekFrom::Start(offset))?;
                        f.read(&mut buf)
                    })
                    .unwrap_or(0);
                if n == 0 {
                    tokio::time::sleep(self.poll).await;
                    continue;
                }
                buf.truncate(n);
                seq += 1;
                self.send(seq, buf.clone()).await;
                offset += n as u64;
                if let Some(tx) = &self.progress {
                    partial.extend_from_slice(&buf);
                    while let Some(pos) = partial.iter().position(|b| *b == b'\n') {
                        let line: Vec<u8> = partial.drain(..=pos).collect();
                        if let Some(p) = parse_progress(&String::from_utf8_lossy(&line)) {
                            let _ = tx.send(p);
                        }
                    }
                    if partial.len() > CHUNK {
                        partial.clear();
                    }
                }
                continue;
            }
            if finished {
                break;
            }
            let _ = tokio::time::timeout(self.poll, done.changed()).await;
        }
        PubResult { last_seq: seq, dropped }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use corral_proto::api::*;
    use corral_proto::envelope::ApiError;
    use std::sync::Mutex;

    /// A sink that fails on demand and records what was accepted.
    #[derive(Default)]
    struct Sink {
        fail_next: Mutex<u32>,
        got: Mutex<Vec<(u64, Vec<u8>)>>,
    }

    impl SchedLink for Sink {
        async fn claim(&self, _: &ClaimReq) -> Result<ClaimReply, ApiError> {
            unreachable!()
        }
        async fn heartbeat(&self, _: &str, _: &HbReq) -> Result<HbReply, ApiError> {
            unreachable!()
        }
        async fn event(&self, _: &str, _: &EventReq) -> Result<EventReply, ApiError> {
            unreachable!()
        }
        async fn publish_log(&self, _: &str, _: &str, h: &LogHeaders, payload: Vec<u8>) -> Result<(), String> {
            let mut f = self.fail_next.lock().unwrap();
            if *f > 0 {
                *f -= 1;
                return Err("disconnected".into());
            }
            self.got.lock().unwrap().push((h.seq, payload));
            Ok(())
        }
        async fn publish_progress(&self, _: &str, _: &ProgressMsg) {}
        async fn put_presence(&self, _: Vec<u8>) -> Result<(), String> {
            Ok(())
        }
    }

    fn publisher(sink: Arc<Sink>, path: PathBuf, spool: u64) -> (LogPublisher<Sink>, Arc<AtomicU64>) {
        let seq = Arc::new(AtomicU64::new(0));
        (
            LogPublisher {
                link: sink,
                job: "j-1".into(),
                attempt: 1,
                stream: "out",
                path,
                spool_max: spool,
                progress: None,
                seq_out: seq.clone(),
                poll: Duration::from_millis(5),
            },
            seq,
        )
    }

    fn tmp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("corral-logs-{}", corral_proto::util::random_b32(8)));
        std::fs::create_dir_all(&d).unwrap();
        d.join(name)
    }

    /// R-IO-03: in order, sequence numbers from 1, retried after a disconnect, the full content delivered.
    #[tokio::test]
    async fn publishes_in_order_and_retries_through_a_disconnect() {
        let path = tmp("out");
        let data: Vec<u8> = (0..(CHUNK * 2 + 100)).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &data).unwrap();
        let sink = Arc::new(Sink::default());
        *sink.fail_next.lock().unwrap() = 3;
        let (p, seq_out) = publisher(sink.clone(), path, u64::MAX);
        let (tx, rx) = watch::channel(true);
        let r = p.run(rx).await;
        drop(tx);
        assert_eq!(r.last_seq, 3);
        assert_eq!(seq_out.load(Ordering::SeqCst), 3);
        let got = sink.got.lock().unwrap();
        assert_eq!(got.iter().map(|(s, _)| *s).collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_eq!(got.iter().flat_map(|(_, p)| p.clone()).collect::<Vec<u8>>(), data);
    }

    #[tokio::test]
    async fn follows_a_growing_file_and_parses_progress() {
        let path = tmp("out");
        std::fs::write(&path, "").unwrap();
        let sink = Arc::new(Sink::default());
        let (mut p, _) = publisher(sink.clone(), path.clone(), u64::MAX);
        let (ptx, mut prx) = mpsc::unbounded_channel();
        p.progress = Some(ptx);
        let (tx, rx) = watch::channel(false);
        let h = tokio::spawn(p.run(rx));
        std::fs::write(&path, "hello\n##corral progress=40 building\nnot ##corral progress=1\n").unwrap();
        let got = tokio::time::timeout(Duration::from_secs(2), prx.recv()).await.unwrap().unwrap();
        assert_eq!(got, (40, "building".to_string()));
        tx.send(true).unwrap();
        let r = h.await.unwrap();
        assert_eq!(r.last_seq, 1);
        assert!(prx.try_recv().is_err(), "malformed progress lines are plain output");
    }

    /// R-IO-03: beyond `log_spool_max` the oldest unsent bytes are replaced by one note.
    #[tokio::test]
    async fn spool_cap_drops_the_oldest_with_a_note() {
        let path = tmp("out");
        std::fs::write(&path, vec![b'x'; 1000]).unwrap();
        let sink = Arc::new(Sink::default());
        let (p, _) = publisher(sink.clone(), path, 100);
        let (_tx, rx) = watch::channel(true);
        let r = p.run(rx).await;
        assert_eq!(r.dropped, 900);
        let got = sink.got.lock().unwrap();
        assert!(String::from_utf8_lossy(&got[0].1).contains("900 bytes dropped"));
        assert_eq!(got[1].1.len(), 100);
    }
}
