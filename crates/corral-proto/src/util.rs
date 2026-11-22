//! Small helpers: ids, time formatting, duration and size parsing.

use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

const B32_LOWER: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
const CROCKFORD: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

pub fn random_bytes<const N: usize>() -> [u8; N] {
    let mut b = [0u8; N];
    getrandom::fill(&mut b).expect("system randomness unavailable");
    b
}

/// `n` random lowercase base32 characters.
pub fn random_b32(n: usize) -> String {
    let mut out = String::with_capacity(n);
    let mut bytes = vec![0u8; n];
    getrandom::fill(&mut bytes).expect("system randomness unavailable");
    for b in bytes {
        out.push(B32_LOWER[(b & 31) as usize] as char);
    }
    out
}

/// A random 128-bit token as 26 lowercase base32 characters (attempt token, `lease_token`).
pub fn new_token() -> String {
    let bytes = random_bytes::<16>();
    let mut bits: u128 = u128::from_be_bytes(bytes);
    let mut out = [0u8; 26];
    // 26 * 5 = 130 bits; the top two bits are zero.
    for slot in out.iter_mut().rev() {
        *slot = B32_LOWER[(bits & 31) as usize];
        bits >>= 5;
    }
    String::from_utf8(out.to_vec()).expect("ascii")
}

/// A ULID-shaped request id: 48-bit millisecond time plus 80 random bits, Crockford base32.
pub fn new_req_id(now_ms: i64) -> String {
    let rnd = random_bytes::<10>();
    let mut v: u128 = (now_ms.max(0) as u128 & 0xFFFF_FFFF_FFFF) << 80;
    for (i, b) in rnd.iter().enumerate() {
        v |= (*b as u128) << (8 * (9 - i));
    }
    let mut out = [0u8; 26];
    for slot in out.iter_mut().rev() {
        *slot = CROCKFORD[(v & 31) as usize];
        v >>= 5;
    }
    String::from_utf8(out.to_vec()).expect("ascii")
}

/// `j-YYYYMMDD-xxxxxx` from a unix-millisecond timestamp.
pub fn new_job_id(now_ms: i64) -> String {
    format!("j-{}-{}", yyyymmdd(now_ms), random_b32(6))
}

fn yyyymmdd(ms: i64) -> String {
    let dt = OffsetDateTime::from_unix_timestamp(ms.div_euclid(1000)).unwrap_or(OffsetDateTime::UNIX_EPOCH);
    format!("{:04}{:02}{:02}", dt.year(), dt.month() as u8, dt.day())
}

/// RFC 3339 UTC, second precision, from unix milliseconds.
pub fn rfc3339(ms: i64) -> String {
    let dt = OffsetDateTime::from_unix_timestamp(ms.div_euclid(1000)).unwrap_or(OffsetDateTime::UNIX_EPOCH);
    dt.format(&Rfc3339).unwrap_or_default()
}

/// Parse an RFC 3339 timestamp into unix milliseconds.
pub fn parse_rfc3339_ms(s: &str) -> Result<i64, String> {
    OffsetDateTime::parse(s, &Rfc3339).map(|d| d.unix_timestamp() * 1000).map_err(|e| e.to_string())
}

/// Parse `90s`, `15m`, `2h`, `14d`, `500ms` into milliseconds.
pub fn parse_duration_ms(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit()).ok_or_else(|| format!("duration {s:?} needs a unit (ms, s, m, h, d)"))?;
    let (num, unit) = s.split_at(split);
    let n: u64 = num.parse().map_err(|_| format!("bad duration {s:?}"))?;
    let mult = match unit {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => return Err(format!("bad duration unit in {s:?} (use ms, s, m, h, d)")),
    };
    n.checked_mul(mult).ok_or_else(|| format!("duration {s:?} overflows"))
}

/// Parse `512M`, `4G`, `1024` (megabytes when no suffix is given) into mebibytes.
pub fn parse_size_mb(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let (num, mult_mb): (&str, f64) = match s.chars().last() {
        Some('K' | 'k') => (&s[..s.len() - 1], 1.0 / 1024.0),
        Some('M' | 'm') => (&s[..s.len() - 1], 1.0),
        Some('G' | 'g') => (&s[..s.len() - 1], 1024.0),
        Some('T' | 't') => (&s[..s.len() - 1], 1024.0 * 1024.0),
        Some(c) if c.is_ascii_digit() => (s, 1.0),
        _ => return Err(format!("bad size {s:?} (use 512M, 4G)")),
    };
    let n: f64 = num.trim().parse().map_err(|_| format!("bad size {s:?}"))?;
    if n < 0.0 || !n.is_finite() {
        return Err(format!("bad size {s:?}"));
    }
    Ok((n * mult_mb).ceil() as u64)
}

/// Hex of a byte slice.
pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// A pipe read on its own thread into a buffer, so the reader can stop waiting at a deadline. Reading to end-of-file
/// would block for as long as *any* process holds the write end, and a child that left a daemon behind (a keep-awake
/// helper, a background job of a hook) holds it for hours.
pub struct PipeDrain {
    buf: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
    done: std::sync::mpsc::Receiver<()>,
}

impl PipeDrain {
    pub fn spawn(mut r: impl std::io::Read + Send + 'static) -> PipeDrain {
        let buf = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let (tx, done) = std::sync::mpsc::channel();
        let b = buf.clone();
        std::thread::spawn(move || {
            let mut chunk = [0u8; 4096];
            while let Ok(n) = r.read(&mut chunk) {
                if n == 0 {
                    break;
                }
                b.lock().unwrap_or_else(|e| e.into_inner()).extend_from_slice(&chunk[..n]);
            }
            let _ = tx.send(());
        });
        PipeDrain { buf, done }
    }

    /// What has been read once the pipe reaches end-of-file or `wait` has passed, whichever is first.
    pub fn finish(self, wait: std::time::Duration) -> Vec<u8> {
        let _ = self.done.recv_timeout(wait);
        self.buf.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_and_sizes() {
        assert_eq!(parse_duration_ms("90s").unwrap(), 90_000);
        assert_eq!(parse_duration_ms("15m").unwrap(), 900_000);
        assert_eq!(parse_duration_ms("14d").unwrap(), 14 * 86_400_000);
        assert!(parse_duration_ms("15").is_err());
        assert!(parse_duration_ms("5x").is_err());
        assert_eq!(parse_size_mb("4G").unwrap(), 4096);
        assert_eq!(parse_size_mb("512M").unwrap(), 512);
        assert_eq!(parse_size_mb("2048").unwrap(), 2048);
        assert!(parse_size_mb("big").is_err());
    }

    #[test]
    fn ids_have_the_documented_shape() {
        let id = new_job_id(1_795_000_000_000);
        assert!(id.starts_with("j-2026"), "{id}");
        assert_eq!(id.len(), "j-YYYYMMDD-xxxxxx".len());
        assert_eq!(new_token().len(), 26);
        assert_eq!(new_req_id(1).len(), 26);
        assert_ne!(new_token(), new_token());
    }

    #[test]
    fn rfc3339_formats_utc() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn a_pipe_drain_stops_waiting_when_the_write_end_stays_open() {
        let (rd, mut wr) = std::io::pipe().unwrap();
        std::io::Write::write_all(&mut wr, b"partial").unwrap();
        let t = std::time::Instant::now();
        let got = PipeDrain::spawn(rd).finish(std::time::Duration::from_millis(200));
        assert_eq!(got, b"partial");
        assert!(t.elapsed() < std::time::Duration::from_secs(2));
        drop(wr);
    }
}
