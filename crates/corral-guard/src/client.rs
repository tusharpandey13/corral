//! A blocking client for the guard socket, for the CLI (`admit`, `run`, `guard status`, ...).

use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

#[derive(Debug)]
pub enum ClientError {
    /// No guard answers on the socket: callers treat this as "refused" (exit 5).
    Unreachable(String),
    Other(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Unreachable(m) | ClientError::Other(m) => f.write_str(m),
        }
    }
}

/// Send `{"v":1,"op":op,...fields}` and read one reply line. An `ok: false` reply is returned, not an error.
pub fn request(socket: &Path, op: &str, fields: Value) -> Result<Value, ClientError> {
    let mut req = json!({ "v": 1, "op": op });
    if let (Some(o), Some(f)) = (req.as_object_mut(), fields.as_object()) {
        o.extend(f.clone());
    }
    let mut s =
        UnixStream::connect(socket).map_err(|e| ClientError::Unreachable(format!("guard unreachable at {}: {e}", socket.display())))?;
    let _ = s.set_read_timeout(Some(Duration::from_secs(15)));
    let _ = s.set_write_timeout(Some(Duration::from_secs(5)));
    s.write_all(format!("{req}\n").as_bytes()).map_err(|e| ClientError::Unreachable(format!("guard unreachable: {e}")))?;
    let mut line = String::new();
    BufReader::new(s).read_line(&mut line).map_err(|e| ClientError::Other(format!("no reply from the guard: {e}")))?;
    serde_json::from_str(line.trim()).map_err(|e| ClientError::Other(format!("bad reply from the guard: {e}")))
}
