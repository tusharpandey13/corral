//! Envelope, errors and request/reply encoding (PROTOCOL.md section 2).

use crate::ident::Identity;
use crate::{PROTOCOL_VERSION, SUPPORTED_VERSIONS};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::fmt;

/// The fields every request carries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub v: u32,
    pub from: String,
    pub req_id: String,
    /// Optional grant token (PROTOCOL.md 12.3). Ignored by v1 policy unless an action requires a grant.
    #[serde(default)]
    pub authz: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    BadRequest,
    Forbidden,
    NotFound,
    Conflict,
    StaleLease,
    GrantRequired,
    Unavailable,
    TooLarge,
    VersionUnsupported,
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::BadRequest => "bad_request",
            ErrorCode::Forbidden => "forbidden",
            ErrorCode::NotFound => "not_found",
            ErrorCode::Conflict => "conflict",
            ErrorCode::StaleLease => "stale_lease",
            ErrorCode::GrantRequired => "grant_required",
            ErrorCode::Unavailable => "unavailable",
            ErrorCode::TooLarge => "too_large",
            ErrorCode::VersionUnsupported => "version_unsupported",
        }
    }
}

/// A protocol-level error, also the Rust error type of API calls.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiError {
    pub error: ErrorCode,
    #[serde(default)]
    pub message: String,
    /// Extra fields (`supported`, `field`, `action`, `waiting`, ...).
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl ApiError {
    pub fn new(error: ErrorCode, message: impl Into<String>) -> Self {
        ApiError {
            error,
            message: message.into(),
            extra: Map::new(),
        }
    }
    pub fn bad(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::BadRequest, message)
    }
    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Forbidden, message)
    }
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotFound, message)
    }
    pub fn with(mut self, key: &str, value: Value) -> Self {
        self.extra.insert(key.to_string(), value);
        self
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.error.as_str(), self.message)
    }
}

impl std::error::Error for ApiError {}

/// Encode a request: the envelope fields merged with the body fields.
pub fn encode_request<B: Serialize>(from: &Identity, req_id: &str, body: &B) -> Vec<u8> {
    let mut obj = match serde_json::to_value(body) {
        Ok(Value::Object(m)) => m,
        _ => Map::new(),
    };
    obj.insert("v".into(), json!(PROTOCOL_VERSION));
    obj.insert("from".into(), json!(from.to_string()));
    obj.insert("req_id".into(), json!(req_id));
    obj.entry("authz").or_insert(Value::Null);
    serde_json::to_vec(&Value::Object(obj)).expect("json encode")
}

/// Decode and version-check a request. Returns the envelope and the full JSON object (the body is parsed from it
/// by the caller, unknown fields ignored).
pub fn decode_request(bytes: &[u8]) -> Result<(Envelope, Value), ApiError> {
    let value: Value =
        serde_json::from_slice(bytes).map_err(|e| ApiError::bad(format!("invalid JSON: {e}")))?;
    let v = value
        .get("v")
        .and_then(Value::as_u64)
        .ok_or_else(|| ApiError::bad("missing v").with("field", json!("v")))?;
    if !SUPPORTED_VERSIONS.contains(&(v as u32)) {
        return Err(ApiError::new(
            ErrorCode::VersionUnsupported,
            format!("protocol version {v} not supported"),
        )
        .with("supported", json!(SUPPORTED_VERSIONS)));
    }
    let env: Envelope = serde_json::from_value(value.clone())
        .map_err(|e| ApiError::bad(format!("bad envelope: {e}")))?;
    Ok((env, value))
}

/// Parse the typed body out of a decoded request object.
pub fn body<B: for<'de> Deserialize<'de>>(value: &Value) -> Result<B, ApiError> {
    serde_json::from_value(value.clone()).map_err(|e| ApiError::bad(e.to_string()))
}

/// Encode a successful reply.
pub fn encode_reply<T: Serialize>(result: &T) -> Vec<u8> {
    let mut obj = match serde_json::to_value(result) {
        Ok(Value::Object(m)) => m,
        _ => Map::new(),
    };
    obj.insert("v".into(), json!(PROTOCOL_VERSION));
    serde_json::to_vec(&Value::Object(obj)).expect("json encode")
}

/// Encode an error reply.
pub fn encode_error(e: &ApiError) -> Vec<u8> {
    let mut obj = match serde_json::to_value(e) {
        Ok(Value::Object(m)) => m,
        _ => Map::new(),
    };
    obj.insert("v".into(), json!(PROTOCOL_VERSION));
    serde_json::to_vec(&Value::Object(obj)).expect("json encode")
}

/// Encode either outcome.
pub fn encode_result<T: Serialize>(r: &Result<T, ApiError>) -> Vec<u8> {
    match r {
        Ok(v) => encode_reply(v),
        Err(e) => encode_error(e),
    }
}

/// Decode a reply into the result type, or the error it carries.
pub fn decode_reply<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, ApiError> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|e| ApiError::bad(format!("invalid reply JSON: {e}")))?;
    if value.get("error").is_some() {
        let err: ApiError = serde_json::from_value(value)
            .map_err(|e| ApiError::bad(format!("bad error reply: {e}")))?;
        return Err(err);
    }
    serde_json::from_value(value).map_err(|e| ApiError::bad(format!("bad reply: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Body {
        id: String,
    }

    #[test]
    fn request_round_trip_and_authz_is_present() {
        let bytes = encode_request(&Identity::Owner, "r1", &Body { id: "j-1".into() });
        let (env, val) = decode_request(&bytes).unwrap();
        assert_eq!(env.v, 1);
        assert_eq!(env.from, "owner");
        assert!(
            val.get("authz").is_some(),
            "R-SEC-04: every request schema carries authz"
        );
        assert_eq!(body::<Body>(&val).unwrap(), Body { id: "j-1".into() });
    }

    #[test]
    fn unsupported_version_is_reported_with_the_supported_list() {
        let e = decode_request(br#"{"v":99,"from":"owner","req_id":"x"}"#).unwrap_err();
        assert_eq!(e.error, ErrorCode::VersionUnsupported);
        assert_eq!(e.extra["supported"], json!([1]));
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let (_, val) =
            decode_request(br#"{"v":1,"from":"owner","req_id":"x","id":"a","future":1}"#).unwrap();
        assert_eq!(body::<Body>(&val).unwrap().id, "a");
    }

    #[test]
    fn error_reply_round_trip() {
        let bytes = encode_error(&ApiError::forbidden("nope").with("action", json!("x")));
        let e = decode_reply::<Body>(&bytes).unwrap_err();
        assert_eq!(e.error, ErrorCode::Forbidden);
        assert_eq!(e.extra["action"], json!("x"));
    }
}
