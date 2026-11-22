//! HTTP capability tokens (PROTOCOL.md section 10): `base64url(payload).base64url(HMAC-SHA256(key, payload))`.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// What a token allows: one job (or upload id), one verb, until `exp` (unix seconds).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Token {
    pub job: String,
    pub verb: String,
    /// The attempt's `lease_token` for worker tokens, binding the token to one attempt.
    #[serde(default)]
    pub lease: Option<String>,
    pub exp: i64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TokenError {
    Malformed,
    BadSignature,
    Expired,
    WrongScope,
}

impl std::fmt::Display for TokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            TokenError::Malformed => "malformed token",
            TokenError::BadSignature => "bad token signature",
            TokenError::Expired => "token expired",
            TokenError::WrongScope => "token does not cover this request",
        })
    }
}

fn mac(key: &[u8]) -> HmacSha256 {
    HmacSha256::new_from_slice(key).expect("hmac accepts any key length")
}

pub fn mint(key: &[u8], t: &Token) -> String {
    let payload = serde_json::to_vec(t).expect("json encode");
    let mut m = mac(key);
    m.update(&payload);
    format!("{}.{}", B64.encode(&payload), B64.encode(m.finalize().into_bytes()))
}

/// Verify signature and expiry (`now_s` is unix seconds).
pub fn verify(key: &[u8], token: &str, now_s: i64) -> Result<Token, TokenError> {
    let (p, s) = token.split_once('.').ok_or(TokenError::Malformed)?;
    let payload = B64.decode(p).map_err(|_| TokenError::Malformed)?;
    let sig = B64.decode(s).map_err(|_| TokenError::Malformed)?;
    let mut m = mac(key);
    m.update(&payload);
    m.verify_slice(&sig).map_err(|_| TokenError::BadSignature)?;
    let t: Token = serde_json::from_slice(&payload).map_err(|_| TokenError::Malformed)?;
    if t.exp <= now_s {
        return Err(TokenError::Expired);
    }
    Ok(t)
}

/// Verify and also require the scope `job` + `verb`.
pub fn verify_scope(key: &[u8], token: &str, now_s: i64, job: &str, verb: &str) -> Result<Token, TokenError> {
    let t = verify(key, token, now_s)?;
    if t.job != job || t.verb != verb {
        return Err(TokenError::WrongScope);
    }
    Ok(t)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tok(job: &str, verb: &str, exp: i64) -> Token {
        Token { job: job.into(), verb: verb.into(), lease: None, exp }
    }

    /// U-17 / R-SEC-05: scope, expiry, tampering.
    #[test]
    fn scope_expiry_tamper() {
        let key = b"k";
        let t = mint(key, &tok("j-a", "artifacts:get", 100));
        assert!(verify_scope(key, &t, 50, "j-a", "artifacts:get").is_ok());
        assert_eq!(verify_scope(key, &t, 50, "j-b", "artifacts:get"), Err(TokenError::WrongScope), "token for job A cannot read job B");
        assert_eq!(verify_scope(key, &t, 50, "j-a", "log:get"), Err(TokenError::WrongScope));
        assert_eq!(verify(key, &t, 100), Err(TokenError::Expired));
        assert_eq!(verify(b"other", &t, 50), Err(TokenError::BadSignature));
        let (p, s) = t.split_once('.').unwrap();
        let forged = mint(key, &tok("j-b", "artifacts:get", 100));
        let (fp, _) = forged.split_once('.').unwrap();
        assert_eq!(verify(key, &format!("{fp}.{s}"), 50), Err(TokenError::BadSignature));
        assert_eq!(verify(key, &format!("{p}x"), 50), Err(TokenError::Malformed));
        assert_eq!(verify(key, "garbage", 50), Err(TokenError::Malformed));
    }

    #[test]
    fn lease_binding_round_trips() {
        let t = Token { job: "j".into(), verb: "artifacts:put".into(), lease: Some("abc".into()), exp: 10 };
        assert_eq!(verify(b"k", &mint(b"k", &t), 1).unwrap(), t);
    }
}
