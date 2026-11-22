//! Caller identities (PROTOCOL.md section 2, 12).

use serde::{Deserialize, Serialize};
use std::fmt;

/// Who is talking. Payload form: `scheduler`, `worker:<host>`, `agent:<host>`, `owner`; subject form (slug):
/// `scheduler`, `worker-<host>`, `agent-<host>`, `owner`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Identity {
    Scheduler,
    Worker(String),
    Agent(String),
    Owner,
}

impl Identity {
    pub fn parse(s: &str) -> Option<Identity> {
        match s {
            "scheduler" => Some(Identity::Scheduler),
            "owner" => Some(Identity::Owner),
            _ => {
                let (kind, host) = s.split_once(':')?;
                valid_host(host).then(|| match kind {
                    "worker" => Some(Identity::Worker(host.to_string())),
                    "agent" => Some(Identity::Agent(host.to_string())),
                    _ => None,
                })?
            }
        }
    }

    /// The subject-safe form.
    pub fn slug(&self) -> String {
        match self {
            Identity::Scheduler => "scheduler".into(),
            Identity::Owner => "owner".into(),
            Identity::Worker(h) => format!("worker-{h}"),
            Identity::Agent(h) => format!("agent-{h}"),
        }
    }

    /// Parse a slug back into an identity.
    pub fn from_slug(slug: &str) -> Option<Identity> {
        match slug {
            "scheduler" => Some(Identity::Scheduler),
            "owner" => Some(Identity::Owner),
            _ => {
                if let Some(h) = slug.strip_prefix("worker-") {
                    valid_host(h).then(|| Identity::Worker(h.to_string()))
                } else if let Some(h) = slug.strip_prefix("agent-") {
                    valid_host(h).then(|| Identity::Agent(h.to_string()))
                } else {
                    None
                }
            }
        }
    }

    pub fn is_owner(&self) -> bool {
        matches!(self, Identity::Owner)
    }

    /// Per-identity inbox prefix so clients cannot read each other's replies.
    pub fn inbox_prefix(&self) -> String {
        format!("_INBOX_{}", self.slug())
    }
}

/// Host names appear in subjects: no dots, wildcards or whitespace.
pub fn valid_host(h: &str) -> bool {
    !h.is_empty() && h.len() <= 63 && h.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

impl fmt::Display for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Identity::Scheduler => write!(f, "scheduler"),
            Identity::Owner => write!(f, "owner"),
            Identity::Worker(h) => write!(f, "worker:{h}"),
            Identity::Agent(h) => write!(f, "agent:{h}"),
        }
    }
}

impl Serialize for Identity {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Identity {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Identity::parse(&s).ok_or_else(|| serde::de::Error::custom(format!("bad identity {s:?}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        for s in ["scheduler", "owner", "worker:server", "agent:laptop"] {
            let id = Identity::parse(s).unwrap();
            assert_eq!(id.to_string(), s);
            assert_eq!(Identity::from_slug(&id.slug()).unwrap(), id);
        }
        assert!(Identity::parse("agent:").is_none());
        assert!(Identity::parse("agent:a.b").is_none());
        assert!(Identity::parse("root").is_none());
        assert!(Identity::from_slug("worker-a>").is_none());
    }
}
