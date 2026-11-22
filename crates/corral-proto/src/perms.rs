//! Per-identity NATS permissions (PROTOCOL.md section 12.1) and a `nats-server` configuration generator.
//!
//! The permission templates are data: the integration tests run every flow under them and try the forbidden ones
//! (R-SEC-01..03), so a missing subject here shows up as a failing flow.

use crate::ident::Identity;
use crate::subjects::KV_WORKERS;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Perms {
    pub publish: Vec<String>,
    pub subscribe: Vec<String>,
    /// Reply to requests received (the scheduler answers into callers' private inboxes).
    pub allow_responses: bool,
}

pub fn for_identity(id: &Identity) -> Perms {
    let slug = id.slug();
    let inbox = format!("_INBOX_{slug}.>");
    match id {
        Identity::Scheduler => Perms {
            publish: ["corral.ctl.>", "corral.wake.>", "corral.event.>", "$JS.API.>", "$JS.ACK.>", "$JS.FC.>", "$KV.>"]
                .map(String::from)
                .to_vec(),
            subscribe: vec!["corral.api.>".into(), "corral.work.>".into(), "corral.progress.>".into(), inbox, "$JS.API.>".into()],
            allow_responses: true,
        },
        Identity::Worker(w) => Perms {
            publish: vec![
                format!("corral.work.{w}.>"),
                format!("corral.log.{w}.>"),
                format!("corral.progress.{w}.>"),
                format!("$KV.{KV_WORKERS}.{w}"),
            ],
            subscribe: vec![format!("corral.ctl.{w}"), format!("corral.wake.{w}"), inbox],
            allow_responses: false,
        },
        Identity::Agent(_) | Identity::Owner => {
            // No JetStream API at all: a consumer an agent could create (a push consumer delivering a job's output to
            // `corral.ctl.<w>`, or an edit of the scheduler's `sink` consumer) is a way to forge control messages
            // and to stop log archiving. Clients also may not subscribe to `corral.event.>` (every job's events): followers ask the scheduler (`follow`), which creates consumers that
            // deliver into the caller's own inbox space.
            Perms { publish: vec![format!("corral.api.{slug}.>")], subscribe: vec![inbox], allow_responses: false }
        }
    }
}

fn q(s: &str) -> String {
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
}

fn list(items: &[String]) -> String {
    items.iter().map(|s| q(s)).collect::<Vec<_>>().join(", ")
}

/// A complete `nats-server` configuration: one account `CORRAL` with JetStream, no anonymous access, one nkey user
/// per identity with the permission template above.
pub fn nats_server_conf(listen: &str, store_dir: &str, users: &[(Identity, String)]) -> String {
    let mut out = String::new();
    out.push_str(&format!("listen: {listen}\n"));
    out.push_str("server_name: corral\n");
    out.push_str(&format!("jetstream {{\n  store_dir: {}\n}}\n", q(store_dir)));
    out.push_str("accounts {\n  CORRAL {\n    jetstream: enabled\n    users: [\n");
    for (id, nkey) in users {
        let p = for_identity(id);
        out.push_str(&format!("      {{ nkey: {}\n        permissions: {{\n", q(nkey)));
        out.push_str(&format!("          publish: {{ allow: [{}] }}\n", list(&p.publish)));
        out.push_str(&format!("          subscribe: {{ allow: [{}] }}\n", list(&p.subscribe)));
        if p.allow_responses {
            out.push_str("          allow_responses: true\n");
        }
        out.push_str("        }\n      }\n");
    }
    out.push_str("    ]\n  }\n}\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// R-SEC-02: each identity may publish only on its own subjects.
    #[test]
    fn identities_are_scoped() {
        let w = for_identity(&Identity::Worker("laptop".into()));
        assert!(w.publish.iter().all(|s| !s.contains("api") && !s.starts_with("corral.ctl")));
        assert!(w.publish.contains(&"corral.work.laptop.>".to_string()));
        let a = for_identity(&Identity::Agent("laptop".into()));
        assert!(a.publish.contains(&"corral.api.agent-laptop.>".to_string()));
        assert!(!a.publish.iter().any(|s| s.starts_with("corral.work") || s.starts_with("corral.log")));
        // R-SEC-02 (M1 review): no JetStream API (consumer create/delete/update, purge) for clients, and no flow
        // control subjects either.
        for who in [Identity::Agent("laptop".into()), Identity::Owner] {
            let p = for_identity(&who);
            assert_eq!(p.publish.len(), 1, "{who}: only its own corral.api.<slug>.>");
            assert!(!p.publish.iter().any(|s| s.contains("$JS")), "{who}: {:?}", p.publish);
        }
        let wk = for_identity(&Identity::Worker("laptop".into()));
        assert!(!wk.publish.iter().any(|s| s.contains("$JS.API")), "workers publish through their own subjects only");
        let o = for_identity(&Identity::Owner);
        assert!(o.publish.contains(&"corral.api.owner.>".to_string()));
        assert!(for_identity(&Identity::Scheduler).allow_responses);
    }

    #[test]
    fn conf_has_no_anonymous_access_and_lists_users() {
        let conf = nats_server_conf("127.0.0.1:4222", "/tmp/js", &[(Identity::Owner, "UABC".into())]);
        assert!(conf.contains("nkey: \"UABC\""));
        assert!(!conf.contains("no_auth_user"));
        assert!(conf.contains("jetstream: enabled"));
    }
}
