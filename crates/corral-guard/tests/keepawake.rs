//! M3: keep-awake holds over the guard socket and the `keepawake_unavailable` refusal (R-AWAKE-01, R-AWAKE-03),
//! against a fake provider script that touches only its own directory.

use corral_guard::testing::{Rig, tmpdir};
use serde_json::json;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

fn provider(body: &str) -> (PathBuf, PathBuf) {
    let d = tmpdir("ka");
    let p = d.join("fake-awake");
    std::fs::write(&p, format!("#!/bin/sh\necho \"$@\" >> \"{}/calls\"\n{body}\n", d.display())).unwrap();
    std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
    (p, d.join("calls"))
}

const OK: &str = r#"case "$1" in
hold) echo "hold $5 until 2026-11-22 18:00:00 (pid 4242)";;
release) exit 0;;
holds) echo '[{"owner":"someone-else","until":1,"pid":9,"started":0,"display":0,"bound_pid":null},{"owner":"corral:j-1:1","until":1790000000,"pid":4242,"started":0,"display":0,"bound_pid":77}]';;
esac"#;

fn rig(cmd: &PathBuf, extra: &str) -> Rig {
    Rig::new(&format!("[guard.keepawake]\nprovider = \"hold-cli\"\ncommand = \"{}\"\n{extra}", cmd.display()))
}

#[test]
fn hold_list_and_release_go_through_the_provider_and_only_show_corral_holds() {
    let (cmd, calls) = provider(OK);
    let r = rig(&cmd, "");
    let h = r.guard.handle(&json!({ "v": 1, "op": "hold", "owner": "corral:run:build", "for_s": 900, "bound_pid": 77, "display": true }));
    assert_eq!(h["ok"], true, "{h}");
    assert_eq!(h["pid"], 4242);
    let l = r.guard.handle(&json!({ "v": 1, "op": "holds" }));
    let owners: Vec<&str> = l["holds"].as_array().unwrap().iter().map(|h| h["owner"].as_str().unwrap()).collect();
    assert_eq!(owners, vec!["corral:j-1:1"], "the other owner's hold is filtered out");
    assert_eq!(r.guard.handle(&json!({ "v": 1, "op": "release_hold", "owner": "corral:run:build" }))["ok"], true);
    let text = std::fs::read_to_string(calls).unwrap();
    assert!(text.contains("hold --for 900s --owner corral:run:build --pid 77 --display"));
    assert!(text.contains("release --owner corral:run:build") && !text.contains("--all"));
    // a name that is not ours never reaches the provider
    let bad = r.guard.handle(&json!({ "v": 1, "op": "release_hold", "owner": "someone-else" }));
    assert_eq!(bad["ok"], false);
}

/// R-AWAKE-03: required for heavy + failing provider -> heavy refused with `keepawake_unavailable`; light is not.
#[test]
fn a_required_but_broken_provider_refuses_heavy_work() {
    let (cmd, _) = provider("echo broken >&2; exit 1");
    let r = rig(&cmd, "required_for = [\"heavy\"]\n");
    r.tick();
    let a = r.guard.handle(&json!({ "v": 1, "op": "admit", "ram_mb": 100, "class": "heavy" }));
    assert_eq!(a["ok"], false);
    assert_eq!(a["reasons"][0]["check"], "keepawake_unavailable", "{a}");
    assert_eq!(r.guard.handle(&json!({ "v": 1, "op": "admit", "ram_mb": 100, "class": "light" }))["ok"], true);
    // not required: admitted, and `checks` still tells
    let r = rig(&cmd, "");
    r.tick();
    assert_eq!(r.guard.handle(&json!({ "v": 1, "op": "admit", "ram_mb": 100, "class": "heavy" }))["ok"], true);
}

#[test]
fn without_a_provider_the_hold_ops_say_so() {
    let r = Rig::new("");
    let h = r.guard.handle(&json!({ "v": 1, "op": "hold", "owner": "corral:x", "for_s": 60 }));
    assert_eq!(h["ok"], false);
    assert!(h["error"].as_str().unwrap().contains("keepawake"));
}
