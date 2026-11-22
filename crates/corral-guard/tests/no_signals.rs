//! R-GUARD-10 / U-15: observe mode never signals a process, and the reason is structural: this crate contains no
//! signalling code at all. Enforce mode (M3) will add it deliberately, and this test is where that is acknowledged.

use std::path::Path;

fn sources(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.is_dir() {
            sources(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

/// Built from pieces so this file does not trip its own scan.
fn banned() -> Vec<String> {
    let j = |a: &[&str]| a.concat();
    vec![
        j(&["kill", "_process"]),
        j(&["rustix::process::", "kill"]),
        j(&["Signal", "::"]),
        j(&[".kill", "()"]),
        j(&["start_", "kill"]),
        j(&["kill", "pg"]),
        j(&["lib", "c::"]),
        j(&["ni", "x::sys"]),
        j(&["ni", "x::unistd"]),
        j(&["SIG", "STOP"]),
        j(&["SIG", "CONT"]),
        j(&["SIG", "TERM"]),
        j(&["SIG", "KILL"]),
        j(&["Command::new(\"", "kill\")"]),
        j(&["Command::new(\"", "pkill\")"]),
    ]
}

#[test]
fn the_guard_crate_has_no_signalling_code() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    sources(&root.join("src"), &mut files);
    assert!(files.len() >= 10, "found only {} source files", files.len());
    let mut hits = Vec::new();
    for f in &files {
        let text = std::fs::read_to_string(f).unwrap();
        for b in banned() {
            if text.contains(&b) {
                hits.push(format!("{}: {b}", f.strip_prefix(root).unwrap().display()));
            }
        }
    }
    assert!(hits.is_empty(), "the guard crate must not signal processes in observe mode: {hits:?}");
}

#[test]
fn the_guard_crate_does_not_depend_on_a_signal_library() {
    let toml = std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml")).unwrap();
    for dep in ["libc", "nix", "signal-hook", "sysinfo"] {
        assert!(!toml.lines().any(|l| l.trim_start().starts_with(dep)), "{dep} would give the guard a way to signal");
    }
}

/// The helper programs the probes run are read-only inspection tools; none of them can signal.
#[test]
fn probe_helpers_are_a_fixed_read_only_list() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(root.join("src/sample.rs")).unwrap() + &std::fs::read_to_string(root.join("src/procs.rs")).unwrap();
    let mut progs: Vec<String> =
        text.match_indices("run_cmd(\"").map(|(i, _)| text[i + 9..].split('"').next().unwrap().to_string()).collect();
    progs.sort();
    progs.dedup();
    assert_eq!(progs, vec!["ioreg", "loginctl", "lsof", "pmset", "ps", "sysctl"]);
}
