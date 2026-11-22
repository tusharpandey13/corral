//! R-GUARD-10 / U-15: observe mode never signals a process, and the reason is structural: the only code in this
//! crate that can signal lives in `src/enforce.rs` (M3), the executor of enforce mode. Every other file must stay
//! free of it, and nothing anywhere may kill by name.

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
        j(&["Command::new(\"p", "kill\")"]),
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
        if f.file_name().is_some_and(|n| n == "enforce.rs") {
            continue; // the one acknowledged place
        }
        let text = std::fs::read_to_string(f).unwrap();
        for b in banned() {
            if text.contains(&b) {
                hits.push(format!("{}: {b}", f.strip_prefix(root).unwrap().display()));
            }
        }
    }
    assert!(hits.is_empty(), "only src/enforce.rs may signal processes: {hits:?}");
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

/// Even the executor never kills by name or by pattern, and takes no signal library.
#[test]
fn the_executor_never_kills_by_name() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(root.join("src/enforce.rs")).unwrap();
    let body = text.split("#[cfg(test)]").next().unwrap();
    for b in [["p", "kill"].concat(), ["kill", "all"].concat(), ["lib", "c::"].concat()] {
        assert!(!body.contains(&b), "{b} in enforce.rs");
    }
}
