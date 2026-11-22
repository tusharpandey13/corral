//! U-15 / R-AWAKE-02 / R-GUARD-04: nothing in the source tree kills by name or signals "everything".

use std::path::Path;

fn sources(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.is_dir() {
            if p.file_name().is_some_and(|n| n == "target" || n == ".git") {
                continue;
            }
            sources(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

#[test]
fn no_kill_by_name_anywhere() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../crates");
    let mut files = Vec::new();
    sources(&root, &mut files);
    assert!(files.len() > 20, "found only {} source files", files.len());
    // Built from pieces so this file does not trip its own check.
    let banned: Vec<String> = vec![
        ["p", "kill"].concat(),
        ["kill", "all"].concat(),
        ["kill", " -", "1"].concat(),
        ["kill_", "process_group(Pid::from_raw(0"].concat(),
        ["kill_", "process_group(Pid::from_raw(-1"].concat(),
    ];
    let mut hits = Vec::new();
    for f in files.iter().filter(|f| !f.ends_with("static_checks.rs")) {
        let text = std::fs::read_to_string(f).unwrap();
        for b in &banned {
            if text.contains(b.as_str()) {
                hits.push(format!("{}: {b}", f.display()));
            }
        }
    }
    assert!(hits.is_empty(), "name-based or broadcast kills are forbidden: {hits:?}");
}

/// The workspace forbids `unsafe` (every signal goes through `rustix`'s safe API).
#[test]
fn unsafe_code_is_forbidden_by_the_workspace() {
    let toml = std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Cargo.toml")).unwrap();
    assert!(toml.contains("unsafe_code = \"forbid\""));
}
