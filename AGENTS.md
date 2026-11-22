# AGENTS.md: working on corral

Rules for AI coding agents (and humans) working in this repo. Read it in full before changing anything.

## Status

The specs in `docs/` were **approved by the owner on 22-Nov-2026**. **Milestone M1 is in progress** (scheduler,
NATS, Linux worker, CLI, git/snapshot input, logs, events, artifacts: SPEC.md section 13). Later milestones (M2 and
on) stay unimplemented until their turn; the round-2 drafts (exec, classifier, agent jobs, external tokens) are still
drafts.

- Implement approved specs only, one milestone at a time, with tests that cite requirement IDs.
- A spec you wrote yourself is not an approved spec. When code and spec disagree, change the spec first.
- Where the spec is ambiguous, pick a sensible default and record it as a short "Implementation note" in the spec.

## What corral is

A job system for a few intermittent machines:
- A scheduler on an always-on host.
- Workers on laptops and servers that connect out to it.
- A CLI for people and AI agents.
- Admission control and guards so that heavy jobs never run the machine someone is using out of memory.

The product summary is in `README.md`; the specs are in `docs/`.

## Ground rules

- **Public repo. No private data, ever.** Write `host-a`, `laptop` or `example.ts.net`. Never commit:
  - Real hostnames, tailnet names, IP addresses, usernames or home paths.
  - Disk layouts, tokens or keys.
  - Content copied from the owner's private docs.
  - Deployment specifics for the owner's machines: those stay in the owner's private deployment repo.
- **Spec first.** Behaviour changes start as a spec change in `docs/` (numbered requirements plus acceptance
  criteria), are approved, and are then implemented with tests that cite the requirement IDs.
- **Small focused commits.** Imperative subject; the body says why. One logical change per commit.
- **Never weaken a safety rule to make a test pass.** These rules are the product:
  - Never cause an OOM.
  - A killed job is final and is never retried.
  - Every wait has a reason.
  - Admission is checked again while holding the lock.

## Engineering conventions (once code lands)

- **Language and toolchain:**
  - Rust, stable toolchain, edition 2024; `cargo fmt`, `cargo clippy -- -D warnings`, `cargo test`.
  - Must build and pass tests on macOS (aarch64) and Linux (x86_64).
- **Dependencies:** few and well maintained. Justify each new crate in the PR or commit body.
- **Processes:** every job runs in its own process group. Terminate with TERM, then KILL after a grace period, always
  on the group. Never signal processes corral didn't start.
- **Keep-awake on macOS:** never `pkill caffeinate` or kill any process by name. Only signal PIDs that corral
  recorded itself. Killing someone else's keep-awake process puts their machine to sleep.
- **Interfaces:**
  - CLI output is plain text by default, `--json` for machines.
  - Exit codes are documented and stable.
  - Messages on the bus are versioned (`v` field) and carry the caller identity and an `authz` field from v1.

## Tests

- Run `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` and `cargo test` before every commit.
- Integration tests need a `nats-server` binary (see `README.md`, "Development").

- Unit tests must not touch real system state.
  - Fake the clock, the memory and pressure readers, idle time, power and thermal state, and the locks behind traits.
  - Never use real system signals other than on processes the test itself started.
- Integration tests run a real NATS server and scheduler in a temporary directory, with workers using fake
  capabilities.
- Chaos and end-to-end rubrics live in `docs/`. Chaos tests run only in a sandbox (temporary directories, test-only
  NATS accounts), never against a live installation unless the owner explicitly asks.

## Layout

```
docs/            specs (SPEC.md, protocol, admission, test rubrics)
crates/
  corral-proto   message schema, subjects, versioning
  corral-sched   scheduler: job state (SQLite), placement, leases, artifacts HTTP
  corral-worker  admission, guards, runner, keep-awake holds
  corral-cli     `corral` command
```
