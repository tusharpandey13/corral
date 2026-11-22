# corral

**Run heavy jobs on whichever machine has room, without ever running the one you're using out of memory.**

corral is a small job system for people with a few machines and not a datacenter: an always-on home server, a laptop
that sleeps, maybe a GPU box that is only sometimes on. You, or your AI coding agents, submit heavy commands
(benchmarks, model evals, video encodes, emulator runs, big builds). corral holds them until a suitable machine is
online **and has room**, runs them under guard, streams the logs back live, and returns the results.

> **Status: early development.** The specs are approved and milestone **M1** (scheduler, NATS, Linux worker, CLI,
> git and snapshot input, logs, events, artifacts) and **M2** (the guard in observe mode: it decides and logs, signals
> nothing; plus `admit`, `run` and `lock`) and **M3** (guard enforce mode, opt-in per host; the macOS worker; keep-awake holds; shims for legacy guard scripts) are
> implemented. Nothing is released yet; the measurement profiles and interactive leases are later milestones (`docs/SPEC.md` section 13).

---

## Why

If you run AI agents against your own hardware, you have probably seen something like this:

- An agent starts a 7 GB model benchmark while you're watching a film and the Android emulator is open. The laptop
  runs out of memory and everything dies, including the agent.
- Three sessions each decide to "just run the test suite" at the same moment.
- A job sat waiting for hours because the screen was locked, and nothing told you why.
- A home server that is always on and mostly idle, next to a laptop that does all the heavy work and is asleep half
  the day.

Over time these get patched with one-off scripts: a memory watchdog here, a lock file there, a "wait until the user
is idle" loop. corral turns them into **one system** with one set of rules.

## What it does

- **A queue across machines that come and go.** Jobs wait durably on the always-on host. Workers connect out when
  they wake up and claim work, so laptops behind NAT just work and nothing has to accept incoming connections.
- **Admission before execution.** A worker takes a job only if it fits:
  - **Memory:** the job's declared RAM fits under a memory ceiling with headroom, and there's no pressure or swap
    storm.
  - **Idle and presence:** for benchmark-grade runs, the user is idle, the screen is unlocked, the machine is on AC
    power, and no game or media is playing.
  - **Health:** temperature, disk space and power mode.
  - **Shared devices:** host-wide locks, for example "one emulator at a time".
  - It checks again while holding the lock, so nothing slips in between.
- **Says why a job is waiting.** For example: `waiting: mac-1 locked screen; box lacks capability metal`.
- **Guarded execution.** Each job runs in its own process group with a memory budget.
  - **Overrun:** if it exceeds its budget, or the machine comes under pressure, corral stops it before the OS has to.
  - **A killed job stays killed.** It's reported, never silently retried.
  - **A vanished worker is different.** If the machine disappears, the job is requeued with protection against the
    old attempt still running somewhere.
- **Placement by capability.** Jobs declare what they need (`metal`, `android`, `linux`, `x86_64`, `gpu`, …). They can
  prefer one machine and fall back to another after a timeout.
- **Live logs, progress, cancel.** `corral wait <id>` streams output and exits with the job's exit code, which is
  exactly what an AI agent's tool call wants.
- **No push needed.** Submit from a dirty working tree: corral snapshots it, so agents don't need to commit
  half-finished work to run a benchmark.
- **Interactive leases.** Hold a slot on a machine and run several commands in a row in one persistent workspace,
  for edit-run loops that need a specific GPU, display or device.
- **Keep-awake that respects you.** Takes a time-limited hold that keeps the machine from sleeping during a job, and
  never ends a keep-awake session you started yourself.
- **Auth-ready.** Each job and request carries its identity from day one. Per-identity permissions now, and a clean
  hook for an external authorization service later (OIDC, CIBA approvals for privileged actions).

## How it works

```
   you / AI agents                      always-on host                          workers
  ┌──────────────┐  submit / wait   ┌──────────────────────┐  claim · heartbeat  ┌───────────────┐
  │ corral (CLI) │ ───────────────▶ │ corral scheduler     │ ◀────────────────── │ corral worker │ laptop (sleeps)
  └──────────────┘ ◀─ live logs ─── │  job state (SQLite)  │ ── cancel ────────▶ │  admission    │
                                    │ message bus (NATS)   │ ◀── logs/progress ─ │  guard · run  │
                                    │ artifacts (HTTP)     │                     └───────────────┘
                                    └──────────────────────┘ ◀────────────────── worker: server, GPU box…
```

- **Transport:** [NATS](https://nats.io), a single small binary. It gives us reconnects, request/reply, fan-out of
  live logs, durable streams and pluggable auth without building our own protocol.
- **Scheduler, worker, CLI:** Rust. Static binaries, a few MB idle, careful process-group and signal handling.
- **Artifacts:** streamed over HTTP to the host, with size caps and retention.

## A taste of the CLI

```sh
# run a benchmark on the laptop when it's idle and has 8 GB free; fall back nowhere
corral submit --caps metal --ram 8G --exclusive --snapshot -- ./bench.sh --model q4
corral wait j-20261122-7f3a          # live logs; exits with the job's exit code

# a build that prefers the laptop but may run on the server after 30 minutes
corral submit --prefer laptop --fallback-after 30m --caps linux -- make release

corral status                        # what's queued, what's running, and why things wait
corral workers                       # who's online and what they'd admit right now
```

## Design principles

- **Never cause the OOM.** Protecting the machine someone is using comes before throughput.
- **A stop is a stop.** A killed job is reported, never retried behind your back.
- **Explain every wait.** A queued job always shows the reason it isn't running.
- **Machines are intermittent.** Sleep, Wi-Fi drops and lid closes are normal, not errors.
- **Agents are first-class users.** Blocking calls, plain-text output, exit codes that mean something.
- **One system, not a pile of scripts.** Admission, locks, guards and the queue share one model and one configuration.

## Non-goals

- A Kubernetes or Nomad replacement. corral targets roughly 1 to 10 machines that you own.
- Workflow DAGs and cron. A job is one command; schedule it with whatever you already use.
- Hard isolation. Jobs run as your user; macOS has no memory cgroups, so corral uses admission control plus a
  watchdog instead.

## Roadmap

1. Specs: scheduler, worker, admission and guards, protocol, auth hooks, end-to-end and chaos test rubric.
2. v1: scheduler + macOS/Linux worker + CLI, memory/idle/AC/thermal admission, host locks, live logs, artifacts.
3. Merge existing per-host memory guards into corral's guard.
4. Interactive leases.
5. External authorization (auth callout, CIBA approvals).

## Development

```sh
cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test
```

- Unit tests fake the clock, memory, process table, locks and the scheduler link behind traits; they touch no real
  system state.
- Integration tests (`crates/corral-cli/tests/`) start a real `nats-server` in a temporary directory with generated
  nkeys and the per-identity permissions of `docs/PROTOCOL.md` section 12, plus an in-process scheduler and workers.
  Install `nats-server` first (`brew install nats-server`, or your distribution's package); set `NATS_SERVER` to use
  a binary that is not on `PATH`. A few tests use `python3` and skip themselves without it.
- Crates: `corral-proto` (wire format, state machine, config), `corral-sched`, `corral-worker`, `corral-cli` (the
  `corral` binary).

## License

[MIT](LICENSE)
