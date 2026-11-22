# corral: design spec

Status: **approved by the owner on 22-Nov-2026 (v0.1); milestone M1 implemented, later milestones not started.** The
round-2 sections marked draft (exec, agent jobs, classifier, external tokens) are not part of the approval. Companions:

| Document | Scope |
|---|---|
| `docs/SPEC.md` (this file) | Overview, goals, glossary, requirements, architecture, justifications, configuration, packaging, observability, failure modes, versioning, scope, prior art, open questions. |
| `docs/PROTOCOL.md` | NATS subjects, message schemas, job state machine, leases, cancel, logs, presence, artifacts over HTTP. |
| `docs/GUARDS.md` | The unified guard: system-wide memory/thermal protection, `corral admit`, host locks, presence signals, keep-awake, migration from one-off scripts. |
| `docs/RUNTIMES.md` | The native runtime (v1), the runtime adapter interface, phase-2 cloud runtimes (TODO). |

Owner decisions this spec rests on (22-Nov-2026):
1. corral **formalizes** existing one-off scripts (memory guards, lock files, idle gates, a measurement queue) into
   **one system**: guards, locks, admission and the queue share one model and one configuration.
2. corral stays a **thin layer**: no cluster scheduler, provisioning or billing of its own.
3. v1 runtime = **native processes only**. No container runtime (too costly on CPU and RAM on small machines).
4. A `runtime` adapter interface is defined now; cloud runtimes are **phase 2** (`docs/RUNTIMES.md` TODO section).
5. **Rust**; **NATS** for transport; **prefer-local-first** placement.

Unverified external facts are marked **[unverified]**.

Round 2 (22-Nov-2026, draft): agent work placement (G11): remote exec (§4.14), agent jobs (§4.15), classifier and
hook mode (§4.16), light/heavy admission classes (R-ADM-08), external DPoP-bound tokens (R-SEC-07). Not approved.

---

## 1. Overview

corral runs heavy commands for people and AI agents on whichever of a few personal machines has room, **without
hurting the machine someone is using**. A job is submitted to a scheduler on an always-on host (`server`); it waits
durably until a worker on some host (`server`, `laptop`, `gpu-box`) admits it; logs and progress stream back live;
artifacts come back over HTTP. On every host a single **guard** daemon protects the machine from memory and thermal
overload, for corral jobs and for any other heavy process it is configured to watch.

Parts:

| Part | Where | Role |
|---|---|---|
| NATS server (JetStream on) | `server` | Message bus. Everything dials into it. Not written by us. |
| `corral scheduler` | `server` | Source of truth for job state (SQLite), placement, leases, HTTP for inputs/artifacts/log archive. |
| `corral worker` | every host that runs jobs | Claims jobs, runs admission, runs jobs through a runtime, streams logs. |
| `corral guard` | every host (with or without a worker) | System-wide memory/thermal protection, kill tiers, kill switch, host locks, local admission, keep-awake holds. |
| `corral` CLI | anywhere | `submit`, `wait`, `logs`, `status`, `cancel`, `lease`, `admit`, `run`, `lock`, `guard`, `admin`. |

All of these are one Rust binary, `corral`, with subcommands (open question Q1).

## 2. Goals and non-goals

Goals:
- **G1. Never cause the OOM.** No job and no watched heavy process takes a host past its configured memory ceiling;
  the guard acts before the OS has to.
- **G2. One heavy job per host** (configurable slots), counted across corral jobs and every heavy process the guard
  watches, including ones started by other tools.
- **G3. Every wait has a reason**, visible to the submitter, machine- and human-readable.
- **G4. A stop is a stop.** A job killed by a guard, timeout, kill switch or external signal is final and is never
  retried automatically. Only a vanished worker (nobody saw the job end) may requeue, fenced by attempt tokens.
- **G5. Intermittent machines are normal.** Workers sleep, roam and are offline for days; jobs wait; nothing is lost.
- **G6. Agent-first.** Blocking `wait` that streams logs and exits with the job's exit code; plain-text output,
  `--json` for programs; stable exit codes; no push of half-finished work needed (worktree snapshots).
- **G7. One system, not a pile of scripts.** Guards, locks, admission and the queue share one model, one
  configuration file per host, one log and one notification path; existing scripts migrate through shims with
  rollback.
- **G8. Auth-ready.** Identities and per-subject permissions from v1; every request carries an `authz` field so an
  external authorization service (auth callout, CIBA approvals) can be enabled later without schema changes.
- **G9. Thin.** Reuse NATS for transport, SQLite for state, the OS for isolation. No provisioning, no billing, no
  cluster consensus.
- **G10. Interactive leases.** A session on one host can hold a slot on another and run successive commands in a
  persistent, synced work directory.
- **G11. Agent work placement** (draft, 22-Nov-2026 round 2). An agent session that lives on an always-on but weak
  host can send its file-heavy commands to a stronger host transparently: a command classifier, a pre-tool hook mode
  that rewrites commands to `corral exec`, quick remote exec with dirty-tree sync, and agent jobs that run a whole
  sub-agent on another host and return a branch and a summary.

Non-goals:
- **N1.** Not Kubernetes, Nomad or Slurm. Target: 1 to 10 machines owned by one person.
- **N2.** No workflow DAGs, matrices or cron. A job is one command.
- **N3.** No container runtime in v1 and no hard isolation. Jobs run as the worker's user; macOS has no memory
  cgroups, so corral uses admission plus a watchdog. Linux uses cgroups v2 where available (`docs/RUNTIMES.md`).
- **N4.** No multi-tenancy. All identities act for one owner; identities exist for attribution, least privilege
  and future approvals.
- **N5.** No GUI in v1. CLI, JSON, metrics, notifications.
- **N6.** No domain logic of the scripts being replaced (experiment stop rules, result voiding, display dimming):
  those stay in their tools, which call `corral admit`/`corral run` or use hooks.
- **N7.** No cloud provisioning in v1 (phase 2, through runtimes, under a budget cap).

## 3. Glossary

| Term | Meaning |
|---|---|
| host | One machine. Runs one `corral guard`, optionally one `corral worker`. Named in config (`laptop`, `server`). |
| server | The always-on host running NATS and the scheduler. |
| job | One command plus input, requirements and limits. Id `j-YYYYMMDD-xxxxxx` (6 lowercase base32 chars). |
| attempt | One run of a job on one worker. Numbered from 1. Only a lost worker creates another attempt. |
| attempt token | Random 128-bit fencing token per attempt (`lease_token`). The scheduler rejects heartbeats, logs and events carrying a stale one. |
| worker | A `corral worker` process; offers caps and slots; pulls work. |
| caps | Capability tags a worker offers and a job requires (`macos`, `linux`, `arm64`, `x86_64`, `metal`, `gpu`, `android`, `tcc-input`, `tcc-capture`, ...). Free-form strings; matching is set inclusion. |
| class | `heavy` (default) or `light` (declared RAM ≤ 1 GiB, not exclusive). Heavy jobs consume a heavy slot. Light work is admitted while the host's user is active; heavy work needs the full profile (R-ADM-08). |
| exec session | A short-lived interactive lease of kind `exec` used by `corral exec`: no queueing, a work directory at the **same absolute path** as on the submitting host (`mirror`) or a scratch dir, dirty-tree sync before each command (§4.14). |
| agent job | A job of kind `agent`: starts a coding agent on the worker's host in its own git worktree and branch, returns branch, head and summary (§4.15). |
| classifier | `corral classify`: splits a shell command into segments and assigns each a placement class (`local_only`, `local_ok`, `heavy`, `unknown`) from configured rules (§4.16). |
| hook mode | Running the classifier as an agent harness pre-tool hook that lets a command run locally, rewrites it to `corral exec`, denies it with a reason, or queues it (§4.16). |
| exclusive | A benchmark-grade job: needs an idle user and no competing GPU/emulator/game work; runs without background QoS. |
| guard (daemon) | `corral guard`: the one per-host process that protects the host. |
| check | One admission test (memory, idle, AC, lock, TCC, ...) returning `{ok, reason, value, since}`. |
| profile | A named set of checks a job requires: `default`, `measurement`, `gui` (built in), or custom. |
| admission | Deciding a job may start now. Twice: before claim, and again while holding the host lock. |
| matcher | A guard rule selecting processes (by registration, cmdline, cwd, cgroup, ...) and assigning a tier, budget and kill method. |
| tier | Kill order under pressure; lower tiers die first. Tier 0 = corral jobs by default. |
| protected / never | Protected processes die only at the critical level or by the kill switch; `never` processes are never signalled. |
| kill switch | `corral guard stop`: kill all watched heavy work and refuse admission until `resume`. |
| host lock | A named, leased lock on one host (e.g. `emulator`), file-compatible with the legacy JSON lock format. |
| claim | Worker → scheduler request for work; success creates an attempt and a lease. |
| lease (attempt lease) | A worker's time-limited right to run an attempt, renewed by heartbeat. |
| orphaned | Running attempt whose lease expired; waits `orphan_grace` for the worker to return. |
| lost | Nobody saw the attempt end (worker vanished). May requeue. |
| killed | Something decided to stop the job. Terminal. |
| interactive lease | A held worker slot plus persistent work dir; runs **steps** (jobs with `parent = <lease id>`). |
| hold (keep-awake) | Named, time-bounded keep-awake taken through a keep-awake provider; never touches a user's own session. |
| runtime | Adapter that actually executes a job (`native` in v1). |
| identity | Who connects: `scheduler`, `worker:<host>`, `agent:<host>`, `owner`. |

## 4. Requirements

Each requirement has an ID, a statement, and an acceptance criterion (**AC**). Tests cite IDs (`AGENTS.md`). Detailed
behaviour lives in the companion documents named per group.

### 4.1 Queue and job state (PROTOCOL.md §5)

- **R-QUEUE-01** `corral submit` returns a job id within 1 s when the scheduler is up, whether or not a worker is
  online. **AC:** integration test with no workers: submit returns id in < 1 s; job state `queued`.
- **R-QUEUE-02** The scheduler persists every state transition in SQLite (WAL) before replying. **AC:** kill -9 the
  scheduler after a submit reply; on restart the job exists with the same state.
- **R-QUEUE-03** Job states and transitions are exactly those of PROTOCOL.md §5; any other transition is rejected and
  logged. **AC:** table test over every (state, event) pair.
- **R-QUEUE-04** A job stopped by anything other than its own exit (guard, timeout, kill switch, external signal,
  kernel OOM) ends `killed` with a reason and is never requeued automatically. **AC:** guard-kill test: job ends
  `killed:ram_exceeded`; queue stays empty; attempts = 1.
- **R-QUEUE-05** A running attempt whose worker is silent becomes `orphaned` after `lease_ttl` (default 90 s) and
  `lost` after `orphan_grace` (default 15 min); a lost job is requeued only if `retry_on_lost` and
  `attempts < max_attempts` (default 2). **AC:** SIGSTOP the worker 120 s → `orphaned`; SIGCONT → `running`; with
  a fake clock past grace → `queued`, attempt 2.
- **R-QUEUE-06** A worker returning after its attempt was declared lost is told `stale_lease` and kills that attempt;
  its late logs and events are discarded. **AC:** test publishes with the old token after requeue; scheduler rejects
  it; worker kills the process group.
- **R-QUEUE-07** A job with `not_after` that has not started by then ends `expired`. **AC:** fake clock test.
- **R-QUEUE-08** `corral resubmit` of a `killed` job requires the `owner` identity (later: an approval grant).
  **AC:** `agent:*` gets `forbidden`; `owner` gets a new id with `retry_of`.

### 4.2 Scheduling and placement (§7)

- **R-SCHED-01** A worker is offered a job only if `job.caps ⊆ worker.caps`, `job.ram_mb ≤ admissible_ram_mb`, the
  class/exclusive slot rules hold, and the worker's summary says the job's profile passes. **AC:** table test of
  claim selection over caps, RAM, class, exclusive, profile.
- **R-SCHED-02** A job with `prefer = W` is offered to other workers only after `fallback_after` since it was queued;
  `fallback_after = never` pins it. **AC:** fake clock: a second worker gets nothing before, the job after.
- **R-SCHED-03** Order: lease steps first, then priority descending, then FIFO; jobs that do not fit do not block
  smaller ones behind them (backfill). **AC:** ordering and backfill table test.
- **R-SCHED-04** Placement is local-first: owned workers (cost tier 0) always rank before any paid runtime; paid tiers
  are phase 2 and are never used without an explicit per-job or per-queue allowance and a budget cap. **AC:** v1 has
  no paid tiers; a config with a non-zero tier worker is rejected unless `phase2.cloud = true` (feature gate).
- **R-SCHED-05** At most `heavy_slots` (default 1) heavy jobs run per host, counting corral jobs and every live heavy
  process the guard sees. **AC:** with a matched non-corral heavy process alive, the worker's summary reports
  `heavy_ok=false` with that process named; no claim.
- **R-SCHED-05a** (proposed, Q14) A worker on a host with a single spinning disk may declare `io_heavy` jobs and an
  `io_slots` cap (default: equal to `heavy_slots`), so that IO-bound jobs (large git, search or install trees) do not
  run in parallel and starve interactive sessions that share the disk. **AC:** with `io_slots = 1` and one `io_heavy`
  job running, a second `io_heavy` job is not claimed (`declines_total{reason="io_slots"}`); a non-`io_heavy` light job
  is.
- **R-SCHED-06** Each claim is a single SQLite `BEGIN IMMEDIATE` transaction; two concurrent claims never receive the
  same job. **AC:** 50 parallel claims against 10 jobs: 10 distinct winners.

### 4.3 Admission and explainable waits (GUARDS.md §4, §7)

- **R-ADM-01** A heavy job starts only after (1) guard admission passes for its declared RAM and profile, (2) the
  required host lock is acquired, and (3) the full profile passes **again while holding the lock**. **AC:** test
  flips a check to failing between lock acquire and re-check: job declined, lock released, state `queued`.
- **R-ADM-02** Memory admission uses a projection: free memory after the job's declared RAM stays at or above the
  host's `admit_floor` (default: ceiling + 3 points). **AC:** fake probes at the boundary admit/refuse exactly.
- **R-ADM-03** A decline never counts as an attempt and always updates the job's waiting reasons. **AC:** after a
  decline: attempts unchanged; `corral status` shows the decline reason.
- **R-ADM-04** Every queued job carries a waiting explanation per candidate worker (first failing rule plus other
  failing checks), updated within 30 s of a change. **AC:** lock the (fake) screen; within 30 s status shows
  `laptop: blocked: screen_unlocked (locked since ...)`.
- **R-ADM-05** A job blocked by a host-local check for longer than `notify_after` (default 30 min) sends exactly one
  notification per (job, reason). **AC:** fake clock + fake notifier: one call.
- **R-ADM-06** `corral admit` is usable by non-corral tools: exit 0 = admitted (with an optional reservation), 1 =
  refused with a one-line reason, 2 = usage, 5 = guard unreachable. **AC:** CLI tests for each code.
- **R-ADM-08** (draft, round 2) **Light and heavy admission classes.** Light work (class `light`, exec sessions of
  class light) needs only `kill_switch_off`, the memory projection under the host's ceiling and a free light slot;
  it runs while the host's user is active and on battery. Heavy work needs the job's full profile (`default`:
  memory, heavy slot, AC power on laptops, thermal, disk; `measurement` adds the idle stages). **AC:** with a fake
  active user on battery: a light exec is admitted; a heavy exec is refused with `ac_power`.
- **R-ADM-07** Two-stage idle for `measurement`: stage 1 (≥ 300 s) gates the claim; stage 2 (≥ 120 s) is re-checked
  under the lock before each run; the host's own synthetic input does not reset idle. **AC:** fake idle tracker
  tests including a synthetic-input stamp.

### 4.4 Guard: system-wide protection (GUARDS.md §2-3)

- **R-GUARD-01** One `corral guard` per host owns all kill decisions for memory and thermal protection, for corral
  jobs and for every process selected by configured matchers. **AC:** a second guard instance on the same host exits
  with "already running" (pid-file flock).
- **R-GUARD-02** Under pressure the guard kills by tier (lowest first), one tier at a time, re-sampling after a
  settle period, and stops as soon as the level clears. **AC:** fake samples: tier 0 killed, level clears, tier 1
  untouched.
- **R-GUARD-03** `never` processes are never signalled; `protected` processes are signalled only at the critical
  level or by the kill switch; if the protected set cannot be resolved, the shed level kills nothing. **AC:** tests
  for each rule including an unresolvable protected matcher.
- **R-GUARD-04** The guard only signals processes it resolved in the current sample and re-verifies pid start time
  before each signal; it never kills by name. **AC:** pid-reuse test: a process with a new start time is not signalled.
- **R-GUARD-05** Kill switch: `corral guard stop` kills all non-`never` watched work, runs restore hooks, and refuses
  all admission until `corral guard resume`; the state survives guard restarts. **AC:** stop → restart guard →
  `corral admit` still refused.
- **R-GUARD-06** Per-process budgets: a registered or matched process tree exceeding `budget × 1.1` (configurable) is
  killed with reason `ram_exceeded`. **AC:** allocator test exceeding a 50 MB budget is killed; job `killed`.
- **R-GUARD-07** Restore hooks run after `stop`, `critical` and (if configured) `shed`, with a timeout, and their
  outcome is logged. **AC:** fake hook records invocation; a hanging hook is killed at its timeout.
- **R-GUARD-08** Every guard action produces one structured log line, one metric increment and (per notify policy)
  one notification; when a corral job is killed the worker learns the exact reason from the guard (no log scraping).
  **AC:** guard-kill test: job's `finished.reason` = guard's reason.
- **R-GUARD-09** On macOS the guard keeps the host under its memory ceiling (default 80% used) for corral-caused load:
  it acts within 2 samples (≤ 4 s) of crossing. **AC:** fake probe sequence; action at the second sample.
- **R-GUARD-10** `observe` mode logs every decision it would take and signals nothing (used during migration).
  **AC:** observe test: decisions logged, no signals sent.

### 4.5 Host locks (GUARDS.md §5)

- **R-LOCK-01** Host locks use the legacy JSON lease-file format (`agent, purpose, apps, pid, started, heartbeat,
  ttl_s`), acquire by `O_EXCL` create, renew by tmp + rename, holder-only release, stale after `ttl_s`, takeover via
  an `O_EXCL` `.takeover` file and one log line. **AC:** conformance suite run against the reference implementation
  and corral on one temp file: never two holders; stale takeover; holder-only release; takeover logged.
- **R-LOCK-02** corral never takes over a **live** lock, and takes over a stale lock only if its holder was a corral
  attempt or the lock is configured `takeover = "any"`. **AC:** stale non-corral holder → decline with holder
  description and one notification.
- **R-LOCK-03** A lock held by a corral attempt is renewed every `ttl_s / 3` while the attempt lives and released on
  every exit path. **AC:** kill -9 the worker → lock stale after ttl → takeover by next corral attempt.
- **R-LOCK-04** A child of a lock holder that presents the holder's `CORRAL_LOCK_TOKEN` re-enters without blocking.
  **AC:** nested `corral lock exec` inside a holder succeeds.

### 4.6 Execution (RUNTIMES.md §2)

- **R-EXEC-01** Every job runs in its own process group; termination is TERM to the group, then KILL after
  `kill_grace` (default 10 s); corral never signals processes it did not start or match. **AC:** a job that spawns
  children: all gone after cancel; an unrelated process with a similar name survives.
- **R-EXEC-02** `timeout` is enforced by the worker and backstopped by the scheduler (`killed:timeout` if the worker
  is silent past `timeout + orphan_grace`). **AC:** `sleep 60` with timeout 2 s ends `killed:timeout`.
- **R-EXEC-03** On Linux each attempt runs in its own cgroup with `memory.high = ram`, `memory.max = 1.1 × ram`;
  where delegation is unavailable the macOS-style watchdog applies. **AC:** integration test on Linux CI reads the
  cgroup files.
- **R-EXEC-04** Non-exclusive jobs run at background priority (macOS `taskpolicy -b`, Linux `nice 10` + `ionice
  idle` + `cpu.weight 50`); exclusive jobs do not. **AC:** process QoS inspected in test.
- **R-EXEC-05** Jobs receive a clean environment (configured `PATH`, `HOME`, `LANG`, `TMPDIR`, `CORRAL_*`, the job's
  `env`); submitter secrets are never forwarded. **AC:** env dump test.
- **R-EXEC-06** A job's exit code in `pause_on_exit` pauses the worker with the reason (legacy "exit 4/5 pauses the
  queue" semantics). **AC:** job exits 4 → worker paused; status shows reason.

### 4.7 Input, output, logs (PROTOCOL.md §8-10)

- **R-IO-01** Input is a git ref (resolved to a sha at submit), a snapshot of the submitter's worktree (tracked +
  untracked-not-ignored, including uncommitted changes), or none. **AC:** snapshot round trip preserves a dirty file
  and excludes an ignored one.
- **R-IO-02** Snapshots refuse files matching the secret deny-list unless `--allow-secret-files`, and are capped at
  `snapshot_max_mb`. **AC:** `.env` present → submit error naming it.
- **R-IO-03** Logs are delivered at least once, in order per stream, de-duplicated by sequence number; nothing is
  lost across a worker disconnect up to `log_spool_max` (512 MiB per attempt). **AC:** NATS restart mid-job; archive
  byte-identical to the local file.
- **R-IO-04** `corral wait` and `corral logs -f` show output within 2 s while connected and `wait` exits with the
  job's exit code (125 for killed, lost, cancelled, expired). **AC:** `echo hi; exit 3` → prints `hi`, exits 3.
- **R-IO-05** Artifacts and snapshots move over HTTP with per-job caps; over-cap uploads get 413 and the job record
  says so; retention is age plus total size. **AC:** over-cap test; GC test with fake clock.
- **R-IO-06** Jobs report progress by printing `##corral progress=<0-100> <text>`; no helper binary is needed.
  **AC:** progress events received by a follower.

### 4.8 Interactive leases (PROTOCOL.md §11)

- **R-LEASE-01** `corral lease open` returns a lease once a worker admits it; `lease exec` syncs changed files and
  runs a step in the persistent directory; `lease close` or the idle timeout frees the slot and the directory.
  **AC:** open → exec twice (second sees first's files) → close → slot free.
- **R-LEASE-02** A guard kill of any lease process ends the **lease** `killed`. **AC:** guard-kill test on a step.

### 4.9 Keep-awake (GUARDS.md §8)

- **R-AWAKE-01** While a job or lease runs on a host with a keep-awake provider, a named hold (`corral:<id>`) is held
  and renewed; it is released on every exit path. **AC:** fake provider records hold/renew/release.
- **R-AWAKE-02** corral never ends, changes or signals a keep-awake session it did not create, never kills a process
  by name, and signals only pids it recorded itself with a matching start time. **AC:** static check (no
  `pkill`/`killall` in code); test with a foreign keep-awake process that survives.
- **R-AWAKE-03** If a host requires keep-awake for heavy jobs and no provider works, heavy jobs are refused with the
  reason `keepawake_unavailable`. **AC:** fake failing provider → waiting reason.

### 4.10 Security and identity (PROTOCOL.md §3, §12)

- **R-SEC-01** Every NATS connection authenticates (nkey); anonymous connections are refused. **AC:** anonymous
  connect fails.
- **R-SEC-02** Each identity may publish and subscribe only on its own subjects. Clients hold no JetStream API
  permission; the scheduler replies only into the caller's own inbox prefix. **AC:** a test tries every forbidden
  publish/subscribe per identity (including consumer create/delete and purge); all fail; an agent's job output cannot
  steer a worker, and a hostile reply subject gets no answer.
- **R-SEC-03** The scheduler authorizes by the identity pinned in the subject; a payload `from` that disagrees is
  rejected. **AC:** mismatched `from` → `forbidden`.
- **R-SEC-04** Every request schema has an optional `authz` field from v1; enabling approvals for an action changes
  scheduler policy, not schemas. **AC:** policy `require_grant = ["exclusive"]` → `grant_required` error with the
  same schema.
- **R-SEC-05** HTTP requests carry a capability token scoped to one job, one verb, and an expiry. **AC:** a token for
  job A cannot read job B; expired token → 401.
- **R-SEC-07** (draft, round 2) **External tokens at the auth plug-in point.** When `auth.external` is configured, the
  scheduler (HTTP) and the NATS auth callout accept access tokens from a configured OIDC issuer (issuer URL, audience,
  JWKS), including **DPoP-bound** tokens (RFC 9449: the `cnf.jkt` thumbprint must match a valid DPoP proof). The
  token's subject maps to an identity slug and the same permission templates as nkey identities; approval grants
  (CIBA) arrive in `authz` as before. nkeys keep working (`auth_users` bypass). **AC:** a valid token with a matching
  DPoP proof is accepted; a replayed proof (same `jti`), a proof signed by another key, a wrong audience and an expired
  token are each rejected with `forbidden`.
- **Known limit (M1 review).** A job runs as the worker's OS user and can read the worker's nkey seed (PROTOCOL.md
  12.1, "Known limits"); a separate unprivileged job user or a container/VM runtime is future work.
- **R-SEC-06** NATS and HTTP listen only on configured addresses (default loopback + the private overlay address).
  **AC:** config validation rejects `0.0.0.0` unless `allow_public_bind = true`.

### 4.11 Operations (§10-12)

- **R-OPS-01** Restarting the worker, scheduler, guard or NATS never duplicates a running attempt. **AC:** chaos
  test for each restart.
- **R-OPS-02** The scheduler exposes Prometheus metrics and `/healthz`; guards and workers expose local metrics on a
  Unix socket or loopback port. **AC:** scrape test.
- **R-OPS-03** After a scheduler outage, lease expiry is evaluated only after leases are extended by the downtime
  (no mass orphaning). **AC:** stop scheduler 5 min; restart; no job `orphaned`.
- **R-OPS-04** One configuration file per host, validated at start with errors naming the key; unknown keys are
  errors. **AC:** config tests.

### 4.12 Runtimes (RUNTIMES.md)

- **R-RT-01** All execution goes through the `Runtime` trait; v1 ships only `native`. **AC:** worker builds with
  the trait and one implementation; a fake runtime drives the worker in tests.
- **R-RT-02** A runtime reports caps and a cost tier; tier 0 = owned hardware. **AC:** native reports tier 0.

### 4.13 Migration (GUARDS.md §10)

- **R-MIG-01** Shims for the legacy guard scripts keep their CLI (`watch|stop|resume|admit [name]|status`), output
  shape (`ok` / `refused: ...`) and exit codes. **AC:** golden tests of the shim against recorded legacy outputs.
- **R-MIG-02** Every migration step has a documented one-command rollback that restores the legacy behaviour.
  **AC:** rollback dry run in the migration test checklist.

### 4.14 Remote exec (draft, round 2; PROTOCOL.md §11.1)

- **R-REXEC-01** `corral exec --on W -- CMD` runs one command on worker W **without queueing**: if W does not admit it
  within `exec_wait` (default 10 s) the CLI exits 75 with the waiting reason; it never falls back to running locally.
  **AC:** W offline → exit 75 within `exec_wait` + 1 s and the reason names W.
- **R-REXEC-02** The CLI keeps one exec session per (caller session, repository root) open while used (`idle` default
  20 min) over a persistent connection; a command's overhead without file changes is ≤ 300 ms on a LAN-class overlay
  [target, to be measured]. **AC:** 20 sequential `exec -- true` calls reuse one session; p50 overhead recorded.
- **R-REXEC-03** **Dirty-tree sync:** before each command the CLI sends the delta of tracked plus
  untracked-not-ignored files against the session's last manifest (deny-list of R-IO-02 applies); a missing base
  commit is fetched by the worker from a configured git remote. **AC:** an edited tracked file and a new untracked
  file are visible remotely; an ignored `.env` is not sent.
- **R-REXEC-04** **Same-path mode** (`workdir = "mirror"`, allowed roots configured on the worker): the command runs
  at the same absolute path as on the caller. The worker refuses with `dirty_foreign` when that checkout has changes
  not written by an earlier sync of an exec session, naming the files; it never overwrites them. `workdir = "scratch"`
  uses a worker-managed directory instead. **AC:** foreign change present → refusal names it; file unchanged.
- **R-REXEC-05** **Outputs back:** stdout/stderr stream live and the CLI exits with the command's code (125 when
  killed or lost); changes the command made to tracked or untracked-not-ignored files return as a patch applied on the
  caller only if it applies cleanly (else saved and reported); ignored paths return only with `--pull PATH`, capped.
  **AC:** lockfile change applied; conflicting local edit → patch kept as a file and reported; `--pull` path present.
- **R-REXEC-06** An exec is an attempt like any other: guard kills are final, a lost exec never re-runs on its own,
  and every exec appears in `corral status`. **AC:** guard kill during exec → exit 125 with the reason; no re-run.

### 4.15 Agent jobs (draft, round 2; PROTOCOL.md §4.1)

- **R-AGENT-01** `corral agent submit --on W --repo PATH --base REF --branch NAME --brief FILE [--provider P]
  [--model M] [--deadline D] [--fallback wait|local]` creates a job of kind `agent`. The worker creates a git worktree
  of the repository at the base ref (fetched from the configured exchange remote) and starts the agent through the
  configured **agent runner**: a command template (placeholders `{workdir}`, `{brief}`, `{result}`, `{provider}`,
  `{model}`) in worker config; corral has no agent-specific adapter (Q18). **Runner contract:** the command stays in
  the foreground until the agent has finished, so the guard, cancel and memory accounting apply to its process group;
  it exits 0 on success; it writes a JSON result file at `{result}` (`status`, `summary`, optional `branch`). A runner
  whose agent outlives it violates the contract; a site-specific wrapper script (outside corral) adapts tools that
  detach. **AC:** with a fake runner, the worktree exists at the base sha, the runner received the brief, and a runner
  that exits without writing `{result}` yields `agent_result.status = "no_result"`.
- **R-AGENT-02** On completion the worker commits nothing itself, pushes the agent's branch to the exchange remote and
  reports `agent_result`: branch, head sha, summary from the result file (capped 4 KiB), diffstat, runner exit
  status; the result file is kept as an artifact. Transcripts stay on
  the worker's host. **AC:** result fields present; branch fetchable by the submitter.
- **R-AGENT-03** Agent jobs use a named slot `agent` (default 6 per host) and are admitted as light work plus a
  per-agent memory reservation (`agent_ram`, default 256 MiB); commands the agent runs take heavy slots under normal
  admission. **AC:** cap + 1 submits → one `queued` with `agent slots full`.
- **R-AGENT-04** Waiting is explained like any job; at `deadline`, `fallback = wait` notifies and keeps waiting,
  `fallback = local` runs the agent on the submitter's host. **AC:** fake clock past deadline → one notification, or a
  local start.
- **R-AGENT-05** Cancel sends TERM to the runner's process group, then KILL after the grace period; the worktree is removed unless
  `--keep`. **AC:** cancel mid-run → runner group terminated; job `cancelled`.

### 4.16 Command placement: classifier and hook mode (draft, round 2)

- **R-PLACE-01** `corral classify [--cwd DIR] [--json] -- STRING` splits a shell command into segments (on `;`, `&&`,
  `||`, `|`, newlines and `(`; backslash-newline joins; unquoted `#` comments dropped; leading `VAR=x`, `sudo`, `env`,
  `command`, `time`, `nohup` skipped; `bash|sh|zsh -c STR` parsed; heredoc bodies and quoted text never match; `cd DIR`
  updates the effective cwd of later segments; `ssh HOST CMD` payloads classified too) and assigns each segment a class
  from configured rules: `local_only`, `local_ok`, or `remote` with a **heaviness level** `weight` 1-3 (1 light, 2
  medium, 3 heavy). A segment no rule matches is `remote` at `default_weight` (default 1) and is marked `unknown` in
  the log. A command's weight is the maximum over its segments. **AC:** golden corpus test including `cd x && npm test
  | tail` (weight 3), a heredoc mentioning a test runner (not remote), `bash -lc "cargo build"` (weight 3), an
  unmatched command (weight = `default_weight`, logged unknown).
- **R-PLACE-02** Combination: the command stays local only if every segment is `local_ok` or `local_only`; a command
  mixing `local_only` with `remote` segments is denied with a reason naming both (it cannot be split).
  **AC:** table test.
- **R-PLACE-03** **Hook mode** (`corral hook <harness>`): reads the harness's pre-tool event, classifies, and answers
  in the harness's format: run locally; **rewrite** the command to `corral exec --on W ...` (recommended mode);
  **deny** with a reason (alternative mode); or, when W is offline: a command whose weight is at or above
  `wait_threshold` (default 2) is **queued** as a job for W and the call returns at once with exit 75 and the reason;
  a command below it runs locally under guard (`corral local --guard`: timeout, low CPU/IO weight, memory cap).
  **AC (thresholds):** offline W, weight 1 → guarded local; weight 2 and 3 → queued; changing `wait_threshold` to 3
  moves weight 2 to guarded local. Every decision is logged. An `observe` mode logs the decision and changes nothing.
  Hook mode is the **only** rewriting hook: other rewriters are chained inside it (configured command list, each fed
  the previous output), because harnesses may run hooks in parallel and keep only one rewrite (Q17). It returns a
  rewrite without overriding the harness's permission decision unless configured to.
  **AC:** for each decision a fixture event produces the expected harness output; observe mode never changes input;
  a chained rewriter's output is the input to placement.
- **R-PLACE-04** Presence for hook decisions is read from a local cache (`corral workers --watch`), so a decision
  makes no network call; p95 decision time < 50 ms. **AC:** benchmark over the golden corpus.
- **R-PLACE-05** Escape hatch: a configured marker comment (default `# @local`) or `corral local --reason TEXT -- CMD`
  runs locally (guard limits without the timeout) and is logged with the reason. **AC:** marker honoured and logged.
- **R-PLACE-06** The rules file is data; corral never edits it. `corral classify --propose` reads the decision log and
  writes a proposal diff (patterns that ran locally under guard, fast and small, proposed as `local_ok`; patterns
  killed by the guard timeout, proposed one weight higher); applying it is the owner's act.
  **AC:** proposal written; rules file unchanged.
- **R-PLACE-07** A per-project profile (`light`, `build-heavy`, `media-device`) may be supplied by an external
  provider (a file or command) and selects rule sets; corral does not store profiles. **AC:** profile switches a rule
  set in a fixture.

## 5. Architecture

```
       you / AI agents                       server (always on)                            workers
  ┌───────────────────┐  NATS req/reply  ┌─────────────────────────────────┐  claim · hb   ┌──────────────────────┐
  │ corral CLI        │ ───────────────▶ │ nats-server (JetStream)         │ ◀──────────── │ corral worker        │
  │ submit wait lease │ ◀─ events/logs ─ │   accounts CORRAL, SYS; nkeys   │ ── ctl/wake ─▶│  admission, runtime  │
  │ admit run lock    │                  │ corral scheduler                │ ◀─ logs ───── │  log spool, hold     │
  └───────────────────┘                  │   SQLite (WAL), placement       │               │      │ unix socket   │
            │                            │   leases, waiting reasons       │ ◀── HTTP ──── │      ▼               │
            └──── HTTP (artifacts) ────▶ │   HTTP: inputs/artifacts/logs   │   artifacts   │ corral guard         │
                                         │ corral worker + corral guard    │               │  matchers, tiers,    │
                                         └─────────────────────────────────┘               │  locks, keep-awake   │
                                                                                           └──────────────────────┘
                                         laptop (sleeps), gpu-box (sometimes on): worker + guard
```

Principles:
- **Everything dials into `server`.** Workers are behind NAT, sleep and roam; they only connect out.
- **The scheduler is the only writer of job state.** NATS carries messages; JetStream stores logs, events and
  presence; it does not schedule (§6.3).
- **The guard is the only killer for resource protection on a host.** The worker owns job semantics (cancel,
  timeout); the guard owns "this host is in danger" (memory, thermal, kill switch, budgets). They talk over a local
  Unix socket (GUARDS.md §6). A host can run the guard without a worker (it then protects heavy work started by other
  tools).
- **Bulk bytes over HTTP, control over NATS.**
- Private overlay network (e.g. a WireGuard mesh such as `example.ts.net`) provides transport encryption in v1; NATS
  TLS is optional.

Planned crate layout (supersedes the sketch in `AGENTS.md` once approved): `corral-proto` (schemas, subjects,
versioning), `corral-sched`, `corral-worker`, `corral-guard` (probes, matchers, locks, keep-awake providers),
`corral-runtime` (trait + native), `corral-cli`, and the `corral` binary crate that links them.

Resource budgets (targets, verified during implementation): nats-server ≤ 256 MiB (MemoryMax), scheduler ≤ 128 MiB,
worker and guard each ≤ 20 MiB idle RSS.

## 6. Justification of choices

### 6.1 Language: Rust vs Go vs Python

| Criterion | Rust | Go | Python |
|---|---|---|---|
| Idle memory of always-on daemons on the smallest host (a laptop) | a few MB | ~10-20 MB plus GC headroom | 20-40 MB; system interpreter versions vary per OS |
| Process groups, signals, cgroups, `proc_pid_rusage`, `setpgid` | `rustix`/`nix`, `tokio::process` with `process_group(0)`, `libc` | good (`syscall`) | possible; signal handling only on the main thread; error-prone in long-lived async daemons |
| NATS client | `async-nats`: request/reply, reconnect, JetStream consumers, KV, nkey/JWT/callback auth | `nats.go` (reference client) | `nats-py` (fine; needs runtime) |
| Distribution | one static binary per target; Nix `buildRustPackage` | one static binary | interpreter + dependencies on every host |
| Correctness under concurrency (locks, leases, signal races) | type system and ownership catch a class of bugs at compile time | race detector at test time | runtime only |
| Cost | slower to write, longer compiles | fastest to write of the compiled options | fastest to prototype |

Decision: **Rust** for scheduler, worker, guard and CLI. The guard and worker are always on, run on the
memory-constrained host and do signal and process-tree work: that is where Rust's advantages are largest. Go is an
acceptable alternative; Python is rejected for daemons. Existing Python and shell tools are integrated **by protocol**
(lock-file format, CLI shims), not by import, and conformance-tested against their reference implementations.

### 6.2 Transport: NATS vs HTTP/1.1 long-poll vs SSE vs WebSockets vs gRPC

Needs: intermittent workers behind NAT that dial out; the server must push cancels and "work available"; many clients
follow live logs; identities on every request; many small messages and a few large files.

| Option | Dial-out worker + server push | Reconnect / resubscribe | Log fan-out to many followers | Durable replay | Auth now + external issuer later | Code we write |
|---|---|---|---|---|---|---|
| HTTP/1.1 long-poll | yes; push = held requests; cancel latency = poll period | ours | ours (offsets) | ours (files) | bearer JWT | most |
| SSE | server → client only; worker still POSTs | ours (Last-Event-ID helps) | ours | ours | bearer JWT | much |
| WebSockets | bidirectional | ours (framing, heartbeats, resume) | ours | ours | bearer at upgrade | much |
| gRPC bidi streams | bidirectional | ours (stream re-establish) | ours | ours | per-call metadata | much, plus protobuf toolchain |
| **NATS + JetStream** | bidirectional over one dial-out TCP connection | **built in** | **built in** (subjects) | **built in** (streams, consumers, dedupe) | nkeys/accounts now; **auth callout** for an external issuer (server 2.10+) | least |

What NATS costs: one extra daemon on `server` (a single small Go binary), a subject and permission design, and the
rule that large files do not travel through it (JetStream's object store is not used for GB artifacts). HTTP is the
right tool for bulk bytes (streaming PUT/GET, `Content-Length`, Range later).

Decision: **NATS for control, logs, progress, presence and wake-ups; HTTP for inputs, artifacts and the log archive.**

### 6.3 Why the scheduler schedules (not a JetStream work queue)

- A work-queue stream hands the next message to whichever consumer pulls; it cannot select by declared RAM against a
  worker's live admissible RAM, caps, prefer-then-fallback timing, exclusivity, or priority with backfill.
- Work-queue streams do not allow overlapping consumer filters, so per-worker filtered views of one queue are not
  possible [unverified in a single primary doc; observed in NATS examples].
- Status, waiting reasons and history want a table, not a stream.

So the scheduler owns state in SQLite; claims are core NATS request/reply; JetStream is used where its semantics fit:
append-only logs and events (limits retention) and presence (KV with max age).

### 6.4 Push vs pull vs poll

| Flow | Mechanism | Why |
|---|---|---|
| Work → worker | **pull** (`claim` when local admission says something could run) plus a **push wake-up** when a job it might take is queued | Admission depends on fast-changing local state only the worker sees; pushing to an absent worker would force presence tracking anyway. |
| Cancel → worker | **push** on the worker's control subject, plus the cancel flag in every heartbeat reply | Push for latency; heartbeat covers a lost push (≤ 10 s). |
| Attempt lease | worker heartbeats every 10 s (request/reply) | Fencing needs a reply. |
| Logs, progress | worker publishes to JetStream; followers subscribe (ordered consumers) | No client polling; late followers replay. |
| Job state → clients | scheduler publishes events; `wait` subscribes | No polling. |
| Presence | worker puts to KV every 15 s; watchers see expiry | Offline is visible without probes. |
| Idle worker | claim retry at most every 60 s when admission is ok and no wake-up arrived | The only poll; covers lost wake-ups. |
| Guard sampling | local sampling every 2 s (5 s for non-memory signals) | OS memory and thermal state are only available by sampling; event APIs (macOS memory-pressure dispatch source, Linux PSI triggers) are used as accelerators where available. |

### 6.5 Why native processes only in v1

Containers on macOS need a Linux VM (GBs of RAM and constant CPU) and cannot use Metal or TCC grants, which are the
point of the laptop worker. On a small Linux server a container daemon competes with the workloads corral protects.
Native processes plus process groups, cgroups (Linux) and the guard's watchdog give the protection that matters
(memory and thermal) at near-zero overhead. Isolation is explicitly a non-goal (N3).

## 7. Scheduling and placement

Per claim (one `BEGIN IMMEDIATE` transaction):
1. Candidates: lease steps addressed to this worker's open leases; then `queued` jobs with `caps ⊆ worker.caps`,
   `ram_mb ≤ admissible_ram_mb`, a free slot of the job's class, `heavy_ok` for heavy jobs, `exclusive_ok` for
   exclusive jobs, the job's profile in the worker's passing set, `prefer` empty or this worker or
   `now − queued_at ≥ fallback_after`, not held (queue or worker paused).
2. Order: lease steps, then `priority DESC, queued_at ASC, id`. First match wins; others backfill.
3. Create attempt, token and lease; reply.

**Local-first ranking.** Each worker has a cost tier (0 = owned). v1 has only tier 0. Phase 2: a job may run on tier
`n > 0` only if `max_cost_tier ≥ n`, `cloud_after` has elapsed with no tier-0 candidate able to take it, and the
budget ledger allows its estimate (RUNTIMES.md §4).

**Defaults the CLI sets:** caps that only one host type has (`metal`, `macos`, `android`, `tcc-*`) imply no fallback;
otherwise `fallback_after = 30m` when `--prefer` is given. `--ram` is required for heavy jobs (no silent default).
`--timeout` defaults to 2 h. Priority 0-100, default 50; > 80 is `owner`-only.

**Starvation.** A large job can be starved by a stream of small heavy jobs. v1 accepts it and reports it in the
waiting text; a reservation rule (a heavy job waiting > 1 h blocks new heavy claims on its preferred worker until it
fits) is listed for later (§13).

**Waiting explanation** (R-ADM-04), recomputed on every presence update and queue change:

```
j-20261122-7f3a2k  queued 14m  heavy 8G  caps macos,metal  prefer laptop
  laptop:  blocked: screen_unlocked (locked since 10:02); also user_idle (idle 40s, need 300s)
  server:  never: lacks caps metal,macos
  gpu-box: offline since 2026-11-21 23:10
```

## 8. CLI

```
corral submit [--repo URL --ref REF | --snapshot | --no-input] [--caps a,b] [--prefer HOST] [--fallback-after 30m|never]
              [--ram 8G] [--light] [--exclusive] [--profile default|measurement|gui|NAME] [--lock NAME|--no-lock]
              [--timeout 2h] [--priority N] [--not-after T] [--artifacts PATH]... [--keep] [--notify never|on_fail|always]
              [--pause-on-exit 4,5] [--label k=v]... [--env K=V]... [--wait] -- CMD...
corral wait ID              # live logs to stdout/stderr, progress to stderr; exit = job's exit code (125 otherwise)
corral logs [-f] [--attempt N] ID
corral status [ID] [--all] [--json]          # includes waiting reasons
corral cancel ID | corral resubmit ID
corral artifacts ID [-o DIR]
corral workers [--json]                      # presence, checks, running jobs, admissible RAM
corral lease open|exec|close|ls ...          # PROTOCOL.md §11
corral admit [--name N] [--ram 4G] [--profile P] [--reserve 60s] [--json]     # GUARDS.md §4 (local, any tool)
corral run [--name N] [--ram 4G] [--profile P] [--lock NAME] [--tier N] [--hold] -- CMD...   # admit+lock+register+exec
corral lock acquire|renew|release|status|wait|exec NAME ...                     # GUARDS.md §5
corral guard watch|stop|resume|status [--json]|explain                          # GUARDS.md §3
corral admin pause|resume [--worker W] | drain W | kill-switch on|off            # owner only
corral exec --on W [--cwd DIR] [--class light|heavy] [--pull PATH]... [--no-sync] -- CMD...   # §4.14, no queue
corral agent submit|status|wait|cancel ...   # §4.15 agent jobs
corral classify [--cwd DIR] [--json] [--propose] -- STRING                       # §4.16
corral hook claude-pretooluse|plugin-permission [--mode observe|rewrite|deny]   # §4.16, reads the event on stdin
corral local [--guard] [--reason TEXT] -- CMD...                                 # run here, guarded and logged
corral scheduler | corral worker             # daemons
corral config check [FILE]
```

Exit codes (stable, R-OPS): `0` ok; job's own code for `wait`; `1` refused (admit, lock busy); `2` usage; `3` not
found; `4` auth denied; `5` scheduler or guard unreachable; `75` not admitted now / queued for later (`exec`, hook queue
decisions; the reason is printed); `125` job ended killed/lost/cancelled/expired. `submit`
never falls back to running locally.

## 9. Configuration

One TOML file per host: `/etc/corral/corral.toml` (system services) or `$XDG_CONFIG_HOME/corral/corral.toml`
(per-user). Sections are optional; a host enables only the roles it runs. `version` is required. Unknown keys are
errors. Durations are strings (`90s`, `15m`); sizes are `4G`/`512M`.

```toml
version = 1
host = "laptop"                       # stable name; bound to the worker identity

[client]
nats_url = "nats://server.example.ts.net:4222"
http_url = "http://server.example.ts.net:4280"
identity = "agent:laptop"
nkey_seed_file = "$XDG_CONFIG_HOME/corral/agent.nk"

[scheduler]                            # only on `server`
data_dir = "/var/lib/corral"
listen = ["127.0.0.1", "192.0.2.10"]    # example overlay address (documentation range)
lease_ttl = "90s"
orphan_grace = "15m"
max_attempts = 2
retention = { age = "14d", cap = "30G", keep_flagged = "90d" }
policy.require_grant = []              # e.g. ["exclusive", "resubmit_killed", "admin"]

[worker]
caps = ["macos", "arm64", "metal", "gpu", "android"]
slots = { heavy = 1, light = 2 }
admissible_ram_cap = "12G"            # never claim more than this, whatever is free
light_ram_default = "512M"            # RAM a light job without ram_mb is admitted and watched against
work_dir = "$HOME/Library/Application Support/corral"
runtime = "native"
keepawake_required_for = ["heavy"]

[guard]
mode = "enforce"                       # enforce | observe
sample = "2s"
ceiling_used_pct = 80                  # never let corral-caused load pass this
admit_floor_free_pct = 23              # projection floor for admission
notify = { backend = "ntfy", topic_file = "/run/secrets/corral-ntfy" }
# levels, matchers, protected sets, restore hooks, locks, keep-awake: see GUARDS.md §9

[exec]                                 # worker side (draft, round 2)
workdir = "mirror"                     # mirror | scratch
mirror_roots = ["$HOME/src"]          # same-path mode only below these roots
exec_wait = "10s"
idle = "20m"

[agents]                               # worker side (draft, round 2)
slots = 4
agent_ram = "256M"
runner = ["agent-runner", "--cwd", "{worktree}", "--brief", "{brief}", "--provider", "{provider}", "--model", "{model}"]
exchange_remote = "hub"                # git remote used for base refs and result branches

[placement]                            # caller side, hook mode (draft, round 2)
mode = "observe"                       # observe | rewrite | deny
target = "laptop"
rules = "$XDG_CONFIG_HOME/corral/placement.toml"
local_guard = { timeout = "30s", cpu_weight = 20, io_weight = 20, memory_max = "1G" }
escape_marker = "# @local"

[auth.external]                        # optional, phase 2 plug-in point (R-SEC-07)
issuer = "https://issuer.example.ts.net"
audience = "corral"
require_dpop = true

[profiles.measurement]
checks = ["kill_switch_off", "memory", "thermal_ok", "disk_free", "ac_power", "no_lpm", "screen_unlocked",
          "user_idle", "no_media", "no_dialog", "no_game", "no_foreign_gpu", "lock_free:device"]
user_idle = { stage1 = "300s", stage2 = "120s" }
```

Precedence: built-in defaults < file < `CORRAL_*` environment variables (client only) < flags. `corral config check`
prints the effective configuration with the source of each value.

## 10. Packaging

| Target | Delivery | Units |
|---|---|---|
| NixOS (`server`, Linux workers) | flake with `packages.corral` (`buildRustPackage`, locked deps) and `nixosModules.corral` | `nats.service` (upstream NixOS `services.nats`, JetStream on), `corral-scheduler.service`, `corral-worker.service` (`Delegate=yes` for per-job cgroups), `corral-guard.service` |
| Other Linux (systemd) | static release binaries (x86_64, aarch64) + unit files in `packaging/systemd/` | same units |
| macOS | release binary (aarch64), signed with one stable identity so TCC grants persist (Q7) | `corral worker install` / `corral guard install` write LaunchAgent plists (`RunAtLoad`, `KeepAlive`, program = the binary directly, since TCC attributes to the launchd program) and **do not load them**; the user runs `launchctl bootstrap` once |

NixOS module options (sketch): `services.corral.{enable, package, host, roles = ["scheduler" "worker" "guard"],
settings (TOML as Nix attrs), identities.<name>.{nkey, role}, credentials.*}`. Secrets (nkey seeds, the HTTP HMAC key)
come from systemd credentials (`LoadCredential`), never from the Nix store. Firewall opening is limited to the
overlay interface by an option.

Unit hardening (Linux): scheduler with `ProtectSystem=strict`, `ReadWritePaths=<data_dir>`, `MemoryMax=128M`;
guard runs as root only if it must signal other users' processes or write sysfs in restore hooks (Q4), otherwise as
the job user with `MemoryMax=32M`, `OOMScoreAdjust=-900`.

## 11. Observability

- **Metrics** (scheduler `/metrics`, Prometheus text, loopback + overlay): `corral_jobs{state}`,
  `corral_queue_wait_seconds` (by worker, class), `corral_claims_total{result}`, `corral_declines_total{reason}`,
  `corral_kills_total{reason}`, `corral_lease_expiries_total`, `corral_lost_total`, `corral_worker_up{worker}`,
  `corral_worker_admissible_ram_bytes`, `corral_storage_bytes`, `corral_log_sink_lag_seconds`. Guard (local
  `/metrics` on a Unix socket or loopback): `corral_guard_level`, `corral_guard_free_pct`, `corral_guard_swap_bytes`,
  `corral_guard_temp_celsius`, `corral_guard_actions_total{action,tier,reason}`, `corral_guard_admits_total{result}`.
- **Health:** scheduler `/healthz` (200 = SQLite writable, NATS connected, log sink lag < 60 s); per-worker freshness
  `/v1/workers/<w>/fresh` for uptime checkers. Intermittent workers are never alerted on for being offline; a job
  waiting > 24 h is.
- **Logs:** structured (`tracing`, JSON to journald / a rotated file on macOS, 10 MB × 3). Every line about a job
  carries its id; every guard action carries level, tier, pids, reason and the sample that triggered it.
- **Notifications:** one `Notifier` interface with backends `ntfy`, `command` (run a program with the message),
  `macos` (user notification), `none`. Sent for: job `killed`/`lost`, per-job `notify`, check blocks over
  `notify_after`, guard actions, kill switch on/off, TCC failures, storage over 90% of cap. Rate-limited per
  (kind, subject).
- **`corral guard explain`** prints the last 20 guard decisions with their inputs: the operator's answer to "why did
  my process die".

## 12. Failure modes

| Failure | Detection | Behaviour | Outcome |
|---|---|---|---|
| Laptop sleeps / lid closed mid-job | heartbeats stop | `orphaned`; on wake worker heartbeats with the same token | resumes, or requeues after grace |
| Worker crashes / host reboots mid-job | on restart the worker finds attempt state with a dead process set | reports `finished(lost_local)` | `lost` → requeue per policy |
| Network partition, job still running | heartbeats fail | worker spools logs, keeps running | resumes within grace; else `stale_lease` → worker kills it |
| Scheduler down | claims/heartbeats fail | running jobs continue, logs spool; retry with backoff | on restart, leases extended by the downtime (R-OPS-03) |
| NATS down | client reconnect loop | as above | catch-up on reconnect; dedupe by seq |
| Guard down while a job runs | worker's socket health check fails (5 s) | worker applies its built-in backstop (budget × 1.25, system free ≤ ceiling) and refuses new heavy claims; supervisor restarts guard | guard re-adopts registered jobs from the worker on reconnect |
| Guard down, no job | `corral admit` exit 5 | non-corral tools must treat 5 as "refused" (shim does) | fail closed |
| Job exceeds its RAM budget | guard sample | TERM group, grace, KILL | `killed:ram_exceeded` |
| Host memory pressure from the user's apps | guard level | SIGSTOP pausable work, then tiered kills | `killed:system_memory`; never restarted |
| Thermal critical | guard | SIGSTOP up to 10 min, then kill | `killed:thermal` |
| Kernel OOM / jetsam kills a job | child dies by a signal neither worker nor guard sent | — | `killed:external_signal` |
| Host lock held by another session | lock acquire | `declined` with holder's description | queued with reason |
| Stale lock of a non-corral holder | lock read | decline + notify (no takeover) | owner decides (Q5) |
| Screen locked / user active / on battery | checks | no claim | waiting reason; notify after 30 min |
| TCC grant missing | check | `gui_ok=false`, notify once, other jobs run | gui jobs wait |
| Snapshot too big / secret file | CLI | refused before upload | submit error |
| Artifacts over cap | HTTP 413 | recorded on the job | job finishes, `artifacts: refused` |
| Data dir full | storage check | refuse new uploads, GC, alert | submit error with reason |
| Duplicate worker name | second connection with the same identity | rejected (name bound to nkey) | config error surfaced |
| Clock skew | lease times use the scheduler's clock only | — | none |
| Keep-awake provider missing or broken | provider probe | heavy jobs refused where required | waiting reason |
| Job double-forks out of its group | ppid walk each sample (macOS); cgroup (Linux) | caught by the walk unless reparented between samples | residual risk on macOS; documented |
| Restore hook hangs | hook timeout | hook killed, logged, notified | guard continues |

## 13. Scope: v1 vs phase 2

Nothing in the goals is dropped; items are sequenced. "Phase 2" means after v1 is in use.

| Feature | v1 | Phase 2 / later | Why |
|---|---|---|---|
| NATS + scheduler + worker + CLI, git/snapshot input, logs, progress, events, artifacts | ✓ | | Core |
| Unified guard (matchers, tiers, protected/never, kill switch, restore hooks, notifications, observe mode) | ✓ | | G1, G7 |
| `corral admit` / `corral run` for non-corral tools, legacy guard shims | ✓ | | G2, G7 |
| Host locks (legacy file format, takeover rules, token re-entry) | ✓ | | G2 |
| Checks: memory, thermal, disk, AC, LPM, lock screen, two-stage idle with synthetic-input exemption, media, dialog, game, foreign GPU, TCC | ✓ | more Linux desktop signals (Wayland idle, logind lock) | G3 |
| Keep-awake provider interface; macOS hold provider; Linux `systemd-inhibit` provider | ✓ | | G5 |
| Thermal cooldown before the next exclusive job | ✓ | | legacy queue behaviour |
| `pause_on_exit` codes | ✓ | | legacy queue behaviour |
| Interactive leases, one-way sync + pulls | ✓ (last v1 milestone) | two-way sync | no current loop needs two-way |
| `corral exec` (no-queue exec sessions, same-path mode, sync-back patch) | draft, after M5 (M7) | | G11; reuses lease sync |
| Classifier + hook mode (observe, rewrite, deny, queue, guarded local) | draft (M7) | | G11 |
| Agent jobs (worktree, runner, exchange remote, `agent` slot) | draft (M8) | | G11 |
| nkey identities, per-subject permissions, capability tokens, `authz` field, policy table (empty) | ✓ | | G8 |
| External authorization: NATS auth callout, HTTP JWTs, CIBA approvals, DPoP-bound OIDC tokens (R-SEC-07) | schema + config key | ✓ | issuer not built yet |
| Runtime trait + native runtime | ✓ | | R-RT |
| Cloud runtimes (Modal Sandbox, rented VM over SSH, SkyPilot, lease-a-Mac), cost tiers, budget ledger | interface + tier field | ✓ | owner decision 4 |
| Host pre/post-run hooks (e.g. dim display before a measurement) | | ✓ | domain-specific; tools can use `corral run` meanwhile |
| Adapter for the legacy measurement queue to submit via corral | interop only | ✓ (optional) | legacy queue is domain-rich and works |
| Large-job reservation against starvation; priority aging | | ✓ | few submitters; reported in waiting text |
| Resumable (Range) uploads | | ✓ | overlay network reliable enough |
| NATS TLS | optional | default | overlay already encrypts |
| Web UI | | maybe | CLI + metrics + notifications suffice |
| Container runtime | | not planned | owner decision 3 |

Rollout (each step has exit criteria in the test rubric, still owed):
1. **M1** scheduler + NATS + Linux worker + CLI on `server`; git/snapshot input; logs, events, artifacts.
2. **M2** `corral guard` on every host in **observe** mode next to the legacy guards (GUARDS.md §10).
3. **M3** macOS worker, `default` profile, guard enforcing, legacy guard shims, keep-awake holds.
4. **M4** `measurement` and `gui` profiles, waiting explanations, notifications, legacy-queue interop checks.
5. **M5** interactive leases.
6. **M6** migrate ad-hoc heavy work to `corral submit` / `corral run`; retire legacy watch loops.
7. **M7** (draft) `corral exec`, classifier, hook mode in `observe`, then `rewrite`.
8. **M8** (draft) agent jobs.
7. **Phase 2:** external authorization; cloud runtimes.

Tests: unit tests with fake probes, clock and runtime; lock conformance against the reference implementation;
integration with a real `nats-server` in a temp dir; a separate end-to-end and chaos rubric (owed) covering laptop
sleep/wake mid-job, network loss > grace, server reboot, kill switch during a job, user starting a game during a
measurement job, two sessions racing for a lock, a memory hog started by the user (ceiling must hold), a 1 GB log
flood, and guard restart mid-job.

## 14. Prior art: why not X

Nothing surveyed meets most of the must-haves. HTCondor and Nomad cover queueing and placement; none protects the
person using the desktop and gives agents a blocking, streaming CLI. corral stays thin and borrows ideas, not code.

| System | Covers | Gap for corral |
|---|---|---|
| HTCondor | ClassAd matching and ranking, owner-idle START/SUSPEND/PREEMPT policies, requeue, file transfer, tokens, durable queue | macOS is second-class (Apple Silicon builds unconfirmed [unverified]); heavy configuration; no blocking streaming CLI, kill-tier watchdog, host locks, AC/lock/media/thermal signals, Metal, or keep-awake |
| Nomad | single binary, `raw_exec` on macOS [unverified], durable queue, constraints and affinity | assumes always-connected nodes (a node away for days is marked lost); no user-idle policy, no watchdog; Raft and ACLs to operate; BSL licence |
| SkyPilot | ~20 clouds plus Kubernetes, managed jobs, spot recovery | SSH node pools Linux-only; assumes always-on machines; no Mac. Candidate phase-2 **runtime**, not a replacement |
| dstack | clouds, Kubernetes, SSH fleets, GPU tasks | Linux/Docker only |
| BOINC | idle-only preferences, suspend/resume | volunteer-science platform; ideas only |
| Ray, Slurm, Kubernetes + Karpenter | autoscaling, bursting | always-on Linux clusters, admins, no Metal |
| Modal Sandbox | per-second billing, fast start, GPU, streaming exec | closed source, SDK-driven; candidate phase-2 runtime |
| RunPod, Vast, Lambda | cheap GPU VMs over REST | exec, logs and teardown are yours; start in minutes [unverified]; candidate for the SSH-VM runtime |
| Cloud Mac hosts (e.g. EC2 Mac) | real Metal | dedicated host with a 24 h minimum [unverified]; phase-2 "lease a Mac" only, behind explicit confirmation |
| Temporal, Windmill, Hatchet | durable workflows, platforms | Postgres, own auth and UI; far heavier than needed |
| beanstalkd, Faktory, Redis/Valkey streams | a queue | only the queue part; no admission, presence, leases with fencing |
| pueue | single-host command queue | no multi-host pull model or leases |
| exo, Petals, distcc, Bazel remote execution, GNU parallel | partial overlap (inference sharding, builds, ssh fan-out) | not job queues with admission |
| Agent offload tools and dev sandboxes (E2B, Daytona, Coder, remote-agent wrappers) | run an agent or command on one remote host | no queue, no admission, no Mac/Metal |
| earlyoom, nohang, systemd-oomd | Linux OOM prevention | Linux only; kill by heuristics, no admission, no tiers tied to a job system, no macOS |

What is new in corral: an agent-first blocking contract (stream logs, return the exit code, explain every wait);
protection of the person using the machine on macOS (idle, lock, AC, media, thermal, memory ceiling) re-checked under
a host lock; one guard with kill tiers where killed is final and a vanished worker means requeue; snapshots of a dirty
working tree; keep-awake holds; a queue that tolerates workers offline for days.

## 15. Versioning policy

- **Binaries:** SemVer. Before 1.0, minor versions may break config and CLI with release notes; patch versions never
  do. All corral components on all hosts should run the same minor version; the scheduler accepts workers and clients
  of the same major protocol version and reports skew in `corral workers`.
- **Wire protocol:** every message has integer `v` (PROTOCOL.md §2). Adding optional fields is allowed within a
  version; receivers ignore unknown fields. Removing or changing the meaning of a field bumps `v`. The scheduler
  accepts `v` and `v − 1` for one minor release after a bump. Subject names change only with a `v` bump.
- **Local guard socket protocol:** versioned the same way (`v` field), independent counter.
- **Lock-file format:** frozen as the legacy format while any legacy tool uses it; extensions only as additional JSON
  keys that the reference implementation ignores.
- **Config:** `version = 1`; a new version comes with `corral config migrate`. Deprecated keys warn for one minor
  release before becoming errors.
- **State:** SQLite schema migrations are forward-only, numbered, applied at scheduler start in a transaction; a
  backup copy is written before each migration.
- **Exit codes and `--json` output:** stable within a major version; new JSON fields may be added.

## 16. Open owner questions

- **Q1. Binary layout.** One `corral` binary with subcommands (proposed: simplest to ship and version) vs separate
  `corrald` / `corral-worker` / `corral-guard`.
- **Q2. Guard vs worker split.** Proposed: separate processes (guard survives worker crashes and protects hosts with
  no worker); same binary. Alternative: one process per host (simpler, but a worker crash removes protection).
- **Q3. Default thresholds.** Proposed defaults: macOS ceiling 80% used, admit floor 23% free projected, shed at
  ≤ 20% free, critical at ≤ 15% free or swap ≥ 7 GiB; Linux shed at MemAvailable < 2 GiB or package ≥ 95 C, critical
  at < 768 MiB or ≥ 99 C; admit at ≥ 5 GiB available and < 85 C. Owner to confirm or set per host.
- **Q4. Guard privileges on Linux.** Root (needed to signal other users' processes, `cgroup.kill` on container
  scopes, sysfs restore hooks) vs the job user with restore hooks via a narrow sudo rule. Proposed: root system
  service with `CapabilityBoundingSet=CAP_KILL CAP_SYS_ADMIN CAP_DAC_OVERRIDE` [unverified as minimal].
- **Q5. Stale lock takeover of non-corral holders.** Proposed: never by default (decline + notify); per-lock
  `takeover = "any"` opt-in.
- **Q6. Retry on lost.** Proposed `max_attempts = 2`, `orphan_grace = 15m`, also for exclusive jobs.
- **Q7. macOS signing identity** for stable TCC grants: a developer certificate if available, otherwise a self-signed
  code-signing certificate [unverified that grants persist across updates on current macOS].
- **Q8. AC power for all heavy jobs on laptops.** Proposed yes; light jobs may run on battery.
- **Q9. Who may submit `exclusive` jobs** in v1: proposed all identities (approvals later).
- **Q10. Lease limits:** proposed `ttl` max 8 h, `idle` 20 min.
- **Q11. Notification backend default:** proposed `ntfy` on `server`, `macos` local notifications on laptops.
- **Q12. Crate layout** change vs `AGENTS.md` (adds `corral-guard`, `corral-runtime`): approve to update `AGENTS.md`.
- **Q13. Phase-2 budget model:** monthly cap per owner vs per-job cap only (RUNTIMES.md §4).
- **Q14. IO-heavy concurrency cap:** is an `io_heavy` flag plus `io_slots` (R-SCHED-05a) the right shape, or should it be a
  generic named-resource slot (`slots = { heavy = 1, light = 2, io = 1 }`)? Proposed: generic named slots.
- **Q15. (decided: `mirror` where roots are configured, else `scratch`) Exec workdir default** (R-REXEC-04): same-path `mirror` (commands with absolute paths work; refuses on
  foreign changes) vs `scratch` (never touches the worker user's checkout). Proposed: `mirror` where roots are
  configured, else `scratch`.
- **Q16. (decided: heaviness levels with a wait threshold, R-PLACE-01/03)** Hook decision when the target is
  offline: weight ≥ `wait_threshold` waits for W, below runs locally under guard.
- **Q17. (resolved by test on one harness version: rewrites need no allow decision and stay subject to permission
  rules, matched on the rewritten command; parallel hooks that both rewrite race and one is dropped; so hook mode must
  be the single rewriting entry point and chain other rewriters itself, R-PLACE-03) Harness rewrite semantics**: whether a pre-tool hook's input rewrite must be paired with an
  "allow" decision (bypassing the harness's own permission rules) and how several rewriting hooks combine. Proposed:
  one hook entry point that can chain other rewriters, until verified.
- **Q18. (decided: command template with a foreground + result-file contract, R-AGENT-01) Agent runner contract.**
- **Q19. (decided: HTTP DPoP first in phase 2, NATS profile after review) DPoP over NATS** [unverified as a standard profile]: DPoP proofs bind an HTTP method and URL; for the NATS
  auth callout corral would define `htm = "CONNECT"`, `htu = "nats://<server>/corral"`. Proposed: HTTP DPoP per RFC
  9449 in phase 2 first, NATS profile after review.
- **Q20. (decided: the classifier decides; weight ≥ `heavy_threshold`, default 3, gets heavy admission) Exec class default:** classify from the classifier's segment classes (`heavy` → heavy) (proposed) or always
  light unless `--class heavy`.

## 17. Implementation notes (M1)

Where the approved text was ambiguous or silent, M1 picked these defaults. Each is a candidate for a spec edit if the
owner prefers otherwise.

1. **Timeout backstop on an orphaned attempt.** R-EXEC-02 says the scheduler ends a silent job `killed:timeout` past
   `timeout + orphan_grace`, but the PROTOCOL.md section 5 table only has `claimed, running -> killed`. A silent
   worker's job is `orphaned`, so the table gained `orphaned -> killed` for that one trigger.
2. **Input failure at the worker declines, it does not fail.** A worker that cannot fetch a git input or download a
   snapshot sends `declined` with check `input` and the reason (not an attempt, visible in `corral status`), because
   the cause (network, a mirror not yet fetched) is usually transient. `failed` with reason `input` is used only for
   an upload that never completes (1 h) or fails verification.
3. **`claim_deadline` is refreshed by heartbeats.** A claimed job whose worker is alive and still fetching its input
   is not declined by the claim deadline: a heartbeat for a `claimed` attempt restarts it (otherwise the deadline
   fires at 120 s while a large clone is still running).
4. **Tokens.** The claim reply carries a second token, `input_token` (`input:get`), next to `http_token`
   (`artifacts:put`), because a token has one verb. A new `corral.api.<who>.artifacts` verb returns an
   `artifacts:get` token and a `log:get` token ("minted in NATS replies ... the `artifacts` CLI call", PROTOCOL.md
   section 10; the verb was missing from the subject table).
5. **Log completeness.** `finished` reports the final sequence number of each log stream (`log_seq`), the scheduler
   stores it as `log_final` and repeats it in the terminal job event. `corral wait` ends when it has seen every stream
   to that sequence, with a 5 s grace for logs that expired or never existed.
6. **Presence.** Workers write KV `corral_workers.<host>` by publishing to `$KV.corral_workers.<host>` (no bucket
   lookup, so the permission is exactly the one in PROTOCOL.md 12.1). The scheduler also refreshes presence from every
   claim, so a worker that only claims is still shown. Offline = no update for `presence_ttl` (60 s).
7. **Waiting text is computed on read** from presence and the same eligibility function the claim uses, so it is
   always current (R-ADM-04's 30 s bound holds trivially); declines add the last decline reasons. The `unplaceable`
   warning at submit is not implemented (workers do not report a static RAM cap).
8. **Default input.** `corral submit` with neither `--repo/--ref`, `--snapshot` nor `--no-input` takes a snapshot
   inside a git work tree and no input elsewhere. `--ref` alone uses the `origin` remote; `--repo` alone uses `HEAD`.
   The snapshot cap (`snapshot_max_mb`) applies to the raw size of the files, checked before archiving.
9. **Worker defaults.** Caps always include the platform (`linux`, `macos`) and architecture (`x86_64`, `arm64`).
   Admission floor defaults to 20% of RAM (GUARDS.md 4.1, Linux). Memory already promised to a running job but not yet
   used (`ram - footprint`) counts against new admissions. One host lock per worker (`<work_dir>/locks/heavy.lock`),
   taken by heavy jobs with `lock = auto|NAME`.
10. **Budget watchdog in the worker.** Until the guard exists (M2), the worker kills a job whose footprint exceeds
    `ram * 1.1` with `ram_exceeded` (the guard's R-GUARD-06 number, not the 1.25 "guard down" backstop). The footprint is
    the RSS sum of the job's process group and recorded descendants (`ps`), which over-counts shared pages and so
    errs on the safe side; `phys_footprint` and PSS arrive with the guard.
11. **Priority.** Non-exclusive heavy jobs run under `nice -n 10`. `ionice`, `cpu.weight`, `taskpolicy -b` and the
    per-attempt cgroup (R-EXEC-03, R-EXEC-04) are deferred; the budget watchdog applies everywhere.
12. **Restart recovery (T-Q5).** A restarted worker does not adopt a live process group. It kills everything the
    attempt left behind, whether or not the recorded leader is still alive: members of the recorded group that started
    no earlier than the leader and, on Linux, processes carrying the attempt's `CORRAL_JOB_ID`/`CORRAL_ATTEMPT`
    (TERM, then KILL after `kill_grace`). Then it reports `lost_local`, so the fenced requeue applies and two attempts
    never overlap.
13. **Scheduler restart.** Replies to mutating requests are kept in SQLite for 10 minutes (idempotency survives a
    restart). Leases, the claim deadline and orphan times are extended by the measured downtime at start (R-OPS-03).
14. **Events and the outbox.** Job events are published after the transaction commits, in order, retried until
    JetStream acks. A crash between commit and publish can lose one event; the `get` reply is always authoritative.
15. **`listen`** configures the scheduler's HTTP bind addresses (R-SEC-06); the NATS bind address belongs to the
    `nats-server` configuration, which `corral nats-config` generates for a loopback or overlay address.
16. **Not in M1:** the NATS micro-service discovery endpoints, `admin kill_switch` (needs the guard; the verb replies
    `unavailable`), worker-local metrics (R-OPS-02, worker half), `corral config check` printing a source for every
    key (only the client keys carry one).

### 17.1 Changes after the M1 review

17. **Followers go through the scheduler** (R-SEC-02, R-IO-04). `follow` asks the scheduler to create push consumers
    that deliver into the caller's inbox space; clients lost every `$JS.API` permission (PROTOCOL.md 8 and 12.1).
    Chosen over a client-side consumer with a restricted subject filter (NATS permissions cannot constrain the consumer
    configuration, only the API subject) and over a scheduler-side relay (one more copy of every byte, plus state per
    follower): the server already does the delivery, ordering and cleanup. The `v` field is unchanged: `follow` is a
    new verb and the log headers lost a field.
18. **No lease token in log headers.** The sink accepts a chunk from the worker named in the log subject for the
    job's current attempt; the token is not needed for that and must not be readable by followers.
19. **Light jobs are never unlimited.** A light job without `ram_mb` is admitted and watched against
    `worker.light_ram_default` (default 512M) times the slack.
20. **The watchdog fails closed.** A process table that cannot be read, or reports an empty unit for a live job, is an
    unknown footprint, not zero; five unknown one-second samples in a row stop the job as `ram_exceeded`.
21. **Claim lifecycle.** A heartbeat revives an orphaned attempt to `claimed` when it never started (new transition,
    PROTOCOL.md 5); the claim deadline also applies to such an orphan; a cancel that races with a decline, `lost_local`
    or the claim deadline ends `cancelled`; claim requests are retried with the same `req_id`.
22. **Attempts are keyed by (job, attempt)** in the worker (registry, admission table, lock owner `corral:<id>:<n>`),
    and a lock file that does not parse is live for 10 s after its last write.
23. **A log gap does not stall archiving** (PROTOCOL.md 8): recorded in the archive after `sink_hold_max` redeliveries.
24. **Scheduler progress subscription.** The scheduler may subscribe to `corral.progress.>` (the template lacked it, so
    `status` never showed progress).
25. **Small hardening.** Progress is accepted only from the worker that holds the job; `artifacts_max_mb` has a
    scheduler-side ceiling (8192) checked at submit and used with saturating arithmetic; the audit (`events`) table is
    pruned with retention.
