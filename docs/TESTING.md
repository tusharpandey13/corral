# corral: test strategy, end-to-end scenarios and chaos rubric

Status: **approved by the owner (v0.1, 22-Nov-2026); round-2 drafts excepted. Milestone M1's unit and integration tests exist (section 12); the end-to-end and chaos scenarios have not been run.** Companion to
`docs/SPEC.md` (requirement IDs `R-*`), `docs/PROTOCOL.md`, `docs/GUARDS.md` and `docs/RUNTIMES.md`. It is the
"end-to-end and chaos rubric (owed)" named in SPEC.md §13 and the milestone exit criteria the rollout refers to.

This document is generic. Host names are `server` (always on: NATS, scheduler, worker, guard), `laptop` (macOS,
sleeps, has a user) and `gpu-box` (Linux, sometimes on). The overlay network is `example.ts.net`. A deployment's own
rubric (real hosts, real services, approval gates) lives in that deployment's private repository and refers to the
test IDs here.

---

## 1. Principles

1. **Tests cite requirement IDs.** Every test names the `R-*` IDs it proves; the matrix in §8 must have no empty row
   before v1 is called done.
2. **Safety rules are never weakened to make a test pass** (`AGENTS.md`). A test that only passes with a relaxed
   ceiling, a retried killed job or a name-based kill is a failing test.
3. **Sandbox by default.** Unit tests touch no real system state; integration and chaos tests run in temporary
   directories with a test-only NATS account and identities generated per run. Live mode exists (§7) but is off
   unless the owner switches it on for one named run.
4. **Fault injection is reversible and bounded.** Every injected fault has an undo that fires on its own (dead-man,
   §7.3) and a blast radius stated before the run.
5. **Observe the whole system, not just the job.** Every test ends with the global invariants of §2 checked, so a
   test cannot pass while breaking something it was not looking at.
6. **One fault at a time** in chaos runs; combined faults are separate, explicitly named cases.

## 2. Global invariants (checked after every integration, end-to-end and chaos test)

| ID | Invariant | How the harness checks it | Spec |
|---|---|---|---|
| INV-1 | No process the test did not start (or explicitly match) was signalled. | Sentinel processes started before the test (one with a name similar to the job, one keep-awake look-alike, one matched by a `never` rule) are alive afterwards with the same pid **and** start time. | R-EXEC-01, R-GUARD-03, R-GUARD-04, R-AWAKE-02 |
| INV-2 | A killed job is final. | For every job that reached `killed`: `attempts` unchanged afterwards, no later `claimed` event, no new process group. | R-QUEUE-04, G4 |
| INV-3 | At most one live attempt per job. | At 1 Hz the harness lists process groups tagged with `CORRAL_JOB`/`CORRAL_ATTEMPT` across all emulated hosts; never two for one job. | R-OPS-01, R-SCHED-06, R-QUEUE-06 |
| INV-4 | Every queued job has a waiting reason per candidate worker. | `corral status --json` after each phase. | R-ADM-04, G3 |
| INV-5 | Memory ceiling held for corral-caused load. | Harness samples used memory of the host (or the VM / cgroup emulating it) at 1 Hz; it never exceeds the configured ceiling for more than 2 guard samples while only corral load is added. | R-GUARD-09, G1 |
| INV-6 | Nothing left behind. | After cleanup: no corral-created process, keep-awake hold, host lock file, reservation, cgroup, temp dir or NATS consumer remains. | R-AWAKE-01, R-LOCK-03 |
| INV-7 | No secret in any output. | A canary secret is placed in the submitter's environment and in an ignored `.env`; it must not appear in job env dumps, logs, archives, artifacts, metrics or notifications. | R-EXEC-05, R-IO-02 |
| INV-8 | State is consistent. | Scheduler SQLite `PRAGMA integrity_check` = `ok`; every job's state is reachable by the PROTOCOL.md §5 table from its event history. | R-QUEUE-02, R-QUEUE-03 |
| INV-9 | (draft, round 3) On a worker with `job_profiles = "required"`, no job process runs as the worker's uid. | At 1 Hz the harness lists process groups tagged `CORRAL_JOB` on such a worker; each runs as the runner uid of its instance's profile. INV-7 there also checks that no secret value or ciphertext appears in scheduler state, bus traffic, logs, artifacts or patches, and that proposal output holds no canary. | R-JPROF-01, R-JPROF-07, R-JPROF-12, R-JPROF-16 |

An invariant failure fails the test **and** is a severity S1 finding (§9), whatever the test was about.

---

## 3. Unit tests

Unit tests run with `cargo test` on macOS (aarch64) and Linux (x86_64) and must not touch real system state.

### 3.1 Fakes (behind traits)

| Fake | Replaces | Capabilities the tests need |
|---|---|---|
| `FakeClock` | wall and monotonic time | manual advance; jump forward/backward (wall only) |
| `FakeMemoryProbe` | `kern.memorystatus_level`, `vm.swapusage`, `/proc/meminfo`, PSI | scripted sequences per sample; per-process footprint table |
| `FakePressureProbe` | memory pressure level / PSI triggers | scripted events |
| `FakeIdleTracker` | HID idle time, logind idle | scripted idle values; synthetic-input stamps |
| `FakePowerProbe` | AC/battery, low power mode | scripted |
| `FakeThermalProbe` | thermal state / hwmon | scripted °C or state |
| `FakeScreenProbe` | lock screen | scripted locked/unlocked with `since` |
| `FakeProcessTable` + `FakeSignaller` | process listing and signalling | pids with start times, cmdlines, cwd, cgroup; records every signal instead of sending it; pid reuse |
| `FakeLockStore` | lock files | in-memory files with `O_EXCL` semantics, plus a mode backed by a temp dir for conformance |
| `FakeKeepAwake` | keep-awake providers | records hold/renew/release; can fail probe |
| `FakeNotifier` | notification backends | records messages |
| `FakeRuntime` | `Runtime` trait | start, output, exit codes, kill reasons, lost (no exit), destroy retries (RUNTIMES.md §5) |

### 3.2 Unit suites

| ID | Suite | Requirements |
|---|---|---|
| U-01 | Job state machine: table test over every (state, event) pair; illegal transitions rejected and logged | R-QUEUE-03 |
| U-02 | Claim selection: caps, RAM, class, exclusive, profile; prefer + `fallback_after` with `FakeClock`; ordering and backfill; paid tier rejected without the phase-2 gate | R-SCHED-01..04 |
| U-03 | Memory projection at the boundary (admit exactly at the floor, refuse one MiB below), reservations included | R-ADM-02 |
| U-04 | Guard levels from sample sequences: each level, `for_samples`, hysteresis, pause → escalate, tier-by-tier shed with settle, stop at clear; action at the second sample after crossing | R-GUARD-02, R-GUARD-09 |
| U-05 | `never` never signalled at any level incl. kill switch; `protected` only at critical/stop; unresolvable protected set → shed kills nothing from that matcher | R-GUARD-03 |
| U-06 | Pid reuse: a pid whose start time changed between sample and signal is not signalled | R-GUARD-04 |
| U-07 | Per-registration budget: footprint > budget × slack → `ram_exceeded` | R-GUARD-06 |
| U-08 | Observe mode: same decisions logged, `FakeSignaller` records nothing | R-GUARD-10 |
| U-09 | Restore hooks: run on configured levels, hanging hook killed at its timeout, outcome logged | R-GUARD-07 |
| U-10 | Idle tracker: stage 1 / stage 2, synthetic stamp within 2 s and exclusive windows ignored | R-ADM-07 |
| U-11 | Re-check under the lock: a check flips between lock acquire and re-check → decline, lock and reservation released | R-ADM-01 |
| U-12 | Waiting explanation composition (headline + `also`), update within 30 s of a change, one notification per (job, reason) after `notify_after` | R-ADM-04, R-ADM-05 |
| U-13 | Reservations: two concurrent `admit --reserve` with one heavy slot → exactly one ok; reservation expiry frees the slot | R-ADM-02, R-SCHED-05 |
| U-14 | Keep-awake contract with `FakeKeepAwake`: named hold, renew without gap, release on every exit path (exit, kill, cancel, stale lease, panic unwind), `keepawake_unavailable` when required and probe fails | R-AWAKE-01, R-AWAKE-03 |
| U-15 | Static checks: no `pkill`, `killall`, name-based kill or `kill -<sig> -1` anywhere in the source tree (CI grep plus a clippy lint where possible) | R-AWAKE-02, R-GUARD-04 |
| U-16 | Config validation: unknown keys, errors naming the key, `0.0.0.0` without `allow_public_bind`, non-zero cost tier without the phase-2 gate | R-OPS-04, R-SEC-06, R-SCHED-04 |
| U-17 | Capability tokens: scope (job, verb, lease), expiry, tampering | R-SEC-05 |
| U-18 | Snapshot builder: dirty tracked file kept, ignored file excluded, deny-list refusal naming the file, size cap | R-IO-01, R-IO-02 |
| U-19 | Progress line parser (`##corral progress=`), malformed lines passed through as output | R-IO-06 |
| U-20 | Lease timing with `FakeClock`: `orphaned` after `lease_ttl`, `lost`/requeue after `orphan_grace`, `not_after` → `expired`, downtime extension after scheduler restart | R-QUEUE-05, R-QUEUE-07, R-OPS-03 |
| U-21 | CLI exit codes: `admit` 0/1/2/5, `wait` job code / 125, lock 0/1/2 | R-ADM-06, R-IO-04 |
| U-22 | Every request schema accepts `authz`; `require_grant` policy returns `grant_required` with the same schema | R-SEC-04 |
| U-23 | Finish classification: own kill (timeout, cancel), guard kill with reason, unexplained signal → `external_signal`; killed never requeued | R-QUEUE-04, R-GUARD-08 |
| U-24 | `pause_on_exit` codes pause the worker with the reason | R-EXEC-06 |
| U-25 | Log sink: append only if `seq = last + 1` and token matches; redelivery idempotent; out-of-order held | R-IO-03, R-QUEUE-06 |
| U-26 | Worker driven by `FakeRuntime`; native runtime reports tier 0 | R-RT-01, R-RT-02 |
| U-27 | Legacy shim golden tests against recorded legacy outputs for every command and exit code | R-MIG-01 |
| U-28 | Kill switch persisted in the state dir; a new guard instance on the same state refuses admission | R-GUARD-05 |
| U-29 | (draft, round 2) Classifier golden corpus: segment splitting (`;`, `&&`, `\|\|`, `\|`, newline, `(`), backslash-newline, comments, skipped prefixes, `bash -lc`, heredoc and quoted text never matching, `cd` updating cwd, `ssh` payloads; each segment's class and rule | R-PLACE-01 |
| U-30 | (draft) Decision combination table: all-local → local; any heavy/unknown → target; `local_only` + heavy → deny naming both; observe mode never rewrites | R-PLACE-02, R-PLACE-03 |
| U-31 | (draft) Hook adapters: fixture pre-tool events → exact harness outputs for local, rewrite, deny, queue (exit 75 text), guarded local; secret-shaped commands logged by hash only | R-PLACE-03, PROTOCOL.md §14 |
| U-32 | (draft) Escape marker and `corral local --reason` logged; `--propose` writes a diff and never edits the rules file; external profile selects a rule set | R-PLACE-05..07 |
| U-33 | (draft) Exec sync manifest delta: edited, added, deleted, renamed files; deny-list; `dirty_foreign` detection against the last applied manifest | R-REXEC-03, R-REXEC-04 |
| U-34 | (draft) Sync-back: patch of tracked + untracked-not-ignored changes; ignored paths only via `pull`; conflicting caller edit → patch kept, not applied | R-REXEC-05 |
| U-35 | (draft) Light vs heavy admission with fakes: user active + battery → light admitted, heavy refused `ac_power` | R-ADM-08 |
| U-36 | (draft) External token validation: valid DPoP-bound token; replayed `jti`; proof key ≠ `cnf.jkt`; wrong `htu`/`htm`; stale `iat`; wrong audience; expired; unknown issuer key | R-SEC-07 |
| U-JP-01 | (draft, round 3) Matching corpus: exact prefix matches; near misses (case, homoglyph, abbreviation, trailing `;`, empty token, NUL); args deny globs incl. `--opt=value`; env deny incl. cache variables; cwd escape by `..` and by symlink; forbidden `prefix[0]` set | R-JPROF-03 |
| U-JP-02 | (draft) Closed schema: each missing and each unknown field refused; caps; 4 KiB limit; `run_as` derived from `profile_id`; `secrets` other than `none` refused; `cache.stores` outside `npm`/`cargo`/`pip` refused; rendering golden file in schema order with `purpose` last | R-JPROF-02, R-JPROF-16, R-JPROF-19 |
| U-JP-03 | (draft) Digest: one-byte change → new digest; rev monotonic; `replaces` must equal the installed digest | R-JPROF-05 |
| U-JP-04 | (draft) Light/heavy line and hook decisions with `placement.job_profiles = "required"`: weight vs `wait_threshold`; heavy matched → rewrite; heavy unmatched → deny + `profile_miss` (online and offline); light unknown → light envelope; escape hatch denied on heavy; mixed command with one unmatched heavy segment → whole command denied | R-JPROF-13, R-PLACE-03, R-PLACE-05 |
| U-JP-05 | (draft) Proposal redaction: canaries in argv, env, URLs, heredocs and high-entropy names never appear; below `--min-sessions` nothing proposed; file mode 0600 | R-JPROF-12 |
| U-JP-06 | (draft) Approval levels: defaults; `approve` or unsafe admin below 2 refused by config check; level 0 needs no grant; safe vs unsafe admin direction mapped correctly | R-JPROF-18 |
| U-JP-07 | (draft) Cargo import/publish logic over a fake pool: import only lockfile crates with a matching hash of the private copy; publish only hashes listed in the lockfile; non-crates.io and git sources ignored | R-JPROF-20 |
| U-JP-08 | (draft, 23-Nov) Prefixes: 1-4 accepted; five, mixed first tokens, duplicates and prefix-of-another refused; four prefixes round-trip under 4 KiB; the matched prefix index is recorded; `cargo fmt && cargo clippy && cargo test` over one profile is accepted | R-JPROF-02, R-JPROF-03, R-JPROF-23 |
| U-JP-09 | (draft, 23-Nov) Tool pins: grammar, uniqueness, sort order, `exe` coverage, `was` handling, rendering with and without a version change; manifest hashing | R-JPROF-22 |
| U-JP-10 | (draft, 23-Nov) Compound-command rule: one case per reason code and per allowed glue form; the `mixed_light` split message golden (two-call split, `&&` marked conditional, `cd` carried into each call); an unmatched heavy step refuses the call whatever else it holds; wrapper unwrapping; `unparseable` with a heavy program word; observe mode logs `split_refused` | R-JPROF-23, R-PLACE-01, R-PLACE-02 |
| U-JP-11 | (draft, 23-Nov) `network` allowlist in a profile is representable and rendered but refused by the installer check `network_allow_unsupported`; `fallback = local` for an agent job to a `required` worker is `bad_request` | R-JPROF-02, R-JPROF-06, R-AGENT-04 |

### 3.3 Lock conformance (LC)

| ID | Case | Requirements |
|---|---|---|
| LC-01 | The reference lock implementation and corral on **one temp file**: contention (never two holders over 10 000 randomized rounds), holder-only release, renew by tmp + rename, corrupt file treated as stale | R-LOCK-01 |
| LC-02 | Stale takeover race with two contenders (both implementations): exactly one wins, one `takeover` log line | R-LOCK-01 |
| LC-03 | Stale non-corral holder → corral declines with the holder description and one notification; `takeover = "any"` → takes over | R-LOCK-02 |
| LC-04 | `flock` kind: kernel release on holder death; `reentry_env` honoured | R-LOCK-04 |

---

## 4. Integration tests (IT)

Setup per test: a real `nats-server` binary (pinned version) started by the harness on a random loopback port with
JetStream in a temp dir; operator, account and nkeys generated for the run; one `corral scheduler`; one or more
`corral worker` + `corral guard` pairs with **fake capabilities and fake probes** (injected through a test-only
probe-override socket, §10), each with its own temp work dir and guard socket. The guard's matchers are scoped to the
test's process tree (§10, `sandbox_scope`), so a real kill can only reach processes the test started.

| ID | Test | Requirements |
|---|---|---|
| IT-01 | Submit with no worker: id in < 1 s, state `queued`, waiting reason present | R-QUEUE-01, R-ADM-04 |
| IT-02 | `kill -9` the scheduler right after a submit reply; restart; job present with the same state | R-QUEUE-02 |
| IT-03 | Allocator declared 50 MB allocates 200 MB → `killed:ram_exceeded`, attempts = 1, queue empty, `finished.reason` equals the guard's, one notification | R-GUARD-06, R-QUEUE-04, R-GUARD-08 |
| IT-04 | SIGSTOP the worker 120 s → `orphaned`; SIGCONT → `running`, same token | R-QUEUE-05 |
| IT-05 | After requeue, publish heartbeat/log/event with the old token → rejected; worker kills its process group; late logs discarded | R-QUEUE-06 |
| IT-06 | `resubmit` of a killed job: `agent:*` → `forbidden`; `owner` → new id with `retry_of` | R-QUEUE-08 |
| IT-07 | 50 parallel claims against 10 jobs → 10 distinct winners | R-SCHED-06 |
| IT-08 | A matched non-corral heavy process alive → `heavy_ok=false` naming it; no claim | R-SCHED-05 |
| IT-09 | Check flipped to failing between lock acquire and re-check → decline, lock released, `queued`, attempts unchanged, reason visible | R-ADM-01, R-ADM-03 |
| IT-10 | `corral admit` exit codes with a live guard, refused guard, bad usage, stopped guard | R-ADM-06 |
| IT-11 | A second guard on the same host exits "already running" | R-GUARD-01 |
| IT-12 | A job that forks children (incl. one that calls `setsid`): all gone after cancel; an unrelated process with a similar name survives | R-EXEC-01 |
| IT-13 | `sleep 60` with timeout 2 s → `killed:timeout`; worker silent past `timeout + orphan_grace` → scheduler backstop | R-EXEC-02 |
| IT-14 | Linux: per-attempt cgroup with `memory.high`/`memory.max` read back | R-EXEC-03 |
| IT-15 | Process QoS of exclusive vs non-exclusive jobs inspected | R-EXEC-04 |
| IT-16 | Env dump: only allowed variables; submitter canary absent | R-EXEC-05 |
| IT-17 | `echo hi; exit 3` → `wait` prints `hi` within 2 s, exits 3 | R-IO-04 |
| IT-18 | Artifacts over cap → 413, recorded; retention GC with `FakeClock` keeps queued/running/`--keep` | R-IO-05 |
| IT-19 | NATS restarted mid-job → archive byte-identical to the worker's local file; no duplicate attempt | R-IO-03, R-OPS-01 |
| IT-20 | Lease open → exec twice (second sees first's files) → close → slot free; guard kill of a step → lease `killed` | R-LEASE-01, R-LEASE-02 |
| IT-21 | Anonymous connect refused; every forbidden publish/subscribe per identity fails; payload `from` mismatch → `forbidden` | R-SEC-01..03 |
| IT-22 | HTTP token for job A cannot read job B; expired → 401 | R-SEC-05 |
| IT-23 | `/metrics` and `/healthz` scrape; guard and worker local metrics | R-OPS-02 |
| IT-24 | Scheduler stopped 5 min with a running job; restart; no job `orphaned` | R-OPS-03 |
| IT-25 | Restart worker, scheduler, guard, NATS one at a time mid-job: never two attempts (worker restart also with the job leader dead and a group member alive) | R-OPS-01 |
| IT-26 | `kill -9` the worker holding a lock → lock stale after ttl → next corral attempt takes over; nested `corral lock exec` re-enters | R-LOCK-03, R-LOCK-04 |
| IT-27 | Keep-awake through a stub `hold-cli` provider: named hold, renew, release on every exit path; failing provider → `keepawake_unavailable` | R-AWAKE-01, R-AWAKE-03 |
| IT-28 | Progress events reach a follower | R-IO-06 |
| IT-29 | Exit code in `pause_on_exit` pauses the worker; status shows reason | R-EXEC-06 |
| IT-30 | Migration rollback dry run per phase (GUARDS.md §10.3) in a temp prefix: rollback command restores the legacy files and units | R-MIG-02 |
| IT-31 | (draft, round 2) `corral exec` with the target worker offline → exit 75 within `exec_wait` + 1 s, reason names the worker; nothing queued | R-REXEC-01 |
| IT-32 | (draft) 20 sequential `exec -- true` reuse one session; overhead p50/p95 recorded; idle timeout closes it and frees the slot | R-REXEC-02 |
| IT-33 | (draft) Exec in `mirror` mode on a temp repo: dirty file and new untracked file visible; ignored canary `.env` absent; missing base commit fetched from a temp exchange remote; foreign change → `dirty_foreign`, file untouched | R-REXEC-03, R-REXEC-04, INV-7 |
| IT-34 | (draft) Exec sync-back: lockfile patch applied; conflicting local edit → patch kept and reported; `--pull` of an ignored path | R-REXEC-05 |
| IT-35 | (draft) Guard kill during an exec → exit 125 with the reason; not re-run | R-REXEC-06, INV-2 |
| IT-36 | (draft) Agent job with a **fake runner** (script that edits a file and commits): worktree at `base_sha`, branch pushed to a temp exchange remote, `agent_result` fields; cancel calls the runner's stop and removes the worktree | R-AGENT-01, R-AGENT-02, R-AGENT-05 |
| IT-37 | (draft) `agent` slot cap + 1 → one `queued` with `agent slots full`; deadline with `fallback = wait` → one notification; `fallback = local` → local start | R-AGENT-03, R-AGENT-04 |
| IT-38 | (draft) Hook mode against a fake harness: rewrite produces an exec that returns the original exit code; target offline: heavy → queued job + exit 75, unknown → guarded local run killed at its timeout and logged | R-PLACE-03, R-PLACE-04 |
| IT-39 | (draft) External issuer stub (temp JWKS): HTTP artifacts read with a DPoP-bound token; NATS connect through the auth callout with a token; each U-36 rejection case end to end | R-SEC-07 |
| IT-JP-01 | (draft, round 3) Fake installer: install, refuse for each check (expiry, replay, rollback, host, unsafe `run_as`, `exe` in a home, unenforceable value), remove; a forced mid-way failure leaves no user, group, directory or service entry; presence lists `id@rev:digest` | R-JPROF-05, R-JPROF-06 |
| IT-JP-02 | (draft) Workspace sync into a runner workspace: dirty and deleted files reflected; ignored cache kept; stray file from an earlier instance reset; `.env` never sent; base via bundle | R-JPROF-08 |
| IT-JP-03 | (draft) Files out: patch applied; `--pull` outside `sync_out.pull` refused; symlink pull refused; translated display vs raw log | R-JPROF-09 |
| IT-JP-04 | (draft) Lease cap: above-cap lease → `bad_request`; `sleep` past lease → `killed:timeout` | R-JPROF-10 |
| IT-JP-05 | (draft) Revocation: disable mid-step → `cancelled:profile_revoked`; expiry mid-step finishes; dispatch after `remove` declined by the worker | R-JPROF-11 |
| IT-JP-06 | (draft) Agent profile with a fake runner: runs as its runner uid; bundle pushed by the worker; cap + 1 queues; `fallback = local` refused | R-JPROF-14, R-AGENT-04 |
| IT-JP-07 | (draft) Plain submit and `mirror` exec to a `required` worker declined (`job_profile`, mirror refused) | R-JPROF-01, R-REXEC-04 |
| IT-JP-08 | (draft) Hook against a fake harness with `job_profiles = "required"`: matched heavy runs on the target; unmatched heavy denied with the hint and logged, nothing runs anywhere; light unknown runs in the light envelope and is killed at its timeout | R-JPROF-13 |
| IT-JP-09 | (draft) Shared cache, npm: two profiles in one group; the second hydrates offline from the first's downloads; indexes stay separate; a miss → `deps_missing` and no `node_modules` left | R-JPROF-19, R-JPROF-20 |
| IT-JP-10 | (draft) Shared cache, cargo and pip with a local registry stub: import and publish by lockfile hash; pip installs only with hashes | R-JPROF-20 |
| IT-JP-11 | (draft) Cache GC: over-limit store shrinks to ≤ 80%; corrupt entry deleted and reported with the owning profile; entries younger than 10 min kept; publish refused under the free-space floor | R-JPROF-21 |
| IT-JP-12 | (draft) Scheduler least privilege: static check of no process spawn; config check warns on readable credentials | R-JPROF-15 |
| IT-JP-13 | (draft, 23-Nov) Pinned tools with a fake installer and fake tool dirs: install; refuse a path in a home, writable by the runner, wrong hash, uncovered `exe`, wrong `was`; alter a pinned file between instances → `tool_changed` with the refresh hint and one notification per day; same size and mtime but changed content is caught through `ctime`; `profiles refresh` builds rev + 1 and the old rev keeps running (or refusing) until approval; a repository toolchain file does not change the executed version; daily verify emits `tool_corrupt` | R-JPROF-22, R-JPROF-21 |
| IT-JP-14 | (draft, 23-Nov) Hydrate fallback: offline miss → one fetch instance as the fetch profile's runner with scripts off → offline retry succeeds; the requesting instance cannot open a connection; a fetch profile that is expired, busy past the lease or failing → `deps_missing` naming the fetch job; an invalid `fallback` is refused at request and by the installer | R-JPROF-20 |
| IT-JP-15 | (draft, 23-Nov) Box worker: an undeclared job, including an agent-submitted one, is `box_job_not_declared`; a declared service job runs; `--fallback local` for agent jobs is `bad_request`; large-repository `git log -p` and `rg` run in the light envelope | R-JPROF-13, R-AGENT-04 |

### 4.1 Security suite for job profiles (SEC-JP, draft, round 3)

Run on a real host with real runner users (macOS and Linux), in the sandbox of §7.1. Each case is MUST.

| ID | Attempt | Expect | Spec |
|---|---|---|---|
| SEC-JP-01 | argv `["npm","test;","rm","-rf","~"]` and `["npm","test && curl x"]` | no match; no shell is ever executed by the launcher (static test) | R-JPROF-03 |
| SEC-JP-02 | case, homoglyph, abbreviation, double space, trailing NUL variants of the prefix | refused | R-JPROF-03 |
| SEC-JP-03 | `argv[0]` as `./npm`, `/tmp/npm`, a planted workspace binary | refused; only `exe` runs | R-JPROF-03 |
| SEC-JP-04 | env injection: `PATH`, `NODE_OPTIONS`, `DYLD_INSERT_LIBRARIES`, `LD_PRELOAD`, `PYTHONSTARTUP`, `BASH_ENV`, `GIT_SSH_COMMAND` | refused | R-JPROF-03 |
| SEC-JP-05 | interpreter `-c`, module look-alikes, script outside the workspace | refused | R-JPROF-03 |
| SEC-JP-06 | cwd escape by `..`, absolute path, workspace symlink | refused | R-JPROF-03 |
| SEC-JP-07 | profile requests with `prefix[0]` a shell, `env`, `xargs`, `sudo`, an OS script host, or `exe` under a home | refused at request; a forged one fails at the installer | R-JPROF-03, R-JPROF-06 |
| SEC-JP-08 | overlong argv; more args than `args.max` | refused | R-JPROF-03 |
| SEC-JP-09 | `bash -lc "npm test"` through the hook | unwrapped; no shell reaches the worker | R-JPROF-13 |
| SEC-JP-10 | a repository test script reads other users' files and opens a network connection under `network: none` | both fail; the job shows the failures | R-JPROF-04 |
| SEC-JP-11 | the runner reads the worker's credentials | EACCES | R-JPROF-07 |
| SEC-JP-12 | the runner signals or debugs a worker-owned process | EPERM | R-JPROF-07 |
| SEC-JP-13 | the runner writes another runner's workspace | EACCES | R-JPROF-07 |
| SEC-JP-14 | the runner reads another runner's home, including its package-cache index | EACCES | R-JPROF-07, R-JPROF-19 |
| SEC-JP-15 | **Cache poisoning, npm:** a group member overwrites a shared content entry that another member needs with a trojan whose install script writes a canary | the consumer's install fails its integrity check, the canary never exists, no partial `node_modules` is used; GC deletes the entry and reports the member's profile | R-JPROF-19..21 |
| SEC-JP-16 | **Cache poisoning, cargo:** a member writes a trojan crate into the pool under the real hash of a crate another member needs, and swaps it again during the import | import rejects the private copy (`cache_reject` names the member); the trojan's build script canary never appears | R-JPROF-20, R-JPROF-21 |
| SEC-JP-17 | **Malicious lockfile:** a member publishes a trojan under a hash its own lockfile names for a popular crate | another member, with the real lockfile, never imports it | R-JPROF-20 |
| SEC-JP-18 | **Cache poisoning, pip:** a member replaces a cached wheel body | the consumer's hash-checking install fails; nothing installed | R-JPROF-20 |
| SEC-JP-19 | caller tries to redirect or unverify a store (env, workspace `.npmrc`, user config file) | env refused; `.npmrc` refused `cache_config_override`; config ignored | R-JPROF-20 |
| SEC-JP-20 | a runner outside the group, and a member of another group, read the store | EACCES | R-JPROF-19 |
| SEC-JP-21 | a repository `rust-toolchain.toml`, `.nvmrc` or version-manager shim tries to select another tool version | the pinned `exe` runs; the notice line is printed | R-JPROF-22 |
| SEC-JP-22 | the runner tries to replace or edit a pinned tool file or directory, or `PATH` is set by the caller | EACCES; caller `PATH` and toolchain variables refused | R-JPROF-22, R-JPROF-03 |
| SEC-JP-23 | a tool swapped after install with its size and mtime restored | detected (`ctime` or re-hash); instance refused `tool_changed` | R-JPROF-22 |
| SEC-JP-24 | the requesting profile's instance tries the network during hydrate fallback; the fetch step runs an install script from the repository | the instance has no network; install scripts do not run | R-JPROF-20 |
| SEC-JP-25 | wrappers and glue that smuggle a heavy step: `xargs`, `find -exec`, `eval`, `$( )`, a loop, `&`, a heredoc on stdin, a non-allowed `VAR=`, `export`, `cd $D` | the whole call is refused with the matching code; nothing runs | R-JPROF-23 |

---

## 5. End-to-end scenarios (E)

### 5.1 Topology

Three emulated hosts, each a VM or a Linux container with its own network namespace and its own cgroup, so that
"sleep" (freeze the cgroup), "partition" (drop packets in its namespace) and "reboot" (kill everything in the cgroup,
restart its units) can be injected per host:

| Emulated host | Runs | Notes |
|---|---|---|
| `server` | NATS, scheduler, worker, guard | Linux |
| `gpu-box` | worker, guard | Linux; fake `gpu` cap |
| `laptop` | worker, guard | Real macOS (a macOS VM or CI runner) for the macOS-only paths (footprint, `taskpolicy`, keep-awake, lock screen probes); otherwise a Linux container with fake macOS probes and caps |

The harness runs the CLI as `agent:<host>` and `owner` identities, collects every event stream, and checks §2 at the
end. A fault proxy (for example a TCP proxy with latency/timeout/reset controls) sits between each worker and NATS
and between each client and the HTTP port, so network faults can be injected without root.

### 5.2 Scenarios

| ID | Scenario | Pass criteria | Requirements |
|---|---|---|---|
| E-01 | Snapshot submit from `laptop` worktree with a dirty file, runs on `server`, `wait` streams and returns the exit code, artifacts fetched | output within 2 s; exit code equal; artifact sha256 equal; dirty file present, ignored file absent | R-IO-01, R-IO-04, R-IO-05 |
| E-02 | `--prefer laptop` with `laptop` offline: waits `fallback_after`, then runs on `server`; with `never`, keeps waiting with reason | timing within one claim interval; reason text names `laptop: offline since ...` | R-SCHED-02, R-ADM-04 |
| E-03 | `metal` job while `laptop` is offline for a simulated 3 days | stays `queued` with reason; runs when `laptop` returns; no alert for the offline worker, alert when waiting > 24 h | R-SCHED-01, G5 |
| E-04 | Caps routing across three hosts and backfill of small jobs behind a large one | placement matches the table | R-SCHED-01, R-SCHED-03 |
| E-05 | Interactive lease from `server` session on `laptop`: open, 3 steps, idle timeout | slot freed after idle; directory removed | R-LEASE-01 |
| E-06 | Cancel in each state (queued, claimed, running, worker offline) | ends `cancelled`; offline worker learns it on reconnect or the attempt ends `cancelled` after grace | PROTOCOL.md §7 |
| E-07 | `measurement` profile on `laptop`: screen locked, then unlocked, user active, user idle 5 min | waiting text changes within 30 s each step; starts only after stage 1 and stage 2 | R-ADM-04, R-ADM-07 |
| E-08 | A non-corral tool uses `corral admit --reserve` and `corral run --lock` while a corral job wants the same slot | never two heavy units; both see reasons | R-ADM-06, R-SCHED-05 |
| E-09 | Legacy lock tool and corral contend for one host lock across hosts (lock on `laptop`, legacy tool over ssh) | LC rules hold end to end | R-LOCK-01, R-LOCK-02 |
| E-10 | Guard `observe` mode next to a legacy guard on one emulated host, fed the same memory hog | every legacy action has a matching corral decision (same tier, ± 1 sample); corral sends no signal | R-GUARD-10, GUARDS.md §10.3 G0 |
| E-11 | Version skew: scheduler at protocol `v`, one worker at `v − 1` | accepted, skew shown in `corral workers` | SPEC.md §15 |
| E-12 | Identities: every CLI verb as `agent:*` and `owner`; priority > 80 and `lock = none` owner-only | forbidden where specified | R-SEC-02, R-SEC-03, R-QUEUE-08 |
| E-13 | Notifications end to end through the `command` backend | one message per action, rate-limited per (kind, subject) | R-ADM-05, R-GUARD-08 |
| E-14 | Full restart of all three hosts in sequence with jobs queued and running | no job lost without a `lost` record; no duplicate attempt | R-OPS-01, R-OPS-03 |
| E-15 | (draft, round 2) Placement end to end: a scripted agent session on `server` with the hook in `observe` then `rewrite` mode runs a mixed command set; `laptop` online, then frozen (sleep) | observe changes nothing and logs every decision; rewrite runs heavy commands on `laptop` with exit codes preserved; with `laptop` asleep heavy commands are queued (exit 75) and run on wake; unknown commands run on `server` under guard | R-PLACE-*, R-REXEC-01 |
| E-16 | (draft) Agent job from `server` to `laptop`, `laptop` frozen mid-run past `orphan_grace` | job `lost` → requeued per policy; the stale attempt's runner is killed on wake (`stale_lease`); never two agents on one branch | R-AGENT-*, R-QUEUE-05, R-QUEUE-06, INV-3 |
| E-17 | (draft) Light vs heavy on `laptop` with a fake active user on battery: light exec and an agent job run; heavy exec waits with `ac_power` | as stated; INV-5 holds | R-ADM-08 |
| E-JP-01 | (draft, round 3) Placement with job profiles: the E-15 session with `job_profiles = "required"` on both sides and two approved profiles (fake authorization server) | matched heavy commands run on `laptop` as their runners; unmatched heavy commands are denied with the hint and logged `profile_miss`, online and while `laptop` sleeps; light commands run on `server`; INV-9 holds | R-JPROF-*, R-PLACE-* |

---

## 6. Chaos rubric (CH)

Each case below is written for the **sandbox** (§5.1 topology). The "live" line says what changes if the owner
approves a live run (§7.2). Durations use test configuration: `lease_ttl = 90s`, `orphan_grace = 15m` unless the case
says it shortens them (shortened values are recorded in the run record).

Format per case: **Inject** (sandbox / live), **Expect** (with IDs), **Assert** (observable, machine-checked),
**Pass/fail**, **Blast radius**, **Cleanup**. Severity if it fails is in §9.

### CH-01 Worker sleep / lid close mid-job

- **Inject (sandbox):** freeze the `laptop` cgroup (worker, guard and job together, as real sleep does) for
  (a) 60 s, (b) 5 min, (c) 20 min with `orphan_grace = 15m`, `retry_on_lost = true`, `max_attempts = 2`.
- **Inject (live):** the owner closes the lid (or the system is put to sleep by the documented OS command) while a
  test job runs; never by killing a keep-awake process.
- **Expect:** (a) stays `running`, no state change. (b) `orphaned` after `lease_ttl`, back to `running` with the same
  token on thaw, attempts = 1 (R-QUEUE-05). (c) `orphaned` → `queued`, attempt 2 starts on an eligible worker (or
  waits with a reason if none); on thaw the first heartbeat gets `stale_lease`, the worker kills attempt 1's process
  group and discards its unsent logs (R-QUEUE-06); the keep-awake hold of attempt 1 is released (R-AWAKE-01).
- **Assert:** event sequence per variant; INV-3 at 1 Hz through the thaw; archive of attempt 1 ends at the last
  pre-requeue sequence; `corral_lease_expiries_total` +1 for (b) and (c); no hold for attempt 1 after thaw.
- **Pass:** all assertions and §2. **Fail:** any duplicate live attempt (S1), attempt 1 still running after its first
  heartbeat on thaw (S1), wrong state (S2).
- **Blast radius:** the `laptop` emulated host only.
- **Cleanup:** thaw; cancel remaining jobs; verify INV-6.

### CH-02 Network partition

- **Inject (sandbox):** in the fault proxy / `laptop` namespace drop all traffic to `server` for (a) 30 s, (b) 5 min,
  (c) 20 min; (d) asymmetric: worker → server works, server → worker dropped; (e) partition only the `wait` follower;
  (f) issue a cancel during (b).
- **Inject (live):** a host firewall rule dropping only the corral ports from one worker's overlay address, never the
  whole interface and never the out-of-band management path.
- **Expect:** the job keeps running; logs spool locally (§8 PROTOCOL) and are delivered in order after heal
  (R-IO-03). (a) no state change. (b) `orphaned` then `running`. (c) as CH-01 (c) (R-QUEUE-06). (d) heartbeat
  replies lost → treated as a failed heartbeat; same outcomes as (b). (e) `wait` resumes without duplicate or missing
  lines and exits with the job's code (R-IO-04). (f) cancel delivered by the next heartbeat reply after heal, within
  10 s; job `cancelled`.
- **Assert:** archive byte-identical to the worker's local file (a, b, d); event sequences; cancel latency after heal.
- **Pass:** all. **Fail:** lost or duplicated log bytes (S2), duplicate attempt (S1), cancel lost (S2).
- **Blast radius:** one emulated host's corral traffic.
- **Cleanup:** remove the drop rule (dead-man undo also scheduled); INV-6.

### CH-03 NATS restart

- **Inject (sandbox):** `kill -9` nats-server mid-job, restart after (a) 5 s, (b) 60 s, (c) 5 min (> `lease_ttl`);
  (d) restart during a log flood (CH-16).
- **Inject (live):** stop the NATS service through its supervisor; the supervisor restarts it.
- **Expect:** clients reconnect; no duplicate attempt (R-OPS-01); logs byte-identical (R-IO-03); `wait` continues and
  exits with the job's code; (c) the scheduler treats the outage like its own downtime for lease purposes (no mass
  orphaning, R-OPS-03); `/healthz` red during, green after.
- **Assert:** INV-3; archive comparison; no `orphaned` event in (c); JetStream streams and consumers intact.
- **Pass:** all. **Fail:** orphaning caused only by the NATS outage (S2; see open question T-Q3), log loss (S2),
  duplicate attempt (S1).
- **Blast radius:** the bus; all hosts see a disconnect.
- **Cleanup:** NATS running; consumers present; INV-6.

### CH-04 Scheduler crash and SQLite recovery

- **Inject (sandbox):** `kill -9` the scheduler at named crash points (test build, §10): after the claim transaction
  commits but before the reply; after a state write but before its event publish; mid retention GC; mid artifact
  upload. Also (e) down for 5 min with jobs running; (f) host power-cut emulation: freeze, copy the data dir as it is
  (WAL included), restart from the copy.
- **Inject (live):** kill the scheduler process through its supervisor (by unit, never by name); the supervisor
  restarts it.
- **Expect:** every acknowledged transition survives (R-QUEUE-02); a claim whose reply was lost is not handed to a
  second worker, and the first worker's retry either gets the same attempt or the attempt expires by lease rules
  (R-SCHED-06, R-OPS-01); leases extended by the downtime (R-OPS-03); SQLite opens cleanly from the WAL; migrations
  are never re-applied.
- **Assert:** INV-8 (`integrity_check` = ok); INV-3; job count and states equal to the pre-crash acknowledged set; no
  `orphaned` in (e).
- **Pass:** all. **Fail:** lost acknowledged transition (S1), double claim (S1), corrupt DB (S1), mass orphaning
  (S2).
- **Blast radius:** scheduler state of the sandbox.
- **Cleanup:** scheduler running; temp copies removed.

### CH-05 Clock skew

- **Inject (sandbox):** run a worker under a time-offset shim (for example `libfaketime`) at −1 day, −10 min,
  +10 min, +1 day; separately step the **scheduler's** wall clock +1 h and −1 h (container with a faked clock).
- **Expect:** worker skew changes nothing: lease timing, token expiry and retention use the scheduler's clock only
  (PROTOCOL.md §6.3, SPEC.md §12 "Clock skew"). Scheduler wall-clock step: lease expiry uses a monotonic clock so a
  step does not orphan running attempts; displayed timestamps follow the wall clock. (The monotonic rule is not yet
  written in the spec: open question T-Q1.)
- **Assert:** no `orphaned`/`expired` caused by the skew; capability tokens valid for their nominal lifetime measured
  on the scheduler; log timestamps in the archive are the worker's, marked as such.
- **Pass:** all. **Fail:** state change caused by skew (S2).
- **Blast radius:** sandbox clocks only. Live mode: **not allowed** (changing a real host clock affects everything
  on it).
- **Cleanup:** remove shims.

### CH-06 Disk full

- **Inject (sandbox):** size-limited filesystems (loop device or tmpfs with a size) for (a) the scheduler data dir,
  (b) the JetStream store, (c) a worker's work dir and log spool; fill to 100% with a test file during a job.
- **Expect:** (a) the scheduler refuses new uploads and submits with a reason, runs GC, alerts; it never replies
  "ok" to a transition it could not persist (R-QUEUE-02); `/healthz` red. (b) publishes fail; the worker keeps
  spooling locally and resumes after space returns (R-IO-03). (c) `disk_free` refuses new claims with a reason
  (GUARDS.md §7.1); a running job that cannot write fails by its own exit code (`failed`, not `killed`); spool overflow
  is recorded as `log_dropped_bytes` with the drop note.
- **Assert:** every API reply after the fill is either a persisted success or an error; INV-8 after space returns;
  waiting reason `disk_free`.
- **Pass:** all. **Fail:** acknowledged-but-unpersisted transition (S1), scheduler crash loop (S2), silent log loss
  (S2).
- **Blast radius:** the size-limited mounts only. Live mode: fill only a dedicated test quota or loop file, never the
  real filesystem.
- **Cleanup:** delete the fill file; unmount; INV-6.

### CH-07 Memory spike past a job's budget

- **Inject (sandbox):** a job declared `--ram 64M` that allocates (a) 256 MB steadily, (b) a 1 GB burst in 1 s,
  (c) through a grandchild that called `setsid`.
- **Expect:** `killed:ram_exceeded` within 2 guard samples of crossing `budget × slack` (R-GUARD-06); never requeued
  (R-QUEUE-04); `wait` exits 125 with the reason; one log line, one metric, one notification (R-GUARD-08); (c) on
  Linux caught through the cgroup; on macOS caught by the ppid walk unless reparented between samples (documented
  residual risk, SPEC.md §12; record the result, do not fail the run on macOS (c)).
- **Assert:** kill latency from the harness's own footprint samples; INV-2; INV-5 on the emulated host.
- **Pass:** all. **Fail:** not killed (S1), requeued (S1), wrong reason (S3).
- **Blast radius:** a VM or cgroup with a fixed memory size well above the burst; the burst never exceeds it.
- **Cleanup:** INV-6.

### CH-08 System memory pressure from a non-corral process

- **Inject (sandbox):** in the memory-limited emulated host, a test-started hog that is (a) unmatched, (b) matched
  at tier 1, (c) matched by a `never` rule; it grows to cross `closed`, then `pressure`, then `shed`, then
  `critical`, with a pausable corral job running (tier 0) and a `protected` unit present.
- **Inject (live):** a hog sized so that the host stays below its ceiling minus 5 points without corral load; the
  harness aborts the hog if used memory reaches ceiling minus 2 points.
- **Expect:** `closed`: admission refused with the level as reason (no kill). `pressure`: the pausable job is
  SIGSTOPped (note on the `sys` stream) and resumed when clear, or escalated to `killed:system_memory` after
  `pause_max`. `shed`: tier 0 killed first, settle, re-sample, tier 1 only if still at `shed` (R-GUARD-02).
  `critical`: everything except `never`, including `protected`; restore hooks run (R-GUARD-07). (a) and (c): the hog
  itself is never signalled. Action within 2 samples of each crossing (R-GUARD-09).
- **Assert:** sequence of guard actions matches the expected tier order; `guard explain` shows the inputs; INV-1
  (hog in (a)/(c), `never` sentinel); INV-2.
- **Pass:** all. **Fail:** a `never` or unmatched process signalled (S1), wrong tier order (S2), late action > 2
  samples (S2).
- **Blast radius:** the emulated host's memory limit. Live: bounded hog with abort as above.
- **Cleanup:** stop the hog (by the pid the harness recorded); INV-6.

### CH-09 Thermal throttle

- **Inject (sandbox):** `FakeThermalProbe` through the probe-override socket: macOS states `fair` → `serious` →
  `critical`; Linux 84 → 86 → 93 → 96 → 99 °C. No real heat is generated.
- **Expect:** `closed` refuses admission with the temperature; `pressure` pauses pausable jobs for up to the thermal
  `pause_max` (10 min) then `killed:thermal`; `shed` and `critical` as CH-08 with reason `thermal`; the `cooldown`
  check delays the next exclusive job until nominal or `cooldown_cap`.
- **Assert:** actions and reasons; pause duration; cooldown wait reason.
- **Pass:** all. **Fail:** as CH-08.
- **Blast radius:** none beyond the sandbox. Live mode: only the probe override, never a real load to heat a host.
- **Cleanup:** clear the override.

### CH-10 User returns mid-exclusive job

- **Inject (sandbox):** a `measurement` job running with `on_user_input = continue`, then `pause`; `FakeIdleTracker`
  reports real input at t+30 s; separately a synthetic-input stamp at the same moment; separately a process matching
  the `no_game` pattern (a stand-in binary, not a real game) starts mid-run; a second measurement job is queued
  behind the first.
- **Inject (live):** the owner (or a test account on a dedicated test machine) touches the input device; the harness
  never injects input into a real user session.
- **Expect:** `continue` → job completes, `finished.disturbed = true`; `pause` → SIGSTOP until idle again within the
  timeout; synthetic stamp → not counted (R-ADM-07); the queued job does not start while real input happened within
  stage 2 (120 s); game stand-in: the next job is blocked by `no_game` with a reason. Whether a running exclusive job
  reacts to `no_game` is not specified (open question T-Q2).
- **Assert:** `disturbed` flag; SIGSTOP/SIGCONT notes; waiting reasons; timing of the second start.
- **Pass:** all. **Fail:** synthetic input counted (S2), second job starts during stage 2 window (S2).
- **Blast radius:** sandbox. Live: owner present, test job only.
- **Cleanup:** stop stand-ins by recorded pid; INV-6.

### CH-11 Lock holder dies

- **Inject (sandbox):** (a) `kill -9` the worker (and its job group) holding host lock `emulator`; (b) `kill -9` a
  reference-implementation (non-corral) holder; (c) two sessions race for the lock: `corral run --lock` and the
  legacy tool, 100 rounds with random delays; (d) the holder's child re-enters with `CORRAL_LOCK_TOKEN`.
- **Expect:** (a) lock stale after `ttl_s`; the next corral attempt takes it over, one `takeover` log line, the
  lock's `on_takeover` hook runs (R-LOCK-03, R-LOCK-02). (b) no takeover; decline with the holder description and
  one notification (R-LOCK-02). (c) never two holders (R-LOCK-01). (d) re-entry without blocking (R-LOCK-04).
- **Assert:** lock file content over time; takeover log; notification count.
- **Pass:** all. **Fail:** two holders (S1), takeover of a live lock or of a non-corral stale lock without opt-in
  (S1).
- **Blast radius:** temp lock dir.
- **Cleanup:** remove temp lock files; INV-6.

### CH-12 Duplicate delivery and stale attempt tokens

- **Inject (sandbox):** a test client holding the worker's test identity (a) replays captured heartbeats, events and
  log chunks with a revoked token; (b) re-publishes log chunks with the same `Nats-Msg-Id`; (c) the scheduler is
  killed after writing a log chunk but before acking it, so JetStream redelivers; (d) a duplicate `finished` event;
  (e) a payload `from` that disagrees with the subject.
- **Expect:** (a) `stale_lease`, nothing applied (R-QUEUE-06); (b) and (c) archive unchanged by the duplicate
  (R-IO-03); (d) idempotent, one terminal transition (R-QUEUE-03); (e) `forbidden` (R-SEC-03).
- **Assert:** archive byte-identical; event history has one terminal event; rejection counters.
- **Pass:** all. **Fail:** any stale message applied (S1), duplicate bytes (S2).
- **Blast radius:** sandbox NATS account.
- **Cleanup:** none beyond INV-6.

### CH-13 Artifact upload interruption

- **Inject (sandbox):** the HTTP fault proxy (a) resets the connection after 50% of an artifact upload, (b) stalls it
  beyond the token lifetime; (c) the worker is killed mid-upload; (d) the scheduler restarts mid-upload.
- **Expect:** no partial artifact is ever served: the scheduler verifies size and sha256 before an upload becomes
  visible; v1 restarts uploads from zero (no Range, SPEC.md §13); the job's final state comes from its exit code, and
  the `artifacts` event records `uploaded`, or `refused` with a reason after the retry budget; (b) the worker gets a
  fresh token through its heartbeat/claim path or records the failure; (c) the attempt is `lost_local` on worker
  restart and handled by the requeue rule; partial upload files are removed by GC.
- **Assert:** `GET artifacts` returns either the full verified archive or 404; no orphan partial files after GC; job
  state.
- **Pass:** all. **Fail:** partial artifact served (S1), job state wrong because of an upload failure (S2). Retry
  budget and token refresh for uploads are not yet specified (open question T-Q4).
- **Blast radius:** sandbox HTTP only.
- **Cleanup:** GC run; INV-6.

### CH-14 Guard crash

- **Inject (sandbox):** `kill -9` the guard while (a) a job runs, (b) no job runs, (c) the kill switch is on, (d) a
  pressure pause is in effect (job SIGSTOPped); start a second guard while the first runs.
- **Expect:** (a) the worker notices within 5 s, applies its backstop (budget × 1.25, system ceiling) and refuses
  new heavy claims with a reason; the supervisor restarts the guard; the guard re-adopts registrations validated by
  pid start time (GUARDS.md §6, SPEC.md §12). (b) `corral admit` exits 5; shims print `refused: guard unreachable`
  and exit 1 (fail closed). (c) the kill switch is still on after restart (R-GUARD-05). (d) the paused job is
  resumed or escalated by the restarted guard; it is never left stopped forever. Second guard exits "already
  running" (R-GUARD-01).
- **Assert:** worker backstop log line; claim refusals while the guard is down; adopted registrations list; kill
  switch state; paused job's state after restart.
- **Pass:** all. **Fail:** admission granted while the guard is down (S1), kill switch lost (S1), job left stopped
  (S2).
- **Blast radius:** one emulated host.
- **Cleanup:** guard running; INV-6.

### CH-15 Kill switch

- **Inject (sandbox):** `corral guard stop --reason chaos` with a running job, a running `corral run` unit, a
  `never` sentinel and a `protected` unit; then restart the guard; then `corral admin kill-switch on` from `server`
  while `gpu-box` is offline; bring `gpu-box` back; `resume`.
- **Expect:** all non-`never` watched units killed (protected included), jobs `killed:kill_switch`, never requeued;
  restore hooks run; admission refused with `kill switch on (since, by, reason)` across guard restart and host
  restart (R-GUARD-05); the offline guard applies it on reconnect (sticky); `resume` clears it; notifications for on
  and off.
- **Assert:** INV-1 (`never` sentinel alive), INV-2; refusal text; restore hook records.
- **Pass:** all. **Fail:** any unit survives that should die (S1), a `never` unit signalled (S1), switch lost (S1).
- **Blast radius:** sandbox hosts.
- **Cleanup:** `resume` on every guard; INV-6.

### CH-16 Log flood

- **Inject (sandbox):** a job writes 1 GB of output in 60 s (mixed stdout/stderr), with a follower attached and with
  NATS restarted once during the flood.
- **Expect:** worker memory stays within its budget (spool on disk); output beyond `log_spool_max` unsent bytes is
  replaced by one drop note and counted; archive complete otherwise; follower keeps up or falls back to the archive;
  scheduler stays within its memory limit.
- **Assert:** worker and scheduler RSS; `log_dropped_bytes`; archive sizes; INV-5.
- **Pass:** all. **Fail:** worker or scheduler OOM or killed (S1), silent loss (S2).
- **Blast radius:** sandbox disks (size-limited).
- **Cleanup:** remove archives; INV-6.

### CH-17 Worker crash and host reboot mid-job

- **Inject (sandbox):** `kill -9` the worker only (job keeps running) and, separately, "reboot" the emulated host
  (kill every process in its cgroup, restart its units).
- **Expect:** worker-only crash: on restart the worker finds the live attempt; behaviour follows the spec's restart
  rule (adopt if the process group is alive and the token is current, else report); host reboot: the worker reports
  `finished(lost_local)` and the requeue rule applies (PROTOCOL.md §5); never two attempts (R-OPS-01); keep-awake
  holds and locks are released or expire (INV-6). Adoption of a still-running job after a worker-only crash is not
  spelled out in the spec (open question T-Q5).
- **Assert:** INV-3; event sequence; lock and hold state.
- **Pass:** all. **Fail:** duplicate attempt (S1), orphaned process group left running with no owner (S2).
- **Blast radius:** one emulated host.
- **Cleanup:** INV-6.

### CH-18 Keep-awake provider failure and foreign keep-awake sessions

- **Inject (sandbox):** a stub keep-awake provider that (a) fails its probe, (b) dies mid-hold, (c) is slow; a
  foreign keep-awake process (same binary name as the provider's tool, started by the harness as "the user's
  session") runs throughout.
- **Expect:** (a) heavy jobs refused with `keepawake_unavailable` where required (R-AWAKE-03); (b) the hold is
  re-taken on the next renewal or the job continues with a `sys` note (no kill); (c) renewals do not leave a gap
  (start new, then end old); the foreign process is never signalled, read or changed (R-AWAKE-02).
- **Assert:** INV-1 for the foreign process; hold timeline; static check U-15.
- **Pass:** all. **Fail:** foreign keep-awake process touched (S1).
- **Blast radius:** stub provider. Live: only the real provider's hold commands for a test owner name; never a
  command that ends or lists-and-kills all keep-awake processes.
- **Cleanup:** release test holds by name; INV-6.

### CH-JP-01..04 Job profiles (draft, round 3)

| ID | Inject | Expect |
|---|---|---|
| CH-JP-01 | Kill a launcher mid-instance | the job ends `lost` (exit 125) and never re-runs on its own; the service manager restarts the launcher; INV-1 |
| CH-JP-02 | Fill a runner workspace past `max_gb`, and a shared cache store past its limit | instance killed `disk_exceeded`; GC shrinks the store; the host volume keeps its free-space floor |
| CH-JP-03 | Flood the installer inbox with 1,000 junk requests | each refused cheaply; installed profiles unaffected |
| CH-JP-04 | Authorization server down during a profile request, with profiles installed | `profiles request` exits 75; installed profiles keep running (offline verification) |

---

## 7. Running chaos safely

### 7.1 Sandbox defaults (no approval needed)

- All processes run under the harness's own process tree and cgroup; the guard's sandbox scope (§10) limits matchers
  to that tree.
- NATS on a random loopback port with a per-run operator, account and nkeys; scheduler and guard state in a temp dir
  deleted at the end (kept on failure for inspection, then deleted after the report).
- Memory faults run in a VM or cgroup with a fixed limit; the harness refuses to start a memory case if the real host
  is above 70% used, and aborts it if the host reaches its own guard's `closed` level.
- Network faults only through the fault proxy or inside the emulated host's network namespace.
- No real input injection, no real clock changes, no real sleep, no real thermal load.

### 7.2 Live mode (opt-in, per run)

Live mode runs a chaos case against a real installation. It is off unless **all** of these hold:

1. The owner approved **this run** (case ID, target host, time window) in writing; the approval reference is stored
   in the run record. An approval covers one run, not a series.
2. The harness is started with `--live --target <host> --case <CH-nn> --approval <ref>`; without all four it
   refuses.
3. A pre-flight passes: the target host's guard is in `enforce` (or `observe` when that is what is being tested),
   memory below the case's start threshold, no exclusive job running, the out-of-band management path to the target
   works (for network cases), and a dead-man undo is scheduled on the target.
4. Only test jobs, test identities and test lock names (`chaos-*`) are used. Never a user's own processes, apps,
   devices or sessions; never their keep-awake session.

### 7.3 Dead-man undo and abort

- Before any fault that can cut the controller off, schedule its undo **on the target** (a transient timer or a
  one-shot job that fires even if every link is lost). Cancel it only after recovery is confirmed.
- Abort conditions (harness stops injecting and runs the undo): any INV violation, used memory at ceiling minus 2
  points on any real host, loss of the out-of-band path, an unexpected process death outside the test scope, or the
  case exceeding twice its expected recovery time.
- Undo actions signal only pids or units the harness recorded; never by name.

### 7.4 Run record

One file per run: case ID, mode (sandbox/live), approval reference (live), configuration diffs (shortened timers),
injection time, recovery time, assertions with evidence (log excerpts with no secrets), invariants, verdict,
severity of any failure, follow-up. Failures are reported before any cleanup that deletes evidence.

---

## 8. Traceability matrix (requirement → tests)

| Requirement | Unit / LC | Integration | E2E | Chaos |
|---|---|---|---|---|
| R-QUEUE-01 | | IT-01 | E-01 | |
| R-QUEUE-02 | | IT-02 | E-14 | CH-04, CH-06 |
| R-QUEUE-03 | U-01 | | | CH-12 |
| R-QUEUE-04 | U-23 | IT-03 | | CH-07, CH-08, CH-15 |
| R-QUEUE-05 | U-20 | IT-04 | | CH-01, CH-02 |
| R-QUEUE-06 | U-25 | IT-05 | | CH-01, CH-02, CH-12 |
| R-QUEUE-07 | U-20 | | | |
| R-QUEUE-08 | | IT-06 | E-12 | |
| R-SCHED-01 | U-02 | | E-03, E-04 | |
| R-SCHED-02 | U-02 | | E-02 | |
| R-SCHED-03 | U-02 | | E-04 | |
| R-SCHED-04 | U-02, U-16 | | | |
| R-SCHED-05 | U-13 | IT-08 | E-08 | |
| R-SCHED-06 | | IT-07 | | CH-04 |
| R-ADM-01 | U-11 | IT-09 | | |
| R-ADM-02 | U-03, U-13 | | | |
| R-ADM-03 | | IT-09 | | |
| R-ADM-04 | U-12 | IT-01 | E-02, E-07 | |
| R-ADM-05 | U-12 | | E-13 | |
| R-ADM-06 | U-21 | IT-10 | E-08 | CH-14 |
| R-ADM-07 | U-10 | | E-07 | CH-10 |
| R-GUARD-01 | | IT-11 | | CH-14 |
| R-GUARD-02 | U-04 | | | CH-08 |
| R-GUARD-03 | U-05 | | | CH-08, CH-15 |
| R-GUARD-04 | U-06, U-15 | | | all (INV-1) |
| R-GUARD-05 | U-28 | | | CH-14, CH-15 |
| R-GUARD-06 | U-07 | IT-03 | | CH-07 |
| R-GUARD-07 | U-09 | | | CH-08, CH-15 |
| R-GUARD-08 | U-23 | IT-03 | E-13 | CH-07 |
| R-GUARD-09 | U-04 | | | CH-07, CH-08 |
| R-GUARD-10 | U-08 | | E-10 | |
| R-LOCK-01 | LC-01, LC-02 | | E-09 | CH-11 |
| R-LOCK-02 | LC-03 | | E-09 | CH-11 |
| R-LOCK-03 | | IT-26 | | CH-11 |
| R-LOCK-04 | LC-04 | IT-26 | | CH-11 |
| R-EXEC-01 | | IT-12 | | all (INV-1) |
| R-EXEC-02 | | IT-13 | | |
| R-EXEC-03 | | IT-14 | | CH-07 |
| R-EXEC-04 | | IT-15 | | |
| R-EXEC-05 | | IT-16 | | all (INV-7) |
| R-EXEC-06 | U-24 | IT-29 | | |
| R-IO-01 | U-18 | | E-01 | |
| R-IO-02 | U-18 | | | all (INV-7) |
| R-IO-03 | U-25 | IT-19 | | CH-02, CH-03, CH-12, CH-16 |
| R-IO-04 | U-21 | IT-17 | E-01 | CH-02, CH-03 |
| R-IO-05 | | IT-18 | E-01 | CH-13 |
| R-IO-06 | U-19 | IT-28 | | |
| R-LEASE-01 | | IT-20 | E-05 | |
| R-LEASE-02 | | IT-20 | | |
| R-AWAKE-01 | U-14 | IT-27 | | CH-01, CH-18 |
| R-AWAKE-02 | U-15 | | | CH-18, all (INV-1) |
| R-AWAKE-03 | U-14 | IT-27 | | CH-18 |
| R-SEC-01 | | IT-21 | | |
| R-SEC-02 | | IT-21 | E-12 | |
| R-SEC-03 | | IT-21 | E-12 | CH-12 |
| R-SEC-04 | U-22 | | | |
| R-SEC-05 | U-17 | IT-22 | | CH-13 |
| R-SEC-06 | U-16 | | | |
| R-OPS-01 | | IT-19, IT-25 | E-14 | CH-03, CH-04, CH-17 |
| R-OPS-02 | | IT-23 | | |
| R-OPS-03 | U-20 | IT-24 | E-14 | CH-03, CH-04 |
| R-OPS-04 | U-16 | | | |
| R-RT-01 | U-26 | | | |
| R-RT-02 | U-26 | | | |
| R-MIG-01 | U-27 | | | CH-14 |
| R-MIG-02 | | IT-30 | E-10 | |
| R-ADM-08 (draft) | U-35 | | E-17 | |
| R-SEC-07 (draft) | U-36 | IT-39 | | |
| R-REXEC-01..02 (draft) | | IT-31, IT-32 | E-15 | |
| R-REXEC-03..05 (draft) | U-33, U-34 | IT-33, IT-34 | | all (INV-7) |
| R-REXEC-06 (draft) | | IT-35 | | |
| R-AGENT-01..05 (draft) | | IT-36, IT-37 | E-16 | |
| R-PLACE-01..07 (draft) | U-29..U-32 | IT-38 | E-15 | |
| R-JPROF-01..03 (draft) | U-JP-01, U-JP-02 | IT-JP-07 | E-JP-01 | SEC-JP-01..09 |
| R-JPROF-04, R-JPROF-07 (draft) | | | | SEC-JP-10..14 |
| R-JPROF-05, R-JPROF-06 (draft) | U-JP-03 | IT-JP-01 | | CH-JP-03, CH-JP-04 |
| R-JPROF-08..11 (draft) | | IT-JP-02..05 | | CH-JP-01, CH-JP-02 |
| R-JPROF-12 (draft) | U-JP-05 | | | |
| R-JPROF-13 (draft) | U-JP-04 | IT-JP-08 | E-JP-01 | SEC-JP-09 |
| R-JPROF-14 (draft) | | IT-JP-06 | | |
| R-JPROF-15..17 (draft) | | IT-JP-12 | | all (INV-7, INV-9) |
| R-JPROF-18 (draft) | U-JP-06 | | | |
| R-JPROF-19..21 (draft) | U-JP-07 | IT-JP-09..11 | | SEC-JP-15..20, CH-JP-02 |

Every requirement has at least one test. Requirements whose only coverage is a unit test with fakes (R-QUEUE-07,
R-SEC-04, R-RT-*) are acceptable because they have no system-level behaviour to break.

---

## 9. Scoring rubric

### 9.1 Severity of a failure

| Severity | Meaning | Examples |
|---|---|---|
| **S1 safety** | A product safety rule broken: OOM caused, killed job retried, foreign process signalled, two holders of a lock, duplicate live attempt, acknowledged state lost, secret leaked, admission granted with the guard down. | INV-1..3, INV-5, INV-7 violations |
| **S2 correctness** | Wrong state, lost or duplicated log bytes, missing waiting reason, wrong timing beyond tolerance, crash loop that recovers. | CH-02 log loss, CH-03 mass orphaning |
| **S3 quality** | Wrong reason string, missing metric or notification, slow but correct recovery (within 2× target). | |
| **S4 cosmetic** | Output formatting, log noise. | |

### 9.2 Test levels

| Level | Meaning | Tests |
|---|---|---|
| **MUST** | Blocks the milestone. | All U, LC, IT; E-01..E-10, E-14; CH-01..CH-04, CH-07, CH-08, CH-11, CH-12, CH-14, CH-15, CH-17 |
| **SHOULD** | Blocks the milestone unless the owner signs a dated waiver. | E-11..E-13; CH-05, CH-06, CH-09, CH-10, CH-13, CH-16, CH-18 |
| **MAY** | Informational. | Live-mode reruns; macOS CH-07 (c) |

### 9.3 Verdict per run

- **Pass:** every MUST passes, every SHOULD passes or has a waiver, no S1 or S2 open.
- **Fail:** any S1 anywhere (including in a SHOULD or MAY test), or any MUST failing.
- A flaky test (passes on retry) counts as failed until its cause is found; retry is allowed only to collect
  evidence.
- Score (for trend only, never a gate): `100 × passed MUST / all MUST`, reported with the open S1..S3 counts.

### 9.4 Milestone exit criteria (SPEC.md §13, GUARDS.md §10.3)

| Milestone | Must pass |
|---|---|
| M1 scheduler + NATS + Linux worker + CLI | U-01, U-02, U-17..U-26; IT-01..IT-07, IT-12, IT-13, IT-16..IT-19, IT-21..IT-25; E-01, E-04, E-06, E-12, E-14; CH-02, CH-03, CH-04, CH-12, CH-17 (Linux) |
| M2 guard in observe | U-04..U-08, U-28; IT-11; E-10 for one week on every host (GUARDS.md §10.3 G0) |
| M3 macOS worker, guard enforcing, shims, keep-awake | U-03, U-09, U-11, U-13..U-15, U-27; LC-01..LC-04; IT-03, IT-08..IT-10, IT-14, IT-15, IT-26, IT-27, IT-30; CH-01, CH-07, CH-08, CH-11, CH-14, CH-15, CH-18 |
| M4 measurement/gui profiles, waits, notifications | U-10, U-12; E-07, E-08, E-09, E-13; CH-09, CH-10 |
| M5 interactive leases | IT-20; E-05 |
| M6 migration of ad-hoc work | E-02, E-03, E-11; CH-05, CH-06, CH-13, CH-16; full matrix green |
| M7 (draft) exec + placement | U-29..U-35; IT-31..IT-35, IT-38; E-15, E-17 |
| M8 (draft) agent jobs | IT-36, IT-37; E-16 |
| Phase-2 auth (draft) | U-36, IT-39 |
| G1 / G2 guard enforce per OS | CH-08 and CH-09 (Linux) or CH-08 and CH-15 (macOS) on that OS, plus the rollback dry run IT-30 |

---

## 10. Test seams the implementation must provide

The rubric needs these hooks; they are compiled in test builds only (`cfg(feature = "test-hooks")`) and are refused
by release builds:

| Seam | Purpose |
|---|---|
| Probe-override socket on the guard | Inject memory, pressure, thermal, idle, power, screen samples into a running guard (CH-08, CH-09, CH-10). |
| `sandbox_scope` for the guard | Restrict every matcher to descendants of one pid or one cgroup; built-in `never` rules still apply. |
| Named crash points (`CORRAL_TEST_CRASH_AT`) | Abort the scheduler or worker at a named point (CH-04, CH-12, CH-13). |
| Injectable clock in daemons | `FakeClock` driven over the test socket for long grace periods in integration tests. |
| Process tags | Every job process group carries `CORRAL_JOB` and `CORRAL_ATTEMPT` in its environment so the harness can check INV-3 without guessing. |

## 11. Open questions (for the owner)

- **T-Q1. Scheduler clock steps.** Should lease expiry use a monotonic clock (proposed) so an NTP step cannot orphan
  running attempts? Needs one line in PROTOCOL.md §6.3.
- **T-Q2. Running exclusive job vs a newly started game/GPU tool.** Proposed: treat like user input (`continue` →
  `disturbed`, `pause` → SIGSTOP). Not specified today.
- **T-Q3. NATS outage and leases.** Proposed: the scheduler extends leases by the time it was disconnected from NATS,
  as it does for its own downtime (R-OPS-03).
- **T-Q4. Artifact upload retries and token refresh.** Proposed: 3 attempts with backoff, a fresh `artifacts:put`
  token from the heartbeat reply when the old one is within 5 min of expiry.
- **T-Q5. Worker-only crash with the job still running.** Proposed: the restarted worker adopts a live process group
  whose attempt token is still current (no new attempt); otherwise kills it and reports `lost_local`.
- **T-Q6. Live-mode tooling.** Build the chaos harness inside this repo (proposed, sandbox only by default), or keep
  live runs entirely in each deployment's private runbooks?
- **T-Q7. macOS CI.** End-to-end on a real macOS runner for the macOS-only paths (proposed), or fake macOS probes on
  Linux only until M3?

## 12. Implementation status (M1)

Unit tests live next to the code (`crates/*/src`, `crates/corral-worker/tests/`); integration tests are
`crates/corral-cli/tests/m1_*.rs` against a real `nats-server`. Each test names the requirement IDs it proves.

| Rubric ID | Where it is covered |
|---|---|
| U-01 | `corral-proto` `state::tests` (every state/event pair; terminal states are final) |
| U-02 | `corral-sched` `tests::claim_selection_table`, `ordering_priority_then_fifo_with_backfill`, `prefer_then_fallback_with_a_fake_clock`, `fifty_parallel_claims_for_ten_jobs_give_ten_distinct_winners` (R-SCHED-06 at the service level) |
| U-03 | `corral-worker` `admission::tests::projection_boundary_is_exact` (worker-side projection; the guard's arrives in M2) |
| U-11 | `corral-worker` `admission::tests::recheck_under_the_lock_declines_and_releases` |
| U-16 | `corral-proto` `config::tests` |
| U-17 | `corral-proto` `token::tests` |
| U-18 | `corral-cli` `snapshot::tests` |
| U-19 | `corral-proto` `progress::tests`, `corral-worker` `logs::tests` |
| U-20 | `corral-sched` `tests::lease_*`, `grace_*`, `not_after_*`, `downtime_extends_leases` |
| U-21 | `corral-cli` `exitcode::tests`, `follow::tests` (the `wait` half; `admit` and `lock` codes arrive with M2 and M3) |
| U-22 | `corral-sched` `tests::require_grant_policy`, `corral-proto` `envelope::tests` |
| U-23 | `corral-worker` `attempt::tests`, `tests/fake_runtime.rs` |
| U-24 | `corral-worker` `tests/fake_runtime.rs::pause_on_exit_pauses_the_worker` |
| U-25 | `corral-sched` `tests::sink_accepts_only_the_next_seq_with_the_current_token`, `corral-worker` `logs::tests` |
| U-26 | `corral-worker` `tests/fake_runtime.rs` |
| IT-01..IT-02, IT-04..IT-06 | `m1_core.rs::submit_with_no_worker_is_fast_and_queued`, `m1_resilience.rs` |
| IT-07 | service level, see U-02 |
| IT-12, IT-13, IT-16..IT-19 | `m1_core.rs`, `m1_resilience.rs::nats_restart_mid_job_loses_no_log_bytes` |
| IT-21..IT-25 | `m1_security.rs`, `m1_resilience.rs` |
| IT-03 (worker watchdog variant) | `m1_core.rs::memory_hog_is_killed_ram_exceeded` (the guard variant is M3) |
| E-01, E-04, E-06, E-12 | approximated in-process (`m1_core.rs`, `corral-sched` `tests::cancel_*`, `lock_none_is_owner_only`); the three-host topology of section 5 is not built |
| CH-* | not automated in M1 |

