# corral: runtimes

Status: **approved by the owner (v0.1, 22-Nov-2026); round-2 drafts excepted.** Companion to `docs/SPEC.md`. v1 ships one runtime,
`native`. The `Runtime` interface is fixed now so that phase-2 cloud runtimes plug in without changing the scheduler,
protocol or CLI. Cloud runtimes are a **TODO list** here (§4), deliberately not designed in detail (owner decision,
22-Nov).

---

## 1. Where a runtime sits

```
scheduler ──claim──▶ worker ──▶ admission (guard) ──▶ Runtime::prepare / start ──▶ process(es)
                        ▲                                    │ stdout/stderr, exit, footprint
                        └──── logs, heartbeats, events ◀─────┘
```

The worker owns the protocol (claims, leases, heartbeats, logs, events) and job semantics (timeout, cancel). A
runtime owns only "how to run this command somewhere and get its bytes and exit status back". One worker process
hosts exactly one runtime; a host that wants two runtimes runs two workers with different names (phase 2: a cloud
worker typically runs on `server`).

## 2. `native` runtime (v1)

Runs the job as a child process of the worker, as the worker's user, on the worker's host.

| Aspect | macOS | Linux |
|---|---|---|
| Process group | `process_group(0)` at spawn; all signals to `-pgid` | same |
| Descendants | ppid walk every guard sample (children that `setsid`) | own cgroup (below) + ppid walk fallback |
| Memory limit | none in the kernel (no memory cgroups); the guard's budget watchdog (`ram × 1.1`) | cgroup v2 child of the worker unit (`Delegate=yes`): `memory.high = ram`, `memory.max = 1.1 × ram`, `memory.swap.max = ram / 2`, `pids.max = 4096`, `oom_score_adj = 800` |
| CPU/IO priority (non-exclusive) | `taskpolicy -b` (background QoS) | `cpu.weight = 50`, `nice 10`, `ionice -c3` |
| Exclusive jobs | normal QoS | `cpu.weight = 100`, normal nice/IO |
| Pause | SIGSTOP / SIGCONT to the group | `cgroup.freeze` |
| Kill | TERM group, `kill_grace` (10 s), KILL | `cgroup.kill` after TERM + grace |
| Environment | clean: configured `PATH`, `HOME`, `LANG`, `TMPDIR=<run>/tmp`, `CORRAL_JOB_ID`, `CORRAL_ATTEMPT`, `CORRAL_RAM_MB`, `CORRAL_WORKER`, `CORRAL_OUT=<run>/out`, the job's `env` | same |
| Work dir | `$HOME/Library/Application Support/corral/{mirrors,runs,leases,spool}` | `<data_dir>/work/...` |
| Input | git: mirror cache + `git worktree add --detach <sha>`; snapshot: download + verify + unpack; none | same |
| Artifacts | after exit: tar + zstd of `artifacts` paths, streamed PUT | same |
| Cleanup | run dir removed after artifacts are acknowledged; kept `keep_failed_runs` (24 h) on failure, within the disk check | same |

Where cgroup delegation is unavailable on a Linux host (non-systemd, or no delegated memory controller), the worker
logs it once and falls back to the macOS model (guard budget watchdog), reporting `cgroup: off` in presence.

Git credentials: whatever git already has on that host. Secrets are never forwarded from the submitter.

## 3. The `Runtime` interface

Rust sketch (final signatures fixed during implementation; semantics are the contract):

```rust
#[async_trait]
pub trait Runtime: Send + Sync {
    /// Static description, sent in claims and presence.
    fn describe(&self) -> RuntimeInfo;               // { name, caps, cost_tier, max_ram_mb, slots }

    /// Optional estimate for placement and budget (phase 2). Native: Some(Cost::zero()).
    async fn estimate(&self, spec: &JobSpec) -> Option<CostEstimate>;   // { per_hour, startup_s, currency }

    /// Acquire whatever the job needs to run (native: run dir, input; cloud: provision/attach a machine).
    /// Must be cancellable and must clean up on drop if start never happens.
    async fn prepare(&self, job: &Attempt, input: InputRef) -> Result<Prepared, RuntimeError>;

    /// Start the command. Returns handles for stdout/stderr byte streams.
    async fn start(&self, p: &Prepared) -> Result<Running, RuntimeError>;   // { out, err, id }

    /// Resource sample for heartbeats and the guard (native: footprint from the guard).
    async fn sample(&self, r: &Running) -> Result<Sample, RuntimeError>;    // { footprint_mb, cpu_s, alive }

    /// Deliver a signal to the whole unit (group / cgroup / remote process tree).
    async fn signal(&self, r: &Running, sig: UnitSignal) -> Result<(), RuntimeError>;  // Term | Kill | Stop | Cont

    /// Wait for exit.
    async fn wait(&self, r: &Running) -> Result<ExitInfo, RuntimeError>;   // { code, signal, by_runtime: Option<Reason> }

    /// Fetch artifact paths as a tar stream.
    async fn collect(&self, r: &Running, paths: &[String]) -> Result<ByteStream, RuntimeError>;

    /// Release everything (native: remove run dir; cloud: destroy the machine). Idempotent; must succeed eventually.
    async fn destroy(&self, p: Prepared) -> Result<(), RuntimeError>;
}
```

Rules every runtime must follow:
- **Kill semantics are unchanged.** If the runtime (or its provider) stops the job, `ExitInfo.by_runtime` names the
  reason and the job ends `killed` (never retried). If the runtime loses the job without knowing its end (provider
  outage, spot reclaim before exit status), the worker reports `lost_local` and the normal fenced requeue applies.
  Spot/preemptible reclaim is therefore `lost`, not `killed`.
- **Destroy is guaranteed.** `destroy` is called on every path, retried with backoff, and recorded in worker state so
  a restarted worker finishes it. A resource the runtime created must never outlive its attempt by more than
  `destroy_deadline` without an alert.
- **Caps are honest.** A runtime only advertises caps it can satisfy for every job it accepts.
- **Admission still applies.** The guard protects the host the worker runs on; a remote runtime adds its own
  admission (quota, budget) in `prepare` and declines through the normal `declined` path with reasons.
- **Logs and exit codes look the same** to `corral wait` whatever the runtime.

`RuntimeInfo.cost_tier`: `0` = owned hardware (no marginal cost), `1` = cheap/elastic (per-second billed), `2` =
expensive or minimum-billed (e.g. a dedicated Mac host with a 24 h minimum). Placement is local-first: tier 0 always
ranks first (SPEC.md §7).

## 4. Phase 2: cloud runtimes (TODO, not designed)

Each item below needs its own short spec and owner approval before work starts.

- [ ] **Budget and cost tiers.**
  - Job field `max_cost_tier` (exists in v1 schema, only `0` accepted) and CLI `--allow-cloud[=TIER]`.
  - `cloud_after` (default 30 min): a job becomes eligible for tier `n > 0` only if no tier-0 worker could take it in
    that time.
  - A budget ledger in the scheduler: per-month cap and per-job cap (Q13); every paid attempt records
    `estimate` at claim and actual cost at destroy; a claim that would exceed the cap is declined with
    `budget: <spent>/<cap>`.
  - Tier ≥ 2 always requires an explicit approval (`cost_tier:2` in `require_grant`), never just a flag.
  - Notifications on every paid attempt start and end with cost.
- [ ] **Modal Sandbox runtime** (tier 1): create sandbox with image + GPU type, streaming exec, file pull, terminate.
  Verify streaming exec semantics and the API surface usable from Rust (HTTP or a sidecar) [unverified].
- [ ] **SSH rented-VM runtime** (tier 1): generic adapter for providers that hand out a VM over REST (RunPod, Vast,
  Lambda, ...): provision via a small per-provider shim, run over SSH with a process group on the remote side,
  stream stdout/stderr, rsync/tar artifacts back, destroy. Startup time is minutes [unverified]; `estimate` reports
  it so placement can prefer waiting for a tier-0 worker.
- [ ] **SkyPilot runtime** (later, tier 1): delegate provisioning to SkyPilot managed jobs; corral keeps queue,
  logs and exit codes. Only if the SSH adapter proves insufficient.
- [ ] **Lease-a-Mac runtime** (tier 2): for `metal`/`macos` jobs when no owned Mac is available, on a dedicated cloud
  Mac host; minimum billing period (24 h on some providers [unverified]) means it is never automatic: explicit
  approval per lease, reuse of the host for queued Mac jobs during the paid window, destroy at window end.
- [ ] **Cloud worker placement:** cloud runtimes run inside a worker on `server` (always on), named e.g.
  `cloud-modal`, with caps from config (`gpu`, `cuda`, `linux`, `x86_64`) and `cost_tier`. No change to claims
  beyond `cost_tier` and `estimate` fields.
- [ ] **Input/output for remote runtimes:** snapshots and artifacts already go through the scheduler's HTTP API;
  remote runtimes pull inputs with a capability token, so no new data path is needed. Large inputs may need a
  direct-to-object-storage path (decide then).
- [ ] **Secrets for cloud providers:** API keys as scheduler-host systemd credentials, scoped to the cloud worker;
  never in job env.
- [ ] **Verify before designing:** Modal Sandbox streaming and pricing; current cloud Mac pricing and minimums;
  SkyPilot's SSH node pools; provider teardown guarantees.

Explicitly not planned: a container runtime (owner decision 3), Kubernetes, a corral-owned VM image pipeline.

## 5. Tests

- A `FakeRuntime` implementing the trait drives worker tests: start, output, exit codes, kill reasons, lost
  (no exit), destroy retries.
- `native` integration tests on macOS and Linux: process-group kill of a forking job, environment, cgroup files on
  Linux, background QoS, artifact collection, cleanup.
- Phase-2 runtimes will ship with a conformance suite run against the trait (same cases as `FakeRuntime`).
