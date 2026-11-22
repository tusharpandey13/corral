# corral: guards, admission, host locks, keep-awake

Status: **approved by the owner (v0.1, 22-Nov-2026); round-2 drafts excepted.** Companion to `docs/SPEC.md` (requirement IDs `R-*` refer
there). This document specifies `corral guard`, the one per-host daemon that replaces a pile of one-off scripts: a
macOS memory killer with kill tiers, a Linux memory/thermal shedder with a protected set and restore hooks, a JSON
lease-file host lock, a measurement queue's idle/AC/lock/media/thermal/TCC checks and device lock, and a keep-awake
script. It also specifies `corral admit` / `corral run` for tools that are not corral jobs, and the migration from
the scripts.

---

## 1. Model

One guard per host. It owns three things:

1. **Protection** (§2-3): sample memory and thermal state; when the host is in danger, pause or kill watched heavy
   processes in tier order, never touching `never` processes and touching `protected` ones only when critical; a kill
   switch; restore hooks; notifications.
2. **Admission** (§4, §7): answer "may this heavy thing start now, and if not, why", for corral workers and for any
   other tool; count heavy slots across everything it watches; evaluate check profiles (idle, AC, lock screen, media,
   thermal, disk, TCC, ...).
3. **Host resources** (§5, §8): named host locks (file-compatible with the legacy lock) and keep-awake holds.

The worker never kills for resource reasons; it asks the guard. The guard never decides job semantics (cancel,
timeout, retry); the worker and scheduler do. They talk over a local Unix socket (§6).

Why one daemon, not a library in the worker: protection must hold for heavy work that corral did not start, on hosts
with no worker, and while the worker restarts or upgrades.

## 2. Signals and levels

### 2.1 Signals per OS

| Signal | macOS source | Linux source |
|---|---|---|
| free memory % | `kern.memorystatus_level` (what jetsam uses) | `MemAvailable / MemTotal` (`/proc/meminfo`) |
| memory pressure | `kern.memorystatus_vm_pressure_level` (1 normal, 2 warn, 4 critical); dispatch-source events as accelerator | PSI `/proc/pressure/memory` `some`/`full` `avg10`; PSI triggers as accelerator |
| swap used | `vm.swapusage` | `SwapTotal − SwapFree` |
| thermal | process thermal state (`nominal`/`fair`/`serious`/`critical`); `pmset -g therm` fallback | configured sensor: hwmon `coretemp` "Package id 0" by default, or a thermal zone; °C |
| per-process memory | `proc_pid_rusage` `ri_phys_footprint` (includes GPU/unified memory that RSS misses) | cgroup `memory.current` when the process has its own cgroup; else `smaps_rollup` PSS |
| process tree | process group + ppid walk each sample (children that `setsid`) | cgroup membership, else process group + ppid walk |

Sampling every `sample` (default 2 s) for memory, 5 s for thermal. Every probe sits behind a trait so tests inject
values (`AGENTS.md`).

### 2.2 Levels

The guard computes one level per sample. Each level has a condition (any listed sub-condition, sustained for
`for_samples`) and an action. Levels are ordered; the highest true level acts.

| Level | Default condition, macOS | Default condition, Linux | Action |
|---|---|---|---|
| `ok` | — | — | none |
| `closed` | free < 40% or swap > 4 GiB or pressure ≥ 2 | MemAvailable < 5 GiB or temp ≥ 85 C | admission refused (no kill) |
| `pressure` | free ≤ 22% or pressure ≥ 2 or swap ≥ 6 GiB, 3 samples | MemAvailable < 2.25 GiB or PSI `full avg10` > 20 or temp ≥ 92 C, 3 samples | pause `pausable` registrations (SIGSTOP group / `cgroup.freeze`); resume when clear (free ≥ 30% and pressure 1 / MemAvailable ≥ 3 GiB) within `pause_max` (60 s memory, 10 min thermal), else escalate those registrations to a kill (`system_memory` / `thermal`) |
| `shed` | free ≤ 20% (the 80% ceiling) or pressure ≥ 4 | MemAvailable < 2 GiB or temp ≥ 95 C | kill **one tier at a time**, lowest first, excluding `protected` and `never`; wait `settle` (8 s) and re-sample after each tier; stop as soon as the level drops below `shed` |
| `critical` | free ≤ 15% or swap ≥ 7 GiB | MemAvailable < 768 MiB or temp ≥ 99 C | kill every watched process except `never`, including `protected`; run restore hooks |
| `stop` | kill switch on | kill switch on | as `critical`, every sample while matches appear; admission refused |

Thresholds are configuration (Q3 in SPEC.md); the defaults above are the values the legacy scripts used, with the
macOS ceiling of 80% used made explicit. Hysteresis: a level clears only after `clear_samples` (default 3) samples
below it.

**Per-registration budgets** are checked every sample regardless of level: footprint > `budget × budget_slack`
(default 1.1) → kill that registration with reason `ram_exceeded` (R-GUARD-06).

## 3. Watched processes: matchers, tiers, protected, never

### 3.1 Matchers

A matcher selects processes and says what to do with them. Fields in `select` must all match.

| `select` field | Meaning |
|---|---|
| `registered` | name glob of a registration (§6): `"corral:*"` for corral jobs, `"run:*"` for `corral run`, `"*"` for all |
| `cmdline` | regex on the full command line |
| `exclude_cmdline` | regex; a match removes the process from this matcher |
| `exe` | regex on the executable path |
| `cwd_prefix` | process working directory starts with this path |
| `user` | owning user name |
| `cgroup_glob` | (Linux) cgroup path glob, e.g. a container runtime's scope units |
| `parent_cmdline` | regex on an ancestor's command line (e.g. a container shim naming a specific runtime socket) |

Other matcher keys:

| Key | Default | Meaning |
|---|---|---|
| `tier` | required | kill order under `shed`; lower dies first. Tier 0 = corral jobs (most deferrable). |
| `heavy` | `true` | counts against the host's heavy slots (one-heavy-at-a-time) |
| `budget` | none | memory budget for each matched unit (registration, cgroup or process group) |
| `pausable` | `false` | may be paused at `pressure` instead of killed |
| `kill` | `"group"` | `group` (TERM to process group + descendants, `kill_grace` 8 s, KILL), `process`, `cgroup` (`cgroup.kill`), `hook` (run a command with the ids) |
| `unit` | `"group"` | what one "unit" is for budgets and logs: `group`, `cgroup`, `process` |
| `notify` | `true` | notify on kill |

### 3.2 Protected and never

- **`[[guard.never]]`**: same `select` syntax. A process matched by any `never` rule is never signalled, at any
  level, by the kill switch included (e.g. the user's own emulator instance, the agent runtime itself, the guard and
  worker). `never` always wins over matchers.
- **`protected`**: a matcher flag, or a `protect` block on a matcher:
  `protect = { resolve_cmd = ["container-cli", "inspect", "-f", "{{.Id}}", "prod-1"], ttl = "60s" }` produces ids
  (pids, cgroup names or container ids) to protect. Protected units die only at `critical` or `stop`.
- **Fail-safe:** if a matcher has a `protect` block and resolution fails or returns nothing, `shed` kills **nothing**
  from that matcher (it logs `shed skipped: protected set unresolved`); `critical` still applies (R-GUARD-03).

### 3.3 Safety rules for signalling (R-GUARD-04, R-AWAKE-02)

- Signal only processes resolved in the current sample; re-check each pid's start time immediately before each
  signal (pid reuse).
- Never kill by name (`pkill`/`killall` do not exist in the code; a static check enforces it).
- Never signal the guard, the worker, the scheduler, NATS, PID 1, session leaders of the user's GUI session, or
  anything matched by `never`. These built-in `never` rules cannot be disabled.
- TERM, `kill_grace`, KILL, always to the unit (group or cgroup).

### 3.4 Kill switch

- `corral guard stop [--reason R]`: writes the persistent `STOP` state (survives restarts and reboots), kills every
  watched non-`never` unit, runs restore hooks, refuses all admission (`refused: kill switch on (since T, by X: R)`),
  and tells the local worker to report affected jobs `killed:kill_switch`.
- `corral guard resume`: clears it. Both are logged and notified. The scheduler's `admin kill_switch` forwards to every
  online guard; offline guards get it on reconnect (sticky until `resume`).

### 3.5 Restore hooks

Configured commands run after `critical`, `stop`, and optionally after `shed` (per hook: `on = ["critical",
"stop"]`). Typical uses on Linux: stop performance-mode units, restore fan control and power limits, tear down a
bench network bridge and its firewall rules. Each hook has `timeout` (default 30 s; killed after), runs with a clean
environment as the guard's user, and its exit code and output tail go to the guard log (R-GUARD-07). Hooks are
idempotent by contract.

### 3.6 Notifications

Through the shared `Notifier` (SPEC.md §11): one message per action, e.g.
`laptop: killed tier 1 (llm-servers: pid 812 llama-server) reason=shed free=19% swap=5.1G`. Rate-limited per
(matcher, reason) to one per 5 min, with a count of suppressed messages.

## 4. Admission for any tool: `corral admit`, `corral run`

### 4.1 `corral admit`

```
corral admit [--name NAME] [--ram 4G] [--class heavy|light] [--profile P] [--reserve 60s] [--json] [--legacy-output]
```

- Asks the local guard (socket) whether a unit of this size and profile may start now.
- Rules (all must hold): kill switch off; level < `closed`; projection `free_pct − ram / total × 100 ≥ admit_floor`
  (Linux: `MemAvailable − ram ≥ admit_floor_mb`, default 20% of RAM) minus live reservations; a free heavy slot
  (counting live heavy matches, registrations and reservations) for class heavy; every check in `--profile` passes.
- Exit 0 and prints `ok` (or `ok reservation=<token> until=<t>`); exit 1 and prints
  `refused: <first reason>` plus `also: <other reasons>`; exit 2 usage; exit 5 guard unreachable (callers must treat it
  as refused; the legacy shims print `refused: guard unreachable` with exit 1).
- **Reservations** close the race between "admitted" and "started": with `--reserve`, the guard holds the RAM and the
  heavy slot for that long or until a registration presents the token (`CORRAL_ADMIT_TOKEN`). Two tools that admit at
  the same moment cannot both pass. Legacy shims reserve 60 s by default.

### 4.2 `corral run`

```
corral run [--name N] [--ram 4G] [--profile P] [--lock NAME] [--tier N] [--budget 5G] [--hold] [--wait 30m] -- CMD...
```

The recommended wrapper for heavy work that is not a queued corral job: admit (with reservation) → acquire lock →
re-check under the lock (§7) → register with the guard (`run:<name>`, tier default 1, budget = `--budget` or
`--ram × 1.1`) → take a keep-awake hold if `--hold` → exec in its own process group → release everything on exit.
`--wait` retries admission and the lock with backoff (1 s doubling to 15 s), printing the current reason each time it
changes. Exit code = the command's; 125 + one line on stderr when the guard killed it (with the reason).

## 5. Host locks

### 5.1 Kinds

| Kind | Semantics | Compatible with |
|---|---|---|
| `lease` (default) | JSON lease file, heartbeat + TTL, takeover rules below | the legacy lock tool (Python and shell) and anything that follows the same file protocol, locally or over ssh |
| `flock` | kernel `flock` on a file, released by the kernel when the holder dies; optional holder JSON file beside it | the legacy measurement queue's device lock (and `lockf -k` users) |

### 5.2 `lease` file protocol (frozen while legacy tools use it)

- File content: JSON `{"agent", "purpose", "apps", "pid", "started", "heartbeat", "ttl_s"}` (times are Unix seconds,
  float). corral may add keys (e.g. `"corral": {"job": "...", "attempt": 1, "token_sha": "..."}`); legacy readers
  ignore them.
- Directory mode 0700, file mode 0600.
- **Acquire:** atomic create (`O_EXCL`). If it exists and is not stale → busy, with the holder description
  (`held by <agent> (<purpose>, apps [...], pid N), heartbeat 20s ago of ttl 300s`). A file that does not parse is
  stale once it has not been modified for 10 s (a holder between `O_EXCL` create and the content write is live).
- **Stale:** `now − heartbeat > ttl_s` (default 300 s).
- **Takeover:** create `<lock>.takeover` with `O_EXCL`; re-read; if the holder is the same and still stale, delete the
  lock and append one JSON line `{"t", "event":"takeover", "by", "from"}` to `<lock>.log`; delete the takeover file;
  retry the create. A takeover file older than 1 min is removed (racer died) and the attempt retried. Up to 3 rounds.
- **Renew:** holder only; rewrite with the new heartbeat via tmp file + rename; every `ttl_s / 3`.
- **Release:** holder only (agent matches); delete.
- corral's agent string: `corral:<job id>` for jobs, `corral-run:<name>` for `corral run`; `pid` = the unit's
  process-group leader.

### 5.3 Takeover policy (R-LOCK-02)

- corral never takes over a live lock.
- Stale lock whose holder agent starts with `corral` → takeover, then the lock's optional `on_takeover` hook runs
  (e.g. stop devices the dead corral holder left running, identified by what it recorded, never by name).
- Stale lock with a non-corral holder → no takeover unless the lock is configured `takeover = "any"`; the job is
  declined with `stale lock held by X; not taking over a non-corral holder` and one notification is sent (Q5).

### 5.4 Re-entry (R-LOCK-04)

The holder exports `CORRAL_LOCK_TOKEN=<random>` to its child processes and records its hash in the guard. A child
calling `corral lock acquire` or `corral run --lock` with that token re-enters (no second acquire, no release on the
child's exit). The legacy flock lock's own token variable is honoured by the `flock` kind when configured
(`reentry_env = "LEGACY_LOCK_TOKEN"`).

### 5.5 Which units take which lock

Per host: `default_heavy_lock = "emulator"` (example). A heavy corral job with `lock = "auto"` takes it; light jobs
do not; `lock = "none"` on a heavy job is `owner`-only (and an approval action later). Profiles can require extra
locks (`lock_free:device` check plus `locks = ["device"]` acquisition).

### 5.6 CLI

`corral lock acquire|wait|renew|release|status [--json]|exec NAME [--agent A] [--purpose P] [--apps a,b] [--ttl 300]
[-- CMD]`. `exec` holds, renews and releases around a command. Exit codes: 0 ok, 1 held (message names holder),
2 usage / not holder.

## 6. Local guard socket protocol

Unix domain socket `$RUNTIME_DIR/corral/guard.sock` (Linux system service: `/run/corral/guard.sock`, group
`corral`; macOS: under the user's runtime dir), mode 0660. Peer credentials (`SO_PEERCRED` / `LOCAL_PEERCRED`)
identify the caller's uid; only the guard's own uid, root, and members of the configured group may call. Framing:
newline-delimited JSON, every message has `"v": 1`.

| Request | Body | Reply |
|---|---|---|
| `admit` | `{name, ram_mb, class, profile, reserve_s}` | `{ok, reservation?, until?, reasons: [{check, reason, value, since}]}` |
| `register` | `{name, pgid, pid_start, tier?, budget_mb?, pausable, heavy, reservation?, lease_token?}` | `{ok, id}` |
| `unregister` | `{id}` | `{ok}` |
| `checks` | `{profile, stage}` | `{ok, results: {name: {ok, reason, value, since}}}` |
| `lock.acquire` / `lock.renew` / `lock.release` / `lock.status` | `{lock, agent, purpose, apps, pid, ttl_s, token?}` | `{ok, holder?, reason?}` |
| `hold` / `release_hold` / `holds` | `{owner, for_s, bound_pid, display}` | `{ok, until}` |
| `status` | `{}` | level, metrics, registrations, reservations, kill switch, last actions |
| `stop` / `resume` | `{by, reason}` | `{ok}` |
| `subscribe` | `{topics: ["actions", "level"]}` | stream of `{"v":1, "event":"kill"|"pause"|"resume"|"level", "id", "name", "reason", "detail", "t"}` |

The worker keeps a `subscribe` stream open; that is how it learns that the guard paused or killed one of its jobs and
why (R-GUARD-08). Registrations persist in the guard's state file and are re-validated by pid start time after a guard
restart; a worker that reconnects re-sends its live registrations (idempotent by `(pgid, pid_start)`).

## 7. Checks, profiles, re-check under lock, explainable waits

### 7.1 Checks

Each check returns `{ok, reason, value, since}`; `reason` is a short human string with the measured value and the
threshold (`idle 40s (need 300s)`). Each check declares `on_unknown`: `pass` (with reason `unknown (<why>)`) or
`fail`.

| Check | macOS probe | Linux probe | on_unknown |
|---|---|---|---|
| `kill_switch_off` | guard state | guard state | — |
| `memory` | level + projection (§4.1) | same | fail |
| `thermal_ok` | thermal state not `serious`/`critical` | sensor < `admit_max_c` (85 C) | pass |
| `disk_free` | `statfs` on work dir ≥ `min_disk` (15 GB) | same | fail |
| `ac_power` | power source (IOKit power sources / `pmset -g batt`) | `/sys/class/power_supply/*/online`; desktops without battery pass | pass |
| `no_lpm` | low power mode off | `power-profiles-daemon` profile ≠ `power-saver` | pass |
| `screen_unlocked` | `CGSessionCopyCurrentDictionary` → `CGSSessionScreenIsLocked` (no helper binary); screensaver process present → reason `locked(shield?)` | logind `LockedHint` of the active graphical session; headless → pass | pass |
| `user_idle` | `IOHIDSystem` `HIDIdleTime`, minus synthetic input (§7.3) | logind `IdleHint`/`IdleSinceHint`; X11 screensaver extension if present; headless → pass | fail |
| `no_media` | `pmset -g assertions`: foreign `PreventUserIdleDisplaySleep`/`NoDisplaySleepAssertion` holders, excluding keep-awake tools and zombies | idle inhibitors from `systemd-inhibit --list`; MPRIS `PlaybackStatus=Playing` over D-Bus if a session bus exists | pass |
| `no_dialog` | security/auth dialog processes that own an on-screen window (window list, never prompts) | — (pass) | pass |
| `no_game` | configured process patterns | same | pass |
| `no_foreign_gpu` | configured patterns of GPU/display tools running outside the lock | same | pass |
| `gpu_cool` | optional external sensor tool; ≤ `max_c` at start | hwmon / vendor tool | pass |
| `lock_free:<name>` | named host lock free (or held by us) | same | fail |
| `keepawake_ok` | provider available (§8) | same | fail where required |
| `tcc_input` | Accessibility (`AXIsProcessTrusted`) + Automation probe with a hard timeout | — | fail |
| `tcc_capture` | `CGPreflightScreenCaptureAccess` (never prompts) | — | fail |
| `queue_not_paused` | worker/queue pause state with its reason | same | — |
| `cooldown` | since the last exclusive job: thermal state nominal and GPU/CPU temperature ≤ `cooldown_max_c`, at most `cooldown_cap` (180 s) | same | pass |

TCC failures are host problems that will not fix themselves: the worker marks `gui_ok = false` with the reason,
keeps serving other jobs, and notifies once. TCC grants attach to the launchd program, i.e. the `corral` binary;
releases are signed with one stable identity (SPEC Q7).

### 7.2 Profiles

| Check | `default` | `measurement` | `gui` |
|---|---|---|---|
| kill_switch_off, memory, thermal_ok, disk_free | ✓ | ✓ | ✓ |
| ac_power | heavy jobs on laptops (Q8) | ✓ | ✓ |
| no_lpm, user_idle (two-stage), no_media, no_dialog, no_game, no_foreign_gpu, gpu_cool, cooldown | | ✓ | |
| screen_unlocked | | ✓ | ✓ |
| tcc_input / tcc_capture | | | when caps include `tcc-input` / `tcc-capture` |
| lock_free:<configured> | | ✓ | ✓ |

**Light class (draft, round 2; SPEC R-ADM-08):** light work (class `light` jobs, light exec sessions, agent jobs'
own admission) checks only `kill_switch_off` and `memory` (projection under the ceiling) plus a free light (or
`agent`) slot, whatever the job's profile; it runs while the user is active and on battery.

Custom profiles are lists of checks with parameters (SPEC.md §9). A legacy "dev" mode (skip idle and media) is the
`default` profile plus `lock_free`.

### 7.3 Idle, two stages, synthetic input

- **Stage 1** (`measurement`): idle ≥ 300 s gates the claim (or `corral admit`).
- **Stage 2:** idle ≥ 120 s, re-checked **under the lock** right before each run; a drained series of measurement
  jobs does not wait 5 min between runs, but real input in the last 2 min still blocks the next start.
- **Synthetic input exemption:** tools that inject input (UI automation) call `corral guard stamp-synthetic` (or
  touch the configured stamp file) right before injecting; the guard also records each exclusive job's run window.
  A HID idle reset within 2 s of a stamp or inside a window is ignored; the guard keeps "last real input" in its
  state, so effective idle = now − last real input.
- `on_user_input` for exclusive jobs: `continue` (mark `disturbed`) or `pause` (SIGSTOP until idle again, within
  the timeout).

### 7.4 Re-check under the lock (R-ADM-01)

Order for every heavy start (corral job, lease step, `corral run`):

1. Continuous evaluation (presence; cheap probes every 15 s) decides whether to claim/admit at all.
2. Admission with reservation (§4.1).
3. Lock acquire, non-blocking for workers (busy → decline with holder description).
4. **Re-run the full profile and the memory projection while holding the lock**, including stage-2 idle.
5. Failure → release lock and reservation → decline with reasons. Success → register, take keep-awake hold, start.

Declines never count as attempts and always update the waiting reasons.

### 7.5 Explainable waits

- Every refusal returns the full list of failing checks with `since`; the first is the headline.
- Presence carries all check results; the scheduler composes per-job, per-worker waiting text (SPEC.md §7).
- Local tools get the same text from `corral admit` / `corral run --wait`.
- `corral guard status` shows level, signals, registrations, reservations, locks, holds, kill switch;
  `corral guard explain` shows the last 20 decisions with inputs.
- A block over `notify_after` (30 min) notifies once per (job or caller, reason).

## 8. Keep-awake providers

```rust
trait KeepAwake {
    fn name(&self) -> &str;
    fn probe(&self) -> Result<(), Reason>;                                 // available and working?
    fn hold(&self, owner: &HoldName, dur: Duration, bound_pid: Option<Pid>, display: bool) -> Result<HoldInfo>;
    fn release(&self, owner: &HoldName) -> Result<()>;                      // idempotent
    fn list(&self) -> Result<Vec<HoldInfo>>;                                // only corral's holds: the provider lists every owner's, corral keeps the `corral:` prefix
}
```

Contract for every provider:
- Holds are **named** (`corral:<job id>:<attempt>` for jobs and leases, `corral:run:<name>` for `corral run`: every name starts with `corral:`, which is how corral tells its holds from anyone else's), **time-bounded** (expire by themselves even if
  corral and every state file vanish), renewable without a gap (start the new hold, then end the old), and optionally
  bound to a pid.
- A hold never reads, changes or ends a keep-awake **session** the user started, and never changes lock-screen,
  screen-saver or lid settings.
- Only pids recorded by the provider itself, with a matching start time and expected command line, are ever
  signalled (R-AWAKE-02).
- Window: `min(timeout, 2 h) + 10 min`, renewed every 30 min while the unit runs; released on every exit path.

| Provider | OS | Mechanism |
|---|---|---|
| `hold-cli` | macOS (any) | an external command implementing the hold contract: `hold --for D --owner N [--pid P] [--display]` (repeating it renews; prints `hold N until T (pid P)`), `release --owner N` (idempotent, exit 0 with no hold), `holds --json` (`[{owner, until, pid, started, display, bound_pid}]`, epoch seconds, **every** owner's holds). Exit codes: 0 ok, 1 provider failure (caffeinate, state or lock), 2 usage error, duration over the provider's maximum, or a dead `--pid`. corral never uses `release --all`. The command is configured (`command`, default `awake` on `PATH`); service environments have a minimal `PATH`, so deployments give an absolute path. |
| `caffeinate` | macOS | corral starts `caffeinate -i -s [-d] -t <secs> [-w <pid>]` itself, records pid + start time, terminates only that pid on release |
| `systemd-inhibit` | Linux | `systemd-inhibit --what=idle:sleep --who=corral --why=<name> --mode=block sleep <secs>` as a recorded child |
| `none` | any | always-on servers |

Lid close: no provider blocks lid-close sleep in v1; a sleeping laptop's attempts become `orphaned` and resume or
requeue (PROTOCOL.md §5). If a host sets `keepawake_required_for = ["heavy"]` and the provider probe fails, heavy
work is refused with `keepawake_unavailable` (R-AWAKE-03).

## 9. Configuration reference (guard part)

```toml
[guard]
mode = "enforce"               # enforce | observe (log decisions, signal nothing: R-GUARD-10). Observe is the default; enforce is opt-in per host
sample = "2s"
socket = "/run/corral/guard.sock"     # default: the runtime directory (§6)
pause_max = "60s"; pause_max_thermal = "10m"; clear_samples = 3
disk_path = "/var/lib/corral"; min_disk = "15G"; admit_max_c = 85   # disk_free and thermal_ok checks
kill_grace = "8s"
settle = "8s"
budget_slack = 1.1
heavy_slots = 1
notify_after = "30m"
state_dir = "/var/lib/corral/guard"   # STOP state, registrations, last real input

[guard.levels.macos]           # defaults shown; any key may be overridden
closed   = { free_pct_below = 40, swap_above = "4G", pressure_at_least = 2 }
pressure = { free_pct_at_most = 22, pressure_at_least = 2, swap_at_least = "6G", for_samples = 3 }
shed     = { free_pct_at_most = 20, pressure_at_least = 4 }
critical = { free_pct_at_most = 15, swap_at_least = "7G" }
admit_floor_free_pct = 23

[guard.levels.linux]
closed   = { available_below = "5G", temp_at_least = 85 }
pressure = { available_below = "2304M", psi_full_avg10_above = 20, temp_at_least = 92, for_samples = 3 }
shed     = { available_below = "2G", temp_at_least = 95 }
critical = { available_below = "768M", temp_at_least = 99 }
admit_floor = "20%"
temp_sensor = { hwmon = "coretemp", label = "Package id 0" }

[[guard.matchers]]
name = "corral-jobs";    select = { registered = "corral:*" };  tier = 0; pausable = true
[[guard.matchers]]
name = "ad-hoc-runs";    select = { registered = "run:*" };     tier = 1
[[guard.matchers]]
name = "llm-servers";    select = { cmdline = 'llama-server|llama-cli|mlx_lm\.(server|generate)' }; tier = 1
[[guard.matchers]]
name = "bench-emulators"; select = { cmdline = 'qemu-system.*(-avd bench-|@bench-)' }; tier = 2
[[guard.matchers]]
name = "test-suites";    select = { cmdline = 'python.*-m unittest', cwd_prefix = "/home/user/src/example-app" }; tier = 3

# Linux example: containers of one dedicated container runtime, killed by cgroup, prod containers protected
[[guard.matchers]]
name = "bench-containers"
select = { cgroup_glob = "system.slice/docker-*.scope", parent_cmdline = 'containerd-shim.*bench-runtime' }
tier = 2; unit = "cgroup"; kill = "cgroup"
protect = { resolve_cmd = ["/run/current-system/sw/bin/docker", "--host", "unix:///var/run/bench.sock",
                           "inspect", "-f", "{{.Id}}", "bench-prod-1"], ttl = "60s" }

[[guard.never]]
name = "personal-emulator"; select = { cmdline = '-avd personal|@personal' }

[[guard.restore_hooks]]
name = "perf-restore"; on = ["critical", "stop"]; cmd = ["/usr/local/libexec/corral-restore-perf"]; timeout = "30s"

[guard.locks.emulator]
kind = "lease"; path = "/var/tmp/locks/emulator.lock"; ttl = "300s"; takeover = "corral"   # corral | any | never
[guard.locks.device]
kind = "flock"; path = "/home/user/.local/state/example-queue/device.lock"
holder_file = "/home/user/.local/state/example-queue/device.holder.json"

[guard.keepawake]
provider = "hold-cli"; command = "/usr/local/bin/awake"   # command defaults to `awake` on PATH
required_for = ["heavy"]       # optional: heavy work is refused (keepawake_unavailable) while the provider probe fails
```

## 10. Migration from the one-off scripts

### 10.1 Inventory and mapping

| Legacy component | What it does | corral replacement | Shim | Rollback |
|---|---|---|---|---|
| macOS memory killer (`heavy-guard`, LaunchAgent `watch` loop every 5 s) | tiers by cmdline (+cwd) pattern, exclusion pattern, TERM/8 s/KILL one tier at a time at ≤ 15% free or ≥ 7 GiB swap; `stop`/`resume` STOP file; `admit [name]` (≥ 40% free, ≤ 4 GiB swap, pressure normal, no tier process running); `status`; notifications | guard levels (macOS), matchers tier 1-3, `never` rule, kill switch, `corral admit` with heavy slots | `heavy-guard` shim (§10.2) | restore original script from `heavy-guard.legacy`; re-bootstrap its LaunchAgent; set guard `mode = "observe"` |
| Linux bench shedder (`bench-guard`, transient unit, root) | MemAvailable/temperature: `shed` non-prod containers by cgroup, `stop_all` (containers + emulator processes) at hard thresholds or STOP; protected container set with "unresolved → kill nothing"; restore hooks (turbo units, fan mode, power limit, bench network teardown); `admit` | guard levels (Linux), cgroup matchers with `protect.resolve_cmd`, `critical`, restore hooks, `corral admit` | `bench-guard` shim | re-run the legacy transient unit; guard to `observe` |
| JSON lease-file host lock (Python tool `acquire|renew|release|status|wait`, 300 s TTL, takeover file + log) | one emulator/full-suite holder per host | `kind = "lease"` locks, same files | none needed: both implementations interoperate on one file (conformance-tested) | nothing to roll back; stop using `corral lock` |
| Measurement queue checks (Python: idle two-stage with synthetic-input exemption, AC, unlocked, media, dialog, game, foreign GPU, LPM, thermal, GPU temp, disk, TCC, paused) | gate GPU/display jobs | checks and `measurement`/`gui` profiles with the same names, semantics and reason strings | the queue may call `corral admit --profile measurement` instead of its own checks (optional) | the queue keeps its own checks; nothing to undo |
| Measurement queue device lock (`flock`, holder JSON, token re-entry) | one GPU/display job at a time | `kind = "flock"` lock on the same file, `reentry_env` | none | — |
| Measurement queue features: snapshots, exit codes that pause the queue, cooldown, retries on "invalid" | queue semantics | snapshots (PROTOCOL.md §10), `pause_on_exit`, `cooldown` check; retries on invalid stay in the tool (corral never retries an exited job) | optional adapter (phase 2) | — |
| Keep-awake script (user session: caffeinate + lock/saver/disablesleep changes) | user's own session | untouched; corral uses named holds via a provider | hold extension in the script (its own spec) | provider `caffeinate` or `none` |

### 10.2 Shims (R-MIG-01)

A shim is a small script installed under the legacy name, keeping CLI, output and exit codes. The scripts call `corral shim <macos|linux> [--program NAME] <admit [name]|stop|resume|status|watch>`, which prints the legacy shapes below (the table shows the equivalent long form):

| Legacy call | Shim does | Output / exit |
|---|---|---|
| `heavy-guard admit [name]` | `corral admit --name "legacy:${name:-?}" --class heavy --reserve 60s --legacy-output` | `ok` / exit 0; `refused: <reason>` / exit 1; guard unreachable → `refused: guard unreachable` / exit 1 |
| `heavy-guard stop` | `corral guard stop --by legacy-cli` | `heavy jobs killed; new ones blocked until: heavy-guard resume` |
| `heavy-guard resume` | `corral guard resume` | `resumed` |
| `heavy-guard status` | `corral guard status --legacy-output` | first line `free=..% pressure=.. swap=..MB stop=ON|off`, then `tier N: pids` |
| `heavy-guard watch` | blocks while the guard is alive (`corral guard await`), so a leftover KeepAlive unit does not respawn-loop | — |
| `bench-guard admit|stop|resume|status|watch` | same mapping with Linux fields (`avail=..MB temp=..C stop=..`) | same codes |

Golden tests record the legacy outputs for each case and compare the shim (R-MIG-01).

### 10.3 Phases (each reversible, R-MIG-02)

| Phase | Action | Exit criterion | Rollback |
|---|---|---|---|
| **G0 observe** | install `corral guard` with `mode = "observe"` beside the legacy guards on every host; legacy stays in charge | one week of logs: every legacy action has a matching corral decision (same tier, same moment ± 1 sample); no corral decision the owner considers wrong | uninstall corral guard |
| **G1 Linux enforce** | stop the legacy Linux shedder; guard `enforce`; install `bench-guard` shim | chaos cases "memory hog" and "thermal" pass on the server; restore hooks verified | §10.1 rollback column |
| **G2 macOS enforce** | unload the legacy LaunchAgent; guard `enforce`; install `heavy-guard` shim; existing callers of `admit` keep working | chaos cases "memory hog with the user's apps", "kill switch during a job" pass; ceiling held | §10.1 |
| **G3 callers adopt** | agent instructions and scripts switch from `admit` + lock + raw command to `corral run` or `corral submit` | no legacy `admit` calls in guard logs for 14 days | callers keep working through shims |
| **G4 retire** | remove legacy scripts and shims after 30 days without rollback | — | reinstall from history |

At no time do two killers enforce on one host: the legacy watch loop is stopped in the same step that switches the
guard to `enforce`.

## 11. Tests specific to guards

- Level computation and actions from injected sample sequences (each level, hysteresis, escalation from pause to
  kill, tier-by-tier shed with settle, critical including protected, `never` never signalled).
- Protected-set resolution failure → shed kills nothing (R-GUARD-03).
- Pid reuse and start-time verification (R-GUARD-04); static check for name-based kills.
- Kill switch persistence across guard restart (R-GUARD-05).
- Reservations: two concurrent `admit --reserve` with one heavy slot → exactly one ok.
- Lock conformance against the reference implementation (R-LOCK-01): contention, stale takeover race with two
  contenders, holder-only release, corrupt file, takeover log line.
- Re-check under lock: a check flipping between admit and lock → decline (R-ADM-01).
- Idle tracker with synthetic stamps and windows (R-ADM-07).
- Keep-awake providers against stubs: named holds, renew without gap, foreign keep-awake process untouched.
- Shim golden outputs (R-MIG-01).
