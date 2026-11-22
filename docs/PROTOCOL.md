# corral: protocol

Status: **approved by the owner (v0.1, 22-Nov-2026); round-2 drafts excepted.** Companion to `docs/SPEC.md` (requirement IDs `R-*` refer
there). Covers the NATS subjects and schemas between CLI, scheduler and workers, the job state machine, leases and
fencing, cancel, logs and progress, presence, interactive leases, and the HTTP API for bulk bytes. The local protocol
between worker/CLI and the guard is in `docs/GUARDS.md` §6.

Round 2 drafts (22-Nov-2026, not approved): agent jobs (§4.1), exec sessions (§11.1), external DPoP-bound tokens
(§12.3), the local hook decision schema (§14).

Notation: JSON shown with `//` comments for explanation only (the wire has no comments). Times are RFC 3339 UTC.
Sizes are bytes unless the key ends in `_mb`. `<w>` = worker host name, `<id>` = job or lease id, `<who>` = caller
identity slug.

---

## 1. Transport summary

| Channel | Used for |
|---|---|
| NATS core request/reply | API calls (submit, get, cancel, ...), claims, heartbeats, attempt events |
| NATS core publish | control pushes to workers (`ctl`), wake-ups |
| JetStream streams | logs, progress, job events (durable, replayable) |
| JetStream KV | worker presence |
| HTTP (scheduler) | snapshot upload/download, artifacts, log archive |

All NATS subjects start with `corral.`. The account is `CORRAL`; the system account `SYS` is used only for monitoring
and (phase 2) auth callout.

## 2. Envelope, versioning, identity, `authz`

Every JSON payload on NATS (requests, replies, events, pushes, presence values) has:

```json
{ "v": 1,                       // protocol version (integer)
  "from": "agent:laptop",       // caller identity; must equal the identity pinned by the subject (R-SEC-03)
  "req_id": "01JD...",          // ULID; idempotency key for requests (scheduler remembers 10 min)
  "authz": null,                // optional grant token (§12.3); ignored by v1 policy unless required
  ...body fields... }
```

Rules:
- Unknown fields are ignored. New optional fields may be added within `v = 1`.
- Replies carry `v` and either the result fields or `{"error": CODE, "message": "...", ...}`. Error codes:
  `bad_request`, `forbidden`, `not_found`, `conflict`, `stale_lease`, `grant_required`, `unavailable`,
  `too_large`, `version_unsupported`.
- A receiver that gets an unsupported `v` replies `version_unsupported` with `{"supported": [1]}`.
- The scheduler learns **who** is calling from the subject (`<who>`, `<w>`), which the caller's NATS permissions pin;
  `from` is checked against it. A NATS responder is not told the requester's identity by the server [unverified for
  current server versions], so corral does not rely on that.
- Retried requests with the same `req_id` return the original reply (no double submit, no double claim).

Identity slugs: `scheduler`, `worker-<host>`, `agent-<host>`, `owner`. In payloads written as `worker:<host>`,
`agent:<host>`, `owner`, `scheduler`.

## 3. Subjects

| Subject | Pattern | From → to | Purpose |
|---|---|---|---|
| `corral.api.<who>.submit` | req/reply | client → scheduler | Submit a job (§4). |
| `corral.api.<who>.get` | req/reply | client → scheduler | One job record incl. waiting reasons. |
| `corral.api.<who>.list` | req/reply | client → scheduler | Filtered job list. |
| `corral.api.<who>.workers` | req/reply | client → scheduler | Presence + admission summaries. |
| `corral.api.<who>.follow` | req/reply | client → scheduler | `{"id", "deliver"}`: stream the job's logs, events and progress into `<deliver>.…` under the caller's own inbox prefix (§8). |
| `corral.api.<who>.cancel` | req/reply | client → scheduler | Cancel own job (others: `owner`). |
| `corral.api.<who>.resubmit` | req/reply | client → scheduler | New job from an old spec; `killed` → `owner` only. |
| `corral.api.<who>.lease.open` / `.exec` / `.close` / `.list` | req/reply | client → scheduler | Interactive leases (§11). |
| `corral.api.<who>.admin.<action>` | req/reply | owner → scheduler | `pause`, `resume`, `drain`, `kill_switch`, `retention`. |
| `corral.work.<w>.claim` | req/reply | worker → scheduler | Ask for work (§6). |
| `corral.work.<w>.<id>.hb` | req/reply | worker → scheduler | Heartbeat; renews attempt lease; reply carries cancel. |
| `corral.work.<w>.<id>.event` | req/reply | worker → scheduler | `started`, `declined`, `finished`, `artifacts`, `note`. |
| `corral.ctl.<w>` | publish | scheduler → worker | `cancel`, `kill_stale`, `pause`, `resume`, `drain`, `kill_switch`. |
| `corral.wake.<w>` | publish | scheduler → worker | "something you may take changed". |
| `corral.log.<w>.<id>.<stream>` | JetStream publish | worker → `CORRAL_LOGS` | `stream` ∈ `out`, `err`, `sys` (corral notes). Clients never read it directly (§8, §12.1). |
| `corral.progress.<w>.<id>` | JetStream publish | worker → `CORRAL_EVENTS` | Progress. |
| `corral.event.<id>` | JetStream publish | scheduler → `CORRAL_EVENTS` | State transitions (what `wait` follows). |
| KV `corral_workers`, key `<w>` | KV put / watch | worker → scheduler, clients | Presence (§9). |

The scheduler also registers its `api` and `work` endpoints as a NATS micro service, which provides the standard
`$SRV.PING|INFO|STATS` discovery subjects [unverified in detail].

### 3.1 Streams and KV

| Name | Subjects | Retention | Limits |
|---|---|---|---|
| `CORRAL_LOGS` | `corral.log.>` | limits | MaxAge 7d, MaxBytes 4 GiB (config), discard old, max msg 1 MiB, duplicate window 2 min (set explicitly) |
| `CORRAL_EVENTS` | `corral.event.>`, `corral.progress.>` | limits | MaxAge 30d, MaxMsgsPerSubject 200 |
| KV `corral_workers` | `$KV.corral_workers.>` | KV | history 1, bucket MaxAge 60 s, limit markers on (expiry visible to watchers; needs server 2.11+ [unverified exact version]) |

## 4. Job spec and submit

```json
{ "v": 1, "from": "agent:server", "req_id": "01JD...", "authz": null,
  "cmd": ["bash", "-lc", "make bench"],        // argv; the CLI wraps a shell string as bash -lc
  "cwd": "sub/dir",                             // relative to the input root; default "."
  "env": {"FOO": "1"},                          // explicit only; worker adds CORRAL_*; nothing inherited
  "input": {"kind": "git", "repo": "https://example.com/owner/name.git", "ref": "main", "sha": "<resolved>"},
        // or {"kind": "snapshot", "sha256": "...", "bytes": 123, "base_sha": "...", "repo_hint": "name"}
        // or {"kind": "none"}
  "caps": ["macos", "metal"],
  "prefer": "laptop", "fallback_after_s": 1800,  // null = no preference; -1 = never fall back
  "ram_mb": 8192, "class": "heavy", "exclusive": false,
  "profile": "default",                         // default | measurement | gui | custom name
  "lock": "auto",                               // auto | none | <lock name>; auto = host's default heavy lock
  "priority": 50, "timeout_s": 7200, "not_after": null,
  "artifacts": ["out/"], "artifacts_max_mb": 2048,
  "retry_on_lost": true, "max_attempts": 2,
  "pause_on_exit": [],                          // exit codes that pause the worker
  "on_user_input": "continue",                  // continue | pause  (exclusive jobs)
  "max_cost_tier": 0,                           // phase 2; v1 accepts only 0
  "labels": {"project": "example"},
  "notify": "on_fail",                          // never | on_fail | always
  "parent": null }                              // lease id for a step
```

Reply: `{"v":1, "id":"j-20261122-7f3a2k", "state":"queued"}`; for snapshot input
`{"v":1, "id":"...", "state":"uploading", "upload_url":"/v1/inputs/s-...", "upload_token":"..."}`. The job becomes
claimable only after the upload is verified (§10).

Validation (reply `bad_request` with the field): heavy without `ram_mb`; `ram_mb` above every known worker's
`admissible_ram_cap` (warning field `unplaceable` instead of error if an offline worker might fit); unknown profile;
`timeout_s` > `max_timeout` (config, default 48 h); `priority > 80` without `owner`.

### 4.1 Agent jobs (draft, round 2; SPEC R-AGENT-*)

An agent job is a job with `"kind": "agent"` (default kind is `"command"`) and an `agent` block; `cmd` is empty.

```json
{ "v": 1, "from": "agent:server", "req_id": "...", "authz": null, "kind": "agent",
  "agent": { "repo_path": "/same/absolute/path/repo",   // same path on both hosts, or a remote URL
             "base": "main", "base_sha": "<resolved>", "branch": "sub/fix-tests",
             "provider": "example-provider", "model": "example-model",
             "brief": {"kind": "inline", "text": "..."},     // or {"kind":"upload", "sha256":..., "bytes":...}
             "fallback": "wait",                             // wait | local
             "deadline_s": 7200, "keep_worktree": false },
  "caps": ["macos"], "prefer": "laptop", "fallback_after_s": -1,
  "ram_mb": 256, "class": "light", "timeout_s": 14400, "labels": {"parent_session": "..."} }
```

The worker admits it against its `agent` slot (presence `slots.agent_free`), creates the worktree from
`exchange_remote` at `base_sha`, runs the configured runner, and on exit pushes `branch` to `exchange_remote`. It then
sends the attempt event `agent_result` (§6.4) before `finished`.

## 5. Job state machine

States: `uploading`, `queued`, `claimed`, `running`, `orphaned` (non-terminal); `succeeded`, `failed`, `killed`,
`cancelled`, `lost`, `expired` (terminal).

```
 submit ─▶ uploading ─verified─▶ queued ─claim─▶ claimed ─started─▶ running ──exit 0──────────▶ succeeded
              │                   ▲ ▲ │            │  │                │  ├──exit ≠ 0──────────▶ failed
              └─fail/1h─▶ failed  │ │ │        declined │                │  ├──guard/timeout/───▶ killed   (final)
                                  │ │ │            │  │                │  │  signal/OOM
                                  │ └─┼────────────┘  │                │  ├──cancel───────────▶ cancelled
                                  │   │               └────────────────┤  └──lease expiry─────▶ orphaned
                                  │   │                                │◀── valid hb (same token) ──┤
                                  │   │                                                  grace over │
                                  └───┼──────── requeue: retry_on_lost and attempts < max ◀─────────┤
                                      │                                         else ────▶ lost (final)
                                      ├──not_after passed──▶ expired
                                      └──cancel────────────▶ cancelled
```

| From | To | Trigger | Notes |
|---|---|---|---|
| uploading | queued | upload verified (sha256, size) | upload fails or 1 h passes → `failed` (reason `input`) |
| queued | claimed | a claim selects it | `attempt += 1`; new attempt token; lease starts |
| claimed | queued | `declined` | attempt record kept as `declined`, not counted; reasons → waiting |
| claimed | running | `started` | process spawned |
| claimed, running | succeeded / failed | `finished{outcome: exited}` | exit 0 / non-zero; `pause_on_exit` also pauses the worker |
| claimed, running | killed | `finished{outcome: killed}`; scheduler timeout backstop | **terminal; never requeued** (R-QUEUE-04) |
| any non-terminal | cancelled | cancel | running: ctl push + hb flag; worker kills then reports `finished{reason: cancelled}` |
| claimed, running | orphaned | no valid heartbeat for `lease_ttl` | |
| orphaned | running | heartbeat with the current token, attempt had `started` | worker woke up / network back |
| orphaned | claimed | heartbeat with the current token, attempt never `started` | `started` / `declined` still accepted; the claim deadline still applies |
| orphaned | queued | grace over, `retry_on_lost`, attempts < max | old token revoked |
| orphaned | lost | grace over, no retry left | terminal |
| claimed, running | lost | `finished{outcome: lost_local}` (worker restarted and found the attempt dead without having seen it end) | then requeue rule as above |
| queued | expired | `not_after` passed | |

### 5.1 Killed is final; lost may requeue

- **killed**: something decided to stop the job: the guard (budget, system memory, thermal, kill switch), a timeout,
  the kernel OOM killer / jetsam, or a signal nobody in corral sent (`external_signal`). The worker always knows
  which: guard kills arrive over the guard socket with a reason (GUARDS.md §6); its own kills (timeout, cancel) it
  made itself; anything else is `external_signal`.
- **lost**: nobody saw the job end (sleep, reboot, crash, partition). Requeue is safe because of fencing (§6.3).
- Exclusive (benchmark) jobs also default to `retry_on_lost: true`: an interrupted measurement is invalid anyway.
- `resubmit` of a `killed` job is `owner`-only (later: approval grant) so an agent cannot quietly restart what a
  guard stopped.

### 5.2 Kill reasons (closed set, extend only with a `v`-compatible addition)

`ram_exceeded`, `system_memory`, `thermal`, `kill_switch`, `timeout`, `external_signal`, `oom_kernel`,
`stale_lease`, `lease_killed` (a step's lease was killed). The worker also reports `reason: "cancelled"` for its own
cancel kills; the scheduler maps that to state `cancelled`, not `killed`.

## 6. Claims, attempt leases, heartbeats, fencing

### 6.1 Claim

Request (`corral.work.<w>.claim`):

```json
{ "v":1, "from":"worker:laptop", "req_id":"...", "authz":null,
  "version":"0.1.0", "runtime":"native", "cost_tier":0,
  "caps":["macos","arm64","metal","gpu","android"],
  "slots":{"heavy_free":1,"light_free":2,"agent_free":4},
  "admission":{ "heavy_ok":true, "exclusive_ok":false, "profiles_ok":["default","gui"],
                "admissible_ram_mb":5200,
                "blocked":[{"check":"user_idle","profile":"measurement","reason":"idle 40s (need 300s)",
                            "since":"2026-11-22T10:01:00Z"}] },
  "running":["j-20261122-aaaaaa"], "leases":["l-20261122-bbbbbb"] }
```

Reply: `{"v":1, "job": <job record> | null, "attempt":1, "lease_token":"<128-bit base32>", "lease_ttl_s":90,
"input_url":"...", "artifact_url":"...", "http_token":"...", "retry_after_s":60}`.

The worker then performs local admission under the lock (GUARDS.md §7) and sends `started` or `declined` within
`claim_deadline` (default 120 s; past that the scheduler treats the claim as declined with reason `claim_timeout`).
*Implementation note (M1 review):* the 90 s lease runs out before the 120 s deadline, so the deadline is also applied
to a never-started claim that has turned `orphaned`; it is a decline either way, never a lost attempt. A claim request
is retried with the same `req_id` (the scheduler replays its answer), so a lost reply does not strand the job. A
cancel that races with a decline, `lost_local` or the claim deadline ends the job `cancelled`; it is never requeued.

### 6.2 Heartbeat

Every 10 s per attempt (`corral.work.<w>.<id>.hb`):

```json
{ "v":1, "from":"worker:laptop", "lease_token":"...", "attempt":1,
  "phase":"preparing|running|uploading", "pid":123, "footprint_mb":3100, "cpu_s":512,
  "log_seq":{"out":812,"err":40,"sys":6} }
```

Reply: `{"v":1, "ok":true, "lease_until":"...", "cancel":false}` or `{"v":1, "ok":false, "error":"stale_lease"}`.
On `stale_lease` the worker kills the attempt's process group, discards unsent logs for it, and does not report.

### 6.3 Fencing with attempt tokens

- Every claim mints a new random `lease_token` bound to `(job, attempt, worker)`.
- Heartbeats, events, log messages (header) and HTTP artifact tokens carry it; the scheduler accepts a message only
  if the token is the job's current one.
- When an orphaned attempt is requeued the token is revoked; a returning worker gets `stale_lease` on its first
  heartbeat and kills the attempt (R-QUEUE-06). Its log messages are dropped by the log sink (token mismatch).
- The scheduler's clock alone defines lease times; worker clocks are never compared.
- After a scheduler restart, all leases are extended by the measured downtime before expiry is evaluated (R-OPS-03).

### 6.4 Attempt events

`corral.work.<w>.<id>.event`, always with `lease_token`:

| `type` | Body |
|---|---|
| `started` | `{"pid":123, "pgid":123, "t":"..."}` |
| `declined` | `{"reasons":[{"check":"lock","reason":"held by build-x (pid 812), heartbeat 20s ago"}]}` |
| `finished` | `{"outcome":"exited|killed|lost_local", "exit_code":3, "signal":null, "reason":null|<kill reason>, "detail":"...", "footprint_peak_mb":8100, "duration_s":312, "disturbed":false, "log_dropped_bytes":0}` |
| `artifacts` | `{"state":"uploaded|refused|none", "bytes":123, "sha256":"..."}` |
| `note` | `{"text":"SIGSTOP: free 21% pressure 2"}` (also written to the `sys` log stream) |
| `agent_result` (draft) | `{"branch":"sub/fix-tests", "head_sha":"...", "pushed":true, "summary":"... (≤ 4 KiB)", "diffstat":{"files":3,"insertions":40,"deletions":7}, "runner_exit":0}` |
| `sync_back` (draft, exec steps) | `{"patch_sha256":"...", "bytes":1234, "files":["package-lock.json"], "pulled":["build/report.html"], "dropped":[]}` |

`disturbed: true` marks an exclusive job during which real user input occurred (`on_user_input: continue`).

## 7. Cancel

1. Client: `corral.api.<who>.cancel {"id": "...", "reason": "..."}`. Allowed for own jobs; others' need `owner`
   (or a grant when policy requires it).
2. Scheduler: queued/uploading → `cancelled` immediately. Claimed/running → records `cancel_requested`, publishes
   `corral.ctl.<w> {"action":"cancel","job":...,"lease_token":...,"by":...,"reason":...}`, and sets `cancel: true` in
   every later heartbeat reply (covers a lost push, ≤ 10 s).
3. Worker: TERM to the process group, `kill_grace`, KILL; reports `finished{outcome:"killed", reason:"cancelled"}`;
   the scheduler maps that to state `cancelled`.
4. A worker offline during cancel learns it on reconnect (heartbeat reply) or, after grace, the attempt simply
   becomes `cancelled` instead of `lost`/requeued.

## 8. Logs

- The worker writes child stdout/stderr to local files first (`runs/<id>/<attempt>/{out,err}`); they are the source
  of truth and the spool.
- A publisher tails them and publishes chunks (≤ 64 KiB) to `corral.log.<w>.<id>.<stream>` with headers
  `Nats-Msg-Id: <id>/<attempt>/<stream>/<seq>`, `Corral-Seq`, `Corral-Attempt`, `Corral-Stream`. There is no lease
  token in a log message: followers can read these headers, and the token is a secret between worker and scheduler. It awaits publish
  acks (≤ 64 in flight). While disconnected it pauses and resumes from the last acked seq.
- Spool cap `log_spool_max` (512 MiB per attempt): beyond it the oldest unsent bytes are replaced by one
  `[corral] N bytes dropped` note; `finished.log_dropped_bytes` records it.
- The scheduler's durable consumer appends to `<data_dir>/jobs/<id>/<attempt>.<stream>.log` only if
  `seq = last + 1` and the chunk comes from the worker (taken from the log subject, which a worker can only publish
  under its own name) that holds the job's current attempt; it acks after the write (idempotent across redelivery). A
  gap that is not filled after `sink_hold_max` redeliveries (default 30, about 30 s) is written into the archive as
  `[corral] log chunks a..b are missing (never arrived)` and archiving moves on (R-IO-03).
- **Followers** (`corral logs -f`, `corral wait`) do not touch JetStream. The client subscribes to the three literal
  subjects `<deliver>.<n>.log`, `.ev` and `.prog` under its own inbox prefix (`<deliver>` = `_INBOX_<slug>.f<random>`),
  then calls `corral.api.<who>.follow {"id", "deliver": "<deliver>.<n>"}`. The scheduler checks that `deliver` is a
  plain subject under the caller's inbox prefix and creates three ephemeral push consumers (all messages, no acks,
  `inactive_threshold` 30 s) that deliver the job's logs, job events and progress there; each message keeps its
  original subject (`corral.log.…`, `corral.event.…`, `corral.progress.…`) and its `Corral-Seq`/`Corral-Attempt`/
  `Corral-Stream` headers. JetStream delivers only to an exact subscription, hence one subscription per subject. A
  follower that hears nothing for a while asks again with `<n>+1` (the consumers vanish when NATS restarts) and skips
  sequence numbers it has seen. Older than stream retention → HTTP archive (§10). *Why:* a client that may create
  JetStream consumers can aim a push consumer at any subject it may not publish to (a worker's `corral.ctl.<w>`),
  or edit the scheduler's own `sink` consumer. The scheduler creates the consumers, so clients need no `$JS.API`
  permission at all.
- The `sys` stream carries corral's own notes about the attempt (admission, guard actions, SIGSTOP/SIGCONT,
  keep-awake, artifact upload).

## 9. Progress, events, presence

**Progress.** A job prints `##corral progress=<0-100> <text>` on stdout or stderr; the worker parses it (the line
stays in the log) and publishes `corral.progress.<w>.<id>`: `{"v":1, "attempt":1, "pct":42, "text":"building",
"t":"..."}`. Rate-limited to 2/s per job.

**Job events.** `corral.event.<id>`: `{"v":1, "id":"...", "state":"running", "prev":"claimed", "attempt":1,
"worker":"laptop", "reason":null, "exit_code":null, "waiting":null|[...], "t":"..."}`. Waiting-reason changes are
published as events with `state` unchanged.

**Presence.** The worker puts KV `corral_workers.<w>` every 15 s and on any admission change:

```json
{ "v":1, "from":"worker:laptop", "host":"laptop", "version":"0.1.0", "runtime":"native", "cost_tier":0,
  "caps":[...], "slots":{...}, "admission":{...}, "running":[...], "leases":[...],
  "metrics":{"free_pct":52, "swap_mb":900, "pressure":1, "thermal":"nominal", "on_ac":true,
             "screen_locked":false, "idle_s":30, "guard_level":"ok"},
  "checks":{"<name>":{"ok":true, "reason":"...", "value":..., "since":"..."}},
  "paused":null, "kill_switch":false, "t":"..." }
```

A key not refreshed for 60 s expires; watchers see the purge and show `offline since <t>`.

**Wake.** `corral.wake.<w>`: `{"v":1, "reason":"queued|cancelled|lease_step|resume", "job":"<id>"}`. Published when a
job that this worker could take (by caps and RAM cap, ignoring live admission) is queued, or a lease step arrives.

**Control.** `corral.ctl.<w>`: `{"v":1, "action":"cancel|kill_stale|pause|resume|drain|kill_switch", "job":null,
"lease_token":null, "by":"owner", "reason":"..."}`. `kill_switch` forwards to the host guard (`corral guard stop`).

## 10. HTTP API (bulk bytes)

Served by the scheduler on `http_listen` (default port 4280; loopback + overlay). All requests carry
`Authorization: Bearer <capability token>`.

| Method + path | Token verb | Who | Notes |
|---|---|---|---|
| `PUT /v1/inputs/<upload>` | `input:put` | submitter, 1 h | `tar.zst`; headers `Content-Length`, `X-Corral-Sha256`; verified, then job → `queued` |
| `GET /v1/inputs/<upload>` | `input:get` | the leased worker, lease lifetime | snapshot download |
| `PUT /v1/jobs/<id>/<attempt>/artifacts` | `artifacts:put` | the leased worker (token bound to `lease_token`) | `tar.zst`; over `artifacts_max_mb` → 413, recorded |
| `GET /v1/jobs/<id>/artifacts` | `artifacts:get` | submitter or owner, 24 h | latest successful attempt unless `?attempt=` |
| `GET /v1/jobs/<id>/logs/<attempt>/<stream>` | `log:get` | submitter or owner, 24 h | archive; `Range` supported |
| `PUT /v1/leases/<lease>/sync/<n>` | `lease:sync` | lease owner | delta `tar.zst` + deletion list (§11) |
| `GET /v1/workers/<w>/fresh` | none (loopback/overlay only) | uptime checks | 200 if presence < 60 s old |
| `GET /healthz`, `GET /metrics` | none | operators | |

**Capability tokens:** `base64url(payload).base64url(HMAC-SHA256(key, payload))`, payload
`{"job":"...", "verb":"...", "lease":"<token or null>", "exp":<unix>}`. The key is a scheduler secret (systemd
credential). Tokens are minted in NATS replies (submit, claim, `artifacts` CLI call). No long-lived HTTP secrets
exist. Phase 2: the same endpoints also accept externally issued JWTs whose scope maps to `{job, verb}`.

**Snapshot construction (CLI):** `git ls-files -co --exclude-standard -z` → `tar.zst`; records `base_sha` and
`git diff HEAD` as `code.patch` inside the archive; refuses deny-list files (`.env*`, `*.pem`, `*.key`, `id_*`,
`*credentials*`, configurable) unless `--allow-secret-files`; cap `snapshot_max_mb` (default 1024).

**Retention:** hourly; finished jobs older than `retention.age` deleted, then oldest first until under
`retention.cap`; queued/running and `--keep` (up to `keep_flagged`) are never deleted.

## 11. Interactive leases

A lease is a job of kind `lease` that holds a slot, its declared RAM, optionally the host lock, and a persistent work
directory; **steps** are jobs with `parent = <lease id>` that run in it, one at a time, in order.

| Call | Request body | Reply |
|---|---|---|
| `lease.open` | `{"caps":[...], "ram_mb":4096, "profile":"gui", "ttl_s":14400, "idle_s":1200, "hold_lock":false, "prefer":"laptop"}` | `{"id":"l-...", "state":"queued"}`; then events as for jobs; `running` = ready |
| `lease.exec` | `{"lease":"l-...", "cmd":[...], "sync":{"manifest_sha256":"...", "bytes":123} , "pull":["out/x.png"], "timeout_s":600}` | `{"id":"j-... (step)", "upload_url":..., "upload_token":...}` |
| `lease.close` | `{"lease":"l-...", "keep_dir_s":0}` | `{"ok":true}` |
| `lease.list` | `{}` | leases of the caller |

- Sync is one-way (client → lease dir) by manifest delta (path, size, mtime, sha256; the client keeps the last
  manifest per lease); `--pull` paths come back as the step's artifacts.
- Admission for the lease is the same as for a job (declines, waiting reasons). Per step, the lease's profile is
  re-checked (stage-2 idle for `measurement`).
- The guard tracks the lease's processes as one registration (processes a step leaves running, e.g. an emulator,
  stay covered). A guard kill of any lease process ends the lease `killed` (R-LEASE-02).
- The host lock and keep-awake hold are taken per step by default; `hold_lock: true` keeps them for the lease's
  life.
- Idle timeout or `close` releases everything and removes the directory (`keep_dir_s` keeps it up to 24 h).
- Limits: `ttl_s` ≤ 8 h, `idle_s` default 20 min (Q10).

### 11.1 Exec sessions (draft, round 2; SPEC R-REXEC-*)

`corral exec` uses a lease of `"kind": "exec"`; the subjects are the lease subjects above.

| Field (`lease.open`) | Meaning |
|---|---|
| `"kind": "exec"` | no queueing: if no worker admits within `wait_s` (default 10), the scheduler replies `{"error":"unavailable", "waiting":[...]}` and drops the request |
| `"workdir": "mirror" \| "scratch"` | `mirror`: run at `"path"` (the caller's absolute repository root) if the worker allows that root; `scratch`: worker-managed dir |
| `"path"`, `"base_sha"`, `"exchange_remote"` | repository root, base commit, remote to fetch the base from when missing |
| `"class": "light" \| "heavy"` | admission class (SPEC R-ADM-08) |

`lease.exec` adds `"sync_back": true` (default) and `"pull": [paths]`. Before each step the worker checks the mirror
checkout: files differing from `base_sha` that are not in the session's last applied manifest → step declined with
`{"check":"dirty_foreign", "reason":"...", "files":[...]}`, nothing written. After the step the worker computes the
patch of changed tracked and untracked-not-ignored files, uploads it with the pulled paths as the step's artifacts,
and sends `sync_back` (§6.4). The client applies the patch only if `git apply --check` passes on its tree.
The session is closed by `lease.close`, by `idle_s`, or when the caller's connection has been gone for `idle_s`.

## 12. Authentication and authorization

### 12.1 v1: nkeys and per-subject permissions

NATS config-mode users with nkeys in account `CORRAL`; anonymous off (R-SEC-01). Clients set a private inbox prefix
(`_INBOX_<slug>.`) so they cannot read each other's replies.

| Identity | publish allow | subscribe allow |
|---|---|---|
| `scheduler` | `corral.ctl.>`, `corral.wake.>`, `corral.event.>`, `$JS.API.>` (its streams), replies | `corral.api.>`, `corral.work.>`, `corral.progress.>`, `_INBOX_scheduler.>` |
| `worker:<w>` | `corral.work.<w>.>`, `corral.log.<w>.>`, `corral.progress.<w>.>`, `$KV.corral_workers.<w>`, JetStream publish-ack paths | `corral.ctl.<w>`, `corral.wake.<w>`, `_INBOX_worker-<w>.>` |
| `agent:<h>` | `corral.api.agent-<h>.>` only (no `$JS.API`: followers go through `follow`, §8) | `_INBOX_agent-<h>.>`, `corral.event.>` |
| `owner` | `corral.api.owner.>` only | as agents |

Exact subject lists are covered by a test that attempts every forbidden publish/subscribe (R-SEC-02), including the
JetStream API (consumer create, update and delete, purge) for agents, the owner and workers. The scheduler answers a
request only into the caller's own inbox prefix (`_INBOX_<slug>.` of the identity pinned by the request subject); a
reply subject outside it is ignored, so a caller cannot make the scheduler publish onto a JetStream API subject.
Control pushes name a lease token that must match the attempt (`cancel`, `kill_stale`). Scheduler-side checks: a worker may only heartbeat/report attempts leased to it; a
client may cancel only jobs it submitted; `owner` may do anything. Every API request and transition is written to an
`events` audit table with identity and subject.

Known limits:
- Processes running as the same OS user can read the same seed file; such agents are one identity. Per-agent
  identity requires a credential broker (phase 2).
- **A job runs as the worker's OS user** (native runtime), so it can read that user's files, including the worker's
  nkey seed, and thereby speak as `worker:<w>` (claim jobs, forge events for its own host's attempts). The permission
  templates confine a stolen worker seed to that host's subjects, but they do not stop it. Running jobs as a
  separate unprivileged user (or in a container or VM runtime) is future work; until then, keep the seed file mode
  0600 and run only jobs you would run on that account.
- Each `follow` creates three short-lived consumers; there is no per-identity quota yet.
- Processes that `setsid` out of a job's group are tracked by a ppid walk everywhere and, on Linux, by the
  `CORRAL_JOB_ID`/`CORRAL_ATTEMPT` environment they inherit; a job that deliberately scrubs its children's
  environment and double-forks can still escape on Linux until the per-attempt cgroup (M2/M3) lands.

### 12.2 HTTP

Capability tokens (§10). Phase 2 adds externally issued JWTs.

### 12.3 Phase-2 integration point (designed now, no schema change)

1. **NATS auth callout** (server 2.10+): an `authz` callout service validates an external access token presented at
   connect and returns a user JWT with the **same permission templates** as §12.1, `<who>` = the token's subject
   slug. Subjects do not change. Scheduler and workers may stay nkey users (`auth_users` bypass).
2. **HTTP:** the scheduler accepts the issuer's JWTs (audience `corral-http`) wherever it accepts a capability token.
3. **Approvals (CIBA):** the scheduler's policy table `require_grant` (empty in v1) names actions:
   `exclusive`, `priority_high`, `cancel_other`, `resubmit_killed`, `admin`, `lock_none`, and in phase 2
   `cost_tier:<n>`. If an action requires a grant and `authz` is absent/invalid, the reply is
   `{"error":"grant_required", "action":"corral:exclusive", "binding_message":"..."}`; the CLI runs the issuer's
   approval flow and retries with `authz` set. Proof-of-possession (DPoP) for NATS requests travels in a
   `Corral-DPoP` header, defined now and ignored in v1.
4. **External DPoP-bound tokens (draft, round 2; SPEC R-SEC-07).** With `auth.external` configured:
   - HTTP: `Authorization: DPoP <access token>` plus a `DPoP` proof header (RFC 9449); the scheduler validates the
     token against the issuer's JWKS (issuer, audience, expiry), the proof (`htm`, `htu`, `iat` within 60 s, `jti`
     not seen in the last 5 min) and that the proof key's thumbprint equals the token's `cnf.jkt`. Scope maps to
     `{job, verb}` as for capability tokens.
   - NATS: the auth callout receives the token and a proof in the connect options; corral defines the proof's
     `htm = "CONNECT"`, `htu = "nats://<server>/corral"` [unverified as a standard profile; SPEC Q19]. The issued user
     JWT carries the §12.1 permission template for the token subject's slug.
   - The issuer is any OIDC provider; approval grants (CIBA) still arrive in `authz`.
5. **Labels for data segregation:** `labels.level` on jobs map to resource URIs (`corral://jobs/<id>`) for scoped
   read grants later.

## 13. Compatibility notes

- A v1 worker talking to a scheduler that answers `version_unsupported` stops claiming, keeps running attempts to
  completion, and reports the skew in presence.
- The CLI prints a one-line warning when its minor version differs from the scheduler's.

## 14. Local hook decision (draft, round 2; SPEC R-PLACE-*)

`corral classify --json` and `corral hook <harness>` share one decision record, also written to the decision log
(JSON lines):

```json
{ "v": 1, "t": "...", "cwd": "/abs/path", "mode": "observe|rewrite|deny",
  "decision": "local|rewrite|deny|queue|guarded_local|escape",
  "target": "laptop", "target_online": true,
  "segments": [ {"text": "cd repo", "class": "local_ok", "rule": "cd"},
                {"text": "npm test", "class": "heavy", "rule": "test.npm"} ],
  "reason": "heavy segment 'npm test' (rule test.npm); laptop online",
  "rewritten": "corral exec --on laptop --cwd /abs/path --class heavy -- cd repo && npm test",
  "job": null, "command_sha256": "...", "profile": "build-heavy" }
```

The harness adapters translate it: a rewrite becomes the harness's input-update output, a deny its deny output with
`reason`, `local` and `observe` produce no change. The log never stores the command text when it matches the secret
deny-list; `command_sha256` is kept instead.
