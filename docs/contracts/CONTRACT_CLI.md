# Quarry CLI Contract

This document defines the **CLI surface** and **read-only guarantees** for
Quarry. It defines the command topology, required invariants, and the
request/response shapes exposed by the runtime.

This is a contract document. Implementations must conform.

---

## Scope

- Defines CLI invariants and non-goals.
- Defines command topology and required subcommands.
- Defines output format selection and rendering rules.
- Defines request/response shapes for CLI-visible operations.

Non-goals:
- Does not define executor behavior.
- Does not define ingestion policy implementation details.
- Does not define storage layouts (see [CONTRACT_LODE.md](CONTRACT_LODE.md)).

---

## Invariants

- The CLI is the **only execution entrypoint**.
- All commands except `run`, `init`, `index rebuild`, and `prune` are
  **read-only** with respect to execution, datasets, and persisted state.
  `init` only creates the files of a script project in its target
  directory: it never overwrites a file and never touches storage (§init).
  `index rebuild` only creates backfilled run records: it never
  overwrites a record and never writes or deletes partition data
  (§index rebuild). `prune` only deletes whole runs that its flags select
  (§prune).
- A small set of `debug` commands may mutate **ephemeral runtime mechanics**
  only when explicitly requested and documented.
- The CLI talks **only** to the Go runtime.
- The executor never exposes CLI surfaces.
- No command starts a long-running background process.
- No scheduler or control-plane commands are permitted.

---

## Command Topology

```
quarry
├─ run
├─ init
├─ inspect
│  ├─ run <run-id>
│  └─ proxy <pool-name>
├─ stats
│  ├─ runs
│  ├─ proxies
│  └─ metrics
├─ list
│  ├─ runs
│  ├─ events
│  └─ pools
├─ index
│  └─ rebuild
├─ prune
├─ debug
│  ├─ resolve proxy <pool>
│  └─ ipc
└─ version
```

Forbidden commands:
- `status`
- `admin`
- `control`
- `serve`

---

## Output and Rendering

All `inspect`, `stats`, and `list` commands share the same output contract.

### Supported Formats

- `json` (canonical, stable)
- `table` (human-readable)
- `yaml` (debugging and config inspection)

### Format Selection Rules

- If output is a TTY, default to `table`.
- If output is not a TTY, default to `json`.
- A user-specified `--format` always overrides defaults.
- Invalid formats are errors.

### Shared Flags

- `--format=json|table|yaml`
- `--no-color` (affects table output only, not TUI)
- `--tui` (inspect and stats commands only)

Rendering is centralized. Command handlers must not implement custom
formatting logic.

### TUI Mode

TUI mode (`--tui`) provides interactive views for select read-only commands.

**TUI Policy:**

- **Opt-in only**: TUI is never the default. Users must explicitly request it.
- **Read-only only**: TUI is only available for `inspect` and `stats` commands.
- **Same data payloads**: TUI renders the same data structures as non-TUI output.
  There is no TUI-exclusive data.
- **Presentation only**: Bubble Tea is a presentation layer. It does not fetch
  additional data or perform mutations.
- **Clean exit**: TUI must exit cleanly on `q` or `Ctrl+C`.

Commands that do not support TUI must error if `--tui` is provided.

Supported TUI commands:
- `inspect run|proxy`
- `stats runs|proxies|metrics`

Unsupported (must error with `--tui`):
- `list` commands
- `debug` commands
- `version`
- `run`

---

## `run` (execution)

The `quarry run` command is the **only** command that executes work.
It must conform to [CONTRACT_RUN.md](CONTRACT_RUN.md) and emit observability defined by
[CONTRACT_EMIT.md](CONTRACT_EMIT.md) and [CONTRACT_POLICY.md](CONTRACT_POLICY.md).

No other command may initiate or control execution.

### Exit Codes

The `run` command exit code is determined by execution outcome:

| Exit Code | OutcomeStatus | Meaning |
|-----------|---------------|---------|
| 0 | `success` | Run completed successfully |
| 1 | `script_error` | Script emitted `run_error` |
| 2 | `executor_crash` | Executor crashed or exited abnormally |
| 3 | `policy_failure` | Ingestion policy failed (non-retryable) |
| 3 | `version_mismatch` | SDK/CLI contract version mismatch (non-retryable) |

The outcomes `policy_failure` and `version_mismatch` share exit code 3 because both
are non-retryable configuration errors that cannot be resolved by re-running.

### Structured Exit Report

The `quarry run` command supports an optional `--report` flag that writes a structured
JSON report on exit, making every run machine-auditable.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--report` | string | | Path to write JSON report on exit (use `-` for stderr) |

**Semantics:**
- The report is written after metrics persistence and adapter notification.
- The report JSON structure is documented in [CONTRACT_RUN.md](CONTRACT_RUN.md) §Structured Exit Report.
- `--report -` writes the report to stderr (useful for piping/capturing).
- Report write failures are logged to stderr as warnings and do not affect
  the run exit code.
- The `exit_code` field in the report matches the process exit code.

### Dry-Run Validation

The `quarry run` command supports a `--dry-run` flag that validates script loadability
without executing a run. No browser is launched, no storage is written,
and no policy/proxy/adapter config is required.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--dry-run` | bool | `false` | Validate script loadability only |

**Semantics:**
- When `--dry-run` is set, `--source`, `--storage-backend`, and `--storage-path`
  are not required. Only `--script` and `--run-id` are required.
- The executor is spawned in `--validate` mode, which loads the script module
  (including `--resolve-from` ESM hook registration if configured), validates
  the default export and optional hook signatures, and exits.
- Output is human-readable validation summary to stderr.
- Exit code 0 = script is loadable and well-formed.
- Exit code 1 = script load or validation failed.
- Exit code 2 = executor binary missing or failed to spawn.
- Policy, storage, proxy, adapter, and fan-out configuration are skipped entirely.

### Streaming Policy Flags

The `quarry run` command supports a `streaming` ingestion policy with configurable flush
triggers. At least one of `--flush-count` or `--flush-interval` must be
specified when `--policy=streaming`.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--flush-count` | int | | Flush after N events accumulate |
| `--flush-interval` | duration | | Flush every T duration (e.g. `5s`, `30s`) |

Semantics:
- Both may be specified. The first trigger to fire wins.
- Buffer is bounded internally. If full before a trigger fires, ingestion
  blocks until the next flush (events are never dropped).
- On run termination, a final flush is always attempted (best effort).
- Flush trigger counts are surfaced in `stats metrics` (see [CONTRACT_METRICS.md](CONTRACT_METRICS.md)).

### Adapter Flags

The `quarry run` command supports optional event-bus adapter notification.
See [CONTRACT_INTEGRATION.md](CONTRACT_INTEGRATION.md) for semantics.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--adapter` | string | | Adapter type (`webhook`, `redis`) |
| `--adapter-url` | string | | Endpoint URL (required when `--adapter` set) |
| `--adapter-header` | string (repeatable) | | Custom header as `key=value` (webhook only) |
| `--adapter-channel` | string | | Pub/sub channel name (redis only, default `quarry:run_completed`) |
| `--adapter-timeout` | duration | `10s` | Per-request timeout |
| `--adapter-retries` | int | `3` | Retry attempts |

Adapter invocation is best-effort. Failures are logged to stderr.
The run exit code is determined by execution outcome, never by adapter status.

### Event Sink Flags

The `quarry run` command supports optional event sink configuration for real-time
event delivery during a run. See [CONTRACT_INTEGRATION.md](CONTRACT_INTEGRATION.md) §Event Sink Model
for semantics.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--event-sink` | string (repeatable) | | Event sink type (`lode`, `redis`) |
| `--event-sink-lode-delivery` | string | `mandatory` | Delivery mode for Lode event sink |
| `--event-sink-redis-url` | string | | Redis URL (required when `--event-sink redis`) |
| `--event-sink-redis-stream-key` | string | `quarry:events` | Redis Streams key |
| `--event-sink-redis-max-len` | int | `100000` | Max stream length (-1 disables) |
| `--event-sink-redis-ttl` | duration | `24h` | Stream key TTL (-1 disables) |
| `--event-sink-redis-timeout` | duration | `2s` | Per-operation timeout |
| `--event-sink-redis-retries` | int | `2` | Retry attempts |
| `--event-sink-redis-delivery` | string | `mandatory` | Delivery mode for Redis event sink |

Semantics:
- `--event-sink` is repeatable. Each value adds a sink to the fan-out.
- When no `--event-sink` flags are set, events go to Lode only (backward
  compatible).
- Duplicate sink types are rejected.
- `--event-sink-redis-*` flags require `--event-sink redis`.
- Delivery modes: `mandatory` (failure may fail the run) or `best_effort`
  (failure logged, run continues).
- Config file: `events.sinks[]` array in YAML.
- Fan-out child runs inherit the parent's event sink configuration.

### Fan-Out Flags

The `quarry run` command supports optional derived work execution via enqueue events.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--depth` | int | `0` | Max fan-out recursion depth (0 = disabled) |
| `--max-runs` | int | | Total child run cap (required when `--depth > 0`) |
| `--parallel` | int | `1` | Max concurrent child runs |

Semantics:
- `--depth 0` (default): enqueue events are advisory only, and no child run starts.
- `--depth > 0`: enqueue events trigger child runs up to the specified depth.
- `--max-runs` is mandatory when `--depth > 0` (safety rail).
- `--parallel > 1` without `--depth > 0` emits a stderr warning (no-op).
- Deduplication: identical `(target, params)` pairs are executed once.
- Exit code is determined by root run outcome only.
- Child run results appear in the fan-out summary printed to stdout.
- Child runs inherit the root run's `--source` and `--category` by default.
  Per-child overrides are supported via `emit.enqueue({ source, category })`.
- `target` is resolved as a file path relative to CWD (same as `--script`).
  Target resolution semantics may change. Do not depend on path resolution details.

**Caveats:**
- `storage.put()` in child scripts requires that storage is properly configured
  on the root run. A missing FileWriter will fail the child run immediately.
- Per-enqueue `source`/`category` overrides apply to the immediate child only.
  grandchildren inherit from their parent unless they also specify overrides.

### Config File

The `quarry run` command supports an optional `--config <path>` flag that loads a YAML
config file providing project-level defaults for run flags.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--config` | string | | Path to YAML config file |

**Precedence rules:**
1. CLI flags (explicitly set) always win.
2. Config file values are used if the CLI flag was not explicitly set.
3. urfave flag defaults apply if neither CLI nor config provides a value.

**Required field handling:**
- `--source`, `--storage-backend`, and `--storage-path` are not
  `Required` on the flag definition. They are validated after config merge.
  If missing from both CLI and config, an actionable error is returned.
- `--script` and `--run-id` remain `Required: true` (per-invocation, never
  in config).

**Proxy pool migration:**
- Proxy pools may be defined inline under `proxies:` in the config file,
  replacing the need for a separate `--proxy-config` JSON file.
- `--proxy-config` and config `proxies:` cannot both be present (config error).
- `--proxy-config` used alone still works but emits a deprecation warning.

**No auto-discovery:** Config files are loaded only via explicit `--config`.
There is no implicit `quarry.yaml` search in the working directory.

### Transparent Browser Reuse

By default, `quarry run` transparently reuses a Chromium browser process
across sequential invocations. The browser is launched as a detached process
on the first run and self-terminates after an idle timeout (default: 60s).

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--no-browser-reuse` | bool | `false` | Disable browser reuse (per-run browser) |

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `QUARRY_BROWSER_ENDPOINT` | — | WebSocket URL of an externally managed browser (equivalent to `--browser-ws-endpoint`) |
| `QUARRY_BROWSER_IDLE_TIMEOUT` | `60` | Idle timeout in seconds before browser self-terminates |

**Semantics:**
- The browser server is a **transparent optimization**, analogous to
  `git credential-cache`. It self-terminates and requires no user management.
- `--browser-ws-endpoint` / `QUARRY_BROWSER_ENDPOINT` (explicit) takes
  absolute priority. Browser reuse is bypassed entirely when set.
- When an external endpoint is configured, the runtime performs a health
  check (`/json/version`) before launching the executor. If the browser is
  unreachable, the run fails with `OutcomeExecutorCrash` and a descriptive
  message.
- Fan-out (`--depth > 0`) uses the reusable browser if available and falls back
  to `LaunchManagedBrowser` when reuse is disabled.
- Proxy mismatch between runs causes a graceful fallback to per-run browser
  launch (the reusable browser is not killed).
- The runtime and the server keep their state in the discovery directory,
  `$XDG_RUNTIME_DIR/quarry/browser-reuse/`, or
  `$TMPDIR/quarry-$UID/browser-reuse/` when `XDG_RUNTIME_DIR` is unset, as
  on macOS. Releases before v0.14.0 used the parent directory. Their
  servers and the current ones share no file, so neither reuses nor stops
  the other's, and each stops at its own idle timeout.
- The browser server writes its diagnostics to `browser-server.log` in the
  discovery directory, truncated at each launch. It never holds the output
  streams of the run that launched it, so that run's output ends when the
  run ends. The run's stderr names the log when it starts a server.
- Config file: `no_browser_reuse: true` in YAML.

**Server lifetime:**
- A run reaches the server through the Unix socket `server.sock` in the
  discovery directory. The run connects, sends a hash of its proxy
  configuration, and receives the browser's endpoint, or a refusal when
  the proxy differs from the server's. The run keeps the connection open
  until it ends.
- Each open connection is a reference to the server. The operating system
  closes a connection when its process ends, however the process ends, so
  the server's count of connections is exact. No file, PID, or timestamp
  stands for a run.
- The server shuts down once it has had no open connection for the idle
  timeout. It first stops listening, which removes `server.sock`, and then
  closes its browser. The server handles connections and its idle timer
  in one event loop, so it never shuts down while a connection is open.
- A run that finds no server, because `server.sock` is missing, refuses
  the connection, or closes it without a reply, takes an exclusive `flock`
  on `browser.lock`. Holding the lock, it connects again in case another
  run has started a server, and otherwise starts one and waits until it
  listens. The operating system releases the lock if its holder ends. A
  server that exits or fails to listen within 30 seconds is the only
  server the runtime stops, and the runtime stops it as its own child.
- A server records its browser's endpoint in `browser.json` as soon as it
  has launched the browser. A server that ends without closing its
  browser, as when it is killed with `SIGKILL`, leaves the browser running
  and recorded. The next server closes each recorded browser through the
  Chrome DevTools Protocol before it launches its own. A browser stays
  recorded, for a later server to close, until its port refuses
  connections or answers as anything but that browser. The endpoint carries
  the browser's own ID, so a record never names another browser. Only a
  server reads or writes `browser.json`, and only while it starts under
  the lock. A server that cannot read the record does not start, so it
  never replaces the record, and the run launches its own browser. A server killed while it launches its browser, before the
  endpoint exists, can leave that browser unrecorded.
- A server whose browser closes or crashes shuts down.
- When the runtime cannot tell whether a server is usable, the run does
  not share a browser. This holds when the connection fails for any other
  reason, the server does not reply within 5 seconds, or the browser it
  grants does not answer the health check (`/json/version`). In the last
  case the run releases its lease at once. The run launches
  its own browser, as on a proxy mismatch, and its stderr gives the
  reason. No such failure stops a server or closes a browser.

**Reconciliation with "no background process" invariant:**
The invariant "No command starts a long-running background process" (§Invariants)
refers to user-visible daemons that require management (start/stop/monitor).
The reusable browser server is:
- Self-terminating (idle timeout)
- Not user-managed (no start/stop commands)
- Discoverable only via an ephemeral runtime socket
- Semantically equivalent to a kernel page cache or credential helper

This is a permitted side-effect, not a background service.

### Module Resolution

The `quarry run` command supports a `--resolve-from` flag for workspace and monorepo
setups where scripts import bare specifiers (`@myorg/db`, `shared-utils`)
that cannot be resolved from the script's directory.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--resolve-from` | string | | Path to `node_modules` directory for ESM resolution fallback |

| Environment Variable | Set by | Description |
|---------------------|--------|-------------|
| `QUARRY_RESOLVE_FROM` | Go runtime | Absolute path to the resolution fallback directory |

**Semantics:**
- The flag value must be an existing directory. It is absolutized at parse time.
- The Go runtime sets `QUARRY_RESOLVE_FROM` and prepends to `NODE_PATH` (CJS
  fallback) on the executor's environment.
- The Node executor registers an ESM resolve hook via `module.register()`.
  The hook tries default resolution first, then falls back to
  `createRequire(resolveFrom)` for bare specifiers only.
- Relative and absolute specifiers are never intercepted by the hook.
- Config file: `resolve_from: /app/node_modules` in YAML.

**Why `NODE_PATH` alone is insufficient:**
`NODE_PATH` only affects CJS `require()`. ESM `import` ignores it entirely.
The `module.register()` hook is the correct mechanism for ESM (stable since
Node 20.6+).

---

## `init` (project setup)

Creates a script project in a directory: `quarry init [<directory>]`. The
directory defaults to the current one and is created when absent.

Files written:
- `package.json`: a private ES module package (`"type": "module"`). It
  depends on the SDK from JSR at a caret range of the CLI's version
  (`^X.Y.Z` for CLI X.Y.Z) and on `proxy-agent`. It depends on `puppeteer`,
  `puppeteer-extra`, and `puppeteer-extra-plugin-stealth` at the executor's
  peer ranges. Its `allowScripts` approves
  Puppeteer's install script, which downloads the browser
- `.npmrc`: the `@jsr` scope's registry
- `mise.toml`: the CLI pinned to its own version
- `.gitignore`: `node_modules/` and the storage directory
- `links.ts`: an example script
- `quarry-data/`: the storage directory, created when absent

Rules:
- When any of those files already exists, `init` writes nothing and exits
  with status 1, naming the files. An existing storage directory is reused
- Each file is created exclusively, so `init` never replaces a file
- `init` installs nothing and starts no process. Its response lists the
  commands that install the project and run the example script
- `init` takes at most one directory
- The project targets Node.js 22.18 or later, as Puppeteer 25 requires 22.12
  or later. On Node 22.6 through 22.11, change the `puppeteer` range to `^24`
  before installing

Response:
```
InitResponse:
  directory: string
  created: string[]   (the paths created, in order; a reused storage
                       directory is not listed)
  next: string[]      (the commands to run next, with the directory
                       quoted for a POSIX shell)
```

In table format, the paths written go to stderr and the next commands are
the rows.

---

## `inspect` (single-entity introspection)

The `inspect` commands return a deep view of a single entity and are read-only.
Data is sourced from the runtime’s read path (see [CONTRACT_LODE.md](CONTRACT_LODE.md)).

### `inspect run <run-id>`

Request:
```
InspectRunRequest:
  run_id: string
```

Response:
```
InspectRunResponse:
  run_id: string
  job_id: string
  state: string
  attempt: number
  parent_run: string | null
  policy: string
  started_at: time
  ended_at: time | null
  sidecar_files: SidecarFileSummary[] | null  # omitted when empty

SidecarFileSummary:
  filename: string
  content_type: string
  size: number
  path: string
```

The `sidecar_files` field lists files written via `storage.put()` during the
run. File refs are collected from Lode snapshot metadata (see [CONTRACT_LODE.md](CONTRACT_LODE.md)
§ Sidecar File Inventory). The field is omitted when no sidecar files exist.

### `inspect proxy <pool-name>`

Response:
```
InspectProxyPoolResponse:
  name: string
  strategy: string
  endpoint_cnt: number
  sticky: ProxySticky | null
  runtime:
    round_robin_index: number
    sticky_entries: number
    last_used_at: time | null
```

This response reinforces that **proxy selection is runtime-owned** and
the executor only applies resolved endpoints (see [CONTRACT_PROXY.md](CONTRACT_PROXY.md)).

---

## `stats` (aggregated facts)

The `stats` commands return aggregated, derived facts. They must not expose
internal counters from executors. Stats are derived from Lode.

### `stats runs`

Response:
```
RunStats:
  total: number
  running: number
  succeeded: number
  failed: number
```

### `stats proxies`

Response:
```
ProxyStats:
  pool: string
  requests: number
  failures: number
  last_used_at: time | null
```

### `stats metrics`

Returns the most recent metrics snapshot.

Optional flags:
- `--storage-dataset=<name>`: Lode dataset ID (default: `"quarry"`)
- `--storage-backend=fs|s3`: storage backend for Lode reads
- `--storage-path=<path>`: storage path (fs: directory, s3: bucket/prefix)
- `--storage-region=<region>`: AWS region for S3 backend
- `--storage-endpoint=<url>`: custom S3 endpoint for S3-compatible providers, with the same meaning as on `run`
- `--storage-s3-path-style`: force path-style S3 addressing (required by many S3-compatible providers), with the same meaning as on `run`
- `--run-id=<id>`: read metrics for a specific run
- `--source=<source>`: filter by source partition

Both `--storage-backend` and `--storage-path` must be provided together.
Any storage flag given without both is an error. With both flags, metrics
are read from Lode storage. Without a storage flag, stub data is returned (see Data Sources in [CONTRACT_METRICS.md](CONTRACT_METRICS.md)).

Response:
```
MetricsSnapshot:
  ts: time
  run_id: string
  job_id: string | null
  policy: string
  executor: string
  storage_backend: string
  runs_started_total: number
  runs_completed_total: number
  runs_failed_total: number
  runs_crashed_total: number
  events_received_total: number
  events_persisted_total: number
  events_dropped_total: number
  dropped_by_type: map[string]number (optional)
  executor_launch_success_total: number
  executor_launch_failure_total: number
  executor_crash_total: number
  ipc_decode_errors_total: number
  lode_write_success_total: number
  lode_write_failure_total: number
  lode_write_retry_total: number
  flush_triggers: map[string]number (optional, streaming policy only)
```

Metric names match [CONTRACT_METRICS.md](CONTRACT_METRICS.md). See [CONTRACT_METRICS.md](CONTRACT_METRICS.md) for the
outcome-to-metric mapping (which outcomes increment which counters).
Dimensions are included for traceability.

Usage examples:
```
quarry stats metrics --storage-backend fs --storage-path /data/quarry
quarry stats metrics --storage-backend fs --storage-path /data/quarry --run-id run-001
quarry stats metrics --storage-backend s3 --storage-path mybucket/quarry --storage-region us-west-2
quarry stats metrics --storage-backend s3 --storage-path mybucket/quarry --storage-endpoint https://s3.example.com --storage-s3-path-style
quarry stats metrics --format json
```

---

## `list` (enumeration)

The `list` commands return thin slices (not inspect-level detail).

### `list runs`

With `--storage-backend` and `--storage-path`, runs are listed
from their run records ([CONTRACT_RUN.md](CONTRACT_RUN.md) §Run Records, §Reading Run Records).
Without them, the stub data below is returned. Giving only one of the pair
is an error.

Storage-backed flags:
- Storage flags as on `stats metrics` (`--storage-dataset`, `--storage-region`,
  `--storage-endpoint`, `--storage-s3-path-style`)
- `--source`, `--category`, `--day`: leading selectors, in that order
  ([CONTRACT_LODE.md](CONTRACT_LODE.md) §Run Record Namespace). `--category` requires
  `--source`, and `--day` requires `--category`. Omitting all three lists
  the whole dataset
- `--state=succeeded|failed|unterminated`
- `--limit=<n>` (0 = no limit)
- `--after=<cursor>`: the `next` value of a previous page

Response (`json`, `yaml`):
```
RunPage:
  runs: Run[]
  next: string        # omitted on the last page

Run:
  run_id: string
  job_id: string          # omitted when unknown
  parent_run_id: string   # omitted when absent
  attempt: number
  dataset: string
  source: string
  category: string
  day: string             # YYYY-MM-DD
  partition: string       # Hive-encoded partition prefix
  started_at: time | null # null when backfilled
  state: succeeded | failed | unterminated  # backfilled: from the terminal event
  backfilled: bool        # records written by index rebuild
  ended_at: time | null   # null when unterminated
  outcome: string | null  # CONTRACT_RUN.md outcome; null when unterminated or backfilled
  terminal: string | null # run_complete | run_error | null
  message: string         # omitted when absent
  event_count: number | null # null when unterminated or backfilled
```

The `table` format renders `runs` only and prints `More runs: --after <cursor>` to
stderr when another page exists.

Ordering and paging: runs are ordered by record key (source, category,
day, run ID) on every backend. `next` is the key of the last run returned.
Passing it as `--after` returns the runs after it. `next` is present only
when another run matching the request exists. The cursor is a key
position, not a reference to a stored run. Listing resumes at the first
start-record key after it, so paging stays correct when the run it names
is deleted between pages (for example, by retention). A value that does
not have the shape of a start-record key under the selected prefix, with
each key component encoded exactly as the write path encodes it, is an
error. A well-formed key that names no run is accepted, and listing
resumes after its position. With `--state`, a page holds up to `--limit`
matching runs.

Selector values are rejected when they are `.`, `..`, or contain a `.` or
`..` path segment ([CONTRACT_LODE.md](CONTRACT_LODE.md) §Key Encoding): stores normalize such
segments, which would widen the prefix beyond the selected runs.

Stub flags and response (no storage): `--state=running|failed|succeeded`,
`--limit=<n>`. Each item includes `run_id`, `state`, and `started_at`. Using a
storage-only flag (`--source`, `--category`, `--day`, or `--after`)
without storage is an error, and any storage flag given without both
`--storage-backend` and `--storage-path` is an error, never a fallback to
stub data.

### `list events`

Lists one run's events, read from storage.

Required flags:
- `--storage-backend`, `--storage-path` (and the other storage flags as on
  `list runs`)
- `--source`, `--category`, `--day`, `--run-id`: these locate the run's
  records directly

Response (`json`, `yaml`):
```
RunEvents:
  run: Run             # as in list runs
  events: object[]     # stored event records, in seq order
```

The `events` field holds the run's stored records of `record_kind` `event` and
`artifact_event` ([CONTRACT_LODE.md](CONTRACT_LODE.md) §Record Types), ordered by `seq`.
Artifact chunks and the metrics record are not events and are omitted.
Item payloads are returned as stored. Quarry does not know a script's
item schema. The run's partitions are addressed from its start record's
`partition`. `table` renders one summary row per record (`seq`, `type`,
`record_kind`, `ts`, `event_id`), and `json` and `yaml` carry the full records.

A run with no start record is an error (`run not found`).

### `list pools`

Response must include:
- `name`
- `strategy`

---

## `index` (run records)

### `index rebuild`

Backfills run records for runs written before v0.14.0, which `list runs`
cannot otherwise find ([CONTRACT_RUN.md](CONTRACT_RUN.md) §Backfilled Run Records). It scans
the dataset's partition tree and writes a start record for each run that
has no start record. An end record follows when the stored data shows that
the run ended, and the command reports what it wrote. Records are created, never
replaced, so a rerun writes only the records still missing, and rerunning
an interrupted rebuild resumes it.

Flags:
- `--storage-backend`, `--storage-path` (required) and the other storage
  flags as on `list runs`
- `--dry-run`: report what would be written, without writing it
- `--format`, `--no-color`

Response (`json`, `yaml`):
```
RebuildReport:
  dataset: string
  dry_run: bool
  runs_scanned: number
  already_indexed: number   # runs that already have records; not listed in runs
  starts_written: number    # in a dry run: would be written
  ends_written: number
  skipped: number
  runs: RebuiltRun[]

RebuiltRun:
  source: string
  category: string
  day: string
  run_id: string
  partition: string
  wrote_start: bool
  wrote_end: bool
  no_end: string    # why no end record was written; omitted otherwise
  skipped: string   # why the run was not indexed; omitted otherwise
```

The `table` format renders `runs` and prints the totals to stderr. A store error
terminates the rebuild with exit code 1 after the partial report is
rendered.

---

## `prune` (run retention)

Removes whole runs, chosen by state and age, as [CONTRACT_LODE.md](CONTRACT_LODE.md) §Run
Retention defines: the run's snapshots, sidecar files, and run records.

Flags:
- `--storage-backend`, `--storage-path` (required) and the other storage
  flags as on `list runs`
- `--source`, `--category`, `--day`: leading selectors, as on `list runs`
- `--run-id`: prune only this run, which requires `--source`, `--category`, and
  `--day`. A run that no longer exists is not an error
- `--succeeded-days=<n>`, `--failed-days=<n>`: prune runs in that state
  whose end record is at least `n` days old. `0` prunes every such run
- `--unterminated-days=<n>`: prune unterminated runs that started, and
  last wrote, at least `n` days ago. `n` must be at least 1 and should
  exceed the longest run
- A day count must lie between 0 and the largest count a duration holds
  (106751). Any other value is an error
- `--sweep-grace=<duration>`: after the runs, sweep the whole dataset,
  reclaiming data of writes that never committed once it is older than the
  duration. `0` (the default) skips the sweep
- `--dry-run`: report what would be removed, without removing anything
- `--format`, `--no-color`

At least one age flag is required. The prune keeps every run in a state whose flag is not set.
A selector or `--run-id` given with an empty value is an error, because
an empty selector would widen the prune instead of narrowing it.

Response (`json`, `yaml`):
```
PruneReport:
  dataset: string
  dry_run: bool
  runs_scanned: number
  not_due: number          # runs the ages do not select; not listed in runs
  runs_pruned: number      # in a dry run: would be pruned
  runs_kept: number        # selected, but kept for the reason in kept
  snapshots: number
  objects: number
  bytes: number
  sidecar_objects: number
  runs: PrunedRun[]
  orphan_end_records: number # end records without a start record; deleted
  unparseable: string[]    # malformed start-record keys; omitted when none
  sweep: SweepReport       # omitted when no sweep ran

PrunedRun:
  source: string
  category: string
  day: string
  run_id: string
  partition: string
  state: succeeded | failed | unterminated
  since: time              # the instant the age is measured from
  pruned: bool
  snapshots: number
  objects: number
  bytes: number
  sidecar_objects: number  # sidecar files and their .meta.json companions
  kept: string             # why a selected run was kept; omitted otherwise
  skipped: string[]        # recorded sidecar paths left in place; omitted when none

SweepReport:
  settled: number
  objects: number
  bytes: number
  unsized: number
  orphans: number
  unparseable: string[]    # omitted when none
```

The prune keeps the run that holds the dataset's head (`kept` gives the
reason), and a later prune removes it once another run commits. `table` renders
`runs` and prints the totals to stderr. A store error terminates the prune
with exit code 1 after the partial report is rendered. A rerun resumes it.

---

## `debug` (sharp tools, opt-in)

The `debug` commands are opt-in diagnostic tools and **must not execute work**.
They are read-only by default. Any mutation must be:
- explicitly requested by a flag
- ephemeral and non-persistent
- non-executing (no runs affected or started)
- observable in logs and inspection surfaces
- limited to runtime mechanics (never datasets, runs, or policies)

### `debug resolve proxy <pool>`

Purpose:
- test selector logic
- validate configuration
- reason about rotation

Response:
```
ResolveProxyResponse:
  endpoint: ProxyEndpoint
  committed: boolean
```

The `committed` field indicates whether the resolution was committed (rotation
counters advanced). It is `false` by default and `true` only when `--commit`
is provided.

By default this command is read-only and **must not** mutate runtime state.

Optional flag:
- `--commit`

If `--commit` is provided, the runtime **may** advance in-memory rotation
counters. This mutation is:
- ephemeral
- non-persistent
- non-executing
- local to proxy selection mechanics

It must not write to Lode, change datasets, or affect runs or policies.
This is the only allowed mutation outside `run` and must be observable in
logs and inspection surfaces.

### `debug ipc`

Response:
```
IPCDebugResponse:
  transport: string
  encoding: string
  messages_sent: number
  errors: number
  last_error: string | null
```

No payload dumping unless `--verbose` is provided.

---

## `version`

The `version` command reports the canonical project version.
It must not contact the executor.

Response:
```
VersionResponse:
  version: string
  commit: string
```

### Lockstep Versioning

All Quarry components share a single canonical version defined in `types.Version`.
This includes:
- CLI version
- Emit contract version ([CONTRACT_EMIT.md](CONTRACT_EMIT.md))
- IPC contract version ([CONTRACT_IPC.md](CONTRACT_IPC.md))

Version changes require updating the single source constant. There is no separate
"contract version": the project version is the contract version.
