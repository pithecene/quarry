# CLI Overview

This document is a user guide for the Quarry CLI.
The authoritative contract is [`docs/contracts/CONTRACT_CLI.md`](../contracts/CONTRACT_CLI.md).

---

## Quick Start

Run a script (the only execution entrypoint):

```
quarry run \
  --script ./examples/demo.ts \
  --run-id run-001 \
  --source demo \
  --storage-backend fs \
  --storage-path ./quarry-data \
  --job '{"url":"https://example.com"}'
```

Inspect a run:

```
quarry inspect run run-001
```

List recent runs (no limit by default):

```
quarry list runs
```

Show JSON output:

```
quarry stats runs --format json
```

Interactive TUI for inspect/stats (opt-in only):

```
quarry inspect run run-001 --tui
```

---

## Command Groups

- `run`: execute work (the only command that runs jobs)
- `inspect`: deep view of a single entity (run, job, task, proxy, executor)
- `stats`: aggregated facts (runs, jobs, tasks, proxies, executors)
- `list`: thin enumerations (runs, jobs, pools, executors)
- `index`: maintain the run records `list runs` reads (`index rebuild`)
- `debug`: opt-in diagnostics (read-only by default)
- `version`: CLI and contract versions

---

## Output Formats

Commands support multiple formats:
- `table` for TTY output
- `json` for non-TTY output
- `yaml` for debugging and config inspection

The `--format` flag overrides defaults.

Supported formats:
- `table`: human-readable (default for TTY)
- `json`: canonical (default for non-TTY)
- `yaml`: debugging and config inspection

### Color

`--no-color` affects **table output only**. It does not disable TUI styling.

### TUI Mode

`--tui` is **opt-in** and **read-only**. It is supported only for:
- `inspect` subcommands
- `stats` subcommands

If `--tui` is provided for other commands, the CLI returns an error.

---

## Command Reference

### `run`

Executes a single script run. This is the only command that performs work.

Required flags:
- `--script <path>`
- `--run-id <id>`
- `--source <id>`
- `--storage-backend <fs|s3>`
- `--storage-path <path>`

Config file flag:
- `--config <path>` (YAML project-level defaults for `quarry run`)

Optional flags:
- `--attempt <n>` (default: 1)
- `--job-id <id>`
- `--parent-run-id <id>`
- `--job <json>` (inline JSON object; mutually exclusive with `--job-json`)
- `--job-json <path>` (load JSON object from file; mutually exclusive with `--job`)
- `--quiet`
- `--policy strict|buffered|streaming`
- `--flush-mode at_least_once|chunks_first|two_phase`
- `--buffer-events <n>`
- `--buffer-bytes <n>`
- `--flush-count <n>` (streaming policy: flush after N events)
- `--flush-interval <duration>` (streaming policy: flush every T, e.g. `5s`)
- `--proxy-config <path>` (JSON pool config)
- `--proxy-pool <name>`
- `--proxy-strategy round_robin|random|sticky`
- `--proxy-sticky-key <key>`
- `--proxy-domain <domain>` (when sticky scope = domain)
- `--proxy-origin <origin>` (when sticky scope = origin, format: scheme://host:port)

Storage flags:
- `--storage-dataset <name>` (Lode dataset ID, default: `"quarry"`)
- `--storage-region <region>` (AWS region, uses default chain if omitted)
- `--storage-endpoint <url>` (custom S3 endpoint for R2, MinIO, etc.)
- `--storage-s3-path-style` (force path-style addressing, required by R2/MinIO)

Adapter flags (event-bus notification):
- `--adapter <type>` (event-bus adapter, e.g. `webhook`, `redis`)
- `--adapter-url <url>` (adapter endpoint URL, required when `--adapter` is set)
- `--adapter-header <key=value>` (custom HTTP header, repeatable, webhook only)
- `--adapter-channel <name>` (Redis pub/sub channel name, default: `quarry:run_completed`)
- `--adapter-timeout <duration>` (per-request timeout, default: `10s`)
- `--adapter-retries <n>` (retry attempts with exponential backoff, default: `3`)

Fan-out flags (derived work execution):
- `--depth <n>` (maximum recursion depth; 0 = disabled, default: `0`)
- `--max-runs <n>` (total child run cap; required when `--depth > 0`)
- `--parallel <n>` (concurrent child runs, default: `1`)

Module resolution flags:
- `--resolve-from <path>` (resolve bare-specifier ESM imports from an alternate `node_modules` directory; for monorepo/container setups)

Browser flags:
- `--browser-ws-endpoint <url>` / `QUARRY_BROWSER_ENDPOINT` (connect to an externally managed browser instead of launching one; see below)
- `--no-browser-reuse` (disable transparent browser reuse across runs; each run launches its own Chromium)

Advanced flags:
- `--executor <path>` (auto-resolved by default; override for troubleshooting)

#### Browser Reuse

Quarry has two browser reuse mechanisms — one transparent, one explicit.

**Transparent reuse (default):** Quarry automatically starts a long-lived
browser server on the first `quarry run` invocation. Subsequent runs on the
same host reuse it via a file-based discovery protocol. The browser
self-terminates after `QUARRY_BROWSER_IDLE_TIMEOUT` seconds (default: 60)
with no active pages. This is analogous to `git credential-cache` — no user
management required.

Use `--no-browser-reuse` (or `no_browser_reuse: true` in config) to disable
this and launch a fresh Chromium per run.

**Explicit endpoint (`--browser-ws-endpoint` / `QUARRY_BROWSER_ENDPOINT`):**
Connects to an externally managed browser instead of launching one. Each run
gets an isolated `BrowserContext` within the shared browser. On cleanup, the
page and context are closed but the browser stays alive.

```bash
# Start a long-lived browser
chrome --headless --remote-debugging-port=9222 &
WS=$(curl -s http://localhost:9222/json/version | jq -r .webSocketDebuggerUrl)

# Connect Quarry to it
quarry run --browser-ws-endpoint "$WS" --script ./detail.ts ...

# Or via environment variable (preferred in containers)
QUARRY_BROWSER_ENDPOINT="$WS" quarry run --script ./detail.ts ...
```

When an explicit endpoint is set, transparent reuse is bypassed entirely.
When fan-out is enabled (`--depth > 0`), all child runs automatically share
the root run's browser connection.

Quarry performs a health check against the browser endpoint before launching
the executor. If the browser is unreachable, the run fails immediately with
a clear error.

> **Important:** When using an external browser, stealth and adblocker plugins
> are skipped (vanilla Puppeteer `connect()` is used). Proxy credentials
> configured via `--proxy-*` flags still apply via `page.authenticate()`.
> See [Container Usage](container.md) for multi-crawler deployment patterns.

Output and reporting flags:
- `--report <path>` (write structured JSON report to file on exit; use `-` for stderr)

Dry-run validation:
- `--dry-run` (validate script loadability without execution; no browser, no storage)

When `--dry-run` is set, `--source`, `--storage-backend`, and `--storage-path`
are not required. Only `--script` and `--run-id` are needed. The executor is
spawned in validation mode, which loads the script module (including
`--resolve-from` ESM hook if configured), checks the default export and
optional hook signatures, and exits. Exit code 0 = valid, 1 = load/validation
failure, 2 = executor missing.

Exit codes (per CONTRACT_RUN.md):
- `0`: success (run_complete)
- `1`: script error (run_error)
- `2`: executor crash
- `3`: policy failure

Example:

```
quarry run \
  --script ./examples/demo.ts \
  --run-id run-001 \
  --source demo \
  --storage-backend fs \
  --storage-path ./quarry-data \
  --policy strict \
  --job '{"source":"demo"}'
```

Proxy selection example:

```
quarry run \
  --script ./examples/demo.ts \
  --run-id run-001 \
  --source demo \
  --storage-backend fs \
  --storage-path ./quarry-data \
  --proxy-config ./proxies.json \
  --proxy-pool residential_sticky \
  --proxy-domain example.com \
  --job '{"source":"demo"}'
```

S3-compatible storage example (Cloudflare R2):

```
quarry run \
  --script ./my-script.ts \
  --run-id run-001 \
  --source my-source \
  --storage-backend s3 \
  --storage-path my-bucket/quarry \
  --storage-endpoint https://ACCOUNT_ID.r2.cloudflarestorage.com \
  --storage-s3-path-style \
  --job '{"url":"https://example.com"}'
```

Fan-out example (derived work execution):

```
quarry run \
  --script ./listing.ts \
  --run-id root-001 \
  --source demo \
  --storage-backend fs \
  --storage-path ./quarry-data \
  --job '{"start_url":"https://example.com/listings"}' \
  --depth 1 \
  --max-runs 50 \
  --parallel 4
```

#### Job Payload Contract

The job payload (`--job` or `--job-json`) **must** be a top-level JSON object.

**Valid:**
```bash
--job '{}'
--job '{"url": "https://example.com"}'
--job-json ./config.json  # file contains {"key": "value"}
```

**Invalid (rejected with error):**
```bash
--job '[]'           # array
--job '"string"'     # primitive
--job '123'          # primitive
--job 'null'         # null
```

The flags are mutually exclusive:
- Use `--job` for inline JSON objects
- Use `--job-json` to load from a file
- Using both is an error
- If neither is specified, defaults to `{}`

### `inspect`

Deep view of a single entity.

Subcommands:
- `inspect run <run-id>`
- `inspect job <job-id>`
- `inspect task <task-id>`
- `inspect proxy <pool-name>`
- `inspect executor <executor-id>`

Examples:

```
quarry inspect run run-001
quarry inspect proxy default
quarry inspect run run-001 --tui
```

### `stats`

Aggregated facts derived from the read path.

Subcommands:
- `stats runs`
- `stats jobs`
- `stats tasks`
- `stats proxies`
- `stats executors`
- `stats metrics`

Examples:

```
quarry stats runs
quarry stats proxies --format json
quarry stats runs --tui
quarry stats metrics --storage-backend fs --storage-path ./quarry-data
quarry stats metrics --storage-backend fs --storage-path ./quarry-data --run-id run-001
quarry stats metrics --storage-backend s3 --storage-path my-bucket/quarry \
  --storage-endpoint https://ACCOUNT_ID.r2.cloudflarestorage.com --storage-s3-path-style
```

`stats metrics` accepts the same S3 flags as `run` (`--storage-region`,
`--storage-endpoint`, `--storage-s3-path-style`), so data written to an
S3-compatible provider is read back from the same provider.

### `list`

Thin enumerations (not inspect-level detail).

Subcommands:
- `list runs [storage flags] [--source <s> [--category <c> [--day <d>]]] [--state succeeded|failed|unterminated] [--limit <n>] [--after <cursor>]`
- `list events <storage flags> --source <s> --category <c> --day <d> --run-id <id>`
- `list jobs`
- `list pools`
- `list executors`

Notes:
- `--limit` defaults to `0` (no limit).
- If output is large, the CLI may warn and suggest `--limit`.
- With `--storage-backend` and `--storage-path`, `list runs` reads the run
  records every run writes (v0.14.0+), and `list events` returns a run's
  events in sequence order. Without storage flags, `list runs` returns stub
  data.
- A run with no end record is `unterminated`: it may still be running, or
  its process was killed. Storage cannot tell which, so it is never shown
  as running.
- Pages are ordered by source, category, day, and run ID. Pass a page's
  `next` value as `--after` to get the following page.
- Runs written before v0.14.0 are listed only after `quarry index rebuild`
  backfills their records. Backfilled runs have no `started_at` or
  `outcome`; a finished one is `succeeded` if it ended with `run_complete`
  and `failed` if it ended with `run_error`.

Examples:

```
quarry list runs --storage-backend fs --storage-path ./quarry-data --source demo
quarry list runs --storage-backend s3 --storage-path my-bucket/quarry \
  --storage-endpoint https://ACCOUNT_ID.r2.cloudflarestorage.com --storage-s3-path-style \
  --source demo --category default --state succeeded --limit 100
quarry list events --storage-backend fs --storage-path ./quarry-data \
  --source demo --category default --day 2026-10-22 --run-id run-001
```

### `index`

`index rebuild <storage flags> [--dry-run]` backfills run records for runs
written before v0.14.0. It writes only what the stored data shows and never
modifies runs that the runtime recorded. A rerun writes only missing
records, so running it again resumes an interrupted rebuild. Run it with
`--dry-run` first, after any pre-v0.14.0 writers have stopped writing to
the dataset.

```
quarry index rebuild --storage-backend fs --storage-path ./quarry-data --dry-run
quarry index rebuild --storage-backend fs --storage-path ./quarry-data
```

### `debug`

Opt-in diagnostics. Read-only by default.

Subcommands:
- `debug resolve proxy <pool> [--commit]`
- `debug ipc [--verbose]`

Examples:

```
quarry debug resolve proxy default --proxy-config ./proxies.json
quarry debug resolve proxy default --proxy-config ./proxies.json --commit
quarry debug ipc --verbose
```

### `version`

Reports the canonical project version (lockstep across all components).

Example:

```
quarry version
```
