# Configuration Reference

This document is a consolidated reference for all Quarry configuration surfaces.
For command details, see [`docs/guides/cli.md`](cli.md). For proxy setup, see [`docs/guides/proxy.md`](proxy.md).

---

## Configuration Layers

Quarry configuration lives in four distinct layers:

| Layer | Where | What it controls |
|-------|-------|------------------|
| **CLI flags** | `quarry run --flag` | Run execution, storage, policy, proxy selection |
| **YAML config file** | `--config quarry.yaml` | Project-level defaults for all run flags |
| **JSON config files** | `--proxy-config` (deprecated) | Proxy pool definitions |
| **Environment variables** | Process environment | Executor/browser behavior |

**Precedence:** CLI flags > YAML config file > flag defaults.

CLI flags control the Go runtime (orchestration, storage, policy).
Environment variables control the Node executor (browser hardening, plugin
behavior). The YAML config file provides reusable project-level defaults
to reduce flag repetition across invocations.

---

## CLI Flags

All flags are passed to `quarry run`. See [`docs/guides/cli.md`](cli.md) for full
command reference.

### Config File

| Flag | Type | Purpose |
|------|------|---------|
| `--config` | path | Path to YAML config file with project-level defaults |

See [YAML Config File](#yaml-config-file) below for schema and examples.

### Required

| Flag | Type | Purpose |
|------|------|---------|
| `--script` | path | Script to execute |
| `--run-id` | string | Unique run identifier |
| `--source` | string | Source identifier (Lode partition key) |
| `--storage-backend` | `fs` or `s3` | Storage backend |
| `--storage-path` | string | `fs`: directory path; `s3`: `bucket/prefix` |

> **Note:** `--source`, `--storage-backend`, and `--storage-path` can be
> provided via `--config` file instead of CLI flags. They are required at
> runtime but do not need to appear on the CLI if set in the config.

### Run Metadata

| Flag | Type | Default | Purpose |
|------|------|---------|---------|
| `--attempt` | int | `1` | Attempt number (1 = initial, >1 = retry) |
| `--job-id` | string | — | Optional job identifier |
| `--parent-run-id` | string | — | Required when `--attempt > 1` |
| `--job` | JSON string | `{}` | Inline job payload (must be a JSON object) |
| `--job-json` | path | — | Job payload from file (mutually exclusive with `--job`) |
| `--category` | string | `"default"` | Category identifier (Lode partition key) |

### Storage

| Flag | Type | Purpose |
|------|------|---------|
| `--storage-dataset` | string | Lode dataset ID (default: `"quarry"`) |
| `--storage-backend` | `fs` or `s3` | Backend type |
| `--storage-path` | string | `fs`: local directory; `s3`: `bucket/optional-prefix` |
| `--storage-region` | string | AWS region (S3 only; uses default credential chain) |
| `--storage-endpoint` | string | Custom S3 endpoint URL (for R2, MinIO, etc.) |
| `--storage-s3-path-style` | bool | Force path-style addressing (required by R2, MinIO) |

### Policy

| Flag | Type | Default | Purpose |
|------|------|---------|---------|
| `--policy` | `strict`, `buffered`, or `streaming` | `strict` | Ingestion policy |
| `--flush-mode` | `at_least_once`, `chunks_first`, `two_phase` | `at_least_once` | Buffered flush semantics |
| `--buffer-events` | int | `0` | Max events to buffer (buffered policy) |
| `--buffer-bytes` | int | `0` | Max buffer bytes (buffered policy) |
| `--flush-count` | int | `0` | Flush after N events (streaming policy) |
| `--flush-interval` | duration | | Flush every T duration, e.g. `5s` (streaming policy) |

Buffered policy requires at least one of `--buffer-events` or `--buffer-bytes` to be set (> 0).

Streaming policy requires at least one of `--flush-count` or `--flush-interval` to be set.
Both may be specified; the first trigger to fire wins.

### Proxy

| Flag | Type | Purpose |
|------|------|---------|
| `--proxy-config` | path | Path to JSON proxy pools config (**deprecated**: use `proxies:` in YAML config) |
| `--proxy-pool` | string | Pool name to select from |
| `--proxy-strategy` | `round_robin`, `random`, `sticky` | Override pool's default strategy |
| `--proxy-sticky-key` | string | Explicit sticky key (overrides derivation) |
| `--proxy-domain` | string | Domain for sticky derivation (scope=domain) |
| `--proxy-origin` | string | Origin for sticky derivation (scope=origin, format: `scheme://host:port`) |

See [`docs/guides/proxy.md`](proxy.md) for pool configuration format and selection behavior.

### Adapter (Event-Bus Notification)

| Flag | Type | Default | Purpose |
|------|------|---------|---------|
| `--adapter` | `webhook`, `redis` | | Adapter type |
| `--adapter-url` | string | | Endpoint URL (required when `--adapter` set) |
| `--adapter-header` | string (repeatable) | | Custom header as `key=value` (webhook only) |
| `--adapter-channel` | string | `quarry:run_completed` | Pub/sub channel name (redis only) |
| `--adapter-timeout` | duration | `10s` | Per-request/publish timeout |
| `--adapter-retries` | int | `3` | Retry attempts |

See [`docs/guides/integration.md`](integration.md) for adapter usage patterns.

### Event Sinks (Real-Time Event Delivery)

| Flag | Type | Default | Purpose |
|------|------|---------|---------|
| `--event-sink` | string (repeatable) | | Event sink type (`lode`, `redis`) |
| `--event-sink-lode-delivery` | string | `mandatory` | Delivery mode for Lode event sink |
| `--event-sink-redis-url` | string | | Redis URL (required when `--event-sink` includes `redis`) |
| `--event-sink-redis-stream-key` | string | `quarry:events` | Redis Streams key |
| `--event-sink-redis-max-len` | int | `100000` | Max stream length (-1 disables trimming) |
| `--event-sink-redis-ttl` | duration | `24h` | Stream key expiry (-1 disables) |
| `--event-sink-redis-timeout` | duration | `2s` | Per-operation timeout |
| `--event-sink-redis-retries` | int | `2` | Retry attempts |
| `--event-sink-redis-delivery` | string | `mandatory` | Delivery mode for Redis event sink |

Event sinks receive events in real time during a run (unlike adapters, which
fire once after a run completes). When no `--event-sink` flags are set, events
go to Lode only (default behavior). Each sink declares a delivery mode:
`mandatory` (failure may fail the run) or `best_effort` (failure logged,
run continues). See [`docs/contracts/CONTRACT_INTEGRATION.md`](../contracts/CONTRACT_INTEGRATION.md) for semantics.

### Fan-Out (Derived Work Execution)

| Flag | Type | Default | Purpose |
|------|------|---------|---------|
| `--depth` | int | `0` | Max recursion depth (0 = disabled) |
| `--max-runs` | int | | Total child run cap (required when `--depth > 0`) |
| `--parallel` | int | `1` | Max concurrent child runs |

When `--depth > 0`, enqueue events emitted by scripts trigger child runs
at runtime. `--max-runs` is mandatory as a safety rail.
`--parallel > 1` without `--depth > 0` emits a warning (no-op).

### Output

| Flag | Type | Default | Purpose |
|------|------|---------|---------|
| `--format` | `table`, `json`, `yaml` | auto (TTY=table, pipe=json) | Output format |
| `--no-color` | bool | `false` | Disable color in table output |
| `--tui` | bool | `false` | Interactive TUI (inspect/stats only) |
| `--quiet` | bool | `false` | Suppress run result output |
| `--report` | string | | Path to write JSON report on exit (use `-` for stderr) |
| `--dry-run` | bool | `false` | Validate script loadability without execution (no browser, no storage) |

### Module Resolution

| Flag | Type | Purpose |
|------|------|---------|
| `--resolve-from` | path | Path to `node_modules` directory for bare-specifier ESM resolution fallback (monorepo/container support) |

When scripts import workspace packages (`@myorg/db`, `shared-utils`) that
are not resolvable from the script's own directory, `--resolve-from` tells
the executor where to look. Must be an existing directory; absolutized at
parse time. See [`docs/contracts/CONTRACT_CLI.md`](../contracts/CONTRACT_CLI.md) for semantics.

### Browser Reuse

| Flag | Env Var | Type | Purpose |
|------|--------|------|---------|
| `--browser-ws-endpoint` | `QUARRY_BROWSER_ENDPOINT` | string | Connect to an externally managed browser via WebSocket URL instead of launching Chromium per run |
| `--no-browser-reuse` | — | bool | Disable transparent browser reuse across runs (per-run Chromium launch) |

**Explicit browser (`--browser-ws-endpoint`):** The executor uses
`puppeteer.connect()` (vanilla, no plugins) and creates an isolated
`BrowserContext` per run. On cleanup, the context is closed but the browser
stays alive. Fan-out child runs inherit the endpoint automatically.

**Transparent reuse (default):** When no explicit endpoint is provided, Quarry
automatically starts a reusable browser server on the first run. Subsequent
runs reuse it. The browser self-terminates after `QUARRY_BROWSER_IDLE_TIMEOUT`
seconds (default: 60) with no active pages. Its diagnostics go to
`browser-server.log` in the discovery directory
(`$XDG_RUNTIME_DIR/quarry/`), whose path the run prints when it starts the
server.

Use `--no-browser-reuse` (or `no_browser_reuse: true` in config) to revert
to per-run browser launch.

See [`docs/guides/cli.md`](cli.md) for usage examples.

### Advanced (development only)

| Flag | Type | Purpose |
|------|------|---------|
| `--executor` | path | Override executor binary (auto-resolved in bundled binary; only needed for local development builds) |

---

## Environment Variables

Environment variables control executor and browser behavior. They are read
by the Node executor process, not the Go runtime.

| Variable | Default | Purpose |
|----------|---------|---------|
| `QUARRY_STEALTH` | enabled | Puppeteer stealth plugin (evades bot detection). Set to `0` to disable. |
| `QUARRY_ADBLOCKER` | disabled | Puppeteer adblocker plugin. Set to `1` to enable. |
| `QUARRY_NO_SANDBOX` | disabled | Disable Chromium sandbox (required in containers/CI). Set to `1` to enable. |
| `QUARRY_BROWSER_ENDPOINT` | — | WebSocket URL of an externally managed browser (equivalent to `--browser-ws-endpoint` flag). Preferred for container deployments. |
| `QUARRY_BROWSER_IDLE_TIMEOUT` | `60` | Seconds before the reusable browser server self-terminates after all pages close. Read by both the Go runtime (to pass to the browser server) and the executor (as its idle timer). |
| `QUARRY_RESOLVE_FROM` | — | Absolute path to `node_modules` for ESM resolution fallback. Set automatically by the Go runtime when `--resolve-from` is specified; not typically set manually. |

### Usage

```bash
# Container/CI: disable sandbox
QUARRY_NO_SANDBOX=1 quarry run ...

# Disable stealth (e.g., for debugging)
QUARRY_STEALTH=0 quarry run ...

# Enable adblocker
QUARRY_ADBLOCKER=1 quarry run ...

# Combine
QUARRY_NO_SANDBOX=1 QUARRY_ADBLOCKER=1 quarry run ...
```

### Why environment variables?

These control Puppeteer browser behavior inside the executor subprocess.
They are deployment-level knobs (container vs local, detection posture)
rather than per-run configuration, so they live outside the run protocol.

---

## YAML Config File

The `--config` flag loads a YAML config file providing project-level defaults
for `quarry run`. This reduces repetition when the same source, storage,
policy, and proxy settings are used across multiple invocations.

### Quick Start

```yaml
# quarry.yaml
source: my-source
storage:
  backend: fs
  path: ./quarry-data
```

```bash
quarry run --config quarry.yaml --script ./script.ts --run-id run-001
```

### Full Schema

```yaml
# quarry.yaml — project defaults for quarry run
# All values are overridden by explicit CLI flags.

source: my-source
category: default

# Connect to an externally managed browser instead of launching one per run.
# Also settable via QUARRY_BROWSER_ENDPOINT env var (preferred in containers).
# browser_ws_endpoint: ws://localhost:9222/devtools/browser/...

# Disable transparent browser reuse across runs.
# no_browser_reuse: true

# Executor path override (development only).
# The bundled binary auto-resolves the executor; only needed for local dev builds.
# executor: ./executor-node/dist/bin/executor.js

# ESM resolution fallback for workspace/monorepo scripts.
# resolve_from: /app/node_modules

storage:
  dataset: quarry
  backend: s3
  path: my-bucket/quarry-data
  region: us-east-1
  endpoint: https://ACCOUNT_ID.r2.cloudflarestorage.com
  s3_path_style: true

policy:
  name: buffered
  flush_mode: at_least_once
  buffer_events: 1000
  buffer_bytes: 10485760
  # Streaming policy example (v0.7.0):
  # name: streaming
  # flush_count: 10
  # flush_interval: 5s

proxies:
  iproyal_nyc:
    strategy: round_robin
    endpoints:
      - protocol: https
        host: geo.iproyal.com
        port: 12321
        username: ${IPROYAL_USER}
        password: ${IPROYAL_PASS}

  rotating_pool:
    strategy: random
    recency_window: 3   # avoid reusing the last 3 endpoints
    endpoints:
      - protocol: http
        host: proxy1.example.com
        port: 8080
      - protocol: http
        host: proxy2.example.com
        port: 8080
      - protocol: http
        host: proxy3.example.com
        port: 8080
      - protocol: http
        host: proxy4.example.com
        port: 8080
      - protocol: http
        host: proxy5.example.com
        port: 8080

  residential_sticky:
    strategy: sticky
    sticky:
      scope: domain
      ttl_ms: 3600000
    endpoints:
      - protocol: http
        host: proxy.example.com
        port: 8080

proxy:
  pool: iproyal_nyc
  strategy: round_robin

adapter:
  type: webhook
  url: https://hooks.example.com/quarry
  headers:
    Authorization: Bearer ${WEBHOOK_TOKEN}
  timeout: 10s
  retries: 3

# Event sinks for real-time event delivery (v0.13.0+).
# When absent, events go to Lode only (default behavior).
# events:
#   sinks:
#     - type: lode
#       delivery: mandatory
#     - type: redis
#       delivery: best_effort
#       url: redis://localhost:6379
#       stream_key: quarry:events
#       max_len: 100000
#       ttl: 24h
#       timeout: 2s
#       retries: 2
```

### Environment Variable Expansion

The config file supports `${VAR}` and `${VAR:-default}` syntax. Expansion
is applied to the raw YAML text before parsing.

- `${VAR}` — expands to the value of `VAR`, or empty string if unset
- `${VAR:-default}` — expands to `default` if `VAR` is unset or empty

Unset variables without defaults are not errors. Required secrets will
fail at downstream validation (e.g., proxy endpoint auth pair validation).

### Proxy Pools in Config

Proxy pools are defined inline under `proxies:`, keyed by pool name. This
replaces the deprecated `--proxy-config` JSON file. Using both
`--proxy-config` and config `proxies:` in the same invocation is an error.

### No Auto-Discovery

Config files are loaded only via explicit `--config <path>`. There is no
implicit search for `quarry.yaml` in the working directory.

---

## Config Files (Legacy)

### Proxy Pools (deprecated)

Proxy pool definitions were previously static JSON files referenced via
`--proxy-config`. This method still works but emits a deprecation warning.
Prefer defining pools in the YAML config file under `proxies:`.

```json
[
  {
    "name": "residential",
    "strategy": "round_robin",
    "endpoints": [
      { "protocol": "http", "host": "proxy1.example.com", "port": 8080 },
      { "protocol": "http", "host": "proxy2.example.com", "port": 8080 }
    ]
  }
]
```

See [`docs/guides/proxy.md`](proxy.md) for the full pool configuration schema.

### Job Payload

Job payloads can be provided inline (`--job`) or from a file (`--job-json`).
The payload must be a JSON object (not array, string, number, or null).

```bash
# Inline
quarry run --job '{"url": "https://example.com"}' ...

# From file
quarry run --job-json ./job.json ...
```

---

## AWS Credentials (S3 Backend)

When using `--storage-backend s3`, Quarry uses the AWS SDK v2 default
credential chain. No explicit credential flags exist.

Resolution order:
1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. Shared credentials file (`~/.aws/credentials`)
3. IAM instance role (EC2, ECS, Lambda)

Set `--storage-region` if the bucket is not in your SDK's default region.

For S3-compatible providers (R2, MinIO), set credentials via environment
variables and use `--storage-endpoint` to point at the provider's endpoint.

---

## Configuration Flow

```
quarry run (Go runtime)
  ├── CLI flags (highest precedence)
  ├── YAML config file (--config, project defaults)
  ├── Flag defaults (lowest precedence)
  └── Spawns executor subprocess
        ├── stdin JSON → run_id, attempt, job, proxy endpoint, browser_ws_endpoint
        ├── env vars → QUARRY_STEALTH, QUARRY_ADBLOCKER, QUARRY_NO_SANDBOX
        │     └── Controls Puppeteer browser behavior
        └── env vars → QUARRY_RESOLVE_FROM (from --resolve-from)
              └── Registers ESM resolve hook for workspace imports
```

The Go runtime merges CLI flags and config file values (CLI wins), resolves
all configuration, selects a proxy endpoint (if configured), and passes a
fully resolved run context to the executor via stdin. The executor reads
only its own environment variables for browser hardening — it does not
access CLI flags or config files.

---

## Common Recipes

### Minimal local run (with config)

```yaml
# quarry.yaml
source: my-source
storage:
  backend: fs
  path: ./data
```

```bash
quarry run --config quarry.yaml --script ./my-script.ts --run-id run-001
```

### Minimal local run (flags only)

```bash
quarry run \
  --script ./my-script.ts \
  --run-id run-001 \
  --source my-source \
  --storage-backend fs \
  --storage-path ./data
```

### S3 with buffered policy

```bash
quarry run \
  --script ./my-script.ts \
  --run-id run-001 \
  --source my-source \
  --storage-backend s3 \
  --storage-path my-bucket/quarry \
  --storage-region us-east-1 \
  --policy buffered \
  --buffer-events 500 \
  --buffer-bytes 10485760
```

### Streaming policy (long-running crawl)

```bash
quarry run \
  --script ./crawl.ts \
  --run-id crawl-001 \
  --source my-source \
  --storage-backend s3 \
  --storage-path my-bucket/quarry \
  --storage-region us-east-1 \
  --policy streaming \
  --flush-count 100 \
  --flush-interval 30s
```

### S3-compatible provider (Cloudflare R2)

```bash
quarry run \
  --script ./my-script.ts \
  --run-id run-001 \
  --source my-source \
  --storage-backend s3 \
  --storage-path my-bucket/quarry \
  --storage-endpoint https://ACCOUNT_ID.r2.cloudflarestorage.com \
  --storage-s3-path-style
```

Credentials via environment variables:
```bash
export AWS_ACCESS_KEY_ID=<r2-access-key>
export AWS_SECRET_ACCESS_KEY=<r2-secret-key>
```

### Proxy with stealth in CI

```bash
QUARRY_NO_SANDBOX=1 quarry run \
  --script ./my-script.ts \
  --run-id run-001 \
  --source my-source \
  --storage-backend fs \
  --storage-path ./data \
  --proxy-config ./proxies.json \
  --proxy-pool residential \
  --proxy-domain example.com
```

### Debug proxy resolution (dry run)

```bash
quarry debug resolve proxy residential \
  --proxy-config ./proxies.json \
  --strategy round_robin
```
