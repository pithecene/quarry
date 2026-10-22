# Quarry Public API

User-facing guide for Quarry.
Normative behavior is defined by contracts under [`docs/contracts/`](docs/contracts/).

---

## Prerequisites

| Tool | Version | Source |
|------|---------|--------|
| Go | 1.25.6 | [`quarry/go.mod`](quarry/go.mod) |
| Node.js | 23+ or 22.6+ | Native TS support (see note) |
| pnpm | 10.28.2 | `package.json#packageManager` |

**Node.js version note:** Quarry scripts are TypeScript and use Node's native
type-stripping. Node 23+ enables this by default. Node 22.6–22.x requires
`--experimental-strip-types` flag (set via `NODE_OPTIONS`). The SDK and
executor accept puppeteer `^24 || ^25`; puppeteer 25 requires Node 22.12+,
so on Node 22.6–22.11 stay on puppeteer 24. With puppeteer 25, browser
downloads honour `HTTP_PROXY`/`HTTPS_PROXY` only if `proxy-agent` is
installed alongside it (it became an optional peer dependency).

Quarry is **TypeScript-first** and **ESM-only**.

---

## Installation

### Via mise (recommended)

```bash
mise install github:pithecene/quarry@0.13.7
```

Or pin in your `mise.toml`:

```toml
[tools]
"github:pithecene/quarry" = "0.13.7"
```

### Via Docker

```bash
# Full image — includes Chrome for Testing + fonts (amd64 only, recommended)
docker pull ghcr.io/pithecene/quarry:0.13.7

# Slim image — no browser, multi-arch (BYO Chromium via --browser-ws-endpoint)
docker pull ghcr.io/pithecene/quarry:0.13.7-slim
```
See [docs/guides/container.md](docs/guides/container.md) for `docker run` and Docker Compose examples.

### SDK

Install the SDK in your script project:

```bash
npx jsr add @pithecene/quarry-sdk
```

Or via pnpm with GitHub Packages:

```bash
pnpm add @pithecene/quarry-sdk
```

### From source

```bash
git clone https://github.com/pithecene/quarry.git
cd quarry
pnpm install
task build
```

---

## Quick Start

### 1. Clone and Build

```bash
git clone https://github.com/pithecene/quarry.git
cd quarry
pnpm install
task build
```

### 2. Run an Example

```bash
mkdir -p ./quarry-data

./quarry/quarry run \
  --script ./examples/demo.ts \
  --run-id my-first-run \
  --source demo \
  --storage-backend fs \
  --storage-path ./quarry-data
```

Expected output:
```
run_id=my-first-run, attempt=1, outcome=success, duration=...
```

---

## Script Authoring

### Export Shape

Scripts **must** export a default async function:

```typescript
import type { QuarryContext } from "@pithecene/quarry-sdk";

export default async function run(ctx: QuarryContext): Promise<void> {
  // Your extraction logic here
}
```

This is the **only** supported entry point shape.

### Lifecycle Hooks

Scripts may export **optional lifecycle hooks** alongside the default function.
All hooks are optional — scripts that export none behave identically to today.

```typescript
import type { QuarryContext, PrepareResult, TerminalSignal, RunMeta } from "@pithecene/quarry-sdk";

// Runs before browser launch. Can skip or transform the job.
export function prepare(job: unknown, run: RunMeta): PrepareResult {
  if (isStale(job)) return { action: "skip", reason: "stale job" };
  return { action: "continue", job: enrichJob(job) };
}

// Runs before the main script function
export async function beforeRun(ctx: QuarryContext): Promise<void> { }

// Main script (required)
export default async function run(ctx: QuarryContext): Promise<void> { }

// Runs after script completes successfully (not called on error)
export async function afterRun(ctx: QuarryContext): Promise<void> { }

// Runs when the script throws (not called after manual terminal emit)
export async function onError(error: unknown, ctx: QuarryContext): Promise<void> { }

// Runs after script execution, before terminal event. Emit is still open.
export async function beforeTerminal(signal: TerminalSignal, ctx: QuarryContext): Promise<void> { }

// Runs after terminal emission attempt. Context exists but emit throws.
export async function cleanup(ctx: QuarryContext): Promise<void> { }
```

**Execution order:**

```
load script
    │
    ▼
[prepare]  ──skip──▶ emit run_complete({skipped}) → run_result → return
    │
    │ continue (optionally with transformed job)
    ▼
acquire browser → create context
    │
    ▼
[beforeRun] → script() → [afterRun] (success) / [onError] (error)
    │
    ▼
[beforeTerminal]  (emit still open)
    │
    ▼
auto-emit terminal → [cleanup] → run_result → return
```

**Hook rules:**

| Hook | When it runs | Receives | Error handling |
|------|-------------|----------|----------------|
| `prepare` | Before browser launch | `(job, run)` | Throw → crash (no browser launched) |
| `beforeRun` | Before script | `(ctx)` | Throw → enters error path |
| `afterRun` | After script success | `(ctx)` | Throw → enters error path |
| `onError` | After script error | `(error, ctx)` | Swallowed |
| `beforeTerminal` | Before terminal emit | `(signal, ctx)` | Swallowed |
| `cleanup` | After terminal attempt | `(ctx)` | Swallowed |

**`prepare` return values:**

| Return | Behavior |
|--------|----------|
| `{ action: "continue" }` | Proceed with original job |
| `{ action: "continue", job }` | Proceed with transformed job |
| `{ action: "skip" }` | Skip run — emit `run_complete({skipped: true})` |
| `{ action: "skip", reason }` | Skip run — emit `run_complete({skipped: true, reason})` |

> **Note:** When `prepare` returns `skip`, **no other hooks run** — there is no
> browser, no context, and no cleanup. The run completes immediately.

### Context Object

The `QuarryContext` provides:

| Property | Type | Description |
|----------|------|-------------|
| `job` | `unknown` | Job payload (from `--job` or `--job-json`) |
| `run` | `RunMeta` | Run metadata (run_id, attempt, job_id, etc.) |
| `page` | `Page` | Puppeteer page instance |
| `browser` | `Browser` | Puppeteer browser instance |
| `browserContext` | `BrowserContext` | Puppeteer browser context |
| `emit` | `EmitAPI` | Output emission interface |
| `storage` | `StorageAPI` | Sidecar file upload interface |
| `memory` | `MemoryAPI` | Memory pressure monitoring |

### Terminal Behavior

A script terminates in one of these ways:

1. **Normal return** — Executor emits `run_complete` automatically
2. **Throw error** — Executor emits `run_error` automatically
3. **Explicit `emit.runComplete()`** — Script signals success
4. **Explicit `emit.runError()`** — Script signals failure

After a terminal event, further emit calls are undefined behavior.

---

## Emit API

`emit.*` is the **primary output mechanism** for scripts. For sidecar
file uploads, see the Storage API below.

### Primary Methods

```typescript
// Emit structured data (primary output)
await ctx.emit.item({
  item_type: "product",
  data: { name: "Widget", price: 9.99 }
});

// Emit binary artifact (screenshot, PDF, etc.)
const artifactId = await ctx.emit.artifact({
  name: "screenshot.png",
  content_type: "image/png",
  data: buffer  // Buffer or Uint8Array
});

// Emit progress checkpoint
await ctx.emit.checkpoint({
  checkpoint_id: "page-2",
  note: "Completed page 2 of 10"
});
```

### Logging

```typescript
await ctx.emit.debug("Debug message", { field: "value" });
await ctx.emit.info("Info message");
await ctx.emit.warn("Warning message");
await ctx.emit.error("Error message");
```

### Advisory Methods (Not Guaranteed)

```typescript
// Suggest enqueueing additional work
await ctx.emit.enqueue({
  target: "detail-page",
  params: { url: "https://example.com/item/123" },
  // Optional partition overrides (default: inherit from root run)
  // source: "other-source",
  // category: "other-category",
});

// Suggest proxy rotation
await ctx.emit.rotateProxy({ reason: "rate limited" });
```

### Batching Enqueue Calls

For high fan-out workloads that discover hundreds or thousands of items,
`createBatcher` groups items into fewer, larger enqueue events — reducing
child run count and scheduling overhead.

```typescript
import { createBatcher } from "@pithecene/quarry-sdk";

export default async function run(ctx: QuarryContext): Promise<void> {
  const batcher = createBatcher(ctx.emit, {
    size: 50,          // items per batch
    target: "detail.ts",
  });

  for (const url of discoveredUrls) {
    await batcher.add({ url });  // auto-flushes every 50 items
  }
  await batcher.flush();          // emit any remaining items
}
```

- `size` must be a positive integer (throws `RangeError` otherwise)
- Each flush emits a single `enqueue` event with `params: { items: T[] }`
- Call `flush()` before `runComplete()` — items buffered after a terminal event are lost
- Optional `source` and `category` in options override partition keys for all batched enqueue events

### Storage API

`storage.put()` writes sidecar files directly to addressable storage paths,
bypassing the event pipeline.

```typescript
// Write a file to storage (e.g., images, CSVs, raw HTML)
const result = await ctx.storage.put({
  filename: "product-image.png",
  content_type: "image/png",
  data: buffer  // Buffer or Uint8Array
});

// result.key is the resolved Hive-partitioned storage path
// e.g. "datasets/quarry/partitions/source=my-source/category=default/day=2026-02-23/run_id=run-001/files/product-image.png"
console.log(result.key);

// Include the storage key in emitted items for downstream correlation
await ctx.emit.item({
  item_type: "product",
  data: { name: "Example", image_key: result.key }
});
```

Rules:
- Filename must be flat (no `/`, `\`, or `..`)
- Maximum file size: 8 MiB
- Shares ordering and fail-fast with `emit.*`
- Cannot be called after a terminal event (`run_complete` / `run_error`)
- Returns `StoragePutResult` with the resolved `key` (v0.11.0+)
- Promise rejects on backend write failure (v0.12.0+) — errors are recoverable

Files land at Hive-partitioned paths under the storage root:
```
<storage-path>/datasets/<dataset>/partitions/source=<s>/category=<c>/day=<d>/run_id=<r>/files/<filename>
```

#### Sidecar File Inventory (v0.13.4+)

Files written via `storage.put()` are automatically tracked in Lode snapshot
`Metadata` under the `sidecar_files` key. Downstream consumers can enumerate
files for a run via `snapshot.Manifest.Metadata["sidecar_files"]` instead of
prefix-scanning storage.

Each entry includes `path`, `filename`, `content_type`, and `size`. File refs
are flushed into the snapshot on event and metrics writes only (not chunk
writes). To reconstruct the full file inventory for a run, union
`sidecar_files` from all snapshots for that `run_id`.

See [`docs/contracts/CONTRACT_LODE.md`](docs/contracts/CONTRACT_LODE.md) § Sidecar File Inventory for the
normative specification.

### Batching File Uploads

For workloads that write many files, `createStorageBatcher` dispatches
multiple `storage.put()` calls with bounded concurrency — eliminating
user-code latency between puts without creating unbounded promise counts.

```typescript
import { createStorageBatcher } from "@pithecene/quarry-sdk";

export default async function run(ctx: QuarryContext): Promise<void> {
  const batch = createStorageBatcher(ctx.storage, { concurrency: 16 });

  for (const img of images) {
    batch.add({
      filename: img.name,
      content_type: "image/png",
      data: img.buffer,
    });
  }
  await batch.flush();  // wait for all writes before terminal event
}
```

- `concurrency` defaults to 16 (must be a positive integer)
- `flush()` must be called before terminal events — buffered writes are lost otherwise
- Fail-fast: first error rejects all queued writes and poisons the batcher
- Reusable after a successful `flush()`

### Memory Pressure API

`ctx.memory` provides pull-based memory monitoring for container-constrained
workloads. Reports node heap, browser heap, and cgroup usage with pressure
classification.

```typescript
export default async function run(ctx: QuarryContext): Promise<void> {
  // Check memory pressure before heavy operations
  if (await ctx.memory.isAbove("high")) {
    await ctx.emit.warn("Memory pressure is high, reducing batch size");
  }

  // Full snapshot with all sources
  const snap = await ctx.memory.snapshot();
  console.log(snap.node.ratio);     // 0.0–1.0
  console.log(snap.cgroup?.ratio);   // null if not in container
  console.log(snap.pressure);        // 'low' | 'moderate' | 'high' | 'critical'

  // Opt out of browser metrics (avoids CDP call)
  const nodeOnly = await ctx.memory.snapshot({ browser: false });
}
```

Pressure levels (default thresholds):

| Ratio | Level |
|-------|-------|
| < 0.5 | `low` |
| 0.5–0.7 | `moderate` |
| 0.7–0.9 | `high` |
| ≥ 0.9 | `critical` |

---

## CLI Reference

### Run Command

```bash
quarry run [options]
```

**Config file:**

| Flag | Description |
|------|-------------|
| `--config <path>` | Path to YAML config file with project-level defaults (see [`docs/guides/configuration.md`](docs/guides/configuration.md)) |

CLI flags always override config file values.

**Required flags:**

| Flag | Description |
|------|-------------|
| `--script <path>` | Path to TypeScript script |
| `--run-id <id>` | Unique run identifier |
| `--source <name>` | Source identifier for partitioning |
| `--storage-backend <fs\|s3>` | Storage backend |
| `--storage-path <path>` | Storage location |

> **Note:** `--source`, `--storage-backend`, and `--storage-path` can also
> be provided via `--config` file. They are required at runtime but do not
> need to appear on the CLI if set in the config.

**Optional flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--attempt <n>` | 1 | Attempt number |
| `--job <json>` | `{}` | Job payload as inline JSON object |
| `--job-json <path>` | | Path to JSON file containing job payload (must be object) |
| `--category <name>` | `default` | Category for partitioning |
| `--policy <strict\|buffered\|streaming>` | `strict` | Ingestion policy |
| `--flush-count <n>` | | Flush after N events (streaming policy) |
| `--flush-interval <duration>` | | Flush every interval, e.g. `5s` (streaming policy) |
| `--report <path>` | | Write structured JSON report to path on exit (use `-` for stderr) |
| `--quiet` | `false` | Suppress non-error output |

> **Note:** `--job` and `--job-json` are mutually exclusive. Using both is an error.
> The payload **must** be a top-level JSON object. Arrays, primitives, and null are rejected.

> **Streaming policy:** At least one of `--flush-count` or `--flush-interval` must be specified
> when `--policy=streaming`. Both may be specified; the first trigger to fire wins.
> See [`docs/guides/policy.md`](docs/guides/policy.md) for details.

**Adapter flags (event-bus notification):**

| Flag | Default | Description |
|------|---------|-------------|
| `--adapter <type>` | | Adapter type (`webhook`, `redis`) |
| `--adapter-url <url>` | | Endpoint URL (required when `--adapter` set) |
| `--adapter-header <key=value>` | | Custom HTTP header (repeatable, webhook only) |
| `--adapter-channel <name>` | `quarry:run_completed` | Pub/sub channel name (redis only) |
| `--adapter-timeout <duration>` | `10s` | Notification timeout |
| `--adapter-retries <n>` | `3` | Retry attempts |

**Event sink flags (real-time event delivery, v0.13.0+):**

| Flag | Default | Description |
|------|---------|-------------|
| `--event-sink <type>` | | Event sink type (`lode`, `redis`); repeatable |
| `--event-sink-lode-delivery` | `mandatory` | Delivery mode for Lode event sink |
| `--event-sink-redis-url <url>` | | Redis URL (required when `--event-sink` includes `redis`) |
| `--event-sink-redis-stream-key` | `quarry:events` | Redis Streams key |
| `--event-sink-redis-max-len` | `100000` | Max stream length (-1 disables) |
| `--event-sink-redis-ttl` | `24h` | Stream key expiry (-1 disables) |
| `--event-sink-redis-timeout` | `2s` | Per-operation timeout |
| `--event-sink-redis-retries` | `2` | Retry attempts |
| `--event-sink-redis-delivery` | `mandatory` | Delivery mode for Redis event sink |

> **Event sinks vs adapters:** Event sinks deliver events in real time during a
> run. Adapters fire once after a run completes. Both can be used together.
> When no `--event-sink` flags are set, events go to Lode only (default).
> See [`docs/contracts/CONTRACT_INTEGRATION.md`](docs/contracts/CONTRACT_INTEGRATION.md) and [`docs/guides/integration.md`](docs/guides/integration.md).

**Fan-out flags (derived work execution):**

| Flag | Default | Description |
|------|---------|-------------|
| `--depth <n>` | `0` | Maximum recursion depth (0 = disabled) |
| `--max-runs <n>` | | Total child run cap (required when `--depth > 0`) |
| `--parallel <n>` | `1` | Maximum concurrent child runs |

**Browser reuse:**

| Flag | Env Var | Description |
|------|---------|-------------|
| `--browser-ws-endpoint <url>` | `QUARRY_BROWSER_ENDPOINT` | Connect to an externally managed browser via WebSocket URL (skips per-run Chromium launch; see [`docs/guides/cli.md`](docs/guides/cli.md)) |
| `--no-browser-reuse` | — | Disable transparent browser reuse across runs (forces per-run Chromium launch) |

By default, Quarry transparently reuses a persistent Chromium process across sequential runs.
The browser auto-terminates after idle timeout (default 60s, configurable via `QUARRY_BROWSER_IDLE_TIMEOUT`).
`--browser-ws-endpoint` / `QUARRY_BROWSER_ENDPOINT` takes priority over transparent reuse when set.
When an external endpoint is configured, a pre-run health check verifies the browser is reachable.
See [Container Usage](docs/guides/container.md) for multi-crawler deployment patterns.

**Module resolution:**

| Flag | Description |
|------|-------------|
| `--resolve-from <path>` | Path to `node_modules` directory for ESM resolution fallback (monorepo/container support) |

When scripts import workspace packages that are not resolvable from the
script's directory, `--resolve-from` registers an ESM resolve hook to fall
back to the specified `node_modules`. See [`docs/contracts/CONTRACT_CLI.md`](docs/contracts/CONTRACT_CLI.md).

**Validation:**

| Flag | Description |
|------|-------------|
| `--dry-run` | Validate script loadability without executing a run (no browser, no storage) |

When `--dry-run` is set, only `--script` and `--run-id` are required. Storage,
policy, proxy, and adapter configuration are skipped entirely. The executor
loads the script module (with `--resolve-from` hook if configured), validates
the default export and hook signatures, and reports the result. Exit 0 = valid,
exit 1 = invalid.

```bash
# Validate a script loads correctly
quarry run --dry-run --script ./my-script.ts --run-id test --source test

# Validate with --resolve-from (monorepo/container)
quarry run --dry-run --script ./my-script.ts --run-id test --source test \
  --resolve-from /app/node_modules
```

**Advanced flags (development only):**

| Flag | Description |
|------|-------------|
| `--executor <path>` | Override executor path (auto-resolved in bundled binary; only needed for local development builds) |

**Storage flags (S3 / S3-compatible):**

| Flag | Description |
|------|-------------|
| `--storage-region <region>` | AWS region (uses default chain if omitted) |
| `--storage-endpoint <url>` | Custom S3 endpoint URL (e.g. Cloudflare R2, MinIO) |
| `--storage-s3-path-style` | Force path-style addressing (required by R2, MinIO) |

### Inspect Command

```bash
quarry inspect run <run-id>     # Inspect a specific run
quarry inspect job <job-id>     # Inspect a specific job
```

### Stats Command

```bash
quarry stats runs               # Run statistics
quarry stats jobs               # Job statistics
quarry stats metrics            # Contract metrics (requires --storage-backend, --storage-path)
```

---

## Examples

All examples use local fixtures (no external network calls).

| Example | Description | Events |
|---------|-------------|--------|
| [`examples/demo.ts`](examples/demo.ts) | Minimal item emission | 1 item |
| [`examples/static-html-list/`](examples/static-html-list/) | Parse HTML fixture | 3 items |
| [`examples/toy-pagination/`](examples/toy-pagination/) | Multi-page pagination | 6 items |
| [`examples/artifact-snapshot/`](examples/artifact-snapshot/) | Screenshot artifact | 1 artifact |
| [`examples/intentional-failure/`](examples/intentional-failure/) | Error handling test | 1 item + error |
| [`examples/hooks-prepare/`](examples/hooks-prepare/) | Job filtering with `prepare` | skipped or 1 item |
| [`examples/hooks-before-terminal/`](examples/hooks-before-terminal/) | Summary emission via `beforeTerminal` | items + summary |

Run all examples:
```bash
task examples
```

### Integration Pattern Examples

See [`examples/integration-patterns/`](examples/integration-patterns/) for downstream ETL trigger examples:
- Event-bus pattern (SNS/SQS, handler)
- Polling pattern (filesystem, S3)

### Example: Minimal Item Emission

```typescript
// examples/demo.ts
import type { QuarryContext } from "@pithecene/quarry-sdk";

export default async function run(ctx: QuarryContext): Promise<void> {
  await ctx.emit.item({
    item_type: "demo",
    data: { message: "hello from quarry" }
  });
}
```

### Example: Screenshot Artifact

```typescript
// examples/artifact-snapshot/script.ts
import type { QuarryContext } from "@pithecene/quarry-sdk";

export default async function run(ctx: QuarryContext): Promise<void> {
  await ctx.page.setContent("<h1>Hello</h1>");
  const data = await ctx.page.screenshot({ type: "png" });
  await ctx.emit.artifact({
    name: "snapshot.png",
    content_type: "image/png",
    data
  });
}
```

### Example: Skip Stale Jobs with `prepare`

```typescript
// examples/hooks-prepare/script.ts
import type { PrepareResult, QuarryContext, RunMeta } from "@pithecene/quarry-sdk";

type Job = { url: string; fetched_at?: string };

export function prepare(job: Job, _run: RunMeta): PrepareResult<Job> {
  if (job.fetched_at) {
    const age = Date.now() - new Date(job.fetched_at).getTime();
    if (age < 3_600_000) {
      return { action: "skip", reason: "fetched less than 1h ago" };
    }
  }
  return { action: "continue" };
}

export default async function run(ctx: QuarryContext<Job>): Promise<void> {
  await ctx.page.goto(ctx.job.url);
  const title = await ctx.page.title();
  await ctx.emit.item({ item_type: "page", data: { url: ctx.job.url, title } });
}
```

### Example: Emit Summary with `beforeTerminal`

```typescript
// examples/hooks-before-terminal/script.ts
import type { QuarryContext, TerminalSignal } from "@pithecene/quarry-sdk";

let itemCount = 0;

export default async function run(ctx: QuarryContext): Promise<void> {
  for (const product of ["Widget", "Gadget", "Gizmo"]) {
    await ctx.emit.item({ item_type: "product", data: { name: product } });
    itemCount++;
  }
}

export async function beforeTerminal(
  signal: TerminalSignal,
  ctx: QuarryContext
): Promise<void> {
  if (signal.outcome === "completed") {
    await ctx.emit.item({
      item_type: "run_summary",
      data: { total_items: itemCount }
    });
  }
}
```

---

## Troubleshooting

### "Module not found" for SDK

Ensure SDK is built:
```bash
pnpm -C sdk run build
```

### "Executor not found"

The executor is auto-resolved but must be built first:
```bash
pnpm -C executor-node run build
```

If auto-resolution fails, specify the path manually:
```bash
./quarry/quarry run --executor ./executor-node/dist/bin/executor.js ...
```

### Chrome/Puppeteer sandbox errors (CI)

Set environment variable:
```bash
QUARRY_NO_SANDBOX=1 quarry run ...
```

### "Contract version mismatch"

SDK and runtime versions must match. Rebuild both:
```bash
task build
```

### Storage path errors

- **FS backend**: Path must be a writable directory
- **S3 backend**: Format is `bucket/prefix`, credentials via AWS default chain

### JSON import attribute error

If the executor crashes with:
```
Module "file:///path/to/data.json" needs an import attribute of "type: json"
```

Node ESM requires an explicit import attribute for JSON modules:
```ts
// ✗ crashes under Node ESM
import data from './data.json'

// ✓ correct
import data from './data.json' with { type: 'json' }
```

This applies to your script and all of its **transitive dependencies**. Other
runtimes (Bun, bundlers, `tsx`) may accept bare JSON imports, but the Quarry
executor runs under Node ESM.

---

## Container Usage

Quarry ships container images via GHCR:

- **Full** (amd64 only): `ghcr.io/pithecene/quarry:0.13.7` — includes Chrome for Testing + fonts
- **Slim** (amd64 + arm64): `ghcr.io/pithecene/quarry:0.13.7-slim` — no browser (BYO via `--browser-ws-endpoint`)

For `docker run`, Docker Compose, and sidecar patterns, see [docs/guides/container.md](docs/guides/container.md).

---

## Known Limitations (v0.13.7)

1. **Single executor type**: Only Node.js executor supported
2. **No built-in retries**: Retry logic is caller's responsibility
3. **No streaming reads**: Artifacts must fit in memory (note: the `streaming` *ingestion policy* is unrelated — it controls write batching, not read access)
4. **No transactional storage writes**: S3 and S3-compatible providers (R2, MinIO) do not provide transactional guarantees across writes
5. **No job scheduling**: Quarry supports in-process derived work via `--depth` but is not a scheduler; external orchestration is the caller's responsibility
6. **Puppeteer required**: All scripts run in a browser context
7. **Integration adapters and sinks**: Webhook, Redis Pub/Sub, and Redis Streams are available. NATS and SNS adapters are planned. Three commands read persisted data, each only when both `--storage-backend` and `--storage-path` are given (any storage flag without both is an error; with no storage flag, `list runs` and `stats metrics` return shape-correct stub values, as do all other `list`/`inspect`/`stats` commands): `quarry list runs` lists runs from their run records, `quarry list events` returns one run's events, and `quarry stats metrics` returns the latest metrics record. They read the filesystem backend or S3, including S3-compatible providers (R2, MinIO) via `--storage-endpoint`/`--storage-s3-path-style`, as on `run`. Runs written before v0.14.0 have no run records until `quarry index rebuild` backfills them; backfilled runs carry only what storage proves (no start time, and no outcome, so they are listed as `unclassified`). A TypeScript reader in the SDK is planned for the same release (see [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md) § v0.14.0 Roadmap, #240). Temporal is an orchestration host, not an adapter, and is deferred until dogfooded. See [`docs/guides/integration.md`](docs/guides/integration.md).
8. **Fan-out partition defaults**: Child runs inherit the root run's `--source` and `--category` unless overridden via `emit.enqueue({ source, category })`. Overrides apply to individual child runs only and do not propagate to grandchildren.
9. **Fan-out target resolution**: `target` in `emit.enqueue()` is resolved as a file path relative to CWD (same as `--script`). Target resolution semantics may change in a future release; config-based logical names are under consideration.

---

## Storage Configuration

Storage is a **Quarry-level** concern. The underlying Lode subsystem is internal.

### Filesystem Backend

```bash
--storage-backend fs \
--storage-path /var/quarry/data
```

Data is stored in Hive-partitioned layout:
```
/var/quarry/data/
  source=my-source/
    category=default/
      day=2026-02-03/
        run_id=run-001/
          event_type=item/
            data.jsonl
```

### S3 Backend

```bash
--storage-backend s3 \
--storage-path my-bucket/quarry-data \
--storage-region us-east-1
```

Uses AWS default credential chain. IAM permissions required:
- `s3:PutObject`
- `s3:GetObject`
- `s3:ListBucket`

### S3-Compatible Providers (R2, MinIO)

Quarry supports S3-compatible storage providers via custom endpoint and path-style flags:

```bash
--storage-backend s3 \
--storage-path my-bucket/quarry-data \
--storage-endpoint https://ACCOUNT_ID.r2.cloudflarestorage.com \
--storage-s3-path-style
```

Set credentials via environment variables:
```bash
export AWS_ACCESS_KEY_ID=<access-key>
export AWS_SECRET_ACCESS_KEY=<secret-key>
```

---

## Downstream Integration

Quarry is an extraction runtime, not a full pipeline. For triggering downstream
processing after runs complete, see [docs/guides/integration.md](docs/guides/integration.md).

**Built-in adapters** (v0.5.0+): `--adapter webhook` sends an HTTP POST, and
`--adapter redis` publishes to a Redis pub/sub channel after each run completes.
See adapter flags above.

**Event sinks** (v0.13.0+): `--event-sink redis` streams every event to a Redis
Stream in real time during the run. Can be combined with adapters for both
real-time event streaming and post-run notification.

**Fallback pattern**: Polling-based triggers with idempotent checkpoints.

---

## Version Information

```bash
quarry version
# 0.13.7 (commit: ...)
```

SDK and runtime versions must match (lockstep versioning).

### Distribution Channels

| Component | Channel | Install |
|-----------|---------|---------|
| CLI binary | GitHub Releases | `mise install github:pithecene/quarry@0.13.7` |
| Container (full, amd64) | GHCR | `docker pull ghcr.io/pithecene/quarry:0.13.7` |
| Container (slim, multi-arch) | GHCR | `docker pull ghcr.io/pithecene/quarry:0.13.7-slim` |
| SDK | JSR | `npx jsr add @pithecene/quarry-sdk` |
| SDK | GitHub Packages | `pnpm add @pithecene/quarry-sdk` |
