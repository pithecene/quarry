# @pithecene/quarry-sdk

TypeScript SDK for writing Quarry extraction scripts.

## Installation

```bash
# via JSR (recommended)
npx jsr add @pithecene/quarry-sdk

# via GitHub Packages
pnpm add @pithecene/quarry-sdk
```

## Quick Start

```typescript
import type { QuarryScript } from '@pithecene/quarry-sdk'

interface MyJob {
  url: string
  maxItems: number
}

const script: QuarryScript<MyJob> = async (ctx) => {
  const { page, emit, job } = ctx

  await emit.info('Starting extraction', { url: job.url })

  await page.goto(job.url)

  const items = await page.$$eval('.product', (els) =>
    els.map((el) => ({
      name: el.querySelector('.name')?.textContent,
      price: el.querySelector('.price')?.textContent
    }))
  )

  for (const item of items.slice(0, job.maxItems)) {
    await emit.item({
      item_type: 'product',
      data: item
    })
  }

  // Take a screenshot
  const screenshot = await page.screenshot()
  await emit.artifact({
    name: 'final-page.png',
    content_type: 'image/png',
    data: screenshot
  })

  await emit.runComplete({
    summary: { itemCount: items.length }
  })
}

export default script
```

## API

### QuarryContext\<Job\>

The context object passed to your script.

| Property | Type | Description |
|----------|------|-------------|
| `job` | `Job` | Your job payload (immutable) |
| `run` | `RunMeta` | Run metadata (run_id, job_id, attempt) |
| `page` | `Page` | Puppeteer Page |
| `browser` | `Browser` | Puppeteer Browser |
| `browserContext` | `BrowserContext` | Puppeteer BrowserContext |
| `emit` | `EmitAPI` | Sole output mechanism |
| `storage` | `StorageAPI` | Sidecar file upload interface |
| `memory` | `MemoryAPI` | Memory pressure monitoring |

### EmitAPI

All methods are async and may block on backpressure.

| Method | Description |
|--------|-------------|
| `item({ item_type, data })` | Emit a structured record |
| `artifact({ name, content_type, data })` | Emit binary data, returns `ArtifactId` |
| `checkpoint({ checkpoint_id, note? })` | Mark progress |
| `enqueue({ target, params })` | Suggest additional work (advisory) |
| `rotateProxy({ reason? })` | Suggest proxy rotation (advisory) |
| `log({ level, message, fields? })` | Emit structured log |
| `debug/info/warn/error(message, fields?)` | Convenience log methods |
| `runError({ error_type, message, stack? })` | Signal fatal error |
| `runComplete({ summary? })` | Signal completion |

### StorageAPI

Sidecar file upload interface available via `ctx.storage`.

| Method | Description |
|--------|-------------|
| `put({ filename, content_type, data })` | Write a file to Hive-partitioned storage (max **8 MiB**, flat filename only). Returns `StoragePutResult` with the resolved `key`. |

Files land under the run's `files/` prefix. Use `emit.artifact()` for larger
payloads or when chunk-level progress tracking is needed.

Promise rejects on backend write failure (v0.12.0+) — errors are recoverable.

### createBatcher

Accumulates items and emits batched enqueue events, reducing child run count
for high fan-out workloads.

```typescript
import { createBatcher } from '@pithecene/quarry-sdk'

const batcher = createBatcher<{ url: string }>(ctx.emit, {
  size: 50,
  target: 'detail.ts',
})

await batcher.add({ url })   // auto-flushes every 50 items
await batcher.flush()         // emit remaining items
console.log(batcher.pending)  // 0
```

| Option | Type | Description |
|--------|------|-------------|
| `size` | `number` | Items per batch (positive integer, required) |
| `target` | `string` | Enqueue target script (required) |
| `source` | `string?` | Partition override |
| `category` | `string?` | Partition override |

### createStorageBatcher

Dispatches multiple `storage.put()` calls with bounded concurrency — eliminating
user-code latency between puts without creating unbounded promise counts.

```typescript
import { createStorageBatcher } from '@pithecene/quarry-sdk'

const batch = createStorageBatcher(ctx.storage, { concurrency: 16 })

for (const img of images) {
  batch.add({
    filename: img.name,
    content_type: 'image/png',
    data: img.buffer,
  })
}
await batch.flush()  // wait for all writes before terminal event
```

| Option | Type | Description |
|--------|------|-------------|
| `concurrency` | `number` | Max in-flight puts (positive integer, default 16) |

- `flush()` must be called before terminal events — buffered writes are lost otherwise
- Fail-fast: first error rejects all queued writes and poisons the batcher

### listRuns / readRun

Read persisted runs from Node.js (v0.14.0+), for example, to ingest finished
runs. Both run the `quarry` CLI, which must be installed (`quarryPath`
overrides the executable), and reject with `QuarryReadError` on failure.

```typescript
import { listRuns, readRun } from '@pithecene/quarry-sdk'

const storage = { backend: 'fs', path: './quarry-data' } as const
const page = await listRuns({ storage, source: 'demo', state: 'succeeded', limit: 100 })
const { events } = await readRun(
  { source: 'demo', category: 'default', day: '2026-10-22', runId: page.runs[0].run_id },
  { storage }
)
```

`page.next`, passed as `after`, returns the following page. See PUBLIC_API.md
§Reading Runs.

### MemoryAPI

Pull-based memory monitoring for container-constrained workloads.

```typescript
// Quick threshold check
if (await ctx.memory.isAbove('high')) {
  await ctx.emit.warn('Memory pressure high')
}

// Full snapshot
const snap = await ctx.memory.snapshot()
snap.node.ratio     // 0.0–1.0
snap.cgroup?.ratio  // null if not in container
snap.pressure       // 'low' | 'moderate' | 'high' | 'critical'
```

| Method | Description |
|--------|-------------|
| `snapshot(opts?)` | Returns `MemorySnapshot` with node, browser, cgroup sources |
| `isAbove(level)` | Returns `true` if current pressure ≥ level |

Pass `{ browser: false }` to `snapshot()` to skip the CDP call.

### Lifecycle Hooks

Export these from your script module for lifecycle control:

```typescript
import type { QuarryScriptModule } from '@pithecene/quarry-sdk'

const module: QuarryScriptModule<MyJob> = {
  async default(ctx) {
    // main script
  },
  async beforeRun(ctx) {
    // setup
  },
  async afterRun(ctx) {
    // teardown on success
  },
  async onError(error, ctx) {
    // error handling
  },
  async cleanup(ctx) {
    // always runs (finally)
  }
}

export default module.default
export const { beforeRun, afterRun, onError, cleanup } = module
```
