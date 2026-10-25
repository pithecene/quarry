# Emitting Data

A script reports everything it observes through `emit.*`: items,
artifacts, checkpoints, logs, and the event that ends the run. This guide
explains what to emit and when, the ordering Quarry guarantees, sidecar
file uploads, and how events stream to the runtime. The
[emit contract](../contracts/CONTRACT_EMIT.md) states the rules.

---

## The Emit Surface

Scripts emit all output through `emit.*`. There are no side channels.

Event types include:
- `item`: structured records
- `artifact`: binary or large payloads
- `checkpoint`: script-defined progress markers
- `log`: structured logs
- `run_error`: fatal errors
- `run_complete`: normal completion

Some advisory types exist (`enqueue`, `rotate_proxy`), but they are optional.

---

## When to Emit What

- **Records**: use `item` for the primary outputs of a run.
- **Large content**: use `artifact` for files, blobs, or large text.
- **Progress**: use `checkpoint` to mark milestones.
- **Errors**: emit `run_error` and then terminate.
- **Completion**: emit `run_complete` once the script finishes.

> **Tip:** The `beforeTerminal` lifecycle hook fires after script execution
> but before the terminal event, with emit still open. This is useful for
> emitting summary items or final metadata. See the [SDK and setup guide](../../PUBLIC_API.md) (Lifecycle
> Hooks section) for details.

---

## Ordering Guarantees

Events are ordered within a run. The runtime expects strictly increasing
sequence numbers and does not reorder types.

---

## Advisory Events

The `enqueue` and `rotateProxy` emit methods are **advisory**. They express
intent but carry no delivery or execution guarantee.

- `emit.enqueue({ target, params, source?, category? })`: suggests additional
  work to the runtime. The runtime may ignore it, deduplicate it, or defer it.
  Optional `source` and `category` override the child run's partition keys
  (default: inherit from root).
- `emit.rotateProxy({ reason? })`: hints that the current proxy should be
  rotated. The runtime applies rotation only if a proxy pool is configured
  and the strategy supports mid-run changes.

Scripts should not depend on advisory events being acted upon.

### Fan-Out Activation

When `quarry run` is invoked with `--depth <n>` (where n > 0), the runtime
**acts on** enqueue events by executing them as child runs:

- `target` names the script to execute (resolved relative to CWD).
- `params` becomes the child run's job payload.
- Identical `(target, params)` pairs are deduplicated.
- Child runs can themselves emit enqueue events (up to the depth limit).

Without `--depth`, enqueue remains purely advisory. The emit contract is
unchanged. The runtime's interpretation depends on CLI flags.

### Batching for High Fan-Out

When a script discovers hundreds or thousands of items, emitting one enqueue
per item creates an equal number of child runs. `createBatcher` groups items
into fewer, larger enqueue events:

```typescript
import { createBatcher } from "@pithecene/quarry-sdk";

const batcher = createBatcher(ctx.emit, {
  size: 50,
  target: "process-batch.ts",
});

for (const url of discoveredUrls) {
  await batcher.add({ url });  // auto-flushes every 50 items
}
await batcher.flush();          // emit remaining partial batch
```

Each flush emits a single enqueue event. The child script receives
`{ items: T[] }` as its job payload.

---

## Convenience Log Methods

The methods `emit.debug()`, `emit.info()`, `emit.warn()`, and `emit.error()` are
shortcuts for `emit.log()` with a preset level:

```typescript
await emit.info('page loaded', { url })
// equivalent to:
await emit.log({ level: 'info', message: 'page loaded', fields: { url } })
```

All four methods accept `(message: string, fields?: Record<string, unknown>)`.

---

## Sidecar File Uploads

For files that do not fit the `emit.artifact()` chunked-streaming model,
scripts can write directly to storage via `ctx.storage.put()`:

```typescript
const result = await ctx.storage.put({
  filename: 'report.json',
  content_type: 'application/json',
  data: Buffer.from(JSON.stringify(report))
})
// result is StoragePutResult { key: string }
// key contains the full storage path where the file was written
console.log(result.key) // e.g. "datasets/quarry/partitions/source=demo/category=default/day=2026-02-23/run_id=run-001/files/report.json"
```

Constraints:
- Maximum file size: **8 MiB**
- Filename must be flat (no path separators, no `..`)
- Files land under the run's Hive-partitioned `files/` prefix

Use `emit.artifact()` for larger payloads or when chunk-level progress
tracking is needed.

---

## Streaming and Backpressure

The executor sends each event to the runtime as soon as the script emits
it, and the runtime applies the ingestion policy as events arrive. The
[IPC contract](../contracts/CONTRACT_IPC.md) states the framing and
ordering rules.

> The `streaming` ingestion policy is a separate subject: it controls how
> the runtime batches writes to storage. The [policy guide](policy.md)
> describes it.

### Artifacts

The executor sends an artifact's bytes as one or more chunk frames, and
then the `artifact` event. The event is the commit record: until it
arrives, the runtime treats the artifact as absent.

- Chunks may arrive before the artifact event.
- Chunks that no artifact event commits may be garbage-collected.

### Backpressure

When the runtime applies backpressure, the executor stalls and the
script's emit calls wait. No hidden buffer sits between them, so an event
is never lost silently.

---

## Versioning Notes

The emit envelope includes a contract version string. Contract changes are
additive only. Breaking changes require explicit version bumps.
