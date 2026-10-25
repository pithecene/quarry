# Quarry Emit — Contract

This document defines the **emit contract** between user scripts and the Quarry
runtime boundary. It defines the **event envelope**, **event types**, **ordering**,
**versioning**, and **error semantics**.

This is a contract document. Implementations must conform.

---

## Scope

- Applies to all events emitted by user scripts via `emit.*`.
- Applies to sidecar file uploads via `storage.put()`.
- Defines an envelope that must wrap every event.
- Defines initial event types and their invariants.
- Defines ordering guarantees per run.
- Defines error semantics across script, executor, and policy failures.

Non-goals:
- Does not define storage layouts or ingestion policy behavior.
- Does not define IPC framing (see CONTRACT_IPC.md).

---

## Envelope

Every event is a single **envelope object** with the following fields.
All fields are required unless marked optional.

- `contract_version` (string)
  - Semantic version string for this contract.
  - Must be present in every event.
- `event_id` (string)
  - Unique ID for the event.
  - Uniqueness is scoped to a run (see CONTRACT_RUN.md).
- `run_id` (string)
  - Canonical run identifier.
- `seq` (integer)
  - Monotonic sequence number per run.
  - Starts at 1 for each run.
- `type` (string)
  - One of the defined event types below.
- `ts` (string)
  - Event timestamp in ISO 8601 UTC.
- `payload` (object)
  - Type-specific payload.
- `job_id` (string, optional)
  - Included when known at emit-time.
- `parent_run_id` (string, optional)
  - Included when the run is a retry or child run.
- `attempt` (integer)
  - Attempt number. Always present. Starts at 1 for initial runs.

The envelope must be **stable** and **parseable without schema discovery**.

---

## Event Types

### 1) `item`
Represents a durable, structured output record.

Required payload fields:
- `item_type` (string) — caller-defined type label
- `data` (object) — the record payload

### 2) `artifact`
Represents a binary or large payload.

Required payload fields:
- `artifact_id` (string)
- `name` (string)
- `content_type` (string)
- `size_bytes` (integer)

The **artifact event is the commit record** for an artifact. Artifact bytes
may be transmitted before this event (see CONTRACT_IPC.md for ordering,
chunking, and `is_last` signaling).

Orphaned bytes (no corresponding artifact event) are eligible for GC.

### 3) `checkpoint`
Represents an explicit script checkpoint.

Required payload fields:
- `checkpoint_id` (string)
- `note` (string, optional)

### 4) `enqueue` (optional advisory)
Suggests the runtime consider enqueueing additional work.

Required payload fields:
- `target` (string)
- `params` (object)

Optional payload fields:
- `source` (string) — override the child run's source partition key (default: inherit from root)
- `category` (string) — override the child run's category partition key (default: inherit from root)

Semantics:
- Advisory only; not guaranteed or required.
- No feedback channel is implied.
- `source` and `category` are partition hints only. They do not affect dedup
  (dedup is by `(target, params)` only).

Runtime interpretation:
- Default (`--depth 0`): advisory only, as above.
- With `--depth > 0`: runtime schedules and executes as child runs.
  Deduplication and depth limits are applied by the fan-out operator.
  The contract itself is unchanged; runtime behavior depends on CLI flags.

### 5) `rotate_proxy` (optional advisory)
Suggests the runtime consider rotating proxy/session identity.

Required payload fields:
- `reason` (string, optional)

Semantics:
- Advisory only; not guaranteed or required.

### 6) `log`
Structured log event emitted by script.

Required payload fields:
- `level` (string) — `debug|info|warn|error`
- `message` (string)
- `fields` (object, optional)

### 7) `run_error`
Represents a script-level error that should terminate the run.

Required payload fields:
- `error_type` (string)
- `message` (string)
- `stack` (string, optional)

### 8) `run_complete`
Represents normal completion of the script.

Required payload fields:
- `summary` (object, optional)

#### Reserved summary fields

When the `prepare` hook returns `{ action: 'skip' }`, the executor emits
`run_complete` with the following reserved fields in `summary`:

- `skipped` (boolean) — `true` when the run was skipped by `prepare`
- `reason` (string, optional) — skip reason provided by the hook

Scripts should not use `skipped` or `reason` as summary keys for other
purposes. These fields are set by the executor, not by user code.

---

## Storage API (`storage.put()`)

Scripts may write sidecar files via `storage.put()`. These files bypass
the event envelope, sequence numbering, and policy pipeline entirely.

### Semantics

- `storage.put()` shares the same serialization chain as `emit.*` for
  ordering and fail-fast behavior.
- **Terminal boundary**: calling `storage.put()` after a terminal event
  (`run_complete` or `run_error`) throws `TerminalEventError`.
- **Not events**: file writes do not produce event envelopes and are
  not counted in `seq`.
- Files are transported as `file_write` IPC frames (see CONTRACT_IPC.md).

### Error Propagation

`storage.put()` reflects the backend write result. If the storage backend
(e.g. S3, local filesystem) fails, the returned promise **rejects** with
the error message from the runtime.

This is implemented via `file_write_ack` frames (see CONTRACT_IPC.md §File
Write Acknowledgement). The error is **recoverable**: the run continues and
the script decides how to handle the failure:

```typescript
try {
  await ctx.storage.put({ filename: 'img.png', contentType: 'image/png', data })
} catch (e) {
  ctx.emit.log({ level: 'warn', message: `storage failed: ${e.message}` })
}
```

### Return Value

`storage.put()` returns `Promise<StoragePutResult>`:

```typescript
interface StoragePutResult {
  /** The resolved Hive-partitioned storage key */
  key: string
}
```

The key follows the Hive partition formula:
```
datasets/{dataset}/partitions/source={source}/category={category}/day={day}/run_id={runID}/files/{filename}
```

All components are known at executor startup and passed via the `storage`
field in the executor stdin payload (see CONTRACT_IPC.md). The SDK computes
the key deterministically — no bidirectional IPC round-trip is needed.

When storage partition metadata is not provided (e.g. older CLI versions),
the key is an empty string.

### Filename Rules

- Must not be empty.
- Must not contain path separators (`/` or `\`).
- Must not contain `..`.

### Content Type

- Caller provides a `content_type` (MIME type) with each file.
- Content type is persisted as a companion `.meta.json` sidecar file
  alongside the data file in storage.

### Storage Batcher (`createStorageBatcher`)

`createStorageBatcher` is an SDK-level utility that dispatches multiple
`storage.put()` calls with bounded concurrency. It does not modify the
underlying IPC protocol or serialization semantics.

- Each file write still produces one `file_write` IPC frame.
- Calls queue through the existing serialization chain — the batcher
  eliminates user-code latency between successive puts.
- Concurrency is bounded (default: 16). At most N promises are in-flight
  at any time.
- **Fail-fast**: after the first `storage.put()` error, all queued and
  subsequent `add()` calls reject with that error. `flush()` rethrows.
- **Reusable**: the batcher may be reused after a successful `flush()`.
- **Must flush before terminal events**: buffered writes after
  `run_complete` or `run_error` will throw `TerminalEventError`.

```typescript
const batch = createStorageBatcher(ctx.storage, { concurrency: 16 })
batch.add({ filename, content_type, data })
await batch.flush()
```

---

## Memory Pressure API (`ctx.memory`)

`ctx.memory` provides a pull-based memory pressure API for proactive
memory management in container-constrained workloads.

### Sources

| Source | Method | Always available |
|--------|--------|-----------------|
| Node heap | `v8.getHeapStatistics()` + `process.memoryUsage()` | Yes |
| Browser | `page.metrics()` (CDP) | No (null when no browser or opted out) |
| Cgroup | `/sys/fs/cgroup/memory.{current,max}` (v2) or `memory.{usage_in_bytes,limit_in_bytes}` (v1) | No (null outside cgroup or when limit is unlimited) |

### Pressure Levels

| Ratio | Level |
|-------|-------|
| < 0.5 | `low` |
| 0.5–0.7 | `moderate` |
| 0.7–0.9 | `high` |
| ≥ 0.9 | `critical` |

Thresholds are configurable. The `pressure` field reports the highest
level across all available sources.

### Semantics

- **Pull-based**: scripts call `snapshot()` when they want; no push events.
- **SDK-local**: no IPC round-trips. Browser metrics require a CDP call (~1-5ms).
- **Graceful degradation**: unavailable sources return `null`.
- **No protocol changes**: memory pressure is entirely SDK-side.

---

## Ordering Guarantees

- **Total order per run** is guaranteed.
- The runtime must observe events in strictly increasing `seq`.
- No reordering across event types is permitted.
- The contract does not specify ordering across different runs.

---

## Versioning Rules

- The envelope includes `contract_version`.
- Contract changes must be **backward-compatible additive**.
  - New fields are allowed if optional.
  - New event types are allowed if optional for consumers.
- Removing fields or changing semantics is a **breaking change** and forbidden in 0.x.

---

## Error Semantics

Errors are classified by **origin** and **impact**.

### Script Abort (Emit `run_error`)
- The script detects a fatal error and emits `run_error`.
- The script **should** terminate shortly after.
- The runtime records the run as failed, with the error payload.

### Executor Crash (No `run_error`)
- The executor process exits unexpectedly.
- The runtime records a **crash** outcome.
- Any partial stream is considered incomplete.

### Policy Failure
- The ingestion policy fails to accept events.
- The runtime records a **policy failure** outcome.
- The script may still be running, but the run is terminated by the runtime.

In all cases, a failed run must be distinguishable by downstream consumers using
run metadata (see CONTRACT_RUN.md).
