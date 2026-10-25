# Quarry IPC and Streaming — Contract

This document defines the IPC framing and streaming behavior between
executor and runtime. It defines framing, encoding, chunking, backpressure,
and failure behavior.

This is a contract document. Implementations must conform.

---

## Scope

- Applies to the executor-runtime pipe or stream.
- Defines wire framing for event envelopes and artifact data.
- Defines backpressure semantics.

Non-goals:
- Does not define event envelope structure (see [CONTRACT_EMIT.md](CONTRACT_EMIT.md)).
- Does not define ingestion policy behavior.

---

## Framing Format

- **Frame structure**: length prefix + payload
- **Length prefix**: 4 bytes, unsigned big-endian integer
- **Payload**: encoded bytes as specified below

The stream is a sequence of frames. Each frame is one of:
- an **event frame** (encoded event envelope),
- an **artifact chunk frame** (binary chunk),
- a **file write frame** (sidecar file upload), or
- a **file write acknowledgement frame** (runtime → executor, via stdin)

---

## Payload Encoding

All frames use msgpack encoding:
- `msgpack`

Rationale: compact, stable, language-agnostic.

Event frames contain the msgpack-encoded EventEnvelope directly.
Artifact chunk frames contain a msgpack-encoded chunk envelope (see Artifact Chunking).
File write frames contain a msgpack-encoded file write envelope (see File Write Frames).

Decoding discrimination uses the `type` field:
- `'artifact_chunk'` → artifact chunk frame
- `'file_write'` → file write frame
- all other values (`'item'`, `'log'`, `'run_complete'`, etc.) → event envelope

---

## Run Control Payloads (Out of Band)

The runtime and executor may exchange **run control payloads** outside the
event stream (e.g., initial run request and final run result).
These payloads must:
- Use the ProxyEndpoint schema defined in [`CONTRACT_PROXY.md`](CONTRACT_PROXY.md) when a proxy is supplied.
- Never include proxy passwords in any result/summary payload.

### Run Request (Runtime → Executor)
If present, the run request includes optional fields:
- `proxy` (optional): `ProxyEndpoint`
- `storage` (optional): `StoragePartition` — Hive partition metadata
  for SDK-side key computation. When present, `storage.put()` returns the
  resolved storage key without a bidirectional IPC round-trip.

```
StoragePartition:
  dataset: string    # Lode dataset ID (e.g. "quarry")
  source: string     # partition key for origin system
  category: string   # partition key for data type
  day: string        # partition day (YYYY-MM-DD UTC)
  run_id: string     # run identifier
```

### Run Result (Executor → Runtime)
If present, the run result may include `proxy_used` metadata:
- `proxy_used` (optional): `ProxyEndpoint` **without** `password`

---

## Run Result Control Frame

The **run_result** frame is a control frame emitted by the executor after
determining run outcome. It provides structured outcome information and
optional proxy usage metadata.

### Frame Structure

The run_result frame is msgpack-encoded with the following fields:

```
RunResultFrame:
  type: "run_result"          # literal string discriminator
  outcome:
    status: string            # "completed" | "error" | "crash"
    message: string | null    # optional error/status message
    error_type: string | null # optional error type (e.g., "TypeError")
    stack: string | null      # optional stack trace
  proxy_used: ProxyEndpointRedacted | null  # optional, no password
```

Where `ProxyEndpointRedacted` is:
```
ProxyEndpointRedacted:
  protocol: string    # "http" | "https" | "socks5"
  host: string
  port: integer
  username: string | null
```

### Control Frame Semantics

- **Not an event**: The run_result frame does NOT affect event sequence ordering.
  It is a control frame, not counted in `seq`.
- **Single emission**: The executor emits exactly one run_result frame per run,
  after the terminal event.
- **Duplicate handling**: If multiple run_result frames are received, only the
  first is processed; subsequent frames are ignored.
- **Exit code authority**: Exit codes are authoritative for outcome classification.
  If exit code conflicts with run_result status, exit code wins. The run_result
  provides supplementary context (message, error_type, stack) but does not
  override exit code determination.

### Outcome Status Mapping

Exit codes are authoritative. The table shows how exit code determines
the final outcome regardless of run_result status:

| Exit code | Meaning        | Final outcome    | run_result used for |
|-----------|----------------|------------------|---------------------|
| 0         | completed      | success          | context only        |
| 1         | script error   | script_error     | message, stack      |
| 2         | executor crash | executor_crash   | message, stack      |
| 3         | invalid input  | executor_crash   | message             |

If run_result status conflicts with exit code (e.g., run_result says
"completed" but exit code is 1), exit code wins and a warning is logged.
The run_result's message, error_type, and stack are still used for context.

---

## Maximum Frame Size

- Maximum frame size: **16 MiB**
- Frames exceeding this limit are invalid and must be rejected.

Artifacts larger than 16 MiB must be chunked (see below).

---

## Artifact Chunking

Artifacts are transmitted as:
1) One or more **artifact chunk frames** (bytes).
2) An `artifact` event (metadata envelope) — the **commit record**.

Chunk rules:
- **Chunk size**: up to 8 MiB of raw bytes.
- **Ordering**: strictly in order; chunk `seq` starts at 1.
- **Completion**: the final chunk has `is_last: true`.
- **Reassembly**: consumer reassembles using `artifact_id` and `seq`.
  Reassembly does not require the artifact event; `is_last` signals
  when all bytes have arrived.

Chunk frame payload layout (msgpack-encoded envelope):
- `type` = `artifact_chunk`
- `artifact_id` (string)
- `seq` (integer, starting at 1)
- `is_last` (boolean)
- `data` (bytes)

The `artifact_chunk` envelope is not a normal emit event and does not use
the standard event envelope. It is a stream-level construct.

---

## Artifact Commit Semantics

The **artifact event** is the authoritative **commit record** for an artifact.

### Ordering Guarantees

- Artifact bytes (chunk frames) **MAY** precede the artifact event.
- The runtime **MUST** accept artifact chunk frames keyed by `artifact_id`
  even if the corresponding artifact event has not yet been observed.
- The runtime **MUST NOT** treat an artifact as "existent" until the
  artifact event is received.

### Orphaned Blobs

- If artifact bytes arrive but no artifact event follows (e.g., script crash),
  the bytes are **orphaned** and eligible for garbage collection.
- GC strategy is implementation-defined (e.g., blobs older than N hours with
  no manifest reference).

### Size Constraints

- Maximum artifact size: implementation-defined (recommended: 1 GiB).
- Maximum chunk size: 8 MiB (per Artifact Chunking above).
- Artifacts exceeding the maximum size **MUST** be rejected by the runtime.

---

## File Write Frames

File write frames transport sidecar files via `storage.put()`.
They bypass the event sequence (`seq`), policy pipeline, and artifact
chunk/manifest machinery. Files are written directly to Lode Store
at Hive-partitioned paths.

File write frame payload layout (msgpack-encoded):
- `type` = `file_write`
- `write_id` (uint32) — monotonic correlation ID, starts at 1. Zero means no ack expected (legacy).
- `filename` (string) — flat filename, no path separators or `..`
- `content_type` (string) — MIME type
- `data` (bytes) — file contents

Constraints:
- **Maximum data size**: 8 MiB (same limit as artifact chunks).
- **Single-frame**: file writes are not chunked.
- **Terminal boundary**: file write frames received after a terminal event
  are rejected with an error ack (`ok: false`, `"run already terminated"`)
  to guarantee executor-side promise settlement.
- **Not events**: file write frames do not affect event sequence ordering
  and are not counted in `seq`.

---

## File Write Acknowledgement (Runtime → Executor)

After processing a `file_write` frame, the runtime sends a `file_write_ack`
frame back to the executor via stdin. This is the only bidirectional IPC
in Quarry; all other frames flow executor→runtime via stdout.

### Two-Phase Stdin

Go's `json.NewEncoder` appends a newline after the initial JSON metadata.
After the executor reads metadata (phase 1), stdin remains open for ack
frames (phase 2). The executor's `AckReader` reads length-prefixed msgpack
frames from stdin and matches them to pending `storage.put()` promises
via `write_id`.

### Ack Frame Layout

File write ack frame payload (msgpack-encoded):
- `type` = `file_write_ack`
- `write_id` (uint32) — correlation ID from the `file_write` frame
- `ok` (bool) — `true` if the backend write succeeded
- `error` (string | null) — error message when `ok` is `false`

Same wire format as all IPC frames: 4-byte big-endian length prefix + msgpack payload.

### Error Model: Recoverable

On storage backend failure (`PutFile()` error), the runtime:
1. Sends `file_write_ack { ok: false, error: "..." }` to executor stdin.
2. Does **NOT** return a stream error — the ingestion loop continues.
3. Increments the `lode_write_failure` metric.
4. Logs the error.

The SDK's `storage.put()` promise rejects. The script can catch and continue:
```typescript
try {
  await ctx.storage.put({ filename: 'img.png', ... })
} catch (e) {
  ctx.emit.log({ level: 'warn', message: `storage failed: ${e.message}` })
}
```

### Backward Compatibility

- **Old runtime + new executor**: Runtime closes stdin after metadata. AckReader
  detects EOF without having received any ack frames — detected as "no ack
  support". All pending and future `waitForAck()` calls resolve immediately.
  StdioSink falls back to fire-and-forget.
- **New runtime + old executor**: Old executor must be updated to use two-phase
  stdin reading. The runtime sends acks which old executors would ignore.

### No Per-Ack Timeout

Ack latency is bounded by storage backend I/O latency. Terminal conditions:
- **stdin EOF**: runtime closed pipe → reject all pending promises.
- **Process exit**: stdin stream ends → same as EOF.
- **Malformed frame**: ignored (future frame types may appear).

---

## Backpressure Semantics

- **Emit calls must block on backpressure.**
- The executor **must not** buffer unboundedly.
- The executor **must not** drop frames.
- Dropping is a policy-layer decision only.

If the runtime applies backpressure, the executor stalls, and the script blocks.
This makes loss explicit and avoids hidden queues.

---

## Failure Behavior

### Partial Frame
- If a frame is truncated or malformed, the runtime must treat it as a fatal
  stream error and terminate the run.

### Pipe Closure
- If the pipe closes cleanly after a valid `run_complete`, the run is complete.
- If the pipe closes without `run_complete`, the runtime records a crash or
  premature termination (see [CONTRACT_EMIT.md](CONTRACT_EMIT.md) error semantics).

---

## Invariants

- Frame boundaries are authoritative; no frame may contain multiple envelopes.
- The runtime must not attempt to resynchronize after invalid framing.
- Event envelopes must be decoded before ingestion policy handling.
