# Downstream Integration Guide

When a run finishes, a downstream system often needs to process its data.
This guide describes two ways to start that work: an event bus that Quarry
notifies, which is recommended, and polling storage, which is the
fallback. The [integration contract](../contracts/CONTRACT_INTEGRATION.md)
states the rules.

---

## Overview

Quarry is an **extraction runtime**, not an end-to-end pipeline. After a run
completes, downstream systems typically need to:

1. Detect that new data is available
2. Read the emitted events and artifacts
3. Transform and load data into target systems

This guide describes two patterns for detecting run completion:

| Pattern | Default | Latency | Complexity | Guarantees |
|---------|---------|---------|------------|------------|
| Event-bus | ✅ Recommended | Low | Medium | Best effort, with retries and possible duplicates |
| Polling | Fallback | Higher | Lower | Eventual consistency |

---

## Event-Bus Pattern (Recommended)

The event-bus pattern triggers downstream processing immediately when Quarry
publishes a run completion signal.

### Architecture

```
Quarry run completes
        ↓
Storage commit (configured backend)
        ↓
Event published to bus
        ↓
Downstream consumer receives event
        ↓
Consumer reads the run through the locator
        ↓
Transform / Load
```

### Visibility Boundary

The event should be published **after** the storage commit is durable.
This ensures consumers can read the data referenced in the event.

Per the [run contract](../contracts/CONTRACT_RUN.md), the visibility boundary is:

- `run_complete` or `run_error` is emitted
- All preceding events are persisted
- Storage sink is flushed and closed

Only publish to the event bus after this boundary is crossed.

### Event Payload

The [webhook payload](#webhook-payload) shows the fields. A consumer filters by `source` and
`category`, decides by `outcome`, and reads the run through `locator`.
`storage_path` is for people to read.

### Failure and Retry Considerations

1. **Publisher failures**: If publishing fails after storage commit, the run
   data exists but consumers will not be notified. Implement retry with backoff.

2. **Consumer failures**: Consumers should be idempotent. Processing the same
   run twice should produce the same result (or be a no-op).

3. **Out-of-order delivery**: Events may arrive out of order. Consumers should
   handle this gracefully (e.g., by checking timestamps or sequence numbers).

4. **Dead letter queue**: Route failed events to a DLQ for manual inspection.

### Webhook Adapter

Quarry ships a built-in webhook adapter that POSTs a JSON event to a
configurable URL after each run whose records Quarry confirmed in storage. The
CLI warns on stderr when it skips a notification.

```bash
quarry run \
  --script ./script.ts \
  --run-id run-001 \
  --source my-source \
  --storage-backend fs \
  --storage-path ./data \
  --adapter webhook \
  --adapter-url https://hooks.example.com/quarry \
  --adapter-header "Authorization=Bearer $TOKEN"
```

The adapter publishes once Quarry has confirmed both of the run's records in storage. If the
webhook fails (after retries), the warning is logged to stderr but the
run exits with its normal outcome code.

#### Webhook Payload

```json
{
  "contract_version": "0.5.0",
  "event_type": "run_completed",
  "run_id": "run-001",
  "source": "my-source",
  "category": "default",
  "day": "2026-02-07",
  "outcome": "success",
  "storage_path": "file:///data/datasets/quarry/partitions/source=my-source/category=default/day=2026-02-07/run_id=run-001",
  "timestamp": "2026-02-07T12:00:00Z",
  "attempt": 1,
  "event_count": 42,
  "duration_ms": 1500,
  "locator": {
    "run": { "source": "my-source", "category": "default", "day": "2026-02-07", "runId": "run-001" },
    "storage": { "backend": "fs", "path": "/data", "dataset": "quarry" }
  }
}
```

A consumer reads the run with the SDK's
`readRun(event.locator.run, { storage: event.locator.storage })`.
The locator says where the run is and grants no access. Before it reads, a consumer
checks that the notification comes from its publisher, that `locator.run` is the run that
the notification names, and that `locator.storage` is storage it permits.

The locator carries `--storage-endpoint`. With either adapter and S3 storage,
`quarry run` refuses to start when that endpoint has a path other than `/`, because a
path can hold a secret. Set
`--adapter-publish-endpoint-path` when yours holds none.

#### Adapter Options

| Flag | Default | Description |
|------|---------|-------------|
| `--adapter-timeout` | `10s` | Per-request timeout |
| `--adapter-retries` | `3` | Retry attempts with exponential backoff |
| `--adapter-header` | | Custom header (repeatable, `key=value` format) |

### Redis Pub/Sub Adapter

Quarry ships a built-in Redis pub/sub adapter that publishes a JSON event
to a configurable Redis channel after each run whose records Quarry confirmed in
storage. The CLI warns on stderr when it skips a notification.

```bash
quarry run \
  --script ./script.ts \
  --run-id run-001 \
  --source my-source \
  --storage-backend fs \
  --storage-path ./data \
  --adapter redis \
  --adapter-url redis://localhost:6379/0 \
  --adapter-channel quarry:run_completed
```

The adapter publishes once Quarry has confirmed both of the run's records in storage. If the
publish fails (after retries), the warning is logged to stderr but the
run exits with its normal outcome code.

#### Redis Adapter Options

| Flag | Default | Description |
|------|---------|-------------|
| `--adapter-channel` | `quarry:run_completed` | Pub/sub channel name |
| `--adapter-timeout` | `5s` | Per-publish timeout |
| `--adapter-retries` | `3` | Retry attempts with exponential backoff |

The `--adapter-header` flag is ignored for the Redis adapter (with a warning).

#### YAML Config Example

```yaml
adapter:
  type: redis
  url: redis://localhost:6379/0
  channel: quarry:run_completed
  timeout: 5s
  retries: 3
```

### Redis Streams Event Sink

Unlike the adapters above, which fire once after a run completes, the Redis
Streams **event sink** publishes every event in real time as the executor
produces it. This enables downstream consumers to watch events across all
runs on a single stream.

```bash
quarry run \
  --script ./script.ts \
  --run-id run-001 \
  --source my-source \
  --storage-backend fs \
  --storage-path ./data \
  --event-sink lode \
  --event-sink redis \
  --event-sink-redis-url redis://localhost:6379
```

Each event is published via `XADD` with flat fields: `run_id`, `event_type`,
`seq`, `timestamp`, `source`, `category`, and `payload` (JSON-encoded).

#### Event Sink vs Adapter

| | Event Sink | Adapter |
|---|---|---|
| **Timing** | During run (real-time) | After run completes |
| **Events** | Every event | Single `run_completed` summary |
| **Failure** | Configurable (`mandatory` or `best_effort`) | Always best-effort |
| **Transport** | Redis Streams (`XADD`) | Redis Pub/Sub (`PUBLISH`) or HTTP POST |
| **Flags** | `--event-sink` | `--adapter` |

Both can be used together in the same run.

#### YAML Config Example

```yaml
events:
  sinks:
    - type: lode
      delivery: mandatory
    - type: redis
      delivery: best_effort
      url: redis://localhost:6379
      stream_key: quarry:events
      max_len: 100000
      ttl: 24h
```

#### Event Sink Options

| Flag | Default | Description |
|------|---------|-------------|
| `--event-sink-redis-stream-key` | `quarry:events` | Shared stream key |
| `--event-sink-redis-max-len` | `100000` | Approximate stream cap (-1 disables) |
| `--event-sink-redis-ttl` | `24h` | Key expiry (-1 disables) |
| `--event-sink-redis-timeout` | `2s` | Per-operation timeout |
| `--event-sink-redis-retries` | `2` | Retry attempts with exponential backoff |
| `--event-sink-redis-delivery` | `mandatory` | `mandatory` or `best_effort` |

See the [integration contract](../contracts/CONTRACT_INTEGRATION.md) §Event Sink Model for full
semantics.

---

## Polling Pattern (Fallback)

The polling pattern periodically scans storage for new runs. Use this when:

- Event infrastructure is unavailable
- Simplicity is preferred over latency
- Batch processing windows are acceptable

### Architecture

```
Poller runs on schedule (cron, etc.)
        ↓
List succeeded runs from their run records
        ↓
Compare against checkpoint (last processed)
        ↓
Process new runs
        ↓
Update checkpoint
```

### Checkpoint Management

Maintain a checkpoint of the run IDs that were processed. Runs list in key order,
not by time, so a timestamp cannot mark where the last poll stopped.

The checkpoint should be **durable** (stored in a database or file) to survive
poller restarts.

### Idempotent Processing

Because polling may re-scan the same runs, processors must be idempotent:

- Check if a run has already been processed before starting
- Use upserts instead of inserts for target systems
- Track processed run IDs in the checkpoint

### Example

[`polling-runs.ts`](../../examples/integration-patterns/polling-runs.ts) polls
filesystem storage with the SDK's `listRuns` and `readRun`. Its header states the change
for S3. It takes completion
from the run records. Storage paths and modification times do not establish it.

---

## Choosing a Pattern

| Factor | Event-Bus | Polling |
|--------|-----------|---------|
| Latency requirement | Real-time | Batch OK |
| Infrastructure | Event bus available | Minimal |
| Ordering needs | Strict | Relaxed |
| Retry complexity | Higher | Lower |
| Cost at scale | Per-event | Per-poll |

**Recommendation**: Start with event-bus for production workloads. Use polling
for development, testing, or when event infrastructure is unavailable.

---

## Anti-Patterns

### Watching for file changes directly

Do not use filesystem watchers (inotify, fswatch) on Quarry storage paths.
These tools may trigger before the storage commit is complete, leading to
partial reads.

### Assuming immediate availability

After `quarry run` exits, data visibility depends on the storage backend's
consistency model. Consumers must respect the visibility boundary defined in the
[run contract](../contracts/CONTRACT_RUN.md) and implement idempotent processing to handle edge cases
where data appears after the run signal.

### Processing without idempotency

Without idempotent processing, duplicate events or re-polled runs will
create duplicate records in target systems.

---

## Related Documents

- [Run contract](../contracts/CONTRACT_RUN.md): run lifecycle and terminal states
- [Storage contract](../contracts/CONTRACT_LODE.md): storage interaction semantics
- [Run guide](run.md): run lifecycle for script authors and operators
