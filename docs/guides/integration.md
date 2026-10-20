# Downstream Integration Guide

This guide describes patterns for triggering downstream ETL or processing systems
after Quarry runs complete. It is **non-normative**; the authoritative contracts
remain in [`docs/contracts/`](../contracts/).

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
| Event-bus | ✅ Recommended | Low | Medium | At-least-once delivery |
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
Consumer reads from storage path
        ↓
Transform / Load
```

### Visibility Boundary

The event should be published **after** the storage commit is durable.
This ensures consumers can read the data referenced in the event.

Per [`CONTRACT_RUN.md`](../contracts/CONTRACT_RUN.md), the visibility boundary is:

- `run_complete` or `run_error` is emitted
- All preceding events are persisted
- Storage sink is flushed and closed

Only publish to the event bus after this boundary is crossed.

### Event Payload

A minimal event payload should include:

```json
{
  "event_type": "run_completed",
  "run_id": "run-001",
  "source": "my-source",
  "category": "default",
  "day": "2026-02-04",
  "outcome": "success",
  "storage_path": "s3://bucket/source=my-source/category=default/day=2026-02-04/run_id=run-001",
  "timestamp": "2026-02-04T12:00:00Z"
}
```

Include enough information for consumers to:
- Filter events by source/category
- Locate data in storage
- Decide whether to process (based on outcome)

### Failure and Retry Considerations

1. **Publisher failures**: If publishing fails after storage commit, the run
   data exists but consumers won't be notified. Implement retry with backoff.

2. **Consumer failures**: Consumers should be idempotent. Processing the same
   run twice should produce the same result (or be a no-op).

3. **Out-of-order delivery**: Events may arrive out of order. Consumers should
   handle this gracefully (e.g., by checking timestamps or sequence numbers).

4. **Dead letter queue**: Route failed events to a DLQ for manual inspection.

### Webhook Adapter (v0.5.0+)

Quarry ships a built-in webhook adapter that POSTs a JSON event to a
configurable URL after each run completes.

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

The adapter publishes after storage commit and metrics persist. If the
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
  "duration_ms": 1500
}
```

#### Adapter Options

| Flag | Default | Description |
|------|---------|-------------|
| `--adapter-timeout` | `10s` | Per-request timeout |
| `--adapter-retries` | `3` | Retry attempts with exponential backoff |
| `--adapter-header` | | Custom header (repeatable, `key=value` format) |

### Redis Pub/Sub Adapter (v0.5.0+)

Quarry ships a built-in Redis pub/sub adapter that publishes a JSON event
to a configurable Redis channel after each run completes.

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

The adapter publishes after storage commit and metrics persist. If the
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

### Redis Streams Event Sink (v0.13.0+)

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

See [`docs/contracts/CONTRACT_INTEGRATION.md`](../contracts/CONTRACT_INTEGRATION.md) §Event Sink Model for full
semantics.

### Interim Pattern (v0.4.x)

Before v0.5.0, use a shell wrapper to trigger downstream notifications:

```bash
#!/bin/bash
# Wrapper: run + notify
quarry run ... "$@"
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then OUTCOME="success"; else OUTCOME="failure"; fi

curl -X POST "$WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  -d "{\"event_type\":\"run_completed\",\"run_id\":\"$RUN_ID\",\"outcome\":\"$OUTCOME\"}"

exit $EXIT_CODE
```

This is superseded by `--adapter webhook` in v0.5.0.

### Provider Examples (Optional)

The patterns above are provider-agnostic. This section shows concrete examples
for common event bus implementations. These are **non-prescriptive**; use
whatever infrastructure fits your environment.

<details>
<summary>AWS SNS + SQS</summary>

```bash
# After successful run, publish to SNS
aws sns publish \
  --topic-arn arn:aws:sns:us-east-1:123456789012:quarry-runs \
  --message '{"run_id":"run-001","source":"my-source","outcome":"success"}'
```

Downstream Lambda or ECS task subscribes to SQS queue backed by the SNS topic.

</details>

<details>
<summary>Kafka</summary>

```bash
# Publish to Kafka topic
echo '{"run_id":"run-001","source":"my-source","outcome":"success"}' | \
  kafka-console-producer --topic quarry-runs --bootstrap-server localhost:9092
```

Downstream consumer group processes messages with automatic offset management.

</details>

<details>
<summary>Redis Pub/Sub</summary>

```bash
# Publish to Redis channel
redis-cli PUBLISH quarry-runs '{"run_id":"run-001","source":"my-source","outcome":"success"}'
```

Subscribers receive messages in real-time; no persistence guarantees.

</details>

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
Scan storage for new/changed runs
        ↓
Compare against checkpoint (last processed)
        ↓
Process new runs
        ↓
Update checkpoint
```

### Checkpoint Management

Maintain a checkpoint to track the last processed state:

```json
{
  "last_processed_timestamp": "2026-02-04T11:00:00Z",
  "last_processed_run_ids": ["run-001", "run-002"]
}
```

The checkpoint should be **durable** (stored in a database or file) to survive
poller restarts.

### Idempotent Processing

Because polling may re-scan the same runs, processors must be idempotent:

- Check if a run has already been processed before starting
- Use upserts instead of inserts for target systems
- Track processed run IDs in the checkpoint

### Example: Filesystem Polling

```bash
#!/bin/bash
# Poll for new runs and process them

STORAGE_PATH="/var/quarry/data"
CHECKPOINT_FILE="/var/quarry/.checkpoint"

# Get last processed timestamp
LAST_TS=$(cat "$CHECKPOINT_FILE" 2>/dev/null || echo "1970-01-01T00:00:00Z")

# Find runs newer than checkpoint
find "$STORAGE_PATH" -name "event_type=run_complete" -newer "$CHECKPOINT_FILE" \
  -exec process_run.sh {} \;

# Update checkpoint
date -u +%Y-%m-%dT%H:%M:%SZ > "$CHECKPOINT_FILE"
```

### Example: Object Storage Polling

For object storage backends (S3, GCS, etc.), list objects by prefix and filter
by modification time:

```bash
#!/bin/bash
# Generic object storage polling (adapt for your CLI/SDK)

BUCKET="my-bucket"
PREFIX="source=my-source/"
LAST_TS=$(cat "$CHECKPOINT_FILE" 2>/dev/null || echo "1970-01-01T00:00:00Z")

# List and filter by modification time
# (Replace with your storage CLI: aws s3, gsutil, etc.)
list_objects "$BUCKET" "$PREFIX" --after "$LAST_TS" | while read -r key; do
    process_run "$key"
done

# Update checkpoint
date -u +%Y-%m-%dT%H:%M:%SZ > "$CHECKPOINT_FILE"
```

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
consistency model. Consumers must respect the visibility boundary defined in
[`CONTRACT_RUN.md`](../contracts/CONTRACT_RUN.md) and implement idempotent processing to handle edge cases
where data appears after the run signal.

### Processing without idempotency

Without idempotent processing, duplicate events or re-polled runs will
create duplicate records in target systems.

---

## Related Documents

- [`CONTRACT_RUN.md`](../contracts/CONTRACT_RUN.md) — Run lifecycle and terminal states
- [`CONTRACT_LODE.md`](../contracts/CONTRACT_LODE.md) — Storage interaction semantics
- `guides/run.md` — User-facing run lifecycle guide
