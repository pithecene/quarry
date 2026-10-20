# Integration Pattern Examples

This directory contains conceptual examples for integrating Quarry with downstream
processing systems. These examples demonstrate the patterns described in
[docs/guides/integration.md](../../docs/guides/integration.md).

## Overview

Quarry is an extraction runtime that emits data to storage. Downstream systems
need to detect when runs complete and process the emitted data.

**Recommended**: Event-bus pattern (low latency, at-least-once delivery)
**Fallback**: Polling pattern (simpler, eventual consistency)

## Examples

### Event-Bus Pattern

- [`event-bus-sns.sh`](event-bus-sns.sh) — AWS SNS/SQS example (publish after run)
- [`event-bus-handler.ts`](event-bus-handler.ts) — TypeScript handler for processing run events

### Polling Pattern

- [`polling-fs.sh`](polling-fs.sh) — Filesystem polling with checkpoint
- [`polling-s3.py`](polling-s3.py) — S3 polling with timestamp filtering

## Usage Notes

These examples are **conceptual** and intended as starting points. Adapt them
to your infrastructure and requirements.

### Running the Event-Bus Example

```bash
# 1. Run a Quarry extraction
quarry run \
  --script ../demo.ts \
  --run-id run-001 \
  --source demo \
  --storage-backend fs \
  --storage-path ./quarry-data

# 2. Publish completion event (after run exits successfully)
./event-bus-sns.sh run-001 demo success ./quarry-data
```

### Running the Polling Example

```bash
# 1. Run multiple Quarry extractions
quarry run --script ../demo.ts --run-id run-001 --source demo \
  --storage-backend fs --storage-path ./quarry-data

quarry run --script ../demo.ts --run-id run-002 --source demo \
  --storage-backend fs --storage-path ./quarry-data

# 2. Poll for new runs
./polling-fs.sh ./quarry-data
```

## Key Concepts

### Visibility Boundary

The event-bus pattern publishes **after** the Quarry run exits successfully.
This ensures all data is committed to storage before consumers are notified.

### Idempotency

Both patterns require idempotent processing. Consumers may receive duplicate
events (event-bus) or re-scan existing runs (polling). Design your processors
to handle this gracefully.

### Checkpointing

The polling pattern uses a checkpoint file to track processed runs. This
prevents reprocessing on each poll cycle.

## Related Documentation

- [Integration Guide](../../docs/guides/integration.md) — Full pattern documentation
- [Run Lifecycle](../../docs/guides/run.md) — Run states and completion semantics
- [Storage Guide](../../docs/guides/lode.md) — Storage layout and access patterns
