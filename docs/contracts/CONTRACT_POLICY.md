# Quarry Ingestion Policy Contract

This document defines the semantics expected of ingestion policies.

This is a contract document. Implementations must conform.

---

## Scope

- Defines what a "drop" means.
- Defines which event types may be dropped.
- Defines buffering expectations.
- Defines required observability.

Non-goals:
- Does not define policy implementations.
- Does not define storage layout (see [CONTRACT_LODE.md](CONTRACT_LODE.md)).

---

## Definitions

### Drop
An event is **dropped** when it is observed by the runtime but **not persisted**
or forwarded to storage, and will not be retried by the policy.

### Buffered
An event is **buffered** when it is held in memory by the policy before
being written to storage. Buffered data is **bounded**.

---

## Drop Rules

- Policies may only drop:
  - `log`
  - `enqueue`
  - `rotate_proxy`
- Policies must **not** drop:
  - `item`
  - `artifact`
  - `checkpoint`
  - `run_error`
  - `run_complete`

If a policy cannot accept non-droppable events, it must fail the run.

---

## Buffering Rules

- Buffers must be **bounded** and observable.
- Buffered events must be flushed on:
  - `run_complete`
  - `run_error`
  - runtime termination (best effort)
- Buffering must never reorder events.

---

## Flush Semantics

A buffered policy writes its buffered events at least once. On any
failure it preserves the whole buffer, so a retry may write an event
twice and never loses one. Artifact bytes do not pass through a policy:
the runtime commits them to storage as they arrive
([CONTRACT_IPC.md](CONTRACT_IPC.md) §Artifact Frames), and the artifact
event is an event like any other.

All modes must satisfy the no-silent-loss invariant.

---

## Streaming Policy

The `streaming` policy provides **continuous persistence with batched writes**.
It is designed for long-running crawl workloads where downstream consumers
need near-real-time visibility into emitted items without the per-event
write overhead of `strict`.

### Semantics

- **No drops**: all event types are persisted. Same guarantee as `strict`.
- **Bounded buffer**: events accumulate in a bounded in-memory buffer.
- **Periodic flush**: buffer is flushed to storage when any trigger fires.
- **Blocking on full**: if the buffer is full and no flush trigger has fired,
  ingestion blocks until the next flush completes. Events are never dropped.

### Flush Triggers

The buffer is flushed when **any** of the following conditions is met:

| Trigger | Flag | Description |
|---------|------|-------------|
| **Count threshold** | `--flush-count` | Flush after N events accumulate. |
| **Time interval** | `--flush-interval` | Flush every T duration (e.g. `5s`, `30s`). |
| **Run termination** | — | Flush on `run_complete`, `run_error`, or runtime termination (best effort). |

At least one of `--flush-count` or `--flush-interval` must be specified.
Both may be specified. The first trigger to fire wins.

### Ordering

- Flush preserves per-run event ordering.
- The events within a single flush are written atomically (a single
  `Sink.WriteEvents` call per flush).
- On flush failure, the buffer is preserved and retried on the next
  trigger.

### Defaults

No defaults are prescribed. If `--policy=streaming` is specified without
at least one flush trigger, the CLI must reject the configuration.

### Required Observability (additive)

In addition to the base policy counters, streaming policy must surface:

- `flush_count`: number of flush cycles completed (already in `Stats`)
- `flush_trigger`: counter per trigger type (`count`, `interval`, `termination`)

These are additive to [CONTRACT_METRICS.md](CONTRACT_METRICS.md) and do not rename existing metrics.

---

## Required Observability

Policies must surface:

- Counters
  - total events received
  - total events persisted
  - total events dropped (by type)
- Logs
  - drop reasons (if any)
  - buffer overflows or flush failures
- Events
  - optional policy-level stats event

---

## Invariants

- No silent loss.
- Policy does not alter event shapes.
- Policy must respect the per-run ordering guarantees.
