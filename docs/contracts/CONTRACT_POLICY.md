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
- A byte bound counts each event at the length of the IPC frame it arrived
  in, length prefix included.
- Completing a flush removes only the events it wrote. An event ingested
  while a flush writes stays buffered for a later flush, subject to the
  drop rules.
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

A policy marks each failed sink write as soon as the write returns. It is
marked as ended with the run when the write ran under the run's recorded
context or one derived from it, the run had ended, and every branch of the
sink's error shows that end: the context's error, or a cause whose value can
be compared. Every other failure is a store's refusal, including a write
under a termination or close-time flush context, which records no run and
has a deadline of its own. A fan-out marks each mandatory sink on its
own, so a refusal beside another sink's cancellation stays a refusal. A context
does not record which of two ends came first, so once
the run has ended, a write that its own deadline or cause ended counts as ended
with the run when its context's cause and its error match the run's end, as when
both deadlines passed with one cause. A
refusal that decides the run's outcome makes it `storage_error`; a refusal
after the run has already failed changes no outcome (CONTRACT_RUN.md §Run
Finalization).

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
- **Blocking on full**: if the next event would exceed the buffer's bounds,
  ingestion flushes the buffered events and blocks until the flush
  completes, then checks again. A call whose context ends before its event
  is admitted does not buffer the event and returns an error: the
  context's, or the flush's when a flush that the call started fails. An
  event already admitted stays buffered for persistence even if its call
  then returns an error. Events are never dropped.

### Flush Triggers

The buffer is flushed when **any** of the following conditions is met:

| Trigger | Flag | Description |
|---------|------|-------------|
| **Count threshold** | `--flush-count` | Flush after N events accumulate. |
| **Time interval** | `--flush-interval` | Flush every T duration (e.g. `5s`, `30s`). |
| **Run termination** | — | Flush on `run_complete`, `run_error`, or runtime termination (best effort). |
| **Buffer capacity** | — | Flush when the buffer reaches its bounds, or before an event that would exceed them. |

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
- `flush_triggers`: counter per trigger type (`count`, `interval`, `termination`, `capacity`)

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
  - written through the run's logger
- Events
  - optional policy-level stats event

---

## Invariants

- No silent loss.
- Policy does not alter event shapes.
- Policy must respect the per-run ordering guarantees.
