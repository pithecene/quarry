# Ingestion Policies

This document describes ingestion policy behavior at a user level.
The authoritative contract is [`docs/contracts/CONTRACT_POLICY.md`](../contracts/CONTRACT_POLICY.md).

---

## What Policies Do

Policies govern how events are buffered, persisted, and (if allowed) dropped.
They do not change event shapes or ordering.

---

## Drop Rules (User View)

Only advisory or diagnostic events may be dropped:
- `log`
- `enqueue`
- `rotate_proxy`

All core output events (`item`, `artifact`, `checkpoint`, `run_error`,
`run_complete`) must not be dropped.

---

## Policy Modes

- **Strict**: synchronous writes, no drops. Every event is written to storage
  immediately. The executor blocks on each write. Best for low-volume runs
  where guaranteed persistence matters more than throughput.
- **Buffered**: bounded buffers, batched writes, explicit drops allowed.
  Events accumulate in memory and are flushed on run completion. Best for
  high-volume runs where throughput matters and advisory events can be dropped.
- **Streaming** *(v0.7.0)*: bounded buffers, batched writes, no drops.
  Combines strict's no-drop guarantee with buffered's batched write efficiency.
  Events are flushed on configurable triggers: count threshold
  (`--flush-count`), time interval (`--flush-interval`), or run termination.
  Best for long-running crawl workloads where downstream consumers need
  near-real-time visibility without per-event write overhead.

All modes preserve per-run ordering and emit observability about drops.
