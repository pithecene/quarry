# Quarry Run Identity & Lineage

This document freezes the identity and lineage rules for runs.

This is a contract document. Implementations must conform.

---

## Definitions

- **Job**: A logical unit of work requested by a user or scheduler.
- **Run**: A single execution attempt of a job.
- **Retry run**: A new run created due to failure of a previous run.

Relationship:

`job` → `run` → `retry run`

Runs are append-only and immutable once complete.

---

## Canonical Identifiers

### `run_id`
- Unique, stable identifier for a run.
- Must be globally unique across all time and jobs.
- Appears in every event envelope.
- run_id generation strategy is implementation-defined but must be collision-resistant (e.g., UUIDv7, ULID).

### `job_id`
- Identifier for the logical job.
- Stable across retries.
- Optional in the emit envelope if unknown at emit-time.

---

## Lineage Fields

When applicable, the following fields must be set:

- `parent_run_id`
  - Set when the current run is a retry or child run.
  - For first attempt runs, this field is absent.

- `attempt`
  - Integer attempt number. **Always present.**
  - Starts at **1** for the initial run of a job.
  - Incremented by 1 for each retry run.
  - A run with `attempt: 1` and no `parent_run_id` is an initial run.

### Child Runs (v0.6.0+)

When fan-out is active (`--depth > 0`), the runtime may create child runs
in response to `enqueue` events. Child runs have:

- A unique `run_id` (distinct from the parent).
- `attempt: 1` (child runs are first attempts of derived work).
- Depth tracked internally by the fan-out operator (not in the envelope).

Child runs are **not** retries. They represent derived work from a different
script, not a re-execution of the same job.

---

## Lifecycle Hooks (v0.9.0+)

Scripts may export optional lifecycle hooks alongside the default run function.
Hooks are invoked by the executor at well-defined points in the run lifecycle.

### Execution Order

```
load script
    │
    ▼
[prepare]  ──skip──▶ emit run_complete({skipped}) → run_result → return
    │
    │ continue (optionally with transformed job)
    ▼
acquire browser → create context
    │
    ▼
[beforeRun] → script() → [afterRun] (success) / [onError] (error)
    │
    ▼
[beforeTerminal]  (emit still open, terminal not yet written)
    │
    ▼
auto-emit terminal → [cleanup] → run_result → return
```

### `prepare`

- Invoked **before** browser acquisition.
- Receives the raw job payload and run metadata.
- Returns `{ action: 'continue' }` to proceed, optionally with a
  transformed `job` field that replaces the original payload.
- Returns `{ action: 'skip', reason? }` to short-circuit the run.
  On skip, the executor emits `run_complete` with `{ skipped: true }` in
  the summary and returns immediately. **No browser is launched** and no
  downstream hooks (`beforeRun`, `afterRun`, `onError`, `beforeTerminal`,
  `cleanup`) are invoked.
- If `prepare` throws, the run is classified as a crash. No browser is launched.

### `beforeTerminal`

- Invoked **after** `afterRun`/`onError` but **before** the terminal event
  (`run_complete` or `run_error`) is auto-emitted.
- The emit channel is still open; the hook may emit items, logs, or artifacts.
- Receives a `TerminalSignal`: `{ outcome: 'completed' }` on success,
  `{ outcome: 'error', error }` on script failure.
- **Not invoked** when the script has already emitted a terminal event
  manually, or when the IPC sink has failed.
- If `beforeTerminal` throws, the error is swallowed and the terminal event
  is still emitted (consistent with `onError`/`cleanup` error handling).

### Hook Contract Rules

- All hooks are optional. Scripts that do not export hooks behave identically
  to pre-0.9.0.
- Hook exports must be functions. Non-function exports of hook names cause
  a load-time validation error.
- `cleanup` is **not called** on `prepare`-skip paths (no context exists).

---

## Run Finalization (v0.14.0+)

The runtime owns a run's whole lifecycle. `Execute`
(`runtime.RunOrchestrator.Execute`) performs, in order:

1. Write the run's **start record** (§Run Records). Nothing else is
   written for the run before it.
2. Launch the executor, ingest, flush the policy, and close it, on every
   path including a failed start record. Buffered and streaming policies
   flush once more on close (bounded at 30 seconds; an interval flush
   still in progress is cancelled and joined, never abandoned), so closing
   happens here, before the end record; close is one-shot, so a caller's later
   close writes nothing. The close happens while the outcome is still being
   decided: if it leaves accepted data unpersisted, a run that would
   otherwise succeed fails with `policy_failure` (a run that already
   failed keeps its outcome). Policy statistics are taken after the close, so the
   metrics record and exit report match what was persisted.
3. Persist the run's metrics record (CONTRACT_LODE.md §Metrics Record
   Storage).
4. Write the run's **end record**.
5. Return.

Nothing is written for the run after the end record, so a present end
record means every other object of the run is final.

The time bounds in this section assume store operations return. A write
that is abandoned at a deadline could still land later, after the end
record, so the runtime never abandons one: a store call that never returns
blocks the run at that call, as it would block any write. Lode's
filesystem and S3 stores return; S3 calls also honour cancellation.

Finalization happens inside `Execute` for every caller: the CLI, fan-out
child runs, and any embedding host (CONTRACT_INTEGRATION.md §Activity
Boundary). Callers do not persist metrics themselves. When `Execute` has
no storage configured (as in unit tests), steps 1, 3, and 4 write nothing.

### Failure Semantics

- **Start record.** If the start record cannot be written, the run does
  not start: no executor is launched and no run data (events, artifacts,
  metrics) is written. The outcome is `executor_crash`, as when the
  executor fails to launch, and a retry is safe.
  - A store can report a write as failed after the object landed, so the
    runtime still attempts the end record, which gives such a run
    terminal evidence.
  - If a start record already exists at the run's key (same dataset,
    source, category, day, and run ID), that key belongs to another
    execution. The runtime then writes nothing at all, not even an end
    record, and reports the conflict in the outcome message. This check
    is scoped to the key: global `run_id` uniqueness (§Canonical
    Identifiers) remains the caller's obligation.
  - Values that can never form a valid key are rejected earlier, as a
    configuration error, before `Execute` (CONTRACT_LODE.md §Key
    Encoding).
- **Metrics record.** Best-effort, as before: failure is a warning, does
  not change the outcome, and is recorded in the end record
  (`metrics_persisted: false`).
- **End record.** Failure does not change the outcome. It is surfaced on
  stderr, in `RunResult.RunRecordError`, and in the structured exit report
  (`run_record_error`). As with the start record, a write reported as
  failed may still have landed.

---

## Run Records (v0.14.0+)

Every run has two small JSON records that the runtime writes outside
Lode's partition tree: a start record and an end record. Their location
and key encoding are defined in CONTRACT_LODE.md §Run Record Namespace.
Records are written once and never rewritten. Runs written before records
existed get backfilled records (§Backfilled Run Records).

### Start Record (`start.json`)

| Field            | Type   | Required | Description |
|------------------|--------|----------|-------------|
| `record_kind`    | string | yes      | `"run_start"` |
| `schema_version` | int    | yes      | `1` |
| `run_id`         | string | yes      | Run identifier |
| `job_id`         | string | no       | Omitted when unknown |
| `parent_run_id`  | string | no       | As in the run's lineage (§Lineage Fields); omitted when absent. Fan-out child runs do not yet carry their enqueueing run here (§Child Runs), so their records omit it |
| `attempt`        | int    | yes      | Per §Lineage Fields |
| `dataset`        | string | yes      | Lode dataset ID |
| `source`         | string | yes      | Partition key (unencoded) |
| `category`       | string | yes      | Partition key (unencoded) |
| `day`            | string | yes      | Partition day, `YYYY-MM-DD`, derived from `started_at` |
| `partition`      | string | yes      | The run's Lode partition prefix relative to the dataset's partition root, Hive-encoded: `source=…/category=…/day=…/run_id=…/`. Every `event_type` partition of the run sits under it |
| `started_at`     | string \| null | yes | Run start time, RFC 3339 UTC with nanoseconds; `null` on a backfilled record |
| `quarry_version` | string | yes      | Version of the writer |
| `backfilled`     | bool   | yes      | `true` when written by `quarry index rebuild` rather than the runtime |

### End Record (`end.json`)

| Field               | Type           | Required | Description |
|---------------------|----------------|----------|-------------|
| `record_kind`       | string         | yes      | `"run_end"` |
| `schema_version`    | int            | yes      | `1` |
| `run_id`            | string         | yes      | Run identifier |
| `ended_at`          | string         | yes      | RFC 3339 UTC with nanoseconds |
| `outcome`           | string \| null | yes      | `success`, `script_error`, `executor_crash`, `policy_failure`, or `version_mismatch` (§Observability Requirements); `null` on a backfilled record |
| `terminal`          | string \| null | yes      | Type of the terminal event the runtime received (`run_complete` or `run_error`); `null` when none was received |
| `message`           | string         | no       | Outcome message; omitted when empty |
| `event_count`       | int \| null    | yes      | Events processed; `null` on a backfilled record |
| `metrics_persisted` | bool           | yes      | Whether the metrics record was written |
| `backfilled`        | bool           | yes      | As on the start record |

### Backfilled Run Records

`quarry index rebuild` (CONTRACT_CLI.md §index rebuild) writes records for
runs that have partitions but no start record. It is the only reader of
the partition tree that finds runs, and it writes only what storage proves:

- **Start record**, for every such run. `day`, `source`, `category`,
  `run_id`, and `partition` come from the run's partition path, which must
  be the canonical encoding of those values; `attempt`, `job_id`, and
  `parent_run_id` come from the run's terminal event, or else from its
  first event record in event-type order. `started_at` is `null`. A run
  with no event record (only a metrics record) is skipped and reported.
- **End record**, only when the run has exactly one terminal event
  (`run_complete` or `run_error`). `ended_at` is that event's `ts`,
  `terminal` its type, and `metrics_persisted` whether the run has a
  metrics record. `outcome` and `event_count` are `null`: a terminal event
  does not determine the runtime's outcome (a policy failure can follow
  `run_complete`). A run with no terminal event, or more than one, gets no
  end record, and the report says why.

Both records carry `backfilled: true`. The rebuild never touches a run
whose start record the runtime wrote, even one without an end record (it
may be live, and only the runtime classifies it). A backfilled run without
an end record gains one on a later rebuild once its terminal event is
stored, so rebuilds are idempotent and resumable.

A backfilled end record states that the run's objects are final. That
holds only when no writer older than v0.14.0 is still writing the run:
run the rebuild after such writers have stopped writing to the dataset.

---

### Reading Run Records

Readers (CONTRACT_CLI.md §list runs) apply these rules:

- A run is listed if and only if its start record exists. An end record
  without a start record (a start write that failed without landing) is
  ignored.
- A run's state is `succeeded` when its end record's outcome is
  `success`, `failed` for any other outcome, `unclassified` when the
  outcome is `null` (a backfilled end record: the run ended, its outcome
  is unknown), and `unterminated` when it has no end record. Unterminated covers a live run, a killed process, and
  a run whose end record could not be written; storage cannot tell them
  apart, so a reader never presents such a run as running.
- A present end record means every other object of the run is final
  (§Run Finalization).

---

## Idempotency Expectations

- Runs are **append-only**; no event mutation is allowed after emission.
- The runtime and policies must not attempt to deduplicate events.
- Deduplication is the responsibility of **downstream consumers**.

---

## Observability Requirements

For every run, the runtime must surface:
- `run_id`
- `job_id` (if known)
- `parent_run_id` (if applicable)
- `attempt` (if applicable)
- outcome status (success, script error, executor crash, policy failure, version mismatch)

This metadata must be available to storage and logs.

---

## Structured Exit Report (v0.11.0+)

When `--report <path>` is specified, the runtime writes a structured JSON
report on exit. `--report -` writes to stderr.

The report is written after `Execute` returns (metrics and the end record
are persisted; §Run Finalization) and after adapter notification.
All data is composed from `RunResult` and `metrics.Snapshot` — no additional
data fetching occurs.

### Report JSON Structure

```json
{
  "run_id": "string",
  "job_id": "string (omitted if empty)",
  "attempt": 1,
  "outcome": "success | script_error | executor_crash | policy_failure | version_mismatch",
  "message": "string",
  "exit_code": 0,
  "duration_ms": 12345,
  "event_count": 42,
  "policy": {
    "name": "strict | buffered | streaming",
    "events_received": 42,
    "events_persisted": 42,
    "events_dropped": 0,
    "flush_triggers": { "interval": 3, "termination": 1 }
  },
  "artifacts": {
    "total": 5,
    "committed": 5,
    "orphaned": 0,
    "chunks": 10,
    "bytes": 524288
  },
  "metrics": { "/* CONTRACT_METRICS counters */" : "..." },
  "terminal_summary": { "/* from terminal event payload, if any */" : "..." },
  "proxy_used": {
    "protocol": "http",
    "host": "proxy.example.com",
    "port": 8080,
    "username": "user (omitted if none)"
  },
  "stderr": "string (omitted if empty)",
  "run_record_error": "string (omitted unless the end record failed)"
}
```

### Field Rules

- `run_id`, `attempt`, `outcome`, `message`, `exit_code`, `duration_ms`,
  `event_count`, `policy`, `artifacts`, `metrics` are always present.
- `job_id` is omitted when empty.
- `terminal_summary` is omitted when no terminal event was received.
- `proxy_used` is omitted when no proxy was configured.
- `stderr` is omitted when empty.
- `run_record_error` is omitted unless writing the end record failed
  (§Run Finalization).
- `policy.flush_triggers` is omitted for non-streaming policies.
- `exit_code` matches the process exit code per §Exit Codes in CONTRACT_CLI.md.
