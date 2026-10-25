# Quarry Metrics — Contract

This document defines the **metrics surface** required of the Quarry runtime.
It specifies the **metric names, semantics, and required visibility** for
CLI stats and operational observability.

This is a contract document. Implementations must conform.

---

## Scope

- Defines required runtime metrics and their meanings.
- Defines required dimensions and aggregation expectations.
- Defines exposure requirements via CLI stats.

Non-goals:
- Does not define exporter implementations (Prometheus, OTLP, etc.).
- Does not define storage schema or persistence format.
- Does not define UI or visualization.

---

## Definitions

### Metric Categories
- **Counter**: monotonically increasing.
- **Gauge**: current value snapshot.
- **Histogram**: optional distribution buckets (if supported).

### Time Horizons
- **Per-run**: metrics scoped to a single run.
- **Lifetime**: metrics aggregated since process start.
- **Rolling**: windowed metrics (optional).

### Cardinality Rules
- Metrics must avoid unbounded label sets.
- `run_id` and `job_id` are optional dimensions and should be used only for
  CLI inspection or short-lived aggregation.

---

## Required Metrics

### Run Lifecycle
- `runs_started_total` (counter)
- `runs_completed_total` (counter)
- `runs_failed_total` (counter)
- `runs_crashed_total` (counter)

Outcome-to-metric mapping:

| OutcomeStatus | Counter Incremented |
|---------------|---------------------|
| `success` | `runs_completed_total` |
| `script_error` | `runs_failed_total` |
| `policy_failure` | `runs_failed_total` |
| `version_mismatch` | `runs_failed_total` |
| `executor_crash` | `runs_crashed_total` |

`version_mismatch` increments `runs_failed_total` (not `runs_crashed_total`)
because it is a configuration error, not an executor fault. It also does NOT
increment `executor_crash_total`.

### Ingestion Policy
- `events_received_total` (counter)
- `events_persisted_total` (counter)
- `events_dropped_total` (counter, by event type)
- `flush_triggers` (counter, by trigger type — streaming policy only)

#### Flush Triggers (streaming policy)

When `policy=streaming`, the runtime tracks per-trigger-type flush counts:

| Trigger | Key | Description |
|---------|-----|-------------|
| Count threshold | `count` | Flush fired when `--flush-count` events accumulated |
| Time interval | `interval` | Flush fired on `--flush-interval` tick |
| Run termination | `termination` | Flush fired on `run_complete`, `run_error`, or runtime exit |
| Buffer capacity | `capacity` | Emergency flush when internal buffer bounds reached |

These are additive to the base policy counters. `flush_triggers` is `nil` for
strict and buffered policies.

### Executor
- `executor_launch_success_total` (counter)
- `executor_launch_failure_total` (counter)
- `executor_crash_total` (counter)
- `ipc_decode_errors_total` (counter)

### Lode / Storage
- `lode_write_success_total` (counter)
- `lode_write_failure_total` (counter)
- `lode_write_retry_total` (counter)
- `lode_write_latency_ms` (histogram, optional)

---

## Required Dimensions

All metrics must support the following dimensions where applicable:
- `policy` (required)
- `executor` (required)
- `storage_backend` (required)

Optional dimensions:
- `run_id`
- `job_id`
- `adapter` (only when integrations exist)

---

## Exposure Requirements

- CLI `stats` commands must surface stable, aggregated views of required metrics.
- Metrics must be derived from runtime-owned sources, not executor counters.
- Metrics snapshots must be persisted as Lode `record_kind=metrics` records
  (see [CONTRACT_LODE.md](CONTRACT_LODE.md)) to support stats reads across processes.
- No exporter is required; exposure via CLI is mandatory.

### Data Sources

A read command that has no Lode-backed reader returns stub data.

- `stats metrics` reads the latest metrics record from storage when
  `--storage-backend` and `--storage-path` are both given, and returns stub
  data when no storage flag is given ([CONTRACT_CLI.md](CONTRACT_CLI.md) §`stats metrics`).
- `list runs` reads run records from storage in the same way, with the
  same stub fallback ([CONTRACT_CLI.md](CONTRACT_CLI.md) §`list runs`). `list events`
  requires the storage flags and has no stub ([CONTRACT_CLI.md](CONTRACT_CLI.md)
  §`list events`).
- Every other `stats`, `inspect`, and `list` command returns stub data
  only. No global reader configuration exists, and the commands read
  storage only through their own storage flags.

---

## Invariants

- No silent loss: drops must be counted and visible.
- Metrics must not alter runtime behavior.
- Metric names and meanings must be consistent across policies.

---

## Versioning

- Additive changes only during 0.x.
- Renames or semantic changes are breaking and forbidden in 0.x.
