# Quarry Lode Expectations

This document freezes Quarry's minimal expectations of Lode.

This is a contract document. Implementations must conform.

---

## Scope

- Defines required partition keys and layout invariants.
- Defines append-only semantics.
- Defines meaning of "consistency across policies".

Non-goals:
- Does not define Lode internal schema or storage engine.
- Does not define deduplication (downstream responsibility).

---

## Dataset Identity

Quarry writes to a Lode dataset ID that defaults to `quarry`.
The Lode dataset ID can be overridden via `--storage-dataset` on `quarry run`
and `quarry stats metrics`.

This dataset ID is a global container and is **not** the same as Quarry's
logical `category` partition key.

---

## Required Partition Keys

Lode must support partitioning by:
- `source` (origin system/provider; required)
- `category` (logical data type within a source; required; default `category=default`)
- `day` (derived from run start time; see below)
- `run_id`
- `event_type`

Additional keys are allowed, but the above must be present.

### Partition Key Semantics

- `category` is required. If no meaningful category exists for a source,
  use `category=default`.
- `day` is derived from the **run start time**, not individual event timestamps.
  Events may span dates, but must remain in the run's `day` partition.

### Recommended Layout Ordering

For Hive-style layouts, the preferred order is:

`source / category / day / run_id / event_type`

---

## Append-Only Semantics

- Events are **append-only**.
- No updates are required or expected. A run is deleted only as a whole,
  by retention (§Run Retention), never record by record.
- Event order within a run must be preserved.

---

## Write Retry

Quarry configures Lode with bounded CAS retry (`WithRetryCount(3)`) on the
write path. On `ErrSnapshotConflict`, the Lode commit is retried up to 3
times with jittered exponential backoff (10ms base, 2s max).

- Data files are written once; only the manifest re-parent and pointer CAS
  are retried.
- Non-conflict errors are fatal (no retry).
- Each retry increments `lode_write_retry_total` via Lode's
  `WithRetryObserver` callback. The counter records retries, not write
  calls: an uncontended write contributes 0; a write that conflicts once
  and then succeeds contributes 1.
- Retries incurred by the metrics write itself (`WriteMetrics`) are NOT
  counted. The persisted metrics record is a snapshot taken before that
  write begins and cannot contain the count of its own retries; excluding
  them keeps the persisted record identical to the collector's final value.

---

## Record Types

All stored records MUST include a `record_kind` discriminator field.

| `record_kind`    | Description                          |
|------------------|--------------------------------------|
| `event`          | Standard event envelope              |
| `artifact_event` | Artifact commit (manifest) event     |
| `artifact_chunk` | Artifact binary chunk                |
| `metrics`        | Run metrics snapshot                 |

The `record_kind` field enables downstream consumers to distinguish record
types without inspecting `event_type` or payload structure.

---

## Artifact Chunk Storage

Artifact binary data is stored as chunk records under `event_type=artifact`.

### Chunk Record Schema

| Field          | Type     | Required | Description                              |
|----------------|----------|----------|------------------------------------------|
| `record_kind`  | string   | yes      | `"artifact_chunk"`                       |
| `artifact_id`  | string   | yes      | Artifact identifier                      |
| `seq`          | int64    | yes      | Sequence number (starts at 1)            |
| `is_last`      | bool     | yes      | True if final chunk                      |
| `offset`       | int64    | yes      | Byte offset within artifact              |
| `length`       | int64    | yes      | Chunk data length in bytes               |
| `data`         | bytes    | yes      | Raw chunk data (base64 in JSON)          |
| `checksum`     | string   | no       | Chunk checksum (see Checksum section)    |
| `checksum_algo`| string   | no       | Checksum algorithm (must be `md5`)       |

### Commit Record Schema

The artifact commit event marks the artifact as complete.

| Field          | Type     | Required | Description                              |
|----------------|----------|----------|------------------------------------------|
| `record_kind`  | string   | yes      | `"artifact_event"`                       |
| `artifact_id`  | string   | yes      | Artifact identifier                      |
| `name`         | string   | yes      | Human-readable artifact name             |
| `content_type` | string   | yes      | MIME content type                        |
| `size_bytes`   | int64    | yes      | Total artifact size in bytes             |

### Ordering Invariant

> Chunk records MUST be written before the corresponding commit record.

The commit record is the commit boundary. Chunks written without a subsequent
commit are orphans and may be garbage collected.

---

## Checksum

The `checksum` field on chunk records is **optional**.

If present:
- `checksum_algo` MUST be set to `"md5"`.
- `checksum` contains the hex-encoded MD5 digest of the chunk data.

Checksum validation is a downstream consumer responsibility.

---

## Metrics Record Storage

A metrics snapshot is written at run completion under `event_type=metrics`,
by the runtime inside `Execute`, before the end record (CONTRACT_RUN.md
§Run Finalization).
Write is best-effort — failure produces a warning but does not change run outcome.

### Metrics Record Schema

Field names use the `_total` suffix to match CONTRACT_METRICS.md naming.

| Field                           | Type              | Required | Description                              |
|---------------------------------|-------------------|----------|------------------------------------------|
| `record_kind`                   | string            | yes      | `"metrics"`                              |
| `event_type`                    | string            | yes      | `"metrics"` (Hive partition key)         |
| `ts`                            | string (RFC3339)  | yes      | Run completion timestamp                 |
| `runs_started_total`            | int64             | yes      | Run lifecycle counter                    |
| `runs_completed_total`          | int64             | yes      | Run lifecycle counter                    |
| `runs_failed_total`             | int64             | yes      | Run lifecycle counter                    |
| `runs_crashed_total`            | int64             | yes      | Run lifecycle counter                    |
| `events_received_total`         | int64             | yes      | Ingestion counter                        |
| `events_persisted_total`        | int64             | yes      | Ingestion counter                        |
| `events_dropped_total`          | int64             | yes      | Ingestion counter                        |
| `dropped_by_type`               | map[string]int64  | no       | Per-type drop breakdown                  |
| `executor_launch_success_total` | int64             | yes      | Executor counter                         |
| `executor_launch_failure_total` | int64             | yes      | Executor counter                         |
| `executor_crash_total`          | int64             | yes      | Executor counter                         |
| `ipc_decode_errors_total`       | int64             | yes      | Executor counter                         |
| `lode_write_success_total`      | int64             | yes      | Storage counter                          |
| `lode_write_failure_total`      | int64             | yes      | Storage counter                          |
| `lode_write_retry_total`        | int64             | yes      | Storage counter (CAS retries, see Write Retry) |
| `policy`                        | string            | yes      | Dimension: policy name                   |
| `executor`                      | string            | yes      | Dimension: executor identity             |
| `storage_backend`               | string            | yes      | Dimension: storage backend               |
| `run_id`                        | string            | yes      | Dimension: run identifier                |
| `job_id`                        | string            | no       | Dimension: job identifier                |
| `source`                        | string            | yes      | Partition key                            |
| `category`                      | string            | yes      | Partition key                            |
| `day`                           | string            | yes      | Partition key (YYYY-MM-DD)               |

---

## Policy-Independent Layout Invariants

Storage layout must remain consistent across policies so that:
- A strict policy run and a buffered policy run produce data in the
  **same partition layout**.
- Downstream consumers can query without knowing policy choice.

---

## Consistency Across Policies

"Consistency across policies" means:
- Identical partition keys.
- Identical event envelopes.
- Identical event ordering within a run.
- Explicit visibility if events were dropped (via policy observability).

---

## Lineage and Metadata

Lode must surface run metadata alongside stored events:
- `run_id`
- `job_id` (if known)
- `parent_run_id` (if applicable)
- `attempt` (if applicable)
- run outcome status

Deduplication is explicitly out of scope and left to downstream consumers.

---

## S3-Compatible Provider Support

The S3 storage backend supports S3-compatible providers (Cloudflare R2, MinIO, etc.)
via custom endpoint and path-style addressing options.

- **Custom endpoint**: overrides the default AWS S3 endpoint URL.
- **Path-style addressing**: uses `endpoint/bucket/key` instead of `bucket.endpoint/key`.
  Required by most S3-compatible providers.

These are runtime configuration options passed via CLI flags (`--storage-endpoint`,
`--storage-s3-path-style`). They do not affect partition layout or record format.

---

## Listing Order and Cursors (Lode v0.10.0+)

Quarry's read side relies on the following Lode listing guarantees, which
Lode's `CONTRACT_READ_API.md` makes normative from v0.10.0:

- Listings are sorted on every store: datasets by ID, partitions by path,
  manifests by (partition, ID). A reader must not assume store order and
  must not re-sort.
- `PartitionListOptions.Prefix` narrows a partition listing to a string
  prefix of the Hive path. A layout that cannot honour a prefix returns
  `ErrPartitionPrefixNotSupported`; Quarry's Hive layout supports it.
- `DatasetListOptions.StartAfter`, `PartitionListOptions.StartAfter`, and
  `ManifestListOptions.StartAfter` are exclusive cursors over the full
  sorted key (dataset ID, partition path, or `(partition, ID)`). `Limit`
  bounds the returned set, not the number of store calls: sorting and the
  cursor are applied after the matching prefix is enumerated, so paging a
  wide prefix re-enumerates it on every page. Narrow the prefix before
  paging.
- `Store.List` matches a string prefix on every backend (filesystem,
  memory, S3); keys, prefixes, and S3 prefixes containing a backslash are
  `ErrInvalidPath` on every platform.
- Refused caller values — nil store factory, out-of-range `WithRetry*`
  options — are classified `ErrInvalidArgument` and reported before the
  store factory is called. Quarry's `WithRetryCount(3)` and
  `WithRetryObserver` registration are valid values and unaffected.

Error text changed in v0.10.0 (`lode: invalid argument: <value>`,
`invalid path`, one extra nesting level on manifest read failures);
`errors.Is` classification is unchanged and Quarry matches on sentinels,
never on message text.

---

## Run Record Namespace (v0.14.0+)

Run start and end records (CONTRACT_RUN.md §Run Records) are stored
outside Lode's partition tree, in a namespace Quarry owns inside the
dataset:

```
datasets/<dataset>/runs/<source>/<category>/<day>/<run_id>/start.json
datasets/<dataset>/runs/<source>/<category>/<day>/<run_id>/end.json
```

- Records are written with `Store.Put`, not `Dataset.Write`; they are not
  Lode records, have no manifest, and are scoped by `--storage-dataset`
  like everything else the run writes. `Store.Put` is create-only on every
  backend, so a record is never overwritten.
- Lode never reads or modifies this namespace: it lies outside
  `datasets/<dataset>/partitions/` and Lode's manifests and pointer.
- Path components are ordered `source/category/day/run_id`, matching the
  partition layout. A listing is narrowed by key prefix only for leading
  selectors: `source`, then `source` and `category`, then `source`,
  `category`, and `day`. A selector cannot skip an earlier one.
- `Store.List` guarantees no order and no continuation, so readers sort the
  matching start-record keys lexicographically and cut pages on them; the
  cursor is the last start-record key returned. Exactly one start record
  exists per run, so a page boundary is a run boundary.

### Key Encoding

The `dataset` component is the dataset ID exactly as Lode uses it in its
own `datasets/<dataset>/` directory, so records sit beside the dataset's
partitions. Every other path component (`source`, `category`, `day`,
`run_id`) is encoded with Lode's Hive value encoding (`url.PathEscape`),
never concatenated raw. A value such as `a/b` is stored as `a%2Fb`,
exactly as the partition tree stores it, so a record can neither escape
the namespace nor create an ambiguous prefix. One builder in
`quarry/lode` computes these keys.

`run` rejects, before anything is written, values that cannot form a safe
key, as a configuration error:

- `source`, `category`, `run_id`: empty, `.`, or `..` (Hive encoding
  leaves `.` and `..` unchanged), or a value with a `.` or `..` path
  segment (split on `/` or `\`). Sidecar file paths (§Sidecar File
  Inventory) still interpolate these values unencoded, so such a segment
  could place one run's files under another run's partition.
- Known limitation, unchanged from earlier releases: for a value that
  Hive encoding changes (one containing `/`, `\`, `%`, a space, and so on),
  the sidecar file path differs from the run's partition prefix, because
  sidecar paths (and the key the SDK reports from `storage.put()`) are
  built raw. Run records and the partition tree always agree. Moving
  sidecar keys to the same encoding requires the IPC acknowledgment to
  carry the resolved key first; it is roadmap decision B1.
- Dataset ID: empty, `.`, `..`, or any value containing `/` or `\`. The
  dataset component is not encoded, so these would leave the dataset's
  directory or add path segments.

Fan-out child runs are checked the same way before they start, and so is
every embedding host: `runtime.NewRunOrchestrator` rejects an unsafe key
when storage is configured, and the record writer refuses one.

The start record's `partition` field uses the same encoding for the
partition prefix (`source=<enc>/category=<enc>/day=<enc>/run_id=<enc>/`),
so the run's Lode partitions are addressed from the record without
re-deriving keys.

---

## Sidecar File Inventory

Sidecar files written via `storage.put()` land at Hive-partitioned paths
under `files/`, bypassing the Dataset segment/manifest machinery. These files
are **not** tracked by Lode's `Manifest.Files` — they are the caller's
responsibility.

Quarry tracks sidecar files in Lode snapshot `Metadata` under the
`sidecar_files` key. Each entry is a `SidecarFileRef`:

| Field          | Type   | Description                              |
|----------------|--------|------------------------------------------|
| `path`         | string | Full Hive-partitioned storage path       |
| `filename`     | string | Flat filename (no path separators)       |
| `content_type` | string | MIME content type                        |
| `size`         | int64  | File size in bytes                       |

### Flush Semantics

- File refs accumulate in the client as files are written via `PutFile`.
- Refs are flushed into snapshot `Metadata` on `WriteEvents` and
  `WriteMetrics` calls only — these are consumer-facing commit boundaries.
- Chunk writes (`WriteChunks`) do **not** flush file refs, because chunk
  snapshots are intermediate and not inspected by downstream consumers.
- On successful write, pending refs are drained. On write failure, pending
  refs are preserved for the next successful write.

### Consumer Access

Downstream consumers enumerate sidecar files via:

```
snapshot.Manifest.Metadata["sidecar_files"]
```

File refs may be spread across multiple snapshots within a run. To
reconstruct the full inventory, union `sidecar_files` from all snapshots
for the run.

---

## Run Retention (v0.14.0+)

Retention removes whole runs, chosen by their state and age. Quarry
chooses the runs, because only Quarry knows a run's outcome. Lode's
`Dataset.Expire` removes their snapshots (Lode CONTRACT_RETENTION.md), and
Quarry deletes the objects it owns. The `quarry/retention` package
implements it.

### Selection

- Runs are found from their start records (CONTRACT_RUN.md §Reading Run
  Records), under the leading selectors `source`, `category`, and `day`,
  or one run named by all three and its run ID. A run without a start
  record is not found, so `quarry index rebuild` must index older runs
  first.
- A run's state is the state that `list runs` reports: `succeeded`,
  `failed`, or `unterminated`. The policy gives each state an age at
  which its runs are removed. A state without an age is kept.
- A run's age is measured from its end record's `ended_at` when it has
  one, and otherwise from its start record's `started_at`. A backfilled
  start record has no `started_at`, so the end of its partition day
  stands in.
- An unterminated run cannot be told apart from a live one, so its age
  must be at least 24 hours and must exceed the longest run. A
  terminated run is final (CONTRACT_RUN.md §Run Finalization), so any age,
  including zero, is safe.
- A start-record key that the write path could not have produced, and a
  start record whose fields do not describe the key it is stored at, are
  reported and never acted on. A record therefore cannot direct deletion
  at another run.

### Order of Operations (per Run)

1. Preview the expiry of the run's partition prefix (the start record's
   `partition`) as a dry run. If the run holds the dataset's head, has a
   snapshot with files outside its partition, or the head is uncommitted
   or absent, the run is kept, nothing of it is deleted, and the report
   gives the reason. Lode never expires the head, so the run that
   committed most recently is removed only by a later prune.
2. Delete the run's sidecar files and their `.meta.json` companions
   (§Sidecar File Inventory): every path that the run's snapshots record,
   and, when Hive encoding leaves the run's source, category, day, and
   run ID unchanged, every direct child of its sidecar prefix. A deeper
   path can belong to a run whose ID continues the prefix, such as
   `r/files/x` beneath run `r`, so it is never listed. Without that
   condition, a direct child could also belong to another run, so the
   prefix is not listed, and a file written after the run's last commit
   remains. A recorded path outside the run's sidecar prefix
   is reported and left in place.
3. Expire the run's snapshots with `Dataset.Expire` on the partition
   prefix.
4. Delete the start record, and then the end record. A run without a
   start record is never listed, and an end record without one is
   ignored by readers.

An end record without a start record names no run: an earlier prune
stopped between the two deletions, or the run's start record was never
persisted. Every prune deletes such end records under its selectors, and
the report counts them.

Each step is idempotent, and a run is listed until step 4. A prune that is
interrupted, or that fails with a store error, is therefore completed by
the next prune that selects the run. If step 3 is interrupted after Lode
deleted the run's last manifest, the next prune no longer finds the
snapshot's residual data; `Dataset.Sweep` removes it.

### Sweep

When a grace period is given, a prune ends with `Dataset.Sweep` over the
whole dataset, whatever the selectors. It completes interrupted expiries
and removes the data of writes that never committed, such as the last
batch of a killed run, once that data is older than the grace period.
The grace period must exceed the longest single write; it never removes
data that a manifest describes.

### Concurrency

A prune can run while runs are written to the dataset. Lode re-checks the
head before each expiry, so a run that commits during the prune is never
removed. A run that the policy selects is presumed final; a prune does not
coordinate with readers of that run.

