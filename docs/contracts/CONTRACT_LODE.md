# Quarry Lode Storage Contract

This document defines Quarry's minimal expectations of Lode.

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

## Storage Layout

One store holds both Quarry's objects and Lode's, and each prefix in it
has one owner. Quarry reads and writes its own prefixes directly. A Lode prefix is
sealed: what lies under it is reached only through Lode's API. Listing a
sealed prefix's parent tells Quarry the names it gave, and no key in the
sealed prefix is interpreted.

| Prefix | Owner | Holds |
|---|---|---|
| `datasets/<dataset>/runs/` | Quarry | Run records (§Run Record Namespace) |
| `datasets/<dataset>/partitions/<run partition>/files/` | Quarry | Sidecar files and their `.meta.json` companions (§Sidecar File Inventory) |
| The rest of `datasets/<dataset>/` | Lode, sealed | The Dataset |
| `artifacts/<dataset>/<run key>/volumes/` | Quarry | One name per artifact ID (§Artifact Storage) |
| `artifacts/<dataset>/<run key>/volumes/<artifact_id>/` | Lode, sealed | One Volume |

- `<run partition>` is `source=<s>/category=<c>/day=<d>/run_id=<r>`, and
  `<run key>` is `<source>/<category>/<day>/<run_id>`, each encoded as
  §Key Encoding defines.
- Sidecar files sit inside the Dataset's partition tree, but Lode tracks
  none of them, so Quarry owns and deletes them.
- The two trees are siblings. `artifacts/<dataset>/` groups a run's
  Volumes with the run's Dataset by name only. Lode relates no Volume to
  any Dataset.

---

## Required Partition Keys

Lode must support partitioning by:
- `source` (origin system/provider, required)
- `category` (logical data type within a source, required, default `category=default`)
- `day` (derived from the run's start time, as described below)
- `run_id`
- `event_type`

Additional keys are allowed, but the above must be present.

### Partition Key Semantics

- `category` is required. If no meaningful category exists for a source,
  use `category=default`.
- `day` is derived from the **run start time**, not individual event timestamps.
  Events may extend across dates, but must remain in the run's `day` partition.

### Recommended Layout Ordering

For Hive-style layouts, the preferred order is:

```text
source / category / day / run_id / event_type
```

---

## Append-Only Semantics

- Events are **append-only**.
- No updates are required or expected. A run is deleted only as a whole,
  by retention (§Run Retention), never record by record.
- Event order within a run must be preserved.

---

## Write Retry

Quarry configures Lode with bounded CAS retry (`WithRetry(3)`) on the
write path. On `ErrSnapshotConflict`, the Lode commit is retried up to 3
times with jittered exponential backoff (10ms base, 2s max).

- Data files are written once. Only the manifest re-parent and pointer CAS
  are retried.
- Non-conflict errors are fatal (no retry).
- Each retry increments `lode_write_retry_total` via Lode's
  `WithRetryObserver` callback. The counter records retries, not write
  calls: an uncontended write contributes 0. A write that conflicts once
  and then succeeds contributes 1.
- Retries incurred by the metrics write itself (`WriteMetrics`) are NOT
  counted. The persisted metrics record is a snapshot taken before that
  write begins and cannot contain the count of its own retries. Excluding
  them keeps the persisted record identical to the collector's final value.

---

## Record Types

All stored records MUST include a `record_kind` discriminator field.

| `record_kind`    | Description                          |
|------------------|--------------------------------------|
| `event`          | Standard event envelope              |
| `artifact_event` | Artifact commit (manifest) event     |
| `artifact_chunk` | Artifact binary chunk, written by releases before 0.15 |
| `metrics`        | Run metrics snapshot                 |

The `record_kind` field enables downstream consumers to distinguish record
types without inspecting `event_type` or payload structure.

---

## Artifact Storage

An artifact's bytes are one Lode Volume, with the artifact ID as its
Volume ID, in a namespace of the run's own beside the datasets:

```
artifacts/<dataset>/<source>/<category>/<day>/<run_id>/volumes/<artifact_id>/...
```

- The run key components are Hive-encoded by the one key builder, as for
  run records (§Key Encoding), so a run's Volumes can neither escape the
  run nor share a prefix with another run's. The artifact ID is one path
  segment, which the runtime and Lode's `volume.New` both require.
- The namespace lies outside `datasets/<dataset>/`. A Lode Dataset rejects
  any manifest in its directory that its layout does not persist, as a
  history written under another layout, and a Volume's manifests are such
  manifests. Run records sit inside the dataset's directory because they
  are not manifests.
- The runtime creates the Volume with the artifact's size as its total
  length before any byte arrives, stages and commits each range as it
  arrives, and keeps no artifact bytes of its own. A run that stops part
  way leaves committed snapshots that name exactly the ranges that
  arrived.
- An artifact of zero bytes has no Volume: its commit record says so.
- No run listing reads this namespace.

A run written by a release before 0.15 holds its artifact bytes as chunk
records under `event_type=artifact`, which readers still read (§Chunk
Record Schema). The runtime writes no chunk record.

### Chunk Record Schema

Chunk records are read, not written, from release 0.15.

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
| `storage`      | string   | no       | `"volume"`, or `"empty"` for an artifact of zero bytes. Absent in records written before release 0.15, whose bytes are chunk records |
| `volume_snapshot_id` | string | no   | With `"volume"` only: the Volume snapshot whose manifest covers `[0, size_bytes)` |
| `sha256`       | string   | no       | Hex SHA-256 of the artifact's bytes, computed by the runtime from what it committed. Present with `storage` |

The storage fields are the runtime's, not the script's. The `artifact`
event a script emits keeps its public shape, and sinks and adapters
receive it unchanged. The runtime passes the artifact's storage result
beside the envelope as a typed value, and the Lode client writes its
fields into the stored record.

A commit record that a release before 0.15 wrote can state a
`size_bytes` of 0 for an artifact of any size. Readers MUST therefore
take the size of a chunk-stored artifact whose record states 0 from its
chunks. The artifact ends where its first chunk with `is_last` ends,
because a retry can append a second copy after a complete one. A reader
MUST refuse a chunk-stored artifact when no chunk with `is_last` ends at
its size, so a stated size that disagrees with the chunks never cuts the
artifact short.

### Ordering Invariant

> The Volume snapshot that covers the whole artifact MUST be committed
> before the corresponding commit record is written.

The commit record is the commit boundary, so a reader that finds a commit
record can read the committed bytes it names. A Volume whose commit
record never follows is an orphan: the runtime removes it when the run
ends, and `quarry prune` removes what the runtime could not. For a run
written before release 0.15, chunk records precede their commit record
in the same way.

### Read-Back Invariant

> Every record that the runtime commits MUST be readable by the run's
> readers (`list runs`, `list events`, and `stats metrics`), whatever the
> size of the run's artifact chunks.

A chunk record carries its data as base64 on one JSON line, so a chunk at
the IPC maximum is a line of about 11 MiB. A reader that bounds a line
below that refuses a run the runtime reported as complete. The bound on a
record is the writer's, and a reader MUST NOT add one of its own.

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
by the runtime inside `Execute`, before the end record ([CONTRACT_RUN.md](CONTRACT_RUN.md)
§Run Finalization).
Write is best-effort. Failure produces a warning but does not change run outcome.

### Metrics Record Schema

Field names use the `_total` suffix to match [CONTRACT_METRICS.md](CONTRACT_METRICS.md) naming.

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

The S3 storage backend supports S3-compatible providers
via custom endpoint and path-style addressing options.

- **Custom endpoint**: overrides the default AWS S3 endpoint URL.
- **Path-style addressing**: uses `endpoint/bucket/key` instead of `bucket.endpoint/key`.
  Required by most S3-compatible providers.

These are runtime configuration options passed via CLI flags (`--storage-endpoint`,
`--storage-s3-path-style`). They do not affect partition layout or record format.

---

## Listing Order and Cursors

Quarry's read side relies on the following Lode listing guarantees, which
Lode's [CONTRACT_READ_API.md](https://github.com/pithecene/lode/blob/main/docs/contracts/CONTRACT_READ_API.md) makes normative:

- Listings are sorted on every store: datasets by ID, partitions by path,
  manifests by (partition, ID). A reader must not assume store order and
  must not re-sort.
- `PartitionListOptions.Prefix` narrows a partition listing to a string
  prefix of the Hive path. A layout that cannot honour a prefix returns
  `ErrPartitionPrefixNotSupported`. Quarry's Hive layout supports it.
- `DatasetListOptions.StartAfter`, `PartitionListOptions.StartAfter`, and
  `ManifestListOptions.StartAfter` are exclusive cursors over the full
  sorted key (dataset ID, partition path, or `(partition, ID)`). `Limit`
  bounds the returned set, not the number of store calls: sorting and the
  cursor are applied after the matching prefix is enumerated, so paging a
  wide prefix re-enumerates it on every page. Narrow the prefix before
  paging.
- `Store.List` matches a string prefix on every backend (filesystem,
  memory, S3). Keys, prefixes, and S3 prefixes containing a backslash are
  `ErrInvalidPath` on every platform.
- Refused caller values (a nil store factory, out-of-range `WithRetry*`
  options) are classified `ErrInvalidArgument` and reported before the
  store factory is called. Quarry's `WithRetry(3)` and
  `WithRetryObserver` registration are valid values and unaffected.

Quarry matches Lode errors on their sentinels with `errors.Is`, never on
message text.

---

## Writer Settings

Every Quarry writer of a dataset opens it with the same settings: the Hive
layout over the required partition keys in the recommended order, the JSONL
codec, no compressor, no checksum, and Lode's default path names.

- Lode records the partition keys and path names in each manifest, under
  manifest format 1.1.0, and compares a writer's settings with the head's
  when the writer opens. A writer whose settings differ is rejected with
  `ErrSettingsMismatch` before it writes anything.
- Opening a writer reads the dataset's head, so constructing a client
  performs storage reads and takes a context.
- Readers open read-only views, which take the layout from the dataset's
  manifests. A view of a dataset without snapshots reports `ErrNotFound`,
  which Quarry's readers return as an empty result.
- Retention (`Expire`, `Sweep`) is available only on writer handles, so
  `quarry prune` opens a writer with the settings above.
- A dataset written before v0.12.0 has manifests in format 1.0.0, which
  record no settings, and opens under the default path names. No migration is
  required. Lode v0.10.0, which Quarry v0.13.7 uses, and Lode v0.11.0 read
  and extend datasets that Lode v0.12.0 has written, and Lode v0.11.0 also
  expires and sweeps them, so a rollback to an earlier Quarry release needs
  no migration either.

---

## Run Record Namespace

Run start and end records ([CONTRACT_RUN.md](CONTRACT_RUN.md) §Run Records) are stored
outside Lode's partition tree, in a namespace Quarry owns inside the
dataset:

```
datasets/<dataset>/runs/<source>/<category>/<day>/<run_id>/start.json
datasets/<dataset>/runs/<source>/<category>/<day>/<run_id>/end.json
```

- Records are written with `Store.Put`, not `Dataset.Write`. They are not
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
  matching start-record keys lexicographically and cut pages on them. The
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

The `run` command rejects, before anything is written, values that cannot form a safe
key, as a configuration error:

- `source`, `category`, `run_id`: empty, `.`, or `..` (Hive encoding
  leaves `.` and `..` unchanged), or a value with a `.` or `..` path
  segment (split on `/` or `\`). Sidecar file paths (§Sidecar File
  Inventory) still interpolate these values unencoded, so such a segment
  could place one run's files under another run's partition.
- The runtime builds sidecar file paths, and the key the SDK reports from
  `storage.put()`, from the raw values. For a value that Hive encoding changes
  (one containing `/`, `\`, `%`, a space, and so on), the sidecar file
  path therefore differs from the run's partition prefix. Run records and
  the partition tree always agree.
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
are **not** tracked by Lode's `Manifest.Files`. They are the caller's
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
  `WriteMetrics` calls only. These are consumer-facing commit boundaries.
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

## Run Retention

Retention removes whole runs, chosen by their state and age. Quarry
chooses the runs, because only Quarry knows a run's outcome. Lode's
`Dataset.Expire` removes their snapshots (Lode [CONTRACT_RETENTION.md](https://github.com/pithecene/lode/blob/main/docs/contracts/CONTRACT_RETENTION.md)), and
Quarry deletes the objects it owns. The `quarry/retention` package
implements it, and `quarry prune` ([CONTRACT_CLI.md](CONTRACT_CLI.md) §prune) exposes it.

### Selection

- Runs are found from their start records ([CONTRACT_RUN.md](CONTRACT_RUN.md) §Reading Run
  Records), under the leading selectors `source`, `category`, and `day`,
  or one run named by all three and its run ID. A run without a start
  record is not found, so `quarry index rebuild` must index older runs
  first.
- A run's state is the state that `list runs` reports: `succeeded`,
  `failed`, or `unterminated`. The policy gives each state an age at
  which its runs are removed. The prune keeps every run in a state without an age.
- A terminated run's age is measured from its end record's `ended_at`.
  An unterminated run's age is measured from the later of its start
  record's `started_at` and the instant its most recent snapshot began
  (read from the snapshot ID), so a run that started long ago and is
  still writing is not selected. A backfilled start record has no
  `started_at`, so the end of its partition day stands in.
- A commit to an artifact Volume is a write of the run, so its snapshot
  counts among the most recent snapshots. The prune lists the names under
  the run's `volumes/` prefix and reads each Volume's head through Lode's
  `volume.Head`. That call needs no total length, so the Volume of an
  artifact whose event never arrived is dated too. A pending commit dates the
  Volume as a completed one does. A Volume with no commit, as one that
  holds only staged ranges, and a removed Volume date no write.
  The prune reads the heads again after it previews the removal of the
  run's Volumes. A head that moved, or a Volume that gained its first
  commit, keeps the run, because the age did not judge that write.
- A Volume whose head Lode cannot tell, because its pointer is absent
  beside its manifests, has no instant for its last write. The prune
  keeps the run, deletes nothing of it, and the report gives the reason.
  A name under `volumes/` that is no artifact ID keeps the run likewise.
- The minimum age protects a live run before its first commit, because a
  live run's age never exceeds its duration.
- An unterminated run cannot be told apart from a live one, so its age
  must be at least 24 hours and must exceed the longest run. A
  terminated run is final ([CONTRACT_RUN.md](CONTRACT_RUN.md) §Run Finalization), so any age,
  including zero, is safe.
- A start-record key that the write path could not have produced
  (including a day that is not `YYYY-MM-DD`), and a
  start record whose fields do not describe the key it is stored at, are
  reported and never acted on. A record therefore cannot direct deletion
  at another run.

### Order of Operations (per Run)

1. Preview the expiry of the run's partition prefix (the start record's
   `partition`) as a dry run. If the run holds the dataset's head, has a
   snapshot with files outside its partition, or the head is uncommitted
   or absent, the prune keeps the run, deletes nothing of it, and the report
   gives the reason. Lode never expires the head, so the run that
   committed most recently is removed only by a later prune.

   Then preview the removal of the run's artifact Volumes. The prune
   lists the names under the run's `volumes/` prefix and calls Lode's
   `volume.Remove` for each against a sandbox store. The sandbox reads
   the real store and keeps every write in memory, so Lode's own code
   runs and the real store is unchanged. Three things keep the run, and
   nothing of it is deleted: a name that is no artifact ID, an object
   stored at an artifact ID itself rather than in its directory, and a
   Volume with a commit in flight. The preview records the head pointer swap that Lode makes,
   and step 5 allows only that swap. A dry run reports what the preview
   found.
2. Gather the run's sidecar files and their `.meta.json` companions
   (§Sidecar File Inventory): every path that the run's snapshots record,
   and, when Hive encoding leaves the run's source, category, day, and
   run ID unchanged, every direct child of its sidecar prefix. A path
   more than one level down can belong to a run whose ID continues the prefix, such as
   `r/files/x` beneath run `r`, so it is never listed. Without that
   condition, a direct child could also belong to another run, so the
   prefix is not listed, and a file written after the run's last commit
   remains. A recorded path outside the run's sidecar prefix is reported
   and left in place. A recorded inventory in any other form names files
   that cannot be found, so the prune keeps the run, deletes nothing of it,
   and the report gives the reason. The paths are gathered first because
   the recorded ones are in the manifests that step 3 deletes.
3. Expire the snapshots that the preview selected with `Dataset.Expire`,
   naming their IDs and the partition prefix, and then list the partition
   again. A snapshot that committed after the preview was not judged by
   it, so it is never expired. If one remains, the prune keeps the run with its
   sidecar files and records. A run with no snapshot in the preview (one
   that committed nothing) has nothing to expire, and Lode requires no
   head for a dataset without snapshots, so such a run loses only its
   sidecar files and records.
4. Delete the gathered sidecar objects.
5. Remove the previewed Volumes with `volume.Remove`, so the Volume of an
   artifact whose event never arrived goes too. A Volume removed already
   changes nothing in the preview and is skipped. Each removal allows only
   the swap its preview recorded, and the store compares the head
   atomically. A Volume that committed since the preview therefore fails
   its removal and keeps the run's records. Then list the names again. A
   name the preview did not list belongs to a Volume opened since, so the
   prune keeps the run's records, and the next prune removes it.
6. Delete the start record, and then the end record. A run without a
   start record is never listed, and an end record without one is
   ignored by readers.

An end record without a start record names no run: an earlier prune
stopped between the two deletions, or the run's start record was never
persisted. Every prune deletes such end records under its selectors, and
the report counts them.

Each step is idempotent, and a run is listed until step 6. A prune that is
interrupted, or that fails with a store error, is therefore completed by
the next prune that selects the run, with one exception: if it stops
after step 3 for a run whose key Hive encoding changes, the recorded
sidecar paths are gone with the manifests, and those files remain. If step 3 is interrupted after Lode
deleted the run's last manifest, the next prune no longer finds the
snapshot's residual data, and `Dataset.Sweep` removes it.

### Sweep

When a grace period is given, a prune ends with `Dataset.Sweep` over the
whole dataset, whatever the selectors. It completes interrupted expiries
and removes the data of writes that never committed, such as the last
batch of a killed run, once that data is older than the grace period.
The grace period must exceed the longest single write. The sweep never removes
data that a manifest describes.

`Dataset.Sweep` never reads the artifact namespace, so the prune then
lists `artifacts/<dataset>/`. It removes every Volume of a run that has
no start record, once the end of the run key's day is older than the
grace period. Such a Volume belongs to a run whose records an earlier
prune removed, or was opened after a prune's final listing. A run's day
is its start's day, and the grace period exceeds the longest run, so the
Volume of a live run is never removed. Each Volume is previewed and
removed as step 5 removes it. A Volume with a commit in flight, or one
that commits meanwhile, is left for the next sweep, and a dry run does
not count it. A Volume removed already is not counted. A key whose run
key or artifact ID the write path could not have produced is reported
and never acted on. The report counts the Volumes removed.

### Concurrency

A prune can run while runs are written to the dataset. A run that the
policy selects is presumed final: a terminated run is final by
[CONTRACT_RUN.md](CONTRACT_RUN.md) §Run Finalization, and an unterminated run has written
nothing for at least its age. If such a run nevertheless commits during
the prune, step 3 keeps its new snapshot and the run's records remain.
Only a commit after that final listing goes undetected. Step 5 removes
only the Volume heads that its preview judged, so a Volume commit during
the prune keeps the run's records. A Volume opened after step 5's final
listing goes undetected, and the sweep removes it. A prune does not coordinate with readers of the run.

