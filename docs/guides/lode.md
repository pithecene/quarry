# Lode Expectations (User View)

Quarry stores every run in a [Lode](https://github.com/pithecene/lode)
dataset on the filesystem or in S3. This guide explains what Quarry relies
on Lode to guarantee, how a run's data is partitioned and stored, and how
each storage backend behaves. The
[storage contract](../contracts/CONTRACT_LODE.md) states the rules.

---

## What Lode Guarantees

- Append-only storage for events.
- Stable partitioning by source, category, day, run, and event type.
- Consistent layout across different ingestion policies.

---

## Why This Matters

Users can query data without knowing which policy produced it, and runs
remain immutable once written.

## Partitioning Semantics (Informational)

- Lode dataset ID is fixed to `quarry` (global container).
- `source` is the origin system/provider (required).
- `category` is the logical data type within a source (required).
  For one-to-one sources, use `category=default`.
- `day` is derived from the **run start time** (not individual event timestamps).
  Events may extend across dates but remain in the run's `day` partition.
- Preferred Hive ordering: `source / category / day / run_id / event_type`.

---

## Record Types

Every stored record includes a `record_kind` field to identify its type:

| Kind             | Description                                    |
|------------------|------------------------------------------------|
| `event`          | Standard event (item, log, checkpoint, etc.)   |
| `artifact_event` | Artifact commit record (marks artifact complete)|
| `artifact_chunk` | Binary chunk of an artifact, in older data only |
| `metrics`        | Metrics of a run, in older data only           |

This allows queries to filter by record type without parsing payloads.

---

## Artifact Storage

The bytes of an artifact are stored in a Lode Volume of the run, and its commit record
names that Volume. The commit record is the durability boundary: a Volume without one is
an orphan. The runtime removes its run's orphans, and `quarry prune` removes the rest.
The [storage contract](../contracts/CONTRACT_LODE.md) § Artifact Storage states the rules.
Older data holds artifact bytes in chunk records, which readers still read.

---

## Dataset Data Objects

New datasets use zstd compression and SHA-256 digests on Lode data objects.
Existing datasets keep their recorded compressor and checksum settings.
Lode verifies a recorded digest when a read reaches the end of the object.

## Chunk Record Checksums

Chunk records of older data may include an MD5 checksum for integrity verification.

- `checksum`: hex-encoded MD5 digest of chunk data
- `checksum_algo`: always `"md5"` when checksum is present

Quarry leaves these chunk fields unset. Downstream consumers validate them
when present.

---

## Storage Backend Behaviors

### Filesystem (FS) Backend

**Path Validation:**
- The `--storage-path` must exist and be a directory before `quarry run` is called.
- Path validation happens at CLI startup, before executor launch.
- If the path is not writable, the first write operation will fail.

**Error Handling:**
- Storage errors are classified and wrapped with context (operation, path).
- A failed event write, such as disk full (`ENOSPC`) or permission denied (`EACCES`), ends
  the run as `storage_error`, unless its sink is best effort.
- Not-found errors (missing directories) are surfaced at initialization or write time.
- Quarry does **not** retry failed data writes. It retries only a commit that conflicts with
  another writer, and the end record.
- The ingestion policy determines whether partial data is preserved on failure.
- Error messages include actionable context (what operation failed, where).

### S3 Backend

**Authentication:**
- S3 uses the AWS SDK default credential chain:
  1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
  2. Shared credentials file (`~/.aws/credentials`)
  3. IAM instance role (EC2/ECS/Lambda)
- Credential validation happens on the **first write attempt**, not at startup.
- If credentials are invalid or expired, the first write fails with an auth error.

**Error Handling:**
- Storage errors are classified and wrapped with context (operation, path).
- A failed event write, such as an authentication, access, throttling, or timeout error, ends
  the run as `storage_error`, unless its sink is best effort.
- Error messages include actionable context (what operation failed, where).

**Consistency Caveats:**
- S3 provides **strong read-after-write consistency** for PUTs (since Dec 2020).
- However, Quarry does **not** guarantee transactional semantics across multiple writes.
- If a run fails mid-stream, partial data may exist in S3.
- `quarry prune` removes the data that a failed run left.

**Required IAM Permissions:**
```json
{
  "Effect": "Allow",
  "Action": [
    "s3:PutObject",
    "s3:GetObject",
    "s3:DeleteObject",
    "s3:ListBucket"
  ],
  "Resource": [
    "arn:aws:s3:::bucket-name",
    "arn:aws:s3:::bucket-name/*"
  ]
}
```

---

## Sidecar File Inventory

Files written via `ctx.storage.put()` are tracked in Lode snapshot metadata
so downstream consumers can discover them without scanning storage.

Each snapshot that includes sidecar file writes will have a `sidecar_files`
entry in `Manifest.Metadata` containing an array of file references:

```json
{
  "sidecar_files": [
    {
      "path": "datasets/quarry/partitions/source=my-source/category=default/day=2026-03-22/run_id=run-001/files/product-image.png",
      "filename": "product-image.png",
      "content_type": "image/png",
      "size": 14230
    }
  ]
}
```

**Key behaviors:**
- If multiple event flushes occur during a run, file refs appear on the
  first event snapshot after each batch of file writes
- To enumerate all files for a run, read `sidecar_files` from its end record.
  When that names none, union `sidecar_files` across all snapshots for that
  `run_id`

---

## Design Non-Goals

The following behaviors are explicitly **out of scope**:

**No Automatic Retries:**
- Quarry does not retry failed data writes.
- If a write fails, the error propagates to the policy and run outcome.
- Callers are responsible for implementing retry logic at the job/orchestration level.

**No Backpressure:**
- Quarry does not implement backpressure for slow storage backends.
- Events are written as fast as the policy allows.
- For high-throughput scenarios, use buffered policy with appropriate limits.

**No Deduplication:**
- Duplicate detection is a downstream consumer responsibility.
- If a run is retried, it may produce duplicate events with different run IDs.

**No Automatic Retention:**
- Quarry deletes stored runs only when `quarry prune` runs.
