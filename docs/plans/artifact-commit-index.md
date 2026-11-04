# Artifact Commit Index Design

This document describes an index that lets a reader find one artifact's
commit record without reading every commit record of its run. The index
answers [#367](https://github.com/pithecene/quarry/issues/367). This
document is not a contract: each contract changes in the pull request
that brings the code into conformance with it.

---

## 1. Cost of Opening One Artifact

The command `read artifact` opens one artifact and needs only that
artifact's commit record. The commit records of a run lie in its `event_type=artifact`
partition, and Lode can find a record there only by loading the
partition's manifests. A run commits its events in batches, and each batch forms
one snapshot. The artifact partition therefore contains at least one
snapshot for each batch that carried an artifact's commit record, and a
retried write can add more (`CONTRACT_POLICY.md` § Flush Semantics). This document
calls the number of those snapshots S, and the cost of reading the
partition grows with S. The function `OpenArtifact` first lists the
partitions under the run. That listing loads the manifest of every
snapshot of the run. This document calls the number of
those snapshots R. R counts the
snapshots of item, checkpoint, and metrics batches as well as the S
snapshots that hold commit records. The
function then lists the artifact partition's S manifests and loads each
one a second time. The function loads each of those S manifests a third time to read
that manifest's snapshot, and it reads each snapshot's data object. The partition read
therefore costs about R + 3S store reads. In the 72-artifact run of
#367, almost every snapshot holds a commit record, so R nearly equals S
and the read costs about four store reads per snapshot. Issue #367 records 294 store
requests and 46 seconds in `OpenArtifact` to open one artifact of a
72-artifact run over Cloudflare R2.

Two changes reduce the cost:

- **One load per manifest.** Lode's `Manifests` loads each manifest once,
  as it lists the manifest, whereas `ListManifests` and `GetManifest` load
  each manifest twice. A reader that names the artifact partition directly,
  instead of listing the run's partitions first, then makes two store reads
  per snapshot instead of four. Its cost still grows with S.
- **An index by artifact ID**, the subject of this document. The index
  makes the cost of opening one artifact independent of S.

---

## 2. Authority of the Commit Record

The commit record in the Dataset is the only authority on an artifact's
commit. An index entry states no commit. It names a snapshot, and the
reader looks for the artifact's commit record there first.

- **The runtime writes an entry only after the Dataset write that
  persists the commit record succeeds.** An entry that the runtime did not
  write can exist too, and the runtime leaves it in place (§3).
- **The reader acts only on a record it finds.** When the named snapshot
  holds the artifact's commit record and that record names a Volume or an
  empty artifact, the reader takes every fact from that record. In the
  fallback cases of §4, the reader reads the partition. When it finds the
  record there, the reader takes every fact from that record, and when it
  finds none, the read returns `ErrArtifactNotFound`.

A missing, stale, or wrong entry therefore costs a partition read and
never produces a wrong answer.

---

## 3. Storage

Each committed artifact has one index entry:

```
artifacts/<dataset>/<run key>/commits/<artifact_id>
```

- The `commits/` prefix is Quarry's. It sits beside `volumes/`, whose
  directories Lode seals, so no entry lies inside a Volume, and
  retention's listing of `volumes/` never returns an entry.
- The entry is a JSON object with one member, `snapshot_id`. That member
  holds the ID of the Dataset snapshot whose `event_type=artifact`
  partition holds the artifact's commit record.
- The runtime writes the entry once, with Lode's put-if-absent `Put`. When
  an entry exists already, the runtime leaves it in place and counts the
  write as failed (§5).
- An empty artifact has an entry too, because only its commit record shows
  that it exists.

---

## 4. Reading

The function `OpenArtifact` reads the entry and opens a view of the
Dataset. To open the view, the function reads the head pointer and the
head's manifest. The function then loads the named snapshot's canonical
manifest with `Snapshot`, reads that snapshot's data object in the run's
artifact partition, and selects the artifact's commit record. The function
reads the canonical manifest because an interrupted expiry can leave a
snapshot's partition manifest and data behind. For such a snapshot,
`Snapshot` returns `ErrSnapshotExpired`, whereas a read of the residual
partition manifest would return the expired snapshot's records. Opening the artifact's
Volume adds the Volume's own reads. The number of store reads in this
sequence does not depend on S, and the harness of §8 counts the whole
sequence.

The reader falls back to the partition read in five cases:

- No entry exists, as for a run written before the index.
- The entry does not decode or names no valid snapshot ID.
- Retention has expired the snapshot, removed it, or deleted one of its
  data objects.
- The snapshot does not hold the artifact's commit record.
- The record names chunk storage, like every record from a release before
  0.15. The artifact's chunk records can lie in other snapshots, so the
  reader cannot assemble the artifact from one snapshot.

The reader's caller can supply an observer, and the reader reports
each fallback and its case to that observer. The command
`quarry read artifact` supplies one that writes the report to stderr.
An operator therefore sees which reads fell back to the partition, and
why.

The fallback returns what the partition read returns today, including
`ErrArtifactNotFound`. That read lists partition manifests. After an
interrupted expiry, Lode's listing may return the expired snapshot until
the next `Sweep` or `Expire` completes that expiry (Lode
`CONTRACT_RETENTION.md` § Snapshot Expiry). The fallback can therefore
still return such a snapshot's record, as the partition read does
without the index.

The commands `list artifacts` and `list events` read every record, so
they continue to read the partition through `Manifests` (§1).

---

## 5. Writing

The method `LodeClient.WriteEvents` writes a batch to the Dataset. When the write
succeeds and the batch holds commit records, `WriteEvents` writes one
entry for each record. Each entry names the snapshot that the Dataset
write returned.

A failed entry write does not fail the batch. The Dataset already holds
the batch, and an error would lead a caller to write the events again. The runtime
therefore counts the failure in a new counter,
`artifact_index_write_failure_total`, in the run's metrics record. The
artifact remains committed, and the reader finds its record through the
fallback. A crash between the
Dataset write and the entry write leaves the same state: a committed
artifact without an entry.

---

## 6. Retention

An entry is never authoritative, so retention may delete it at any step
without causing a wrong answer. A retention prune deletes a run's entries
together with the run's Volumes, in step 5 of `CONTRACT_LODE.md` § Order of Operations,
after it expires the run's snapshots. A prune that stops before step 5
leaves entries that name expired snapshots. For such an entry, the reader
falls back to the partition read, and the next prune deletes the entry. When a run has no start record,
the retention sweep deletes the run's entries together with the run's
Volumes.

---

## 7. Failure Cases

| Case | Reader's result |
|---|---|
| A run predates the index | Partition read |
| The process stops between the Dataset write and the entry write | Partition read |
| The entry write fails | Partition read, and the runtime counts the failure |
| The entry names a snapshot that retention expired | Partition read. The read finds the record when a retried write persisted the record in another snapshot or when an interrupted expiry left the record behind. Otherwise the read returns `ErrArtifactNotFound`. |
| The entry names a snapshot without the artifact's record | Partition read |
| An entry exists before the runtime writes one | Snapshot read if the entry's snapshot holds the artifact's record, otherwise partition read, and the runtime counts a failed write |

---

## 8. Measurement

The `cmd/readtrace` harness (#365) counts the store requests of
`OpenArtifact`. Over R2, the runtime writes a 1-artifact run and a
72-artifact run, each with its index entries, and the harness reads one
artifact of each. The pull request that adds the index records both
counts. Issue #367 records 10 and 294 requests for the same run
sizes without the index. A benchmark over a store with simulated latency
reports the request counts for runs of 1, 72, and 1,000 artifacts.

---

## 9. Order of Work

1. This document.
2. A call to Lode's `Manifests` in `ReadPartitionRecords`, once Lode
   releases that function.
3. The index: the write in `WriteEvents`, the read in `OpenArtifact`, the
   counter, entry removal in retention, `CONTRACT_LODE.md`,
   `CONTRACT_METRICS.md`, and `CONTRACT_CLI.md`. The same change extends
   retention to the new prefix, because its sweep would otherwise report
   every entry as an unparseable key.
