# Artifact Volumes Design

This document designs the replacement for the experimental artifact byte
path (`PUBLIC_API.md` § Experimental Surface). The design settles the open
questions of [#255](https://github.com/pithecene/quarry/issues/255) and
answers [#341](https://github.com/pithecene/quarry/issues/341). It is not
a contract: each contract changes in the pull request that makes the code
satisfy it.

---

## 1. One Cause

Quarry stores an artifact's bytes in the Dataset, as base64
`artifact_chunk` records in the same `event_type=artifact` partition as
the `artifact_event` commit record (`CONTRACT_LODE.md` § Artifact Chunk
Storage). Five costs follow from that one layout:

1. **Reads follow stored bytes.** `ReadEvents` must read the artifact
   partition to find the commit records, and so decodes every chunk with
   them (#341). A run that retained one 596 MB archive as 72 artifacts
   took about 4 minutes and 920 MiB to list 148 events.
2. **Runtime memory follows stored bytes.** `ArtifactManager`
   (`quarry/runtime/artifacts.go`) appends every chunk, data included, to
   its accumulator and never releases it, although the policy has already
   written the chunk.
3. **Storage is a third larger.** Base64 adds about 33 percent to every
   stored byte.
4. **No object outgrows a buffer.** `emit.artifact` takes the whole
   artifact in memory (Known Limitation 3), and a chunk is at most 8 MiB.
   A script that retains a larger object must split it into artifacts and
   leave every reader to stitch them back.
5. **No range reads.** Reading part of an artifact decodes every chunk
   that covers the part.

Approaches that keep bytes in the Dataset and work around the read, such
as sniffing `record_kind` before decoding, caching, parallel decoding or
more memory, leave each cost tied to the bytes a run retained.

---

## 2. The Rule

> Artifact bytes live in Lode Volumes. The Dataset holds only records.

Lode's Volume is a byte address space `[0..N)` with staged range writes,
cumulative manifests, CAS commits and range reads that refuse a missing
range (Lode `CONTRACT_VOLUME.md`). An artifact is exactly that: an object
of known length, written in ranges, whose committed ranges are the
authority for what a reader may read.

The Dataset stays the catalog. `event`, `artifact_event` and `metrics`
records stay where they are. New runs write no `artifact_chunk` records.

---

## 3. Storage

### Placement

Each artifact is one Volume, with the artifact ID as its Volume ID. The
Volume store is rooted at the run's own prefix in Quarry's run namespace
(`CONTRACT_LODE.md` § Run Record Namespace):

```text
datasets/<dataset>/runs/<source>/<category>/<day>/<run_id>/volumes/<artifact_id>/...
```

- The run key components are Hive-encoded by the one key builder, as for
  run records, so a run's Volumes can neither escape the run nor share a
  prefix with another run's.
- The artifact ID is the SDK's UUID. The runtime refuses an artifact ID
  that is not one canonical path segment, and Lode's `volume.New` refuses
  one too.
- Lode's Dataset never lists this prefix. `Dataset.Sweep` lists only the
  dataset's snapshot and tombstone prefixes.
- `volume.New` writes under the store it is given (`volumes/<id>/...`),
  and Quarry holds one store rooted at the dataset's storage root. Lode
  gains a prefix-scoping factory, `lode.Prefixed(factory, prefix)`, whose
  store maps every key, listing and range read under `prefix` for any
  backend. Quarry opens a run's Volumes through
  `lode.Prefixed(root, <run prefix>)`. The S3 store's `Prefix` and
  `lode.FS(root)` would serve two backends, but not the memory store or a
  caller's own.

### Commit record

The commit record (`artifact_event`) keeps its fields and gains the
Volume reference:

| Field | Type | Description |
|---|---|---|
| `storage` | string | `"volume"`, or `"empty"` for an artifact of zero bytes (§4). Absent in records written before this change, whose bytes are chunk records |
| `volume_snapshot_id` | string | With `"volume"` only: the Volume snapshot whose manifest covers `[0..size_bytes)` |
| `sha256` | string | Hex SHA-256 of the artifact's bytes, computed by the runtime |

A reader reads an `"empty"` artifact as zero bytes and opens nothing.

The Volume ID is the `artifact_id` the record already carries, so the
record does not repeat it.

### Ordering

> The runtime commits the Volume snapshot that covers the whole artifact
> before it writes the `artifact_event`.

A reader that finds an `artifact_event` can therefore always read the
committed bytes, as the current rule that chunks precede their commit
record guarantees today.

---

## 4. Transport

### Frames

Three frames replace `artifact_chunk`:

- `artifact_open`: `artifact_id`, `size_bytes`, and an optional
  `expected_sha256`. The runtime creates the Volume with `size_bytes` as
  its total length before any byte arrives.
- `artifact_range`: `artifact_id`, `range_id`, `offset`, `data` (at most
  8 MiB, as the frame limit requires). The `range_id` is a correlation ID
  that counts up from 1 within the run, as `write_id` does for
  `file_write`.
- `artifact_range_ack`, from the runtime to the executor: `range_id`,
  `ok`, and an `error` when `ok` is false. The runtime sends it once the
  range is committed, or once the range is refused.

Ranges may arrive in any order and must not overlap. The runtime refuses
a range for an artifact that has no `artifact_open`, and a range that
ends past `size_bytes`. The `artifact` event stays the commit record. The
runtime accepts it only when its `size_bytes` equals the opened length
and committed ranges cover `[0..size_bytes)`, and refuses it otherwise.

An artifact of zero bytes has no byte to store, and a Volume's length is
positive, so an empty artifact has no Volume. The protocol stays the
same: the executor sends its `artifact_open` with `size_bytes` 0, and
any `expected_sha256` with it. The runtime creates no Volume, refuses any
range, checks the expected digest against the SHA-256 of no bytes, and
writes the record with `storage` `"empty"`. The rule follows from the
length, not from a size threshold: every artifact with a byte is a
Volume.

The executor and the runtime ship in one release, and the frames are part
of the experimental surface, so the old frame is retired, not kept beside
the new ones.

### Runtime

- Each `artifact_range` is staged with `StageWriteAt` and committed before
  the runtime reads the next frame, so IPC backpressure bounds the bytes
  in flight to one frame.
- The runtime keeps only block references and the hash state. It never
  keeps an artifact's bytes.
- A commit per range makes every received range durable. A run that
  stops part way leaves committed snapshots that name exactly the ranges
  that arrived. A commit costs 5 store calls and writes a manifest of
  O(B) blocks, so an artifact of B ranges writes O(B²) manifest bytes. A
  1 GiB artifact in 8 MiB ranges has 128 blocks.
- The manager's per-artifact state is released when the `artifact_event`
  is written.
- When the run ends, before its end record, the runtime removes the
  Volume of every artifact it opened whose `artifact_event` was not
  written, as a script that stops writing leaves committed ranges with no
  record. A runtime that dies first leaves them to `quarry prune` (§7).

### Digest

The runtime hashes the covered prefix `[0..p)`:

- A range at offset `p` is hashed as it passes, and `p` advances.
- A range ahead of `p` is committed and not hashed. When `p` reaches it,
  the runtime reads it back with `ReadAt` and hashes it.

Bytes that arrive in order, the common case, are never read back. The
digest is the runtime's own, computed from what it committed, so a
reader needs no trust in the script. A script may also declare the
SHA-256 it expects (`expected_sha256` in `artifact_open`), and the
runtime then refuses an `artifact` event whose computed digest differs.

---

## 5. Script API

```ts
const writer = await ctx.emit.artifactWriter({
  name: 'aisdk-2026-10-20.zip',
  content_type: 'application/zip',
  size_bytes: 595_847_268,
  sha256: '1ded…', // optional: the runtime refuses a mismatch
})
await writer.writeAt(0, firstRange)
await writer.writeAt(8_388_608, secondRange)
const artifactId = await writer.commit()
```

- `writeAt` sends `artifact_range` frames of at most 8 MiB. It resolves
  when every frame's `artifact_range_ack` reports `ok`, and rejects with
  the runtime's error otherwise, so a resolved range is committed and a
  script holds one range at a time.
- `commit` emits the `artifact` event.
- `emit.artifact({ name, content_type, data })` stays, and is a writer
  with one `writeAt` at offset 0. No size threshold switches storage
  behavior, and an empty `data` follows the zero-length rule of §4.

The script keeps acquisition policy (URLs, authentication, Range
requests, retries), and the runtime owns storage. A runtime-performed
fetch was considered and declined. It would move an HTTP client, its
proxy and authentication handling, and its retry policy into the runtime,
and it would serve only sources a plain fetch can reach.

---

## 6. Reading

- `ReadEvents` is unchanged. The artifact partition of a new run holds
  only `artifact_event` records, so listing events reads no artifact
  bytes, whatever the run retained. This is #341's acceptance test.
- The reader gains an artifact read. It opens the Volume named by an
  `artifact_event` and reads the committed snapshot in ranges, as an
  `io.Reader` or with `ReadAt`.
- A record without a `storage` field is read from its chunk records, as
  today. Runs written before the change keep #341's cost, and no backfill
  converts them.

---

## 7. Retention

Lode's Volumes have no retention (Lode `CONTRACT_RETENTION.md` excludes
them). Lode owns its persisted format, so Lode gains the removal:

- `volume.Remove(ctx, id, factory)` first replaces the head pointer with
  a tombstone through `CompareAndSwap`. A commit that races the removal
  then fails its own swap, and a commit that reloads the head finds the
  tombstone and returns an error instead of starting a new history. The
  removal then deletes the manifests, staged manifests and data, and keeps
  the tombstone. The tombstone stays as a fence: a writer that held the
  Volume open through the removal can never commit again, and a second
  call completes an interrupted removal. Each removed Volume therefore
  leaves one small object, as Lode's Dataset expiry leaves its own
  tombstones. The Lode pull request settles the tombstone's form and
  error.

The prune command (`quarry prune`) removes a run's Volumes before the run's records
(`CONTRACT_LODE.md` § Order of Operations). It lists the run's `volumes/`
prefix, so the Volumes of artifacts whose event never arrived are removed
too, and calls `volume.Remove` for each. The prune then lists the prefix
again, as step 3 lists the partition again after expiry. A Volume opened
after the first listing, or a range staged into a removed Volume, leaves
an object other than a tombstone. The prune then keeps the run's
records, and the next prune that selects the run removes what remains.
Only an object written after that final listing goes
undetected, as only a commit after step 3's listing does
(`CONTRACT_LODE.md` § Concurrency).

A run writes Volume snapshots between its Dataset writes, and an artifact
that takes long to acquire writes nothing to the Dataset until its event.
What keeps a live run from selection is the rule that an unterminated
run's age must exceed the longest run (`CONTRACT_LODE.md` § Selection). A
live run's last durable write is no older than its start, so its age is
at most its duration, whatever the gap between its writes, before its
first range included. Selection also counts the run's newest Volume
snapshot as a write, so the measured age stays a true last-write age. A
Volume snapshot ID carries its creation time, as a Dataset snapshot ID
does, so selection reads it from the pointer of each Volume under the
run's prefix. A terminated run writes nothing after its end record, so its age
stays the end record's.

---

## 8. Lode

Quarry becomes the first consumer that writes Volumes, which is the
evidence Lode's Volume gate waits for (Lode `V1_READINESS.md` §4). Lode
needs two changes: `lode.Prefixed` (§3) and `volume.Remove` (§7).
`ReadAt` returns a slice, and range-sized reads are enough for
streaming, so no streaming reader is required.

---

## 9. Order of Work

1. **Lode:** `lode.Prefixed` with its text in `CONTRACT_STORAGE.md`,
   `volume.Remove` with its text in `CONTRACT_VOLUME.md` and
   `CONTRACT_RETENTION.md`, and a release.
2. **Quarry reader:** the artifact read, over chunk records as they are
   stored today and over a Volume named by an `artifact_event`. Tests
   write the Volume case directly.
3. **Quarry write path:** `artifact_open` and `artifact_range`, the
   Volume store at the run prefix, the runtime's staging, commit and
   digest, the released manager state, the `artifact_event` fields, and
   `emit.artifact` sending the new frames. `CONTRACT_LODE.md` and
   `CONTRACT_IPC.md` change here.
4. **Quarry SDK:** `artifactWriter`. `CONTRACT_EMIT.md` and
   `PUBLIC_API.md` change here.
5. **Quarry retention:** `quarry prune` removes a run's Volumes.

Each step leaves `main` releasable. A Volume needs its total length
before its first byte, and today's frames do not carry it, so storage and
transport change together in step 3. That step changes the executor and
the runtime in one release, and retires `artifact_chunk` with them.

---

## 10. Decisions on #255's Open Questions

| Question | Decision |
|---|---|
| Who downloads | The script, through `artifactWriter` (§5) |
| The record that links content to a run's Volumes | `artifact_event` with `volume_snapshot_id` and `sha256` (§3) |
| Deleting Volumes | A Lode primitive, `volume.Remove` (§7) |
| Small artifacts | One Volume each, with no threshold. An empty artifact has none (§4) |
| Migration | Old records stay readable from chunk records, with no backfill (§6) |
| Volume layout | Under the run's prefix in the run namespace (§3) |
| Content identity and deduplication | Downstream, as `CONTRACT_LODE.md` § Scope already states |
| Chunk trees (many objects per acquisition) | One artifact per object. A tree is many artifacts |
