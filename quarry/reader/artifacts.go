package reader

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/pithecene/lode/lode"
	"github.com/pithecene/lode/lode/volume"

	qlode "github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/types"
)

// ArtifactStorage is where an artifact's bytes are stored, as its commit
// record states (docs/plans/artifact-volumes.md §3).
type ArtifactStorage string

const (
	// ArtifactStorageChunks is an artifact whose bytes are chunk records in
	// the run's artifact partition: its commit record has no storage field,
	// because the run was written before artifact bytes moved to Volumes.
	ArtifactStorageChunks ArtifactStorage = "chunks"
	// ArtifactStorageVolume is an artifact whose bytes are a Lode Volume under
	// the run's artifact directory, in the snapshot its commit record names.
	ArtifactStorageVolume ArtifactStorage = "volume"
	// ArtifactStorageEmpty is an artifact of zero bytes, which has no Volume.
	ArtifactStorageEmpty ArtifactStorage = "empty"
)

var (
	// ErrArtifactNotFound is returned when the run has no commit record for
	// the artifact.
	ErrArtifactNotFound = errors.New("artifact not found")
	// ErrArtifactIncomplete is returned when the stored bytes do not cover
	// the size the commit record states: a chunk is missing or out of place,
	// or the Volume snapshot does not cover the artifact.
	ErrArtifactIncomplete = errors.New("artifact incomplete")
)

// artifactPartition is the run-relative path of the partition that holds a
// run's artifact commit records and, for a run written before Volumes, its
// chunk records.
var artifactPartition = "event_type=" + string(types.EventTypeArtifact)

// artifactRecords returns every record in the run's artifact partition.
func (r *Reader) artifactRecords(ctx context.Context, run Run) ([]lode.D, error) {
	path := run.Partition + artifactPartition
	return r.readPartitions(ctx, path, func(p lode.PartitionPath) bool { return p.String() == path })
}

// Artifact is one artifact as the run's commit record describes it.
type Artifact struct {
	ArtifactID  string          `json:"artifact_id" yaml:"artifact_id"`
	Name        string          `json:"name" yaml:"name"`
	ContentType string          `json:"content_type" yaml:"content_type"`
	SizeBytes   int64           `json:"size_bytes" yaml:"size_bytes"`
	Seq         int64           `json:"seq" yaml:"seq"`
	Storage     ArtifactStorage `json:"storage" yaml:"storage"`
	// VolumeSnapshotID names the Volume snapshot whose manifest covers the
	// artifact. It is set for ArtifactStorageVolume only.
	VolumeSnapshotID string `json:"volume_snapshot_id,omitempty" yaml:"volume_snapshot_id,omitempty"`
	// SHA256 is the hex digest the runtime computed of the artifact's bytes.
	// It is empty for a run written before Volumes.
	SHA256 string `json:"sha256,omitempty" yaml:"sha256,omitempty"`
}

// ArtifactReader reads an artifact's bytes as a stream or by range. Its
// reads use the context that OpenArtifact received.
type ArtifactReader interface {
	io.ReadSeeker
	io.ReaderAt
}

// ListArtifacts returns the run's artifacts in sequence order, from the
// commit records in its artifact partition, one per artifact ID. A run
// written before Volumes keeps its chunk records in that partition, so
// listing its artifacts reads its artifact bytes.
func (r *Reader) ListArtifacts(ctx context.Context, run Run) ([]Artifact, error) {
	if _, err := r.validateRun(run); err != nil {
		return nil, err
	}
	recs, err := r.artifactRecords(ctx, run)
	if err != nil {
		return nil, err
	}
	return artifactsOf(recs), nil
}

// artifactsOf reads the commit records among recs in sequence order. A
// retry under an at-least-once policy may persist a commit record twice
// (CONTRACT_POLICY.md §Flush Modes), so the first record of each artifact
// ID stands for the artifact.
//
// A commit record that a release before 0.15 wrote may state a size of 0
// for an artifact of any size, so the size of a chunk-stored artifact whose
// record states 0 comes from its chunks (chunkedSizes).
func artifactsOf(recs []lode.D) []Artifact {
	artifacts := []Artifact{}
	var sizes map[string]int64 // computed once, when a record needs it
	for _, rec := range recs {
		if kind, _ := rec["record_kind"].(string); kind != qlode.RecordKindArtifactEvent {
			continue
		}
		a := artifactOf(rec)
		if a.Storage == ArtifactStorageChunks && a.SizeBytes == 0 {
			if sizes == nil {
				sizes = chunkedSizes(recs)
			}
			a.SizeBytes = sizes[a.ArtifactID]
		}
		artifacts = append(artifacts, a)
	}
	slices.SortStableFunc(artifacts, func(a, b Artifact) int { return int(a.Seq - b.Seq) })
	seen := map[string]bool{}
	return slices.DeleteFunc(artifacts, func(a Artifact) bool {
		if seen[a.ArtifactID] {
			return true
		}
		seen[a.ArtifactID] = true
		return false
	})
}

// OpenArtifact returns the artifact as its commit record describes it and a
// reader over its bytes, from the storage the record names. It returns
// ErrArtifactNotFound when the run has no commit record for artifactID and
// ErrArtifactIncomplete when the stored bytes do not cover the artifact.
func (r *Reader) OpenArtifact(ctx context.Context, run Run, artifactID string) (Artifact, ArtifactReader, error) {
	cfg, err := r.validateRun(run)
	if err != nil {
		return Artifact{}, nil, err
	}
	a, recs, err := r.commitOf(ctx, run, cfg, artifactID)
	if err != nil {
		return Artifact{}, nil, err
	}
	if a.SizeBytes < 0 {
		return Artifact{}, nil, fmt.Errorf("%w: artifact %q has size %d", ErrArtifactIncomplete, a.ArtifactID, a.SizeBytes)
	}
	var rd ArtifactReader
	switch a.Storage {
	case ArtifactStorageEmpty:
		if a.SizeBytes != 0 {
			return Artifact{}, nil, fmt.Errorf("%w: artifact %q is empty with size %d", ErrArtifactIncomplete, a.ArtifactID, a.SizeBytes)
		}
		rd = bytes.NewReader(nil)
	case ArtifactStorageVolume:
		rd, err = r.openVolume(ctx, cfg, a)
	case ArtifactStorageChunks:
		rd, err = chunkReader(recs, a)
	default:
		err = fmt.Errorf("artifact %q: unknown storage %q", a.ArtifactID, a.Storage)
	}
	if err != nil {
		return Artifact{}, nil, err
	}
	return a, rd, nil
}

// commitOf finds the artifact's commit record: first through the run's
// commit index (CONTRACT_LODE.md §Commit Index), which names the snapshot
// that holds the record, and otherwise by reading the run's artifact
// partition. It returns the partition's records too when it read them,
// since a chunk-stored artifact is assembled from them.
func (r *Reader) commitOf(ctx context.Context, run Run, cfg qlode.Config, artifactID string) (Artifact, []lode.D, error) {
	a, why, err := r.indexedCommit(ctx, run, cfg, artifactID)
	if err != nil || why == "" {
		return a, nil, err
	}
	if r.onFallback != nil {
		r.onFallback(artifactID, why)
	}
	recs, err := r.artifactRecords(ctx, run)
	if err != nil {
		return Artifact{}, nil, err
	}
	artifacts := artifactsOf(recs)
	i := slices.IndexFunc(artifacts, func(a Artifact) bool { return a.ArtifactID == artifactID })
	if i < 0 {
		return Artifact{}, nil, fmt.Errorf("%w: %q in run %q", ErrArtifactNotFound, artifactID, run.RunID)
	}
	return artifacts[i], recs, nil
}

// indexedCommit reads the artifact's commit record from the snapshot that
// its commit index entry names. It reports found false, so that the caller
// reads the partition, when there is no entry, or the snapshot is gone or
// holds no record of the artifact, or the record is of an artifact whose
// bytes are chunk records, which the index never names. The entry is never
// trusted: every fact comes from the record.
func (r *Reader) indexedCommit(ctx context.Context, run Run, cfg qlode.Config, artifactID string) (Artifact, IndexFallback, error) {
	snapshot, found, err := qlode.ReadCommitIndex(ctx, r.store, cfg, artifactID)
	if err != nil {
		return Artifact{}, "", err
	}
	if !found {
		return Artifact{}, FallbackNoEntry, nil
	}
	partition, err := lode.ParsePartitionPath(run.Partition + artifactPartition)
	if err != nil {
		return Artifact{}, "", fmt.Errorf("%w: %w", ErrInvalidRun, err)
	}
	dr, err := qlode.OpenView(ctx, r.dataset, r.store)
	if errors.Is(err, lode.ErrNotFound) {
		return Artifact{}, FallbackSnapshotGone, nil
	}
	if err != nil {
		return Artifact{}, "", qlode.WrapReadError(err, "datasets/"+r.dataset)
	}
	recs, err := qlode.ReadSnapshotPartitionRecords(ctx, dr, snapshot, partition)
	if errors.Is(err, lode.ErrNotFound) || errors.Is(err, lode.ErrSnapshotExpired) || errors.Is(err, lode.ErrInvalidPath) {
		return Artifact{}, FallbackSnapshotGone, nil
	}
	if err != nil {
		return Artifact{}, "", err
	}
	for _, a := range artifactsOf(recs) {
		switch {
		case a.ArtifactID != artifactID:
		case a.Storage == ArtifactStorageChunks:
			return Artifact{}, FallbackChunkStorage, nil
		default:
			return a, "", nil
		}
	}
	return Artifact{}, FallbackNoRecord, nil
}

// IndexFallback is why OpenArtifact read the run's artifact partition
// instead of the snapshot a commit index entry names.
type IndexFallback string

const (
	// FallbackNoEntry: the run has no usable entry for the artifact, as a
	// run written before the index has none, or the entry does not decode.
	FallbackNoEntry IndexFallback = "no usable commit index entry"
	// FallbackSnapshotGone: the snapshot the entry names has no manifest in
	// the artifact partition, as after retention expires it.
	FallbackSnapshotGone IndexFallback = "the entry's snapshot is gone"
	// FallbackNoRecord: the snapshot holds no commit record of the artifact.
	FallbackNoRecord IndexFallback = "the entry's snapshot holds no record of the artifact"
	// FallbackChunkStorage: the record names chunk storage, whose chunk
	// records can lie in other snapshots.
	FallbackChunkStorage IndexFallback = "the artifact is stored as chunk records"
)

// artifactOf reads a commit record. A record without a storage field was
// written before Volumes, so its bytes are chunk records.
func artifactOf(rec lode.D) Artifact {
	a := Artifact{
		ArtifactID:       stringOf(rec["artifact_id"]),
		Name:             stringOf(rec["name"]),
		ContentType:      stringOf(rec["content_type"]),
		SizeBytes:        intOf(rec["size_bytes"]),
		Seq:              intOf(rec["seq"]),
		Storage:          ArtifactStorageChunks,
		VolumeSnapshotID: stringOf(rec["volume_snapshot_id"]),
		SHA256:           stringOf(rec["sha256"]),
	}
	if s := stringOf(rec["storage"]); s != "" {
		a.Storage = ArtifactStorage(s)
	}
	return a
}

// openVolume opens the artifact's Volume under the run's artifact directory
// and checks that the named snapshot covers [0, SizeBytes) before any byte
// is read.
func (r *Reader) openVolume(ctx context.Context, cfg qlode.Config, a Artifact) (ArtifactReader, error) {
	if a.SizeBytes <= 0 || a.VolumeSnapshotID == "" {
		return nil, fmt.Errorf("%w: artifact %q names volume snapshot %q with size %d", ErrArtifactIncomplete, a.ArtifactID, a.VolumeSnapshotID, a.SizeBytes)
	}
	dir := qlode.RunArtifactDir(cfg)
	vol, err := volume.New(volume.ID(a.ArtifactID), lode.Prefixed(lode.Use(r.store), dir), a.SizeBytes)
	if err != nil {
		return nil, qlode.WrapReadError(err, dir)
	}
	snapshotID := volume.SnapshotID(a.VolumeSnapshotID)
	snap, err := vol.Snapshot(ctx, snapshotID)
	if err != nil {
		if errors.Is(err, lode.ErrNotFound) || errors.Is(err, volume.ErrRemoved) {
			return nil, fmt.Errorf("%w: artifact %q volume snapshot %q: %w", ErrArtifactIncomplete, a.ArtifactID, a.VolumeSnapshotID, err)
		}
		return nil, qlode.WrapReadError(err, dir+"volumes/"+a.ArtifactID)
	}
	if covered := coveredPrefix(snap.Manifest.Blocks); covered < a.SizeBytes {
		return nil, fmt.Errorf("%w: artifact %q volume snapshot %q covers %d of %d bytes", ErrArtifactIncomplete, a.ArtifactID, a.VolumeSnapshotID, covered, a.SizeBytes)
	}
	ra := &volumeReaderAt{ctx: ctx, vol: vol, snapshot: snapshotID, size: a.SizeBytes, artifactID: a.ArtifactID}
	return io.NewSectionReader(ra, 0, a.SizeBytes), nil
}

// coveredPrefix is the length of the contiguous prefix [0, n) that blocks
// cover. Lode keeps a manifest's blocks sorted by offset and non-overlapping.
func coveredPrefix(blocks []volume.BlockRef) int64 {
	var n int64
	for _, b := range blocks {
		if b.Offset > n {
			break
		}
		n = max(n, b.Offset+b.Length)
	}
	return n
}

// volumeReaderAt reads ranges of one Volume snapshot. ReaderAt carries no
// context, so it reads with the one OpenArtifact received.
type volumeReaderAt struct {
	ctx        context.Context
	vol        volume.Reader
	snapshot   volume.SnapshotID
	size       int64
	artifactID string
}

func (v *volumeReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("reader: negative offset")
	}
	if off >= v.size {
		return 0, io.EOF
	}
	n := min(int64(len(p)), v.size-off)
	if n == 0 {
		return 0, nil
	}
	data, err := v.vol.ReadAt(v.ctx, v.snapshot, off, n)
	if err != nil {
		if errors.Is(err, volume.ErrRangeMissing) {
			return 0, fmt.Errorf("%w: artifact %q range [%d, %d): %w", ErrArtifactIncomplete, v.artifactID, off, off+n, err)
		}
		return 0, qlode.WrapReadError(err, "volumes/"+v.artifactID)
	}
	copy(p, data)
	if n < int64(len(p)) {
		return int(n), io.EOF
	}
	return int(n), nil
}

// chunkReader assembles the artifact from its chunk records, which must
// cover [0, SizeBytes) without a gap. A retry under an at-least-once policy
// may persist a chunk twice at one offset, or append a second copy of the
// artifact after a complete one (CONTRACT_POLICY.md §Flush Modes), so a
// chunk that repeats bytes the assembled prefix already holds, and every
// chunk at or past SizeBytes, is ignored. A chunk that conflicts with the
// prefix is an error. So is an artifact none of whose chunks is a last chunk
// ending at SizeBytes: a release before 0.15 wrote a last chunk for every
// artifact, a zero-length one for an empty artifact, so an artifact without
// one lost chunks, and a size that disagrees with the chunks would
// otherwise cut the artifact short.
func chunkReader(recs []lode.D, a Artifact) (ArtifactReader, error) {
	type chunk struct {
		offset int64
		data   []byte
	}
	var chunks []chunk
	lastAtSize := false
	for _, rec := range recs {
		kind, _ := rec["record_kind"].(string)
		if kind != qlode.RecordKindArtifactChunk || stringOf(rec["artifact_id"]) != a.ArtifactID {
			continue
		}
		data, err := chunkData(rec["data"])
		if err != nil {
			return nil, fmt.Errorf("artifact %q chunk at offset %d: %w", a.ArtifactID, intOf(rec["offset"]), err)
		}
		offset := intOf(rec["offset"])
		chunks = append(chunks, chunk{offset: offset, data: data})
		if rec["is_last"] == true && offset+int64(len(data)) == a.SizeBytes {
			lastAtSize = true
		}
	}
	if !lastAtSize {
		return nil, fmt.Errorf("%w: artifact %q has no last chunk ending at its size %d", ErrArtifactIncomplete, a.ArtifactID, a.SizeBytes)
	}
	slices.SortStableFunc(chunks, func(x, y chunk) int { return int(x.offset - y.offset) })
	buf := make([]byte, 0, a.SizeBytes)
	for _, c := range chunks {
		end := c.offset + int64(len(c.data))
		if c.offset >= a.SizeBytes {
			continue // a second copy past the artifact
		}
		if c.offset >= 0 && end <= int64(len(buf)) {
			if !bytes.Equal(buf[c.offset:end], c.data) {
				return nil, fmt.Errorf("%w: artifact %q has conflicting chunks at offset %d", ErrArtifactIncomplete, a.ArtifactID, c.offset)
			}
			continue // a retried chunk
		}
		if c.offset != int64(len(buf)) {
			return nil, fmt.Errorf("%w: artifact %q has a chunk at offset %d, want %d", ErrArtifactIncomplete, a.ArtifactID, c.offset, len(buf))
		}
		buf = append(buf, c.data...)
	}
	if int64(len(buf)) != a.SizeBytes {
		return nil, fmt.Errorf("%w: artifact %q has %d bytes of chunks, want %d", ErrArtifactIncomplete, a.ArtifactID, len(buf), a.SizeBytes)
	}
	return bytes.NewReader(buf), nil
}

// chunkedSizes returns the size of each chunk-stored artifact among recs:
// where its first last chunk ends. Every chunk record states is_last
// (CONTRACT_LODE.md §Chunk Record Schema). A retry may append a second copy
// after a complete one, whose last chunk ends further on, so the earliest
// end stands. An artifact with no last chunk has no entry.
func chunkedSizes(recs []lode.D) map[string]int64 {
	sizes := map[string]int64{}
	for _, rec := range recs {
		if kind, _ := rec["record_kind"].(string); kind != qlode.RecordKindArtifactChunk || rec["is_last"] != true {
			continue
		}
		id, end := stringOf(rec["artifact_id"]), intOf(rec["offset"])+intOf(rec["length"])
		if size, ok := sizes[id]; !ok || end < size {
			sizes[id] = end
		}
	}
	return sizes
}

// chunkData decodes a chunk record's data field, which JSON carries as
// base64.
func chunkData(v any) ([]byte, error) {
	switch d := v.(type) {
	case []byte:
		return d, nil
	case string:
		return base64.StdEncoding.DecodeString(d)
	}
	return nil, fmt.Errorf("data is %T, want base64", v)
}

func stringOf(v any) string {
	s, _ := v.(string)
	return s
}

func intOf(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	}
	return 0
}
