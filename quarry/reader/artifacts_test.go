package reader

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"maps"
	"testing"

	"github.com/pithecene/lode/lode"
	"github.com/pithecene/lode/lode/volume"

	qlode "github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/types"
)

const (
	testArtifactType = "application/octet-stream"
	testArtifactTs   = "2026-10-21T01:00:00Z"
)

// writeChunked writes the run with one artifact stored as chunk records of
// at most chunkSize bytes and committed by an artifact event, as the runtime
// wrote artifacts before Volumes. sizeBytes is what the commit event claims.
func (f *fixture) writeChunked(spec runSpec, artifactID string, data []byte, chunkSize int, sizeBytes int64) {
	f.t.Helper()
	var chunks []chunkRecord
	for off := 0; off < len(data); off += chunkSize {
		end := min(off+chunkSize, len(data))
		chunks = append(chunks, chunkRecord{artifactID: artifactID, seq: int64(len(chunks) + 1), isLast: end == len(data), data: data[off:end]})
	}
	commit := &types.EventEnvelope{
		ContractVersion: types.ContractVersion, EventID: "a-" + artifactID, RunID: spec.runID, Seq: int64(len(spec.events) + 1),
		Type: types.EventTypeArtifact, Ts: testArtifactTs, Attempt: 1,
		Payload: map[string]any{"artifact_id": artifactID, "name": artifactID + ".bin", "content_type": testArtifactType, "size_bytes": float64(sizeBytes)},
	}
	f.writeRun(spec, chunks, commit)
}

// writeVolume writes the run and, beside it, one artifact stored as a Volume
// under the run's artifact directory: the first commitUpTo bytes of data in
// one committed snapshot, and a commit record that names the snapshot, as
// the runtime will write it once it writes Volumes.
func (f *fixture) writeVolume(spec runSpec, artifactID string, data []byte, commitUpTo int) volume.SnapshotID {
	f.t.Helper()
	f.write(spec)
	ctx := f.t.Context()
	cfg := f.config(spec)
	vol, err := volume.New(volume.ID(artifactID), lode.Prefixed(f.factory, qlode.RunArtifactDir(cfg)), int64(len(data)))
	if err != nil {
		f.t.Fatalf("volume.New: %v", err)
	}
	blk, err := vol.StageWriteAt(ctx, 0, bytes.NewReader(data[:commitUpTo]))
	if err != nil {
		f.t.Fatalf("StageWriteAt: %v", err)
	}
	snap, err := vol.Commit(ctx, []volume.BlockRef{blk}, nil)
	if err != nil {
		f.t.Fatalf("Commit: %v", err)
	}
	sum := sha256.Sum256(data)
	f.writeCommitRecord(cfg, artifactID, int64(len(spec.events)+1), lode.D{
		"size_bytes": int64(len(data)), "storage": "volume", "volume_snapshot_id": string(snap.ID), "sha256": hex.EncodeToString(sum[:]),
	})
	return snap.ID
}

// writeCommitRecord writes an artifact commit record into the run's artifact
// partition with the fields given over the record's defaults.
func (f *fixture) writeCommitRecord(cfg qlode.Config, artifactID string, seq int64, fields lode.D) {
	f.t.Helper()
	ctx := f.t.Context()
	rec := lode.D{
		"record_kind": qlode.RecordKindArtifactEvent, "artifact_id": artifactID, "name": artifactID + ".bin", "content_type": testArtifactType,
		"contract_version": types.ContractVersion, "event_id": "a-" + artifactID, "run_id": cfg.RunID, "seq": seq,
		"event_type": string(types.EventTypeArtifact), "ts": testArtifactTs, "attempt": 1,
		"source": cfg.Source, "category": cfg.Category, "day": cfg.Day,
	}
	maps.Copy(rec, fields)
	ds, err := qlode.OpenDataset(ctx, cfg.Dataset, f.store)
	if err != nil {
		f.t.Fatalf("OpenDataset: %v", err)
	}
	if _, err := ds.Write(ctx, []lode.D{rec}, nil); err != nil {
		f.t.Fatalf("Write: %v", err)
	}
}

// writeChunkRecord writes one chunk record into the run's artifact
// partition, as a retried write under an at-least-once policy persists a
// chunk a second time.
func (f *fixture) writeChunkRecord(cfg qlode.Config, artifactID string, offset int64, data []byte) {
	f.t.Helper()
	ctx := f.t.Context()
	rec := lode.D{
		"record_kind": qlode.RecordKindArtifactChunk, "artifact_id": artifactID, "seq": 1, "is_last": false,
		"offset": offset, "length": len(data), "data": data, "event_type": string(types.EventTypeArtifact),
		"source": cfg.Source, "category": cfg.Category, "day": cfg.Day, "run_id": cfg.RunID,
	}
	ds, err := qlode.OpenDataset(ctx, cfg.Dataset, f.store)
	if err != nil {
		f.t.Fatalf("OpenDataset: %v", err)
	}
	if _, err := ds.Write(ctx, []lode.D{rec}, nil); err != nil {
		f.t.Fatalf("Write: %v", err)
	}
}

func (f *fixture) config(spec runSpec) qlode.Config {
	return qlode.Config{Dataset: "quarry", Source: spec.source, Category: spec.category, Day: spec.day, RunID: spec.runID}
}

// find returns the run as the reader lists it.
func (f *fixture) find(r *Reader, spec runSpec) Run {
	f.t.Helper()
	run, err := r.FindRun(f.t.Context(), Selector{Source: spec.source, Category: spec.category, Day: spec.day}, spec.runID)
	if err != nil {
		f.t.Fatalf("FindRun: %v", err)
	}
	return run
}

func spec(runID string) runSpec {
	return runSpec{"src", "cat", "2026-10-21", runID, []types.EventType{types.EventTypeItem}, nil}
}

// assertReads checks the reader against data as a stream, by range, and
// after a seek.
func assertReads(t *testing.T, rd ArtifactReader, data []byte) {
	t.Helper()
	got, err := io.ReadAll(rd)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("ReadAll = %q, %v; want %q", got, err, data)
	}
	if len(data) < 10 {
		return
	}
	buf := make([]byte, 4)
	if n, err := rd.ReadAt(buf, 5); err != nil || n != 4 || !bytes.Equal(buf, data[5:9]) {
		t.Fatalf("ReadAt(5) = %q, %d, %v; want %q", buf, n, err, data[5:9])
	}
	tail := make([]byte, 8)
	if n, err := rd.ReadAt(tail, int64(len(data))-3); !errors.Is(err, io.EOF) || n != 3 || !bytes.Equal(tail[:n], data[len(data)-3:]) {
		t.Fatalf("ReadAt(end-3) = %q, %d, %v; want the last 3 bytes and io.EOF", tail[:n], n, err)
	}
	if _, err := rd.Seek(10, io.SeekStart); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	if rest, err := io.ReadAll(rd); err != nil || !bytes.Equal(rest, data[10:]) {
		t.Fatalf("ReadAll after Seek(10) = %q, %v; want %q", rest, err, data[10:])
	}
}

func TestArtifacts_ChunksReadBack(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	data := []byte("0123456789abcdef")
	f.writeChunked(spec("r1"), "a1", data, 5, int64(len(data)))
	r := f.reader()
	run := f.find(r, spec("r1"))

	arts, err := r.ListArtifacts(t.Context(), run)
	if err != nil {
		t.Fatalf("ListArtifacts: %v", err)
	}
	want := Artifact{ArtifactID: "a1", Name: "a1.bin", ContentType: testArtifactType, SizeBytes: 16, Seq: 2, Storage: ArtifactStorageChunks}
	if len(arts) != 1 || arts[0] != want {
		t.Fatalf("ListArtifacts = %+v; want [%+v]", arts, want)
	}

	a, rd, err := r.OpenArtifact(t.Context(), run, "a1")
	if err != nil {
		t.Fatalf("OpenArtifact: %v", err)
	}
	if a != want {
		t.Fatalf("OpenArtifact = %+v; want %+v", a, want)
	}
	assertReads(t, rd, data)
}

// A commit record that Quarry 0.14 wrote states a size of 0 whatever the
// artifact's size. The reader takes the size from where the last chunk
// ends, including when a retry appended a second copy of the artifact,
// and reads the whole artifact back rather than none of it.
func TestArtifacts_ChunksWithZeroStatedSizeReadBack(t *testing.T) {
	t.Parallel()
	data := []byte("0123456789abcdef")
	for _, tc := range []struct {
		name   string
		chunks []chunkRecord
		want   []byte
	}{
		{"one copy", []chunkRecord{{artifactID: "a1", seq: 1, data: data[:10]}, {artifactID: "a1", seq: 2, isLast: true, data: data[10:]}}, data},
		{"a second copy after a complete one", []chunkRecord{
			{artifactID: "a1", seq: 1, data: data[:10]}, {artifactID: "a1", seq: 2, isLast: true, data: data[10:]},
			{artifactID: "a1", seq: 1, data: data[:10]}, {artifactID: "a1", seq: 2, isLast: true, data: data[10:]},
		}, data},
		// An empty artifact was written as one zero-length last chunk.
		{"an empty artifact", []chunkRecord{{artifactID: "a1", seq: 1, isLast: true, data: []byte{}}}, []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			commit := &types.EventEnvelope{
				ContractVersion: types.ContractVersion, EventID: "a-a1", RunID: "r1", Seq: 2, Type: types.EventTypeArtifact, Ts: testArtifactTs, Attempt: 1,
				Payload: map[string]any{"artifact_id": "a1", "name": "a1.bin", "content_type": testArtifactType, "size_bytes": float64(0)},
			}
			f.writeRun(spec("r1"), tc.chunks, commit)
			r := f.reader()
			run := f.find(r, spec("r1"))

			arts, err := r.ListArtifacts(t.Context(), run)
			if err != nil || len(arts) != 1 || arts[0].SizeBytes != int64(len(tc.want)) {
				t.Fatalf("ListArtifacts = %+v, %v; want one artifact of %d bytes", arts, err, len(tc.want))
			}
			a, rd, err := r.OpenArtifact(t.Context(), run, "a1")
			if err != nil || a.SizeBytes != int64(len(tc.want)) {
				t.Fatalf("OpenArtifact = %+v, %v; want %d bytes", a, err, len(tc.want))
			}
			assertReads(t, rd, tc.want)
		})
	}
}

// A retry under an at-least-once policy persists a chunk twice at one
// offset, or a second copy of the artifact after a complete one, and a
// commit record twice. The reader returns the artifact once.
func TestArtifacts_RetriedWritesReadBackOnce(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	data := []byte("0123456789abcdef")
	run := spec("r1")
	chunks := []chunkRecord{
		{artifactID: "a1", seq: 1, data: data[:10]}, {artifactID: "a1", seq: 2, isLast: true, data: data[10:]},
		{artifactID: "a1", seq: 1, data: data[:10]}, {artifactID: "a1", seq: 2, isLast: true, data: data[10:]},
	}
	commit := &types.EventEnvelope{
		ContractVersion: types.ContractVersion, EventID: "a-a1", RunID: run.runID, Seq: 2, Type: types.EventTypeArtifact, Ts: testArtifactTs, Attempt: 1,
		Payload: map[string]any{"artifact_id": "a1", "name": "a1.bin", "content_type": testArtifactType, "size_bytes": float64(len(data))},
	}
	f.writeRun(run, chunks, commit)
	f.writeChunkRecord(f.config(run), "a1", 0, data[:10])
	f.writeCommitRecord(f.config(run), "a1", 3, lode.D{"size_bytes": int64(len(data))})

	r := f.reader()
	arts, err := r.ListArtifacts(t.Context(), f.find(r, run))
	if err != nil || len(arts) != 1 || arts[0].Seq != 2 {
		t.Fatalf("ListArtifacts = %+v, %v; want the one commit with seq 2", arts, err)
	}
	_, rd, err := r.OpenArtifact(t.Context(), f.find(r, run), "a1")
	if err != nil {
		t.Fatalf("OpenArtifact: %v", err)
	}
	assertReads(t, rd, data)
}

func TestArtifacts_VolumeReadBack(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	data := []byte("the quick brown fox jumps")
	snap := f.writeVolume(spec("r1"), "a1", data, len(data))
	r := f.reader()
	run := f.find(r, spec("r1"))

	sum := sha256.Sum256(data)
	want := Artifact{
		ArtifactID: "a1", Name: "a1.bin", ContentType: testArtifactType, SizeBytes: int64(len(data)), Seq: 2,
		Storage: ArtifactStorageVolume, VolumeSnapshotID: string(snap), SHA256: hex.EncodeToString(sum[:]),
	}
	arts, err := r.ListArtifacts(t.Context(), run)
	if err != nil || len(arts) != 1 || arts[0] != want {
		t.Fatalf("ListArtifacts = %+v, %v; want [%+v]", arts, err, want)
	}
	a, rd, err := r.OpenArtifact(t.Context(), run, "a1")
	if err != nil {
		t.Fatalf("OpenArtifact: %v", err)
	}
	if a != want {
		t.Fatalf("OpenArtifact = %+v; want %+v", a, want)
	}
	assertReads(t, rd, data)
}

func TestArtifacts_EmptyHasNoVolume(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.write(spec("r1"))
	f.writeCommitRecord(f.config(spec("r1")), "e1", 2, lode.D{"size_bytes": 0, "storage": "empty"})
	r := f.reader()
	a, rd, err := r.OpenArtifact(t.Context(), f.find(r, spec("r1")), "e1")
	if err != nil {
		t.Fatalf("OpenArtifact: %v", err)
	}
	if a.Storage != ArtifactStorageEmpty || a.SizeBytes != 0 {
		t.Fatalf("artifact = %+v; want empty storage and size 0", a)
	}
	if got, err := io.ReadAll(rd); err != nil || len(got) != 0 {
		t.Fatalf("ReadAll = %q, %v; want no bytes", got, err)
	}
}

// ListArtifacts orders by seq and includes only commit records; the run's
// a1 chunk, which no event committed, is not an artifact.
func TestListArtifacts_OrderedCommitsOnly(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.writeVolume(spec("r1"), "v9", []byte("later"), 5)
	f.writeCommitRecord(f.config(spec("r1")), "v3", 3, lode.D{"size_bytes": 0, "storage": "empty"})
	r := f.reader()
	arts, err := r.ListArtifacts(t.Context(), f.find(r, spec("r1")))
	if err != nil {
		t.Fatalf("ListArtifacts: %v", err)
	}
	if len(arts) != 2 || arts[0].ArtifactID != "v9" || arts[1].ArtifactID != "v3" {
		t.Fatalf("ListArtifacts = %+v; want v9 (seq 2) then v3 (seq 3)", arts)
	}
}

// r1 and r10 share a raw prefix and an artifact ID; each run reads its own
// Volume.
func TestOpenArtifact_NoPrefixBleed(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.writeVolume(spec("r1"), "a1", []byte("run one bytes"), 13)
	f.writeVolume(spec("r10"), "a1", []byte("run ten bytes!"), 14)
	r := f.reader()
	for runID, want := range map[string]string{"r1": "run one bytes", "r10": "run ten bytes!"} {
		_, rd, err := r.OpenArtifact(t.Context(), f.find(r, spec(runID)), "a1")
		if err != nil {
			t.Fatalf("OpenArtifact(%s): %v", runID, err)
		}
		if got, err := io.ReadAll(rd); err != nil || string(got) != want {
			t.Fatalf("run %s read %q, %v; want %q", runID, got, err, want)
		}
	}
}

func TestOpenArtifact_Errors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		setup      func(f *fixture)
		artifactID string
		want       error
	}{
		{"no commit record", func(f *fixture) { f.write(spec("r1")) }, "a1", ErrArtifactNotFound},
		{"chunks shorter than size_bytes", func(f *fixture) { f.writeChunked(spec("r1"), "a1", []byte("12345"), 5, 9) }, "a1", ErrArtifactIncomplete},
		{"chunks longer than size_bytes", func(f *fixture) { f.writeChunked(spec("r1"), "a1", []byte("12345"), 5, 3) }, "a1", ErrArtifactIncomplete},
		{"size_bytes ends inside the chunks", func(f *fixture) { f.writeChunked(spec("r1"), "a1", []byte("0123456789"), 5, 5) }, "a1", ErrArtifactIncomplete},
		{"a commit without chunks", func(f *fixture) {
			f.write(spec("r1"))
			f.writeCommitRecord(f.config(spec("r1")), "a2", 2, lode.D{"size_bytes": 0})
		}, "a2", ErrArtifactIncomplete},
		{"no last chunk", func(f *fixture) {
			commit := &types.EventEnvelope{
				ContractVersion: types.ContractVersion, EventID: "a-a1", RunID: "r1", Seq: 2, Type: types.EventTypeArtifact, Ts: testArtifactTs, Attempt: 1,
				Payload: map[string]any{"artifact_id": "a1", "name": "a1.bin", "content_type": testArtifactType, "size_bytes": float64(0)},
			}
			f.writeRun(spec("r1"), []chunkRecord{{artifactID: "a1", seq: 1, data: []byte("12345")}}, commit)
		}, "a1", ErrArtifactIncomplete},
		{"volume covers a prefix only", func(f *fixture) { f.writeVolume(spec("r1"), "a1", []byte("twenty bytes of data"), 10) }, "a1", ErrArtifactIncomplete},
		{"volume snapshot missing", func(f *fixture) {
			f.write(spec("r1"))
			f.writeCommitRecord(f.config(spec("r1")), "a1", 2, lode.D{"size_bytes": 4, "storage": "volume", "volume_snapshot_id": "gone"})
		}, "a1", ErrArtifactIncomplete},
		{"chunk conflicts with the assembled bytes", func(f *fixture) {
			f.writeChunked(spec("r1"), "a1", []byte("0123456789"), 5, 10)
			f.writeChunkRecord(f.config(spec("r1")), "a1", 2, []byte("xx"))
		}, "a1", ErrArtifactIncomplete},
		{"negative size", func(f *fixture) {
			f.write(spec("r1"))
			f.writeCommitRecord(f.config(spec("r1")), "a1", 2, lode.D{"size_bytes": -1})
		}, "a1", ErrArtifactIncomplete},
		{"empty with a size", func(f *fixture) {
			f.write(spec("r1"))
			f.writeCommitRecord(f.config(spec("r1")), "a1", 2, lode.D{"size_bytes": 4, "storage": "empty"})
		}, "a1", ErrArtifactIncomplete},
		{"volume record without a snapshot", func(f *fixture) {
			f.write(spec("r1"))
			f.writeCommitRecord(f.config(spec("r1")), "a1", 2, lode.D{"size_bytes": 4, "storage": "volume"})
		}, "a1", ErrArtifactIncomplete},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			tc.setup(f)
			r := f.reader()
			_, _, err := r.OpenArtifact(t.Context(), f.find(r, spec("r1")), tc.artifactID)
			if !errors.Is(err, tc.want) {
				t.Fatalf("OpenArtifact = %v; want %v", err, tc.want)
			}
		})
	}
}

func TestArtifacts_RejectForeignRun(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.write(spec("r1"))
	r := f.reader()
	run := f.find(r, spec("r1"))
	run.Partition = "source=src/category=cat/day=2026-10-21/"
	if _, err := r.ListArtifacts(t.Context(), run); !errors.Is(err, ErrInvalidRun) {
		t.Fatalf("ListArtifacts = %v; want ErrInvalidRun", err)
	}
	if _, _, err := r.OpenArtifact(t.Context(), run, "a1"); !errors.Is(err, ErrInvalidRun) {
		t.Fatalf("OpenArtifact = %v; want ErrInvalidRun", err)
	}
}
