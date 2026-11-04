package lode_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/lode/lode"
	"github.com/pithecene/lode/lode/volume"

	qlode "github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/reader"
	"github.com/pithecene/quarry/types"
)

const artifactTestDay = "2026-10-21"

// artifactFixture is one run's Lode client over one memory store.
type artifactFixture struct {
	t       *testing.T
	store   lode.RangeStore
	factory lode.Factory[lode.RangeStore]
	cfg     qlode.Config
	client  *qlode.LodeClient
}

func newArtifactFixture(t *testing.T, runID string) *artifactFixture {
	t.Helper()
	store := lode.NewMemory()
	factory := func() (lode.RangeStore, error) { return store, nil }
	cfg := qlode.Config{Dataset: "quarry", Source: "src", Category: "cat", Day: artifactTestDay, RunID: runID, Policy: "strict"}
	client, err := qlode.NewLodeClientWithFactory(t.Context(), cfg, factory)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	return &artifactFixture{t: t, store: store, factory: factory, cfg: cfg, client: client}
}

// commitEvent is the artifact event a script emits, with the storage result
// the runtime attaches before policy dispatch.
func (f *artifactFixture) commitEvent(artifactID string, size int64, storage types.ArtifactStorage) *types.EventEnvelope {
	return &types.EventEnvelope{
		ContractVersion: types.ContractVersion, EventID: "a-" + artifactID, RunID: f.cfg.RunID, Seq: 1,
		Type: types.EventTypeArtifact, Ts: "2026-10-21T01:00:00Z", Attempt: 1,
		Payload: map[string]any{"artifact_id": artifactID, "name": artifactID + ".bin", "content_type": "application/octet-stream", "size_bytes": float64(size)},
		Storage: &storage,
	}
}

// readBack reads the artifact through the reader package, as a consumer does.
func (f *artifactFixture) readBack(artifactID string) (reader.Artifact, []byte) {
	f.t.Helper()
	ctx := f.t.Context()
	day, _ := time.Parse(time.DateOnly, artifactTestDay)
	if err := f.client.WriteStartRecord(ctx, &types.RunMeta{RunID: f.cfg.RunID, Attempt: 1}, day.Add(time.Hour)); err != nil {
		f.t.Fatalf("start record: %v", err)
	}
	r, err := reader.New(f.cfg.Dataset, f.factory)
	if err != nil {
		f.t.Fatalf("reader: %v", err)
	}
	run, err := r.FindRun(ctx, reader.Selector{Source: f.cfg.Source, Category: f.cfg.Category, Day: f.cfg.Day}, f.cfg.RunID)
	if err != nil {
		f.t.Fatalf("FindRun: %v", err)
	}
	a, rd, err := r.OpenArtifact(ctx, run, artifactID)
	if err != nil {
		f.t.Fatalf("OpenArtifact: %v", err)
	}
	data, err := io.ReadAll(rd)
	if err != nil {
		f.t.Fatalf("ReadAll: %v", err)
	}
	return a, data
}

func hexSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func TestArtifactStore_RangesInAnyOrderReadBack(t *testing.T) {
	t.Parallel()
	data := bytes.Repeat([]byte("0123456789"), 5)
	cases := []struct {
		name    string
		offsets []int64 // ranges of 20 bytes, then the rest
	}{
		{"in order", []int64{0, 20, 40}},
		{"last first", []int64{40, 0, 20}},
		{"middle first", []int64{20, 40, 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newArtifactFixture(t, "r-"+tc.name)
			ctx := t.Context()
			w, err := f.client.OpenArtifact(ctx, "a1", int64(len(data)), "")
			if err != nil {
				t.Fatalf("OpenArtifact: %v", err)
			}
			for _, off := range tc.offsets {
				end := min(off+20, int64(len(data)))
				if err := w.WriteRange(ctx, off, data[off:end]); err != nil {
					t.Fatalf("WriteRange(%d): %v", off, err)
				}
			}
			storage, err := w.Finish(ctx)
			if err != nil {
				t.Fatalf("Finish: %v", err)
			}
			if storage.Kind != types.ArtifactStorageVolume || storage.VolumeSnapshotID == "" || storage.SHA256 != hexSHA256(data) {
				t.Fatalf("Finish = %+v; want a volume with sha256 %s", storage, hexSHA256(data))
			}
			if err := f.client.WriteEvents(ctx, f.cfg.Dataset, f.cfg.RunID, []*types.EventEnvelope{f.commitEvent("a1", int64(len(data)), storage)}); err != nil {
				t.Fatalf("WriteEvents: %v", err)
			}
			a, got := f.readBack("a1")
			if a.Storage != reader.ArtifactStorageVolume || a.VolumeSnapshotID != storage.VolumeSnapshotID || a.SHA256 != storage.SHA256 {
				t.Fatalf("record = %+v; want storage %+v", a, storage)
			}
			if !bytes.Equal(got, data) {
				t.Fatalf("read back %q; want %q", got, data)
			}
		})
	}
}

func TestArtifactStore_EmptyArtifact(t *testing.T) {
	t.Parallel()
	f := newArtifactFixture(t, "r1")
	ctx := t.Context()
	w, err := f.client.OpenArtifact(ctx, "e1", 0, hexSHA256(nil))
	if err != nil {
		t.Fatalf("OpenArtifact: %v", err)
	}
	if err := w.WriteRange(ctx, 0, []byte("x")); !errors.Is(err, qlode.ErrRangeRefused) {
		t.Fatalf("WriteRange on an empty artifact = %v; want ErrRangeRefused", err)
	}
	storage, err := w.Finish(ctx)
	if err != nil || storage.Kind != types.ArtifactStorageEmpty || storage.VolumeSnapshotID != "" || storage.SHA256 != hexSHA256(nil) {
		t.Fatalf("Finish = %+v, %v; want empty storage with the digest of no bytes", storage, err)
	}
	if err := w.Abandon(ctx); err != nil {
		t.Fatalf("Abandon of an empty artifact: %v", err)
	}
	if err := f.client.WriteEvents(ctx, f.cfg.Dataset, f.cfg.RunID, []*types.EventEnvelope{f.commitEvent("e1", 0, storage)}); err != nil {
		t.Fatalf("WriteEvents: %v", err)
	}
	if a, got := f.readBack("e1"); a.Storage != reader.ArtifactStorageEmpty || len(got) != 0 {
		t.Fatalf("read back %+v, %q; want an empty artifact", a, got)
	}
	if keys, _ := f.store.List(ctx, qlode.RunArtifactDir(f.cfg)+"volumes/"); len(keys) != 0 {
		t.Fatalf("an empty artifact wrote %v; want no Volume", keys)
	}
	// Its commit record is the only fact that it exists, so it is indexed.
	if _, found, err := qlode.ReadCommitIndex(ctx, f.store, f.cfg, "e1"); err != nil || !found {
		t.Fatalf("commit index of an empty artifact: found %v, %v; want an entry", found, err)
	}
}

func TestArtifactStore_RefusedRanges(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		offset int64
		data   []byte
	}{
		{"empty range", 5, nil},
		{"negative offset", -1, []byte("x")},
		{"past the size", 15, []byte("123456")},
		{"overlaps the range before it", 9, []byte("xx")},
		{"overlaps the range after it", 14, []byte("xx")},
		{"inside a committed range", 1, []byte("x")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newArtifactFixture(t, "r1")
			ctx := t.Context()
			w, err := f.client.OpenArtifact(ctx, "a1", 20, "")
			if err != nil {
				t.Fatalf("OpenArtifact: %v", err)
			}
			for _, off := range []int64{0, 15} {
				if err := w.WriteRange(ctx, off, []byte("0123456789")[:min(10, 20-off)]); err != nil {
					t.Fatalf("WriteRange(%d): %v", off, err)
				}
			}
			if err := w.WriteRange(ctx, tc.offset, tc.data); !errors.Is(err, qlode.ErrRangeRefused) {
				t.Fatalf("WriteRange = %v; want ErrRangeRefused", err)
			}
		})
	}
}

func TestArtifactStore_FinishErrors(t *testing.T) {
	t.Parallel()
	data := []byte("twenty bytes of data")
	cases := []struct {
		name     string
		expected string
		write    []int64 // offsets of 10-byte ranges
		want     error
	}{
		{"a gap", "", []int64{10}, qlode.ErrArtifactNotCovered},
		{"nothing written", "", nil, qlode.ErrArtifactNotCovered},
		{"digest differs", hexSHA256([]byte("other")), []int64{0, 10}, qlode.ErrArtifactDigestMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newArtifactFixture(t, "r1")
			ctx := t.Context()
			w, err := f.client.OpenArtifact(ctx, "a1", int64(len(data)), tc.expected)
			if err != nil {
				t.Fatalf("OpenArtifact: %v", err)
			}
			for _, off := range tc.write {
				if err := w.WriteRange(ctx, off, data[off:off+10]); err != nil {
					t.Fatalf("WriteRange(%d): %v", off, err)
				}
			}
			if _, err := w.Finish(ctx); !errors.Is(err, tc.want) {
				t.Fatalf("Finish = %v; want %v", err, tc.want)
			}
		})
	}
}

// A script may declare the expected digest in either case of hex.
func TestArtifactStore_ExpectedDigestInAnyCase(t *testing.T) {
	t.Parallel()
	f := newArtifactFixture(t, "r1")
	ctx := t.Context()
	data := []byte("case")
	w, err := f.client.OpenArtifact(ctx, "a1", int64(len(data)), strings.ToUpper(hexSHA256(data)))
	if err != nil {
		t.Fatalf("OpenArtifact: %v", err)
	}
	if err := w.WriteRange(ctx, 0, data); err != nil {
		t.Fatalf("WriteRange: %v", err)
	}
	if storage, err := w.Finish(ctx); err != nil || storage.SHA256 != hexSHA256(data) {
		t.Fatalf("Finish = %+v, %v; want the lowercase digest and no error", storage, err)
	}
}

func TestArtifactStore_AbandonRemovesTheVolume(t *testing.T) {
	t.Parallel()
	f := newArtifactFixture(t, "r1")
	ctx := t.Context()
	w, err := f.client.OpenArtifact(ctx, "a1", 4, "")
	if err != nil {
		t.Fatalf("OpenArtifact: %v", err)
	}
	if err := w.WriteRange(ctx, 0, []byte("ab")); err != nil {
		t.Fatalf("WriteRange: %v", err)
	}
	if err := w.Abandon(ctx); err != nil {
		t.Fatalf("Abandon: %v", err)
	}
	vol, err := volume.New("a1", lode.Prefixed(f.factory, qlode.RunArtifactDir(f.cfg)), 4)
	if err != nil {
		t.Fatalf("volume.New: %v", err)
	}
	if _, err := vol.Latest(ctx); !errors.Is(err, volume.ErrRemoved) {
		t.Fatalf("Latest after Abandon = %v; want ErrRemoved", err)
	}
	if err := w.Abandon(ctx); err != nil {
		t.Fatalf("second Abandon: %v", err)
	}
}

func TestArtifactStore_OpenRejectsBadArguments(t *testing.T) {
	t.Parallel()
	f := newArtifactFixture(t, "r1")
	cases := []struct {
		name string
		id   string
		size int64
	}{
		{"empty id", "", 1}, {"dot", ".", 1}, {"dot dot", "..", 1}, {"slash", "a/b", 1}, {"backslash", `a\b`, 1},
		{"lode reserved prefix", ".lode-tmp-a", 1}, {"lode reserved prefix, empty artifact", ".lode-tmp-a", 0}, {"negative size", "a1", -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := f.client.OpenArtifact(t.Context(), tc.id, tc.size, ""); !errors.Is(err, qlode.ErrInvalidArtifactID) {
				t.Fatalf("OpenArtifact(%q, %d) = %v; want ErrInvalidArtifactID", tc.id, tc.size, err)
			}
		})
	}
}

// swapFaultStore fails the next CompareAndSwap, which is the call that
// commits a Volume snapshot, after the range's block is staged.
type swapFaultStore struct {
	lode.RangeStore
	failNext bool
}

func (s *swapFaultStore) CompareAndSwap(ctx context.Context, path, expected, replacement string) error {
	if s.failNext {
		s.failNext = false
		return errors.New("injected: swap failed")
	}
	return s.RangeStore.CompareAndSwap(ctx, path, expected, replacement)
}

// A range whose commit fails leaves no staged block behind, so the executor
// can retry it, and the retried range commits.
func TestArtifactStore_CommitFailureLeavesTheRangeRetryable(t *testing.T) {
	t.Parallel()
	fault := &swapFaultStore{RangeStore: lode.NewMemory()}
	factory := func() (lode.RangeStore, error) { return fault, nil }
	cfg := qlode.Config{Dataset: "quarry", Source: "src", Category: "cat", Day: artifactTestDay, RunID: "r1", Policy: "strict"}
	client, err := qlode.NewLodeClientWithFactory(t.Context(), cfg, factory)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	ctx := t.Context()
	data := []byte("twenty bytes of data")
	w, err := client.OpenArtifact(ctx, "a1", int64(len(data)), "")
	if err != nil {
		t.Fatalf("OpenArtifact: %v", err)
	}
	if err := w.WriteRange(ctx, 0, data[:10]); err != nil {
		t.Fatalf("WriteRange(0): %v", err)
	}
	fault.failNext = true
	if err := w.WriteRange(ctx, 10, data[10:]); err == nil {
		t.Fatal("WriteRange(10) under the fault = nil; want the commit's error")
	}
	if err := w.WriteRange(ctx, 10, []byte("OTHER BYTES")); !errors.Is(err, qlode.ErrRangeRefused) {
		t.Fatalf("retried WriteRange(10) with other bytes = %v; want ErrRangeRefused", err)
	}
	if err := w.WriteRange(ctx, 10, data[10:]); err != nil {
		t.Fatalf("retried WriteRange(10) = %v; want the range staged and committed again", err)
	}
	storage, err := w.Finish(ctx)
	if err != nil || storage.SHA256 != hexSHA256(data) {
		t.Fatalf("Finish = %+v, %v; want the whole artifact with its digest", storage, err)
	}
}

// manifestFaultStore fails the next write of a Volume's canonical manifest,
// which a commit makes after it advanced the head pointer, so the commit is
// left pending.
type manifestFaultStore struct {
	lode.RangeStore
	failNext bool
}

func (s *manifestFaultStore) Put(ctx context.Context, path string, r io.Reader) error {
	if s.failNext && strings.HasSuffix(path, "/manifest.json") {
		s.failNext = false
		return errors.New("injected: manifest write failed")
	}
	return s.RangeStore.Put(ctx, path, r)
}

// A range whose commit fails after the head pointer advanced is retryable
// too: the retry completes the pending commit, so the range commits and
// the artifact finishes. The first range covers the commit without a
// parent, and the second range the commit over one.
func TestArtifactStore_RetryCompletesAPendingCommit(t *testing.T) {
	t.Parallel()
	data := []byte("twenty bytes of data")
	for _, failed := range []int64{0, 10} {
		t.Run(fmt.Sprintf("range at %d", failed), func(t *testing.T) {
			fault := &manifestFaultStore{RangeStore: lode.NewMemory()}
			factory := func() (lode.RangeStore, error) { return fault, nil }
			cfg := qlode.Config{Dataset: "quarry", Source: "src", Category: "cat", Day: artifactTestDay, RunID: "r1", Policy: "strict"}
			client, err := qlode.NewLodeClientWithFactory(t.Context(), cfg, factory)
			if err != nil {
				t.Fatalf("client: %v", err)
			}
			ctx := t.Context()
			w, err := client.OpenArtifact(ctx, "a1", int64(len(data)), "")
			if err != nil {
				t.Fatalf("OpenArtifact: %v", err)
			}
			for _, offset := range []int64{0, 10} {
				fault.failNext = offset == failed
				err := w.WriteRange(ctx, offset, data[offset:offset+10])
				if offset != failed {
					if err != nil {
						t.Fatalf("WriteRange(%d) = %v", offset, err)
					}
					continue
				}
				if err == nil {
					t.Fatalf("WriteRange(%d) under the fault = nil; want the commit's error", offset)
				}
				if err := w.WriteRange(ctx, offset, data[offset:offset+10]); err != nil {
					t.Fatalf("retried WriteRange(%d) = %v; want the pending commit completed", offset, err)
				}
			}
			storage, err := w.Finish(ctx)
			if err != nil || storage.SHA256 != hexSHA256(data) {
				t.Fatalf("Finish = %+v, %v; want the whole artifact with its digest", storage, err)
			}
		})
	}
}

// A commit event without a storage result still needs chunk records before
// it, as the runs of earlier releases wrote them.
func TestArtifactStore_CommitWithoutStorageIsRefused(t *testing.T) {
	t.Parallel()
	f := newArtifactFixture(t, "r1")
	ev := f.commitEvent("a1", 1, types.ArtifactStorage{})
	ev.Storage = nil
	err := f.client.WriteEvents(t.Context(), f.cfg.Dataset, f.cfg.RunID, []*types.EventEnvelope{ev})
	if !errors.Is(err, qlode.ErrCommitWithoutStorage) {
		t.Fatalf("WriteEvents = %v; want ErrCommitWithoutStorage", err)
	}
}

// A retry of a failed range with another length is refused, so no second
// block is staged beside the one the failed commit left.
func TestArtifactStore_ResizedRetryOfStagedRangeIsRefused(t *testing.T) {
	t.Parallel()
	fault := &swapFaultStore{RangeStore: lode.NewMemory()}
	factory := func() (lode.RangeStore, error) { return fault, nil }
	cfg := qlode.Config{Dataset: "quarry", Source: "src", Category: "cat", Day: artifactTestDay, RunID: "r1", Policy: "strict"}
	client, err := qlode.NewLodeClientWithFactory(t.Context(), cfg, factory)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	ctx := t.Context()
	w, err := client.OpenArtifact(ctx, "a1", 20, "")
	if err != nil {
		t.Fatalf("OpenArtifact: %v", err)
	}
	fault.failNext = true
	if err := w.WriteRange(ctx, 0, []byte("0123456789")); err == nil {
		t.Fatal("WriteRange under the fault = nil; want the commit's error")
	}
	if err := w.WriteRange(ctx, 0, []byte("01234")); !errors.Is(err, qlode.ErrRangeRefused) {
		t.Fatalf("resized retry = %v; want ErrRangeRefused", err)
	}
	keys, err := fault.List(ctx, qlode.RunArtifactDir(cfg))
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var blocks int
	for _, k := range keys {
		if strings.Contains(k, "/data/") {
			blocks++
		}
	}
	if blocks != 1 {
		t.Fatalf("staged blocks = %d (%v); want the one block of the failed commit", blocks, keys)
	}
}
