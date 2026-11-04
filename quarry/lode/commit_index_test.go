package lode_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pithecene/lode/lode"

	"github.com/pithecene/quarry/iox"
	qlode "github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/reader"
	"github.com/pithecene/quarry/types"
)

// Tests of the commit index (CONTRACT_LODE.md §Commit Index): the runtime
// writes an entry for each committed artifact once the Dataset holds its
// commit record, and a reader opens one artifact through the entry with a
// number of store requests that does not grow with the run's artifacts.

// countingStore counts the store requests a reader makes.
type countingStore struct {
	lode.RangeStore
	mu       sync.Mutex
	requests int
	putErr   func(path string) error
}

func (s *countingStore) count() {
	s.mu.Lock()
	s.requests++
	s.mu.Unlock()
}

func (s *countingStore) Get(ctx context.Context, p string) (io.ReadCloser, error) {
	s.count()
	return s.RangeStore.Get(ctx, p)
}

func (s *countingStore) List(ctx context.Context, p string) ([]string, error) {
	s.count()
	return s.RangeStore.List(ctx, p)
}

func (s *countingStore) Exists(ctx context.Context, p string) (bool, error) {
	s.count()
	return s.RangeStore.Exists(ctx, p)
}

func (s *countingStore) ReadRange(ctx context.Context, p string, off, n int64) ([]byte, error) {
	s.count()
	return s.RangeStore.ReadRange(ctx, p, off, n)
}

func (s *countingStore) Put(ctx context.Context, p string, r io.Reader) error {
	if s.putErr != nil {
		if err := s.putErr(p); err != nil {
			return err
		}
	}
	return s.RangeStore.Put(ctx, p, r)
}

func (s *countingStore) reset() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.requests
	s.requests = 0
	return n
}

// indexRun is one run written through the real write path over a counting
// store, with a client whose index failures are recorded.
type indexRun struct {
	t        *testing.T
	store    *countingStore
	cfg      qlode.Config
	client   *qlode.LodeClient
	failures []string
	// fallbacks records each reason the reader gave for a partition read.
	fallbacks []reader.IndexFallback
}

func newIndexRun(t *testing.T, store *countingStore, runID string) *indexRun {
	t.Helper()
	run := &indexRun{t: t, store: store}
	run.cfg = qlode.Config{
		Dataset: "quarry", Source: "src", Category: "cat", Day: artifactTestDay, RunID: runID, Policy: "strict",
		IndexFailureObserver: func(id string, _ error) { run.failures = append(run.failures, id) },
	}
	client, err := qlode.NewLodeClientWithFactory(t.Context(), run.cfg, func() (lode.RangeStore, error) { return store, nil })
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	run.client = client
	day, _ := time.Parse(time.DateOnly, artifactTestDay)
	if err := client.WriteStartRecord(t.Context(), &types.RunMeta{RunID: runID, Attempt: 1}, day.Add(time.Hour)); err != nil {
		t.Fatalf("start record: %v", err)
	}
	return run
}

// storeArtifact writes the artifact's bytes to its Volume and returns the
// commit event that the runtime emits for it.
func (r *indexRun) storeArtifact(id string, data []byte, seq int64) *types.EventEnvelope {
	r.t.Helper()
	ctx := r.t.Context()
	w, err := r.client.OpenArtifact(ctx, id, int64(len(data)), "")
	if err != nil {
		r.t.Fatalf("OpenArtifact: %v", err)
	}
	if len(data) > 0 {
		if err := w.WriteRange(ctx, 0, data); err != nil {
			r.t.Fatalf("WriteRange: %v", err)
		}
	}
	storage, err := w.Finish(ctx)
	if err != nil {
		r.t.Fatalf("Finish: %v", err)
	}
	return &types.EventEnvelope{
		ContractVersion: types.ContractVersion, EventID: "a-" + id, RunID: r.cfg.RunID, Seq: seq,
		Type: types.EventTypeArtifact, Ts: "2026-10-21T01:00:00Z", Attempt: 1,
		Payload: map[string]any{"artifact_id": id, "name": id + ".bin", "content_type": "application/octet-stream", "size_bytes": float64(len(data))},
		Storage: &storage,
	}
}

// commit writes the events as one batch.
func (r *indexRun) commit(events ...*types.EventEnvelope) {
	r.t.Helper()
	if err := r.client.WriteEvents(r.t.Context(), r.cfg.Dataset, r.cfg.RunID, events); err != nil {
		r.t.Fatalf("WriteEvents: %v", err)
	}
}

// writeArtifacts commits n artifacts of a few bytes each, one batch each, as
// the strict policy writes them, and returns their bytes by ID.
func (r *indexRun) writeArtifacts(n int) map[string][]byte {
	r.t.Helper()
	data := map[string][]byte{}
	for i := range n {
		id := fmt.Sprintf("a%04d", i)
		data[id] = []byte("bytes of " + id)
		r.commit(r.storeArtifact(id, data[id], int64(i+1)))
	}
	return data
}

// open opens the artifact through a fresh reader, as `read artifact` does.
func (r *indexRun) open(id string) (reader.Artifact, []byte, error) {
	r.t.Helper()
	ctx := r.t.Context()
	observe := reader.WithIndexFallback(func(_ string, why reader.IndexFallback) { r.fallbacks = append(r.fallbacks, why) })
	rd, err := reader.New(r.cfg.Dataset, func() (lode.RangeStore, error) { return r.store, nil }, observe)
	if err != nil {
		r.t.Fatalf("reader: %v", err)
	}
	run, err := rd.FindRun(ctx, reader.Selector{Source: r.cfg.Source, Category: r.cfg.Category, Day: r.cfg.Day}, r.cfg.RunID)
	if err != nil {
		r.t.Fatalf("FindRun: %v", err)
	}
	a, ar, err := rd.OpenArtifact(ctx, run, id)
	if err != nil {
		return reader.Artifact{}, nil, err
	}
	got, err := io.ReadAll(ar)
	if err != nil {
		r.t.Fatalf("ReadAll: %v", err)
	}
	return a, got, nil
}

// openCost opens the artifact and returns the store requests of
// OpenArtifact alone, after FindRun.
func (r *indexRun) openCost(id string) int {
	r.t.Helper()
	ctx := r.t.Context()
	rd, err := reader.New(r.cfg.Dataset, func() (lode.RangeStore, error) { return r.store, nil })
	if err != nil {
		r.t.Fatalf("reader: %v", err)
	}
	run, err := rd.FindRun(ctx, reader.Selector{Source: r.cfg.Source, Category: r.cfg.Category, Day: r.cfg.Day}, r.cfg.RunID)
	if err != nil {
		r.t.Fatalf("FindRun: %v", err)
	}
	r.store.reset()
	if _, _, err := rd.OpenArtifact(ctx, run, id); err != nil {
		r.t.Fatalf("OpenArtifact: %v", err)
	}
	return r.store.reset()
}

// The runtime writes one entry per committed artifact, naming the snapshot
// that holds its commit record, and a batch's artifacts name one snapshot.
func TestCommitIndex_WrittenPerArtifact(t *testing.T) {
	t.Parallel()
	run := newIndexRun(t, &countingStore{RangeStore: lode.NewMemory()}, "r1")
	ctx := t.Context()
	run.commit(run.storeArtifact("a1", []byte("one"), 1), run.storeArtifact("a2", []byte("two"), 2))
	run.commit(run.storeArtifact("e1", nil, 3))

	snapshots := map[string]lode.DatasetSnapshotID{}
	for _, id := range []string{"a1", "a2", "e1"} {
		snap, found, err := qlode.ReadCommitIndex(ctx, run.store, run.cfg, id)
		if err != nil || !found {
			t.Fatalf("entry of %s: found %v, %v", id, found, err)
		}
		snapshots[id] = snap
	}
	if snapshots["a1"] != snapshots["a2"] || snapshots["a1"] == snapshots["e1"] {
		t.Fatalf("snapshots %v: want a1 and a2 in one batch's snapshot, e1 in another", snapshots)
	}
	if len(run.failures) != 0 {
		t.Fatalf("index failures %v", run.failures)
	}
	if keys, _ := run.store.List(ctx, qlode.CommitIndexPrefix(run.cfg)); len(keys) != 3 {
		t.Fatalf("entries %v, want 3", keys)
	}
}

// An entry that cannot be written fails nothing: the batch is committed,
// the failure is observed, and the artifact still opens, by the partition.
func TestCommitIndex_FailedEntryIsObserved(t *testing.T) {
	t.Parallel()
	failing := errors.New("store down")
	store := &countingStore{RangeStore: lode.NewMemory()}
	run := newIndexRun(t, store, "r1")
	store.putErr = func(p string) error {
		if strings.Contains(p, "/commits/") {
			return failing
		}
		return nil
	}
	run.commit(run.storeArtifact("a1", []byte("one"), 1))
	if len(run.failures) != 1 || run.failures[0] != "a1" {
		t.Fatalf("index failures %v, want [a1]", run.failures)
	}
	store.putErr = nil
	if _, got, err := run.open("a1"); err != nil || string(got) != "one" {
		t.Fatalf("open = %q, %v; want the artifact by the partition", got, err)
	}
}

// An entry that exists before the runtime writes it is left in place, and
// the write is observed as failed.
func TestCommitIndex_ExistingEntryIsKept(t *testing.T) {
	t.Parallel()
	run := newIndexRun(t, &countingStore{RangeStore: lode.NewMemory()}, "r1")
	ctx := t.Context()
	prior := []byte(`{"snapshot_id":"prior"}`)
	if err := run.store.Put(ctx, qlode.CommitIndexKey(run.cfg, "a1"), bytes.NewReader(prior)); err != nil {
		t.Fatal(err)
	}
	run.commit(run.storeArtifact("a1", []byte("one"), 1))
	if len(run.failures) != 1 {
		t.Fatalf("index failures %v, want one", run.failures)
	}
	if snap, _, _ := qlode.ReadCommitIndex(ctx, run.store, run.cfg, "a1"); snap != "prior" {
		t.Fatalf("entry names %q, want the prior entry kept", snap)
	}
	if _, got, err := run.open("a1"); err != nil || string(got) != "one" {
		t.Fatalf("open = %q, %v; want the artifact by the partition", got, err)
	}
}

// An entry is never trusted: an absent, unreadable, stale or wrong entry
// costs a partition read and gives the same answer.
func TestCommitIndex_ReaderFallsBack(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		entry func(run *indexRun, other lode.DatasetSnapshotID) []byte // nil: delete the entry
		why   reader.IndexFallback
	}{
		{"absent", func(*indexRun, lode.DatasetSnapshotID) []byte { return nil }, reader.FallbackNoEntry},
		{"not JSON", func(*indexRun, lode.DatasetSnapshotID) []byte { return []byte("{") }, reader.FallbackNoEntry},
		{"no snapshot", func(*indexRun, lode.DatasetSnapshotID) []byte { return []byte(`{}`) }, reader.FallbackNoEntry},
		{"oversized", func(*indexRun, lode.DatasetSnapshotID) []byte { return bytes.Repeat([]byte(" "), 8<<10) }, reader.FallbackNoEntry},
		{"a snapshot that is gone", func(*indexRun, lode.DatasetSnapshotID) []byte {
			return []byte(`{"snapshot_id":"20260101T000000.000000000Z-0000000000000000"}`)
		}, reader.FallbackSnapshotGone},
		{"a path, not a snapshot", func(*indexRun, lode.DatasetSnapshotID) []byte { return []byte(`{"snapshot_id":"../x"}`) }, reader.FallbackSnapshotGone},
		{"another artifact's snapshot", func(_ *indexRun, other lode.DatasetSnapshotID) []byte {
			return []byte(`{"snapshot_id":"` + string(other) + `"}`)
		}, reader.FallbackNoRecord},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			run := newIndexRun(t, &countingStore{RangeStore: lode.NewMemory()}, "r1")
			ctx := t.Context()
			run.commit(run.storeArtifact("a1", []byte("one"), 1))
			run.commit(run.storeArtifact("a2", []byte("two"), 2))
			other, _, _ := qlode.ReadCommitIndex(ctx, run.store, run.cfg, "a2")
			key := qlode.CommitIndexKey(run.cfg, "a1")
			if err := run.store.Delete(ctx, key); err != nil {
				t.Fatal(err)
			}
			if body := tc.entry(run, other); body != nil {
				if err := run.store.Put(ctx, key, bytes.NewReader(body)); err != nil {
					t.Fatal(err)
				}
			}
			a, got, err := run.open("a1")
			if err != nil || string(got) != "one" || a.ArtifactID != "a1" {
				t.Fatalf("open = %+v, %q, %v; want a1 by the partition", a, got, err)
			}
			if len(run.fallbacks) != 1 || run.fallbacks[0] != tc.why {
				t.Fatalf("fallbacks %v, want [%s]", run.fallbacks, tc.why)
			}
		})
	}
}

// An entry whose artifact's commit record is gone, as after retention,
// leads to the partition read, which finds no record.
func TestCommitIndex_NoRecordIsNotFound(t *testing.T) {
	t.Parallel()
	run := newIndexRun(t, &countingStore{RangeStore: lode.NewMemory()}, "r1")
	run.commit(run.storeArtifact("a1", []byte("one"), 1))
	if _, _, err := run.open("missing"); !errors.Is(err, reader.ErrArtifactNotFound) {
		t.Fatalf("open of an artifact never committed: %v, want ErrArtifactNotFound", err)
	}
}

// Opening one artifact through the index costs the same store requests in
// a run of 1 artifact as in a run of 200, where the partition read grows
// with the run.
func TestCommitIndex_OpenCostIndependentOfRun(t *testing.T) {
	t.Parallel()
	cost := func(n int, index bool) int {
		run := newIndexRun(t, &countingStore{RangeStore: lode.NewMemory()}, fmt.Sprintf("r%d-%v", n, index))
		run.writeArtifacts(n)
		if !index {
			keys, err := run.store.List(t.Context(), qlode.CommitIndexPrefix(run.cfg))
			if err != nil {
				t.Fatal(err)
			}
			for _, k := range keys {
				if err := run.store.Delete(t.Context(), k); err != nil {
					t.Fatal(err)
				}
			}
		}
		return run.openCost("a0000")
	}
	small, large := cost(1, true), cost(200, true)
	if small != large {
		t.Errorf("open through the index: %d requests for 1 artifact, %d for 200; want equal", small, large)
	}
	if scan := cost(200, false); scan <= 10*large {
		t.Errorf("open by the partition of 200 artifacts: %d requests, through the index %d; want the scan at least 10 times dearer", scan, large)
	}
	t.Logf("OpenArtifact requests: index %d (1 and 200 artifacts), partition read %d (200 artifacts)", large, cost(200, false))
}

// latencyStore adds a fixed delay to every request a reader makes, as a
// remote object store does.
type latencyStore struct {
	*countingStore
	delay time.Duration
}

func (s *latencyStore) Get(ctx context.Context, p string) (io.ReadCloser, error) {
	time.Sleep(s.delay)
	return s.countingStore.Get(ctx, p)
}

func (s *latencyStore) List(ctx context.Context, p string) ([]string, error) {
	time.Sleep(s.delay)
	return s.countingStore.List(ctx, p)
}

func (s *latencyStore) ReadRange(ctx context.Context, p string, off, n int64) ([]byte, error) {
	time.Sleep(s.delay)
	return s.countingStore.ReadRange(ctx, p, off, n)
}

// BenchmarkOpenArtifact opens the first artifact of a run of 1, 72 or 1,000
// artifacts over a store that adds 1 ms to each request, through the commit
// index and by the partition read, and reports the store requests of each
// open. The 72 is the artifact count of entry 15 of Panthalassa's
// dogfooding log, which #367 traces.
func BenchmarkOpenArtifact(b *testing.B) {
	for _, n := range []int{1, 72, 1000} {
		for _, index := range []bool{true, false} {
			path := "Index"
			if !index {
				path = "Partition"
			}
			b.Run(fmt.Sprintf("%s/%d", path, n), func(b *testing.B) {
				store := &countingStore{RangeStore: lode.NewMemory()}
				run := newBenchRun(b, store, n, index)
				slow := &latencyStore{countingStore: store, delay: time.Millisecond}
				rd, err := reader.New(run.Dataset, func() (lode.RangeStore, error) { return slow, nil })
				if err != nil {
					b.Fatal(err)
				}
				found, err := rd.FindRun(b.Context(), reader.Selector{Source: run.Source, Category: run.Category, Day: run.Day}, run.RunID)
				if err != nil {
					b.Fatal(err)
				}
				requests := 0
				b.ResetTimer()
				for b.Loop() {
					store.reset()
					if _, _, err := rd.OpenArtifact(b.Context(), found, "a0000"); err != nil {
						b.Fatal(err)
					}
					requests += store.reset()
				}
				b.ReportMetric(float64(requests)/float64(b.N), "requests/op")
			})
		}
	}
}

// newBenchRun writes a run of n artifacts, one batch each, and removes its
// commit index unless index is set.
func newBenchRun(b *testing.B, store *countingStore, n int, index bool) qlode.Config {
	b.Helper()
	ctx := b.Context()
	cfg := qlode.Config{Dataset: "quarry", Source: "src", Category: "cat", Day: artifactTestDay, RunID: fmt.Sprintf("r%d", n), Policy: "strict"}
	client, err := qlode.NewLodeClientWithFactory(ctx, cfg, func() (lode.RangeStore, error) { return store, nil })
	if err != nil {
		b.Fatal(err)
	}
	day, _ := time.Parse(time.DateOnly, artifactTestDay)
	if err := client.WriteStartRecord(ctx, &types.RunMeta{RunID: cfg.RunID, Attempt: 1}, day.Add(time.Hour)); err != nil {
		b.Fatal(err)
	}
	for i := range n {
		id := fmt.Sprintf("a%04d", i)
		data := []byte("bytes of " + id)
		w, err := client.OpenArtifact(ctx, id, int64(len(data)), "")
		if err != nil {
			b.Fatal(err)
		}
		if err := w.WriteRange(ctx, 0, data); err != nil {
			b.Fatal(err)
		}
		storage, err := w.Finish(ctx)
		if err != nil {
			b.Fatal(err)
		}
		ev := &types.EventEnvelope{
			ContractVersion: types.ContractVersion, EventID: "a-" + id, RunID: cfg.RunID, Seq: int64(i + 1),
			Type: types.EventTypeArtifact, Ts: "2026-10-21T01:00:00Z", Attempt: 1,
			Payload: map[string]any{"artifact_id": id, "name": id + ".bin", "content_type": "application/octet-stream", "size_bytes": float64(len(data))},
			Storage: &storage,
		}
		if err := client.WriteEvents(ctx, cfg.Dataset, cfg.RunID, []*types.EventEnvelope{ev}); err != nil {
			b.Fatal(err)
		}
	}
	if !index {
		keys, err := store.List(ctx, qlode.CommitIndexPrefix(cfg))
		if err != nil {
			b.Fatal(err)
		}
		for _, k := range keys {
			if err := store.Delete(ctx, k); err != nil {
				b.Fatal(err)
			}
		}
	}
	return cfg
}

// A read through a usable entry reports no fallback.
func TestCommitIndex_HitReportsNoFallback(t *testing.T) {
	t.Parallel()
	run := newIndexRun(t, &countingStore{RangeStore: lode.NewMemory()}, "r1")
	run.commit(run.storeArtifact("a1", []byte("one"), 1))
	if _, got, err := run.open("a1"); err != nil || string(got) != "one" {
		t.Fatalf("open = %q, %v", got, err)
	}
	if len(run.fallbacks) != 0 {
		t.Fatalf("fallbacks %v, want none", run.fallbacks)
	}
}

// An entry that names the snapshot of a chunk-stored commit record, as a
// release before 0.15 wrote, falls back: the artifact's chunk records can
// lie in other snapshots, so one snapshot cannot assemble it.
func TestCommitIndex_ChunkStorageFallsBack(t *testing.T) {
	t.Parallel()
	run := newIndexRun(t, &countingStore{RangeStore: lode.NewMemory()}, "r1")
	ctx := t.Context()
	ds, err := qlode.OpenDataset(ctx, run.cfg.Dataset, run.store)
	if err != nil {
		t.Fatal(err)
	}
	base := lode.D{"event_type": string(types.EventTypeArtifact), "source": run.cfg.Source, "category": run.cfg.Category, "day": run.cfg.Day, "run_id": run.cfg.RunID}
	chunk := lode.D{"record_kind": qlode.RecordKindArtifactChunk, "artifact_id": "a1", "seq": 1, "is_last": true, "offset": 0, "length": 3, "data": []byte("old")}
	commit := lode.D{"record_kind": qlode.RecordKindArtifactEvent, "artifact_id": "a1", "name": "a1.bin", "content_type": "application/octet-stream", "size_bytes": 3, "seq": 2, "event_id": "a-a1", "ts": "2026-10-21T01:00:00Z", "attempt": 1}
	for k, v := range base {
		chunk[k], commit[k] = v, v
	}
	if _, err := ds.Write(ctx, []lode.D{chunk}, nil); err != nil {
		t.Fatal(err)
	}
	snap, err := ds.Write(ctx, []lode.D{commit}, nil)
	if err != nil {
		t.Fatal(err)
	}
	entry := []byte(`{"snapshot_id":"` + string(snap.ID) + `"}`)
	if err := run.store.Put(ctx, qlode.CommitIndexKey(run.cfg, "a1"), bytes.NewReader(entry)); err != nil {
		t.Fatal(err)
	}
	if _, got, err := run.open("a1"); err != nil || string(got) != "old" {
		t.Fatalf("open = %q, %v; want the chunks assembled by the partition", got, err)
	}
	if len(run.fallbacks) != 1 || run.fallbacks[0] != reader.FallbackChunkStorage {
		t.Fatalf("fallbacks %v, want [%s]", run.fallbacks, reader.FallbackChunkStorage)
	}
}

// An entry whose snapshot retention expired falls back to the partition.
// A retried write left the same commit record in a later snapshot, so the
// partition read finds the artifact. The expired snapshot's partition
// manifest may remain as a residual object, and it is never served.
func TestCommitIndex_ExpiredSnapshotFallsBack(t *testing.T) {
	t.Parallel()
	run := newIndexRun(t, &countingStore{RangeStore: lode.NewMemory()}, "r1")
	ctx := t.Context()
	ev := run.storeArtifact("a1", []byte("one"), 1)
	run.commit(ev)
	expired, _, _ := qlode.ReadCommitIndex(ctx, run.store, run.cfg, "a1")
	run.commit(ev) // the retry: the entry exists, so it still names the first snapshot
	run.commit(run.storeArtifact("a2", []byte("two"), 2))
	ds, err := qlode.OpenDataset(ctx, run.cfg.Dataset, run.store)
	if err != nil {
		t.Fatal(err)
	}
	// An interrupted expiry leaves the snapshot's partition manifest and
	// data behind once its canonical manifest is gone, so the test puts them
	// back after a complete expiry.
	residual := map[string][]byte{}
	keys, err := run.store.List(ctx, "datasets/"+run.cfg.Dataset+"/partitions/")
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if strings.Contains(k, string(expired)) {
			rc, err := run.store.Get(ctx, k)
			if err != nil {
				t.Fatal(err)
			}
			b, err := io.ReadAll(rc)
			iox.DiscardClose(rc)
			if err != nil {
				t.Fatal(err)
			}
			residual[k] = b
		}
	}
	if len(residual) == 0 {
		t.Fatal("the snapshot has no partition objects to leave behind")
	}
	if _, err := ds.Expire(ctx, lode.ExpireRequest{IDs: []lode.DatasetSnapshotID{expired}}); err != nil {
		t.Fatalf("Expire: %v", err)
	}
	for k, b := range residual {
		if err := run.store.Put(ctx, k, bytes.NewReader(b)); err != nil {
			t.Fatal(err)
		}
	}
	if _, got, err := run.open("a1"); err != nil || string(got) != "one" {
		t.Fatalf("open = %q, %v; want the retried record by the partition", got, err)
	}
	if len(run.fallbacks) != 1 || run.fallbacks[0] != reader.FallbackSnapshotGone {
		t.Fatalf("fallbacks %v, want [%s]", run.fallbacks, reader.FallbackSnapshotGone)
	}
}
