package lode_test

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/pithecene/lode/lode"

	qlode "github.com/pithecene/quarry/lode"
)

// getCountingStore counts the Get requests for each key.
type getCountingStore struct {
	lode.RangeStore
	mu   sync.Mutex
	gets map[string]int
}

func (s *getCountingStore) Get(ctx context.Context, p string) (io.ReadCloser, error) {
	s.mu.Lock()
	s.gets[p]++
	s.mu.Unlock()
	return s.RangeStore.Get(ctx, p)
}

// A partition read loads each manifest once: Lode's Manifests yields the
// manifest that its listing loaded, so no key is read a second time.
func TestReadPartitionRecords_ReadsEachKeyOnce(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := &getCountingStore{RangeStore: lode.NewMemory(), gets: map[string]int{}}
	ds, err := qlode.OpenDataset(ctx, "quarry", store)
	if err != nil {
		t.Fatal(err)
	}
	const batches = 5
	for i := range batches {
		rec := lode.D{"source": "src", "category": "cat", "day": artifactTestDay, "run_id": "r1", "event_type": "item", "seq": i}
		if _, err := ds.Write(ctx, []lode.D{rec}, nil); err != nil {
			t.Fatal(err)
		}
	}
	view, err := qlode.OpenView(ctx, "quarry", store)
	if err != nil {
		t.Fatal(err)
	}
	parts, err := view.ListPartitions(ctx, lode.PartitionListOptions{})
	if err != nil || len(parts) != 1 {
		t.Fatalf("partitions %v, %v; want one", parts, err)
	}
	store.mu.Lock()
	clear(store.gets)
	store.mu.Unlock()

	recs, err := qlode.ReadPartitionRecords(ctx, view, parts[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != batches {
		t.Fatalf("read %d records, want %d", len(recs), batches)
	}
	manifests := 0
	for key, n := range store.gets {
		if n > 1 {
			t.Errorf("Get %s: %d requests, want 1", key, n)
		}
		if strings.HasSuffix(key, "manifest.json") {
			manifests++
		}
	}
	if manifests != batches {
		t.Errorf("read %d manifests, want %d", manifests, batches)
	}
}
