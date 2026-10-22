package reader

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/pithecene-io/lode/lode"

	"github.com/pithecene/quarry/types"
)

// countingStore counts the store work a reader does: List calls, keys those
// calls return (on S3, ListObjectsV2 returns at most 1,000 keys per request,
// so the keys returned bound the requests), and objects read.
type countingStore struct {
	lode.Store
	mu         sync.Mutex
	lists      int
	keysListed int
	gets       int
}

func (s *countingStore) List(ctx context.Context, prefix string) ([]string, error) {
	keys, err := s.Store.List(ctx, prefix)
	s.mu.Lock()
	s.lists++
	s.keysListed += len(keys)
	s.mu.Unlock()
	return keys, err
}

func (s *countingStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	s.mu.Lock()
	s.gets++
	s.mu.Unlock()
	return s.Store.Get(ctx, path)
}

func (s *countingStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lists, s.keysListed, s.gets = 0, 0, 0
}

// TestMeasure_ListRunsVsPartitionEnumeration records, for the v0.14 roadmap
// measurement B2/C, the store work to find runs and their states from run
// records versus from Lode partitions (what a consumer without records must
// do: enumerate the partition tree and look for each run's terminal event
// partition). Runs are written through the real write path: a few event
// batches each, so every run has several partitions and segments.
func TestMeasure_ListRunsVsPartitionEnumeration(t *testing.T) {
	t.Parallel()
	const runs, days = 200, 10
	f := newFixture(t)
	for i := range runs {
		evs := []types.EventType{types.EventTypeItem, types.EventTypeItem, types.EventTypeItem, types.EventTypeRunComplete}
		end := ended(types.OutcomeSuccess, types.EventTypeRunComplete)
		if i%4 == 0 { // a quarter never finished
			evs, end = evs[:2], nil
		}
		f.write(runSpec{"src", "cat", fmt.Sprintf("2026-10-%02d", 1+i%days), fmt.Sprintf("run-%04d", i), evs, end})
	}
	cs := &countingStore{Store: f.store}
	factory := func() (lode.Store, error) { return cs, nil }
	ctx := t.Context()

	r, err := New("quarry", factory)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Records: one page of 50 runs, and the whole listing.
	cs.reset()
	page, err := r.ListRuns(ctx, Selector{Source: "src"}, ListOptions{Limit: 50})
	if err != nil || len(page.Runs) != 50 {
		t.Fatalf("page: %d runs, err %v", len(page.Runs), err)
	}
	recPage := fmt.Sprintf("lists=%d keys=%d gets=%d", cs.lists, cs.keysListed, cs.gets)
	cs.reset()
	all, err := r.ListRuns(ctx, Selector{Source: "src"}, ListOptions{})
	if err != nil || len(all.Runs) != runs {
		t.Fatalf("all: %d runs, err %v", len(all.Runs), err)
	}
	recAll := fmt.Sprintf("lists=%d keys=%d gets=%d", cs.lists, cs.keysListed, cs.gets)

	// Partition enumeration: list the source's partition tree and derive each
	// run and whether it has a run_complete or run_error partition.
	cs.reset()
	keys, err := cs.List(ctx, "datasets/quarry/partitions/source=src/")
	if err != nil {
		t.Fatalf("list partitions: %v", err)
	}
	found := map[string]bool{}
	for _, k := range keys {
		i := strings.Index(k, "/run_id=")
		if i < 0 {
			continue
		}
		run := k[:i+len("/run_id=")+strings.Index(k[i+len("/run_id="):], "/")]
		found[run] = found[run] || strings.Contains(k, "/event_type=run_complete/") || strings.Contains(k, "/event_type=run_error/")
	}
	if len(found) != runs {
		t.Fatalf("partition enumeration found %d runs, want %d", len(found), runs)
	}
	partAll := fmt.Sprintf("lists=%d keys=%d gets=%d", cs.lists, cs.keysListed, cs.gets)

	t.Logf("%d runs over %d days; records page of 50: %s", runs, days, recPage)
	t.Logf("records, all runs with states: %s", recAll)
	t.Logf("partition enumeration, all runs with terminal presence only: %s", partAll)

	// The claim the roadmap makes: records enumerate fewer keys than the
	// partition tree, and a page reads only its own runs' records.
	if cs.keysListed <= 2*runs {
		t.Fatalf("expected the partition tree to hold more keys than the records (%d)", cs.keysListed)
	}
}
