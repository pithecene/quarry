package lode_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/pithecene/lode/lode"

	"github.com/pithecene/quarry/iox"
	qlode "github.com/pithecene/quarry/lode"
)

// A later read of an inventory loads only the snapshots that committed since
// its last read, unindexed ones included, and drops the expired ones.
func TestInventory_ExtendsRatherThanRepeats(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := &getCountingStore{RangeStore: lode.NewMemory(), gets: map[string]int{}}
	ds, write := openRun1(t, "quarry", store)
	const prefix = "source=src/category=cat/day=" + artifactTestDay + "/run_id=r1/"
	first, second := write(1), write(2)
	inv := qlode.NewInventory("quarry", store, ds)
	read := func() []lode.SnapshotID {
		t.Helper()
		snaps, err := inv.RunSnapshots(ctx, prefix)
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]lode.SnapshotID, len(snaps))
		for i, s := range snaps {
			ids[i] = s.ID
		}
		return ids
	}
	if got, want := read(), slices.Sorted(slices.Values([]lode.SnapshotID{first, second})); !slices.Equal(got, want) {
		t.Fatalf("first read = %v, want %v", got, want)
	}

	// A snapshot that commits without its partition index is found, and only
	// its manifest is loaded.
	third := write(3)
	keys, err := store.List(ctx, "datasets/quarry/partitions/")
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if strings.Contains(k, "/segments/"+string(third)+"/") {
			if err := store.Delete(ctx, k); err != nil {
				t.Fatal(err)
			}
		}
	}
	store.mu.Lock()
	clear(store.gets)
	store.mu.Unlock()
	if got, want := read(), slices.Sorted(slices.Values([]lode.SnapshotID{first, second, third})); !slices.Equal(got, want) {
		t.Fatalf("second read = %v, want %v with the unindexed %s", got, want, third)
	}
	for key := range store.gets {
		if strings.HasSuffix(key, "/manifest.json") && !strings.Contains(key, string(third)) {
			t.Errorf("second read loaded the held manifest %s", key)
		}
	}

	// An expired snapshot drops out.
	if _, err := ds.Expire(ctx, lode.ExpireRequest{IDs: []lode.SnapshotID{first}}); err != nil {
		t.Fatal(err)
	}
	if got, want := read(), slices.Sorted(slices.Values([]lode.SnapshotID{second, third})); !slices.Equal(got, want) {
		t.Fatalf("read after the expiry = %v, want %v", got, want)
	}
}

// inventoryFixture writes snapshots of run r1 and returns a read of the
// run's snapshot IDs through one inventory.
func inventoryFixture(t *testing.T, store lode.RangeStore, n int) (*lode.Records[lode.D], []lode.SnapshotID, func() ([]lode.SnapshotID, error)) {
	t.Helper()
	ctx := t.Context()
	ds, err := qlode.OpenDataset(ctx, "quarry", store)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]lode.SnapshotID, n)
	for i := range n {
		rec := lode.D{"source": "src", "category": "cat", "day": artifactTestDay, "run_id": "r1", "event_type": "item", "seq": i}
		snap, err := ds.Write(ctx, []lode.D{rec}, nil)
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = snap.ID
	}
	inv := qlode.NewInventory("quarry", store, ds)
	read := func() ([]lode.SnapshotID, error) {
		snaps, err := inv.RunSnapshots(ctx, "source=src/category=cat/day="+artifactTestDay+"/run_id=r1/")
		var got []lode.SnapshotID
		for _, s := range snaps {
			got = append(got, s.ID)
		}
		return got, err
	}
	return ds, ids, read
}

// A held snapshot whose canonical manifest is gone without a tombstone is a
// legacy snapshot that its partition manifests record, so it stays; once
// expiry records its tombstone, it drops out.
func TestInventory_KeepsAPartitionOnlySnapshotUntilItExpires(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := &listCountingStore{RangeStore: lode.NewMemory()}
	ds, ids, read := inventoryFixture(t, store, 3)
	want := slices.Sorted(slices.Values(ids))
	if got, err := read(); err != nil || !slices.Equal(got, want) {
		t.Fatalf("first read = %v, %v; want %v", got, err, want)
	}
	if err := store.Delete(ctx, "datasets/quarry/segments/"+string(ids[0])+"/manifest.json"); err != nil {
		t.Fatal(err)
	}
	store.reset()
	if got, err := read(); err != nil || !slices.Equal(got, want) {
		t.Fatalf("read without the canonical manifest = %v, %v; want %v", got, err, want)
	}
	// The legacy snapshot is kept without Lode's scan of the dataset.
	if wide := store.wide(); len(wide) != 0 {
		t.Fatalf("read listed %v, want only the canonical manifests and the run's partitions", wide)
	}
	if _, err := ds.Expire(ctx, lode.ExpireRequest{IDs: []lode.SnapshotID{ids[0]}}); err != nil {
		t.Fatal(err)
	}
	want = slices.Sorted(slices.Values(ids[1:]))
	if got, err := read(); err != nil || !slices.Equal(got, want) {
		t.Fatalf("read after the expiry = %v, %v; want %v", got, err, want)
	}
}

// A held snapshot without its canonical manifest whose tombstone key exists is
// read through Lode: an empty placeholder keeps the live snapshot, and a
// malformed tombstone fails the read rather than dropping it.
func TestInventory_ReadsATombstoneThroughLode(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, tombstone string
		fails           bool
	}{
		{"an empty placeholder keeps the snapshot", "", false},
		{"a malformed tombstone fails the read", "{", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			store := lode.NewMemory()
			_, ids, read := inventoryFixture(t, store, 2)
			if _, err := read(); err != nil {
				t.Fatal(err)
			}
			if err := store.Delete(ctx, "datasets/quarry/segments/"+string(ids[0])+"/manifest.json"); err != nil {
				t.Fatal(err)
			}
			if err := store.Put(ctx, "datasets/quarry/expired/"+string(ids[0]), strings.NewReader(tc.tombstone)); err != nil {
				t.Fatal(err)
			}
			got, err := read()
			if tc.fails {
				if err == nil {
					t.Fatalf("read = %v; want the malformed tombstone's error", got)
				}
				return
			}
			if want := slices.Sorted(slices.Values(ids)); err != nil || !slices.Equal(got, want) {
				t.Fatalf("read = %v, %v; want %v", got, err, want)
			}
		})
	}
}

// listCountingStore records the prefixes that List reads.
type listCountingStore struct {
	lode.RangeStore
	mu    sync.Mutex
	lists []string
}

func (s *listCountingStore) List(ctx context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	s.lists = append(s.lists, prefix)
	s.mu.Unlock()
	return s.RangeStore.List(ctx, prefix)
}

func (s *listCountingStore) reset() {
	s.mu.Lock()
	s.lists = nil
	s.mu.Unlock()
}

// wide returns the listings other than the canonical manifest directory, the
// tombstones that Lode's partition listing filters by, and those under the
// read run's own partition prefix: a listing of the dataset.
func (s *listCountingStore) wide() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	run := "datasets/quarry/partitions/source=src/category=cat/day=" + artifactTestDay + "/run_id=r1/"
	for _, p := range s.lists {
		if p != "datasets/quarry/segments/" && p != "datasets/quarry/expired/" && !strings.HasPrefix(p, run) {
			out = append(out, p)
		}
	}
	return out
}

// A later read checks Lode's removal fence, as each Lode listing does, even
// when nothing committed since the last read.
func TestInventory_ReportsARemovedDataset(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := lode.NewMemory()
	_, _, read := inventoryFixture(t, store, 1)
	if _, err := read(); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "datasets/quarry/latest"); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "datasets/quarry/latest", strings.NewReader("lode:removed")); err != nil {
		t.Fatal(err)
	}
	if got, err := read(); !errors.Is(err, lode.ErrRemoved) {
		t.Fatalf("read after the removal fence = %v, %v; want lode.ErrRemoved", got, err)
	}
}

// Snapshots that commit with partition manifests alone after the first read,
// as a writer before canonical manifests commits them, are found by the next
// read of their run without a listing of the dataset, and a partition copy
// that an interrupted expiry left of a snapshot never held is left out.
func TestInventory_FindsPartitionOnlyCommits(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		late    int
		expired bool
	}{
		{"one late commit", 1, false},
		{"two late commits", 2, false},
		{"a residual copy of an expired snapshot", 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			gets := &getCountingStore{RangeStore: lode.NewMemory(), gets: map[string]int{}}
			store := &listCountingStore{RangeStore: gets}
			ds, ids, read := inventoryFixture(t, store, 1)
			if got, err := read(); err != nil || len(got) != 1 {
				t.Fatalf("first read = %v, %v; want one snapshot", got, err)
			}
			want := slices.Clone(ids)
			for i := range tc.late {
				rec := lode.D{"source": "src", "category": "cat", "day": artifactTestDay, "run_id": "r1", "event_type": "item", "seq": 10 + i}
				late, err := ds.Write(ctx, []lode.D{rec}, nil)
				if err != nil {
					t.Fatal(err)
				}
				if tc.expired {
					residual(t, store, ds, late.ID)
					continue
				}
				if err := store.Delete(ctx, "datasets/quarry/segments/"+string(late.ID)+"/manifest.json"); err != nil {
					t.Fatal(err)
				}
				want = append(want, late.ID)
			}
			store.reset()
			gets.mu.Lock()
			clear(gets.gets)
			gets.mu.Unlock()
			if got, err := read(); err != nil || !slices.Equal(got, slices.Sorted(slices.Values(want))) {
				t.Fatalf("read after the late commits = %v, %v; want %v", got, err, slices.Sorted(slices.Values(want)))
			}
			if wide := store.wide(); len(wide) != 0 {
				t.Fatalf("read listed %v, want no listing of the dataset", wide)
			}
			for key := range gets.gets {
				if strings.HasSuffix(key, "/manifest.json") && strings.Contains(key, string(ids[0])) {
					t.Errorf("read loaded the held manifest %s", key)
				}
			}
		})
	}
}

// residual expires a snapshot and puts back one of its partition copies, as
// an expiry that stopped after its tombstone and canonical manifest leaves it.
func residual(t *testing.T, store lode.RangeStore, ds *lode.Records[lode.D], id lode.SnapshotID) {
	t.Helper()
	ctx := t.Context()
	// Another run takes the head, which expiry never removes.
	other := lode.D{"source": "src", "category": "cat", "day": artifactTestDay, "run_id": "r2", "event_type": "item", "seq": 0}
	if _, err := ds.Write(ctx, []lode.D{other}, nil); err != nil {
		t.Fatal(err)
	}
	keys, err := store.List(ctx, "datasets/quarry/partitions/")
	if err != nil {
		t.Fatal(err)
	}
	var copyKey string
	var body []byte
	for _, k := range keys {
		if strings.Contains(k, "/segments/"+string(id)+"/manifest.json") {
			var err error
			if body, err = readAll(ctx, store, k); err != nil {
				t.Fatal(err)
			}
			copyKey = k
		}
	}
	if copyKey == "" {
		t.Fatal("no partition copy to leave behind")
	}
	if report, err := ds.Expire(ctx, lode.ExpireRequest{IDs: []lode.SnapshotID{id}}); err != nil || len(report.Expired) != 1 {
		t.Fatalf("Expire = %+v, %v; want %s expired", report, err, id)
	}
	if left, err := store.Exists(ctx, copyKey); err != nil {
		t.Fatal(err)
	} else if !left {
		if err := store.Put(ctx, copyKey, bytes.NewReader(body)); err != nil {
			t.Fatal(err)
		}
	}
}

// readAll reads one object and closes it on every path.
func readAll(ctx context.Context, store lode.RangeStore, key string) ([]byte, error) {
	rc, err := store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer iox.DiscardClose(rc)
	return io.ReadAll(rc)
}

// A dataset named "segments" finds a late partition-only commit like any
// other: the key parse cannot mistake the dataset's name for the snapshot
// directory.
func TestInventory_FindsAPartitionOnlyCommitInADatasetNamedSegments(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := lode.NewMemory()
	ds, write := openRun1(t, "segments", store)
	first := write(1)
	inv := qlode.NewInventory("segments", store, ds)
	prefix := "source=src/category=cat/day=" + artifactTestDay + "/run_id=r1/"
	if _, err := inv.RunSnapshots(ctx, prefix); err != nil {
		t.Fatal(err)
	}
	late := write(2)
	if err := store.Delete(ctx, "datasets/segments/segments/"+string(late)+"/manifest.json"); err != nil {
		t.Fatal(err)
	}
	snaps, err := inv.RunSnapshots(ctx, prefix)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]lode.SnapshotID, len(snaps))
	for i, s := range snaps {
		got[i] = s.ID
	}
	if want := slices.Sorted(slices.Values([]lode.SnapshotID{first, late})); !slices.Equal(got, want) {
		t.Fatalf("read = %v, want %v", got, want)
	}
}

// openRun1 opens dataset over store and returns it with a write of one record
// of run r1, which returns the snapshot's ID.
func openRun1(t *testing.T, dataset string, store lode.RangeStore) (*lode.Records[lode.D], func(seq int) lode.SnapshotID) {
	t.Helper()
	ds, err := qlode.OpenDataset(t.Context(), dataset, store)
	if err != nil {
		t.Fatal(err)
	}
	return ds, func(seq int) lode.SnapshotID {
		t.Helper()
		rec := lode.D{"source": "src", "category": "cat", "day": artifactTestDay, "run_id": "r1", "event_type": "item", "seq": seq}
		snap, err := ds.Write(t.Context(), []lode.D{rec}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return snap.ID
	}
}
