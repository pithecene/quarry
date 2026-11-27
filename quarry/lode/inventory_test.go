package lode_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/pithecene/lode/lode"

	qlode "github.com/pithecene/quarry/lode"
)

// A later read of an inventory loads only the snapshots that committed since
// its last read, unindexed ones included, and drops the expired ones.
func TestInventory_ExtendsRatherThanRepeats(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := &getCountingStore{RangeStore: lode.NewMemory(), gets: map[string]int{}}
	ds, err := qlode.OpenDataset(ctx, "quarry", store)
	if err != nil {
		t.Fatal(err)
	}
	write := func(seq int) lode.SnapshotID {
		t.Helper()
		rec := lode.D{"source": "src", "category": "cat", "day": artifactTestDay, "run_id": "r1", "event_type": "item", "seq": seq}
		snap, err := ds.Write(ctx, []lode.D{rec}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return snap.ID
	}
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

// wide returns the listings other than the canonical manifest directory and
// the read run's own partition prefix: a listing of the dataset.
func (s *listCountingStore) wide() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	run := "datasets/quarry/partitions/source=src/category=cat/day=" + artifactTestDay + "/run_id=r1/"
	for _, p := range s.lists {
		if p != "datasets/quarry/segments/" && p != run {
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

// A snapshot that commits with partition manifests alone after the first
// read, as a writer before canonical manifests commits, is found by the next
// read of its run.
func TestInventory_FindsAPartitionOnlyCommit(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := lode.NewMemory()
	ds, ids, read := inventoryFixture(t, store, 1)
	if got, err := read(); err != nil || len(got) != 1 {
		t.Fatalf("first read = %v, %v; want one snapshot", got, err)
	}
	rec := lode.D{"source": "src", "category": "cat", "day": artifactTestDay, "run_id": "r1", "event_type": "item", "seq": 9}
	late, err := ds.Write(ctx, []lode.D{rec}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "datasets/quarry/segments/"+string(late.ID)+"/manifest.json"); err != nil {
		t.Fatal(err)
	}
	want := slices.Sorted(slices.Values([]lode.SnapshotID{ids[0], late.ID}))
	if got, err := read(); err != nil || !slices.Equal(got, want) {
		t.Fatalf("read after the partition-only commit = %v, %v; want %v", got, err, want)
	}
}
