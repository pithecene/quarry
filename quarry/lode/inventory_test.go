package lode_test

import (
	"strings"
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
	if got := read(); len(got) != 2 || got[0] != first || got[1] != second {
		t.Fatalf("first read = %v, want %s and %s", got, first, second)
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
	if got := read(); len(got) != 3 || got[2] != third {
		t.Fatalf("second read = %v, want the unindexed %s last", got, third)
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
	if got := read(); len(got) != 2 || got[0] != second || got[1] != third {
		t.Fatalf("read after the expiry = %v, want %s and %s", got, second, third)
	}
}
