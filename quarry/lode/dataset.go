package lode

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/pithecene/lode/lode"
)

// openReadStore creates the store and opens a read-only view of the dataset over
// it. ok is false when the dataset has no snapshot, which a reader reports as an
// empty result rather than an error. The store is returned then too.
func openReadStore(ctx context.Context, dataset string, factory lode.Factory[lode.RangeStore]) (view *lode.RecordView[lode.D], store lode.RangeStore, ok bool, err error) {
	if factory == nil {
		return nil, nil, false, errors.New("read: nil store factory")
	}
	store, err = factory()
	if err != nil {
		return nil, nil, false, err
	}
	view, err = OpenView(ctx, dataset, store)
	if errors.Is(err, lode.ErrNotFound) {
		return nil, store, false, nil
	}
	return view, store, err == nil, err
}

// isMetricsSnapshot checks if a snapshot contains metrics data
// by examining file paths for the event_type=metrics partition.
func isMetricsSnapshot(snap *lode.Snapshot) bool {
	for _, f := range snap.Objects {
		if matchesPartitionValue(f.Path, "event_type", "metrics") {
			return true
		}
	}
	return false
}

// snapshotMatchesFilter checks if a snapshot's file paths match
// the given partition key=value filter.
func snapshotMatchesFilter(snap *lode.Snapshot, key, value string) bool {
	if value == "" {
		return true
	}
	for _, f := range snap.Objects {
		if matchesPartitionValue(f.Path, key, value) {
			return true
		}
	}
	return false
}

// matchesPartitionValue checks if a Hive-partitioned path contains an exact
// key=value segment. Segments are delimited by "/" in paths. This avoids
// substring false positives (e.g., run_id=run-1 matching run_id=run-10).
func matchesPartitionValue(path, key, value string) bool {
	return slices.Contains(strings.Split(path, "/"), key+"="+encode(value))
}
