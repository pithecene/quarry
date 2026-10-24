package lode

import (
	"context"
	"errors"
	"strings"

	"github.com/pithecene/lode/lode"
)

// openReadView creates the store and opens a read-only view of the dataset
// over it. ok is false when the dataset has no snapshot, which a reader
// reports as an empty result rather than an error.
func openReadView(ctx context.Context, dataset string, factory lode.Factory[lode.RangeStore]) (view lode.RecordView[lode.D], ok bool, err error) {
	if factory == nil {
		return nil, false, errors.New("read: nil store factory")
	}
	store, err := factory()
	if err != nil {
		return nil, false, err
	}
	view, err = OpenView(ctx, dataset, store)
	if errors.Is(err, lode.ErrNotFound) {
		return nil, false, nil
	}
	return view, err == nil, err
}

// isMetricsSnapshot checks if a snapshot contains metrics data
// by examining file paths for the event_type=metrics partition.
func isMetricsSnapshot(snap *lode.DatasetSnapshot) bool {
	for _, f := range snap.Files {
		if matchesPartitionValue(f.Path, "event_type", "metrics") {
			return true
		}
	}
	return false
}

// snapshotMatchesFilter checks if a snapshot's file paths match
// the given partition key=value filter.
func snapshotMatchesFilter(snap *lode.DatasetSnapshot, key, value string) bool {
	if value == "" {
		return true
	}
	for _, f := range snap.Files {
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
	segment := key + "=" + value
	// Split path into segments and match exactly
	for _, part := range strings.Split(path, "/") {
		if part == segment {
			return true
		}
	}
	return false
}
