package lode

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/pithecene/lode/lode"
)

// openReadView creates the store and opens a read-only view of the dataset
// over it. ok is false when the dataset has no snapshot, which a reader
// reports as an empty result rather than an error.
func openReadView(ctx context.Context, dataset string, factory lode.Factory[lode.RangeStore]) (view *lode.RecordView[lode.D], ok bool, err error) {
	view, _, ok, err = openReadStore(ctx, dataset, factory)
	return view, ok, err
}

// openReadStore is openReadView that also returns the store it built, for a
// caller that opens another view of the same dataset.
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
		return nil, nil, false, nil
	}
	return view, store, err == nil, err
}

// readSnapshots returns a read view and its snapshot catalog. A nil view
// means the dataset has no snapshots.
func readSnapshots(ctx context.Context, dataset string, factory lode.Factory[lode.RangeStore]) (*lode.RecordView[lode.D], []*lode.Snapshot, error) {
	ds, ok, err := openReadView(ctx, dataset, factory)
	if err != nil {
		return nil, nil, WrapReadError(err, "datasets/"+dataset)
	}
	if !ok {
		return nil, nil, nil
	}
	snapshots, err := ds.Snapshots(ctx)
	if err != nil {
		return nil, nil, WrapReadError(err, "quarry/snapshots")
	}
	return ds, snapshots, nil
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
