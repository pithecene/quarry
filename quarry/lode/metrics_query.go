package lode

import (
	"context"
	"errors"
	"fmt"

	"github.com/pithecene/lode/lode"
)

// ErrNoMetricsFound is returned when no metrics records exist in the dataset.
var ErrNoMetricsFound = errors.New("no metrics records found")

// QueryLatestMetrics finds and reads the most recent metrics record of the
// dataset stored behind factory. Filters by runID and source if non-empty.
// Returns the raw record map or ErrNoMetricsFound if none exist.
func QueryLatestMetrics(ctx context.Context, factory lode.Factory[lode.RangeStore], dataset, runID, source string) (map[string]any, error) {
	ds, ok, err := openReadView(ctx, dataset, factory)
	if err != nil {
		return nil, WrapReadError(err, "datasets/"+dataset)
	}
	if !ok {
		return nil, ErrNoMetricsFound
	}
	snapshots, err := ds.Snapshots(ctx)
	if err != nil {
		return nil, WrapReadError(err, "quarry/snapshots")
	}

	// Iterate in reverse (latest first) — snapshots are ordered by creation time
	for i := len(snapshots) - 1; i >= 0; i-- {
		snap := snapshots[i]

		if !isMetricsSnapshot(snap) {
			continue
		}
		if !snapshotMatchesFilter(snap, "run_id", runID) {
			continue
		}
		if !snapshotMatchesFilter(snap, "source", source) {
			continue
		}

		// Read snapshot data
		data, err := ds.Read(ctx, snap.ID)
		if err != nil {
			return nil, WrapReadError(err, fmt.Sprintf("quarry/snapshot/%s", snap.ID))
		}

		// Find a metrics record that passes record-level filters.
		// Manifest path filtering is a coarse pre-filter; record fields
		// are authoritative (handles cumulative/multi-record snapshots).
		for _, record := range data {
			if record["record_kind"] != RecordKindMetrics {
				continue
			}
			if runID != "" && toString(record["run_id"]) != runID {
				continue
			}
			if source != "" && toString(record["source"]) != source {
				continue
			}
			return record, nil
		}
	}

	return nil, ErrNoMetricsFound
}

// toString converts a value to string, returning empty string for nil/non-string.
func toString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
