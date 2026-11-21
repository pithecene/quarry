package lode

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/pithecene/lode/lode"
)

// ErrNoMetricsFound is returned when no metrics records exist in the dataset.
var ErrNoMetricsFound = errors.New("no metrics records found")

// QueryLatestMetrics finds and reads the most recent metrics record of the
// dataset stored behind factory. Filters by runID and source if non-empty.
// Returns the raw record map or ErrNoMetricsFound if none exist.
func QueryLatestMetrics(ctx context.Context, factory lode.Factory[lode.RangeStore], dataset, runID, source string) (map[string]any, error) {
	ds, snapshots, err := readSnapshots(ctx, dataset, factory)
	if err != nil {
		return nil, err
	}
	if ds == nil {
		return nil, ErrNoMetricsFound
	}
	return latestMetrics(ctx, ds, snapshots, Config{RunID: runID, Source: source})
}

func latestMetrics(ctx context.Context, ds *lode.RecordView[lode.D], snapshots []*lode.Snapshot, filter Config) (map[string]any, error) {
	// Iterate in reverse (latest first) — snapshots are ordered by creation time
	for _, snap := range slices.Backward(snapshots) {
		if !isMetricsSnapshot(snap) {
			continue
		}
		if !snapshotMatchesFilter(snap, "run_id", filter.RunID) {
			continue
		}
		if !snapshotMatchesFilter(snap, "source", filter.Source) {
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
			if matchesMetricsRecord(record, filter) {
				return record, nil
			}
		}
	}

	return nil, ErrNoMetricsFound
}

func matchesMetricsRecord(record lode.D, filter Config) bool {
	return record["record_kind"] == RecordKindMetrics &&
		(filter.RunID == "" || toString(record["run_id"]) == filter.RunID) &&
		(filter.Source == "" || toString(record["source"]) == filter.Source) &&
		(filter.Category == "" || toString(record["category"]) == filter.Category) &&
		(filter.Day == "" || toString(record["day"]) == filter.Day)
}

// toString converts a value to string, returning empty string for nil/non-string.
func toString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
