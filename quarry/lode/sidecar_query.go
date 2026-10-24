package lode

import (
	"context"

	"github.com/pithecene/lode/lode"
)

// QuerySidecarFiles collects all sidecar file refs from snapshot metadata
// for a given run of the dataset stored behind factory. Returns the union of
// sidecar_files across all snapshots that match the runID filter.
//
// Returns nil (not an error) if no sidecar files are found.
func QuerySidecarFiles(ctx context.Context, factory lode.Factory[lode.RangeStore], dataset, runID string) ([]SidecarFileRef, error) {
	ds, ok, err := openReadView(ctx, dataset, factory)
	if err != nil {
		return nil, WrapReadError(err, "datasets/"+dataset)
	}
	if !ok {
		return nil, nil
	}
	snapshots, err := ds.Snapshots(ctx)
	if err != nil {
		return nil, WrapReadError(err, "quarry/snapshots")
	}

	var result []SidecarFileRef

	for _, snap := range snapshots {
		if !snapshotMatchesFilter(snap, "run_id", runID) {
			continue
		}

		var meta map[string]any
		if err := snap.Metadata.Decode(&meta); err != nil {
			return nil, WrapReadError(err, "quarry/snapshot/"+string(snap.ID))
		}
		raw, ok := meta[MetadataKeySidecarFiles]
		if !ok {
			continue
		}

		// Metadata values are deserialized as []any from JSON round-trip.
		files, ok := raw.([]any)
		if !ok {
			continue
		}

		for _, f := range files {
			m, ok := f.(map[string]any)
			if !ok {
				continue
			}
			result = append(result, SidecarFileRef{
				Path:        toString(m["path"]),
				Filename:    toString(m["filename"]),
				ContentType: toString(m["content_type"]),
				Size:        toInt64Any(m["size"]),
			})
		}
	}

	return result, nil
}

// toInt64Any converts a value to int64, handling both int64 (direct) and
// float64 (JSON round-trip) representations.
func toInt64Any(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case float64:
		return int64(n)
	case int:
		return int64(n)
	default:
		return 0
	}
}
