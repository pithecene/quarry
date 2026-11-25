package lode

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/pithecene/lode/lode"
)

// RunDetails holds a run's policy from its metrics and its sidecar inventory.
// SidecarFiles keeps each path as the inventory records it, consecutive slashes
// included (CONTRACT_LODE.md § Sidecar File Inventory).
type RunDetails struct {
	Policy       *string
	SidecarFiles []SidecarFileRef
}

// QueryRunDetails reads metrics and sidecar metadata for the run identified by
// key from its committed snapshots. A run with no visible metrics has a nil policy.
func QueryRunDetails(ctx context.Context, factory lode.Factory[lode.RangeStore], key Config) (RunDetails, error) {
	ds, store, ok, err := openReadStore(ctx, key.Dataset, factory)
	if err != nil {
		return RunDetails{}, WrapReadError(err, "datasets/"+key.Dataset)
	}
	end, err := readEndFields(ctx, store, key)
	if err != nil {
		return RunDetails{}, fmt.Errorf("read end record: %w", err)
	}
	record, err := endMetrics(end, key)
	if err != nil {
		return RunDetails{}, fmt.Errorf("read metrics: %w", err)
	}
	var details RunDetails
	var files []SidecarFileRef
	// A run can write files and its end record without a snapshot.
	if ok {
		prefix := PartitionPrefix(key)
		snapshots, err := RunSnapshots(ctx, ds, prefix)
		if err != nil {
			return RunDetails{}, err
		}
		// The latest metrics stand, and files keep the order of their commits.
		slices.SortFunc(snapshots, func(a, b *lode.Snapshot) int {
			return cmp.Or(a.CreatedAt.Compare(b.CreatedAt), cmp.Compare(a.ID, b.ID))
		})
		if record == nil {
			record, err = latestRunMetrics(ctx, ds, snapshots, key, prefix)
			if err != nil && !errors.Is(err, ErrNoMetricsFound) {
				return RunDetails{}, fmt.Errorf("read metrics: %w", err)
			}
		}
		files, err = sidecarFiles(ctx, snapshots, runSidecarOwner(key, store))
		if err != nil {
			return RunDetails{}, fmt.Errorf("read sidecar files: %w", err)
		}
	}
	if record != nil {
		policy, ok := record["policy"].(string)
		if !ok {
			return RunDetails{}, fmt.Errorf("read metrics: expected string policy for run %q, got %T", key.RunID, record["policy"])
		}
		switch policy {
		case "strict", "buffered", "streaming":
			details.Policy = &policy
		default:
			return RunDetails{}, fmt.Errorf("read metrics: unknown policy %q for run %q", policy, key.RunID)
		}
	}
	recorded, err := endSidecars(end, key)
	if err != nil {
		return RunDetails{}, fmt.Errorf("read sidecar files: %w", err)
	}
	// Snapshots and the end record can each name a file.
	seen := make(map[SidecarFileRef]bool, len(files))
	for _, file := range files {
		seen[file] = true
	}
	for _, file := range recorded {
		if !seen[file] {
			files = append(files, file)
		}
	}
	details.SidecarFiles = files
	return details, nil
}

func latestRunMetrics(ctx context.Context, ds *lode.RecordView[lode.D], snapshots []*lode.Snapshot, key Config, prefix string) (lode.D, error) {
	seen := make(map[string]bool)
	for _, snap := range slices.Backward(snapshots) {
		records, err := readManifestRecords(ctx, ds, snap.Manifest, prefix+"event_type=metrics/", seen)
		if err != nil {
			return nil, WrapReadError(err, "quarry/snapshot/"+string(snap.ID))
		}
		for _, record := range records {
			if matchesMetricsRecord(record, key) {
				return record, nil
			}
		}
	}
	return nil, ErrNoMetricsFound
}
