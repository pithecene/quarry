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
// key from its indexed partitions. A run with no visible metrics has a nil policy.
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
		snapshots, err := readRunSnapshots(ctx, ds, prefix)
		if err != nil {
			return RunDetails{}, err
		}
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

func readRunSnapshots(ctx context.Context, ds *lode.RecordView[lode.D], prefix string) ([]*lode.Snapshot, error) {
	parts, err := ds.ListPartitions(ctx, lode.PartitionListOptions{Prefix: prefix})
	if err != nil {
		return nil, WrapReadError(err, prefix)
	}

	var snapshots []*lode.Snapshot
	seen := make(map[lode.SnapshotID]bool)
	for _, part := range parts {
		for loaded, err := range ds.Manifests(ctx, part, lode.ManifestListOptions{}) {
			if errors.Is(err, lode.ErrNotFound) {
				break // Retention can empty a partition after it was listed.
			}
			if err != nil {
				return nil, WrapReadError(err, part.String())
			}
			id := loaded.Ref.ID
			if !seen[id] {
				seen[id] = true
				snapshots = append(snapshots, &lode.Snapshot{ID: id, Manifest: loaded.Manifest})
			}
		}
	}
	// Merge the partition listings in the same order as Lode's snapshot catalog.
	slices.SortFunc(snapshots, func(a, b *lode.Snapshot) int {
		return cmp.Or(a.CreatedAt.Compare(b.CreatedAt), cmp.Compare(a.ID, b.ID))
	})
	return snapshots, nil
}

func latestRunMetrics(ctx context.Context, ds *lode.RecordView[lode.D], snapshots []*lode.Snapshot, key Config, prefix string) (lode.D, error) {
	partition, err := lode.ParsePartitionPath(prefix + "event_type=metrics")
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	for _, snap := range slices.Backward(snapshots) {
		records, err := readManifestRecords(ctx, ds, snap.Manifest, partition, seen)
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
