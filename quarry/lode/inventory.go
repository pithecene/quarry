package lode

import (
	"cmp"
	"context"
	"errors"
	"iter"
	"slices"
	"strings"

	"github.com/pithecene/lode/lode"
)

// manifestLister is a dataset's manifest listing, which views and writers have.
type manifestLister interface {
	Manifests(ctx context.Context, partition lode.PartitionPath, opts lode.ManifestListOptions) iter.Seq2[lode.LoadedManifest, error]
}

// RunSnapshots returns every committed snapshot that holds an object under
// prefix, in commit order. It lists the dataset without a partition, because a
// partition's listing omits a snapshot whose partition index is incomplete
// (CONTRACT_LODE.md §Run Contents). Each call reads one manifest of each
// snapshot in the dataset. A dataset without snapshots holds none.
func RunSnapshots(ctx context.Context, ds manifestLister, prefix string) ([]*lode.Snapshot, error) {
	var out []*lode.Snapshot
	for loaded, err := range ds.Manifests(ctx, lode.PartitionPath{}, lode.ManifestListOptions{}) {
		if errors.Is(err, lode.ErrNotFound) {
			break
		}
		if err != nil {
			return nil, WrapReadError(err, prefix)
		}
		if holdsPrefix(loaded.Manifest, prefix) {
			out = append(out, &lode.Snapshot{ID: loaded.Ref.ID, Manifest: loaded.Manifest})
		}
	}
	slices.SortFunc(out, func(a, b *lode.Snapshot) int {
		return cmp.Or(a.CreatedAt.Compare(b.CreatedAt), cmp.Compare(a.ID, b.ID))
	})
	return out, nil
}

// ReadSnapshotRecords returns the records that the snapshots store under
// prefix, each data file read once.
func ReadSnapshotRecords(ctx context.Context, ds *lode.RecordView[lode.D], snapshots []*lode.Snapshot, prefix string) ([]lode.D, error) {
	seen := map[string]bool{}
	var out []lode.D
	for _, snap := range snapshots {
		recs, err := readManifestRecords(ctx, ds, snap.Manifest, prefix, seen)
		if err != nil {
			return nil, err
		}
		out = append(out, recs...)
	}
	return out, nil
}

func holdsPrefix(m *lode.Manifest, prefix string) bool {
	return slices.ContainsFunc(m.Objects, func(obj lode.ObjectRef) bool { return underPrefix(obj.Path, prefix) })
}

// underPrefix reports whether an object lies in the partition tree under
// prefix, which ends with "/". An encoded partition value holds no "/", so
// the prefix matches whole segments.
func underPrefix(path, prefix string) bool {
	return strings.Contains(path, "/partitions/"+prefix)
}
