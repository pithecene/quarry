package lode

import (
	"context"
	"errors"
	"iter"
	"maps"
	"slices"
	"strings"

	"github.com/pithecene/lode/lode"
)

// manifestLister is a dataset's manifest listing, which views and writers have.
type manifestLister interface {
	Manifests(ctx context.Context, partition lode.PartitionPath, opts lode.ManifestListOptions) iter.Seq2[lode.LoadedManifest, error]
}

// RunSnapshots returns every committed snapshot that holds an object under
// prefix, in Lode's listing order. It lists the dataset without a partition, because a
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

// snapshotLoader is a dataset's manifest listing and its load of one
// snapshot by ID, which views and writers have.
type snapshotLoader interface {
	manifestLister
	Snapshot(ctx context.Context, id lode.SnapshotID) (*lode.Snapshot, error)
}

// Inventory answers RunSnapshots for one operation that reads many runs,
// such as a prune, without loading every manifest for each run. The first
// read takes Lode's unfiltered listing, which loads and validates each
// manifest once. Each later read lists the canonical manifest keys again
// and loads only the snapshots that the inventory does not hold, so it
// finds each snapshot that committed since, as a new unfiltered listing
// would (CONTRACT_LODE.md §Run Contents). A held snapshot whose canonical
// manifest is no longer listed, and that holds an object under the read's
// prefix, is loaded again by ID: Lode then finds a legacy snapshot through
// its partition manifests, and reports an expired one as absent. An
// Inventory is not safe for concurrent use.
type Inventory struct {
	dataset string
	store   lode.Store
	ds      snapshotLoader
	// held is each committed snapshot read so far, nil before the first read.
	held map[lode.SnapshotID]*lode.Manifest
}

// NewInventory returns an empty inventory of dataset, whose handle ds
// reads through store.
func NewInventory(dataset string, store lode.Store, ds snapshotLoader) *Inventory {
	return &Inventory{dataset: dataset, store: store, ds: ds}
}

// RunSnapshots returns every committed snapshot that holds an object under
// prefix, in snapshot ID order, as the package's RunSnapshots does.
func (inv *Inventory) RunSnapshots(ctx context.Context, prefix string) ([]*lode.Snapshot, error) {
	if inv.held == nil {
		return inv.first(ctx, prefix)
	}
	listed, err := inv.refresh(ctx)
	if err != nil {
		return nil, err
	}
	var out []*lode.Snapshot
	for _, id := range slices.Sorted(maps.Keys(inv.held)) {
		m := inv.held[id]
		if !holdsPrefix(m, prefix) {
			continue
		}
		if !listed[id] {
			snap, err := inv.ds.Snapshot(ctx, id)
			if errors.Is(err, lode.ErrNotFound) {
				delete(inv.held, id)
				continue
			}
			if err != nil {
				return nil, WrapReadError(err, prefix)
			}
			m = snap.Manifest
		}
		out = append(out, &lode.Snapshot{ID: id, Manifest: m})
	}
	return out, nil
}

// first fills the inventory from Lode's unfiltered listing.
func (inv *Inventory) first(ctx context.Context, prefix string) ([]*lode.Snapshot, error) {
	all, err := RunSnapshots(ctx, inv.ds, "")
	if err != nil {
		return nil, err
	}
	inv.held = make(map[lode.SnapshotID]*lode.Manifest, len(all))
	for _, s := range all {
		inv.held[s.ID] = s.Manifest
	}
	var out []*lode.Snapshot
	for _, id := range slices.Sorted(maps.Keys(inv.held)) {
		if m := inv.held[id]; holdsPrefix(m, prefix) {
			out = append(out, &lode.Snapshot{ID: id, Manifest: m})
		}
	}
	return out, nil
}

// refresh lists the canonical manifest keys and loads each listed snapshot
// that the inventory does not hold. It returns the listed snapshot IDs. A
// listed snapshot that Lode reports absent, as one whose expiry deleted its
// manifest after the listing, is not held.
func (inv *Inventory) refresh(ctx context.Context) (map[lode.SnapshotID]bool, error) {
	dir := "datasets/" + inv.dataset + "/snapshots/"
	keys, err := inv.store.List(ctx, dir)
	if err != nil {
		return nil, WrapReadError(err, dir)
	}
	listed := make(map[lode.SnapshotID]bool, len(inv.held))
	for _, key := range keys {
		id, ok := strings.CutSuffix(strings.TrimPrefix(key, dir), "/manifest.json")
		if ok && id != "" && !strings.Contains(id, "/") {
			listed[lode.SnapshotID(id)] = true
		}
	}
	for _, id := range slices.Sorted(maps.Keys(listed)) {
		if _, ok := inv.held[id]; ok {
			continue
		}
		snap, err := inv.ds.Snapshot(ctx, id)
		if errors.Is(err, lode.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, WrapReadError(err, dir)
		}
		inv.held[id] = snap.Manifest
	}
	return listed, nil
}
