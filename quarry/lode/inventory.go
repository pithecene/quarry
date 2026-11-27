package lode

import (
	"context"
	"errors"
	"io"
	"iter"
	"maps"
	"slices"
	"strings"

	"github.com/pithecene/lode/lode"

	"github.com/pithecene/quarry/iox"
)

// listPrefetch bounds the manifests that an unfiltered listing loads ahead,
// concurrently. On a development bucket of 14,834 snapshots, loading them one
// at a time took 43 minutes, and 128 ahead took 67 seconds, in the same order
// with the same results.
const listPrefetch = 128

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
	for loaded, err := range ds.Manifests(ctx, lode.PartitionPath{}, lode.ManifestListOptions{Prefetch: listPrefetch}) {
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

// Inventory answers a prune's reads of each run's snapshots without loading
// every manifest for each run. The first read takes Lode's unfiltered
// listing, which loads and validates each manifest once. Each later read
// checks Lode's removal fence, lists the keys of the canonical manifests and
// of the run's partition manifests, and loads only the snapshots that the
// inventory does not hold, so it finds each snapshot that committed since,
// a legacy one with partition manifests alone included, as a new unfiltered
// listing would (CONTRACT_LODE.md §Order of Operations). A held snapshot of
// the run whose canonical manifest is no longer listed is a legacy snapshot
// that only partition manifests record when no tombstone exists, and
// otherwise Lode reads it by ID. An Inventory is not safe for concurrent use.
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
// prefix. Unlike the package's RunSnapshots, which keeps Lode's listing order,
// it returns them in snapshot ID order, so its result does not depend on
// which read first found each snapshot.
func (inv *Inventory) RunSnapshots(ctx context.Context, prefix string) ([]*lode.Snapshot, error) {
	if inv.held == nil {
		return inv.first(ctx, prefix)
	}
	listed, err := inv.refresh(ctx)
	if err != nil {
		return nil, err
	}
	if err := inv.partitionOnly(ctx, prefix, listed); err != nil {
		return nil, err
	}
	var out []*lode.Snapshot
	for id, m := range inv.held {
		if !holdsPrefix(m, prefix) {
			continue
		}
		if !listed[id] {
			live, err := inv.unlisted(ctx, id)
			if err != nil {
				return nil, WrapReadError(err, prefix)
			}
			if !live {
				delete(inv.held, id)
				continue
			}
		}
		out = append(out, &lode.Snapshot{ID: id, Manifest: m})
	}
	slices.SortFunc(out, func(a, b *lode.Snapshot) int { return strings.Compare(string(a.ID), string(b.ID)) })
	return out, nil
}

// partitionOnly holds each snapshot that a partition manifest under the
// run's prefix records and that neither the inventory nor the canonical
// listing holds. A writer before canonical manifests commits a snapshot with
// partition manifests alone (Lode CONTRACT_CORE §Head Pointer), so a refresh
// of canonical keys cannot find one committed after the first read. A Quarry
// write holds one run's records (LodeClient.WriteEvents builds each from its
// run's config), so every partition manifest of such a snapshot lies under
// the run's prefix. Each partition that names an unseen ID is read through
// Lode's partition listing, which validates its manifests and leaves out an
// expired snapshot, so no lookup scans the dataset.
func (inv *Inventory) partitionOnly(ctx context.Context, prefix string, listed map[lode.SnapshotID]bool) error {
	dir := inv.dir() + "partitions/" + prefix
	keys, err := inv.store.List(ctx, dir)
	if err != nil {
		return WrapReadError(err, dir)
	}
	unseen := map[lode.SnapshotID]bool{}
	partitions := map[string]bool{}
	for _, key := range keys {
		partition, id, ok := partitionManifest(strings.TrimPrefix(key, dir))
		if !ok {
			continue
		}
		if _, held := inv.held[id]; held || listed[id] {
			continue
		}
		unseen[id] = true
		partitions[prefix+partition] = true
	}
	for _, p := range slices.Sorted(maps.Keys(partitions)) {
		path, err := lode.ParsePartitionPath(p)
		if err != nil {
			return WrapReadError(err, dir)
		}
		for loaded, err := range inv.ds.Manifests(ctx, path, lode.ManifestListOptions{}) {
			if errors.Is(err, lode.ErrNotFound) {
				break
			}
			if err != nil {
				return WrapReadError(err, p)
			}
			if unseen[loaded.Ref.ID] {
				inv.held[loaded.Ref.ID] = loaded.Manifest
			}
		}
	}
	return nil
}

// partitionManifest parses a key below the run's partition prefix of the form
// <partition>/segments/<snapshot>/manifest.json into its partition and ID.
func partitionManifest(rel string) (partition string, id lode.SnapshotID, ok bool) {
	rest, ok := strings.CutSuffix(rel, "/manifest.json")
	if !ok {
		return "", "", false
	}
	i := strings.LastIndex(rest, "/segments/")
	if i <= 0 {
		return "", "", false
	}
	idPart := rest[i+len("/segments/"):]
	if idPart == "" || strings.Contains(idPart, "/") {
		return "", "", false
	}
	return rest[:i], lode.SnapshotID(idPart), true
}

// unlisted reports whether a held snapshot whose canonical manifest is no
// longer listed is still live. Without a tombstone it is a legacy snapshot
// that its partition manifests record, since expiry records the tombstone
// before it deletes a manifest. With one, Lode reads the tombstone, which
// tells an expiry from a placeholder and fails on a malformed one.
func (inv *Inventory) unlisted(ctx context.Context, id lode.SnapshotID) (bool, error) {
	tombstoned, err := inv.store.Exists(ctx, inv.dir()+"expired/"+string(id))
	if err != nil || !tombstoned {
		return !tombstoned, err
	}
	_, err = inv.ds.Snapshot(ctx, id)
	if errors.Is(err, lode.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
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

// dir is the dataset's directory in the store.
func (inv *Inventory) dir() string { return "datasets/" + inv.dataset + "/" }

// refresh checks the removal fence, lists the canonical manifest keys, and
// loads each listed snapshot that the inventory does not hold. It returns
// the listed snapshot IDs. A listed snapshot that Lode reports absent, as one
// whose expiry deleted its manifest after the listing, is not held.
func (inv *Inventory) refresh(ctx context.Context) (map[lode.SnapshotID]bool, error) {
	if err := inv.checkLive(ctx); err != nil {
		return nil, err
	}
	// Quarry's Hive layout keeps each canonical manifest at
	// datasets/<dataset>/segments/<snapshot>/manifest.json.
	dir := inv.dir() + "segments/"
	keys, err := inv.store.List(ctx, dir)
	if err != nil {
		return nil, WrapReadError(err, dir)
	}
	listed := make(map[lode.SnapshotID]bool, len(inv.held))
	var unheld []lode.SnapshotID
	for _, key := range keys {
		id, ok := strings.CutSuffix(strings.TrimPrefix(key, dir), "/manifest.json")
		if !ok || id == "" || strings.Contains(id, "/") {
			continue
		}
		listed[lode.SnapshotID(id)] = true
		if _, held := inv.held[lode.SnapshotID(id)]; !held {
			unheld = append(unheld, lode.SnapshotID(id))
		}
	}
	slices.Sort(unheld)
	for _, id := range unheld {
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

// removedPointer is the value of the head pointer that Lode's removal of a
// dataset leaves (Lode PUBLIC_API.md §Root.RemoveDataset).
const removedPointer = "lode:removed"

// checkLive returns lode.ErrRemoved when the dataset's head pointer holds
// Lode's removal fence, as each Lode read checks before it lists. A missing
// pointer is live: a dataset can hold snapshots from before pointers.
func (inv *Inventory) checkLive(ctx context.Context) error {
	path := inv.dir() + "latest"
	rc, err := inv.store.Get(ctx, path)
	if errors.Is(err, lode.ErrNotFound) {
		return nil
	}
	if err != nil {
		return WrapReadError(err, path)
	}
	defer iox.DiscardClose(rc)
	body, err := io.ReadAll(rc)
	if err != nil {
		return WrapReadError(err, path)
	}
	if strings.TrimSpace(string(body)) == removedPointer {
		return WrapReadError(lode.ErrRemoved, path)
	}
	return nil
}
