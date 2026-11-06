package lode

import (
	"context"
	"fmt"
	"strings"

	"github.com/pithecene/lode/lode"

	"github.com/pithecene/quarry/iox"
)

// partitionKeys is the Hive layout every Quarry dataset is written with.
var partitionKeys = []string{"source", "category", "day", "run_id", "event_type"}

// recordCodec is the codec every Quarry dataset is written with. Its records
// are maps, so the Hive layout reads partition values from their keys.
func recordCodec() lode.Codec[lode.D] { return lode.JSONL[lode.D]() }

// OpenDataset opens the dataset over store for writing with the settings
// every Quarry writer shares (CONTRACT_LODE.md §Writer Settings): the Hive
// layout, the JSONL codec, and bounded commit retry. Lode records the
// settings in each manifest and rejects, with lode.ErrSettingsMismatch, a
// writer whose settings differ from the head's. Opening reads the head and
// writes nothing.
func OpenDataset(ctx context.Context, dataset string, store lode.Store, opts ...lode.DatasetOption) (lode.Records[lode.D], error) {
	settings := []lode.DatasetOption{lode.WithLayout(lode.Hive(partitionKeys...)), lode.WithRetry(datasetRetryCount)}
	return lode.Open(lode.Use(store)).Records(ctx, lode.DatasetID(dataset), recordCodec(), append(settings, opts...)...)
}

// OpenView opens a read-only view of the dataset over store, so reads come
// from exactly that store. The view takes the layout from the dataset's
// manifests. It returns lode.ErrNotFound when the dataset has no snapshot.
func OpenView(ctx context.Context, dataset string, store lode.ObjectStore) (lode.RecordView[lode.D], error) {
	return lode.OpenReadOnly(lode.Use(store)).Records(ctx, lode.DatasetID(dataset), recordCodec())
}

// OpenViewAt opens a read-only view of the dataset over store at one
// snapshot, which it reads in place of the head (Lode's WithPinnedSnapshot).
// The view learns the layout from that snapshot, so the view's Snapshot of
// that ID reads its canonical manifest with one store request. It returns
// lode.ErrNotFound when the snapshot does not exist, lode.ErrSnapshotExpired
// when retention expired it, and lode.ErrInvalidArgument when the ID is not
// one path segment.
func OpenViewAt(ctx context.Context, dataset string, store lode.ObjectStore, snapshot lode.DatasetSnapshotID) (lode.RecordView[lode.D], error) {
	return lode.OpenReadOnly(lode.Use(store)).Records(ctx, lode.DatasetID(dataset), recordCodec(), lode.WithPinnedSnapshot(snapshot))
}

// ReadPartitionRecords returns every record stored in one partition (a path
// as ListPartitions returns it), across all of its committed snapshots, each
// data file read once. Lode's Manifests yields each manifest with the load
// that its listing made, so no manifest is read twice, and it skips a
// manifest that expired after the listing.
func ReadPartitionRecords(ctx context.Context, ds lode.SnapshotReader, partition lode.PartitionPath) ([]lode.D, error) {
	seen := map[string]bool{}
	var out []lode.D
	for loaded, err := range ds.Manifests(ctx, partition, lode.ManifestListOptions{}) {
		if err != nil {
			return nil, WrapReadError(err, partition.String())
		}
		recs, err := readManifestRecords(ctx, ds, loaded.Ref, loaded.Manifest, partition, seen)
		if err != nil {
			return nil, err
		}
		out = append(out, recs...)
	}
	return out, nil
}

// ReadSnapshotPartitionRecords returns the records that one committed
// snapshot stored in one partition. It reads the snapshot's canonical
// manifest, so a snapshot that retention expired, whose partition manifest
// may remain as a residual object, is never served (Lode
// CONTRACT_RETENTION §Expired Snapshot). It returns lode.ErrNotFound or
// lode.ErrSnapshotExpired, wrapped, when the snapshot is gone, and
// lode.ErrNotFound when one of its data objects is.
func ReadSnapshotPartitionRecords(ctx context.Context, ds lode.SnapshotReader, snapshot lode.DatasetSnapshotID, partition lode.PartitionPath) ([]lode.D, error) {
	snap, err := ds.Snapshot(ctx, snapshot)
	if err != nil {
		return nil, WrapReadError(err, partition.String())
	}
	ref := lode.ManifestRef{ID: snapshot, Partition: partition}
	return readManifestRecords(ctx, ds, ref, snap.Manifest, partition, map[string]bool{})
}

// readManifestRecords reads the records of the manifest's files in the
// partition, skipping a file that seen holds and adding each file it reads.
func readManifestRecords(ctx context.Context, ds lode.SnapshotReader, ref lode.ManifestRef, m *lode.Manifest, partition lode.PartitionPath, seen map[string]bool) ([]lode.D, error) {
	dir := "/" + partition.String() + "/"
	var out []lode.D
	for _, f := range m.Files {
		if seen[f.Path] || !strings.Contains(f.Path, dir) {
			continue
		}
		seen[f.Path] = true
		recs, err := readObject(ctx, ds, lode.ObjectRef{Manifest: ref, Path: f.Path})
		if err != nil {
			return nil, err
		}
		out = append(out, recs...)
	}
	return out, nil
}

func readObject(ctx context.Context, ds lode.SnapshotReader, ref lode.ObjectRef) ([]lode.D, error) {
	rc, err := ds.OpenObject(ctx, ref)
	if err != nil {
		return nil, WrapReadError(err, ref.Path)
	}
	defer iox.DiscardClose(rc)
	recs, err := recordCodec().Decode(rc)
	if err != nil {
		return nil, fmt.Errorf("decode %s: %w", ref.Path, err)
	}
	return recs, nil
}

// datasetRetryCount bounds the commit retries of every Quarry writer, the
// dataset's and each artifact Volume's alike.
const datasetRetryCount = 3
