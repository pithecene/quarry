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
	settings := []lode.DatasetOption{lode.WithLayout(lode.Hive(partitionKeys...)), lode.WithRetry(3)}
	return lode.Open(lode.Use(store)).Records(ctx, lode.DatasetID(dataset), recordCodec(), append(settings, opts...)...)
}

// OpenView opens a read-only view of the dataset over store, so reads come
// from exactly that store. The view takes the layout from the dataset's
// manifests. It returns lode.ErrNotFound when the dataset has no snapshot.
func OpenView(ctx context.Context, dataset string, store lode.ObjectStore) (lode.RecordView[lode.D], error) {
	return lode.OpenReadOnly(lode.Use(store)).Records(ctx, lode.DatasetID(dataset), recordCodec())
}

// ReadPartitionRecords returns every record stored in one partition (a path
// as ListPartitions returns it), across all of its committed snapshots, each
// data file read once.
func ReadPartitionRecords(ctx context.Context, ds lode.SnapshotReader, partition lode.PartitionPath) ([]lode.D, error) {
	manifests, err := ds.ListManifests(ctx, partition, lode.ManifestListOptions{})
	if err != nil {
		return nil, WrapReadError(err, partition.String())
	}
	dir := "/" + partition.String() + "/"
	seen := map[string]bool{}
	var out []lode.D
	for _, ref := range manifests {
		m, err := ds.GetManifest(ctx, ref)
		if err != nil {
			return nil, WrapReadError(err, partition.String())
		}
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
