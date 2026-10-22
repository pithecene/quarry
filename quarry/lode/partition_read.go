package lode

import (
	"context"
	"fmt"
	"strings"

	"github.com/pithecene-io/lode/lode"

	"github.com/pithecene/quarry/iox"
)

// partitionKeys is the Hive layout every Quarry dataset is written with.
var partitionKeys = []string{"source", "category", "day", "run_id", "event_type"}

// NewPartitionReader returns a Lode dataset reader over store with Quarry's
// partition layout, so reads come from exactly that store.
func NewPartitionReader(store lode.Store) (lode.DatasetReader, error) {
	return lode.NewDatasetReader(func() (lode.Store, error) { return store, nil }, lode.WithHiveLayout(partitionKeys...))
}

// ReadPartitionRecords returns every record stored in one partition (a path
// as ListPartitions returns it), across all of its committed snapshots, each
// data file read once.
func ReadPartitionRecords(ctx context.Context, dr lode.DatasetReader, ds lode.DatasetID, partition string) ([]map[string]any, error) {
	manifests, err := dr.ListManifests(ctx, ds, partition, lode.ManifestListOptions{})
	if err != nil {
		return nil, WrapReadError(err, partition)
	}
	codec := lode.NewJSONLCodec()
	seen := map[string]bool{}
	var out []map[string]any
	for _, ref := range manifests {
		m, err := dr.GetManifest(ctx, ds, ref)
		if err != nil {
			return nil, WrapReadError(err, partition)
		}
		for _, f := range m.Files {
			if seen[f.Path] || !strings.Contains(f.Path, "/"+partition+"/") {
				continue
			}
			seen[f.Path] = true
			recs, err := readObject(ctx, dr, codec, lode.ObjectRef{Dataset: ds, Manifest: ref, Path: f.Path})
			if err != nil {
				return nil, err
			}
			out = append(out, recs...)
		}
	}
	return out, nil
}

func readObject(ctx context.Context, dr lode.DatasetReader, codec lode.Codec, ref lode.ObjectRef) ([]map[string]any, error) {
	rc, err := dr.OpenObject(ctx, ref)
	if err != nil {
		return nil, WrapReadError(err, ref.Path)
	}
	defer iox.DiscardClose(rc)
	items, err := codec.Decode(rc)
	if err != nil {
		return nil, fmt.Errorf("decode %s: %w", ref.Path, err)
	}
	out := make([]map[string]any, 0, len(items))
	for _, it := range items {
		if m, ok := it.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out, nil
}
