package reader

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/pithecene-io/lode/lode"

	"github.com/pithecene/quarry/iox"
	qlode "github.com/pithecene/quarry/lode"
)

// ErrRunWithoutPartition is returned when a run's start record names no
// partition prefix.
var ErrRunWithoutPartition = errors.New("run has no partition")

// ReadEvents returns the run's event records (record_kind "event" and
// "artifact_event") in sequence order. Artifact chunks and the metrics record
// are not events and are omitted. The run's partitions are addressed from its
// start record's partition prefix, never re-derived.
func (r *Reader) ReadEvents(ctx context.Context, run Run) ([]map[string]any, error) {
	if run.Partition == "" || !strings.HasSuffix(run.Partition, "/") {
		return nil, fmt.Errorf("%w: %q", ErrRunWithoutPartition, run.RunID)
	}
	dr, err := lode.NewDatasetReader(r.factory, lode.WithHiveLayout("source", "category", "day", "run_id", "event_type"))
	if err != nil {
		return nil, qlode.WrapReadError(err, "datasets/"+r.dataset)
	}
	ds := lode.DatasetID(r.dataset)

	// The prefix ends with "/", so run_id=r1/ never matches run_id=r10/.
	parts, err := dr.ListPartitions(ctx, ds, lode.PartitionListOptions{Prefix: run.Partition})
	if err != nil {
		if errors.Is(err, lode.ErrNotFound) {
			return []map[string]any{}, nil
		}
		return nil, qlode.WrapReadError(err, run.Partition)
	}

	codec := lode.NewJSONLCodec()
	seen := map[string]bool{}
	var events []map[string]any
	for _, part := range parts {
		manifests, err := dr.ListManifests(ctx, ds, part.Path, lode.ManifestListOptions{})
		if err != nil {
			return nil, qlode.WrapReadError(err, part.Path)
		}
		for _, ref := range manifests {
			m, err := dr.GetManifest(ctx, ds, ref)
			if err != nil {
				return nil, qlode.WrapReadError(err, part.Path)
			}
			for _, f := range m.Files {
				if seen[f.Path] || !strings.Contains(f.Path, "/"+part.Path+"/") {
					continue
				}
				seen[f.Path] = true
				recs, err := readObject(ctx, dr, codec, ds, ref, f.Path)
				if err != nil {
					return nil, err
				}
				for _, rec := range recs {
					if kind, _ := rec["record_kind"].(string); kind == qlode.RecordKindEvent || kind == qlode.RecordKindArtifactEvent {
						events = append(events, rec)
					}
				}
			}
		}
	}
	sort.SliceStable(events, func(i, j int) bool { return seqOf(events[i]) < seqOf(events[j]) })
	if events == nil {
		events = []map[string]any{}
	}
	return events, nil
}

func readObject(ctx context.Context, dr lode.DatasetReader, codec lode.Codec, ds lode.DatasetID, ref lode.ManifestRef, path string) ([]map[string]any, error) {
	rc, err := dr.OpenObject(ctx, lode.ObjectRef{Dataset: ds, Manifest: ref, Path: path})
	if err != nil {
		return nil, qlode.WrapReadError(err, path)
	}
	defer iox.DiscardClose(rc)
	items, err := codec.Decode(rc)
	if err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	out := make([]map[string]any, 0, len(items))
	for _, it := range items {
		if m, ok := it.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out, nil
}

func seqOf(rec map[string]any) float64 {
	switch v := rec["seq"].(type) {
	case float64:
		return v
	case int64:
		return float64(v)
	case int:
		return float64(v)
	}
	return 0
}
