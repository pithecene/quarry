package reader

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/pithecene-io/lode/lode"

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
	// Bind the dataset reader to the reader's store, so events come from the
	// same store the run was listed from.
	dr, err := qlode.NewPartitionReader(r.store)
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

	var events []map[string]any
	for _, part := range parts {
		recs, err := qlode.ReadPartitionRecords(ctx, dr, ds, part.Path)
		if err != nil {
			return nil, err
		}
		for _, rec := range recs {
			if kind, _ := rec["record_kind"].(string); kind == qlode.RecordKindEvent || kind == qlode.RecordKindArtifactEvent {
				events = append(events, rec)
			}
		}
	}
	sort.SliceStable(events, func(i, j int) bool { return seqOf(events[i]) < seqOf(events[j]) })
	if events == nil {
		events = []map[string]any{}
	}
	return events, nil
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
