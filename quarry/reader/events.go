package reader

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/pithecene/lode/lode"

	qlode "github.com/pithecene/quarry/lode"
)

// ErrInvalidRun is returned when a run passed to ReadEvents was not read
// from this reader's dataset or its partition is not the run's own prefix.
var ErrInvalidRun = errors.New("invalid run")

// ReadEvents returns the run's event records (record_kind "event" and
// "artifact_event") in sequence order. Artifact chunks and the metrics record
// are not events and are omitted. The run's partitions are addressed from its
// start record's partition prefix, never re-derived, and every committed
// snapshot under it is read, indexed or not.
func (r *Reader) ReadEvents(ctx context.Context, run Run) ([]map[string]any, error) {
	if _, err := r.validateRun(run); err != nil {
		return nil, err
	}
	recs, err := r.readRecords(ctx, run.Partition)
	if err != nil {
		return nil, err
	}
	events := []map[string]any{}
	for _, rec := range recs {
		if kind, _ := rec["record_kind"].(string); kind == qlode.RecordKindEvent || kind == qlode.RecordKindArtifactEvent {
			events = append(events, rec)
		}
	}
	sort.SliceStable(events, func(i, j int) bool { return intOf(events[i]["seq"]) < intOf(events[j]["seq"]) })
	return events, nil
}

// validateRun checks that run names exactly its own partition in this
// dataset and returns its key. A Run is a plain value, so a foreign or
// altered Run must not widen a listing.
func (r *Reader) validateRun(run Run) (qlode.Config, error) {
	if run.Dataset != r.dataset {
		return qlode.Config{}, fmt.Errorf("%w: run %q is in dataset %q, not %q", ErrInvalidRun, run.RunID, run.Dataset, r.dataset)
	}
	cfg := qlode.Config{Dataset: run.Dataset, Source: run.Source, Category: run.Category, Day: run.Day, RunID: run.RunID}
	if err := qlode.ValidateRunKey(cfg.Dataset, cfg.Source, cfg.Category, cfg.RunID); err != nil {
		return qlode.Config{}, fmt.Errorf("%w: %w", ErrInvalidRun, err)
	}
	if want := qlode.PartitionPrefix(cfg); run.Partition != want {
		return qlode.Config{}, fmt.Errorf("%w: run %q has partition %q, want %q", ErrInvalidRun, run.RunID, run.Partition, want)
	}
	return cfg, nil
}

// readRecords returns every record under prefix, which ends with "/": a run's
// partition prefix, or one partition of it. The view is bound to the reader's
// store, so records come from the same store the run was listed from. A
// dataset without snapshots, or a prefix without objects, holds no records.
func (r *Reader) readRecords(ctx context.Context, prefix string) ([]lode.D, error) {
	dr, err := qlode.OpenView(ctx, r.dataset, r.store)
	if errors.Is(err, lode.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, qlode.WrapReadError(err, "datasets/"+r.dataset)
	}
	// The prefix ends with "/", so run_id=r1/ never matches run_id=r10/.
	snapshots, err := qlode.RunSnapshots(ctx, dr, prefix)
	if err != nil {
		return nil, err
	}
	return qlode.ReadSnapshotRecords(ctx, dr, snapshots, prefix)
}
