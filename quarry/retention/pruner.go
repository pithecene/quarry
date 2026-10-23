package retention

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pithecene/lode/lode"

	qlode "github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/reader"
)

// Reasons a run the policy selected is kept.
const (
	keptMismatch = "start record does not describe its key"
	keptNoSince  = "no recorded time to measure its age from"
	keptHead     = "holds the dataset head; a later prune removes it once another run commits"
	keptMixed    = "has snapshots with files outside its partition"
	keptPending  = "the dataset head is uncommitted or absent; rerun the prune"
)

// pruner removes one run at a time from one dataset.
type pruner struct {
	dataset string
	opts    Options
	runs    *reader.Reader
	store   lode.Store
	ds      lode.Dataset
	dr      lode.DatasetReader
}

func newPruner(dataset string, factory lode.StoreFactory, opts Options) (*pruner, error) {
	runs, err := reader.New(dataset, factory)
	if err != nil {
		return nil, err
	}
	store, err := factory()
	if err != nil {
		return nil, qlode.WrapInitError(err, dataset)
	}
	if store == nil {
		return nil, errors.New("prune: store factory returned a nil store")
	}
	// The dataset and reader share the store, so every call sees one view.
	shared := func() (lode.Store, error) { return store, nil }
	ds, err := qlode.NewReadDataset(dataset, shared)
	if err != nil {
		return nil, qlode.WrapInitError(err, dataset)
	}
	dr, err := qlode.NewPartitionReader(store)
	if err != nil {
		return nil, qlode.WrapInitError(err, dataset)
	}
	return &pruner{dataset: dataset, opts: opts, runs: runs, store: store, ds: ds, dr: dr}, nil
}

// keys lists the runs to consider, or names the one run Options selects.
func (p *pruner) keys(ctx context.Context) ([]reader.Key, []string, error) {
	sel := p.opts.Selector
	if p.opts.RunID == "" {
		return p.runs.ListRunKeys(ctx, sel)
	}
	return []reader.Key{{Source: sel.Source, Category: sel.Category, Day: sel.Day, RunID: p.opts.RunID}}, nil, nil
}

// prune removes the run at k if the policy selects it. due reports whether
// the policy selected it; a selected run that is kept has Pruned false and a
// reason in Kept.
func (p *pruner) prune(ctx context.Context, k reader.Key) (out Run, due bool, err error) {
	run, err := p.runs.FindRun(ctx, reader.Selector{Source: k.Source, Category: k.Category, Day: k.Day}, k.RunID)
	if err != nil {
		return Run{}, false, err
	}
	out = Run{Source: k.Source, Category: k.Category, Day: k.Day, RunID: k.RunID, Partition: run.Partition, State: run.State}
	threshold := p.opts.Policy.threshold(run.State)
	if threshold == nil {
		return out, false, nil
	}
	since, ok := ageSince(run)
	switch {
	case !ok:
		out.Kept = keptNoSince
		return out, true, nil
	case p.opts.Now.Sub(since) < *threshold:
		return out, false, nil
	}
	out.Since = since.UTC().Format(time.RFC3339Nano)
	if !recordKeyMatches(p.dataset, k, run) {
		out.Kept = keptMismatch
		return out, true, nil
	}
	return p.remove(ctx, k, out)
}

// remove deletes a selected run: a dry-run expiry first, so a run that
// holds the head loses nothing; then its sidecar files, its snapshots, and
// its records, in the order a rerun resumes.
func (p *pruner) remove(ctx context.Context, k reader.Key, out Run) (Run, bool, error) {
	preview, kept, err := p.expire(ctx, out.Partition, true)
	if kept != "" || err != nil {
		out.Kept = kept
		return out, true, err
	}
	cfg := qlode.Config{Dataset: p.dataset, Source: k.Source, Category: k.Category, Day: k.Day, RunID: k.RunID}
	sidecars, skipped, err := p.sidecarPaths(ctx, cfg, out.Partition)
	out.Skipped = skipped
	if err != nil {
		return out, true, err
	}
	out.SidecarObjects = len(sidecars)
	if p.opts.DryRun {
		out.Pruned = true
		out.Snapshots, out.Objects, out.Bytes = len(preview.Expired), preview.Objects, preview.Bytes
		return out, true, nil
	}
	if err := p.deleteAll(ctx, sidecars); err != nil {
		return out, true, err
	}
	report, kept, err := p.expire(ctx, out.Partition, false)
	out.Snapshots, out.Objects, out.Bytes = len(report.Expired), report.Objects, report.Bytes
	if kept != "" || err != nil {
		out.Kept = kept
		return out, true, err
	}
	// The start record goes first: a run without one is never listed, and
	// an end record left by an interruption here is ignored by readers.
	dir := qlode.RunRecordDir(cfg)
	if err := p.deleteAll(ctx, []string{dir + qlode.StartRecordName, dir + qlode.EndRecordName}); err != nil {
		return out, true, err
	}
	out.Pruned = true
	return out, true, nil
}

// expire expires every snapshot of the run's partition. It returns a reason
// when the run must be kept: Lode retains the head and mixed snapshots, and
// refuses while the head is uncommitted.
func (p *pruner) expire(ctx context.Context, partition string, dryRun bool) (*lode.ExpireReport, string, error) {
	report, err := p.ds.Expire(ctx, lode.ExpireRequest{Partition: partition, DryRun: dryRun})
	if report == nil {
		report = &lode.ExpireReport{}
	}
	switch {
	case errors.Is(err, lode.ErrHeadUncommitted):
		return report, keptPending, nil
	case err != nil:
		return report, "", fmt.Errorf("expire %s: %w", partition, err)
	case report.RetainedHead != "":
		return report, keptHead, nil
	case len(report.Mixed) > 0:
		return report, keptMixed, nil
	}
	return report, "", nil
}

func (p *pruner) deleteAll(ctx context.Context, paths []string) error {
	for _, path := range paths {
		if err := p.store.Delete(ctx, path); err != nil {
			return qlode.WrapWriteError(err, path)
		}
	}
	return nil
}
