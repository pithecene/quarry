package retention

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pithecene/lode/lode"

	qlode "github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/reader"
)

// Reasons a run the policy selected is kept.
const (
	keptMismatch  = "start record does not describe its key"
	keptNoSince   = "no recorded time to measure its age from"
	keptHead      = "holds the dataset head; a later prune removes it once another run commits"
	keptMixed     = "has snapshots with files outside its partition"
	keptPending   = "the dataset head is uncommitted or absent; rerun the prune"
	keptNoInstant = "a snapshot ID carries no write instant"
	keptCommitted = "committed during the prune; its records remain"
	keptInventory = "a snapshot's sidecar inventory is malformed"
)

// pruner removes one run at a time from one dataset.
type pruner struct {
	dataset string
	opts    Options
	runs    *reader.Reader
	store   lode.Store
	// ds is a writer handle: Lode retains only through writers, and a writer
	// opens on a dataset without snapshots, where a view would not.
	ds lode.Records[lode.D]
}

func newPruner(ctx context.Context, dataset string, factory lode.Factory[lode.RangeStore], opts Options) (*pruner, error) {
	if factory == nil {
		return nil, errors.New("prune: nil store factory")
	}
	store, err := factory()
	if err != nil {
		return nil, qlode.WrapInitError(err, dataset)
	}
	if store == nil {
		return nil, errors.New("prune: store factory returned a nil store")
	}
	// Every component uses this one store: a factory may return a separate
	// store per call, and the records read must be the ones deleted.
	runs, err := reader.New(dataset, lode.Use(store))
	if err != nil {
		return nil, err
	}
	ds, err := qlode.OpenDataset(ctx, dataset, store)
	if err != nil {
		return nil, qlode.WrapInitError(err, dataset)
	}
	return &pruner{dataset: dataset, opts: opts, runs: runs, store: store, ds: ds}, nil
}

// keys lists the runs to consider, or names the one run Options selects,
// with the orphaned end records under the same selectors.
func (p *pruner) keys(ctx context.Context) (reader.RunKeys, error) {
	sel := p.opts.Selector
	if p.opts.RunID == "" {
		return p.runs.ListRunKeys(ctx, sel)
	}
	key := reader.Key{Source: sel.Source, Category: sel.Category, Day: sel.Day, RunID: p.opts.RunID}
	dir := qlode.RunRecordPrefix(p.dataset, key.Source, key.Category, key.Day, key.RunID)
	listed, err := p.runs.ListRunKeys(ctx, sel)
	if err != nil {
		return reader.RunKeys{}, err
	}
	// Malformed keys under the selectors are reported as in a wide prune: a
	// requested run ID that the write path would encode differently, such
	// as "a b", appears among them unencoded, never under the run's key.
	out := reader.RunKeys{Runs: []reader.Key{key}, Malformed: listed.Malformed}
	for _, k := range listed.OrphanEnds {
		if k == dir+qlode.EndRecordName {
			out.OrphanEnds = append(out.OrphanEnds, k)
		}
	}
	return out, nil
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
	return p.remove(ctx, k, out, *threshold)
}

// remove deletes a selected run: a dry-run expiry first, so a run that
// holds the head loses nothing; then the previewed snapshots, and, once a fresh
// listing shows none remain, its sidecar files and records. The sidecar
// paths are gathered before the expiry, because the paths a run records
// live in the manifests the expiry deletes.
func (p *pruner) remove(ctx context.Context, k reader.Key, out Run, threshold time.Duration) (Run, bool, error) {
	preview, kept, err := p.expire(ctx, out.Partition, nil, true)
	if kept != "" || err != nil {
		out.Kept = kept
		return out, true, err
	}
	if out.State == reader.StateUnterminated {
		due := p.lastWriteDue(&out, preview.Expired, threshold)
		if !due || out.Kept != "" {
			return out, due, nil
		}
	}
	cfg := qlode.Config{Dataset: p.dataset, Source: k.Source, Category: k.Category, Day: k.Day, RunID: k.RunID}
	sidecars, skipped, err := p.sidecarPaths(ctx, cfg, out.Partition)
	out.Skipped = skipped
	if errors.Is(err, errMalformedInventory) {
		out.Kept = keptInventory
		return out, true, nil
	}
	if err != nil {
		return out, true, err
	}
	if p.opts.DryRun {
		out.SidecarObjects = len(sidecars)
		out.Pruned = true
		out.Snapshots, out.Objects, out.Bytes = len(preview.Expired), preview.Objects, preview.Bytes
		return out, true, nil
	}
	// Only the previewed snapshots are expired: one that committed since the
	// preview was not judged by it, so it must survive. A run with none has
	// nothing to expire, and a partition-only request would select a late
	// commit.
	if len(preview.Expired) > 0 {
		report, kept, err := p.expire(ctx, out.Partition, preview.Expired, false)
		out.Snapshots, out.Objects, out.Bytes = len(report.Expired), report.Objects, report.Bytes
		if kept != "" || err != nil {
			out.Kept = kept
			return out, true, err
		}
	}
	// A snapshot that committed after the preview remains, so the partition
	// is listed again; a snapshot there keeps the records.
	live, err := p.hasSnapshots(ctx, out.Partition)
	if err != nil {
		return out, true, err
	}
	if live {
		out.Kept = keptCommitted
		return out, true, nil
	}
	if err := p.deleteAll(ctx, sidecars); err != nil {
		return out, true, err
	}
	out.SidecarObjects = len(sidecars) // counted once deleted, so a kept run reports none
	// The start record goes first: a run without one is never listed, and
	// an end record left by an interruption here is ignored by readers.
	dir := qlode.RunRecordDir(cfg)
	if err := p.deleteAll(ctx, []string{dir + qlode.StartRecordName, dir + qlode.EndRecordName}); err != nil {
		return out, true, err
	}
	out.Pruned = true
	return out, true, nil
}

// lastWriteDue reports whether an unterminated run is still due once its age
// runs from its last write: such a run may be live although it started long
// ago. It moves out.Since to the last write when that is later, and sets
// out.Kept when a snapshot ID carries no write instant.
func (p *pruner) lastWriteDue(out *Run, ids []lode.DatasetSnapshotID, threshold time.Duration) bool {
	last, ok := lastWrite(ids)
	if !ok {
		out.Kept = keptNoInstant
		return true
	}
	if p.opts.Now.Sub(last) < threshold {
		return false
	}
	if since, _ := time.Parse(time.RFC3339Nano, out.Since); last.After(since) {
		out.Since = last.UTC().Format(time.RFC3339Nano)
	}
	return true
}

// expire expires the snapshots of the run's partition, narrowed to ids when
// they are given. It returns a reason when the run must be kept: Lode
// retains the head and mixed snapshots, and refuses while the head is
// uncommitted.
func (p *pruner) expire(ctx context.Context, partition string, ids []lode.DatasetSnapshotID, dryRun bool) (*lode.ExpireReport, string, error) {
	report, err := p.ds.Expire(ctx, lode.ExpireRequest{IDs: ids, Partition: partition, DryRun: dryRun})
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

// lastWrite is the latest instant at which one of the snapshots began its
// write, read from the generated snapshot ID form "<unix nanoseconds>-<nonce>"
// or the legacy form "<unix nanoseconds>" (Lode CONTRACT_CORE §Snapshot ID).
// It is false if any ID is in neither form. A run with no snapshot has
// written nothing, so its start alone dates it.
func lastWrite(ids []lode.DatasetSnapshotID) (time.Time, bool) {
	var last time.Time
	for _, id := range ids {
		t, ok := idInstant(string(id))
		if !ok {
			return time.Time{}, false
		}
		if t.After(last) {
			last = t
		}
	}
	return last, true
}

// idInstant parses a snapshot ID in exactly the generated or legacy form:
// a canonical decimal instant, optionally followed by "-" and a nonce of
// 16 lowercase hexadecimal digits.
func idInstant(id string) (time.Time, bool) {
	ns, nonce, hasNonce := strings.Cut(id, "-")
	n, err := strconv.ParseInt(ns, 10, 64)
	switch {
	case err != nil, n < 0, strconv.FormatInt(n, 10) != ns:
		return time.Time{}, false
	case hasNonce && !isNonce(nonce):
		return time.Time{}, false
	}
	return time.Unix(0, n), true
}

func isNonce(s string) bool {
	return len(s) == 16 && strings.Trim(s, "0123456789abcdef") == ""
}

// hasSnapshots reports whether any snapshot still has a manifest under the
// run's partition.
func (p *pruner) hasSnapshots(ctx context.Context, partition string) (bool, error) {
	parts, err := p.ds.ListPartitions(ctx, lode.PartitionListOptions{Prefix: partition})
	if errors.Is(err, lode.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, qlode.WrapReadError(err, partition)
	}
	for _, part := range parts {
		refs, err := p.ds.ListManifests(ctx, part.Path, lode.ManifestListOptions{Limit: 1})
		if err != nil {
			return false, qlode.WrapReadError(err, part.Path.String())
		}
		if len(refs) > 0 {
			return true, nil
		}
	}
	return false, nil
}
