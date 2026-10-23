// Package retention removes whole runs from a dataset by their state and age
// (CONTRACT_LODE.md §Run Retention). Quarry decides which runs to remove,
// because only Quarry knows a run's outcome; Lode's Dataset.Expire removes
// the run's snapshots, and Quarry deletes the objects it owns: the run's
// sidecar files and run records.
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

// MinUnterminatedAge is the least age at which an unterminated run may be
// pruned. Storage cannot tell a live run from a killed one, so the age is
// what presumes the run dead; it must exceed the longest run.
const MinUnterminatedAge = 24 * time.Hour

// ErrInvalidOptions is returned for options that cannot select runs safely.
var ErrInvalidOptions = errors.New("invalid prune options")

// Policy gives, per run state, the age at which a run in that state is
// pruned. A nil field keeps every run in that state.
type Policy struct {
	Succeeded    *time.Duration
	Failed       *time.Duration
	Unterminated *time.Duration
}

func (p Policy) threshold(s reader.State) *time.Duration {
	switch s {
	case reader.StateSucceeded:
		return p.Succeeded
	case reader.StateFailed:
		return p.Failed
	case reader.StateUnterminated:
		return p.Unterminated
	}
	return nil
}

// Options controls a prune.
type Options struct {
	// Selector narrows the runs considered to a key prefix.
	Selector reader.Selector
	// RunID narrows them to one run; Selector must then name its source,
	// category, and day.
	RunID  string
	Policy Policy
	// SweepGrace, when positive, runs Dataset.Sweep with this grace period
	// after the runs are pruned. Zero skips the sweep.
	SweepGrace time.Duration
	// DryRun reports what would be removed and removes nothing.
	DryRun bool
	// Now is the instant ages are measured at.
	Now time.Time
}

func (o Options) validate() error {
	p := o.Policy
	switch {
	case p.Succeeded == nil && p.Failed == nil && p.Unterminated == nil:
		return fmt.Errorf("%w: no run state is selected", ErrInvalidOptions)
	case p.Succeeded != nil && *p.Succeeded < 0, p.Failed != nil && *p.Failed < 0:
		return fmt.Errorf("%w: an age must not be negative", ErrInvalidOptions)
	case p.Unterminated != nil && *p.Unterminated < MinUnterminatedAge:
		return fmt.Errorf("%w: the unterminated age must be at least %s", ErrInvalidOptions, MinUnterminatedAge)
	case o.SweepGrace < 0:
		return fmt.Errorf("%w: the sweep grace period must not be negative", ErrInvalidOptions)
	case o.RunID != "" && (o.Selector.Source == "" || o.Selector.Category == "" || o.Selector.Day == ""):
		return fmt.Errorf("%w: a run ID requires its source, category, and day", ErrInvalidOptions)
	case o.Now.IsZero():
		return fmt.Errorf("%w: no current time", ErrInvalidOptions)
	}
	return nil
}

// Run is one run the prune removed, would remove in a dry run, or kept
// although the policy selected it.
type Run struct {
	Source    string       `json:"source" yaml:"source"`
	Category  string       `json:"category" yaml:"category"`
	Day       string       `json:"day" yaml:"day"`
	RunID     string       `json:"run_id" yaml:"run_id"`
	Partition string       `json:"partition" yaml:"partition"`
	State     reader.State `json:"state" yaml:"state"`
	// Since is the instant the run's age is measured from.
	Since string `json:"since" yaml:"since"`
	// Pruned reports that the run was removed (in a dry run, would be).
	Pruned bool `json:"pruned" yaml:"pruned"`
	// Snapshots, Objects, and Bytes are what Dataset.Expire reported.
	Snapshots int   `json:"snapshots" yaml:"snapshots"`
	Objects   int   `json:"objects" yaml:"objects"`
	Bytes     int64 `json:"bytes" yaml:"bytes"`
	// SidecarObjects counts the sidecar files and their metadata companions
	// deleted.
	SidecarObjects int `json:"sidecar_objects" yaml:"sidecar_objects"`
	// Kept gives the reason a selected run was not removed.
	Kept string `json:"kept,omitempty" yaml:"kept,omitempty"`
	// Skipped lists sidecar paths recorded for the run that lie outside its
	// sidecar prefix; they are left in place.
	Skipped []string `json:"skipped,omitempty" yaml:"skipped,omitempty"`
}

// SweepReport summarizes the Dataset.Sweep that follows the runs.
type SweepReport struct {
	Settled     int      `json:"settled" yaml:"settled"`
	Objects     int      `json:"objects" yaml:"objects"`
	Bytes       int64    `json:"bytes" yaml:"bytes"`
	Unsized     int      `json:"unsized" yaml:"unsized"`
	Orphans     int      `json:"orphans" yaml:"orphans"`
	Unparseable []string `json:"unparseable,omitempty" yaml:"unparseable,omitempty"`
}

// Report summarizes a prune. Runs lists only the runs the policy selected;
// the others are counted in NotDue.
type Report struct {
	Dataset        string `json:"dataset" yaml:"dataset"`
	DryRun         bool   `json:"dry_run" yaml:"dry_run"`
	RunsScanned    int    `json:"runs_scanned" yaml:"runs_scanned"`
	NotDue         int    `json:"not_due" yaml:"not_due"`
	RunsPruned     int    `json:"runs_pruned" yaml:"runs_pruned"`
	RunsKept       int    `json:"runs_kept" yaml:"runs_kept"`
	Snapshots      int    `json:"snapshots" yaml:"snapshots"`
	Objects        int    `json:"objects" yaml:"objects"`
	Bytes          int64  `json:"bytes" yaml:"bytes"`
	SidecarObjects int    `json:"sidecar_objects" yaml:"sidecar_objects"`
	Runs           []Run  `json:"runs" yaml:"runs"`
	// OrphanEndRecords counts end records without a start record that the
	// prune deleted (in a dry run, would delete).
	OrphanEndRecords int `json:"orphan_end_records" yaml:"orphan_end_records"`
	// Unparseable lists start-record keys the write path could not have
	// produced. Such runs are not addressed.
	Unparseable []string     `json:"unparseable,omitempty" yaml:"unparseable,omitempty"`
	Sweep       *SweepReport `json:"sweep,omitempty" yaml:"sweep,omitempty"`
}

func (r *Report) add(run Run) {
	r.Runs = append(r.Runs, run)
	if !run.Pruned {
		r.RunsKept++
	} else {
		r.RunsPruned++
	}
	r.Snapshots += run.Snapshots
	r.Objects += run.Objects
	r.Bytes += run.Bytes
	r.SidecarObjects += run.SidecarObjects
}

// Prune removes the runs the policy selects, in key order, then sweeps when
// asked. Each run is removed in an order that a
// rerun resumes (CONTRACT_LODE.md §Run Retention): sidecar files, then the
// run's snapshots, then its start record and its end record. A store error
// terminates the prune and returns the partial report.
func Prune(ctx context.Context, dataset string, factory lode.StoreFactory, opts Options) (Report, error) {
	report := Report{Dataset: dataset, DryRun: opts.DryRun, Runs: []Run{}}
	if err := opts.validate(); err != nil {
		return report, err
	}
	p, err := newPruner(dataset, factory, opts)
	if err != nil {
		return report, err
	}
	keys, err := p.keys(ctx)
	if err != nil {
		return report, err
	}
	report.Unparseable = keys.Malformed
	for _, k := range keys.Runs {
		run, due, err := p.prune(ctx, k)
		switch {
		case errors.Is(err, reader.ErrRunNotFound):
			continue // removed since the listing
		case !due && err != nil:
			return report, err
		}
		report.RunsScanned++
		if !due {
			report.NotDue++
			continue
		}
		report.add(run)
		if err != nil {
			return report, err
		}
	}
	// An end record whose start record is gone names no run: an earlier
	// prune stopped between deleting the two, or the run never started.
	if !opts.DryRun {
		if err := p.deleteAll(ctx, keys.OrphanEnds); err != nil {
			return report, err
		}
	}
	report.OrphanEndRecords = len(keys.OrphanEnds)
	if opts.SweepGrace > 0 {
		sweep, err := p.ds.Sweep(ctx, lode.SweepRequest{Grace: opts.SweepGrace, DryRun: opts.DryRun})
		report.Sweep = sweepSummary(sweep)
		if err != nil {
			return report, fmt.Errorf("sweep: %w", err)
		}
	}
	return report, nil
}

func sweepSummary(r *lode.SweepReport) *SweepReport {
	if r == nil {
		return &SweepReport{}
	}
	return &SweepReport{
		Settled: len(r.Settled), Objects: r.Objects, Bytes: r.Bytes,
		Unsized: r.Unsized, Orphans: r.Orphans, Unparseable: r.Unparseable,
	}
}

// ageSince is the instant a run's age is measured from: its end for a
// terminated run, and its start for an unterminated one. A backfilled run
// has no recorded start, so the end of its partition day stands in.
func ageSince(run reader.Run) (time.Time, bool) {
	if run.EndedAt != nil {
		t, err := time.Parse(time.RFC3339Nano, *run.EndedAt)
		return t, err == nil
	}
	if run.StartedAt != nil {
		t, err := time.Parse(time.RFC3339Nano, *run.StartedAt)
		return t, err == nil
	}
	day, err := time.Parse(time.DateOnly, run.Day)
	return day.Add(24 * time.Hour), err == nil
}

// recordKeyMatches reports whether a run's start record describes the key it
// was read from, so a record can never direct deletion at another run.
func recordKeyMatches(dataset string, k reader.Key, run reader.Run) bool {
	cfg := qlode.Config{Dataset: dataset, Source: k.Source, Category: k.Category, Day: k.Day, RunID: k.RunID}
	return run.Dataset == dataset && run.Source == k.Source && run.Category == k.Category &&
		run.Day == k.Day && run.RunID == k.RunID && run.Partition == qlode.PartitionPrefix(cfg)
}
