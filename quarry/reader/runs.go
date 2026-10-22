// Package reader reads Quarry's own storage layout: it finds runs from their
// run records (CONTRACT_RUN.md §Run Records) and reads a run's events from the
// Lode partitions its start record names. It never enumerates partitions to
// find runs.
package reader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/pithecene-io/lode/lode"

	"github.com/pithecene/quarry/iox"
	qlode "github.com/pithecene/quarry/lode"
)

// State is a run's state as storage proves it.
type State string

const (
	// StateSucceeded is a run whose end record has outcome "success".
	StateSucceeded State = "succeeded"
	// StateFailed is a run whose end record has any other outcome.
	StateFailed State = "failed"
	// StateUnterminated is a run with no end record: a live run, a killed
	// process, or a run whose end record could not be written. Storage
	// cannot tell these apart, so it is never presented as running.
	StateUnterminated State = "unterminated"
)

var (
	// ErrSelectorOrder is returned when a selector skips an earlier one
	// (a category without a source, or a day without a category).
	ErrSelectorOrder = errors.New("selectors must be given in order: source, category, day")
	// ErrInvalidCursor is returned for a cursor that this listing did not
	// produce.
	ErrInvalidCursor = errors.New("invalid cursor")
	// ErrInvalidState is returned for an unknown state filter.
	ErrInvalidState = errors.New("invalid state: must be succeeded, failed, or unterminated")
)

// Selector narrows a listing to a key prefix. Fields are leading selectors:
// Category requires Source, and Day requires Category.
type Selector struct {
	Source   string
	Category string
	Day      string
}

// ListOptions controls a run listing.
type ListOptions struct {
	// State keeps only runs in this state; empty keeps all.
	State State
	// After resumes after the cursor a previous page returned.
	After string
	// Limit bounds the runs returned; zero means no limit.
	Limit int
}

// Run is one run as its records describe it.
type Run struct {
	RunID       string  `json:"run_id"`
	JobID       *string `json:"job_id,omitempty"`
	ParentRunID *string `json:"parent_run_id,omitempty"`
	Attempt     int     `json:"attempt"`
	Dataset     string  `json:"dataset"`
	Source      string  `json:"source"`
	Category    string  `json:"category"`
	Day         string  `json:"day"`
	Partition   string  `json:"partition"`
	StartedAt   string  `json:"started_at"`
	State       State   `json:"state"`
	// The fields below come from the end record and are null for an
	// unterminated run.
	EndedAt    *string `json:"ended_at"`
	Outcome    *string `json:"outcome"`
	Terminal   *string `json:"terminal"`
	Message    *string `json:"message,omitempty"`
	EventCount *int64  `json:"event_count"`
}

// Page is one page of a run listing. Next is empty on the last page.
type Page struct {
	Runs []Run  `json:"runs"`
	Next string `json:"next,omitempty"`
}

// Reader reads one dataset.
type Reader struct {
	dataset string
	store   lode.Store // one store for listing runs and reading their events
}

// New returns a reader for the dataset stored behind factory.
func New(dataset string, factory lode.StoreFactory) (*Reader, error) {
	// The dataset component is used unencoded; reject unsafe IDs as the
	// write path does.
	if err := qlode.ValidateRunKey(dataset, "x", "x", "x"); err != nil {
		return nil, err
	}
	if factory == nil {
		return nil, errors.New("reader: nil store factory")
	}
	store, err := factory()
	if err != nil {
		return nil, qlode.WrapReadError(err, "datasets/"+dataset)
	}
	if store == nil {
		return nil, errors.New("reader: store factory returned a nil store")
	}
	return &Reader{dataset: dataset, store: store}, nil
}

// ListRuns lists the runs under the selector's key prefix, ordered by key
// (source, category, day, run ID), one page at a time.
func (r *Reader) ListRuns(ctx context.Context, sel Selector, opts ListOptions) (Page, error) {
	prefix, err := r.prefix(sel)
	if err != nil {
		return Page{}, err
	}
	if err := opts.State.validate(); err != nil {
		return Page{}, err
	}
	if opts.After != "" && !r.isStartKeyUnder(opts.After, prefix) {
		return Page{}, fmt.Errorf("%w: %q is not a run start-record key under %q", ErrInvalidCursor, opts.After, prefix)
	}

	keys, err := r.store.List(ctx, prefix)
	if err != nil {
		if errors.Is(err, lode.ErrNotFound) {
			return Page{Runs: []Run{}}, nil
		}
		return Page{}, qlode.WrapReadError(err, prefix)
	}
	// Exactly one start record per run, so a page cut on these keys is a
	// run boundary. Store listings have no guaranteed order: sort.
	starts := make([]string, 0, len(keys))
	for _, k := range keys {
		if strings.HasSuffix(k, "/"+qlode.StartRecordName) && k > opts.After {
			starts = append(starts, k)
		}
	}
	sort.Strings(starts)

	page := Page{Runs: []Run{}}
	for i, key := range starts {
		run, err := r.readRun(ctx, key)
		if err != nil {
			return Page{}, err
		}
		if opts.State != "" && run.State != opts.State {
			continue
		}
		page.Runs = append(page.Runs, run)
		if opts.Limit > 0 && len(page.Runs) == opts.Limit {
			more, err := r.anyMatch(ctx, starts[i+1:], opts.State)
			if err != nil {
				return Page{}, err
			}
			if more {
				page.Next = key
			}
			break
		}
	}
	return page, nil
}

// anyMatch reports whether any of the remaining start keys is a run in the
// wanted state, so a page carries a cursor only when another match exists.
// Without a state filter every remaining key matches, and nothing is read.
func (r *Reader) anyMatch(ctx context.Context, rest []string, state State) (bool, error) {
	if state == "" {
		return len(rest) > 0, nil
	}
	for _, key := range rest {
		run, err := r.readRun(ctx, key)
		if err != nil {
			return false, err
		}
		if run.State == state {
			return true, nil
		}
	}
	return false, nil
}

// isStartKeyUnder reports whether key has the shape of a start-record key,
// datasets/<dataset>/runs/<source>/<category>/<day>/<run_id>/start.json,
// and lies under prefix.
func (r *Reader) isStartKeyUnder(key, prefix string) bool {
	root := qlode.RunRecordPrefix(r.dataset)
	if !strings.HasPrefix(key, prefix) || !strings.HasPrefix(key, root) {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(key, root), "/")
	if len(parts) != 5 || parts[4] != qlode.StartRecordName {
		return false
	}
	for _, p := range parts[:4] {
		if p == "" || p == "." || p == ".." {
			return false
		}
	}
	return true
}

// prefix builds the listing prefix, enforcing selector order.
func (r *Reader) prefix(sel Selector) (string, error) {
	if (sel.Category != "" && sel.Source == "") || (sel.Day != "" && sel.Category == "") {
		return "", ErrSelectorOrder
	}
	var parts []string
	for _, c := range []struct{ name, value string }{{"source", sel.Source}, {"category", sel.Category}, {"day", sel.Day}} {
		if c.value == "" {
			break
		}
		if err := qlode.ValidateKeyComponent(c.name, c.value); err != nil {
			return "", err
		}
		parts = append(parts, c.value)
	}
	return qlode.RunRecordPrefix(r.dataset, parts...), nil
}

func (s State) validate() error {
	switch s {
	case "", StateSucceeded, StateFailed, StateUnterminated:
		return nil
	}
	return fmt.Errorf("%w: %q", ErrInvalidState, s)
}

// readRun reads a run's start record and, if present, its end record.
func (r *Reader) readRun(ctx context.Context, startKey string) (Run, error) {
	var start qlode.StartRecord
	if err := r.getJSON(ctx, startKey, &start); err != nil {
		return Run{}, err
	}
	run := Run{
		RunID: start.RunID, JobID: start.JobID, ParentRunID: start.ParentRunID, Attempt: start.Attempt,
		Dataset: start.Dataset, Source: start.Source, Category: start.Category, Day: start.Day,
		Partition: start.Partition, StartedAt: start.StartedAt, State: StateUnterminated,
	}

	endKey := strings.TrimSuffix(startKey, qlode.StartRecordName) + qlode.EndRecordName
	var end qlode.EndRecord
	err := r.getJSON(ctx, endKey, &end)
	switch {
	case errors.Is(err, lode.ErrNotFound):
		return run, nil // no terminal evidence: a normal state
	case err != nil:
		return Run{}, err
	}
	run.EndedAt = &end.EndedAt
	run.Outcome = &end.Outcome
	run.Terminal = end.Terminal
	if end.Message != "" {
		run.Message = &end.Message
	}
	run.EventCount = &end.EventCount
	run.State = StateFailed
	if end.Outcome == "success" {
		run.State = StateSucceeded
	}
	return run, nil
}

func (r *Reader) getJSON(ctx context.Context, key string, v any) error {
	rc, err := r.store.Get(ctx, key)
	if err != nil {
		if errors.Is(err, lode.ErrNotFound) {
			return err
		}
		return qlode.WrapReadError(err, key)
	}
	defer iox.DiscardClose(rc)
	data, err := io.ReadAll(rc)
	if err != nil {
		return qlode.WrapReadError(err, key)
	}
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("run record %s: %w", key, err)
	}
	return nil
}

// ErrRunNotFound is returned when no start record exists for a run.
var ErrRunNotFound = errors.New("run not found")

// FindRun reads one run. The selector must name its source, category, and
// day: those locate the run's records directly, without a listing.
func (r *Reader) FindRun(ctx context.Context, sel Selector, runID string) (Run, error) {
	if sel.Source == "" || sel.Category == "" || sel.Day == "" || runID == "" {
		return Run{}, fmt.Errorf("%w: source, category, day, and run ID are required", ErrSelectorOrder)
	}
	if _, err := r.prefix(sel); err != nil {
		return Run{}, err
	}
	if err := qlode.ValidateKeyComponent("run_id", runID); err != nil {
		return Run{}, err
	}
	key := qlode.RunRecordPrefix(r.dataset, sel.Source, sel.Category, sel.Day, runID) + qlode.StartRecordName
	run, err := r.readRun(ctx, key)
	if errors.Is(err, lode.ErrNotFound) {
		return Run{}, fmt.Errorf("%w: %s", ErrRunNotFound, runID)
	}
	return run, err
}
