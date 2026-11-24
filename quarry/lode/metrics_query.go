package lode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pithecene/lode/lode"
)

// ErrNoMetricsFound is returned when no metrics records exist in the dataset.
var ErrNoMetricsFound = errors.New("no metrics records found")

// clockStep is how far a writer's clock may step back between a run's end and its
// metrics record's commit, so that the commit still bounds the record's time.
const clockStep = time.Hour

// endRecordReads is how many end records a query reads at once.
const endRecordReads = 16

// QueryLatestMetrics returns the metrics of the dataset's run that ended last,
// from the run's end record or from a metrics record of an earlier release.
// Filters by runID and source if non-empty. Returns the fields of a metrics
// record, or ErrNoMetricsFound if no run has any. Nothing orders end records by
// time, so it reads each that matches.
func QueryLatestMetrics(ctx context.Context, factory lode.Factory[lode.RangeStore], dataset, runID, source string) (map[string]any, error) {
	ds, store, ok, err := openReadStore(ctx, dataset, factory)
	if err != nil {
		return nil, WrapReadError(err, "datasets/"+dataset)
	}
	filter := Config{Dataset: dataset, RunID: runID, Source: source}
	runs, err := endedRuns(ctx, store, filter)
	if err != nil {
		return nil, err
	}
	var latest map[string]any
	var latestAt time.Time
	reading := true // the end records
	consider := func(record map[string]any) error {
		at, err := time.Parse(time.RFC3339Nano, toString(record["ts"]))
		if err != nil {
			return fmt.Errorf("metrics of run %v: %w", record["run_id"], err)
		}
		// End records arrive in no order, so of two at one instant the greater run
		// ID is taken. A metrics record displaces no end record of its instant.
		if latest == nil || at.After(latestAt) || at.Equal(latestAt) && reading && toString(record["run_id"]) > toString(latest["run_id"]) {
			latest, latestAt = record, at
		}
		return nil
	}
	// The end record of a run is its source, and a metrics record only without one.
	ended := make(map[string]bool) // by record directory
	err = readEndMetrics(ctx, store, runs, func(run Config, record map[string]any) error {
		ended[RunRecordDir(run)] = true
		return consider(record)
	})
	if err != nil {
		return nil, err
	}
	reading = false
	// The end record of a named run decides, without its metrics records.
	if ok && (runID == "" || latest == nil) {
		snapshots, err := ds.Snapshots(ctx)
		if err != nil {
			return nil, WrapReadError(err, "quarry/snapshots")
		}
		// Metrics records keep whole seconds, so of two in one second the later in
		// Lode's snapshot order is taken.
		for _, snap := range slices.Backward(metricsSnapshots(snapshots, filter)) {
			// A metrics record's time precedes its commit, up to a clock step.
			if latest != nil && snap.CreatedAt.Add(clockStep).Before(latestAt) {
				continue
			}
			data, err := ds.Read(ctx, snap.ID)
			if err != nil {
				return nil, WrapReadError(err, fmt.Sprintf("quarry/snapshot/%s", snap.ID))
			}
			// Manifest path filtering is a coarse pre-filter; record fields
			// are authoritative (handles cumulative/multi-record snapshots).
			for _, record := range data {
				run := Config{
					Dataset: dataset, Source: toString(record["source"]), Category: toString(record["category"]),
					Day: toString(record["day"]), RunID: toString(record["run_id"]),
				}
				if !matchesMetricsRecord(record, filter) || ended[RunRecordDir(run)] {
					continue
				}
				if err := consider(record); err != nil {
					return nil, fmt.Errorf("quarry/snapshot/%s: %w", snap.ID, err)
				}
			}
		}
	}
	if latest == nil {
		return nil, ErrNoMetricsFound
	}
	return latest, nil
}

// readEndMetrics calls consider, one call at a time and in no order, with the
// metrics of each run's end record that has any.
func readEndMetrics(ctx context.Context, store lode.Store, runs []Config, consider func(Config, map[string]any) error) error {
	var next atomic.Int64
	var mu sync.Mutex // guards failure and consider
	var failure error
	var wg sync.WaitGroup
	for range min(endRecordReads, len(runs)) {
		wg.Go(func() {
			for i := next.Add(1) - 1; i < int64(len(runs)); i = next.Add(1) - 1 {
				rec, err := readEndFields(ctx, store, runs[i])
				var record map[string]any
				if err == nil {
					record, err = endMetrics(rec, runs[i])
				}
				mu.Lock()
				if err == nil && record != nil {
					err = consider(runs[i], record)
				}
				if failure == nil {
					failure = err
				}
				stop := failure != nil
				mu.Unlock()
				if stop {
					return
				}
			}
		})
	}
	wg.Wait()
	return failure
}

// endedRuns returns the runs that match filter, have a start record, and have
// an end record. An end record without a start record names no run.
func endedRuns(ctx context.Context, store lode.Store, filter Config) ([]Config, error) {
	prefix := RunRecordPrefix(filter.Dataset)
	if filter.Source != "" {
		prefix = RunRecordPrefix(filter.Dataset, filter.Source)
	}
	keys, err := store.List(ctx, prefix)
	if err != nil && !errors.Is(err, lode.ErrNotFound) {
		return nil, WrapReadError(err, prefix)
	}
	started := make(map[string]bool)
	for _, key := range keys {
		if dir, ok := strings.CutSuffix(key, "/"+StartRecordName); ok {
			started[dir+"/"] = true
		}
	}
	var runs []Config
	for _, key := range keys {
		cfg, ok := endRecordConfig(filter.Dataset, key)
		if ok && started[RunRecordDir(cfg)] && (filter.RunID == "" || cfg.RunID == filter.RunID) {
			runs = append(runs, cfg)
		}
	}
	return runs, nil
}

// endRecordConfig returns the run whose end record key names, and false for any
// other key.
func endRecordConfig(dataset, key string) (Config, bool) {
	rest, ok := strings.CutPrefix(key, RunRecordPrefix(dataset))
	parts := strings.Split(rest, "/")
	if !ok || len(parts) != 5 || parts[4] != EndRecordName {
		return Config{}, false
	}
	for i, part := range parts[:4] {
		value, err := url.PathUnescape(part)
		if err != nil {
			return Config{}, false
		}
		parts[i] = value
	}
	cfg := Config{Dataset: dataset, Source: parts[0], Category: parts[1], Day: parts[2], RunID: parts[3]}
	return cfg, validateRecordKey(cfg) == nil && RunRecordDir(cfg)+EndRecordName == key
}

// endMetrics returns the metrics of an end record as a metrics record has them,
// or nil when it has none. Both run IDs it carries must be the run's.
func endMetrics(rec map[string]json.RawMessage, cfg Config) (map[string]any, error) {
	if rec["metrics"] == nil {
		return nil, nil
	}
	var runID, endedAt string
	var record map[string]any
	for field, v := range map[string]any{"run_id": &runID, "ended_at": &endedAt, "metrics": &record} {
		if err := json.Unmarshal(rec[field], v); err != nil {
			return nil, fmt.Errorf("%w %s%s: %s: %w", errMalformedRecord, RunRecordDir(cfg), EndRecordName, field, err)
		}
	}
	if record == nil {
		return nil, nil
	}
	if runID != cfg.RunID || record["run_id"] != cfg.RunID {
		return nil, fmt.Errorf("%w %s%s: names run %q with metrics of run %v", errMalformedRecord, RunRecordDir(cfg), EndRecordName, runID, record["run_id"])
	}
	record["record_kind"] = RecordKindMetrics
	record["event_type"] = metricsEventType
	record["ts"] = endedAt
	record["source"], record["category"], record["day"] = cfg.Source, cfg.Category, cfg.Day
	return record, nil
}

// metricsSnapshots returns the snapshots with metrics records that can match
// filter, in Lode's snapshot order.
func metricsSnapshots(snapshots []*lode.Snapshot, filter Config) []*lode.Snapshot {
	var matched []*lode.Snapshot
	for _, snap := range snapshots {
		if isMetricsSnapshot(snap) && snapshotMatchesFilter(snap, "run_id", filter.RunID) && snapshotMatchesFilter(snap, "source", filter.Source) {
			matched = append(matched, snap)
		}
	}
	return matched
}

func matchesMetricsRecord(record lode.D, filter Config) bool {
	return record["record_kind"] == RecordKindMetrics &&
		(filter.RunID == "" || toString(record["run_id"]) == filter.RunID) &&
		(filter.Source == "" || toString(record["source"]) == filter.Source) &&
		(filter.Category == "" || toString(record["category"]) == filter.Category) &&
		(filter.Day == "" || toString(record["day"]) == filter.Day)
}

// toString converts a value to string, returning empty string for nil/non-string.
func toString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
