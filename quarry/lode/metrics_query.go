package lode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/pithecene/lode/lode"
)

// ErrNoMetricsFound is returned when no metrics records exist in the dataset.
var ErrNoMetricsFound = errors.New("no metrics records found")

// QueryLatestMetrics returns the metrics of the dataset's latest run, by its day
// and then by when it ended, from the run's end record or from a metrics record
// of an earlier release. Filters by runID and source if non-empty. Returns the
// fields of a metrics record, or ErrNoMetricsFound if no run has any.
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
	// The end record of a named run decides, without its metrics records.
	if runID != "" {
		if record, err := latestOfDays(ctx, store, nil, runs, nil, filter); err != nil || record != nil {
			return record, err
		}
	}
	var legacy map[string][]*lode.Snapshot
	if ok {
		snapshots, err := ds.Snapshots(ctx)
		if err != nil {
			return nil, WrapReadError(err, "quarry/snapshots")
		}
		legacy = metricsSnapshots(snapshots, filter)
	}
	record, err := latestOfDays(ctx, store, ds, runs, legacy, filter)
	if err == nil && record == nil {
		return nil, ErrNoMetricsFound
	}
	return record, err
}

// latestOfDays returns the metrics of the run that ended last on the latest day
// that has any, or nil. It reads one day at a time, the latest day first. The
// end record of a run is its source, and a metrics record only without one.
func latestOfDays(ctx context.Context, store lode.Store, ds *lode.RecordView[lode.D], runs map[string][]Config, legacy map[string][]*lode.Snapshot, filter Config) (map[string]any, error) {
	days := slices.Concat(slices.Collect(maps.Keys(runs)), slices.Collect(maps.Keys(legacy)))
	slices.Sort(days)
	for _, day := range slices.Backward(slices.Compact(days)) {
		var latest map[string]any
		var latestAt time.Time
		consider := func(record map[string]any) error {
			at, err := time.Parse(time.RFC3339Nano, toString(record["ts"]))
			if err != nil {
				return fmt.Errorf("metrics of run %v: %w", record["run_id"], err)
			}
			if latest == nil || at.After(latestAt) {
				latest, latestAt = record, at
			}
			return nil
		}
		ended := make(map[string]bool) // by record directory
		for _, cfg := range runs[day] {
			rec, err := readEndFields(ctx, store, cfg)
			if err != nil {
				return nil, err
			}
			record, err := endMetrics(rec, cfg)
			if err != nil {
				return nil, err
			}
			if record != nil {
				ended[RunRecordDir(cfg)] = true
				if err := consider(record); err != nil {
					return nil, err
				}
			}
		}
		// Metrics records keep whole seconds, so of two in one second the later
		// commit is taken.
		of := filter
		of.Day = day
		for _, snap := range slices.Backward(legacy[day]) {
			data, err := ds.Read(ctx, snap.ID)
			if err != nil {
				return nil, WrapReadError(err, fmt.Sprintf("quarry/snapshot/%s", snap.ID))
			}
			// Manifest path filtering is a coarse pre-filter; record fields
			// are authoritative (handles cumulative/multi-record snapshots).
			for _, record := range data {
				run := Config{
					Dataset: filter.Dataset, Source: toString(record["source"]), Category: toString(record["category"]),
					Day: toString(record["day"]), RunID: toString(record["run_id"]),
				}
				if !matchesMetricsRecord(record, of) || ended[RunRecordDir(run)] {
					continue
				}
				if err := consider(record); err != nil {
					return nil, fmt.Errorf("quarry/snapshot/%s: %w", snap.ID, err)
				}
			}
		}
		if latest != nil {
			return latest, nil
		}
	}
	return nil, nil
}

// endedRuns returns the runs that match filter, have a start record, and have
// an end record, by day. An end record without a start record names no run.
func endedRuns(ctx context.Context, store lode.Store, filter Config) (map[string][]Config, error) {
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
	byDay := make(map[string][]Config)
	for _, key := range keys {
		cfg, ok := endRecordConfig(filter.Dataset, key)
		if ok && started[RunRecordDir(cfg)] && (filter.RunID == "" || cfg.RunID == filter.RunID) {
			byDay[cfg.Day] = append(byDay[cfg.Day], cfg)
		}
	}
	return byDay, nil
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
// filter, by the days of their records, each in commit order.
func metricsSnapshots(snapshots []*lode.Snapshot, filter Config) map[string][]*lode.Snapshot {
	byDay := make(map[string][]*lode.Snapshot)
	for _, snap := range snapshots {
		if !isMetricsSnapshot(snap) || !snapshotMatchesFilter(snap, "run_id", filter.RunID) || !snapshotMatchesFilter(snap, "source", filter.Source) {
			continue
		}
		days := make(map[string]bool)
		for _, f := range snap.Objects {
			for segment := range strings.SplitSeq(f.Path, "/") {
				if day, ok := strings.CutPrefix(segment, "day="); ok {
					days[day] = true
				}
			}
		}
		for day := range days {
			byDay[day] = append(byDay[day], snap)
		}
	}
	return byDay
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
