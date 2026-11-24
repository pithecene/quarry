package lode

import (
	"cmp"
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
		if ended, err := latestEndMetrics(ctx, store, runs, ""); err != nil || ended != nil {
			return ended, err
		}
	}
	var legacy map[string]any
	if ok {
		snapshots, err := ds.Snapshots(ctx)
		if err != nil {
			return nil, WrapReadError(err, "quarry/snapshots")
		}
		legacy, err = latestMetrics(ctx, ds, snapshots, filter)
		if err != nil {
			return nil, err
		}
	}
	latest := legacy
	if runID == "" {
		// No end record of an earlier day can be later.
		ended, err := latestEndMetrics(ctx, store, runs, toString(legacy["day"]))
		if err != nil {
			return nil, err
		}
		if ended != nil && !laterMetrics(legacy, ended) {
			latest = ended
		}
	}
	if latest == nil {
		return nil, ErrNoMetricsFound
	}
	return latest, nil
}

// laterMetrics reports whether a is of a later run than b, by its day and then
// by when it ended. A nil record is of no run.
func laterMetrics(a, b map[string]any) bool {
	if a == nil || b == nil {
		return a != nil
	}
	if byDay := cmp.Compare(toString(a["day"]), toString(b["day"])); byDay != 0 {
		return byDay > 0
	}
	endA, _ := time.Parse(time.RFC3339Nano, toString(a["ts"]))
	endB, _ := time.Parse(time.RFC3339Nano, toString(b["ts"]))
	return endA.After(endB)
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

// latestEndMetrics returns the metrics of the latest run whose end record has
// any, or nil. It reads the end records of one day at a time, the latest day
// first, and none of a day before since.
func latestEndMetrics(ctx context.Context, store lode.Store, byDay map[string][]Config, since string) (map[string]any, error) {
	for _, day := range slices.Backward(slices.Sorted(maps.Keys(byDay))) {
		if day < since {
			break
		}
		var latest map[string]any
		for _, cfg := range byDay[day] {
			rec, err := readEndFields(ctx, store, cfg)
			if err != nil {
				return nil, err
			}
			record, err := endMetrics(rec, cfg)
			if err != nil {
				return nil, err
			}
			if laterMetrics(record, latest) {
				latest = record
			}
		}
		if latest != nil {
			return latest, nil
		}
	}
	return nil, nil
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

// latestMetrics returns the latest metrics record that matches filter, or nil.
// Snapshots are ordered by when they committed, so every metrics snapshot of
// the latest day is read.
func latestMetrics(ctx context.Context, ds *lode.RecordView[lode.D], snapshots []*lode.Snapshot, filter Config) (map[string]any, error) {
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
	for _, day := range slices.Backward(slices.Sorted(maps.Keys(byDay))) {
		of := filter
		of.Day = day
		var latest map[string]any
		for _, snap := range byDay[day] {
			data, err := ds.Read(ctx, snap.ID)
			if err != nil {
				return nil, WrapReadError(err, fmt.Sprintf("quarry/snapshot/%s", snap.ID))
			}
			// Manifest path filtering is a coarse pre-filter; record fields
			// are authoritative (handles cumulative/multi-record snapshots).
			for _, record := range data {
				if matchesMetricsRecord(record, of) && laterMetrics(record, latest) {
					latest = record
				}
			}
		}
		if latest != nil {
			return latest, nil
		}
	}
	return nil, nil
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
