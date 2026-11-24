package lode

import (
	"context"
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
	ended, err := latestEndMetrics(ctx, store, filter)
	if err != nil && !errors.Is(err, ErrNoMetricsFound) {
		return nil, err
	}
	if !ok {
		return ended, err
	}
	snapshots, snapErr := ds.Snapshots(ctx)
	if snapErr != nil {
		return nil, WrapReadError(snapErr, "quarry/snapshots")
	}
	legacy, legacyErr := latestMetrics(ctx, ds, snapshots, filter)
	switch {
	case errors.Is(legacyErr, ErrNoMetricsFound):
		return ended, err
	case legacyErr != nil:
		return nil, legacyErr
	case ended == nil || metricsOrder(legacy) > metricsOrder(ended):
		return legacy, nil
	}
	return ended, nil
}

// metricsOrder orders metrics by the run's day and then by the second it ended.
func metricsOrder(record map[string]any) string {
	ts := toString(record["ts"])
	if at, err := time.Parse(time.RFC3339Nano, ts); err == nil {
		ts = at.UTC().Format(time.RFC3339)
	}
	return toString(record["day"]) + " " + ts
}

// latestEndMetrics returns the metrics of the latest run whose end record has
// any. It reads the end records of one day at a time, the latest day first.
func latestEndMetrics(ctx context.Context, store lode.Store, filter Config) (map[string]any, error) {
	prefix := RunRecordPrefix(filter.Dataset)
	if filter.Source != "" {
		prefix = RunRecordPrefix(filter.Dataset, filter.Source)
	}
	keys, err := store.List(ctx, prefix)
	if err != nil && !errors.Is(err, lode.ErrNotFound) {
		return nil, WrapReadError(err, prefix)
	}
	byDay := make(map[string][]Config)
	for _, key := range keys {
		cfg, ok := endRecordConfig(filter.Dataset, key)
		if ok && (filter.RunID == "" || cfg.RunID == filter.RunID) {
			byDay[cfg.Day] = append(byDay[cfg.Day], cfg)
		}
	}
	for _, day := range slices.Backward(slices.Sorted(maps.Keys(byDay))) {
		var latest map[string]any
		for _, cfg := range byDay[day] {
			record, err := readEndMetrics(ctx, store, cfg)
			if err != nil {
				return nil, err
			}
			if record != nil && (latest == nil || toString(record["ts"]) > toString(latest["ts"])) {
				latest = record
			}
		}
		if latest != nil {
			return latest, nil
		}
	}
	return nil, ErrNoMetricsFound
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

// readEndMetrics returns the metrics of the run's end record as a metrics record
// has them, or nil when the run has no end record or its record has none.
func readEndMetrics(ctx context.Context, store lode.Store, cfg Config) (map[string]any, error) {
	var end struct {
		EndedAt string         `json:"ended_at"`
		Metrics map[string]any `json:"metrics"`
	}
	found, err := readEndRecord(ctx, store, cfg, &end)
	if err != nil || !found || end.Metrics == nil {
		return nil, err
	}
	record := end.Metrics
	record["record_kind"] = RecordKindMetrics
	record["ts"] = end.EndedAt
	record["source"], record["category"], record["day"] = cfg.Source, cfg.Category, cfg.Day
	return record, nil
}

func latestMetrics(ctx context.Context, ds *lode.RecordView[lode.D], snapshots []*lode.Snapshot, filter Config) (map[string]any, error) {
	// Iterate in reverse (latest first) — snapshots are ordered by creation time
	for _, snap := range slices.Backward(snapshots) {
		if !isMetricsSnapshot(snap) {
			continue
		}
		if !snapshotMatchesFilter(snap, "run_id", filter.RunID) {
			continue
		}
		if !snapshotMatchesFilter(snap, "source", filter.Source) {
			continue
		}

		// Read snapshot data
		data, err := ds.Read(ctx, snap.ID)
		if err != nil {
			return nil, WrapReadError(err, fmt.Sprintf("quarry/snapshot/%s", snap.ID))
		}

		// Find a metrics record that passes record-level filters.
		// Manifest path filtering is a coarse pre-filter; record fields
		// are authoritative (handles cumulative/multi-record snapshots).
		for _, record := range data {
			if matchesMetricsRecord(record, filter) {
				return record, nil
			}
		}
	}

	return nil, ErrNoMetricsFound
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
