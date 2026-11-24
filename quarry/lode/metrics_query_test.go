package lode

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pithecene/lode/lode"

	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/types"
)

// toInt64 converts a value to int64 for test assertions on raw map fields.
func toInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case float64:
		return int64(n)
	case int:
		return int64(n)
	default:
		return 0
	}
}

// sharedFactory returns a StoreFactory that always returns the given store.
// This allows write and read datasets to share the same in-memory state.
func sharedFactory(store lode.RangeStore) lode.Factory[lode.RangeStore] {
	return lode.Use(store)
}

// metadataOf decodes a snapshot's metadata as a map, so values have the
// types a JSON round trip gives them.
func metadataOf(t *testing.T, snap *lode.Snapshot) map[string]any {
	t.Helper()
	var meta map[string]any
	if err := snap.Metadata.Decode(&meta); err != nil {
		t.Fatalf("decode metadata of %s: %v", snap.ID, err)
	}
	return meta
}

// readView opens a view of the quarry dataset behind factory, as the read
// path does.
func readView(ctx context.Context, factory lode.Factory[lode.RangeStore]) (*lode.RecordView[lode.D], error) {
	store, err := factory()
	if err != nil {
		return nil, err
	}
	return OpenView(ctx, "quarry", store)
}

func TestQueryLatestMetrics_WriteAndRead(t *testing.T) {
	t.Parallel()

	store := lode.NewMemory()
	factory := sharedFactory(store)

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-001",
	}

	// Write via LodeClient
	client, err := NewLodeClientWithFactory(t.Context(), cfg, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	snap := metrics.Snapshot{
		RunsStarted:           1,
		RunsCompleted:         1,
		EventsReceived:        42,
		EventsPersisted:       40,
		EventsDropped:         2,
		DroppedByType:         map[string]int64{"debug": 2},
		ExecutorLaunchSuccess: 1,
		LodeWriteSuccess:      10,
		Policy:                "strict",
		Executor:              "executor.js",
		StorageBackend:        "fs",
		RunID:                 "run-001",
		JobID:                 "job-xyz",
	}

	completedAt := time.Date(2026, 2, 3, 15, 0, 0, 0, time.UTC)
	if err := writeLegacyMetrics(t.Context(), client, snap, completedAt); err != nil {
		t.Fatalf("write metrics: %v", err)
	}

	// Read via QueryLatestMetrics

	record, err := QueryLatestMetrics(t.Context(), factory, "quarry", "", "")
	if err != nil {
		t.Fatalf("QueryLatestMetrics failed: %v", err)
	}

	// Verify round-trip fidelity on raw map fields
	if v := toInt64(record["runs_started_total"]); v != 1 {
		t.Errorf("runs_started_total = %d, want 1", v)
	}
	if v := toInt64(record["runs_completed_total"]); v != 1 {
		t.Errorf("runs_completed_total = %d, want 1", v)
	}
	if v := toInt64(record["events_received_total"]); v != 42 {
		t.Errorf("events_received_total = %d, want 42", v)
	}
	if v := toInt64(record["events_persisted_total"]); v != 40 {
		t.Errorf("events_persisted_total = %d, want 40", v)
	}
	if v := toInt64(record["events_dropped_total"]); v != 2 {
		t.Errorf("events_dropped_total = %d, want 2", v)
	}
	if v := toInt64(record["executor_launch_success_total"]); v != 1 {
		t.Errorf("executor_launch_success_total = %d, want 1", v)
	}
	if v := toInt64(record["lode_write_success_total"]); v != 10 {
		t.Errorf("lode_write_success_total = %d, want 10", v)
	}
	if v := toString(record["policy"]); v != "strict" {
		t.Errorf("policy = %q, want %q", v, "strict")
	}
	if v := toString(record["executor"]); v != "executor.js" {
		t.Errorf("executor = %q, want %q", v, "executor.js")
	}
	if v := toString(record["storage_backend"]); v != "fs" {
		t.Errorf("storage_backend = %q, want %q", v, "fs")
	}
	if v := toString(record["run_id"]); v != "run-001" {
		t.Errorf("run_id = %q, want %q", v, "run-001")
	}
	if v := toString(record["job_id"]); v != "job-xyz" {
		t.Errorf("job_id = %q, want %q", v, "job-xyz")
	}
	if record["dropped_by_type"] == nil {
		t.Fatal("dropped_by_type should not be nil")
	}
}

// QueryLatestMetrics returns the latest metrics record that matches its
// run ID and source filters. Each write i records RunsStarted i+1, one
// minute after the previous write.
func TestQueryLatestMetrics_Filters(t *testing.T) {
	t.Parallel()
	type write struct{ source, runID string }
	cases := []struct {
		name          string
		writes        []write
		runID, source string
		wantRunID     string
		wantStarted   int64
	}{
		{"latest of several runs", []write{{"test-source", "run-001"}, {"test-source", "run-002"}, {"test-source", "run-003"}}, "", "", "run-003", 3},
		{"by run ID", []write{{"test-source", "run-001"}, {"test-source", "run-002"}, {"test-source", "run-003"}}, "run-002", "", "run-002", 2},
		{"by source", []write{{"alpha", "run-001"}, {"beta", "run-001"}}, "", "alpha", "run-001", 1},
		// source=alpha must not match source=alphabet.
		{"source is not a substring match", []write{{"alpha", "run-001"}, {"alphabet", "run-001"}}, "", "alpha", "run-001", 1},
	}
	completedAt := time.Date(2026, 2, 3, 15, 0, 0, 0, time.UTC)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			factory := sharedFactory(lode.NewMemory())
			for i, w := range tc.writes {
				cfg := Config{Dataset: "quarry", Source: w.source, Category: "test-category", Day: "2026-02-03", RunID: w.runID}
				client, err := NewLodeClientWithFactory(t.Context(), cfg, factory)
				if err != nil {
					t.Fatalf("NewLodeClientWithFactory failed: %v", err)
				}
				snap := metrics.Snapshot{RunsStarted: int64(i + 1), RunID: w.runID, Policy: "strict", Executor: "executor.js", StorageBackend: "fs"}
				if err := writeLegacyMetrics(t.Context(), client, snap, completedAt.Add(time.Duration(i)*time.Minute)); err != nil {
					t.Fatalf("WriteMetrics for %+v failed: %v", w, err)
				}
			}
			record, err := QueryLatestMetrics(t.Context(), factory, "quarry", tc.runID, tc.source)
			if err != nil {
				t.Fatalf("QueryLatestMetrics failed: %v", err)
			}
			if v := toString(record["run_id"]); v != tc.wantRunID {
				t.Errorf("run_id = %q, want %q", v, tc.wantRunID)
			}
			if v := toInt64(record["runs_started_total"]); v != tc.wantStarted {
				t.Errorf("runs_started_total = %d, want %d", v, tc.wantStarted)
			}
		})
	}
}

func TestQueryLatestMetrics_NoMetrics(t *testing.T) {
	t.Parallel()

	store := lode.NewMemory()
	factory := sharedFactory(store)

	_, err := QueryLatestMetrics(t.Context(), factory, "quarry", "", "")
	if err == nil {
		t.Fatal("expected error for empty dataset, got nil")
	}
	if !errors.Is(err, ErrNoMetricsFound) {
		t.Errorf("expected ErrNoMetricsFound, got: %v", err)
	}
}

// TestQueryLatestMetrics_RunIDSubstringNoCollision verifies that filtering
// by run_id=run-1 does not match run_id=run-10 (substring false positive).
func TestQueryLatestMetrics_RunIDSubstringNoCollision(t *testing.T) {
	t.Parallel()

	store := lode.NewMemory()
	factory := sharedFactory(store)

	completedAt := time.Date(2026, 2, 3, 15, 0, 0, 0, time.UTC)

	// Write metrics for run-1 and run-10
	for i, runID := range []string{"run-1", "run-10"} {
		cfg := Config{
			Dataset:  "quarry",
			Source:   "test-source",
			Category: "test-category",
			Day:      "2026-02-03",
			RunID:    runID,
		}

		client, err := NewLodeClientWithFactory(t.Context(), cfg, factory)
		if err != nil {
			t.Fatalf("NewLodeClientWithFactory failed: %v", err)
		}

		snap := metrics.Snapshot{
			RunsStarted:    int64(i + 1),
			RunID:          runID,
			Policy:         "strict",
			Executor:       "executor.js",
			StorageBackend: "fs",
		}

		if err := writeLegacyMetrics(t.Context(), client, snap, completedAt.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatalf("WriteMetrics for %s failed: %v", runID, err)
		}
	}

	// Filter by run-1 — must NOT match run-10
	record, err := QueryLatestMetrics(t.Context(), factory, "quarry", "run-1", "")
	if err != nil {
		t.Fatalf("QueryLatestMetrics failed: %v", err)
	}

	if v := toString(record["run_id"]); v != "run-1" {
		t.Errorf("run_id = %q, want %q (must not match run-10)", v, "run-1")
	}
	if v := toInt64(record["runs_started_total"]); v != 1 {
		t.Errorf("runs_started_total = %d, want 1", v)
	}
}

// TestQueryLatestMetrics_RecordLevelFiltering verifies that record-level
// run_id filtering works when manifest paths might match broadly.
func TestQueryLatestMetrics_RecordLevelFiltering(t *testing.T) {
	t.Parallel()

	store := lode.NewMemory()
	factory := sharedFactory(store)

	completedAt := time.Date(2026, 2, 3, 15, 0, 0, 0, time.UTC)

	// Write metrics for run-abc
	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-abc",
	}

	client, err := NewLodeClientWithFactory(t.Context(), cfg, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	snap := metrics.Snapshot{
		RunsStarted:    5,
		RunID:          "run-abc",
		Policy:         "strict",
		Executor:       "executor.js",
		StorageBackend: "fs",
	}

	if err := writeLegacyMetrics(t.Context(), client, snap, completedAt); err != nil {
		t.Fatalf("write metrics: %v", err)
	}

	// Filter by a run_id that doesn't exist — should get ErrNoMetricsFound
	_, err = QueryLatestMetrics(t.Context(), factory, "quarry", "run-nonexistent", "")
	if err == nil {
		t.Fatal("expected error for non-matching run_id filter, got nil")
	}
	if !errors.Is(err, ErrNoMetricsFound) {
		t.Errorf("expected ErrNoMetricsFound, got: %v", err)
	}
}

// TestQueryLatestMetrics_TsRoundTrip verifies ts survives write/read cycle.
func TestQueryLatestMetrics_TsRoundTrip(t *testing.T) {
	t.Parallel()

	store := lode.NewMemory()
	factory := sharedFactory(store)

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-001",
	}

	client, err := NewLodeClientWithFactory(t.Context(), cfg, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	completedAt := time.Date(2026, 2, 3, 15, 30, 0, 0, time.UTC)
	snap := metrics.Snapshot{
		RunsStarted:    1,
		RunID:          "run-001",
		Policy:         "strict",
		Executor:       "executor.js",
		StorageBackend: "fs",
	}

	if err := writeLegacyMetrics(t.Context(), client, snap, completedAt); err != nil {
		t.Fatalf("write metrics: %v", err)
	}

	record, err := QueryLatestMetrics(t.Context(), factory, "quarry", "", "")
	if err != nil {
		t.Fatalf("QueryLatestMetrics failed: %v", err)
	}

	if v := toString(record["ts"]); v != "2026-02-03T15:30:00Z" {
		t.Errorf("ts = %q, want %q", v, "2026-02-03T15:30:00Z")
	}
}

// Lode escapes partition values in paths, so a source or run ID that
// escaping changes still matches the snapshot that holds its metrics.
func TestQueryLatestMetrics_FiltersOnEscapedValues(t *testing.T) {
	t.Parallel()
	store := lode.NewMemory()
	factory := sharedFactory(store)
	cfg := Config{Dataset: "quarry", Source: "test source", Category: "test-category", Day: "2026-02-03", RunID: "run 001"}
	client, err := NewLodeClientWithFactory(t.Context(), cfg, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}
	snap := metrics.Snapshot{RunsStarted: 1, Policy: "strict", RunID: cfg.RunID}
	if err := writeLegacyMetrics(t.Context(), client, snap, time.Date(2026, 2, 3, 15, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("write metrics: %v", err)
	}
	record, err := QueryLatestMetrics(t.Context(), factory, "quarry", cfg.RunID, cfg.Source)
	if err != nil {
		t.Fatalf("QueryLatestMetrics(%q, %q): %v", cfg.RunID, cfg.Source, err)
	}
	if v := toString(record["run_id"]); v != cfg.RunID {
		t.Errorf("run_id = %q, want %q", v, cfg.RunID)
	}
}

// The latest run's metrics come from its end record or from a metrics record of
// an earlier release, whichever run ended later.
func TestQueryLatestMetrics_EndRecords(t *testing.T) {
	t.Parallel()
	factory := sharedMemoryFactory()
	at := func(day, hour int) time.Time { return time.Date(2026, 2, day, hour, 0, 0, 0, time.UTC) }
	client := func(source, runID, day string) *LodeClient {
		t.Helper()
		c, err := NewLodeClientWithFactory(t.Context(), Config{Dataset: "quarry", Source: source, Category: "cat", Day: day, RunID: runID}, factory)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	end := func(source, runID, day string, ended time.Time, collected bool) {
		t.Helper()
		rec := RunEnd{EndedAt: ended}
		if collected {
			rec.Metrics = &metrics.Snapshot{RunID: runID, Policy: "strict", Executor: "e", StorageBackend: "fs"}
		}
		c := client(source, runID, day)
		started, err := time.Parse(time.DateOnly, day)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.WriteStartRecord(t.Context(), &types.RunMeta{RunID: runID, Attempt: 1}, started, nil); err != nil {
			t.Fatal(err)
		}
		if err := c.WriteEndRecord(t.Context(), rec); err != nil {
			t.Fatal(err)
		}
	}
	legacy := func(source, runID, day string, ended time.Time) {
		t.Helper()
		if err := writeLegacyMetrics(t.Context(), client(source, runID, day), metrics.Snapshot{RunID: runID, Policy: "strict"}, ended); err != nil {
			t.Fatal(err)
		}
	}
	latest := func(runID, source string) string {
		t.Helper()
		record, err := QueryLatestMetrics(t.Context(), factory, "quarry", runID, source)
		if errors.Is(err, ErrNoMetricsFound) {
			return "none"
		}
		if err != nil {
			t.Fatal(err)
		}
		return toString(record["run_id"])
	}

	end("a b", "run-none", "2026-02-05", at(5, 1), false)
	if got := latest("", ""); got != "none" {
		t.Errorf("latest = %s, want none from a run that collects none", got)
	}
	end("a b", "run-1", "2026-02-03", at(3, 1), true)
	end("c", "run-2", "2026-02-03", at(3, 2), true)
	legacy("c", "run-old", "2026-02-02", at(2, 5))
	for _, tc := range []struct{ name, runID, source, want string }{
		{"no filter", "", "", "run-2"},
		{"an escaped source", "", "a b", "run-1"},
		{"a run", "run-1", "", "run-1"},
		{"a run of an earlier release", "run-old", "", "run-old"},
		{"a run that collects none", "run-none", "", "none"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := latest(tc.runID, tc.source); got != tc.want {
				t.Errorf("latest = %s, want %s", got, tc.want)
			}
		})
	}
	// An end record without a start record names no run.
	if err := client("c", "run-pruned", "2026-02-09").WriteEndRecord(t.Context(), RunEnd{EndedAt: at(9, 1), Metrics: &metrics.Snapshot{RunID: "run-pruned"}}); err != nil {
		t.Fatal(err)
	}
	if got := latest("", ""); got != "run-2" {
		t.Errorf("latest = %s, want no run without a start record", got)
	}
	// Times compare as instants, not as text.
	end("c", "run-tenth", "2026-02-03", at(3, 3).Add(100*time.Millisecond), true)
	end("c", "run-later", "2026-02-03", at(3, 3).Add(110*time.Millisecond), true)
	if got := latest("", ""); got != "run-later" {
		t.Errorf("latest = %s, want the later instant", got)
	}
	// The run that ended last is the latest, whatever its day and commit order.
	legacy("c", "run-slow", "2026-02-01", at(4, 2))
	legacy("c", "run-late", "2026-02-04", at(4, 1))
	if got := latest("", ""); got != "run-slow" {
		t.Errorf("latest = %s, want the metrics record that ended last", got)
	}
	end("c", "run-long", "2026-02-01", at(5, 1), true)
	end("c", "run-short", "2026-02-04", at(4, 23), true)
	if got := latest("", ""); got != "run-long" {
		t.Errorf("latest = %s, want the end record that ended last", got)
	}
	record, err := QueryLatestMetrics(t.Context(), factory, "quarry", "run-2", "")
	if err != nil || record["ts"] != "2026-02-03T02:00:00Z" || record["source"] != "c" || record["day"] != "2026-02-03" || record["event_type"] != "metrics" {
		t.Errorf("record = %v, %v, want the fields of a metrics record", record, err)
	}
	// A run's end record is its source, whatever a metrics record of it says, and
	// of two metrics records in one second the later snapshot is taken.
	end("c", "run-both", "2026-02-07", at(7, 10), true)
	legacy("c", "run-both", "2026-02-07", at(7, 12))
	legacy("c", "run-first", "2026-02-07", at(7, 11))
	legacy("c", "run-second", "2026-02-07", at(7, 11))
	if got := latest("", ""); got != "run-second" {
		t.Errorf("latest = %s, want run-second", got)
	}
	// A metrics record whose time does not parse fails the queries that read it.
	broken := client("c", "run-broken", "2026-02-06")
	record = toMetricsRecordMap(metrics.Snapshot{RunID: "run-broken", Policy: "strict"}, broken.config, at(6, 1))
	record["ts"] = "not-a-time"
	brokenSnap, err := broken.dataset.Write(t.Context(), lode.R(record), map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := QueryLatestMetrics(t.Context(), factory, "quarry", "", ""); err == nil {
		t.Error("QueryLatestMetrics skipped a broken record that may be the latest")
	}
	// A clock may step back an hour between a run's end and its commit, so a record
	// that committed within that of the latest end is read, and an earlier one is not.
	end("c", "run-near", "2026-02-07", brokenSnap.CreatedAt.Add(time.Hour), true)
	if _, err := QueryLatestMetrics(t.Context(), factory, "quarry", "", ""); err == nil {
		t.Error("QueryLatestMetrics skipped a broken record within a clock step of the latest end")
	}
	end("c", "run-future-a", "2026-02-07", brokenSnap.CreatedAt.Add(time.Hour+time.Nanosecond), true)
	if got := latest("", ""); got != "run-future-a" {
		t.Errorf("latest = %s, want an end after every commit", got)
	}
	// Of two ends at one instant the greater run ID is taken.
	record, err = QueryLatestMetrics(t.Context(), factory, "quarry", "run-future-a", "")
	if err != nil {
		t.Fatal(err)
	}
	same, err := time.Parse(time.RFC3339Nano, toString(record["ts"]))
	if err != nil {
		t.Fatal(err)
	}
	end("c", "run-future-b", "2026-02-07", same, true)
	if got := latest("", ""); got != "run-future-b" {
		t.Errorf("latest = %s, want run-future-b", got)
	}
	if _, err := QueryLatestMetrics(t.Context(), factory, "quarry", "run-broken", ""); err == nil || errors.Is(err, ErrNoMetricsFound) {
		t.Errorf("a broken time gave %v, want its parse error", err)
	}
	// Metrics of another run are refused.
	foreign := client("c", "run-foreign", "2026-02-03")
	if err := foreign.WriteStartRecord(t.Context(), &types.RunMeta{RunID: "run-foreign", Attempt: 1}, at(3, 0), nil); err != nil {
		t.Fatal(err)
	}
	if err := foreign.WriteEndRecord(t.Context(), RunEnd{EndedAt: at(3, 1), Metrics: &metrics.Snapshot{RunID: "run-2"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := QueryLatestMetrics(t.Context(), factory, "quarry", "run-foreign", ""); err == nil {
		t.Error("QueryLatestMetrics returned another run's metrics")
	}
}
