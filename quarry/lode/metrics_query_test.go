package lode

import (
	"errors"
	"testing"
	"time"

	"github.com/pithecene/lode/lode"

	"github.com/pithecene/quarry/metrics"
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
func sharedFactory(store lode.Store) lode.StoreFactory {
	return func() (lode.Store, error) { return store, nil }
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
	client, err := NewLodeClientWithFactory(cfg, factory)
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
	if err := client.WriteMetrics(t.Context(), snap, completedAt); err != nil {
		t.Fatalf("WriteMetrics failed: %v", err)
	}

	// Read via QueryLatestMetrics
	ds, err := NewReadDataset("quarry", factory)
	if err != nil {
		t.Fatalf("NewReadDataset failed: %v", err)
	}

	record, err := QueryLatestMetrics(t.Context(), ds, "", "")
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
				client, err := NewLodeClientWithFactory(cfg, factory)
				if err != nil {
					t.Fatalf("NewLodeClientWithFactory failed: %v", err)
				}
				snap := metrics.Snapshot{RunsStarted: int64(i + 1), RunID: w.runID, Policy: "strict", Executor: "executor.js", StorageBackend: "fs"}
				if err := client.WriteMetrics(t.Context(), snap, completedAt.Add(time.Duration(i)*time.Minute)); err != nil {
					t.Fatalf("WriteMetrics for %+v failed: %v", w, err)
				}
			}
			ds, err := NewReadDataset("quarry", factory)
			if err != nil {
				t.Fatalf("NewReadDataset failed: %v", err)
			}
			record, err := QueryLatestMetrics(t.Context(), ds, tc.runID, tc.source)
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

	ds, err := NewReadDataset("quarry", factory)
	if err != nil {
		t.Fatalf("NewReadDataset failed: %v", err)
	}

	_, err = QueryLatestMetrics(t.Context(), ds, "", "")
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

		client, err := NewLodeClientWithFactory(cfg, factory)
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

		if err := client.WriteMetrics(t.Context(), snap, completedAt.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatalf("WriteMetrics for %s failed: %v", runID, err)
		}
	}

	ds, err := NewReadDataset("quarry", factory)
	if err != nil {
		t.Fatalf("NewReadDataset failed: %v", err)
	}

	// Filter by run-1 — must NOT match run-10
	record, err := QueryLatestMetrics(t.Context(), ds, "run-1", "")
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

	client, err := NewLodeClientWithFactory(cfg, factory)
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

	if err := client.WriteMetrics(t.Context(), snap, completedAt); err != nil {
		t.Fatalf("WriteMetrics failed: %v", err)
	}

	ds, err := NewReadDataset("quarry", factory)
	if err != nil {
		t.Fatalf("NewReadDataset failed: %v", err)
	}

	// Filter by a run_id that doesn't exist — should get ErrNoMetricsFound
	_, err = QueryLatestMetrics(t.Context(), ds, "run-nonexistent", "")
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

	client, err := NewLodeClientWithFactory(cfg, factory)
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

	if err := client.WriteMetrics(t.Context(), snap, completedAt); err != nil {
		t.Fatalf("WriteMetrics failed: %v", err)
	}

	ds, err := NewReadDataset("quarry", factory)
	if err != nil {
		t.Fatalf("NewReadDataset failed: %v", err)
	}

	record, err := QueryLatestMetrics(t.Context(), ds, "", "")
	if err != nil {
		t.Fatalf("QueryLatestMetrics failed: %v", err)
	}

	if v := toString(record["ts"]); v != "2026-02-03T15:30:00Z" {
		t.Errorf("ts = %q, want %q", v, "2026-02-03T15:30:00Z")
	}
}
