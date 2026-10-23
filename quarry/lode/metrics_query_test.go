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
		RunsStarted:          1,
		RunsCompleted:        1,
		EventsReceived:       42,
		EventsPersisted:      40,
		EventsDropped:        2,
		DroppedByType:        map[string]int64{"debug": 2},
		ExecutorLaunchSuccess: 1,
		LodeWriteSuccess:     10,
		Policy:               "strict",
		Executor:             "executor.js",
		StorageBackend:       "fs",
		RunID:                "run-001",
		JobID:                "job-xyz",
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

func TestQueryLatestMetrics_MultipleRuns(t *testing.T) {
	store := lode.NewMemory()
	factory := sharedFactory(store)

	completedAt := time.Date(2026, 2, 3, 15, 0, 0, 0, time.UTC)

	// Write metrics for 3 different runs
	for i, runID := range []string{"run-001", "run-002", "run-003"} {
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
			RunsStarted: int64(i + 1),
			RunID:        runID,
			Policy:       "strict",
			Executor:     "executor.js",
			StorageBackend: "fs",
		}

		if err := client.WriteMetrics(t.Context(), snap, completedAt.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatalf("WriteMetrics for %s failed: %v", runID, err)
		}
	}

	// Read without filter — should get latest (run-003)
	ds, err := NewReadDataset("quarry", factory)
	if err != nil {
		t.Fatalf("NewReadDataset failed: %v", err)
	}

	record, err := QueryLatestMetrics(t.Context(), ds, "", "")
	if err != nil {
		t.Fatalf("QueryLatestMetrics failed: %v", err)
	}

	if v := toString(record["run_id"]); v != "run-003" {
		t.Errorf("run_id = %q, want %q (latest)", v, "run-003")
	}
	if v := toInt64(record["runs_started_total"]); v != 3 {
		t.Errorf("runs_started_total = %d, want 3", v)
	}
}

func TestQueryLatestMetrics_FilterByRunID(t *testing.T) {
	store := lode.NewMemory()
	factory := sharedFactory(store)

	completedAt := time.Date(2026, 2, 3, 15, 0, 0, 0, time.UTC)

	// Write metrics for 3 runs
	for i, runID := range []string{"run-001", "run-002", "run-003"} {
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
			RunsStarted:   int64(i + 1),
			RunID:          runID,
			Policy:         "strict",
			Executor:       "executor.js",
			StorageBackend: "fs",
		}

		if err := client.WriteMetrics(t.Context(), snap, completedAt.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatalf("WriteMetrics for %s failed: %v", runID, err)
		}
	}

	// Filter by specific run-id
	ds, err := NewReadDataset("quarry", factory)
	if err != nil {
		t.Fatalf("NewReadDataset failed: %v", err)
	}

	record, err := QueryLatestMetrics(t.Context(), ds, "run-002", "")
	if err != nil {
		t.Fatalf("QueryLatestMetrics failed: %v", err)
	}

	if v := toString(record["run_id"]); v != "run-002" {
		t.Errorf("run_id = %q, want %q", v, "run-002")
	}
	if v := toInt64(record["runs_started_total"]); v != 2 {
		t.Errorf("runs_started_total = %d, want 2", v)
	}
}

func TestQueryLatestMetrics_FilterBySource(t *testing.T) {
	store := lode.NewMemory()
	factory := sharedFactory(store)

	completedAt := time.Date(2026, 2, 3, 15, 0, 0, 0, time.UTC)

	// Write metrics with different sources
	for i, source := range []string{"alpha", "beta"} {
		cfg := Config{
			Dataset:  "quarry",
			Source:   source,
			Category: "test-category",
			Day:      "2026-02-03",
			RunID:    "run-001",
		}

		client, err := NewLodeClientWithFactory(cfg, factory)
		if err != nil {
			t.Fatalf("NewLodeClientWithFactory failed: %v", err)
		}

		snap := metrics.Snapshot{
			RunsStarted:   int64(i + 1),
			RunID:          "run-001",
			Policy:         "strict",
			Executor:       "executor.js",
			StorageBackend: "fs",
		}

		if err := client.WriteMetrics(t.Context(), snap, completedAt.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatalf("WriteMetrics for source %s failed: %v", source, err)
		}
	}

	ds, err := NewReadDataset("quarry", factory)
	if err != nil {
		t.Fatalf("NewReadDataset failed: %v", err)
	}

	// Filter by source=alpha
	record, err := QueryLatestMetrics(t.Context(), ds, "", "alpha")
	if err != nil {
		t.Fatalf("QueryLatestMetrics failed: %v", err)
	}

	if v := toInt64(record["runs_started_total"]); v != 1 {
		t.Errorf("runs_started_total = %d, want 1 (alpha source)", v)
	}
}

func TestQueryLatestMetrics_NoMetrics(t *testing.T) {
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
			RunsStarted:   int64(i + 1),
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

// TestQueryLatestMetrics_SourceSubstringNoCollision verifies that filtering
// by source=alpha does not match source=alphabet.
func TestQueryLatestMetrics_SourceSubstringNoCollision(t *testing.T) {
	store := lode.NewMemory()
	factory := sharedFactory(store)

	completedAt := time.Date(2026, 2, 3, 15, 0, 0, 0, time.UTC)

	// Write metrics for source=alpha and source=alphabet
	for i, source := range []string{"alpha", "alphabet"} {
		cfg := Config{
			Dataset:  "quarry",
			Source:   source,
			Category: "test-category",
			Day:      "2026-02-03",
			RunID:    "run-001",
		}

		client, err := NewLodeClientWithFactory(cfg, factory)
		if err != nil {
			t.Fatalf("NewLodeClientWithFactory failed: %v", err)
		}

		snap := metrics.Snapshot{
			RunsStarted:   int64(i + 1),
			RunID:          "run-001",
			Policy:         "strict",
			Executor:       "executor.js",
			StorageBackend: "fs",
		}

		if err := client.WriteMetrics(t.Context(), snap, completedAt.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatalf("WriteMetrics for source %s failed: %v", source, err)
		}
	}

	ds, err := NewReadDataset("quarry", factory)
	if err != nil {
		t.Fatalf("NewReadDataset failed: %v", err)
	}

	// Filter by source=alpha — must NOT match source=alphabet
	record, err := QueryLatestMetrics(t.Context(), ds, "", "alpha")
	if err != nil {
		t.Fatalf("QueryLatestMetrics failed: %v", err)
	}

	if v := toInt64(record["runs_started_total"]); v != 1 {
		t.Errorf("runs_started_total = %d, want 1 (alpha source, not alphabet)", v)
	}
}

// TestQueryLatestMetrics_RecordLevelFiltering verifies that record-level
// run_id filtering works when manifest paths might match broadly.
func TestQueryLatestMetrics_RecordLevelFiltering(t *testing.T) {
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
		RunsStarted:   5,
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
		RunsStarted:   1,
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
