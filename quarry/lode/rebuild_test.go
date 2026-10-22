package lode

import (
	"fmt"
	"testing"
	"time"

	"github.com/pithecene-io/lode/lode"

	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/types"
)

const legacyTs = "2026-10-21T03:04:05.5Z"

// writeLegacyRun writes a run the way pre-record versions did: partitions
// only, no run records. A nil eventTypes writes only the metrics record.
func writeLegacyRun(t *testing.T, factory lode.StoreFactory, source, runID string, eventTypes ...types.EventType) Config {
	t.Helper()
	cfg := Config{Dataset: "quarry", Source: source, Category: "cat", Day: "2026-10-21", RunID: runID}
	c, err := NewLodeClientWithFactory(cfg, factory)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	job := "job-7"
	for i, et := range eventTypes {
		ev := &types.EventEnvelope{
			ContractVersion: types.ContractVersion, EventID: fmt.Sprintf("e%d", i+1), RunID: runID,
			Seq: int64(i + 1), Type: et, Ts: legacyTs, Payload: map[string]any{}, JobID: &job, Attempt: 2,
		}
		if err := c.WriteEvents(t.Context(), "quarry", runID, []*types.EventEnvelope{ev}); err != nil {
			t.Fatalf("events: %v", err)
		}
	}
	if err := c.WriteMetrics(t.Context(), metrics.Snapshot{RunID: runID, Policy: "strict"}, time.Now()); err != nil {
		t.Fatalf("metrics: %v", err)
	}
	return cfg
}

func rebuild(t *testing.T, factory lode.StoreFactory, dryRun bool) RebuildReport {
	t.Helper()
	report, err := RebuildRunRecords(t.Context(), "quarry", factory, RebuildOptions{DryRun: dryRun})
	if err != nil {
		t.Fatalf("RebuildRunRecords: %v", err)
	}
	return report
}

func hasKey(t *testing.T, factory lode.StoreFactory, key string) bool {
	t.Helper()
	store, _ := factory()
	keys, err := listKeys(t.Context(), store, key)
	if err != nil {
		t.Fatalf("list %s: %v", key, err)
	}
	return keys[key]
}

func TestRebuild_WritesOnlyWhatStorageProves(t *testing.T) {
	t.Parallel()
	factory := sharedMemoryFactory()
	cfg := writeLegacyRun(t, factory, "a/b", "r1", types.EventTypeItem, types.EventTypeRunComplete)

	report := rebuild(t, factory, false)
	if report.StartsWritten != 1 || report.EndsWritten != 1 || len(report.Runs) != 1 {
		t.Fatalf("report = %+v", report)
	}
	store, _ := factory()
	var start map[string]any
	readJSON(t, store, RunRecordDir(cfg)+StartRecordName, &start)
	for k, want := range map[string]any{
		"record_kind": "run_start", "run_id": "r1", "job_id": "job-7", "attempt": float64(2), "source": "a/b",
		"day": "2026-10-21", "partition": "source=a%2Fb/category=cat/day=2026-10-21/run_id=r1/",
		"started_at": nil, "backfilled": true,
	} {
		if start[k] != want {
			t.Errorf("start[%q] = %v, want %v", k, start[k], want)
		}
	}
	var end map[string]any
	readJSON(t, store, RunRecordDir(cfg)+EndRecordName, &end)
	for k, want := range map[string]any{
		"record_kind": "run_end", "ended_at": "2026-10-21T03:04:05.5Z", "terminal": "run_complete",
		"outcome": nil, "event_count": nil, "metrics_persisted": true, "backfilled": true,
	} {
		if end[k] != want {
			t.Errorf("end[%q] = %v, want %v", k, end[k], want)
		}
	}
}

func TestRebuild_RunShapes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name                 string
		events               []types.EventType
		wroteStart, wroteEnd bool
		noEnd, skipped       string
	}{
		{"no terminal event", []types.EventType{types.EventTypeItem}, true, false, noEndNoTerminal, ""},
		{"two terminal events", []types.EventType{types.EventTypeRunError, types.EventTypeRunComplete}, true, false, noEndAmbiguous, ""},
		{"run_error", []types.EventType{types.EventTypeLog, types.EventTypeRunError}, true, true, "", ""},
		{"metrics only", nil, false, false, noEndNoTerminal, skipNoLineage},
	}
	for _, tc := range cases {
		factory := sharedMemoryFactory()
		writeLegacyRun(t, factory, "src", "r1", tc.events...)
		report := rebuild(t, factory, false)
		if len(report.Runs) != 1 {
			t.Errorf("%s: runs = %+v", tc.name, report.Runs)
			continue
		}
		got := report.Runs[0]
		if got.WroteStart != tc.wroteStart || got.WroteEnd != tc.wroteEnd || got.NoEnd != tc.noEnd || got.Skipped != tc.skipped {
			t.Errorf("%s: got %+v", tc.name, got)
		}
	}
}

func TestRebuild_DryRunWritesNothing(t *testing.T) {
	t.Parallel()
	factory := sharedMemoryFactory()
	cfg := writeLegacyRun(t, factory, "src", "r1", types.EventTypeRunComplete)
	report := rebuild(t, factory, true)
	if !report.DryRun || report.StartsWritten != 1 || report.EndsWritten != 1 {
		t.Fatalf("dry-run report = %+v", report)
	}
	if hasKey(t, factory, RunRecordDir(cfg)+StartRecordName) {
		t.Fatal("dry run wrote a start record")
	}
}

// A rerun writes nothing new, and a backfilled run whose terminal event
// lands later gains its end record.
func TestRebuild_IdempotentAndResumes(t *testing.T) {
	t.Parallel()
	factory := sharedMemoryFactory()
	writeLegacyRun(t, factory, "src", "r1", types.EventTypeItem, types.EventTypeRunComplete)
	cfg := writeLegacyRun(t, factory, "src", "r2", types.EventTypeItem)
	rebuild(t, factory, false)

	again := rebuild(t, factory, false)
	if again.AlreadyIndexed != 2 || again.StartsWritten != 0 || again.EndsWritten != 0 || len(again.Runs) != 0 {
		t.Fatalf("rerun = %+v", again)
	}

	c, err := NewLodeClientWithFactory(cfg, factory)
	if err != nil {
		t.Fatal(err)
	}
	ev := &types.EventEnvelope{ContractVersion: types.ContractVersion, EventID: "e9", RunID: "r2", Seq: 9, Type: types.EventTypeRunComplete, Ts: legacyTs, Attempt: 2}
	if err := c.WriteEvents(t.Context(), "quarry", "r2", []*types.EventEnvelope{ev}); err != nil {
		t.Fatal(err)
	}
	resumed := rebuild(t, factory, false)
	if resumed.StartsWritten != 0 || resumed.EndsWritten != 1 {
		t.Fatalf("resume = %+v", resumed)
	}
}

// A run the runtime started is never touched, even with no end record: it
// may be live, and only the runtime classifies it.
func TestRebuild_LeavesRuntimeRecordsAlone(t *testing.T) {
	t.Parallel()
	factory := sharedMemoryFactory()
	cfg := writeLegacyRun(t, factory, "src", "r1", types.EventTypeRunComplete)
	c, err := NewLodeClientWithFactory(cfg, factory)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.WriteStartRecord(t.Context(), &types.RunMeta{RunID: "r1", Attempt: 1}, time.Date(2026, 10, 21, 1, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	report := rebuild(t, factory, false)
	if report.AlreadyIndexed != 1 || report.EndsWritten != 0 {
		t.Fatalf("report = %+v", report)
	}
	if hasKey(t, factory, RunRecordDir(cfg)+EndRecordName) {
		t.Fatal("rebuild wrote an end record for a runtime-started run")
	}
}

func TestRebuild_EmptyDataset(t *testing.T) {
	t.Parallel()
	report := rebuild(t, sharedMemoryFactory(), false)
	if report.RunsScanned != 0 || len(report.Runs) != 0 {
		t.Fatalf("report = %+v", report)
	}
	if _, err := RebuildRunRecords(t.Context(), "..", sharedMemoryFactory(), RebuildOptions{}); err == nil {
		t.Fatal("unsafe dataset accepted")
	}
}
