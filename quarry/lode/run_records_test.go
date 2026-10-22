package lode

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/pithecene-io/lode/lode"

	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/types"
)

func TestValidateRunKey(t *testing.T) {
	cases := []struct {
		name                              string
		dataset, source, category, runID string
		ok                                bool
	}{
		{"valid", "quarry", "src", "cat", "run-1", true},
		{"slash in source is encodable", "quarry", "a/b", "cat", "run-1", true},
		{"empty source", "quarry", "", "cat", "run-1", false},
		{"dot category", "quarry", "src", ".", "run-1", false},
		{"dotdot run id", "quarry", "src", "cat", "..", false},
		{"empty dataset", "", "src", "cat", "run-1", false},
		{"dotdot dataset", "..", "src", "cat", "run-1", false},
		{"slash in dataset", "a/b", "src", "cat", "run-1", false},
		{"backslash in dataset", `a\b`, "src", "cat", "run-1", false},
		{"dotdot segment in source", "quarry", "a/../b", "cat", "run-1", false},
		{"dot segment in run id", "quarry", "src", "cat", "x/./y", false},
		{"dotdot segment in category via backslash", "quarry", "src", `a\..`, "run-1", false},
		{"dots inside a segment are fine", "quarry", "a..b", "c.d", "run.1", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateRunKey(tc.dataset, tc.source, tc.category, tc.runID)
			if tc.ok && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tc.ok && !errors.Is(err, ErrInvalidRunKey) {
				t.Fatalf("err = %v, want ErrInvalidRunKey", err)
			}
		})
	}
}

func TestRunRecordDir_EncodesComponents(t *testing.T) {
	cfg := Config{Dataset: "quarry", Source: "a/b", Category: "c d", Day: "2026-10-21", RunID: "run/1"}
	want := "datasets/quarry/runs/a%2Fb/c%20d/2026-10-21/run%2F1/"
	if got := RunRecordDir(cfg); got != want {
		t.Fatalf("RunRecordDir = %q, want %q", got, want)
	}
}

// The start record's partition must address the partitions Lode actually
// writes, so readers never re-derive keys.
func TestPartitionPrefix_MatchesLodeHiveLayout(t *testing.T) {
	cfg := Config{Dataset: "quarry", Source: "a/b", Category: "c d", Day: "2026-10-21", RunID: "run-1"}
	factory := sharedMemoryFactory()
	client, err := NewLodeClientWithFactory(cfg, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory: %v", err)
	}
	ev := &types.EventEnvelope{
		ContractVersion: types.ContractVersion, EventID: "e1", RunID: cfg.RunID,
		Seq: 1, Type: types.EventTypeItem, Ts: "2026-10-21T00:00:00Z", Payload: map[string]any{}, Attempt: 1,
	}
	if err := client.WriteEvents(t.Context(), cfg.Dataset, cfg.RunID, []*types.EventEnvelope{ev}); err != nil {
		t.Fatalf("WriteEvents: %v", err)
	}
	store, err := factory()
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	keys, err := store.List(t.Context(), "datasets/quarry/partitions/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	want := "datasets/quarry/partitions/" + PartitionPrefix(cfg) + "event_type=item/"
	for _, k := range keys {
		if strings.HasPrefix(k, want) {
			return
		}
	}
	t.Fatalf("no key under %q; keys: %v", want, keys)
}

func TestWriteStartAndEndRecords(t *testing.T) {
	cfg := Config{Dataset: "quarry", Source: "src", Category: "cat", Day: "2026-10-21", RunID: "run-1"}
	factory := sharedMemoryFactory()
	client, err := NewLodeClientWithFactory(cfg, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory: %v", err)
	}
	job := "job-1"
	meta := &types.RunMeta{RunID: cfg.RunID, JobID: &job, Attempt: 1}
	startedAt := time.Date(2026, 10, 21, 8, 30, 0, 123, time.UTC)
	if err := client.WriteStartRecord(t.Context(), meta, startedAt); err != nil {
		t.Fatalf("WriteStartRecord: %v", err)
	}
	end := RunEnd{
		EndedAt: startedAt.Add(time.Minute), Outcome: types.OutcomeScriptError,
		Terminal: types.EventTypeRunError, Message: "boom", EventCount: 7, MetricsPersisted: true,
	}
	if err := client.WriteEndRecord(t.Context(), end); err != nil {
		t.Fatalf("WriteEndRecord: %v", err)
	}

	store, _ := factory()
	var start map[string]any
	readJSON(t, store, RunRecordDir(cfg)+"start.json", &start)
	for k, want := range map[string]any{
		"record_kind": "run_start", "schema_version": float64(1), "run_id": "run-1", "job_id": "job-1",
		"attempt": float64(1), "dataset": "quarry", "source": "src", "category": "cat", "day": "2026-10-21",
		"partition": "source=src/category=cat/day=2026-10-21/run_id=run-1/",
		"started_at": "2026-10-21T08:30:00.000000123Z", "quarry_version": types.Version, "backfilled": false,
	} {
		if start[k] != want {
			t.Errorf("start[%q] = %v, want %v", k, start[k], want)
		}
	}
	if _, ok := start["parent_run_id"]; ok {
		t.Error("parent_run_id should be omitted for an initial run")
	}

	var endRec map[string]any
	readJSON(t, store, RunRecordDir(cfg)+"end.json", &endRec)
	for k, want := range map[string]any{
		"record_kind": "run_end", "run_id": "run-1", "outcome": "script_error", "terminal": "run_error",
		"message": "boom", "event_count": float64(7), "metrics_persisted": true, "backfilled": false,
	} {
		if endRec[k] != want {
			t.Errorf("end[%q] = %v, want %v", k, endRec[k], want)
		}
	}
}

func TestWriteEndRecord_NoTerminalIsNull(t *testing.T) {
	cfg := Config{Dataset: "quarry", Source: "src", Category: "cat", Day: "2026-10-21", RunID: "run-1"}
	factory := sharedMemoryFactory()
	client, _ := NewLodeClientWithFactory(cfg, factory)
	if err := client.WriteEndRecord(t.Context(), RunEnd{EndedAt: time.Now(), Outcome: types.OutcomeExecutorCrash}); err != nil {
		t.Fatalf("WriteEndRecord: %v", err)
	}
	store, _ := factory()
	var endRec map[string]any
	readJSON(t, store, RunRecordDir(cfg)+"end.json", &endRec)
	if v, ok := endRec["terminal"]; !ok || v != nil {
		t.Fatalf("terminal = %v (present %v), want explicit null", v, ok)
	}
}

// A run ID that already has a start record is refused: Put is create-only.
func TestWriteStartRecord_DuplicateRunIDRefused(t *testing.T) {
	cfg := Config{Dataset: "quarry", Source: "src", Category: "cat", Day: "2026-10-21", RunID: "run-1"}
	client, _ := NewLodeClientWithFactory(cfg, lode.NewMemoryFactory())
	meta := &types.RunMeta{RunID: cfg.RunID, Attempt: 1}
	at := time.Date(2026, 10, 21, 8, 0, 0, 0, time.UTC)
	if err := client.WriteStartRecord(t.Context(), meta, at); err != nil {
		t.Fatalf("first WriteStartRecord: %v", err)
	}
	err := client.WriteStartRecord(t.Context(), meta, at)
	// The runtime matches Quarry's sentinel to tell a duplicate run ID apart
	// from a failed write (CONTRACT_RUN.md §Failure Semantics).
	if !errors.Is(err, ErrPathExists) || !errors.Is(err, lode.ErrPathExists) {
		t.Fatalf("second WriteStartRecord err = %v, want ErrPathExists (Quarry and Lode)", err)
	}
}

// sharedMemoryFactory returns a factory that always yields the same memory
// store, so a test can read back what the client wrote.
func sharedMemoryFactory() lode.StoreFactory {
	store := lode.NewMemory()
	return func() (lode.Store, error) { return store, nil }
}

func readJSON(t *testing.T, store lode.Store, path string, v any) {
	t.Helper()
	rc, err := store.Get(t.Context(), path)
	if err != nil {
		t.Fatalf("Get %s: %v", path, err)
	}
	defer iox.DiscardClose(rc)
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("unmarshal %s: %v", path, err)
	}
}

// Programmatic callers that bypass the CLI still cannot write outside the
// dataset's namespace.
func TestWriteStartRecord_RejectsUnsafeKey(t *testing.T) {
	cfg := Config{Dataset: "../other", Source: "src", Category: "cat", Day: "2026-10-21", RunID: "run-1"}
	client, err := NewLodeClientWithFactory(cfg, sharedMemoryFactory())
	if err != nil {
		t.Skipf("Lode rejected the dataset itself: %v", err)
	}
	if err := client.WriteStartRecord(t.Context(), &types.RunMeta{RunID: "run-1", Attempt: 1}, time.Now()); !errors.Is(err, ErrInvalidRunKey) {
		t.Fatalf("err = %v, want ErrInvalidRunKey", err)
	}
}

func TestWriteStartRecord_DayChecks(t *testing.T) {
	meta := &types.RunMeta{RunID: "run-1", Attempt: 1}
	on := time.Date(2026, 10, 21, 8, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name, day string
		at        time.Time
	}{
		{"empty day", "", on},
		{"malformed day", "21-10-2026", on},
		{"started_at on another day", "2026-10-22", on},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{Dataset: "quarry", Source: "src", Category: "cat", Day: tc.day, RunID: "run-1"}
			client, _ := NewLodeClientWithFactory(cfg, sharedMemoryFactory())
			if err := client.WriteStartRecord(t.Context(), meta, tc.at); !errors.Is(err, ErrInvalidRunKey) {
				t.Fatalf("err = %v, want ErrInvalidRunKey", err)
			}
		})
	}
}
