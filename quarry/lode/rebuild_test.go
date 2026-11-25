package lode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/lode/lode"

	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/types"
)

const legacyTs = "2026-10-21T03:04:05.5Z"

// writeLegacyRun writes a run the way pre-record versions did: partitions
// only, no run records. A nil eventTypes writes only the metrics record.
func writeLegacyRun(t *testing.T, factory lode.Factory[lode.RangeStore], source, runID string, eventTypes ...types.EventType) Config {
	t.Helper()
	cfg := Config{Dataset: "quarry", Source: source, Category: "cat", Day: "2026-10-21", RunID: runID}
	c, err := NewLodeClientWithFactory(t.Context(), cfg, factory)
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
	if err := writeLegacyMetrics(t.Context(), c, metrics.Snapshot{RunID: runID, Policy: "strict"}, time.Now()); err != nil {
		t.Fatalf("metrics: %v", err)
	}
	return cfg
}

func rebuild(t *testing.T, factory lode.Factory[lode.RangeStore], dryRun bool) RebuildReport {
	t.Helper()
	report, err := RebuildRunRecords(t.Context(), "quarry", factory, RebuildOptions{DryRun: dryRun})
	if err != nil {
		t.Fatalf("RebuildRunRecords: %v", err)
	}
	return report
}

func hasKey(t *testing.T, factory lode.Factory[lode.RangeStore], key string) bool {
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

// A rerun writes nothing new, and a backfilled run whose terminal event is
// persisted later gains its end record.
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

	c, err := NewLodeClientWithFactory(t.Context(), cfg, factory)
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
// may still be in progress, and only the runtime records its outcome.
func TestRebuild_LeavesRuntimeRecordsAlone(t *testing.T) {
	t.Parallel()
	factory := sharedMemoryFactory()
	cfg := writeLegacyRun(t, factory, "src", "r1", types.EventTypeRunComplete)
	c, err := NewLodeClientWithFactory(t.Context(), cfg, factory)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.WriteStartRecord(t.Context(), &types.RunMeta{RunID: "r1", Attempt: 1}, time.Date(2026, 10, 21, 1, 0, 0, 0, time.UTC), nil); err != nil {
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

// failEndPut fails every end-record write.
type failEndPut struct{ lode.RangeStore }

var errEndPut = errors.New("end put failed")

func (s failEndPut) Put(ctx context.Context, path string, r io.Reader) error {
	if strings.HasSuffix(path, "/"+EndRecordName) {
		return errEndPut
	}
	return s.RangeStore.Put(ctx, path, r)
}

// A store error terminates the rebuild, and the report still lists the start
// record written before it.
func TestRebuild_ErrorReportsRecordsWritten(t *testing.T) {
	t.Parallel()
	factory := sharedMemoryFactory()
	writeLegacyRun(t, factory, "src", "r1", types.EventTypeRunComplete)
	store, _ := factory()
	failing := func() (lode.RangeStore, error) { return failEndPut{store}, nil }

	report, err := RebuildRunRecords(t.Context(), "quarry", failing, RebuildOptions{})
	if !errors.Is(err, errEndPut) {
		t.Fatalf("err = %v, want the end-record write error", err)
	}
	if report.StartsWritten != 1 || len(report.Runs) != 1 || !report.Runs[0].WroteStart || report.Runs[0].WroteEnd {
		t.Fatalf("report = %+v", report)
	}
}

// A dry run reports exactly the records a real rebuild would write: an
// existing end record is not reported as written.
func TestRebuild_DryRunMatchesExistingRecords(t *testing.T) {
	t.Parallel()
	factory := sharedMemoryFactory()
	cfg := writeLegacyRun(t, factory, "src", "r1", types.EventTypeRunComplete)
	store, _ := factory()
	if err := putRunRecord(t.Context(), store, cfg, EndRecordName, map[string]any{"record_kind": RecordKindRunEnd}); err != nil {
		t.Fatal(err)
	}
	dry := rebuild(t, factory, true)
	real := rebuild(t, factory, false)
	if dry.StartsWritten != 1 || dry.EndsWritten != 0 || real.StartsWritten != 1 || real.EndsWritten != 0 {
		t.Fatalf("dry = %+v, real = %+v", dry, real)
	}
}

// A terminal event without usable lineage does not skip the run: its
// lineage comes from the first event record instead.
func TestRebuild_LineageFallsBackWhenTerminalLacksIt(t *testing.T) {
	t.Parallel()
	factory := sharedMemoryFactory()
	cfg := Config{Dataset: "quarry", Source: "src", Category: "cat", Day: "2026-10-21", RunID: "r1"}
	c, err := NewLodeClientWithFactory(t.Context(), cfg, factory)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	job := "job-7"
	for i, ev := range []struct {
		et      types.EventType
		attempt int
	}{{types.EventTypeItem, 3}, {types.EventTypeRunComplete, 0}} {
		env := &types.EventEnvelope{
			ContractVersion: types.ContractVersion, EventID: fmt.Sprintf("e%d", i+1), RunID: "r1",
			Seq: int64(i + 1), Type: ev.et, Ts: legacyTs, Payload: map[string]any{}, JobID: &job, Attempt: ev.attempt,
		}
		if err := c.WriteEvents(t.Context(), "quarry", "r1", []*types.EventEnvelope{env}); err != nil {
			t.Fatalf("events: %v", err)
		}
	}

	report := rebuild(t, factory, false)
	if report.StartsWritten != 1 || report.EndsWritten != 1 || report.Skipped != 0 {
		t.Fatalf("report = %+v", report)
	}
	store, _ := factory()
	var start map[string]any
	readJSON(t, store, RunRecordDir(cfg)+StartRecordName, &start)
	if start["attempt"] != float64(3) {
		t.Errorf("attempt = %v, want 3 from the first event record", start["attempt"])
	}
}

// A terminal event without a partition manifest still ends its run.
func TestRebuildRunRecords_IncompleteIndex(t *testing.T) {
	t.Parallel()
	store := lode.NewMemory()
	factory := sharedFactory(store)
	cfg := writeLegacyRun(t, factory, "src", "r1", types.EventTypeItem, types.EventTypeRunComplete)
	keys, err := store.List(t.Context(), "datasets/quarry/partitions/")
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if strings.HasSuffix(k, "/manifest.json") {
			if err := store.Delete(t.Context(), k); err != nil {
				t.Fatal(err)
			}
		}
	}
	report := rebuild(t, factory, false)
	if report.StartsWritten != 1 || report.EndsWritten != 1 || !hasKey(t, factory, RunRecordDir(cfg)+EndRecordName) {
		t.Fatalf("report = %+v, want a start and an end record", report)
	}
}

// A dataset ID that repeats a path segment, and an index key without a record
// behind it, do not mislead the rebuild.
func TestRebuildRunRecords_TakesFactsFromObjects(t *testing.T) {
	t.Parallel()
	store := lode.NewMemory()
	cfg := Config{Dataset: "partitions", Source: "src", Category: "cat", Day: "2026-10-21", RunID: "r1"}
	c, err := NewLodeClientWithFactory(t.Context(), cfg, sharedFactory(store))
	if err != nil {
		t.Fatal(err)
	}
	ev := &types.EventEnvelope{ContractVersion: types.ContractVersion, EventID: "e1", RunID: "r1", Seq: 1, Type: types.EventTypeRunComplete, Ts: legacyTs, Payload: map[string]any{}, Attempt: 1}
	if err := c.WriteEvents(t.Context(), "partitions", "r1", []*types.EventEnvelope{ev}); err != nil {
		t.Fatal(err)
	}
	keys, err := store.List(t.Context(), "datasets/partitions/partitions/")
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if !strings.HasSuffix(k, "/manifest.json") {
			continue
		}
		rc, err := store.Get(t.Context(), k)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Put(t.Context(), strings.Replace(k, "event_type=run_complete", "event_type=metrics", 1), rc); err != nil {
			t.Fatal(err)
		}
		_ = rc.Close()
	}
	report, err := RebuildRunRecords(t.Context(), "partitions", sharedFactory(store), RebuildOptions{})
	if err != nil || report.StartsWritten != 1 || report.EndsWritten != 1 {
		t.Fatalf("report = %+v, %v; want a start and an end record", report, err)
	}
	rc, err := store.Get(t.Context(), RunRecordDir(cfg)+EndRecordName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close() }()
	var end EndRecord
	if err := json.NewDecoder(rc).Decode(&end); err != nil || end.MetricsPersisted {
		t.Errorf("end = %+v, %v; want no metrics", end, err)
	}
}

// A partition with another field count is refused, and nothing is written.
func TestRebuildRunRecords_RefusesAnotherLayout(t *testing.T) {
	t.Parallel()
	for name, replace := range map[string][2]string{
		"a sixth field":   {"event_type=item/", "event_type=item/extra=x/"},
		"a missing field": {"event_type=item/", ""},
	} {
		t.Run(name, func(t *testing.T) {
			store := lode.NewMemory()
			cfg := writeLegacyRun(t, sharedFactory(store), "src", "r1", types.EventTypeItem)
			keys, err := store.List(t.Context(), "datasets/quarry/partitions/")
			if err != nil {
				t.Fatal(err)
			}
			for _, k := range keys {
				if strings.Contains(k, "event_type=item/") && strings.HasSuffix(k, "/manifest.json") {
					rc, err := store.Get(t.Context(), k)
					if err != nil {
						t.Fatal(err)
					}
					if err := store.Put(t.Context(), strings.Replace(k, replace[0], replace[1], 1), rc); err != nil {
						t.Fatal(err)
					}
					_ = rc.Close()
				}
			}
			_, err = RebuildRunRecords(t.Context(), "quarry", sharedFactory(store), RebuildOptions{})
			if err == nil || !strings.Contains(err.Error(), "does not have the layout") || hasKey(t, sharedFactory(store), RunRecordDir(cfg)+StartRecordName) {
				t.Fatalf("err = %v, want a layout error and no record", err)
			}
		})
	}
}

func TestSnapshotsByPartition(t *testing.T) {
	t.Parallel()
	const partition = "source=s/category=c/day=2026-10-21/run_id=r/event_type=item"
	for _, tc := range []struct {
		name, path string
		want       int
	}{
		{"in the partition tree", "datasets/quarry/partitions/" + partition + "/data", 1},
		{"without the dataset's prefix", partition + "/data", 0},
		{"in another dataset", "datasets/other/partitions/" + partition + "/data", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap := &lode.Snapshot{ID: "1", Manifest: &lode.Manifest{Objects: []lode.ObjectRef{{Path: tc.path}}}}
			if got := snapshotsByPartition("quarry", []*lode.Snapshot{snap}); len(got) != tc.want || len(got[partition]) != tc.want {
				t.Errorf("snapshotsByPartition(%q) = %v, want %d entries", tc.path, got, tc.want)
			}
		})
	}
}

// A metrics partition is no metrics record: the record itself is read, and one
// that cannot be read ends the rebuild before the end record.
func TestRebuildRunRecords_ReadsTheMetricsRecord(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		change          func(lode.D)
		lose            bool
		want, wantError bool
	}{
		{"a record", func(lode.D) {}, false, true, false},
		{"another kind", func(d lode.D) { d["record_kind"] = RecordKindEvent }, false, false, false},
		{"a lost data file", func(lode.D) {}, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := lode.NewMemory()
			cfg := Config{Dataset: "quarry", Source: "src", Category: "cat", Day: "2026-10-21", RunID: "r1"}
			c, err := NewLodeClientWithFactory(t.Context(), cfg, sharedFactory(store))
			if err != nil {
				t.Fatal(err)
			}
			ev := &types.EventEnvelope{ContractVersion: types.ContractVersion, EventID: "e1", RunID: "r1", Seq: 1, Type: types.EventTypeRunComplete, Ts: legacyTs, Payload: map[string]any{}, Attempt: 1}
			if err := c.WriteEvents(t.Context(), "quarry", "r1", []*types.EventEnvelope{ev}); err != nil {
				t.Fatal(err)
			}
			rec := lode.D{"source": "src", "category": "cat", "day": cfg.Day, "run_id": "r1", "event_type": "metrics", "record_kind": RecordKindMetrics}
			tc.change(rec)
			if _, err := c.dataset.Write(t.Context(), []lode.D{rec}, nil); err != nil {
				t.Fatal(err)
			}
			if tc.lose {
				keys, err := store.List(t.Context(), "datasets/quarry/partitions/")
				if err != nil {
					t.Fatal(err)
				}
				for _, k := range keys {
					if strings.Contains(k, "event_type=metrics/") && strings.Contains(k, "/data/") {
						if err := store.Delete(t.Context(), k); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			report, err := RebuildRunRecords(t.Context(), "quarry", sharedFactory(store), RebuildOptions{})
			if (err != nil) != tc.wantError {
				t.Fatalf("report = %+v, %v; want an error = %t", report, err, tc.wantError)
			}
			if ended := hasKey(t, sharedFactory(store), RunRecordDir(cfg)+EndRecordName); ended == tc.wantError {
				t.Fatalf("end record = %t, want %t", ended, !tc.wantError)
			}
			if tc.wantError {
				return
			}
			rc, err := store.Get(t.Context(), RunRecordDir(cfg)+EndRecordName)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = rc.Close() }()
			var end EndRecord
			if err := json.NewDecoder(rc).Decode(&end); err != nil || end.MetricsPersisted != tc.want {
				t.Errorf("end = %+v, %v; want metrics = %t", end, err, tc.want)
			}
		})
	}
}
