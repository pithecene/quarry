package lode_test

import (
	"os"
	"slices"
	"testing"
	"time"

	"github.com/pithecene/lode/lode"

	qlode "github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/reader"
	"github.com/pithecene/quarry/retention"
	"github.com/pithecene/quarry/types"
)

// fixtureV0137 is a dataset that Quarry v0.13.7 wrote with Lode v0.10.0:
// four runs and no run records, as every dataset from before v0.14.0 is.
// testdata/compat/gen/main.go regenerates it.
const fixtureV0137 = "testdata/compat/v0.13.7"

// fixtureV0180 is a dataset that Quarry v0.18.0 wrote with Lode v0.17.1, in
// Lode format 1.1.0: four runs with their run records, one artifact Volume
// with its commit index entry, a sidecar file, and metrics.
// testdata/compat/gen-v0.18.0/main.go regenerates it.
const fixtureV0180 = "testdata/compat/v0.18.0.zip"

// A dataset that a released Quarry wrote is indexed, read, extended, and
// pruned by this release without migration (CONTRACT_LODE.md §Writer
// Settings): the upgrade path from v0.13.7.
func TestCompat_v0_13_7_Dataset(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := copyFixture(t, os.DirFS(fixtureV0137))
	factory := lode.FS(root)

	rebuilt, err := qlode.RebuildRunRecords(ctx, "quarry", factory, qlode.RebuildOptions{})
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if rebuilt.RunsScanned != 4 || rebuilt.StartsWritten != 4 || rebuilt.EndsWritten != 3 || rebuilt.Skipped != 0 {
		t.Fatalf("rebuild = %+v, want 4 runs scanned, 4 starts and 3 ends written, none skipped", rebuilt)
	}
	again, err := qlode.RebuildRunRecords(ctx, "quarry", factory, qlode.RebuildOptions{})
	if err != nil || again.StartsWritten != 0 || again.EndsWritten != 0 {
		t.Fatalf("second rebuild = %+v, %v; want nothing written", again, err)
	}

	rd, err := reader.New("quarry", factory)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	checkFixtureRuns(t, rd, "2026-10-01", true)
	checkOKRun(t, rd, factory, findRun(t, rd, "shop", "products", "2026-10-01", "run-ok-1"))

	appendRun(t, root)
	pruneFailedRun(t, rd, factory, time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
}

// A dataset that the last release on Lode v0.17.1 wrote is read, extended,
// and pruned by this release in place: its run records list each run, its
// commit index entry opens the artifact without a partition read, and the
// format 1.1.0 manifests need no migration (CONTRACT_LODE.md §Writer
// Settings).
func TestCompat_v0_18_0_Dataset(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	root := unpackFixture(t, fixtureV0180)
	factory := lode.FS(root)

	rebuilt, err := qlode.RebuildRunRecords(ctx, "quarry", factory, qlode.RebuildOptions{})
	if err != nil || rebuilt.AlreadyIndexed != 4 || rebuilt.StartsWritten != 0 || rebuilt.EndsWritten != 0 {
		t.Fatalf("rebuild = %+v, %v; want 4 runs already indexed and nothing written", rebuilt, err)
	}

	var fallbacks []reader.IndexFallback
	rd, err := reader.New("quarry", factory, reader.WithIndexFallback(func(_ string, why reader.IndexFallback) {
		fallbacks = append(fallbacks, why)
	}))
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	checkFixtureRuns(t, rd, "2026-11-01", false)
	ok := findRun(t, rd, "shop", "products", "2026-11-01", "run-ok-1")
	checkOKRun(t, rd, factory, ok)
	readArtifact(t, rd, ok, "report of run-ok-1")
	if len(fallbacks) != 0 {
		t.Errorf("opening art-1 fell back to the partition read: %v", fallbacks)
	}

	appendRun(t, root)
	pruneFailedRun(t, rd, factory, time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC))
	readArtifact(t, rd, findRun(t, rd, "shop", "products", "2026-11-01", "run-ok-1"), "report of run-ok-1")
}

// fixtureStates are the states of the four runs that each fixture holds.
var fixtureStates = map[string]reader.State{
	"run-ok-1":     reader.StateSucceeded,
	"run-fail-1":   reader.StateFailed,
	"run-retry-1":  reader.StateSucceeded,
	"run-killed-1": reader.StateUnterminated,
}

// checkFixtureRuns checks each fixture run's state and backfill mark, and the
// lineage of the retry, whose day is day.
func checkFixtureRuns(t *testing.T, rd *reader.Reader, day string, backfilled bool) {
	t.Helper()
	runs := listRuns(t, rd)
	if len(runs) != len(fixtureStates) {
		t.Fatalf("listed %d runs, want %d", len(runs), len(fixtureStates))
	}
	for _, run := range runs {
		if run.State != fixtureStates[run.RunID] || run.Backfilled != backfilled {
			t.Errorf("run %s: state %s, backfilled %t; want %s, backfilled %t", run.RunID, run.State, run.Backfilled, fixtureStates[run.RunID], backfilled)
		}
	}
	retry := findRun(t, rd, "shop", "products", day, "run-retry-1")
	if retry.Attempt != 2 || retry.ParentRunID == nil || *retry.ParentRunID != "run-fail-1" || retry.JobID == nil || *retry.JobID != "job-1" {
		t.Errorf("run-retry-1 lineage = attempt %d, parent %v, job %v; want 2, run-fail-1, job-1", retry.Attempt, retry.ParentRunID, retry.JobID)
	}
}

// checkOKRun checks the events, metrics, and sidecar file of run-ok-1.
func checkOKRun(t *testing.T, rd *reader.Reader, factory lode.Factory[lode.RangeStore], ok reader.Run) {
	t.Helper()
	ctx := t.Context()
	events, err := rd.ReadEvents(ctx, ok)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	var kinds []string
	for i, ev := range events {
		if seq, _ := ev["seq"].(float64); int(seq) != i+1 {
			t.Errorf("event %d has seq %v, want %d", i, ev["seq"], i+1)
		}
		kinds = append(kinds, ev["event_type"].(string))
	}
	if want := []string{"item", "item", "item", "artifact", "run_complete"}; !slices.Equal(kinds, want) {
		t.Errorf("run-ok-1 events = %v, want %v", kinds, want)
	}
	m, err := qlode.QueryLatestMetrics(ctx, factory, "quarry", "run-ok-1", "")
	if err != nil || m["run_id"] != "run-ok-1" || m["events_persisted_total"] != float64(5) {
		t.Errorf("metrics of run-ok-1 = %v, %v; want 5 events persisted", m, err)
	}
	files, err := qlode.QuerySidecarFiles(ctx, factory, "quarry", "run-ok-1")
	if err != nil || len(files) != 1 || files[0].Filename != "page.png" {
		t.Errorf("sidecar files of run-ok-1 = %+v, %v; want page.png", files, err)
	}
}

// pruneFailedRun prunes failed runs at now and checks that only run-fail-1
// went and that the run appendRun wrote took its place in the listing.
func pruneFailedRun(t *testing.T, rd *reader.Reader, factory lode.Factory[lode.RangeStore], now time.Time) {
	t.Helper()
	zero := time.Duration(0)
	pruned, err := retention.Prune(t.Context(), "quarry", factory, retention.Options{
		Policy: retention.Policy{Failed: &zero}, SweepGrace: time.Nanosecond, Now: now,
	})
	if err != nil || pruned.RunsPruned != 1 || pruned.Runs[0].RunID != "run-fail-1" || pruned.Sweep == nil || len(pruned.Sweep.Unparseable) != 0 {
		t.Fatalf("prune = %+v, %v; want run-fail-1 pruned and a clean sweep", pruned, err)
	}
	if n := len(listRuns(t, rd)); n != len(fixtureStates) {
		t.Errorf("after the prune %d runs are listed, want %d: three from the fixture and one new", n, len(fixtureStates))
	}
}

// readArtifact opens art-1 of run and checks its bytes.
func readArtifact(t *testing.T, rd *reader.Reader, run reader.Run, want string) {
	t.Helper()
	a, r, err := rd.OpenArtifact(t.Context(), run, "art-1")
	if err != nil {
		t.Fatalf("open art-1 of %s: %v", run.RunID, err)
	}
	got := make([]byte, a.SizeBytes)
	if _, err := r.ReadAt(got, 0); err != nil {
		t.Fatalf("read art-1 of %s: %v", run.RunID, err)
	}
	if string(got) != want {
		t.Errorf("art-1 of %s = %q, want %q", run.RunID, got, want)
	}
}

// appendRun writes a run with this release into the fixture's dataset.
func appendRun(t *testing.T, root string) {
	t.Helper()
	ctx := t.Context()
	started := time.Date(2026, 10, 3, 1, 0, 0, 0, time.UTC)
	cfg := qlode.Config{Dataset: "quarry", Source: "shop", Category: "products", Day: qlode.DeriveDay(started), RunID: "run-new-1", Policy: "strict"}
	c, err := qlode.NewLodeClient(ctx, cfg, root)
	if err != nil {
		t.Fatalf("open a client on the fixture dataset: %v", err)
	}
	if err := c.WriteStartRecord(ctx, &types.RunMeta{Attempt: 1}, started, nil); err != nil {
		t.Fatalf("start record: %v", err)
	}
	ev := &types.EventEnvelope{
		ContractVersion: types.ContractVersion, EventID: "run-new-1-e1", RunID: cfg.RunID, Seq: 1,
		Type: types.EventTypeRunComplete, Ts: started.Format(time.RFC3339), Payload: map[string]any{}, Attempt: 1,
	}
	if err := c.WriteEvents(ctx, cfg.Dataset, cfg.RunID, []*types.EventEnvelope{ev}); err != nil {
		t.Fatalf("write events: %v", err)
	}
	if err := c.WriteMetrics(ctx, metrics.Snapshot{RunID: cfg.RunID}, started); err != nil {
		t.Fatalf("write metrics: %v", err)
	}
	end := qlode.RunEnd{EndedAt: started, Outcome: types.OutcomeSuccess, Terminal: types.EventTypeRunComplete, EventCount: 1, MetricsPersisted: true}
	if err := c.WriteEndRecord(ctx, end); err != nil {
		t.Fatalf("end record: %v", err)
	}
}

func listRuns(t *testing.T, rd *reader.Reader) []reader.Run {
	t.Helper()
	page, err := rd.ListRuns(t.Context(), reader.Selector{}, reader.ListOptions{})
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	return page.Runs
}

func findRun(t *testing.T, rd *reader.Reader, source, category, day, runID string) reader.Run {
	t.Helper()
	run, err := rd.FindRun(t.Context(), reader.Selector{Source: source, Category: category, Day: day}, runID)
	if err != nil {
		t.Fatalf("find %s: %v", runID, err)
	}
	return run
}
