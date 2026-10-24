package retention

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/lode/lode"

	qlode "github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/reader"
	"github.com/pithecene/quarry/types"
)

// Snapshot IDs carry the real time of each write, so the dates are set
// around today: old runs began 20 days ago, and ages are measured 30 days
// from now.
var (
	today     = time.Now().UTC().Truncate(24 * time.Hour)
	oldDay    = today.AddDate(0, 0, -20).Format(time.DateOnly)
	recentDay = today.AddDate(0, 0, -1).Format(time.DateOnly)
	now       = today.AddDate(0, 0, 30)
)

// fixture writes runs through the real write path into one memory store.
type fixture struct {
	t       *testing.T
	store   lode.RangeStore
	factory lode.Factory[lode.RangeStore]
}

func newFixture(t *testing.T) *fixture {
	store := lode.NewMemory()
	return &fixture{t: t, store: store, factory: func() (lode.RangeStore, error) { return store, nil }}
}

type runSpec struct {
	source, runID, day string
	outcome            types.OutcomeStatus // "": no end record
	// lateFile is written after the run's last commit, so no snapshot
	// records it.
	lateFile bool
	// again writes another snapshot for a run that already started, without
	// a second start record or sidecar file.
	again bool
}

func (f *fixture) write(spec runSpec) qlode.Config {
	f.t.Helper()
	cfg := qlode.Config{Dataset: "quarry", Source: spec.source, Category: "cat", Day: spec.day, RunID: spec.runID}
	c, err := qlode.NewLodeClientWithFactory(f.t.Context(), cfg, f.factory)
	if err != nil {
		f.t.Fatalf("client: %v", err)
	}
	ctx := f.t.Context()
	day, err := time.Parse(time.DateOnly, spec.day)
	if err != nil {
		f.t.Fatalf("day: %v", err)
	}
	if spec.again {
		ev := &types.EventEnvelope{
			ContractVersion: types.ContractVersion, EventID: "e2", RunID: spec.runID,
			Seq: 2, Type: types.EventTypeItem, Ts: spec.day + "T01:30:00Z", Payload: map[string]any{"n": 2}, Attempt: 1,
		}
		if err := c.WriteEvents(ctx, "quarry", spec.runID, []*types.EventEnvelope{ev}); err != nil {
			f.t.Fatalf("events: %v", err)
		}
		return cfg
	}
	if err := c.WriteStartRecord(ctx, &types.RunMeta{RunID: spec.runID, Attempt: 1}, day.Add(time.Hour)); err != nil {
		f.t.Fatalf("start: %v", err)
	}
	if err := c.PutFile(ctx, "page.html", "text/html", []byte("<p>")); err != nil {
		f.t.Fatalf("file: %v", err)
	}
	ev := &types.EventEnvelope{
		ContractVersion: types.ContractVersion, EventID: "e1", RunID: spec.runID,
		Seq: 1, Type: types.EventTypeItem, Ts: spec.day + "T01:00:00Z", Payload: map[string]any{"n": 1}, Attempt: 1,
	}
	if err := c.WriteEvents(ctx, "quarry", spec.runID, []*types.EventEnvelope{ev}); err != nil {
		f.t.Fatalf("events: %v", err)
	}
	if spec.lateFile {
		if err := c.PutFile(ctx, "late.txt", "text/plain", []byte("x")); err != nil {
			f.t.Fatalf("late file: %v", err)
		}
	}
	if spec.outcome != "" {
		end := qlode.RunEnd{EndedAt: day.Add(2 * time.Hour), Outcome: spec.outcome, EventCount: 1}
		if spec.outcome == types.OutcomeSuccess {
			end.Terminal = types.EventTypeRunComplete
		}
		if err := c.WriteEndRecord(ctx, end); err != nil {
			f.t.Fatalf("end: %v", err)
		}
	}
	return cfg
}

// keys lists every key in the dataset that names the run.
func (f *fixture) keys(cfg qlode.Config) []string {
	f.t.Helper()
	all, err := f.store.List(f.t.Context(), "datasets/quarry/")
	if err != nil {
		f.t.Fatalf("list: %v", err)
	}
	var out []string
	for _, k := range all {
		if strings.Contains(k, "run_id="+cfg.RunID+"/") || strings.HasPrefix(k, qlode.RunRecordDir(cfg)) {
			out = append(out, k)
		}
	}
	slices.Sort(out) // store listings have no order
	return out
}

func (f *fixture) prune(opts Options) Report {
	f.t.Helper()
	opts.Now = now
	report, err := Prune(f.t.Context(), "quarry", f.factory, opts)
	if err != nil {
		f.t.Fatalf("Prune: %v", err)
	}
	return report
}

func days(n int) *time.Duration {
	d := time.Duration(n) * 24 * time.Hour
	return &d
}

func outcomes(r Report) string {
	out := make([]string, 0, len(r.Runs))
	for _, run := range r.Runs {
		s := run.RunID + ":pruned"
		if !run.Pruned {
			s = run.RunID + ":kept"
		}
		out = append(out, s)
	}
	return fmt.Sprint(out)
}

// seed writes an old failed, an old unterminated, an old succeeded, and a
// recent succeeded run; the recent one commits last and holds the head.
func seed(t *testing.T) (*fixture, map[string]qlode.Config) {
	f := newFixture(t)
	cfgs := map[string]qlode.Config{
		"failed":       f.write(runSpec{source: "src", runID: "r1", day: oldDay, outcome: types.OutcomeScriptError, lateFile: true}),
		"unterminated": f.write(runSpec{source: "src", runID: "r2", day: oldDay}),
		"succeeded":    f.write(runSpec{source: "src", runID: "r3", day: oldDay, outcome: types.OutcomeSuccess}),
		"head":         f.write(runSpec{source: "src", runID: "r4", day: recentDay, outcome: types.OutcomeSuccess}),
	}
	return f, cfgs
}

func TestPrune_RemovesSelectedRunsEntirely(t *testing.T) {
	t.Parallel()
	f, cfgs := seed(t)
	report := f.prune(Options{Policy: Policy{Failed: days(7), Unterminated: days(7)}})

	if got, want := outcomes(report), "[r1:pruned r2:pruned]"; got != want {
		t.Fatalf("runs = %s, want %s", got, want)
	}
	if report.RunsScanned != 4 || report.NotDue != 2 || report.RunsPruned != 2 || report.Snapshots != 2 {
		t.Fatalf("report = %+v", report)
	}
	// r1: page.html, late.txt, and their companions; r2: page.html and its companion.
	if report.SidecarObjects != 6 {
		t.Fatalf("sidecar objects = %d, want 6", report.SidecarObjects)
	}
	for _, name := range []string{"failed", "unterminated"} {
		if left := f.keys(cfgs[name]); len(left) != 0 {
			t.Errorf("%s run left %v", name, left)
		}
	}
	for _, name := range []string{"succeeded", "head"} {
		if len(f.keys(cfgs[name])) == 0 {
			t.Errorf("%s run was removed", name)
		}
	}
	if again := f.prune(Options{Policy: Policy{Failed: days(7), Unterminated: days(7)}}); again.RunsScanned != 2 || len(again.Runs) != 0 {
		t.Fatalf("rerun = %+v", again)
	}
}

func TestPrune_DryRunRemovesNothing(t *testing.T) {
	t.Parallel()
	f, cfgs := seed(t)
	before := f.keys(cfgs["failed"])
	report := f.prune(Options{Policy: Policy{Failed: days(7)}, DryRun: true})

	if got, want := outcomes(report), "[r1:pruned]"; got != want {
		t.Fatalf("runs = %s, want %s", got, want)
	}
	if report.Snapshots != 1 || report.Objects == 0 || report.SidecarObjects != 4 {
		t.Fatalf("report = %+v", report)
	}
	if after := f.keys(cfgs["failed"]); !slices.Equal(before, after) {
		t.Fatalf("dry run changed keys: %v -> %v", before, after)
	}
}

func TestPrune_KeepsTheRunHoldingTheHead(t *testing.T) {
	t.Parallel()
	f, cfgs := seed(t)
	before := f.keys(cfgs["head"])
	report := f.prune(Options{Policy: Policy{Succeeded: days(0)}})

	if got, want := outcomes(report), "[r3:pruned r4:kept]"; got != want {
		t.Fatalf("runs = %s, want %s", got, want)
	}
	if report.Runs[1].Kept != keptHead {
		t.Fatalf("kept = %q", report.Runs[1].Kept)
	}
	if after := f.keys(cfgs["head"]); !slices.Equal(before, after) {
		t.Fatalf("head run changed: %v -> %v", before, after)
	}
}

func TestPrune_AgeThreshold(t *testing.T) {
	t.Parallel()
	f, _ := seed(t)
	// The failed run ended 50 days before now, less two hours.
	if r := f.prune(Options{Policy: Policy{Failed: days(50)}, DryRun: true}); len(r.Runs) != 0 {
		t.Fatalf("a run younger than the threshold was selected: %s", outcomes(r))
	}
	if r := f.prune(Options{Policy: Policy{Failed: days(49)}, DryRun: true}); outcomes(r) != "[r1:pruned]" {
		t.Fatalf("a run past the threshold was not selected: %s", outcomes(r))
	}
	// The unterminated run started 50 days before now but last wrote
	// within the last 31 days, so its age runs from that write.
	if r := f.prune(Options{Policy: Policy{Unterminated: days(31)}, DryRun: true}); len(r.Runs) != 0 {
		t.Fatalf("a run that wrote recently was selected: %s", outcomes(r))
	}
	if r := f.prune(Options{Policy: Policy{Unterminated: days(28)}, DryRun: true}); outcomes(r) != "[r2:pruned]" {
		t.Fatalf("a run idle past the threshold was not selected: %s", outcomes(r))
	}
}

func TestPrune_LiveRunThatStartedLongAgoIsNotDue(t *testing.T) {
	t.Parallel()
	f, cfgs := seed(t)
	// Measured now, the unterminated run started 20 days ago and wrote today.
	report, err := Prune(t.Context(), "quarry", f.factory, Options{Policy: Policy{Unterminated: days(7)}, Now: time.Now()})
	if err != nil || len(report.Runs) != 0 || report.NotDue != 4 {
		t.Fatalf("report = %+v, err = %v", report, err)
	}
	if len(f.keys(cfgs["unterminated"])) == 0 {
		t.Fatal("live run removed")
	}
}

func TestPrune_OneRun(t *testing.T) {
	t.Parallel()
	f, cfgs := seed(t)
	sel := reader.Selector{Source: "src", Category: "cat", Day: oldDay}
	report := f.prune(Options{Selector: sel, RunID: "r3", Policy: Policy{Succeeded: days(0)}})
	if got, want := outcomes(report), "[r3:pruned]"; got != want {
		t.Fatalf("runs = %s, want %s", got, want)
	}
	if left := f.keys(cfgs["succeeded"]); len(left) != 0 {
		t.Fatalf("run left %v", left)
	}
	// Pruning a run that is gone is not an error.
	if again := f.prune(Options{Selector: sel, RunID: "r3", Policy: Policy{Succeeded: days(0)}}); again.RunsScanned != 0 {
		t.Fatalf("rerun = %+v", again)
	}
}

func TestPrune_RecordForAnotherKeyIsKept(t *testing.T) {
	t.Parallel()
	f, cfgs := seed(t)
	// Copy r1's start record under r9's key: it describes r1, not r9.
	ctx := t.Context()
	rc, err := f.store.Get(ctx, qlode.RunRecordDir(cfgs["failed"])+qlode.StartRecordName)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	other := cfgs["failed"]
	other.RunID = "r9"
	if err := f.store.Put(ctx, qlode.RunRecordDir(other)+qlode.StartRecordName, rc); err != nil {
		t.Fatalf("put: %v", err)
	}
	report := f.prune(Options{Policy: Policy{Unterminated: days(7)}, DryRun: true})
	if got, want := outcomes(report), "[r2:pruned r9:kept]"; got != want {
		t.Fatalf("runs = %s, want %s", got, want)
	}
	if report.Runs[1].Kept != keptMismatch {
		t.Fatalf("kept = %q", report.Runs[1].Kept)
	}
}

func TestPrune_EncodedKeyUsesRecordedSidecarsOnly(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	cfg := f.write(runSpec{source: "a/b", runID: "r1", day: oldDay, outcome: types.OutcomeScriptError, lateFile: true})
	f.write(runSpec{source: "src", runID: "head", day: recentDay, outcome: types.OutcomeSuccess})

	report := f.prune(Options{Policy: Policy{Failed: days(7)}})
	if got, want := outcomes(report), "[r1:pruned]"; got != want {
		t.Fatalf("runs = %s, want %s", got, want)
	}
	// Only the recorded page.html and its companion: the prefix of an
	// encoded key is not listed, so the unrecorded late file remains.
	if report.SidecarObjects != 2 {
		t.Fatalf("sidecar objects = %d, want 2", report.SidecarObjects)
	}
	left, err := f.store.List(t.Context(), qlode.SidecarFilesPrefix(cfg))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	slices.Sort(left)
	if want := []string{qlode.SidecarFilesPrefix(cfg) + "late.txt", qlode.SidecarFilesPrefix(cfg) + "late.txt" + metaSuffix}; !slices.Equal(left, want) {
		t.Fatalf("left %v, want %v", left, want)
	}
}

func TestPrune_Sweep(t *testing.T) {
	t.Parallel()
	f, _ := seed(t)
	// Data of a write that never committed, begun 20 days ago.
	began := today.AddDate(0, 0, -20).Add(time.Hour).UnixNano()
	orphan := fmt.Sprintf("datasets/quarry/partitions/source=src/category=cat/day=%s/run_id=rx/event_type=item/segments/%d-0123456789abcdef/data/data", oldDay, began)
	if err := f.store.Put(t.Context(), orphan, strings.NewReader("{}\n")); err != nil {
		t.Fatalf("put: %v", err)
	}
	report := f.prune(Options{Policy: Policy{Failed: days(7)}, SweepGrace: 24 * time.Hour})
	if report.Sweep == nil || report.Sweep.Orphans != 1 {
		t.Fatalf("sweep = %+v, want one orphan", report.Sweep)
	}
	if _, err := f.store.Get(t.Context(), orphan); !errors.Is(err, lode.ErrNotFound) {
		t.Fatalf("orphan still readable: %v", err)
	}
}

func TestPrune_RejectsUnsafeOptions(t *testing.T) {
	t.Parallel()
	neg := -time.Hour
	cases := map[string]Options{
		"no state":             {Now: now},
		"negative age":         {Now: now, Policy: Policy{Failed: &neg}},
		"unterminated too new": {Now: now, Policy: Policy{Unterminated: days(0)}},
		"negative grace":       {Now: now, Policy: Policy{Failed: days(1)}, SweepGrace: -time.Second},
		"run without day":      {Now: now, Policy: Policy{Failed: days(1)}, RunID: "r1"},
		"run with day only":    {Now: now, Policy: Policy{Failed: days(1)}, RunID: "r1", Selector: reader.Selector{Day: oldDay}},
		"no time":              {Policy: Policy{Failed: days(1)}},
	}
	for name, opts := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Prune(t.Context(), "quarry", newFixture(t).factory, opts)
			if !errors.Is(err, ErrInvalidOptions) {
				t.Fatalf("err = %v, want ErrInvalidOptions", err)
			}
		})
	}
}

func TestPrune_SidecarListingStaysInsideTheRun(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.write(runSpec{source: "src", runID: "r", day: oldDay, outcome: types.OutcomeScriptError})
	// A run whose ID continues r's sidecar prefix.
	nested := f.write(runSpec{source: "src", runID: "r/files/x", day: oldDay, outcome: types.OutcomeSuccess, lateFile: true})
	f.write(runSpec{source: "src", runID: "head", day: recentDay, outcome: types.OutcomeSuccess})
	before := f.keys(nested)

	if r := f.prune(Options{Policy: Policy{Failed: days(7)}}); r.RunsPruned != 1 || r.SidecarObjects != 2 {
		t.Fatalf("report = %+v", r)
	}
	all, err := f.store.List(t.Context(), qlode.SidecarFilesPrefix(nested))
	if err != nil || len(all) != 4 {
		t.Fatalf("nested run's sidecars = %v, %v; keys before %v", all, err, before)
	}
}

func TestPrune_DeletesOrphanedEndRecords(t *testing.T) {
	t.Parallel()
	f, cfgs := seed(t)
	start := qlode.RunRecordDir(cfgs["failed"]) + qlode.StartRecordName
	if err := f.store.Delete(t.Context(), start); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if r := f.prune(Options{Policy: Policy{Failed: days(7)}, DryRun: true}); r.OrphanEndRecords != 1 {
		t.Fatalf("dry run = %+v", r)
	}
	if r := f.prune(Options{Policy: Policy{Failed: days(7)}}); r.OrphanEndRecords != 1 {
		t.Fatalf("prune = %+v", r)
	}
	end := qlode.RunRecordDir(cfgs["failed"]) + qlode.EndRecordName
	if _, err := f.store.Get(t.Context(), end); !errors.Is(err, lode.ErrNotFound) {
		t.Fatalf("end record remains: %v", err)
	}
}

func TestPrune_UsesOneStore(t *testing.T) {
	t.Parallel()
	f, cfgs := seed(t)
	calls := 0
	factory := func() (lode.RangeStore, error) {
		calls++
		if calls > 1 {
			return lode.NewMemory(), nil // a separate, empty store per call
		}
		return f.store, nil
	}
	if _, err := Prune(t.Context(), "quarry", factory, Options{Policy: Policy{Failed: days(7)}, Now: now}); err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if left := f.keys(cfgs["failed"]); len(left) != 0 {
		t.Fatalf("run left in the listed store: %v", left)
	}
}

func TestLastWrite(t *testing.T) {
	t.Parallel()
	cases := []struct {
		ids  []lode.DatasetSnapshotID
		want int64
		ok   bool
	}{
		{nil, 0, true},
		{[]lode.DatasetSnapshotID{"100-0123456789abcdef", "300-0123456789abcdef", "200"}, 300, true},
		{[]lode.DatasetSnapshotID{"100", "x-1"}, 0, false},
		{[]lode.DatasetSnapshotID{"100-xyz"}, 0, false},
		{[]lode.DatasetSnapshotID{"00100-0123456789abcdef"}, 0, false},
		{[]lode.DatasetSnapshotID{"100-0123456789ABCDEF"}, 0, false},
	}
	for _, tc := range cases {
		got, ok := lastWrite(tc.ids)
		if ok != tc.ok || (ok && len(tc.ids) > 0 && got.UnixNano() != tc.want) {
			t.Errorf("lastWrite(%v) = %v, %v; want %d, %v", tc.ids, got.UnixNano(), ok, tc.want, tc.ok)
		}
	}
}

func TestPrune_OneRunReportsItsMalformedKey(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	dir := qlode.RunRecordPrefix("quarry", "src", "cat", oldDay)
	bad := dir + "a b/start.json" // a run ID the write path would encode
	if err := f.store.Put(t.Context(), bad, strings.NewReader("{}")); err != nil {
		t.Fatalf("put: %v", err)
	}
	report := f.prune(Options{Selector: reader.Selector{Source: "src", Category: "cat", Day: oldDay}, RunID: "a b", Policy: Policy{Failed: days(0)}})
	if fmt.Sprint(report.Unparseable) != fmt.Sprint([]string{bad}) || report.RunsScanned != 0 {
		t.Fatalf("report = %+v", report)
	}
}

// commitDuring is a store that runs commit once, the first time the prune
// lists a sidecar prefix: after the preview expiry and before the real one.
type commitDuring struct {
	lode.RangeStore
	commit func()
	done   bool
}

func (s *commitDuring) List(ctx context.Context, prefix string) ([]string, error) {
	if !s.done && strings.HasSuffix(prefix, "/files/") {
		s.done = true
		s.commit()
	}
	return s.RangeStore.List(ctx, prefix)
}

// A run that commits after the preview keeps that snapshot and its records,
// even when another run then takes the head.
func TestPrune_KeepsASnapshotCommittedAfterThePreview(t *testing.T) {
	t.Parallel()
	f, cfgs := seed(t)
	store := &commitDuring{RangeStore: f.store}
	store.commit = func() {
		f.write(runSpec{source: "src", runID: "r1", day: oldDay, outcome: types.OutcomeScriptError, again: true})
		f.write(runSpec{source: "src", runID: "r5", day: recentDay, outcome: types.OutcomeSuccess})
	}
	factory := func() (lode.RangeStore, error) { return store, nil }
	report, err := Prune(t.Context(), "quarry", factory, Options{Policy: Policy{Failed: days(7)}, Now: now})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if got, want := outcomes(report), "[r1:kept]"; got != want || report.Runs[0].Kept != keptCommitted {
		t.Fatalf("runs = %s (%+v), want %s", got, report.Runs, want)
	}
	start := qlode.RunRecordDir(cfgs["failed"]) + qlode.StartRecordName
	if _, err := f.store.Get(t.Context(), start); err != nil {
		t.Fatalf("start record removed: %v", err)
	}
}

// A sidecar inventory in another shape names files that cannot be found, so
// the run is kept rather than losing them.
func TestPrune_KeepsARunWithAMalformedSidecarInventory(t *testing.T) {
	t.Parallel()
	f, cfgs := seed(t)
	ctx := t.Context()
	var manifest string
	for _, k := range f.keys(cfgs["failed"]) {
		if strings.HasSuffix(k, "/manifest.json") {
			manifest = k
		}
	}
	rc, err := f.store.Get(ctx, manifest)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := f.store.Delete(ctx, manifest); err != nil {
		t.Fatalf("delete: %v", err)
	}
	broken := strings.Replace(string(body), `"path"`, `"location"`, 1)
	if err := f.store.Put(ctx, manifest, strings.NewReader(broken)); err != nil {
		t.Fatalf("put: %v", err)
	}
	report := f.prune(Options{Policy: Policy{Failed: days(7)}})
	if got, want := outcomes(report), "[r1:kept]"; got != want || report.Runs[0].Kept != keptInventory {
		t.Fatalf("runs = %s (%+v), want %s", got, report.Runs, want)
	}
	if len(f.keys(cfgs["failed"])) == 0 {
		t.Fatal("run removed")
	}
}

func TestSidecarRefPaths(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		raw  any
		want []string
		ok   bool
	}{
		"null":          {nil, nil, false},
		"empty":         {[]any{}, []string{}, true},
		"paths":         {[]any{map[string]any{"path": "a"}, map[string]any{"path": "b"}}, []string{"a", "b"}, true},
		"not a list":    {"a", nil, false},
		"entry not map": {[]any{"a"}, nil, false},
		"no path":       {[]any{map[string]any{"filename": "a"}}, nil, false},
		"empty path":    {[]any{map[string]any{"path": ""}}, nil, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, ok := sidecarRefPaths(tc.raw)
			if ok != tc.ok || !slices.Equal(got, tc.want) {
				t.Fatalf("got %v, %v; want %v, %v", got, ok, tc.want, tc.ok)
			}
		})
	}
}
