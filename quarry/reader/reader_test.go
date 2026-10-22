package reader

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pithecene-io/lode/lode"

	qlode "github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/types"
)

// fixture writes runs through the real write path into one memory store.
type fixture struct {
	t       *testing.T
	store   lode.Store
	factory lode.StoreFactory
}

func newFixture(t *testing.T) *fixture {
	store := lode.NewMemory()
	return &fixture{t: t, store: store, factory: func() (lode.Store, error) { return store, nil }}
}

type runSpec struct {
	source, category, day, runID string
	events                       []types.EventType // written in order, seq from 1
	end                          *qlode.RunEnd     // nil: no end record
}

func (f *fixture) write(spec runSpec) {
	f.t.Helper()
	cfg := qlode.Config{Dataset: "quarry", Source: spec.source, Category: spec.category, Day: spec.day, RunID: spec.runID}
	c, err := qlode.NewLodeClientWithFactory(cfg, f.factory)
	if err != nil {
		f.t.Fatalf("client: %v", err)
	}
	ctx := f.t.Context()
	day, err := time.Parse(time.DateOnly, spec.day)
	if err != nil {
		f.t.Fatalf("day: %v", err)
	}
	if err := c.WriteStartRecord(ctx, &types.RunMeta{RunID: spec.runID, Attempt: 1}, day.Add(time.Hour)); err != nil {
		f.t.Fatalf("start: %v", err)
	}
	// Write events in two batches, out of seq order across event types, so
	// ReadEvents has to order them.
	for i := len(spec.events) - 1; i >= 0; i-- {
		ev := &types.EventEnvelope{
			ContractVersion: types.ContractVersion, EventID: fmt.Sprintf("e%d", i+1), RunID: spec.runID,
			Seq: int64(i + 1), Type: spec.events[i], Ts: "2026-10-21T01:00:00Z", Payload: map[string]any{"n": i + 1}, Attempt: 1,
		}
		if err := c.WriteEvents(ctx, "quarry", spec.runID, []*types.EventEnvelope{ev}); err != nil {
			f.t.Fatalf("events: %v", err)
		}
	}
	if err := c.WriteChunks(ctx, "quarry", spec.runID, []*types.ArtifactChunk{{ArtifactID: "a1", Seq: 1, IsLast: true, Data: []byte("x")}}); err != nil {
		f.t.Fatalf("chunks: %v", err)
	}
	if err := c.WriteMetrics(ctx, metricsSnapshot(spec.runID), time.Now()); err != nil {
		f.t.Fatalf("metrics: %v", err)
	}
	if spec.end != nil {
		if err := c.WriteEndRecord(ctx, *spec.end); err != nil {
			f.t.Fatalf("end: %v", err)
		}
	}
}

func (f *fixture) reader() *Reader {
	f.t.Helper()
	r, err := New("quarry", f.factory)
	if err != nil {
		f.t.Fatalf("New: %v", err)
	}
	return r
}

func ended(outcome types.OutcomeStatus, terminal types.EventType) *qlode.RunEnd {
	return &qlode.RunEnd{EndedAt: time.Date(2026, 10, 21, 2, 0, 0, 0, time.UTC), Outcome: outcome, Terminal: terminal, EventCount: 2, MetricsPersisted: true}
}

func seedRuns(t *testing.T) *fixture {
	f := newFixture(t)
	f.write(runSpec{"src", "cat", "2026-10-21", "r1", []types.EventType{types.EventTypeItem, types.EventTypeRunComplete}, ended(types.OutcomeSuccess, types.EventTypeRunComplete)})
	f.write(runSpec{"src", "cat", "2026-10-21", "r2", []types.EventType{types.EventTypeItem, types.EventTypeRunError}, ended(types.OutcomeScriptError, types.EventTypeRunError)})
	f.write(runSpec{"src", "cat", "2026-10-22", "r3", []types.EventType{types.EventTypeItem}, nil})
	f.write(runSpec{"src", "other", "2026-10-21", "r4", []types.EventType{types.EventTypeItem}, ended(types.OutcomeSuccess, types.EventTypeRunComplete)})
	f.write(runSpec{"a/b", "cat", "2026-10-21", "r5", []types.EventType{types.EventTypeItem}, nil})
	return f
}

func runIDs(p Page) []string {
	out := make([]string, 0, len(p.Runs))
	for _, r := range p.Runs {
		out = append(out, r.RunID)
	}
	return out
}

func TestListRuns_SelectorsStatesAndOrder(t *testing.T) {
	t.Parallel()
	r := seedRuns(t).reader()
	ctx := t.Context()
	cases := []struct {
		name string
		sel  Selector
		opts ListOptions
		want string
	}{
		{"whole dataset", Selector{}, ListOptions{}, "[r5 r1 r2 r3 r4]"},
		{"source", Selector{Source: "src"}, ListOptions{}, "[r1 r2 r3 r4]"},
		{"source and category", Selector{Source: "src", Category: "cat"}, ListOptions{}, "[r1 r2 r3]"},
		{"source, category, day", Selector{Source: "src", Category: "cat", Day: "2026-10-21"}, ListOptions{}, "[r1 r2]"},
		{"encoded source", Selector{Source: "a/b"}, ListOptions{}, "[r5]"},
		{"succeeded", Selector{Source: "src"}, ListOptions{State: StateSucceeded}, "[r1 r4]"},
		{"failed", Selector{Source: "src"}, ListOptions{State: StateFailed}, "[r2]"},
		{"unterminated", Selector{Source: "src"}, ListOptions{State: StateUnterminated}, "[r3]"},
		{"unknown source", Selector{Source: "nope"}, ListOptions{}, "[]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := r.ListRuns(ctx, tc.sel, tc.opts)
			if err != nil {
				t.Fatalf("ListRuns: %v", err)
			}
			if got := fmt.Sprint(runIDs(p)); got != tc.want {
				t.Fatalf("runs = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestListRuns_RunFields(t *testing.T) {
	t.Parallel()
	r := seedRuns(t).reader()
	p, err := r.ListRuns(t.Context(), Selector{Source: "src", Category: "cat"}, ListOptions{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	r1, r3 := p.Runs[0], p.Runs[2]
	if r1.State != StateSucceeded || *r1.Outcome != "success" || *r1.Terminal != "run_complete" || *r1.EventCount != 2 {
		t.Errorf("r1 = %+v", r1)
	}
	if r1.Partition != "source=src/category=cat/day=2026-10-21/run_id=r1/" || r1.StartedAt != "2026-10-21T01:00:00Z" {
		t.Errorf("r1 start fields = %q %q", r1.Partition, r1.StartedAt)
	}
	if r3.State != StateUnterminated || r3.EndedAt != nil || r3.Outcome != nil || r3.EventCount != nil {
		t.Errorf("unterminated r3 should have null end fields: %+v", r3)
	}
}

func TestListRuns_Paging(t *testing.T) {
	t.Parallel()
	r := seedRuns(t).reader()
	ctx := t.Context()
	var got []string
	var after string
	pages := 0
	for {
		p, err := r.ListRuns(ctx, Selector{Source: "src"}, ListOptions{Limit: 2, After: after})
		if err != nil {
			t.Fatalf("ListRuns: %v", err)
		}
		pages++
		got = append(got, runIDs(p)...)
		if p.Next == "" {
			break
		}
		after = p.Next
	}
	if fmt.Sprint(got) != "[r1 r2 r3 r4]" || pages != 2 {
		t.Fatalf("paged = %v in %d pages, want [r1 r2 r3 r4] in 2", got, pages)
	}
}

func TestListRuns_Errors(t *testing.T) {
	t.Parallel()
	r := seedRuns(t).reader()
	ctx := t.Context()
	if _, err := r.ListRuns(ctx, Selector{Category: "cat"}, ListOptions{}); !errors.Is(err, ErrSelectorOrder) {
		t.Errorf("category without source: %v", err)
	}
	if _, err := r.ListRuns(ctx, Selector{Source: "src", Day: "2026-10-21"}, ListOptions{}); !errors.Is(err, ErrSelectorOrder) {
		t.Errorf("day without category: %v", err)
	}
	if _, err := r.ListRuns(ctx, Selector{Source: "src"}, ListOptions{After: "datasets/elsewhere/x"}); !errors.Is(err, ErrInvalidCursor) {
		t.Errorf("foreign cursor: %v", err)
	}
	if _, err := r.ListRuns(ctx, Selector{}, ListOptions{State: "running"}); !errors.Is(err, ErrInvalidState) {
		t.Errorf("running state: %v", err)
	}
	if _, err := New("../x", seedRuns(t).factory); !errors.Is(err, qlode.ErrInvalidRunKey) {
		t.Errorf("unsafe dataset: %v", err)
	}
}

// Stores normalize "." and ".." segments, which would widen a prefix
// beyond the selected runs.
func TestListRuns_RejectsDotSelectors(t *testing.T) {
	t.Parallel()
	r := seedRuns(t).reader()
	for _, sel := range []Selector{{Source: ".."}, {Source: "src", Category: "cat", Day: ".."}, {Source: "src/../x"}} {
		if _, err := r.ListRuns(t.Context(), sel, ListOptions{}); !errors.Is(err, qlode.ErrInvalidRunKey) {
			t.Errorf("selector %+v: err = %v, want ErrInvalidRunKey", sel, err)
		}
	}
	if _, err := r.FindRun(t.Context(), Selector{Source: "src", Category: "cat", Day: "2026-10-21"}, ".."); !errors.Is(err, qlode.ErrInvalidRunKey) {
		t.Errorf("FindRun dot run id: %v", err)
	}
}

// A cursor must be a start-record key: an arbitrary path under the prefix
// would silently skip runs.
func TestListRuns_RejectsNonStartKeyCursor(t *testing.T) {
	t.Parallel()
	r := seedRuns(t).reader()
	for _, after := range []string{
		"datasets/quarry/runs/src/z",
		"datasets/quarry/runs/src/cat/2026-10-21/r1/end.json",
		"datasets/quarry/runs/src/cat/2026-10-21/start.json",
	} {
		if _, err := r.ListRuns(t.Context(), Selector{Source: "src"}, ListOptions{After: after}); !errors.Is(err, ErrInvalidCursor) {
			t.Errorf("cursor %q: err = %v, want ErrInvalidCursor", after, err)
		}
	}
}

// With a state filter, a page that fills on the last match carries no
// cursor: there is no further matching run.
func TestListRuns_NoCursorWhenNoFurtherMatch(t *testing.T) {
	t.Parallel()
	r := seedRuns(t).reader()
	p, err := r.ListRuns(t.Context(), Selector{Source: "src"}, ListOptions{State: StateFailed, Limit: 1})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if fmt.Sprint(runIDs(p)) != "[r2]" || p.Next != "" {
		t.Fatalf("page = %v next %q, want [r2] and no cursor", runIDs(p), p.Next)
	}
	p, err = r.ListRuns(t.Context(), Selector{Source: "src"}, ListOptions{State: StateSucceeded, Limit: 1})
	if err != nil || fmt.Sprint(runIDs(p)) != "[r1]" || p.Next == "" {
		t.Fatalf("page = %v next %q err %v, want [r1] with a cursor (r4 matches)", runIDs(p), p.Next, err)
	}
}

// An end record without a start record (a start write that failed and never
// landed) is ignored: runs are listed by their start record only.
func TestListRuns_IgnoresEndWithoutStart(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	cfg := qlode.Config{Dataset: "quarry", Source: "src", Category: "cat", Day: "2026-10-21", RunID: "orphan"}
	c, _ := qlode.NewLodeClientWithFactory(cfg, f.factory)
	if err := c.WriteEndRecord(t.Context(), *ended(types.OutcomeExecutorCrash, "")); err != nil {
		t.Fatalf("end: %v", err)
	}
	p, err := f.reader().ListRuns(t.Context(), Selector{}, ListOptions{})
	if err != nil || len(p.Runs) != 0 {
		t.Fatalf("runs = %v err = %v, want none", runIDs(p), err)
	}
}

func TestReadEvents_OrderedEventsOnly(t *testing.T) {
	t.Parallel()
	r := seedRuns(t).reader()
	ctx := t.Context()
	p, _ := r.ListRuns(ctx, Selector{Source: "src", Category: "cat", Day: "2026-10-21"}, ListOptions{})
	evs, err := r.ReadEvents(ctx, p.Runs[0]) // r1: item, run_complete
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(evs) != 2 {
		t.Fatalf("got %d events, want 2 (no chunks, no metrics): %v", len(evs), evs)
	}
	if evs[0]["type"] != "item" || evs[1]["type"] != "run_complete" {
		t.Fatalf("order = %v, %v; want item, run_complete", evs[0]["type"], evs[1]["type"])
	}
	for _, e := range evs {
		if e["run_id"] != "r1" {
			t.Fatalf("event from another run: %v", e)
		}
	}
}

// r1 and r10 share a raw prefix; the partition prefix ends with "/" so one
// run's events never include another's.
func TestReadEvents_NoPrefixBleed(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.write(runSpec{"src", "cat", "2026-10-21", "r1", []types.EventType{types.EventTypeItem}, nil})
	f.write(runSpec{"src", "cat", "2026-10-21", "r10", []types.EventType{types.EventTypeItem, types.EventTypeItem}, nil})
	r := f.reader()
	p, _ := r.ListRuns(t.Context(), Selector{}, ListOptions{})
	evs, err := r.ReadEvents(t.Context(), p.Runs[0])
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if p.Runs[0].RunID != "r1" || len(evs) != 1 {
		t.Fatalf("run %s has %d events, want r1 with 1", p.Runs[0].RunID, len(evs))
	}
}

func metricsSnapshot(runID string) metrics.Snapshot {
	return metrics.Snapshot{RunID: runID, Policy: "strict", Executor: "test", StorageBackend: "fs"}
}

func TestNew_RejectsInvalidFactory(t *testing.T) {
	t.Parallel()
	if _, err := New("quarry", nil); err == nil {
		t.Error("nil factory accepted")
	}
	if _, err := New("quarry", func() (lode.Store, error) { return nil, nil }); err == nil {
		t.Error("nil store accepted")
	}
}

// A factory that returns a new store per call must not split listing and
// event reads across stores.
func TestReadEvents_UsesTheListingStore(t *testing.T) {
	t.Parallel()
	f := seedRuns(t)
	calls := 0
	r, err := New("quarry", func() (lode.Store, error) {
		calls++
		if calls == 1 {
			return f.store, nil
		}
		return lode.NewMemory(), nil
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p, _ := r.ListRuns(t.Context(), Selector{Source: "src", Category: "cat", Day: "2026-10-21"}, ListOptions{})
	evs, err := r.ReadEvents(t.Context(), p.Runs[0])
	if err != nil || len(evs) != 2 {
		t.Fatalf("events = %d err %v, want 2 from the listing store", len(evs), err)
	}
}
