package retention

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/lode/lode"
	"github.com/pithecene/lode/lode/volume"

	qlode "github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/reader"
	"github.com/pithecene/quarry/types"
)

// writeArtifact stores one artifact of four bytes as a Volume of the run.
func (f *fixture) writeArtifact(c *qlode.LodeClient, id string) {
	f.t.Helper()
	ctx := f.t.Context()
	w, err := c.OpenArtifact(ctx, id, 4, "")
	if err != nil {
		f.t.Fatalf("open artifact: %v", err)
	}
	if err := w.WriteRange(ctx, 0, []byte("data")); err != nil {
		f.t.Fatalf("write range: %v", err)
	}
	if _, err := w.Finish(ctx); err != nil {
		f.t.Fatalf("finish: %v", err)
	}
}

// volumeKeys lists every key under the run's artifact directory.
func (f *fixture) volumeKeys(cfg qlode.Config) []string {
	f.t.Helper()
	keys, err := f.store.List(f.t.Context(), qlode.RunArtifactDir(cfg))
	if err != nil && !errors.Is(err, lode.ErrNotFound) {
		f.t.Fatalf("list: %v", err)
	}
	slices.Sort(keys)
	return keys
}

// pointers is the head pointer key of each of the run's artifacts.
func pointers(cfg qlode.Config, ids ...string) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, qlode.RunArtifactDir(cfg)+"volumes/"+id+"/latest")
	}
	return out
}

func TestPrune_RemovesArtifactVolumes(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	cfg := f.write(runSpec{source: "src", runID: "r1", day: oldDay, outcome: types.OutcomeScriptError, artifacts: 2})
	f.write(runSpec{source: "src", runID: "r2", day: recentDay, outcome: types.OutcomeSuccess, artifacts: 1})
	if n := len(f.volumeKeys(cfg)); n <= 2 {
		t.Fatalf("the run has %d volume keys, want more than its pointers", n)
	}

	dry := f.prune(Options{Policy: Policy{Failed: days(7)}, DryRun: true})
	if dry.Volumes != 2 || dry.Runs[0].Volumes != 2 || len(f.volumeKeys(cfg)) <= 2 {
		t.Fatalf("dry run = %+v, keys = %v", dry, f.volumeKeys(cfg))
	}

	report := f.prune(Options{Policy: Policy{Failed: days(7)}})
	if got, want := outcomes(report), "[r1:pruned]"; got != want || report.Volumes != 2 || report.Runs[0].Volumes != 2 {
		t.Fatalf("report = %+v", report)
	}
	// A removed Volume keeps its pointer as the fence that keeps it removed.
	if got, want := f.volumeKeys(cfg), pointers(cfg, "a1", "a2"); !slices.Equal(got, want) {
		t.Fatalf("volume keys = %v, want %v", got, want)
	}
	vol, err := volume.New("a1", lode.Prefixed(lode.Use(f.store), qlode.RunArtifactDir(cfg)), 4)
	if err != nil {
		t.Fatalf("volume: %v", err)
	}
	if _, err := vol.Latest(t.Context()); !errors.Is(err, volume.ErrRemoved) {
		t.Fatalf("Latest = %v, want ErrRemoved", err)
	}
	if left := f.keys(cfg); len(left) != 0 {
		t.Fatalf("run left %v", left)
	}
	if again := f.prune(Options{Policy: Policy{Failed: days(7)}}); again.RunsScanned != 1 || len(again.Runs) != 0 {
		t.Fatalf("rerun = %+v", again)
	}
}

// A Volume whose pointer names a snapshot without a manifest has a commit
// in flight: Lode fences it and deletes nothing, so the run is kept.
func TestPrune_KeepsARunWhoseVolumeHasACommitInFlight(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	cfg := f.write(runSpec{source: "src", runID: "r1", day: oldDay, outcome: types.OutcomeScriptError})
	f.write(runSpec{source: "src", runID: "r2", day: recentDay, outcome: types.OutcomeSuccess}) // holds the head
	if err := f.store.Put(t.Context(), pointers(cfg, "a1")[0], strings.NewReader(pendingSnapshot())); err != nil {
		t.Fatalf("put: %v", err)
	}
	// The dry run reports what the removal does.
	for _, dryRun := range []bool{true, false} {
		report := f.prune(Options{Policy: Policy{Failed: days(7)}, DryRun: dryRun})
		if got, want := outcomes(report), "[r1:kept]"; got != want || report.Runs[0].Kept != keptVolumePending || report.Volumes != 0 {
			t.Fatalf("dry run %v: report = %+v", dryRun, report)
		}
	}
	start := qlode.RunRecordDir(cfg) + qlode.StartRecordName
	if _, err := f.store.Get(t.Context(), start); err != nil {
		t.Fatalf("start record removed: %v", err)
	}
}

// openDuring is a store that writes one more artifact the first time the
// prune lists a run's Volumes: after the listing the removal acts on and
// before the one that checks it.
type openDuring struct {
	lode.RangeStore
	open func()
	done bool
}

func (s *openDuring) List(ctx context.Context, prefix string) ([]string, error) {
	keys, err := s.RangeStore.List(ctx, prefix)
	if !s.done && strings.HasSuffix(prefix, "/volumes/") {
		s.done = true
		s.open()
	}
	return keys, err
}

func TestPrune_KeepsARunWhoseVolumeWasWrittenDuringThePrune(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	cfg := f.write(runSpec{source: "src", runID: "r1", day: oldDay, outcome: types.OutcomeScriptError, artifacts: 1})
	f.write(runSpec{source: "src", runID: "r2", day: recentDay, outcome: types.OutcomeSuccess}) // holds the head
	store := &openDuring{RangeStore: f.store}
	store.open = func() {
		c, err := qlode.NewLodeClientWithFactory(t.Context(), cfg, f.factory)
		if err != nil {
			t.Fatalf("client: %v", err)
		}
		f.writeArtifact(c, "a2")
	}
	factory := func() (lode.RangeStore, error) { return store, nil }
	report, err := Prune(t.Context(), "quarry", factory, Options{Policy: Policy{Failed: days(7)}, Now: now})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if got, want := outcomes(report), "[r1:kept]"; got != want || report.Runs[0].Kept != keptVolumeWritten || report.Runs[0].Volumes != 1 {
		t.Fatalf("report = %+v", report)
	}
	start := qlode.RunRecordDir(cfg) + qlode.StartRecordName
	if _, err := f.store.Get(t.Context(), start); err != nil {
		t.Fatalf("start record removed: %v", err)
	}
	// The next prune removes what remains: the Volume opened meanwhile, as
	// the first is removed already.
	if again := f.prune(Options{Policy: Policy{Failed: days(7)}}); outcomes(again) != "[r1:pruned]" || again.Volumes != 1 {
		t.Fatalf("rerun = %+v", again)
	}
}

func TestPrune_SweepRemovesOrphanedVolumes(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	kept := f.write(runSpec{source: "src", runID: "r1", day: oldDay, outcome: types.OutcomeSuccess, artifacts: 1})
	orphan := f.write(runSpec{source: "src", runID: "r2", day: oldDay, outcome: types.OutcomeScriptError, artifacts: 2})
	recent := f.write(runSpec{source: "src", runID: "r3", day: now.Format(time.DateOnly), outcome: types.OutcomeScriptError, artifacts: 1})
	// An orphan whose Volume holds a key Lode never wrote: the Volume's
	// directory is Lode's, so the key goes with the Volume.
	strayRun := f.write(runSpec{source: "src", runID: "r6", day: oldDay, outcome: types.OutcomeScriptError, artifacts: 1})
	stray := qlode.RunArtifactDir(strayRun) + "volumes/a1/stray"
	if err := f.store.Put(t.Context(), stray, strings.NewReader("x")); err != nil {
		t.Fatalf("put: %v", err)
	}
	for _, cfg := range []qlode.Config{orphan, recent, strayRun} {
		dir := qlode.RunRecordDir(cfg)
		for _, name := range []string{qlode.StartRecordName, qlode.EndRecordName} {
			if err := f.store.Delete(t.Context(), dir+name); err != nil {
				t.Fatalf("delete: %v", err)
			}
		}
	}
	// Neither key is one the write path produces: a day in another form,
	// and an ID that Lode reserves, which volume.Remove would refuse.
	reserved := qlode.ArtifactPrefix("quarry") + "src/cat/" + oldDay + "/r8/volumes/.lode-tmp-x/latest"
	malformed := qlode.ArtifactPrefix("quarry") + "src/cat/not-a-day/r9/volumes/a1/latest"
	for _, key := range []string{reserved, malformed} {
		if err := f.store.Put(t.Context(), key, strings.NewReader("x")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	unparseable := []string{reserved, malformed}
	// A Volume of a run without records whose commit is in flight: the
	// sweep leaves it, and the dry run counts it as left.
	pending := qlode.Config{Dataset: "quarry", Source: "src", Category: "cat", Day: oldDay, RunID: "r7"}
	if err := f.store.Put(t.Context(), pointers(pending, "a1")[0], strings.NewReader(pendingSnapshot())); err != nil {
		t.Fatalf("put: %v", err)
	}

	dry := f.prune(Options{Policy: Policy{Succeeded: days(90)}, SweepGrace: 24 * time.Hour, DryRun: true})
	if dry.Sweep == nil || dry.Sweep.Volumes != 3 || !slices.Equal(dry.Sweep.Unparseable, unparseable) {
		t.Fatalf("dry sweep = %+v", dry.Sweep)
	}
	if got, want := f.volumeKeys(orphan), pointers(orphan, "a1", "a2"); len(got) <= len(want) {
		t.Fatalf("dry run removed volume objects: %v", got)
	}

	report := f.prune(Options{Policy: Policy{Succeeded: days(90)}, SweepGrace: 24 * time.Hour})
	if report.Sweep == nil || report.Sweep.Volumes != 3 || !slices.Equal(report.Sweep.Unparseable, unparseable) {
		t.Fatalf("sweep = %+v", report.Sweep)
	}
	if got, want := f.volumeKeys(orphan), pointers(orphan, "a1", "a2"); !slices.Equal(got, want) {
		t.Fatalf("orphan volume keys = %v, want %v", got, want)
	}
	if got, want := f.volumeKeys(strayRun), pointers(strayRun, "a1"); !slices.Equal(got, want) {
		t.Fatalf("stray volume keys = %v, want %v", got, want)
	}
	for name, cfg := range map[string]qlode.Config{"with a start record": kept, "within the grace period": recent} {
		if got := f.volumeKeys(cfg); len(got) <= 1 {
			t.Errorf("the run %s lost its volume: %v", name, got)
		}
	}
	for _, key := range unparseable {
		if _, err := f.store.Get(t.Context(), key); err != nil {
			t.Errorf("unparseable key %s removed: %v", key, err)
		}
	}
}

func TestArtifactKey(t *testing.T) {
	t.Parallel()
	cases := []struct {
		rel  string
		key  reader.Key
		id   string
		ok   bool
		note string
	}{
		{"src/cat/2026-10-01/r1/volumes/a1/latest", reader.Key{Source: "src", Category: "cat", Day: "2026-10-01", RunID: "r1"}, "a1", true, "a pointer"},
		{"s%20p/cat/2026-10-01/r1/volumes/a1/snapshots/1-0123456789abcdef/manifest.json", reader.Key{Source: "s p", Category: "cat", Day: "2026-10-01", RunID: "r1"}, "a1", true, "an encoded component"},
		{"src/cat/2026-10-01/r1/volumes/a1", reader.Key{}, "", false, "no object within the volume"},
		{"src/cat/2026-10-01/r1/files/a1/x", reader.Key{}, "", false, "not under volumes"},
		{"src/cat/not-a-day/r1/volumes/a1/latest", reader.Key{}, "", false, "a day in another form"},
		{"s p/cat/2026-10-01/r1/volumes/a1/latest", reader.Key{}, "", false, "an unencoded component"},
		{"src/cat/2026-10-01/r1/volumes//latest", reader.Key{}, "", false, "an empty artifact ID"},
		{"src/cat/2026-10-01/r1/volumes/.lode-tmp-x/latest", reader.Key{}, "", false, "an ID that Lode reserves"},
	}
	for _, tc := range cases {
		key, id, ok := artifactKey(tc.rel)
		if ok != tc.ok || key != tc.key || id != tc.id {
			t.Errorf("artifactKey(%q) = %+v, %q, %v; want %+v, %q, %v (%s)", tc.rel, key, id, ok, tc.key, tc.id, tc.ok, tc.note)
		}
	}
}

// pendingSnapshot is a generated snapshot ID, 20 days old, that no manifest
// names: a head pointer holding it marks a commit in flight.
func pendingSnapshot() string {
	return fmt.Sprintf("%d-0123456789abcdef", today.AddDate(0, 0, -20).UnixNano())
}

// listHook is a store that runs fn once, the first time the prune lists a
// key prefix ending in suffix, after the listing.
type listHook struct {
	lode.RangeStore
	suffix string
	fn     func()
	done   bool
}

func (s *listHook) List(ctx context.Context, prefix string) ([]string, error) {
	keys, err := s.RangeStore.List(ctx, prefix)
	if !s.done && strings.HasSuffix(prefix, s.suffix) {
		s.done = true
		s.fn()
	}
	return keys, err
}

// A Volume that commits, or an object the write path never produced that
// appears, after the prune read the run's Volumes keeps the run's records.
// The sidecar listing falls between that read and the removal.
func TestPrune_KeepsARunWhoseVolumesChangeBeforeTheRemoval(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		kept string
		put  func(cfg qlode.Config) (key, content string)
	}{
		{"a commit moves a head", keptVolumeWritten, func(cfg qlode.Config) (string, string) {
			return pointers(cfg, "a1")[0], fmt.Sprintf("%d-fedcba9876543210", today.UnixNano())
		}},
		{"an unknown object appears", keptVolumeUnknown, func(cfg qlode.Config) (string, string) {
			return qlode.RunArtifactDir(cfg) + "volumes/.lode-tmp-x/latest", "x"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			cfg := f.write(runSpec{source: "src", runID: "r1", day: oldDay, outcome: types.OutcomeScriptError, artifacts: 1})
			f.write(runSpec{source: "src", runID: "r2", day: recentDay, outcome: types.OutcomeSuccess}) // holds the head
			store := &listHook{RangeStore: f.store, suffix: "/files/", fn: func() {
				key, content := tc.put(cfg)
				if err := f.store.Delete(t.Context(), key); err != nil && !errors.Is(err, lode.ErrNotFound) {
					t.Fatalf("delete: %v", err)
				}
				if err := f.store.Put(t.Context(), key, strings.NewReader(content)); err != nil {
					t.Fatalf("put: %v", err)
				}
			}}
			factory := func() (lode.RangeStore, error) { return store, nil }
			report, err := Prune(t.Context(), "quarry", factory, Options{Policy: Policy{Failed: days(7)}, Now: now})
			if err != nil {
				t.Fatalf("Prune: %v", err)
			}
			if got, want := outcomes(report), "[r1:kept]"; got != want || report.Runs[0].Kept != tc.kept {
				t.Fatalf("report = %+v", report)
			}
			start := qlode.RunRecordDir(cfg) + qlode.StartRecordName
			if _, err := f.store.Get(t.Context(), start); err != nil {
				t.Fatalf("start record removed: %v", err)
			}
		})
	}
}


// On the filesystem store a pointer swap leaves a lock companion beside the
// pointer. Retention never reads below a Volume's name, so the Volume is
// removed there as on any store, and the sweep removes an orphan's.
func TestPrune_RemovesArtifactVolumesOnTheFilesystem(t *testing.T) {
	t.Parallel()
	store, err := lode.NewFS(t.TempDir())
	if err != nil {
		t.Fatalf("fs: %v", err)
	}
	f := &fixture{t: t, store: store, factory: func() (lode.RangeStore, error) { return store, nil }}
	cfg := f.write(runSpec{source: "src", runID: "r1", day: oldDay, outcome: types.OutcomeScriptError, artifacts: 2})
	orphan := f.write(runSpec{source: "src", runID: "r2", day: oldDay, outcome: types.OutcomeScriptError, artifacts: 1})
	f.write(runSpec{source: "src", runID: "r3", day: recentDay, outcome: types.OutcomeSuccess}) // holds the head
	for _, name := range []string{qlode.StartRecordName, qlode.EndRecordName} {
		if err := store.Delete(t.Context(), qlode.RunRecordDir(orphan)+name); err != nil {
			t.Fatalf("delete: %v", err)
		}
	}
	report := f.prune(Options{Policy: Policy{Failed: days(7)}, SweepGrace: 24 * time.Hour})
	if got, want := outcomes(report), "[r1:pruned]"; got != want || report.Volumes != 2 || report.Sweep == nil || report.Sweep.Volumes != 1 {
		t.Fatalf("report = %+v, sweep = %+v", report, report.Sweep)
	}
	if left := f.keys(cfg); len(left) != 0 {
		t.Fatalf("run left %v", left)
	}
}

// A name under a run's volumes directory that is no Volume keeps the run,
// and nothing of it is deleted: a name Lode reserves, which no artifact ID
// can take, or an object stored at an artifact ID itself, which lies
// outside the directory volume.Remove deletes.
func TestPrune_KeepsARunWithAnUnknownArtifactObject(t *testing.T) {
	t.Parallel()
	for _, name := range []string{".lode-tmp-x/latest", "a1"} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			cfg := f.write(runSpec{source: "src", runID: "r1", day: oldDay, outcome: types.OutcomeScriptError, artifacts: 1})
			f.write(runSpec{source: "src", runID: "r2", day: recentDay, outcome: types.OutcomeSuccess}) // holds the head
			if err := f.store.Put(t.Context(), qlode.RunArtifactDir(cfg)+"volumes/"+name, strings.NewReader("x")); err != nil {
				t.Fatalf("put: %v", err)
			}
			before := f.volumeKeys(cfg)
			report := f.prune(Options{Policy: Policy{Failed: days(7)}})
			if got, want := outcomes(report), "[r1:kept]"; got != want || report.Runs[0].Kept != keptVolumeUnknown {
				t.Fatalf("report = %+v", report)
			}
			if after := f.volumeKeys(cfg); !slices.Equal(before, after) {
				t.Fatalf("volume keys changed: %v -> %v", before, after)
			}
		})
	}
}

// An orphan's Volumes are due only once the end of its day is strictly
// older than the grace period.
func TestOrphanDue_GraceBoundary(t *testing.T) {
	t.Parallel()
	day := "2026-10-01"
	end := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	grace := 24 * time.Hour
	cases := []struct {
		now  time.Time
		want bool
	}{
		{end.Add(grace - time.Nanosecond), false},
		{end.Add(grace), false},
		{end.Add(grace + time.Nanosecond), true},
	}
	for _, tc := range cases {
		p := &pruner{dataset: "quarry", opts: Options{Now: tc.now}, store: lode.NewMemory()}
		cfg := qlode.Config{Dataset: "quarry", Source: "src", Category: "cat", Day: day, RunID: "r1"}
		got, err := p.orphanDue(t.Context(), cfg, grace)
		if err != nil || got != tc.want {
			t.Errorf("orphanDue at %s = %v, %v; want %v", tc.now, got, err, tc.want)
		}
	}
}
