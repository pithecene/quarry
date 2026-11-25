package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pithecene/lode/lode"

	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/types"
)

func TestValidateRunKey(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name                             string
		dataset, source, category, runID string
		ok                               bool
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
	t.Parallel()

	cfg := Config{Dataset: "quarry", Source: "a/b", Category: "c d", Day: "2026-10-21", RunID: "run/1"}
	want := "datasets/quarry/runs/a%2Fb/c%20d/2026-10-21/run%2F1/"
	if got := RunRecordDir(cfg); got != want {
		t.Fatalf("RunRecordDir = %q, want %q", got, want)
	}
}

// The start record's partition must address the partitions Lode actually
// writes, so readers never re-derive keys.
func TestPartitionPrefix_MatchesLodeHiveLayout(t *testing.T) {
	t.Parallel()

	cfg := Config{Dataset: "quarry", Source: "a/b", Category: "c d", Day: "2026-10-21", RunID: "run-1"}
	factory := sharedMemoryFactory()
	client, err := NewLodeClientWithFactory(t.Context(), cfg, factory)
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
	t.Parallel()

	cfg := Config{Dataset: "quarry", Source: "src", Category: "cat", Day: "2026-10-21", RunID: "run-1"}
	factory := sharedMemoryFactory()
	client, err := NewLodeClientWithFactory(t.Context(), cfg, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory: %v", err)
	}
	job := "job-1"
	meta := &types.RunMeta{RunID: cfg.RunID, JobID: &job, Attempt: 1}
	startedAt := time.Date(2026, 10, 21, 8, 30, 0, 123, time.UTC)
	if err := client.WriteStartRecord(t.Context(), meta, startedAt, nil); err != nil {
		t.Fatalf("WriteStartRecord: %v", err)
	}
	end := RunEnd{
		EndedAt: startedAt.Add(time.Minute), Outcome: types.OutcomeScriptError,
		Terminal: types.EventTypeRunError, Message: "boom", EventCount: 7,
		Metrics: &metrics.Snapshot{
			EventsPersisted: 7, Policy: "strict",
			DroppedByType: map[string]int64{"log": 2}, FlushTriggers: map[string]int64{"count": 4},
		},
		TerminalSummary: json.RawMessage(`{"error_type":"fetch"}`),
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
		"partition":  "source=src/category=cat/day=2026-10-21/run_id=run-1/",
		"started_at": "2026-10-21T08:30:00.000000123Z", "quarry_version": types.Version, "backfilled": false,
	} {
		if start[k] != want {
			t.Errorf("start[%q] = %v, want %v", k, start[k], want)
		}
	}
	if _, ok := start["parent_run_id"]; ok {
		t.Error("parent_run_id should be omitted for an initial run")
	}
	if _, ok := start["deadline_at"]; ok {
		t.Error("deadline_at should be omitted for a run without a max duration")
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
	recorded, _ := endRec["metrics"].(map[string]any)
	dropped, _ := recorded["dropped_by_type"].(map[string]any)
	if recorded["events_persisted_total"] != float64(7) || recorded["policy"] != "strict" || dropped["log"] != float64(2) {
		t.Errorf("end metrics = %v, want the run's counters and dimensions", recorded)
	}
	if triggers, _ := recorded["flush_triggers"].(map[string]any); triggers["count"] != float64(4) {
		t.Errorf("end flush_triggers = %v, want the run's", recorded["flush_triggers"])
	}
	for _, key := range []string{"record_kind", "event_type", "ts", "source", "category", "day"} {
		if _, ok := recorded[key]; ok {
			t.Errorf("end metrics carry %q, which belongs to the metrics record", key)
		}
	}
	if summary, _ := endRec["terminal_summary"].(map[string]any); summary["error_type"] != "fetch" {
		t.Errorf("end terminal_summary = %v, want the terminal event's payload", endRec["terminal_summary"])
	}
}

// nested returns a payload of the given depth.
func nested(depth int) map[string]any {
	m := map[string]any{}
	for range depth {
		m = map[string]any{"m": m}
	}
	return m
}

// The terminal payload keeps the difference between none and an empty one, and
// one that JSON cannot represent does not stop the record. A run that collects
// no metrics records none.
func TestNewEndRecord_TerminalSummary(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		payload map[string]any
		want    any
		present bool
	}{
		{"no terminal event", nil, nil, false},
		{"an empty payload", map[string]any{}, map[string]any{}, true},
		{"a payload", map[string]any{"pages": 3}, map[string]any{"pages": float64(3)}, true},
		{"a payload that JSON cannot represent", map[string]any{"ratio": math.NaN()}, nil, false},
		{"a payload nested too deeply to read back", nested(9999), nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var end RunEnd
			if tc.payload != nil {
				// The caller writes the record without a summary that is refused.
				end.TerminalSummary, _ = EncodeTerminalSummary(tc.payload)
			}
			data, err := json.Marshal(NewEndRecord(Config{RunID: "run-1"}, end))
			if err != nil {
				t.Fatal(err)
			}
			var rec map[string]any
			if err := json.Unmarshal(data, &rec); err != nil {
				t.Fatal(err)
			}
			got, present := rec["terminal_summary"]
			if present != tc.present || !reflect.DeepEqual(got, tc.want) {
				t.Errorf("terminal_summary = %v (present %t), want %v (present %t)", got, present, tc.want, tc.present)
			}
			if _, ok := rec["metrics"]; ok {
				t.Error("metrics present for a run that collects none")
			}
		})
	}
}

// The end record names every sidecar file of the run, whether or not a snapshot
// names it too, and run details report each once. A record that names another
// run's file is malformed.
func TestEndRecord_SidecarFiles(t *testing.T) {
	t.Parallel()
	cfg := Config{Dataset: "quarry", Source: "src", Category: "cat", Day: "2026-10-21", RunID: "run-1", Policy: "strict"}
	factory := sharedMemoryFactory()
	client, err := NewLodeClientWithFactory(t.Context(), cfg, factory)
	if err != nil {
		t.Fatal(err)
	}
	put := func(name string) {
		if err := client.PutFile(t.Context(), name, "text/plain", []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	put("a.txt")
	if err := writeLegacyMetrics(t.Context(), client, metrics.Snapshot{RunID: cfg.RunID, Policy: "strict"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	put("late.txt")
	if err := client.WriteEndRecord(t.Context(), RunEnd{EndedAt: time.Now(), Outcome: types.OutcomeSuccess}); err != nil {
		t.Fatal(err)
	}
	store, err := factory()
	if err != nil {
		t.Fatal(err)
	}
	names := func(files []SidecarFileRef) string {
		var out []string
		for _, f := range files {
			out = append(out, f.Filename)
		}
		return strings.Join(out, ",")
	}
	recorded, err := ReadEndSidecars(t.Context(), store, cfg)
	if err != nil || names(recorded) != "a.txt,late.txt" {
		t.Errorf("end record files %s, %v, want a.txt,late.txt", names(recorded), err)
	}
	details, err := QueryRunDetails(t.Context(), factory, cfg)
	if err != nil || names(details.SidecarFiles) != "a.txt,late.txt" {
		t.Errorf("details files %s, %v, want each file once", names(details.SidecarFiles), err)
	}

	// A run without a snapshot has its files too.
	bare, bareFactory := cfg, sharedMemoryFactory()
	bareClient, err := NewLodeClientWithFactory(t.Context(), bare, bareFactory)
	if err != nil {
		t.Fatal(err)
	}
	if err := bareClient.PutFile(t.Context(), "only.txt", "text/plain", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := bareClient.WriteEndRecord(t.Context(), RunEnd{EndedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if details, err := QueryRunDetails(t.Context(), bareFactory, bare); err != nil || names(details.SidecarFiles) != "only.txt" {
		t.Errorf("details files %s, %v, want only.txt without a snapshot", names(details.SidecarFiles), err)
	}

	other := cfg
	other.RunID = "run-2"
	twice := cfg
	twice.RunID = "run-3"
	ref := SidecarFileRef{Path: SidecarFilesPrefix(twice) + "a.txt", Filename: "a.txt", ContentType: "text/plain"}
	if err := putRunRecord(t.Context(), store, twice, EndRecordName, EndRecord{SidecarFiles: []SidecarFileRef{ref, ref}}); err != nil {
		t.Fatal(err)
	}
	if files, err := ReadEndSidecars(t.Context(), store, twice); err != nil || names(files) != "a.txt" {
		t.Errorf("a repeated entry gave %s, %v, want it once", names(files), err)
	}
	foreign := EndRecord{RunID: other.RunID, SidecarFiles: recorded}
	if err := putRunRecord(t.Context(), store, other, EndRecordName, foreign); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadEndSidecars(t.Context(), store, other); !errors.Is(err, ErrMalformedSidecars) {
		t.Errorf("a foreign path gave %v, want ErrMalformedSidecars", err)
	}
}

// companionFailingStore fails companion writes while failing is set.
type companionFailingStore struct {
	lode.RangeStore
	failing bool
}

var errCompanion = errors.New("companion refused")

func (s *companionFailingStore) Put(ctx context.Context, key string, r io.Reader) error {
	if s.failing && strings.HasSuffix(key, ".meta.json") {
		return errCompanion
	}
	return s.RangeStore.Put(ctx, key, r)
}

// uncertainStore reports an error after it persists a key with the suffix, and
// its reads fail while unreadable is set.
type uncertainStore struct {
	lode.RangeStore
	suffix     string
	unreadable bool
	loses      bool // fails the write without persisting it
}

var errUncertain = errors.New("no response")

func (s *uncertainStore) Put(ctx context.Context, key string, r io.Reader) error {
	if s.loses && strings.HasSuffix(key, s.suffix) {
		return errUncertain
	}
	if err := s.RangeStore.Put(ctx, key, r); err != nil || !strings.HasSuffix(key, s.suffix) {
		return err
	}
	return errUncertain
}

func (s *uncertainStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if s.unreadable {
		return nil, errUncertain
	}
	return s.RangeStore.Get(ctx, key)
}

// uncertainClient returns a client on store and the keys that it reconciled.
func uncertainClient(t *testing.T, store *uncertainStore) (*LodeClient, *[]string) {
	t.Helper()
	observed := new([]string)
	client, err := NewLodeClientWithFactory(t.Context(), Config{
		Dataset: "quarry", Source: "s", Category: "c", Day: "2026-02-03", RunID: "run-1",
		ReconcileObserver: func(key string, _ error) { *observed = append(*observed, key) },
	}, sharedFactory(lode.NewMemory()))
	if err != nil {
		t.Fatal(err)
	}
	if store != nil {
		client.store = store
	}
	return client, observed
}

// A companion write that reports an error after it persisted succeeds by
// readback. One that the readback cannot settle leaves the data, whose name
// stays taken.
func TestPutFile_ReconcilesAnUncertainCompanion(t *testing.T) {
	t.Parallel()
	for _, unreadable := range []bool{false, true} {
		t.Run(fmt.Sprintf("unreadable=%t", unreadable), func(t *testing.T) {
			store := &uncertainStore{RangeStore: lode.NewMemory(), suffix: ".meta.json", unreadable: unreadable}
			client, observed := uncertainClient(t, store)
			put := func(contentType string) error {
				return client.PutFile(t.Context(), "page.png", contentType, []byte("png"))
			}
			err := put("image/png")
			if unreadable {
				if !errors.Is(err, errUncertain) || len(client.files) != 0 {
					t.Fatalf("first PutFile: %v with files %v, want the write's error and no file", err, client.files)
				}
				store.unreadable = false
				for _, contentType := range []string{"image/gif", "image/png"} {
					if err := put(contentType); !errors.Is(err, ErrPathExists) || len(client.files) != 0 {
						t.Fatalf("retry: %v with files %v, want ErrPathExists and no file", err, client.files)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("PutFile: %v", err)
			}
			key := SidecarFilesPrefix(client.config) + "page.png"
			if len(client.files) != 1 || !slices.Contains(*observed, key+".meta.json") {
				t.Errorf("files = %v, observed = %v, want one file and its companion", client.files, *observed)
			}
			var companion map[string]any
			readJSON(t, store.RangeStore, key+".meta.json", &companion)
			if companion["content_type"] != "image/png" || companion["run_id"] != "run-1" {
				t.Errorf("companion = %v, want the content type and the run's ID", companion)
			}
			if err := put("image/png"); !errors.Is(err, ErrPathExists) {
				t.Errorf("second write: %v, want ErrPathExists", err)
			}
		})
	}
}

// A data write that reports an error succeeds only when the data reads back.
// Otherwise it is no file of the run, and what is stored stays.
func TestPutFile_ReconcilesUncertainData(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, before string // the bytes that another write stored first
		unreadable   bool
		persists     bool
		want         error
		stored       string
	}{
		{"persisted", "", false, true, nil, "png"},
		{"persisted and unreadable", "", true, true, errUncertain, "png"},
		{"lost", "", false, false, errUncertain, ""},
		{"different bytes", "other", false, false, errUncertain, "other"},
		{"a prefix of the bytes", "pn", false, false, errUncertain, "pn"},
		{"more than the bytes", "pngs", false, false, errUncertain, "pngs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &uncertainStore{RangeStore: lode.NewMemory(), suffix: "page.png", unreadable: tc.unreadable, loses: !tc.persists}
			client, observed := uncertainClient(t, store)
			key := SidecarFilesPrefix(client.config) + "page.png"
			if tc.before != "" {
				if err := store.RangeStore.Put(t.Context(), key, strings.NewReader(tc.before)); err != nil {
					t.Fatal(err)
				}
			}
			err := client.PutFile(t.Context(), "page.png", "image/png", []byte("png"))
			if !errors.Is(err, tc.want) || (err == nil) != (len(client.files) == 1) || (err == nil) != slices.Equal(*observed, []string{key}) {
				t.Errorf("PutFile: %v with files %v and observed %v, want %v", err, client.files, *observed, tc.want)
			}
			stored := ""
			if rc, err := store.RangeStore.Get(t.Context(), key); err == nil {
				defer iox.DiscardClose(rc)
				data, err := io.ReadAll(rc)
				if err != nil {
					t.Fatal(err)
				}
				stored = string(data)
			}
			if stored != tc.stored {
				t.Errorf("stored = %q, want %q", stored, tc.stored)
			}
			// The data names no run, so an existing path is refused without a read.
			if tc.before != "" {
				reads := &equalUploadStore{RangeStore: store.RangeStore, strict: true}
				fresh, _ := uncertainClient(t, nil)
				for _, c := range []*LodeClient{fresh, client} {
					c.store = reads
					if err := c.PutFile(t.Context(), "page.png", "image/png", []byte(tc.before)); !errors.Is(err, ErrPathExists) || len(c.files) != 0 {
						t.Errorf("another write's bytes: %v with files %v, want ErrPathExists and no file", err, c.files)
					}
				}
				if reads.reads != 0 {
					t.Errorf("read the data %d times, want none", reads.reads)
				}
				client.store = store
			}
			store.unreadable, store.suffix = false, "none"
			if err := client.PutFile(t.Context(), "other.txt", "text/plain", []byte("x")); err != nil {
				t.Errorf("later write: %v", err)
			}
		})
	}
}

// blockingPutStore blocks the first Put of data until release is closed.
type blockingPutStore struct {
	lode.RangeStore
	blocked          atomic.Bool
	entered, release chan struct{}
}

func (s *blockingPutStore) Put(ctx context.Context, key string, r io.Reader) error {
	if !s.blocked.Swap(true) {
		close(s.entered)
		<-s.release
	}
	return s.RangeStore.Put(ctx, key, r)
}

// A put in progress reserves its filename and no other.
func TestPutFile_ReservesTheNameOfAPutInProgress(t *testing.T) {
	t.Parallel()
	client, _ := uncertainClient(t, nil)
	store := &blockingPutStore{
		RangeStore: &equalUploadStore{RangeStore: lode.NewMemory()},
		entered:    make(chan struct{}), release: make(chan struct{}),
	}
	client.store = store
	release := sync.OnceFunc(func() { close(store.release) })
	defer release()
	put := func(name string) chan error {
		done := make(chan error, 1)
		go func() { done <- client.PutFile(t.Context(), name, "text/plain", []byte("same")) }()
		return done
	}
	await := func(name string, done chan error) error {
		t.Helper()
		select {
		case err := <-done:
			return err
		case <-time.After(10 * time.Second):
			t.Fatalf("%s is still blocked", name)
			return nil
		}
	}
	first := put("a.txt")
	select {
	case <-store.entered:
	case err := <-first:
		t.Fatalf("the first put returned %v before it reached the store", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the first put did not reach the store")
	}
	if err := await("the same name", put("a.txt")); !errors.Is(err, ErrPathExists) {
		t.Errorf("same name: %v, want ErrPathExists", err)
	}
	if err := await("the other name", put("b.txt")); err != nil {
		t.Errorf("other name: %v", err)
	}
	release()
	if err := await("the first put", first); err != nil || len(client.files) != 2 {
		t.Errorf("first put: %v with files %v, want two files", err, client.files)
	}
}

// equalUploadStore takes a second upload of equal bytes for its own first, as
// Lode's S3 store does, and counts the reads of data files.
type equalUploadStore struct {
	lode.RangeStore
	strict bool // refuse equal bytes too
	reads  int
}

func (s *equalUploadStore) Put(ctx context.Context, key string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	putErr := s.RangeStore.Put(ctx, key, bytes.NewReader(data))
	if s.strict || !errors.Is(putErr, lode.ErrPathExists) {
		return putErr
	}
	rc, err := s.RangeStore.Get(ctx, key)
	if err != nil {
		return err
	}
	defer iox.DiscardClose(rc)
	if stored, err := io.ReadAll(rc); err != nil || !bytes.Equal(stored, data) {
		return putErr
	}
	return nil
}

func (s *equalUploadStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if !strings.HasSuffix(key, ".meta.json") {
		s.reads++
	}
	return s.RangeStore.Get(ctx, key)
}

// No second write of a name removes the first one's file, whichever client made
// it: two runs whose keys collapse to one can share a name.
func TestPutFile_KeepsAnEarlierFileOfEqualBytes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, contentType string
		other             bool
	}{
		{"same client, other type", "text/html", false},
		{"same client, same type", "text/plain", false},
		{"other client", "text/html", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &equalUploadStore{RangeStore: lode.NewMemory()}
			newClient := func(category string) *LodeClient {
				client, err := NewLodeClientWithFactory(t.Context(), Config{
					Dataset: "quarry", Source: "s", Category: category, Day: "2026-02-03", RunID: "run-1",
				}, sharedFactory(lode.NewMemory()))
				if err != nil {
					t.Fatal(err)
				}
				client.store = store
				return client
			}
			first := newClient("c/d")
			second := first
			if tc.other {
				second = newClient("c//d")
			}
			if err := first.PutFile(t.Context(), "a.txt", "text/plain", []byte("same")); err != nil {
				t.Fatal(err)
			}
			if err := second.PutFile(t.Context(), "a.txt", tc.contentType, []byte("same")); !errors.Is(err, ErrPathExists) || len(first.files) != 1 {
				t.Errorf("second write: %v, want ErrPathExists", err)
			}
			if store.reads != 0 {
				t.Errorf("read the data %d times, want none", store.reads)
			}
			key := SidecarFilesPrefix(first.config) + "a.txt"
			var companion map[string]any
			readJSON(t, store.RangeStore, key+".meta.json", &companion)
			if exists, err := store.Exists(t.Context(), key); err != nil || !exists || companion["content_type"] != "text/plain" {
				t.Errorf("data exists = %v, %v with companion %v, want the first file", exists, err, companion)
			}
		})
	}
}

// Another write's data file or companion is never taken for this one's, and
// neither is removed. The data beside another write's companion stays too.
func TestPutFile_RefusesAnotherWritesObject(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, content string }{
		{"page.png", "png"},
		{"page.png.meta.json", `{"content_type":"image/png"}`},
		{"page.png.meta.json", `{"content_type":"image/png","run_id":"run-2"}`},
	} {
		t.Run(tc.name+tc.content, func(t *testing.T) {
			store := lode.NewMemory()
			client, err := NewLodeClientWithFactory(t.Context(), Config{
				Dataset: "quarry", Source: "s", Category: "c", Day: "2026-02-03", RunID: "run-1",
			}, sharedFactory(store))
			if err != nil {
				t.Fatal(err)
			}
			prefix := SidecarFilesPrefix(client.config)
			if err := store.Put(t.Context(), prefix+tc.name, strings.NewReader(tc.content)); err != nil {
				t.Fatal(err)
			}
			if err := client.PutFile(t.Context(), "page.png", "image/png", []byte("png")); !errors.Is(err, ErrPathExists) {
				t.Errorf("PutFile: %v, want ErrPathExists", err)
			}
			keys, err := store.List(t.Context(), prefix)
			if err != nil || !slices.Contains(keys, prefix+tc.name) || !slices.Contains(keys, prefix+"page.png") || len(client.files) != 0 {
				t.Errorf("keys = %v, %v with files %v, want the other write's object and the data", keys, err, client.files)
			}
		})
	}
}

// A file whose companion fails is no file of the run, and its data stays, so its
// name stays taken.
func TestPutFile_KeepsTheDataOfAnIncompleteFile(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name             string
		uncertain, equal bool // the data write's error, and a store that takes equal bytes
	}{
		{"data confirmed", false, false},
		{"data read back", true, false},
		{"a store that takes equal bytes", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := lode.NewMemory()
			store := &companionFailingStore{RangeStore: mem, failing: true}
			client, _ := uncertainClient(t, nil)
			client.store = store
			if tc.uncertain {
				store.RangeStore = &uncertainStore{RangeStore: mem, suffix: "a.txt"}
			}
			key := SidecarFilesPrefix(client.config) + "a.txt"
			put := func(data string) error {
				return client.PutFile(t.Context(), "a.txt", "text/plain", []byte(data))
			}
			if err := put("x"); !errors.Is(err, errCompanion) || len(client.files) != 0 {
				t.Fatalf("PutFile = %v with files %v, want the companion's failure and no file", err, client.files)
			}
			if exists, err := mem.Exists(t.Context(), key); err != nil || !exists {
				t.Fatalf("data exists = %v, %v, want it kept", exists, err)
			}
			store.failing, store.RangeStore = false, mem
			if tc.equal {
				store.RangeStore = &equalUploadStore{RangeStore: mem}
				if err := put("x"); err != nil || len(client.files) != 1 {
					t.Errorf("retry: %v with files %v, want one file", err, client.files)
				}
				return
			}
			for _, data := range []string{"x", "y"} {
				if err := put(data); !errors.Is(err, ErrPathExists) || len(client.files) != 0 {
					t.Errorf("retry of %q: %v with files %v, want ErrPathExists and no file", data, err, client.files)
				}
			}
		})
	}
}

// An end record whose write reports an error after it persisted is written, at
// once by readback or by a repeated call, and one that differs is refused.
func TestWriteEndRecord_ReconcilesAnUncertainWrite(t *testing.T) {
	t.Parallel()
	for _, unreadable := range []bool{false, true} {
		t.Run(fmt.Sprintf("unreadable=%t", unreadable), func(t *testing.T) {
			client, err := NewLodeClientWithFactory(t.Context(), Config{
				Dataset: "quarry", Source: "s", Category: "c", Day: "2026-02-03", RunID: "run-1",
			}, sharedFactory(lode.NewMemory()))
			if err != nil {
				t.Fatal(err)
			}
			store := &uncertainStore{RangeStore: lode.NewMemory(), suffix: EndRecordName, unreadable: unreadable}
			client.store = store
			end := RunEnd{EndedAt: time.Date(2026, 2, 3, 1, 0, 0, 0, time.UTC), Outcome: types.OutcomeSuccess}
			err = client.WriteEndRecord(t.Context(), end)
			if unreadable {
				if !errors.Is(err, errUncertain) {
					t.Fatalf("WriteEndRecord: %v, want the write's error", err)
				}
				store.unreadable = false
				err = client.WriteEndRecord(t.Context(), end)
			}
			if err != nil {
				t.Fatalf("WriteEndRecord: %v", err)
			}
			end.EventCount = 1
			if err := client.WriteEndRecord(t.Context(), end); !errors.Is(err, ErrPathExists) {
				t.Errorf("another end: %v, want ErrPathExists", err)
			}
		})
	}
}

func TestWriteEndRecord_NoTerminalIsNull(t *testing.T) {
	t.Parallel()

	cfg := Config{Dataset: "quarry", Source: "src", Category: "cat", Day: "2026-10-21", RunID: "run-1"}
	factory := sharedMemoryFactory()
	client, _ := NewLodeClientWithFactory(t.Context(), cfg, factory)
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
	t.Parallel()

	cfg := Config{Dataset: "quarry", Source: "src", Category: "cat", Day: "2026-10-21", RunID: "run-1"}
	client, _ := NewLodeClientWithFactory(t.Context(), cfg, lode.Memory())
	meta := &types.RunMeta{RunID: cfg.RunID, Attempt: 1}
	at := time.Date(2026, 10, 21, 8, 0, 0, 0, time.UTC)
	if err := client.WriteStartRecord(t.Context(), meta, at, nil); err != nil {
		t.Fatalf("first WriteStartRecord: %v", err)
	}
	err := client.WriteStartRecord(t.Context(), meta, at, nil)
	// The runtime matches Quarry's sentinel to tell a duplicate run ID apart
	// from a failed write (CONTRACT_RUN.md §Failure Semantics).
	if !errors.Is(err, ErrPathExists) || !errors.Is(err, lode.ErrPathExists) {
		t.Fatalf("second WriteStartRecord err = %v, want ErrPathExists (Quarry and Lode)", err)
	}
}

// sharedMemoryFactory returns a factory that always yields the same memory
// store, so a test can read back what the client wrote.
func sharedMemoryFactory() lode.Factory[lode.RangeStore] {
	store := lode.NewMemory()
	return func() (lode.RangeStore, error) { return store, nil }
}

func readJSON(t *testing.T, store lode.RangeStore, path string, v any) {
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
	t.Parallel()

	cfg := Config{Dataset: "../other", Source: "src", Category: "cat", Day: "2026-10-21", RunID: "run-1"}
	client, err := NewLodeClientWithFactory(t.Context(), cfg, sharedMemoryFactory())
	if err != nil {
		t.Skipf("Lode rejected the dataset itself: %v", err)
	}
	if err := client.WriteStartRecord(t.Context(), &types.RunMeta{RunID: "run-1", Attempt: 1}, time.Now(), nil); !errors.Is(err, ErrInvalidRunKey) {
		t.Fatalf("err = %v, want ErrInvalidRunKey", err)
	}
}

func TestWriteStartRecord_DayChecks(t *testing.T) {
	t.Parallel()

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
			client, _ := NewLodeClientWithFactory(t.Context(), cfg, sharedMemoryFactory())
			if err := client.WriteStartRecord(t.Context(), meta, tc.at, nil); !errors.Is(err, ErrInvalidRunKey) {
				t.Fatalf("err = %v, want ErrInvalidRunKey", err)
			}
		})
	}
}

// A run that declares a max duration states its deadline in the start record.
func TestWriteStartRecord_StatesTheDeadline(t *testing.T) {
	t.Parallel()

	cfg := Config{Dataset: "quarry", Source: "src", Category: "cat", Day: "2026-10-21", RunID: "run-1"}
	factory := sharedMemoryFactory()
	client, err := NewLodeClientWithFactory(t.Context(), cfg, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory: %v", err)
	}
	startedAt := time.Date(2026, 10, 21, 8, 30, 0, 0, time.UTC)
	deadline := startedAt.Add(90 * time.Minute)
	if err := client.WriteStartRecord(t.Context(), &types.RunMeta{RunID: cfg.RunID, Attempt: 1}, startedAt, &deadline); err != nil {
		t.Fatalf("WriteStartRecord: %v", err)
	}
	store, _ := factory()
	var start map[string]any
	readJSON(t, store, RunRecordDir(cfg)+"start.json", &start)
	if got, want := start["deadline_at"], "2026-10-21T10:00:00Z"; got != want {
		t.Errorf("deadline_at = %v, want %v", got, want)
	}
}
