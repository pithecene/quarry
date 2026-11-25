package lode

import (
	"context"
	"encoding/json"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pithecene/lode/lode"

	"github.com/pithecene/quarry/metrics"
)

type listingStore struct {
	lode.RangeStore
	lists []string
	gets  []string
}

func (s *listingStore) List(ctx context.Context, prefix string) ([]string, error) {
	s.lists = append(s.lists, prefix)
	return s.RangeStore.List(ctx, prefix)
}

func (s *listingStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	s.gets = append(s.gets, path)
	return s.RangeStore.Get(ctx, path)
}

func TestQueryRunDetails_ScopesSnapshotReads(t *testing.T) {
	t.Parallel()
	store := lode.NewMemory()
	cfg := Config{
		Dataset: "quarry", Source: "test source", Category: "raw//category",
		Day: "2026-10-22", RunID: "run with spaces",
	}
	client, err := NewLodeClientWithFactory(t.Context(), cfg, sharedFactory(store))
	if err != nil {
		t.Fatal(err)
	}
	// Another run with a similar ID shares the dataset and selectors. The
	// inventory reads its manifest, and nothing reads its data.
	unrelated, err := client.dataset.Write(t.Context(), lode.R(lode.D{
		"source": cfg.Source, "category": cfg.Category, "day": cfg.Day, "run_id": cfg.RunID + "-other",
		"event_type": "metrics", "record_kind": RecordKindMetrics, "policy": "buffered",
	}), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, policy := range []string{"strict", "streaming"} {
		if err := client.PutFile(t.Context(), policy+".txt", "text/plain", []byte(policy)); err != nil {
			t.Fatal(err)
		}
		if err := writeLegacyMetrics(t.Context(), client, metrics.Snapshot{RunID: cfg.RunID, Policy: policy}, time.Now()); err != nil {
			t.Fatal(err)
		}
	}

	// One snapshot can be indexed under multiple event types in the run and
	// also contain data from another run. Its inventory should appear once.
	mixedFile := SidecarFileRef{Filename: "mixed.txt", ContentType: "text/plain", Path: SidecarFilesPrefix(cfg) + "mixed.txt"}
	mixed := []lode.D{
		{"source": cfg.Source, "category": cfg.Category, "day": cfg.Day, "run_id": cfg.RunID, "event_type": "item", "record_kind": RecordKindEvent},
		{"source": cfg.Source, "category": cfg.Category, "day": cfg.Day, "run_id": cfg.RunID, "event_type": "debug", "record_kind": RecordKindEvent},
		{"source": cfg.Source, "category": "other", "day": cfg.Day, "run_id": cfg.RunID, "event_type": "metrics", "record_kind": RecordKindMetrics, "policy": "buffered"},
	}
	if _, err := client.dataset.Write(t.Context(), mixed, map[string]any{MetadataKeySidecarFiles: []SidecarFileRef{mixedFile}}); err != nil {
		t.Fatal(err)
	}

	counted := &listingStore{RangeStore: store}
	details, err := QueryRunDetails(t.Context(), sharedFactory(counted), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(counted.lists); n != 1 {
		t.Errorf("storage listings = %v, want the one inventory", counted.lists)
	}
	for _, path := range counted.gets {
		if strings.Contains(path, "/data/") && (strings.Contains(path, "/"+string(unrelated.ID)+"/") || strings.Contains(path, "/category=other/")) {
			t.Errorf("read unrelated snapshot data: %q", path)
		}
	}
	if details.Policy == nil || *details.Policy != "streaming" {
		t.Errorf("latest metrics policy = %v, want streaming", details.Policy)
	}
	files := details.SidecarFiles
	if len(files) != 3 || files[0].Filename != "strict.txt" || files[1].Filename != "streaming.txt" || files[2].Filename != "mixed.txt" {
		t.Fatalf("sidecar files = %+v, want one entry from each of the three snapshots", files)
	}
	wantPath := "datasets/quarry/partitions/source=test source/category=raw//category/day=2026-10-22/run_id=run with spaces/files/strict.txt"
	if files[0].Path != wantPath {
		t.Errorf("sidecar path = %q, want %q", files[0].Path, wantPath)
	}
}

// The owner check reads a shared snapshot at that snapshot, not at the head:
// a shared snapshot older than the head costs no second read of the head's
// manifest.
func TestQueryRunDetails_ResolvesOwnersAtTheirSnapshot(t *testing.T) {
	t.Parallel()
	store := lode.NewMemory()
	cfg := Config{Dataset: "quarry", Source: "src", Category: "raw", Day: "2026-10-22", RunID: "run-a"}
	other := cfg
	other.Category = "other"
	client, err := NewLodeClientWithFactory(t.Context(), cfg, sharedFactory(store))
	if err != nil {
		t.Fatal(err)
	}
	record := func(k Config) lode.D {
		return lode.D{"source": k.Source, "category": k.Category, "day": k.Day, "run_id": k.RunID, "event_type": "item", "record_kind": RecordKindEvent}
	}
	inventory := []SidecarFileRef{
		{Filename: "a.txt", ContentType: "text/plain", Path: SidecarFilesPrefix(cfg) + "a.txt"},
		{Filename: "o.txt", ContentType: "text/plain", Path: SidecarFilesPrefix(other) + "o.txt"},
	}
	if _, err := client.dataset.Write(t.Context(), []lode.D{record(cfg), record(other)}, map[string]any{MetadataKeySidecarFiles: inventory}); err != nil {
		t.Fatal(err)
	}
	head, err := client.dataset.Write(t.Context(), []lode.D{record(Config{Dataset: "quarry", Source: "zzz", Category: "raw", Day: "2026-10-22", RunID: "run-z"})}, nil)
	if err != nil {
		t.Fatal(err)
	}

	counted := &listingStore{RangeStore: store}
	details, err := QueryRunDetails(t.Context(), sharedFactory(counted), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(details.SidecarFiles) != 1 || details.SidecarFiles[0].Filename != "a.txt" {
		t.Fatalf("sidecar files = %+v, want only a.txt", details.SidecarFiles)
	}
	var headReads int
	for _, path := range counted.gets {
		if strings.Contains(path, string(head.ID)) {
			headReads++
		}
	}
	if headReads != 2 {
		t.Errorf("read the head snapshot %s %d times, want twice, by the details view and the inventory: %v", head.ID, headReads, counted.gets)
	}
}

// A snapshot can hold records of several runs while its inventory lists the
// files of only one of them.
func TestQueryRunDetails_SelectsTheRunsInventory(t *testing.T) {
	t.Parallel()
	base := Config{Dataset: "quarry", Source: "src", Category: "raw//category", Day: "2026-10-22", RunID: "run-a"}
	other := base
	other.Category = "other"
	collapsed := base
	collapsed.Category = "raw/category"
	file := func(cfg Config, name string) SidecarFileRef {
		return SidecarFileRef{Filename: name, ContentType: "text/plain", Path: SidecarFilesPrefix(cfg) + name}
	}
	metricsOf := func(cfg Config) lode.D {
		return lode.D{
			"source": cfg.Source, "category": cfg.Category, "day": cfg.Day, "run_id": cfg.RunID,
			"event_type": "metrics", "record_kind": RecordKindMetrics, "policy": "buffered",
		}
	}
	// snapshot is one write: its records and the inventory its metadata records.
	type snapshot struct {
		records   []lode.D
		inventory []SidecarFileRef
	}
	noRunID := other
	noRunID.RunID = ""
	numericRunID := metricsOf(other)
	numericRunID["run_id"] = 7
	bogus := SidecarFileRef{Filename: "a.txt", ContentType: "text/plain", Path: "datasets/quarry/partitions/bogus/run_id=C/files/a.txt"}
	cases := []struct {
		name      string
		snapshots []snapshot
		query     Config
		want      []SidecarFileRef
		wantErr   string
	}{
		{
			"another run's file in a shared snapshot",
			[]snapshot{{[]lode.D{metricsOf(base), metricsOf(other)}, []SidecarFileRef{file(base, "a.txt")}}},
			other, nil, "",
		},
		{
			"a file of no run in a shared snapshot",
			[]snapshot{{[]lode.D{metricsOf(base), metricsOf(other)}, []SidecarFileRef{bogus}}},
			other, nil, "does not match the selected run's file",
		},
		{
			"a file under a record with a numeric run ID",
			[]snapshot{{[]lode.D{metricsOf(base), numericRunID}, []SidecarFileRef{file(noRunID, "a.txt")}}},
			base, nil, "expected string source, category, day, and run_id",
		},
		{
			"a file under a record with an empty run ID",
			[]snapshot{{[]lode.D{metricsOf(base), metricsOf(noRunID)}, []SidecarFileRef{file(noRunID, "a.txt")}}},
			base, nil, "invalid run key",
		},
		{
			"the same file recorded by two snapshots",
			[]snapshot{
				{[]lode.D{metricsOf(base)}, []SidecarFileRef{file(base, "a.txt")}},
				{[]lode.D{metricsOf(base)}, []SidecarFileRef{file(base, "a.txt")}},
			},
			base,
			[]SidecarFileRef{file(base, "a.txt")},
			"",
		},
		{
			"a file of a run whose key collapses to the selected run's",
			[]snapshot{{[]lode.D{metricsOf(collapsed)}, []SidecarFileRef{file(base, "a.txt")}}},
			collapsed, nil, "does not match the selected run's file",
		},
		{
			"a filename containing two dots",
			[]snapshot{{[]lode.D{metricsOf(base)}, []SidecarFileRef{file(base, "page..png")}}},
			base, nil, `filename that does not contain ".."`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := lode.NewMemory()
			client, err := NewLodeClientWithFactory(t.Context(), base, sharedFactory(store))
			if err != nil {
				t.Fatal(err)
			}
			for _, snap := range tc.snapshots {
				meta := map[string]any{MetadataKeySidecarFiles: snap.inventory}
				if _, err := client.dataset.Write(t.Context(), snap.records, meta); err != nil {
					t.Fatal(err)
				}
			}
			details, err := QueryRunDetails(t.Context(), sharedFactory(store), tc.query)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want one naming %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if details.Policy == nil || *details.Policy != "buffered" {
				t.Errorf("policy = %v, want buffered", details.Policy)
			}
			if !slices.Equal(details.SidecarFiles, tc.want) {
				t.Errorf("sidecar files = %+v, want %+v", details.SidecarFiles, tc.want)
			}
		})
	}
}

// A record names its run by the exact keys source, category, day, and run_id.
// Struct decoding would match RUN_ID as run_id, so recordKey reads each field
// by its exact name.
func TestRecordKey_ReadsExactFieldNames(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		json string
		want recordKey
	}{
		{"exact names", `{"source":"s","category":"c","day":"2026-11-21","run_id":"r","payload":{"x":1}}`, recordKey{"s", "c", "2026-11-21", "r"}},
		{"an uppercase run ID", `{"source":"s","category":"c","day":"2026-11-21","RUN_ID":"r"}`, recordKey{"s", "c", "2026-11-21", nil}},
		{"a numeric run ID", `{"run_id":7}`, recordKey{nil, nil, nil, float64(7)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got recordKey
			if err := json.Unmarshal([]byte(tc.json), &got); err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

// The inventory of a snapshot without a partition manifest is read.
func TestQueryRunDetails_IncompleteIndex(t *testing.T) {
	t.Parallel()
	store := lode.NewMemory()
	cfg := Config{Dataset: "quarry", Source: "src", Category: "raw", Day: "2026-10-22", RunID: "run-a"}
	client, err := NewLodeClientWithFactory(t.Context(), cfg, sharedFactory(store))
	if err != nil {
		t.Fatal(err)
	}
	ref := SidecarFileRef{Filename: "a.txt", ContentType: "text/plain", Path: SidecarFilesPrefix(cfg) + "a.txt"}
	record := lode.D{"source": cfg.Source, "category": cfg.Category, "day": cfg.Day, "run_id": cfg.RunID, "event_type": "item", "record_kind": RecordKindEvent}
	if _, err := client.dataset.Write(t.Context(), []lode.D{record}, map[string]any{MetadataKeySidecarFiles: []SidecarFileRef{ref}}); err != nil {
		t.Fatal(err)
	}
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
	details, err := QueryRunDetails(t.Context(), sharedFactory(store), cfg)
	if err != nil || len(details.SidecarFiles) != 1 || details.SidecarFiles[0] != ref {
		t.Fatalf("details = %+v, %v; want the one file", details, err)
	}
}
