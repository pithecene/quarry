package lode

import (
	"context"
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
	// Another run with a similar ID shares the dataset and selectors. Its
	// manifest is older than the head and should never be loaded.
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
		if err := client.WriteMetrics(t.Context(), metrics.Snapshot{RunID: cfg.RunID, Policy: policy}, time.Now()); err != nil {
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
	prefix := "datasets/" + cfg.Dataset + "/partitions/" + PartitionPrefix(cfg)
	if !slices.Contains(counted.lists, prefix) {
		t.Errorf("storage listings = %v, want run partition prefix %q", counted.lists, prefix)
	}
	for _, listed := range counted.lists {
		if !strings.HasPrefix(listed, prefix) && listed != "datasets/"+cfg.Dataset+"/expired/" {
			t.Errorf("listed outside the run and Lode's retention index: %q", listed)
		}
	}
	for _, path := range counted.gets {
		if strings.Contains(path, "/"+string(unrelated.ID)+"/") || strings.Contains(path, "/category=other/") {
			t.Errorf("read unrelated snapshot data or metadata: %q", path)
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
