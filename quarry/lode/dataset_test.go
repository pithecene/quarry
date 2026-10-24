package lode

import (
	"errors"
	"testing"
	"time"

	"github.com/pithecene/lode/lode"

	"github.com/pithecene/quarry/metrics"
)

// A view needs a head to take the layout from, so a dataset without
// snapshots is reported as not found; readers turn that into an empty result.
func TestOpenView_EmptyDataset(t *testing.T) {
	t.Parallel()

	store, err := lode.NewFS(t.TempDir())
	if err != nil {
		t.Fatalf("NewFS failed: %v", err)
	}
	if _, err := OpenView(t.Context(), "quarry", store); !errors.Is(err, lode.ErrNotFound) {
		t.Fatalf("OpenView on an empty dataset: err = %v, want lode.ErrNotFound", err)
	}
}

// Every writer of a Quarry dataset must share its settings: Lode records
// them in each manifest and rejects a writer that differs when it opens.
func TestOpenDataset_RejectsOtherSettings(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		opts []lode.DatasetOption
	}{
		{"another partition layout", []lode.DatasetOption{lode.WithLayout(lode.Hive("day"))}},
		{"a compressor", []lode.DatasetOption{lode.WithCompressor(lode.NewGzipCompressor())}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := lode.NewMemory()
			cfg := Config{Dataset: "quarry", Source: "src", Category: "cat", Day: "2026-02-04", RunID: "run-1", Policy: "strict"}
			client, err := NewLodeClientWithFactory(t.Context(), cfg, lode.Use(store))
			if err != nil {
				t.Fatalf("NewLodeClientWithFactory failed: %v", err)
			}
			if err := client.WriteMetrics(t.Context(), metrics.Snapshot{RunID: "run-1"}, time.Date(2026, 2, 4, 10, 0, 0, 0, time.UTC)); err != nil {
				t.Fatalf("WriteMetrics failed: %v", err)
			}
			if _, err := OpenDataset(t.Context(), "quarry", store, tc.opts...); !errors.Is(err, lode.ErrSettingsMismatch) {
				t.Fatalf("OpenDataset with %s: err = %v, want lode.ErrSettingsMismatch", tc.name, err)
			}
		})
	}
}

func TestOpenView_WriteReadRoundTrip(t *testing.T) {
	t.Parallel()

	store := lode.NewMemory()
	factory := sharedFactory(store)

	// Write via LodeClient
	cfg := Config{
		Dataset:  "quarry",
		Source:   "rt-source",
		Category: "rt-category",
		Day:      "2026-02-04",
		RunID:    "run-rt",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(t.Context(), cfg, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	snap := metrics.Snapshot{
		RunsStarted:   7,
		RunsCompleted: 6,
		RunsFailed:    1,
		Policy:        "strict",
		Executor:      "test-exec.js",
		StorageBackend: "fs",
		RunID:          "run-rt",
	}

	completedAt := time.Date(2026, 2, 4, 10, 0, 0, 0, time.UTC)
	if err := client.WriteMetrics(t.Context(), snap, completedAt); err != nil {
		t.Fatalf("WriteMetrics failed: %v", err)
	}

	// Read through a view, as the read path does
	ds, err := readView(t.Context(), factory)
	if err != nil {
		t.Fatalf("readView failed: %v", err)
	}

	latest, err := ds.Latest(t.Context())
	if err != nil {
		t.Fatalf("Latest failed: %v", err)
	}

	data, err := ds.Read(t.Context(), latest.ID)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if len(data) != 1 {
		t.Fatalf("Read returned %d items, want 1", len(data))
	}

	record := data[0]
	if record["record_kind"] != RecordKindMetrics {
		t.Errorf("record_kind = %v, want %q", record["record_kind"], RecordKindMetrics)
	}
}
