package lode

import (
	"testing"
	"time"

	"github.com/pithecene/lode/lode"

	"github.com/pithecene/quarry/metrics"
)

func TestNewReadDatasetFS(t *testing.T) {
	dir := t.TempDir()
	ds, err := NewReadDatasetFS("quarry", dir)
	if err != nil {
		t.Fatalf("NewReadDatasetFS failed: %v", err)
	}
	if ds.ID() != "quarry" {
		t.Errorf("Dataset ID = %q, want %q", ds.ID(), "quarry")
	}
}

func TestNewReadDataset_WriteReadRoundTrip(t *testing.T) {
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

	client, err := NewLodeClientWithFactory(cfg, factory)
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

	// Read via Dataset.Read
	ds, err := NewReadDataset("quarry", factory)
	if err != nil {
		t.Fatalf("NewReadDataset failed: %v", err)
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

	record, ok := data[0].(map[string]any)
	if !ok {
		t.Fatalf("record type = %T, want map[string]any", data[0])
	}
	if record["record_kind"] != RecordKindMetrics {
		t.Errorf("record_kind = %v, want %q", record["record_kind"], RecordKindMetrics)
	}
}
