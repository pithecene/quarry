package lode

import (
	"testing"
	"time"

	"github.com/pithecene/lode/lode"

	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/types"
)

func TestQuerySidecarFiles_RoundTrip(t *testing.T) {
	store := lode.NewMemory()
	factory := sharedFactory(store)

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-03-22",
		RunID:    "run-query",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(cfg, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	ctx := t.Context()

	// Write two sidecar files
	if err := client.PutFile(ctx, "page.html", "text/html", []byte("<html></html>")); err != nil {
		t.Fatalf("PutFile 1 failed: %v", err)
	}
	if err := client.PutFile(ctx, "image.png", "image/png", []byte("png-bytes")); err != nil {
		t.Fatalf("PutFile 2 failed: %v", err)
	}

	// Flush via WriteEvents
	events := []*types.EventEnvelope{
		{Type: types.EventTypeItem, Seq: 1, RunID: "run-query", Payload: map[string]any{"key": "value"}},
	}
	if err := client.WriteEvents(ctx, cfg.Dataset, cfg.RunID, events); err != nil {
		t.Fatalf("WriteEvents failed: %v", err)
	}

	// Query sidecar files
	ds, err := NewReadDataset("quarry", factory)
	if err != nil {
		t.Fatalf("NewReadDataset failed: %v", err)
	}

	files, err := QuerySidecarFiles(ctx, ds, "run-query")
	if err != nil {
		t.Fatalf("QuerySidecarFiles failed: %v", err)
	}

	if len(files) != 2 {
		t.Fatalf("got %d files, want 2", len(files))
	}

	// Verify first file
	if files[0].Filename != "page.html" {
		t.Errorf("files[0].Filename = %q, want page.html", files[0].Filename)
	}
	if files[0].ContentType != "text/html" {
		t.Errorf("files[0].ContentType = %q, want text/html", files[0].ContentType)
	}
	if files[0].Size != 13 { // len("<html></html>")
		t.Errorf("files[0].Size = %d, want 13", files[0].Size)
	}
	if files[0].Path == "" {
		t.Error("files[0].Path should not be empty")
	}

	// Verify second file
	if files[1].Filename != "image.png" {
		t.Errorf("files[1].Filename = %q, want image.png", files[1].Filename)
	}
}

func TestQuerySidecarFiles_NoFiles(t *testing.T) {
	store := lode.NewMemory()
	factory := sharedFactory(store)

	ds, err := NewReadDataset("quarry", factory)
	if err != nil {
		t.Fatalf("NewReadDataset failed: %v", err)
	}

	files, err := QuerySidecarFiles(t.Context(), ds, "run-nonexistent")
	if err != nil {
		t.Fatalf("QuerySidecarFiles failed: %v", err)
	}
	if files != nil {
		t.Errorf("expected nil for no files, got %d entries", len(files))
	}
}

func TestQuerySidecarFiles_FiltersByRunID(t *testing.T) {
	store := lode.NewMemory()
	factory := sharedFactory(store)

	ctx := t.Context()

	// Write files for two different runs
	for _, runID := range []string{"run-A", "run-B"} {
		cfg := Config{
			Dataset:  "quarry",
			Source:   "test-source",
			Category: "test-category",
			Day:      "2026-03-22",
			RunID:    runID,
			Policy:   "strict",
		}

		client, err := NewLodeClientWithFactory(cfg, factory)
		if err != nil {
			t.Fatalf("NewLodeClientWithFactory failed: %v", err)
		}

		if err := client.PutFile(ctx, runID+".html", "text/html", []byte("data")); err != nil {
			t.Fatalf("PutFile failed: %v", err)
		}

		events := []*types.EventEnvelope{
			{Type: types.EventTypeItem, Seq: 1, RunID: runID, Payload: map[string]any{"run": runID}},
		}
		if err := client.WriteEvents(ctx, cfg.Dataset, cfg.RunID, events); err != nil {
			t.Fatalf("WriteEvents failed: %v", err)
		}
	}

	ds, err := NewReadDataset("quarry", factory)
	if err != nil {
		t.Fatalf("NewReadDataset failed: %v", err)
	}

	// Query for run-A only
	files, err := QuerySidecarFiles(ctx, ds, "run-A")
	if err != nil {
		t.Fatalf("QuerySidecarFiles failed: %v", err)
	}

	if len(files) != 1 {
		t.Fatalf("got %d files, want 1", len(files))
	}
	if files[0].Filename != "run-A.html" {
		t.Errorf("filename = %q, want run-A.html", files[0].Filename)
	}
}

func TestQuerySidecarFiles_AcrossMultipleSnapshots(t *testing.T) {
	store := lode.NewMemory()
	factory := sharedFactory(store)

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-03-22",
		RunID:    "run-multi",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(cfg, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	ctx := t.Context()

	// First batch: file + event flush
	if err := client.PutFile(ctx, "batch1.csv", "text/csv", []byte("a,b")); err != nil {
		t.Fatalf("PutFile 1 failed: %v", err)
	}
	events1 := []*types.EventEnvelope{
		{Type: types.EventTypeItem, Seq: 1, RunID: "run-multi", Payload: map[string]any{"batch": 1}},
	}
	if err := client.WriteEvents(ctx, cfg.Dataset, cfg.RunID, events1); err != nil {
		t.Fatalf("WriteEvents 1 failed: %v", err)
	}

	// Second batch: file + metrics flush
	if err := client.PutFile(ctx, "batch2.csv", "text/csv", []byte("c,d")); err != nil {
		t.Fatalf("PutFile 2 failed: %v", err)
	}
	snap := metrics.Snapshot{RunID: "run-multi", Policy: "strict", Executor: "e", StorageBackend: "mem"}
	if err := client.WriteMetrics(ctx, snap, time.Now()); err != nil {
		t.Fatalf("WriteMetrics failed: %v", err)
	}

	// Query should union across both snapshots
	ds, err := NewReadDataset("quarry", factory)
	if err != nil {
		t.Fatalf("NewReadDataset failed: %v", err)
	}

	files, err := QuerySidecarFiles(ctx, ds, "run-multi")
	if err != nil {
		t.Fatalf("QuerySidecarFiles failed: %v", err)
	}

	if len(files) != 2 {
		t.Fatalf("got %d files, want 2 (unioned across snapshots)", len(files))
	}
	if files[0].Filename != "batch1.csv" {
		t.Errorf("files[0].Filename = %q, want batch1.csv", files[0].Filename)
	}
	if files[1].Filename != "batch2.csv" {
		t.Errorf("files[1].Filename = %q, want batch2.csv", files[1].Filename)
	}
}
