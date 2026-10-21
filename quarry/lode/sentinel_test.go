package lode

import (
	"errors"
	"testing"
	"time"

	"github.com/pithecene-io/lode/lode"

	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/types"
)

// =============================================================================
// Lode Sentinel Error Path Validation (V1_READINESS §5)
//
// These tests exercise Lode error sentinels from Quarry's actual code paths.
// They validate that Quarry correctly observes and handles:
//   - lode.ErrPathExists (immutability enforcement on double-write)
//   - lode.ErrNotFound   (missing snapshot lookup)
//
// ErrNoSnapshots is already validated via production dogfooding (2026-02-23).
// =============================================================================

// TestPutFile_ErrPathExists_DoubleWrite verifies that writing the same sidecar
// file twice returns lode.ErrPathExists, confirming Lode's immutability
// enforcement from Quarry's PutFile code path.
func TestPutFile_ErrPathExists_DoubleWrite(t *testing.T) {
	store := lode.NewMemory()
	factory := sharedFactory(store)

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-001",
	}

	client, err := NewLodeClientWithFactory(cfg, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	ctx := t.Context()
	data := []byte("file-content")

	// First write succeeds
	if err := client.PutFile(ctx, "report.pdf", "application/pdf", data); err != nil {
		t.Fatalf("first PutFile failed: %v", err)
	}

	// Second write to the same filename must return ErrPathExists
	err = client.PutFile(ctx, "report.pdf", "application/pdf", data)
	if err == nil {
		t.Fatal("expected ErrPathExists on double-write, got nil")
	}

	if !errors.Is(err, lode.ErrPathExists) {
		t.Errorf("expected errors.Is(err, lode.ErrPathExists), got: %v", err)
	}

	// PutFile errors must be wrapped as StorageError with classification
	var storageErr *StorageError
	if !errors.As(err, &storageErr) {
		t.Fatalf("expected *StorageError, got %T: %v", err, err)
	}
	if storageErr.Op != "write" {
		t.Errorf("Op = %q, want \"write\"", storageErr.Op)
	}
	if storageErr.Path == "" {
		t.Error("expected non-empty path in StorageError")
	}
	if !errors.Is(err, ErrPathExists) {
		t.Errorf("expected errors.Is(err, ErrPathExists) via classifier, got kind: %v", storageErr.Kind)
	}
}

// TestPutFile_ErrPathExists_MetaSidecar verifies that the companion .meta.json
// file also enforces immutability. If the data file succeeds but the meta file
// is a duplicate, ErrPathExists is returned.
func TestPutFile_ErrPathExists_MetaSidecar(t *testing.T) {
	store := lode.NewMemory()
	factory := sharedFactory(store)

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-001",
	}

	client, err := NewLodeClientWithFactory(cfg, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	ctx := t.Context()

	// Write once — creates both data file and .meta.json
	if err := client.PutFile(ctx, "image.png", "image/png", []byte("png-data")); err != nil {
		t.Fatalf("first PutFile failed: %v", err)
	}

	// Write again — data file path collides, returning ErrPathExists
	err = client.PutFile(ctx, "image.png", "image/png", []byte("png-data-v2"))
	if err == nil {
		t.Fatal("expected error on duplicate sidecar write, got nil")
	}

	if !errors.Is(err, lode.ErrPathExists) {
		t.Errorf("expected errors.Is(err, lode.ErrPathExists), got: %v", err)
	}
}

// TestQueryLatestMetrics_ErrNotFound_StaleSnapshotID verifies that
// ds.Read() with a fabricated/stale snapshot ID returns a wrapped error
// that Quarry propagates as a read-path failure.
//
// This exercises the exact code path in metrics_query.go:38 where
// ds.Read(snap.ID) could fail if a snapshot is removed between listing
// and reading.
func TestQueryLatestMetrics_ErrNotFound_StaleSnapshotID(t *testing.T) {
	store := lode.NewMemory()
	factory := sharedFactory(store)

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-001",
	}

	// Write a metrics record so we have a real snapshot
	client, err := NewLodeClientWithFactory(cfg, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	snap := metrics.Snapshot{
		RunsStarted:   1,
		RunID:          "run-001",
		Policy:         "strict",
		Executor:       "executor.js",
		StorageBackend: "memory",
	}

	completedAt := time.Date(2026, 2, 3, 15, 0, 0, 0, time.UTC)
	if err := client.WriteMetrics(t.Context(), snap, completedAt); err != nil {
		t.Fatalf("WriteMetrics failed: %v", err)
	}

	// Read dataset directly to exercise ds.Read with a bad snapshot ID
	ds, err := NewReadDataset("quarry", factory)
	if err != nil {
		t.Fatalf("NewReadDataset failed: %v", err)
	}

	// Fabricate a stale snapshot ID that doesn't exist
	staleID := lode.DatasetSnapshotID("nonexistent-snapshot-id")
	_, err = ds.Read(t.Context(), staleID)
	if err == nil {
		t.Fatal("expected error for stale snapshot ID, got nil")
	}

	// Lode should return ErrNotFound for a missing snapshot
	if !errors.Is(err, lode.ErrNotFound) {
		t.Errorf("expected errors.Is(err, lode.ErrNotFound), got: %v", err)
	}
}

// =============================================================================
// Sidecar File Metadata Inventory (V1_READINESS §9)
//
// These tests verify that sidecar files written via PutFile are tracked
// in snapshot Metadata, enabling downstream consumers to enumerate files
// without prefix-scanning storage.
// =============================================================================

// TestPutFile_MetadataInventory_IncludedInWriteEvents verifies that sidecar
// files written via PutFile appear in the next WriteEvents snapshot Metadata.
func TestPutFile_MetadataInventory_IncludedInWriteEvents(t *testing.T) {
	store := lode.NewMemory()
	factory := sharedFactory(store)

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-001",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(cfg, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	ctx := t.Context()

	// Write two sidecar files
	if err := client.PutFile(ctx, "page-1.html", "text/html", []byte("<html>1</html>")); err != nil {
		t.Fatalf("PutFile 1 failed: %v", err)
	}
	if err := client.PutFile(ctx, "page-2.html", "text/html", []byte("<html>22</html>")); err != nil {
		t.Fatalf("PutFile 2 failed: %v", err)
	}

	// Write events — this flushes pending files into Metadata
	events := []*types.EventEnvelope{
		{Type: types.EventTypeItem, Seq: 1, Payload: map[string]any{"url": "http://example.com"}},
	}
	if err := client.WriteEvents(ctx, cfg.Dataset, cfg.RunID, events); err != nil {
		t.Fatalf("WriteEvents failed: %v", err)
	}

	// Read the snapshot and verify Metadata contains file refs
	ds, err := NewReadDataset("quarry", factory)
	if err != nil {
		t.Fatalf("NewReadDataset failed: %v", err)
	}

	snapshots, err := ds.Snapshots(ctx)
	if err != nil {
		t.Fatalf("Snapshots failed: %v", err)
	}

	// Find the snapshot with sidecar_files in metadata
	var found bool
	for _, snap := range snapshots {
		raw, ok := snap.Manifest.Metadata[MetadataKeySidecarFiles]
		if !ok {
			continue
		}
		found = true

		// Metadata values are deserialized as []any from JSON
		files, ok := raw.([]any)
		if !ok {
			t.Fatalf("sidecar_files is %T, want []any", raw)
		}
		if len(files) != 2 {
			t.Fatalf("sidecar_files has %d entries, want 2", len(files))
		}

		// Verify first file ref
		ref0, ok := files[0].(map[string]any)
		if !ok {
			t.Fatalf("file ref is %T, want map[string]any", files[0])
		}
		if ref0["filename"] != "page-1.html" {
			t.Errorf("filename = %v, want page-1.html", ref0["filename"])
		}
		if ref0["content_type"] != "text/html" {
			t.Errorf("content_type = %v, want text/html", ref0["content_type"])
		}
	}
	if !found {
		t.Fatal("no snapshot contains sidecar_files metadata")
	}
}

// TestPutFile_MetadataInventory_DrainedAfterWrite verifies that pending files
// are cleared after a successful write, so the next snapshot doesn't duplicate them.
func TestPutFile_MetadataInventory_DrainedAfterWrite(t *testing.T) {
	store := lode.NewMemory()
	factory := sharedFactory(store)

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-001",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(cfg, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	ctx := t.Context()

	// Write a file and flush via WriteEvents
	if err := client.PutFile(ctx, "batch-1.csv", "text/csv", []byte("a,b,c")); err != nil {
		t.Fatalf("PutFile failed: %v", err)
	}

	events1 := []*types.EventEnvelope{
		{Type: types.EventTypeItem, Seq: 1, Payload: map[string]any{"batch": 1}},
	}
	if err := client.WriteEvents(ctx, cfg.Dataset, cfg.RunID, events1); err != nil {
		t.Fatalf("WriteEvents 1 failed: %v", err)
	}

	// Verify pending files are drained
	client.mu.Lock()
	pending := len(client.pendingFiles)
	client.mu.Unlock()
	if pending != 0 {
		t.Errorf("pendingFiles after flush = %d, want 0", pending)
	}

	// Second write without new files — metadata should be empty
	events2 := []*types.EventEnvelope{
		{Type: types.EventTypeItem, Seq: 2, Payload: map[string]any{"batch": 2}},
	}
	if err := client.WriteEvents(ctx, cfg.Dataset, cfg.RunID, events2); err != nil {
		t.Fatalf("WriteEvents 2 failed: %v", err)
	}

	// Read all snapshots and verify only the first has sidecar_files
	ds, err := NewReadDataset("quarry", factory)
	if err != nil {
		t.Fatalf("NewReadDataset failed: %v", err)
	}

	snapshots, err := ds.Snapshots(ctx)
	if err != nil {
		t.Fatalf("Snapshots failed: %v", err)
	}

	filesSnapshots := 0
	for _, snap := range snapshots {
		if raw, ok := snap.Manifest.Metadata[MetadataKeySidecarFiles]; ok {
			if files, ok := raw.([]any); ok && len(files) > 0 {
				filesSnapshots++
			}
		}
	}
	if filesSnapshots != 1 {
		t.Errorf("snapshots with sidecar_files = %d, want 1", filesSnapshots)
	}
}

// TestPutFile_MetadataInventory_PreservedOnWriteFailure verifies that pending
// files are NOT drained when a write fails, so they are included in the next
// successful write.
func TestPutFile_MetadataInventory_PreservedOnWriteFailure(t *testing.T) {
	// Use FailingStore so the write fails but PutFile succeeds
	failStore := &FailingStore{PutErr: nil}
	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-001",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(cfg, FailingStoreFactory(failStore))
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	ctx := t.Context()

	// PutFile succeeds (FailingStore.Put returns nil initially)
	if err := client.PutFile(ctx, "keep-me.json", "application/json", []byte("{}")); err != nil {
		t.Fatalf("PutFile failed: %v", err)
	}

	// Now make Dataset.Write fail via store error
	failStore.PutErr = errors.New("simulated write failure")

	events := []*types.EventEnvelope{
		{Type: types.EventTypeItem, Seq: 1, Payload: map[string]any{"key": "value"}},
	}
	err = client.WriteEvents(ctx, cfg.Dataset, cfg.RunID, events)
	if err == nil {
		t.Fatal("expected write failure, got nil")
	}

	// Verify pending files are preserved
	client.mu.Lock()
	pending := len(client.pendingFiles)
	client.mu.Unlock()
	if pending != 1 {
		t.Errorf("pendingFiles after failed write = %d, want 1 (preserved)", pending)
	}
}

// TestPutFile_MetadataInventory_IncludedInWriteMetrics verifies that sidecar
// files are also flushed when WriteMetrics is called (run completion path).
func TestPutFile_MetadataInventory_IncludedInWriteMetrics(t *testing.T) {
	store := lode.NewMemory()
	factory := sharedFactory(store)

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-001",
	}

	client, err := NewLodeClientWithFactory(cfg, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	ctx := t.Context()

	// Write a sidecar file
	if err := client.PutFile(ctx, "final-report.pdf", "application/pdf", []byte("pdf-data")); err != nil {
		t.Fatalf("PutFile failed: %v", err)
	}

	// Flush via WriteMetrics (run completion)
	snap := metrics.Snapshot{
		RunsStarted:   1,
		RunID:          "run-001",
		Policy:         "strict",
		Executor:       "executor.js",
		StorageBackend: "memory",
	}
	completedAt := time.Date(2026, 2, 3, 15, 0, 0, 0, time.UTC)
	if err := client.WriteMetrics(ctx, snap, completedAt); err != nil {
		t.Fatalf("WriteMetrics failed: %v", err)
	}

	// Verify pending files are drained
	client.mu.Lock()
	pending := len(client.pendingFiles)
	client.mu.Unlock()
	if pending != 0 {
		t.Errorf("pendingFiles after WriteMetrics = %d, want 0", pending)
	}

	// Verify metadata in snapshot
	ds, err := NewReadDataset("quarry", factory)
	if err != nil {
		t.Fatalf("NewReadDataset failed: %v", err)
	}

	snapshots, err := ds.Snapshots(ctx)
	if err != nil {
		t.Fatalf("Snapshots failed: %v", err)
	}

	var found bool
	for _, s := range snapshots {
		if raw, ok := s.Manifest.Metadata[MetadataKeySidecarFiles]; ok {
			files, ok := raw.([]any)
			if !ok || len(files) == 0 {
				continue
			}
			found = true
			ref, ok := files[0].(map[string]any)
			if !ok {
				t.Fatalf("file ref is %T, want map[string]any", files[0])
			}
			if ref["filename"] != "final-report.pdf" {
				t.Errorf("filename = %v, want final-report.pdf", ref["filename"])
			}
		}
	}
	if !found {
		t.Fatal("no snapshot contains sidecar_files metadata after WriteMetrics")
	}
}

// TestPutFile_MetadataInventory_SurvivesChunkWrite verifies that chunk writes
// do NOT drain pending sidecar file refs. Quarry's persistence order is
// chunks-first (both buffered and strict policy), so file refs written via
// PutFile before a chunk flush must survive to appear in the subsequent
// event or metrics snapshot — the consumer-facing boundaries.
func TestPutFile_MetadataInventory_SurvivesChunkWrite(t *testing.T) {
	store := lode.NewMemory()
	factory := sharedFactory(store)

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-001",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(cfg, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	ctx := t.Context()

	// Write a sidecar file
	if err := client.PutFile(ctx, "screenshot.png", "image/png", []byte("png-data")); err != nil {
		t.Fatalf("PutFile failed: %v", err)
	}

	// Write chunks — this must NOT drain pending files
	chunks := []*types.ArtifactChunk{
		{ArtifactID: "art-1", Seq: 1, Data: []byte("chunk-data")},
	}
	if err := client.WriteChunks(ctx, cfg.Dataset, cfg.RunID, chunks); err != nil {
		t.Fatalf("WriteChunks failed: %v", err)
	}

	// Verify pending files are still present after chunk write
	client.mu.Lock()
	pending := len(client.pendingFiles)
	client.mu.Unlock()
	if pending != 1 {
		t.Fatalf("pendingFiles after WriteChunks = %d, want 1 (must survive chunk writes)", pending)
	}

	// Now flush via WriteEvents — file refs should appear here
	events := []*types.EventEnvelope{
		{Type: types.EventTypeItem, Seq: 1, Payload: map[string]any{"url": "http://example.com"}},
	}
	if err := client.WriteEvents(ctx, cfg.Dataset, cfg.RunID, events); err != nil {
		t.Fatalf("WriteEvents failed: %v", err)
	}

	// Verify pending files are now drained
	client.mu.Lock()
	pending = len(client.pendingFiles)
	client.mu.Unlock()
	if pending != 0 {
		t.Errorf("pendingFiles after WriteEvents = %d, want 0", pending)
	}

	// Verify the event snapshot (not the chunk snapshot) has the file refs
	ds, err := NewReadDataset("quarry", factory)
	if err != nil {
		t.Fatalf("NewReadDataset failed: %v", err)
	}

	snapshots, err := ds.Snapshots(ctx)
	if err != nil {
		t.Fatalf("Snapshots failed: %v", err)
	}

	// Count snapshots with sidecar_files — must be exactly 1 (the event snapshot)
	filesSnapshots := 0
	for _, snap := range snapshots {
		if raw, ok := snap.Manifest.Metadata[MetadataKeySidecarFiles]; ok {
			if files, ok := raw.([]any); ok && len(files) > 0 {
				filesSnapshots++
				ref, ok := files[0].(map[string]any)
				if !ok {
					t.Fatalf("file ref is %T, want map[string]any", files[0])
				}
				if ref["filename"] != "screenshot.png" {
					t.Errorf("filename = %v, want screenshot.png", ref["filename"])
				}
			}
		}
	}
	if filesSnapshots != 1 {
		t.Errorf("snapshots with sidecar_files = %d, want 1 (event snapshot only, not chunk snapshot)", filesSnapshots)
	}
}
