package lode

import (
	"errors"
	"testing"
	"time"

	"github.com/pithecene/lode/lode"

	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/types"
)

func TestLodeClient_WriteEvents(t *testing.T) {
	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-123",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(cfg, lode.NewMemoryFactory())
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	events := []*types.EventEnvelope{
		{
			ContractVersion: "1.0.0",
			EventID:         "evt-1",
			RunID:           "run-123",
			Seq:             1,
			Type:            types.EventTypeItem,
			Ts:              "2026-02-03T12:00:00Z",
			Payload:         map[string]any{"key": "value"},
			Attempt:         1,
		},
		{
			ContractVersion: "1.0.0",
			EventID:         "evt-2",
			RunID:           "run-123",
			Seq:             2,
			Type:            types.EventTypeLog,
			Ts:              "2026-02-03T12:00:01Z",
			Payload:         map[string]any{"message": "test log"},
			Attempt:         1,
		},
	}

	err = client.WriteEvents(t.Context(), cfg.Dataset, cfg.RunID, events)
	if err != nil {
		t.Fatalf("WriteEvents failed: %v", err)
	}
	// Success: records written without error
}

func TestLodeClient_WriteArtifactEvent(t *testing.T) {
	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-123",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(cfg, lode.NewMemoryFactory())
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	ctx := t.Context()

	// Must write chunks before commit (chunks-before-commit invariant)
	chunks := []*types.ArtifactChunk{
		{ArtifactID: "art-1", Seq: 1, IsLast: true, Data: []byte("image data")},
	}
	if err := client.WriteChunks(ctx, cfg.Dataset, cfg.RunID, chunks); err != nil {
		t.Fatalf("WriteChunks failed: %v", err)
	}

	events := []*types.EventEnvelope{
		{
			ContractVersion: "1.0.0",
			EventID:         "evt-3",
			RunID:           "run-123",
			Seq:             3,
			Type:            types.EventTypeArtifact,
			Ts:              "2026-02-03T12:00:02Z",
			Payload: map[string]any{
				"artifact_id":  "art-1",
				"name":         "screenshot.png",
				"content_type": "image/png",
				"size_bytes":   float64(1024), // JSON unmarshals numbers as float64
			},
			Attempt: 1,
		},
	}

	err = client.WriteEvents(ctx, cfg.Dataset, cfg.RunID, events)
	if err != nil {
		t.Fatalf("WriteEvents (artifact) failed: %v", err)
	}
	// Success: artifact commit record written
}

func TestLodeClient_WriteChunks(t *testing.T) {
	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-123",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(cfg, lode.NewMemoryFactory())
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	chunks := []*types.ArtifactChunk{
		{
			ArtifactID: "art-1",
			Seq:        1,
			IsLast:     false,
			Data:       []byte("hello "),
		},
		{
			ArtifactID: "art-1",
			Seq:        2,
			IsLast:     true,
			Data:       []byte("world"),
		},
	}

	err = client.WriteChunks(t.Context(), cfg.Dataset, cfg.RunID, chunks)
	if err != nil {
		t.Fatalf("WriteChunks failed: %v", err)
	}
	// Success: chunk records written
}

func TestLodeClient_ChunkOffset(t *testing.T) {
	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-123",
		Policy:   "strict",
	}

	// Test that offsets are computed correctly per-artifact
	chunks := []*types.ArtifactChunk{
		{ArtifactID: "art-1", Seq: 1, Data: []byte("12345")},           // offset 0, length 5
		{ArtifactID: "art-1", Seq: 2, Data: []byte("67890")},           // offset 5, length 5
		{ArtifactID: "art-2", Seq: 1, Data: []byte("abc")},             // offset 0 (different artifact)
		{ArtifactID: "art-1", Seq: 3, IsLast: true, Data: []byte("!")}, // offset 10, length 1
	}

	// Verify the toChunkRecord offset computation
	offsets := make(map[string]int64)
	for _, chunk := range chunks {
		offset := offsets[chunk.ArtifactID]
		record := toChunkRecord(chunk, offset, cfg)

		expectedOffset := offsets[chunk.ArtifactID]
		if record.Offset != expectedOffset {
			t.Errorf("chunk %s seq %d: offset = %d, want %d",
				chunk.ArtifactID, chunk.Seq, record.Offset, expectedOffset)
		}
		if record.Length != int64(len(chunk.Data)) {
			t.Errorf("chunk %s seq %d: length = %d, want %d",
				chunk.ArtifactID, chunk.Seq, record.Length, len(chunk.Data))
		}

		offsets[chunk.ArtifactID] = offset + int64(len(chunk.Data))
	}
}

func TestLodeClient_ChunkOffset_AcrossBatches(t *testing.T) {
	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-123",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(cfg, lode.NewMemoryFactory())
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	ctx := t.Context()

	// First batch: 10 bytes for art-1
	batch1 := []*types.ArtifactChunk{
		{ArtifactID: "art-1", Seq: 1, Data: []byte("0123456789")}, // 10 bytes
	}
	if err := client.WriteChunks(ctx, cfg.Dataset, cfg.RunID, batch1); err != nil {
		t.Fatalf("WriteChunks batch 1 failed: %v", err)
	}

	// Verify offset tracked
	if client.offsets["art-1"] != 10 {
		t.Errorf("after batch 1: offset = %d, want 10", client.offsets["art-1"])
	}

	// Second batch: 5 more bytes for art-1
	batch2 := []*types.ArtifactChunk{
		{ArtifactID: "art-1", Seq: 2, IsLast: true, Data: []byte("abcde")}, // 5 bytes
	}
	if err := client.WriteChunks(ctx, cfg.Dataset, cfg.RunID, batch2); err != nil {
		t.Fatalf("WriteChunks batch 2 failed: %v", err)
	}

	// Verify cumulative offset
	if client.offsets["art-1"] != 15 {
		t.Errorf("after batch 2: offset = %d, want 15", client.offsets["art-1"])
	}
}

func TestLodeClient_CommitRequiresChunks(t *testing.T) {
	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-123",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(cfg, lode.NewMemoryFactory())
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	ctx := t.Context()

	// Try to commit without writing any chunks
	commitEvent := &types.EventEnvelope{
		EventID: "evt-commit",
		Type:    types.EventTypeArtifact,
		Seq:     1,
		Payload: map[string]any{
			"artifact_id":  "art-no-chunks",
			"name":         "test.txt",
			"content_type": "text/plain",
			"size_bytes":   float64(100),
		},
	}

	err = client.WriteEvents(ctx, cfg.Dataset, cfg.RunID, []*types.EventEnvelope{commitEvent})
	if err == nil {
		t.Fatal("expected error for commit without chunks, got nil")
	}

	// Verify it's the right error
	if !containsError(err, ErrCommitWithoutChunks) {
		t.Errorf("expected ErrCommitWithoutChunks, got: %v", err)
	}
}

func TestLodeClient_CommitSucceedsWithChunks(t *testing.T) {
	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-123",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(cfg, lode.NewMemoryFactory())
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	ctx := t.Context()

	// Write chunks first
	chunks := []*types.ArtifactChunk{
		{ArtifactID: "art-1", Seq: 1, Data: []byte("hello")},
		{ArtifactID: "art-1", Seq: 2, IsLast: true, Data: []byte("world")},
	}
	if err := client.WriteChunks(ctx, cfg.Dataset, cfg.RunID, chunks); err != nil {
		t.Fatalf("WriteChunks failed: %v", err)
	}

	// Now commit should succeed
	commitEvent := &types.EventEnvelope{
		EventID: "evt-commit",
		Type:    types.EventTypeArtifact,
		Seq:     3,
		Payload: map[string]any{
			"artifact_id":  "art-1",
			"name":         "test.txt",
			"content_type": "text/plain",
			"size_bytes":   float64(10),
		},
	}

	err = client.WriteEvents(ctx, cfg.Dataset, cfg.RunID, []*types.EventEnvelope{commitEvent})
	if err != nil {
		t.Fatalf("WriteEvents (commit) failed: %v", err)
	}
}

func TestLodeClient_OffsetsResetAfterCommit(t *testing.T) {
	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-123",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(cfg, lode.NewMemoryFactory())
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	ctx := t.Context()

	// Write chunks
	chunks := []*types.ArtifactChunk{
		{ArtifactID: "art-1", Seq: 1, Data: []byte("12345")}, // 5 bytes
	}
	if err := client.WriteChunks(ctx, cfg.Dataset, cfg.RunID, chunks); err != nil {
		t.Fatalf("WriteChunks failed: %v", err)
	}

	// Verify offset is tracked
	if client.offsets["art-1"] != 5 {
		t.Errorf("before commit: offset = %d, want 5", client.offsets["art-1"])
	}

	// Commit the artifact
	commitEvent := &types.EventEnvelope{
		EventID: "evt-commit",
		Type:    types.EventTypeArtifact,
		Seq:     2,
		Payload: map[string]any{
			"artifact_id":  "art-1",
			"name":         "test.txt",
			"content_type": "text/plain",
			"size_bytes":   float64(5),
		},
	}
	if err := client.WriteEvents(ctx, cfg.Dataset, cfg.RunID, []*types.EventEnvelope{commitEvent}); err != nil {
		t.Fatalf("WriteEvents (commit) failed: %v", err)
	}

	// Verify offset is reset
	if _, exists := client.offsets["art-1"]; exists {
		t.Errorf("after commit: offset still exists, should be deleted")
	}

	// Verify chunksSeen is reset
	if _, exists := client.chunksSeen["art-1"]; exists {
		t.Errorf("after commit: chunksSeen still exists, should be deleted")
	}

	// New artifact with same ID should start fresh
	chunks2 := []*types.ArtifactChunk{
		{ArtifactID: "art-1", Seq: 1, Data: []byte("abc")}, // 3 bytes
	}
	if err := client.WriteChunks(ctx, cfg.Dataset, cfg.RunID, chunks2); err != nil {
		t.Fatalf("WriteChunks (new artifact) failed: %v", err)
	}

	// Should start from offset 0 again
	if client.offsets["art-1"] != 3 {
		t.Errorf("new artifact: offset = %d, want 3", client.offsets["art-1"])
	}
}

func TestLodeClient_CommitRejectsMissingArtifactID(t *testing.T) {
	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-123",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(cfg, lode.NewMemoryFactory())
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	ctx := t.Context()

	// Commit with missing artifact_id
	commitEvent := &types.EventEnvelope{
		EventID: "evt-commit",
		Type:    types.EventTypeArtifact,
		Seq:     1,
		Payload: map[string]any{
			"name":         "test.txt",
			"content_type": "text/plain",
			"size_bytes":   float64(100),
			// artifact_id missing
		},
	}

	err = client.WriteEvents(ctx, cfg.Dataset, cfg.RunID, []*types.EventEnvelope{commitEvent})
	if err == nil {
		t.Fatal("expected error for commit without artifact_id, got nil")
	}
	if !errors.Is(err, ErrMissingArtifactID) {
		t.Errorf("expected ErrMissingArtifactID, got: %v", err)
	}
}

// containsError checks if err wraps or is target.
func containsError(err, target error) bool {
	return errors.Is(err, target)
}

func TestComputeMD5(t *testing.T) {
	data := []byte("hello world")
	got := computeMD5(data)
	want := "5eb63bbbe01eeed093cb22bb8f5acdc3" // known MD5 of "hello world"

	if got != want {
		t.Errorf("computeMD5() = %q, want %q", got, want)
	}
}

func TestToEventRecord(t *testing.T) {
	cfg := Config{
		Dataset:  "quarry",
		Source:   "my-source",
		Category: "my-category",
		Day:      "2026-02-03",
		RunID:    "run-abc",
		Policy:   "strict",
	}

	jobID := "job-xyz"
	e := &types.EventEnvelope{
		ContractVersion: "1.0.0",
		EventID:         "evt-1",
		RunID:           "run-abc",
		Seq:             1,
		Type:            types.EventTypeItem,
		Ts:              "2026-02-03T12:00:00Z",
		Payload:         map[string]any{"foo": "bar"},
		JobID:           &jobID,
		Attempt:         1,
	}

	record := toEventRecord(e, cfg)

	if record.RecordKind != RecordKindEvent {
		t.Errorf("RecordKind = %q, want %q", record.RecordKind, RecordKindEvent)
	}
	if record.Source != cfg.Source {
		t.Errorf("Source = %q, want %q", record.Source, cfg.Source)
	}
	if record.Category != cfg.Category {
		t.Errorf("Category = %q, want %q", record.Category, cfg.Category)
	}
	if record.Day != cfg.Day {
		t.Errorf("Day = %q, want %q", record.Day, cfg.Day)
	}
	if record.EventID != e.EventID {
		t.Errorf("EventID = %q, want %q", record.EventID, e.EventID)
	}
}

func TestS3Config_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     S3Config
		wantErr bool
	}{
		{
			name:    "empty bucket fails",
			cfg:     S3Config{Bucket: ""},
			wantErr: true,
		},
		{
			name:    "valid bucket only",
			cfg:     S3Config{Bucket: "my-bucket"},
			wantErr: false,
		},
		{
			name:    "valid bucket with prefix",
			cfg:     S3Config{Bucket: "my-bucket", Prefix: "quarry/data"},
			wantErr: false,
		},
		{
			name:    "valid bucket with region",
			cfg:     S3Config{Bucket: "my-bucket", Region: "us-west-2"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseS3Path(t *testing.T) {
	tests := []struct {
		path       string
		wantBucket string
		wantPrefix string
	}{
		{"my-bucket", "my-bucket", ""},
		{"my-bucket/prefix", "my-bucket", "prefix"},
		{"my-bucket/multi/level/prefix", "my-bucket", "multi/level/prefix"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			bucket, prefix := ParseS3Path(tt.path)
			if bucket != tt.wantBucket {
				t.Errorf("bucket = %q, want %q", bucket, tt.wantBucket)
			}
			if prefix != tt.wantPrefix {
				t.Errorf("prefix = %q, want %q", prefix, tt.wantPrefix)
			}
		})
	}
}

func TestLodeClient_WriteMetrics(t *testing.T) {
	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-123",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(cfg, lode.NewMemoryFactory())
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	snap := metrics.Snapshot{
		RunsStarted:          1,
		RunsCompleted:        1,
		EventsReceived:       10,
		EventsPersisted:      10,
		DroppedByType:        map[string]int64{"log": 2},
		ExecutorLaunchSuccess: 1,
		LodeWriteSuccess:     5,
		Policy:               "strict",
		Executor:             "executor.js",
		StorageBackend:       "fs",
		RunID:                "run-123",
	}

	completedAt := time.Date(2026, 2, 3, 15, 0, 0, 0, time.UTC)

	err = client.WriteMetrics(t.Context(), snap, completedAt)
	if err != nil {
		t.Fatalf("WriteMetrics failed: %v", err)
	}
	// Success: metrics record written without error
}

func TestToMetricsRecordMap(t *testing.T) {
	cfg := Config{
		Dataset:  "quarry",
		Source:   "my-source",
		Category: "my-category",
		Day:      "2026-02-03",
		RunID:    "run-abc",
		Policy:   "strict",
	}

	snap := metrics.Snapshot{
		RunsStarted:          1,
		RunsCompleted:        1,
		EventsReceived:       42,
		EventsPersisted:      40,
		EventsDropped:        2,
		DroppedByType:        map[string]int64{"debug": 2},
		ExecutorLaunchSuccess: 1,
		ExecutorCrash:        0,
		LodeWriteSuccess:     10,
		Policy:               "strict",
		Executor:             "executor.js",
		StorageBackend:       "fs",
		RunID:                "run-abc",
		JobID:                "job-xyz",
	}

	completedAt := time.Date(2026, 2, 3, 15, 30, 0, 0, time.UTC)
	record := toMetricsRecordMap(snap, cfg, completedAt)

	if record["record_kind"] != RecordKindMetrics {
		t.Errorf("record_kind = %v, want %q", record["record_kind"], RecordKindMetrics)
	}
	if record["event_type"] != "metrics" {
		t.Errorf("event_type = %v, want %q", record["event_type"], "metrics")
	}
	if record["source"] != cfg.Source {
		t.Errorf("source = %v, want %q", record["source"], cfg.Source)
	}
	if record["ts"] != "2026-02-03T15:30:00Z" {
		t.Errorf("ts = %v, want %q", record["ts"], "2026-02-03T15:30:00Z")
	}
	if record["runs_started_total"] != int64(1) {
		t.Errorf("runs_started_total = %v, want 1", record["runs_started_total"])
	}
	if record["events_received_total"] != int64(42) {
		t.Errorf("events_received_total = %v, want 42", record["events_received_total"])
	}
	if record["policy"] != "strict" {
		t.Errorf("policy = %v, want %q", record["policy"], "strict")
	}
	if record["job_id"] != "job-xyz" {
		t.Errorf("job_id = %v, want %q", record["job_id"], "job-xyz")
	}

	// Verify job_id is omitted when empty
	snapNoJob := snap
	snapNoJob.JobID = ""
	recordNoJob := toMetricsRecordMap(snapNoJob, cfg, completedAt)
	if _, exists := recordNoJob["job_id"]; exists {
		t.Error("job_id should be omitted when empty")
	}

	// Verify dropped_by_type is a deep copy
	dropped, ok := record["dropped_by_type"].(map[string]int64)
	if !ok {
		t.Fatalf("dropped_by_type type = %T, want map[string]int64", record["dropped_by_type"])
	}
	if dropped["debug"] != 2 {
		t.Errorf("dropped_by_type[debug] = %d, want 2", dropped["debug"])
	}
}

func TestNewClient_InitializesMaps(t *testing.T) {
	// Regression: NewLodeS3Client previously returned &LodeClient{} without
	// initializing offsets/chunksSeen maps, causing nil-map panic on first
	// WriteChunks call. All constructors now use newClient() which must
	// initialize both maps.
	ds, err := lode.NewDataset(
		lode.DatasetID("test"),
		lode.NewMemoryFactory(),
		lode.WithHiveLayout("source", "category", "day", "run_id", "event_type"),
		lode.WithCodec(lode.NewJSONLCodec()),
	)
	if err != nil {
		t.Fatalf("NewDataset failed: %v", err)
	}

	cfg := Config{
		Dataset:  "test",
		Source:   "src",
		Category: "cat",
		Day:      "2026-02-03",
		RunID:    "run-1",
	}

	client := newClient(ds, cfg, lode.NewMemoryFactory(), &retryGate{})

	if client.offsets == nil {
		t.Fatal("offsets map is nil, must be initialized")
	}
	if client.chunksSeen == nil {
		t.Fatal("chunksSeen map is nil, must be initialized")
	}

	// Verify WriteChunks works immediately (would panic with nil maps)
	chunks := []*types.ArtifactChunk{
		{ArtifactID: "art-1", Seq: 1, IsLast: true, Data: []byte("data")},
	}
	if err := client.WriteChunks(t.Context(), cfg.Dataset, cfg.RunID, chunks); err != nil {
		t.Fatalf("WriteChunks on newClient: %v", err)
	}
}

func TestToArtifactCommitRecord(t *testing.T) {
	cfg := Config{
		Dataset:  "quarry",
		Source:   "my-source",
		Category: "my-category",
		Day:      "2026-02-03",
		RunID:    "run-abc",
		Policy:   "strict",
	}

	e := &types.EventEnvelope{
		ContractVersion: "1.0.0",
		EventID:         "evt-2",
		RunID:           "run-abc",
		Seq:             2,
		Type:            types.EventTypeArtifact,
		Ts:              "2026-02-03T12:00:01Z",
		Payload: map[string]any{
			"artifact_id":  "art-123",
			"name":         "report.pdf",
			"content_type": "application/pdf",
			"size_bytes":   float64(2048),
		},
		Attempt: 1,
	}

	record := toArtifactCommitRecord(e, cfg)

	if record.RecordKind != RecordKindArtifactEvent {
		t.Errorf("RecordKind = %q, want %q", record.RecordKind, RecordKindArtifactEvent)
	}
	if record.ArtifactID != "art-123" {
		t.Errorf("ArtifactID = %q, want %q", record.ArtifactID, "art-123")
	}
	if record.Name != "report.pdf" {
		t.Errorf("Name = %q, want %q", record.Name, "report.pdf")
	}
	if record.ContentType != "application/pdf" {
		t.Errorf("ContentType = %q, want %q", record.ContentType, "application/pdf")
	}
	if record.SizeBytes != 2048 {
		t.Errorf("SizeBytes = %d, want %d", record.SizeBytes, 2048)
	}
}

// TestLodeClient_RetryObserver_CountsCASRetries proves the end-to-end path
// from a Lode snapshot conflict to lode_write_retry_total: a competing
// writer advances the dataset pointer, the client's next write conflicts
// once and retries, and the collector observes exactly one retry.
func TestLodeClient_RetryObserver_CountsCASRetries(t *testing.T) {
	mem := lode.NewMemory()
	factory := func() (lode.Store, error) { return mem, nil }

	collector := metrics.NewCollector("strict", "test", "memory", "run-123", "")
	cfg := Config{
		Dataset:       "quarry",
		Source:        "test-source",
		Category:      "test-category",
		Day:           "2026-02-03",
		RunID:         "run-123",
		Policy:        "strict",
		RetryObserver: func(int) { collector.IncLodeWriteRetry() },
	}

	client, err := NewLodeClientWithFactory(cfg, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	// Competing writer on the same store, same dataset, no retries.
	competitor, err := NewLodeClientWithFactory(Config{
		Dataset:  cfg.Dataset,
		Source:   cfg.Source,
		Category: cfg.Category,
		Day:      cfg.Day,
		RunID:    "run-other",
		Policy:   cfg.Policy,
	}, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory (competitor) failed: %v", err)
	}

	event := func(runID, id string, seq int64) []*types.EventEnvelope {
		return []*types.EventEnvelope{{
			ContractVersion: "1.0.0",
			EventID:         id,
			RunID:           runID,
			Seq:             seq,
			Type:            types.EventTypeItem,
			Ts:              "2026-02-03T12:00:00Z",
			Payload:         map[string]any{"key": "value"},
			Attempt:         1,
		}}
	}

	// Uncontended write: no retry.
	if err := client.WriteEvents(t.Context(), "run-123", "", event("run-123", "evt-1", 1)); err != nil {
		t.Fatalf("first WriteEvents failed: %v", err)
	}
	if got := collector.Snapshot().LodeWriteRetry; got != 0 {
		t.Fatalf("LodeWriteRetry after uncontended write = %d, want 0", got)
	}

	// Competitor advances the pointer; the client's cached snapshot is now stale.
	if err := competitor.WriteEvents(t.Context(), "run-other", "", event("run-other", "evt-2", 1)); err != nil {
		t.Fatalf("competitor WriteEvents failed: %v", err)
	}
	if err := client.WriteEvents(t.Context(), "run-123", "", event("run-123", "evt-3", 2)); err != nil {
		t.Fatalf("contended WriteEvents failed: %v", err)
	}
	if got := collector.Snapshot().LodeWriteRetry; got != 1 {
		t.Fatalf("LodeWriteRetry after one conflict = %d, want 1", got)
	}
}

// TestLodeClient_RetryObserver_NotCountedDuringWriteMetrics proves the
// contract rule that retries incurred persisting the metrics record are not
// observed: a competing writer makes the client's cached snapshot stale, the
// metrics write still succeeds (only possible via CAS retry), and the
// collector's retry counter is unchanged.
func TestLodeClient_RetryObserver_NotCountedDuringWriteMetrics(t *testing.T) {
	mem := lode.NewMemory()
	factory := func() (lode.Store, error) { return mem, nil }

	collector := metrics.NewCollector("strict", "test", "memory", "run-123", "")
	cfg := Config{
		Dataset:       "quarry",
		Source:        "test-source",
		Category:      "test-category",
		Day:           "2026-02-03",
		RunID:         "run-123",
		Policy:        "strict",
		RetryObserver: func(int) { collector.IncLodeWriteRetry() },
	}

	client, err := NewLodeClientWithFactory(cfg, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}
	competitor, err := NewLodeClientWithFactory(Config{
		Dataset:  cfg.Dataset,
		Source:   cfg.Source,
		Category: cfg.Category,
		Day:      cfg.Day,
		RunID:    "run-other",
		Policy:   cfg.Policy,
	}, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory (competitor) failed: %v", err)
	}

	event := func(runID, id string) []*types.EventEnvelope {
		return []*types.EventEnvelope{{
			ContractVersion: "1.0.0",
			EventID:         id,
			RunID:           runID,
			Seq:             1,
			Type:            types.EventTypeItem,
			Ts:              "2026-02-03T12:00:00Z",
			Payload:         map[string]any{"key": "value"},
			Attempt:         1,
		}}
	}

	// Seed the client's cached snapshot, then let the competitor advance the pointer.
	if err := client.WriteEvents(t.Context(), "run-123", "", event("run-123", "evt-1")); err != nil {
		t.Fatalf("client WriteEvents failed: %v", err)
	}
	if err := competitor.WriteEvents(t.Context(), "run-other", "", event("run-other", "evt-2")); err != nil {
		t.Fatalf("competitor WriteEvents failed: %v", err)
	}

	before := collector.Snapshot().LodeWriteRetry
	completedAt := time.Date(2026, 2, 3, 15, 0, 0, 0, time.UTC)
	if err := client.WriteMetrics(t.Context(), collector.Snapshot(), completedAt); err != nil {
		t.Fatalf("contended WriteMetrics failed: %v", err)
	}
	if got := collector.Snapshot().LodeWriteRetry; got != before {
		t.Fatalf("LodeWriteRetry changed during WriteMetrics: %d -> %d, want unchanged", before, got)
	}

	// Observation resumes for subsequent event writes.
	if err := competitor.WriteEvents(t.Context(), "run-other", "", event("run-other", "evt-3")); err != nil {
		t.Fatalf("competitor WriteEvents failed: %v", err)
	}
	if err := client.WriteEvents(t.Context(), "run-123", "", event("run-123", "evt-4")); err != nil {
		t.Fatalf("client WriteEvents after metrics failed: %v", err)
	}
	if got := collector.Snapshot().LodeWriteRetry; got != before+1 {
		t.Fatalf("LodeWriteRetry after post-metrics conflict = %d, want %d", got, before+1)
	}
}
