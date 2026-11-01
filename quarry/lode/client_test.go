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
	t.Parallel()

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-123",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(t.Context(), cfg, lode.Memory())
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

func TestLodeClient_CommitRejectsMissingArtifactID(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-123",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(t.Context(), cfg, lode.Memory())
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

func TestToEventRecord(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

	cfg := Config{
		Dataset:  "quarry",
		Source:   "test-source",
		Category: "test-category",
		Day:      "2026-02-03",
		RunID:    "run-123",
		Policy:   "strict",
	}

	client, err := NewLodeClientWithFactory(t.Context(), cfg, lode.Memory())
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	snap := metrics.Snapshot{
		RunsStarted:           1,
		RunsCompleted:         1,
		EventsReceived:        10,
		EventsPersisted:       10,
		DroppedByType:         map[string]int64{"log": 2},
		ExecutorLaunchSuccess: 1,
		LodeWriteSuccess:      5,
		Policy:                "strict",
		Executor:              "executor.js",
		StorageBackend:        "fs",
		RunID:                 "run-123",
	}

	completedAt := time.Date(2026, 2, 3, 15, 0, 0, 0, time.UTC)

	err = client.WriteMetrics(t.Context(), snap, completedAt)
	if err != nil {
		t.Fatalf("WriteMetrics failed: %v", err)
	}
	// Success: metrics record written without error
}

func TestToMetricsRecordMap(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Dataset:  "quarry",
		Source:   "my-source",
		Category: "my-category",
		Day:      "2026-02-03",
		RunID:    "run-abc",
		Policy:   "strict",
	}

	snap := metrics.Snapshot{
		RunsStarted:           1,
		RunsCompleted:         1,
		EventsReceived:        42,
		EventsPersisted:       40,
		EventsDropped:         2,
		DroppedByType:         map[string]int64{"debug": 2},
		ExecutorLaunchSuccess: 1,
		ExecutorCrash:         0,
		LodeWriteSuccess:      10,
		Policy:                "strict",
		Executor:              "executor.js",
		StorageBackend:        "fs",
		RunID:                 "run-abc",
		JobID:                 "job-xyz",
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

func TestToArtifactCommitRecord(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

	mem := lode.NewMemory()
	factory := func() (lode.RangeStore, error) { return mem, nil }

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

	client, err := NewLodeClientWithFactory(t.Context(), cfg, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	// Competing writer on the same store, same dataset, no retries.
	competitor, err := NewLodeClientWithFactory(t.Context(), Config{
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
	t.Parallel()

	mem := lode.NewMemory()
	factory := func() (lode.RangeStore, error) { return mem, nil }

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

	client, err := NewLodeClientWithFactory(t.Context(), cfg, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}
	competitor, err := NewLodeClientWithFactory(t.Context(), Config{
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

// An artifact event commits with the storage result the runtime attached,
// and is refused without one.
func TestLodeClient_WriteArtifactEvent(t *testing.T) {
	t.Parallel()

	cfg := Config{Dataset: "quarry", Source: "test-source", Category: "test-category", Day: "2026-02-03", RunID: "run-123", Policy: "strict"}
	client, err := NewLodeClientWithFactory(t.Context(), cfg, lode.Memory())
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}
	event := func(storage *types.ArtifactStorage) *types.EventEnvelope {
		return &types.EventEnvelope{
			ContractVersion: "1.0.0", EventID: "evt-3", RunID: "run-123", Seq: 3, Type: types.EventTypeArtifact, Ts: "2026-02-03T12:00:02Z",
			Payload: map[string]any{"artifact_id": "art-1", "name": "screenshot.png", "content_type": "image/png", "size_bytes": float64(1024)},
			Attempt: 1, Storage: storage,
		}
	}
	if err := client.WriteEvents(t.Context(), cfg.Dataset, cfg.RunID, []*types.EventEnvelope{event(nil)}); !errors.Is(err, ErrCommitWithoutStorage) {
		t.Fatalf("WriteEvents without storage = %v; want ErrCommitWithoutStorage", err)
	}
	storage := &types.ArtifactStorage{Kind: types.ArtifactStorageVolume, VolumeSnapshotID: "snap-1", SHA256: "ab"}
	if err := client.WriteEvents(t.Context(), cfg.Dataset, cfg.RunID, []*types.EventEnvelope{event(storage)}); err != nil {
		t.Fatalf("WriteEvents with storage: %v", err)
	}
}
