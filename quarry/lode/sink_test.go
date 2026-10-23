package lode

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/types"
)

func TestDeriveDay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		startTime time.Time
		want      string
	}{
		{
			name:      "UTC time",
			startTime: time.Date(2026, 2, 3, 14, 30, 0, 0, time.UTC),
			want:      "2026-02-03",
		},
		{
			name:      "Non-UTC time converts to UTC",
			startTime: time.Date(2026, 2, 3, 22, 0, 0, 0, time.FixedZone("EST", -5*3600)),
			want:      "2026-02-04", // 22:00 EST = 03:00 UTC next day
		},
		{
			name:      "Single digit month and day",
			startTime: time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC),
			want:      "2026-01-05",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DeriveDay(tt.startTime)
			if got != tt.want {
				t.Errorf("DeriveDay() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSink_WriteEvents(t *testing.T) {
	t.Parallel()

	client := NewStubClient()
	sink := NewSink(Config{
		Dataset: "test-dataset",
		RunID:   "run-123",
	}, client)

	events := []*types.EventEnvelope{
		{Type: "item", RunID: "run-123", Seq: 1},
		{Type: "log", RunID: "run-123", Seq: 2},
	}

	err := sink.WriteEvents(t.Context(), events)
	if err != nil {
		t.Fatalf("WriteEvents failed: %v", err)
	}

	if len(client.Events) != 1 {
		t.Fatalf("expected 1 event record, got %d", len(client.Events))
	}

	record := client.Events[0]
	if record.Dataset != "test-dataset" {
		t.Errorf("Dataset = %q, want %q", record.Dataset, "test-dataset")
	}
	if record.RunID != "run-123" {
		t.Errorf("RunID = %q, want %q", record.RunID, "run-123")
	}
	if len(record.Events) != 2 {
		t.Errorf("len(Events) = %d, want 2", len(record.Events))
	}
}

func TestSink_WriteChunks(t *testing.T) {
	t.Parallel()

	client := NewStubClient()
	sink := NewSink(Config{
		Dataset: "test-dataset",
		RunID:   "run-123",
	}, client)

	chunks := []*types.ArtifactChunk{
		{ArtifactID: "art-1", Seq: 1, Data: []byte("hello")},
		{ArtifactID: "art-1", Seq: 2, Data: []byte("world"), IsLast: true},
	}

	err := sink.WriteChunks(t.Context(), chunks)
	if err != nil {
		t.Fatalf("WriteChunks failed: %v", err)
	}

	if len(client.Chunks) != 1 {
		t.Fatalf("expected 1 chunk record, got %d", len(client.Chunks))
	}

	record := client.Chunks[0]
	if record.Dataset != "test-dataset" {
		t.Errorf("Dataset = %q, want %q", record.Dataset, "test-dataset")
	}
	if len(record.Chunks) != 2 {
		t.Errorf("len(Chunks) = %d, want 2", len(record.Chunks))
	}
}

func TestSink_Close(t *testing.T) {
	t.Parallel()

	client := NewStubClient()
	sink := NewSink(Config{
		Dataset: "test-dataset",
		RunID:   "run-123",
	}, client)

	if client.Closed {
		t.Error("client should not be closed before Close()")
	}

	err := sink.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if !client.Closed {
		t.Error("client should be closed after Close()")
	}
}

// =============================================================================
// Phase 5: Storage Write Error Tests
// =============================================================================

// FailingClient simulates storage write failures (disk full, permission errors, etc.)
type FailingClient struct {
	EventWriteErr   error
	ChunkWriteErr   error
	MetricsWriteErr error
	CloseErr        error
	// Track calls for verification
	EventWriteCalls   int
	ChunkWriteCalls   int
	MetricsWriteCalls int
	CloseCalls        int
}

func (c *FailingClient) WriteEvents(_ context.Context, _, _ string, _ []*types.EventEnvelope) error {
	c.EventWriteCalls++
	return c.EventWriteErr
}

func (c *FailingClient) WriteChunks(_ context.Context, _, _ string, _ []*types.ArtifactChunk) error {
	c.ChunkWriteCalls++
	return c.ChunkWriteErr
}

func (c *FailingClient) WriteMetrics(_ context.Context, _ metrics.Snapshot, _ time.Time) error {
	c.MetricsWriteCalls++
	return c.MetricsWriteErr
}

func (c *FailingClient) Close() error {
	c.CloseCalls++
	return c.CloseErr
}

var _ Client = (*FailingClient)(nil)

func TestSink_WriteEvents_DiskFullError(t *testing.T) {
	t.Parallel()

	diskFullErr := &diskFullError{msg: "no space left on device"}
	client := &FailingClient{EventWriteErr: diskFullErr}
	sink := NewSink(Config{Dataset: "test", RunID: "run-1"}, client)

	events := []*types.EventEnvelope{
		{Type: "item", RunID: "run-1", Seq: 1},
	}

	err := sink.WriteEvents(t.Context(), events)
	if err == nil {
		t.Fatal("expected error for disk full, got nil")
	}

	// Error should propagate
	if !errors.Is(err, diskFullErr) {
		t.Errorf("expected disk full error, got: %v", err)
	}

	// Write should have been attempted
	if client.EventWriteCalls != 1 {
		t.Errorf("expected 1 write call, got %d", client.EventWriteCalls)
	}
}

func TestSink_WriteChunks_DiskFullError(t *testing.T) {
	t.Parallel()

	diskFullErr := &diskFullError{msg: "no space left on device"}
	client := &FailingClient{ChunkWriteErr: diskFullErr}
	sink := NewSink(Config{Dataset: "test", RunID: "run-1"}, client)

	chunks := []*types.ArtifactChunk{
		{ArtifactID: "art-1", Seq: 1, Data: []byte("data")},
	}

	err := sink.WriteChunks(t.Context(), chunks)
	if err == nil {
		t.Fatal("expected error for disk full, got nil")
	}

	if !errors.Is(err, diskFullErr) {
		t.Errorf("expected disk full error, got: %v", err)
	}

	if client.ChunkWriteCalls != 1 {
		t.Errorf("expected 1 write call, got %d", client.ChunkWriteCalls)
	}
}

func TestSink_WriteEvents_PermissionError(t *testing.T) {
	t.Parallel()

	permErr := &permissionError{msg: "permission denied"}
	client := &FailingClient{EventWriteErr: permErr}
	sink := NewSink(Config{Dataset: "test", RunID: "run-1"}, client)

	events := []*types.EventEnvelope{
		{Type: "item", RunID: "run-1", Seq: 1},
	}

	err := sink.WriteEvents(t.Context(), events)
	if err == nil {
		t.Fatal("expected error for permission denied, got nil")
	}

	if !errors.Is(err, permErr) {
		t.Errorf("expected permission error, got: %v", err)
	}
}

func TestSink_WriteChunks_PermissionError(t *testing.T) {
	t.Parallel()

	permErr := &permissionError{msg: "permission denied"}
	client := &FailingClient{ChunkWriteErr: permErr}
	sink := NewSink(Config{Dataset: "test", RunID: "run-1"}, client)

	chunks := []*types.ArtifactChunk{
		{ArtifactID: "art-1", Seq: 1, Data: []byte("data")},
	}

	err := sink.WriteChunks(t.Context(), chunks)
	if err == nil {
		t.Fatal("expected error for permission denied, got nil")
	}

	if !errors.Is(err, permErr) {
		t.Errorf("expected permission error, got: %v", err)
	}
}

func TestSink_Close_Error(t *testing.T) {
	t.Parallel()

	closeErr := &closeError{msg: "failed to close storage"}
	client := &FailingClient{CloseErr: closeErr}
	sink := NewSink(Config{Dataset: "test", RunID: "run-1"}, client)

	err := sink.Close()
	if err == nil {
		t.Fatal("expected error on close, got nil")
	}

	if !errors.Is(err, closeErr) {
		t.Errorf("expected close error, got: %v", err)
	}

	if client.CloseCalls != 1 {
		t.Errorf("expected 1 close call, got %d", client.CloseCalls)
	}
}

// Error types for simulating storage failures
type diskFullError struct{ msg string }

func (e *diskFullError) Error() string { return e.msg }

type permissionError struct{ msg string }

func (e *permissionError) Error() string { return e.msg }

type closeError struct{ msg string }

func (e *closeError) Error() string { return e.msg }
