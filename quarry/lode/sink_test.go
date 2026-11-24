package lode

import (
	"context"
	"errors"
	"testing"
	"time"

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

	client := &fakeClient{}
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

	if len(client.writes) != 1 {
		t.Fatalf("expected 1 event record, got %d", len(client.writes))
	}

	record := client.writes[0]
	if record.dataset != "test-dataset" {
		t.Errorf("Dataset = %q, want %q", record.dataset, "test-dataset")
	}
	if record.runID != "run-123" {
		t.Errorf("RunID = %q, want %q", record.runID, "run-123")
	}
	if len(record.events) != 2 {
		t.Errorf("len(Events) = %d, want 2", len(record.events))
	}
}

func TestSink_Close(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	sink := NewSink(Config{
		Dataset: "test-dataset",
		RunID:   "run-123",
	}, client)

	if client.closes != 0 {
		t.Error("client should not be closed before Close()")
	}

	err := sink.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if client.closes != 1 {
		t.Error("client should be closed after Close()")
	}
}

// fakeClient records its calls and returns the errors it is given.
type fakeClient struct {
	writeErr, closeErr error
	writes             []eventWrite
	closes             int
}

type eventWrite struct {
	dataset, runID string
	events         []*types.EventEnvelope
}

func (c *fakeClient) WriteEvents(_ context.Context, dataset, runID string, events []*types.EventEnvelope) error {
	c.writes = append(c.writes, eventWrite{dataset, runID, events})
	return c.writeErr
}

func (c *fakeClient) Close() error {
	c.closes++
	return c.closeErr
}

var _ Client = (*fakeClient)(nil)

func TestSink_WriteEvents_DiskFullError(t *testing.T) {
	t.Parallel()

	diskFullErr := &diskFullError{msg: "no space left on device"}
	client := &fakeClient{writeErr: diskFullErr}
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
	if len(client.writes) != 1 {
		t.Errorf("expected 1 write call, got %d", len(client.writes))
	}
}

func TestSink_WriteEvents_PermissionError(t *testing.T) {
	t.Parallel()

	permErr := &permissionError{msg: "permission denied"}
	client := &fakeClient{writeErr: permErr}
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

func TestSink_Close_Error(t *testing.T) {
	t.Parallel()

	closeErr := &closeError{msg: "failed to close storage"}
	client := &fakeClient{closeErr: closeErr}
	sink := NewSink(Config{Dataset: "test", RunID: "run-1"}, client)

	err := sink.Close()
	if err == nil {
		t.Fatal("expected error on close, got nil")
	}

	if !errors.Is(err, closeErr) {
		t.Errorf("expected close error, got: %v", err)
	}

	if client.closes != 1 {
		t.Errorf("expected 1 close call, got %d", client.closes)
	}
}

// Error types for simulating storage failures
type diskFullError struct{ msg string }

func (e *diskFullError) Error() string { return e.msg }

type permissionError struct{ msg string }

func (e *permissionError) Error() string { return e.msg }

type closeError struct{ msg string }

func (e *closeError) Error() string { return e.msg }
