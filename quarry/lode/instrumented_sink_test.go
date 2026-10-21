package lode

import (
	"context"
	"errors"
	"testing"

	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/types"
)

// failingSink is a test double that returns errors on writes.
type failingSink struct {
	writeErr error
	closed   bool
}

func (s *failingSink) WriteEvents(_ context.Context, _ []*types.EventEnvelope) error {
	return s.writeErr
}

func (s *failingSink) WriteChunks(_ context.Context, _ []*types.ArtifactChunk) error {
	return s.writeErr
}

func (s *failingSink) Close() error {
	s.closed = true
	return nil
}

// successSink is a test double that accepts all writes.
type successSink struct {
	eventCalls int
	chunkCalls int
	closed     bool
}

func (s *successSink) WriteEvents(_ context.Context, _ []*types.EventEnvelope) error {
	s.eventCalls++
	return nil
}

func (s *successSink) WriteChunks(_ context.Context, _ []*types.ArtifactChunk) error {
	s.chunkCalls++
	return nil
}

func (s *successSink) Close() error {
	s.closed = true
	return nil
}

func TestInstrumentedSink_WriteEventsSuccess(t *testing.T) {
	inner := &successSink{}
	collector := metrics.NewCollector("strict", "node", "fs", "run-001", "")
	sink := NewInstrumentedSink(inner, collector)

	ctx := t.Context()
	events := []*types.EventEnvelope{{Type: types.EventTypeItem, Seq: 1}}

	if err := sink.WriteEvents(ctx, events); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	snap := collector.Snapshot()
	if snap.LodeWriteSuccess != 1 {
		t.Errorf("LodeWriteSuccess = %d, want 1", snap.LodeWriteSuccess)
	}
	if snap.LodeWriteFailure != 0 {
		t.Errorf("LodeWriteFailure = %d, want 0", snap.LodeWriteFailure)
	}
	if inner.eventCalls != 1 {
		t.Errorf("inner.eventCalls = %d, want 1", inner.eventCalls)
	}
}

func TestInstrumentedSink_WriteEventsFailure(t *testing.T) {
	writeErr := errors.New("disk full")
	inner := &failingSink{writeErr: writeErr}
	collector := metrics.NewCollector("strict", "node", "fs", "run-001", "")
	sink := NewInstrumentedSink(inner, collector)

	ctx := t.Context()
	events := []*types.EventEnvelope{{Type: types.EventTypeItem, Seq: 1}}

	err := sink.WriteEvents(ctx, events)
	if !errors.Is(err, writeErr) {
		t.Fatalf("expected %v, got %v", writeErr, err)
	}

	snap := collector.Snapshot()
	if snap.LodeWriteSuccess != 0 {
		t.Errorf("LodeWriteSuccess = %d, want 0", snap.LodeWriteSuccess)
	}
	if snap.LodeWriteFailure != 1 {
		t.Errorf("LodeWriteFailure = %d, want 1", snap.LodeWriteFailure)
	}
}

func TestInstrumentedSink_WriteChunksSuccess(t *testing.T) {
	inner := &successSink{}
	collector := metrics.NewCollector("strict", "node", "fs", "run-001", "")
	sink := NewInstrumentedSink(inner, collector)

	ctx := t.Context()
	chunks := []*types.ArtifactChunk{{ArtifactID: "art-1", Seq: 1, Data: []byte("data")}}

	if err := sink.WriteChunks(ctx, chunks); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	snap := collector.Snapshot()
	if snap.LodeWriteSuccess != 1 {
		t.Errorf("LodeWriteSuccess = %d, want 1", snap.LodeWriteSuccess)
	}
	if snap.LodeWriteFailure != 0 {
		t.Errorf("LodeWriteFailure = %d, want 0", snap.LodeWriteFailure)
	}
	if inner.chunkCalls != 1 {
		t.Errorf("inner.chunkCalls = %d, want 1", inner.chunkCalls)
	}
}

func TestInstrumentedSink_WriteChunksFailure(t *testing.T) {
	writeErr := errors.New("s3 timeout")
	inner := &failingSink{writeErr: writeErr}
	collector := metrics.NewCollector("strict", "node", "fs", "run-001", "")
	sink := NewInstrumentedSink(inner, collector)

	ctx := t.Context()
	chunks := []*types.ArtifactChunk{{ArtifactID: "art-1", Seq: 1, Data: []byte("data")}}

	err := sink.WriteChunks(ctx, chunks)
	if !errors.Is(err, writeErr) {
		t.Fatalf("expected %v, got %v", writeErr, err)
	}

	snap := collector.Snapshot()
	if snap.LodeWriteSuccess != 0 {
		t.Errorf("LodeWriteSuccess = %d, want 0", snap.LodeWriteSuccess)
	}
	if snap.LodeWriteFailure != 1 {
		t.Errorf("LodeWriteFailure = %d, want 1", snap.LodeWriteFailure)
	}
}

func TestInstrumentedSink_CloseDelegate(t *testing.T) {
	inner := &successSink{}
	collector := metrics.NewCollector("strict", "node", "fs", "run-001", "")
	sink := NewInstrumentedSink(inner, collector)

	if err := sink.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !inner.closed {
		t.Error("Close should delegate to inner sink")
	}
}

func TestInstrumentedSink_MultipleCalls(t *testing.T) {
	inner := &successSink{}
	collector := metrics.NewCollector("strict", "node", "fs", "run-001", "")
	sink := NewInstrumentedSink(inner, collector)

	ctx := t.Context()

	// 3 successful event writes + 2 successful chunk writes
	for range 3 {
		_ = sink.WriteEvents(ctx, []*types.EventEnvelope{{Type: types.EventTypeItem, Seq: 1}})
	}
	for range 2 {
		_ = sink.WriteChunks(ctx, []*types.ArtifactChunk{{ArtifactID: "art-1", Seq: 1, Data: []byte("data")}})
	}

	snap := collector.Snapshot()
	if snap.LodeWriteSuccess != 5 {
		t.Errorf("LodeWriteSuccess = %d, want 5", snap.LodeWriteSuccess)
	}
}
