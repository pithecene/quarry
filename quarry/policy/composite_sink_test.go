package policy

import (
	"errors"
	"testing"

	"github.com/pithecene/quarry/types"
)

func TestCompositeSink_RoutesEventsToEventSink(t *testing.T) {
	t.Parallel()

	eventSink := &stubEventSink{}
	chunkSink := NewStubSink()

	composite := NewCompositeSink(eventSink, chunkSink)

	events := testEnvelopes(3)
	if err := composite.WriteEvents(t.Context(), events); err != nil {
		t.Fatalf("write events: %v", err)
	}

	if eventSink.eventsWritten != 3 {
		t.Errorf("event sink: expected 3 events, got %d", eventSink.eventsWritten)
	}
	// Chunks sink should not have received events
	stats := chunkSink.Stats()
	if stats.EventsWritten != 0 {
		t.Errorf("chunk sink should not receive events, got %d", stats.EventsWritten)
	}
}

func TestCompositeSink_RoutesChunksToChunkSink(t *testing.T) {
	t.Parallel()

	eventSink := &stubEventSink{}
	chunkSink := NewStubSink()

	composite := NewCompositeSink(eventSink, chunkSink)

	chunks := []*types.ArtifactChunk{
		{ArtifactID: "art-001", Seq: 1},
		{ArtifactID: "art-001", Seq: 2},
	}
	if err := composite.WriteChunks(t.Context(), chunks); err != nil {
		t.Fatalf("write chunks: %v", err)
	}

	stats := chunkSink.Stats()
	if stats.ChunksWritten != 2 {
		t.Errorf("chunk sink: expected 2 chunks, got %d", stats.ChunksWritten)
	}
	if eventSink.eventsWritten != 0 {
		t.Errorf("event sink should not receive chunks, got %d events", eventSink.eventsWritten)
	}
}

func TestCompositeSink_EventErrorPropagates(t *testing.T) {
	t.Parallel()

	eventSink := &stubEventSink{writeErr: errors.New("event write failed")}
	chunkSink := NewStubSink()

	composite := NewCompositeSink(eventSink, chunkSink)

	err := composite.WriteEvents(t.Context(), testEnvelopes(1))
	if err == nil {
		t.Fatal("expected error from event sink")
	}
}

func TestCompositeSink_ChunkErrorPropagates(t *testing.T) {
	t.Parallel()

	eventSink := &stubEventSink{}
	chunkSink := NewStubSink()
	chunkSink.ErrorOnWrite = errors.New("chunk write failed")

	composite := NewCompositeSink(eventSink, chunkSink)

	chunks := []*types.ArtifactChunk{{ArtifactID: "art-001", Seq: 1}}
	err := composite.WriteChunks(t.Context(), chunks)
	if err == nil {
		t.Fatal("expected error from chunk sink")
	}
}

func TestCompositeSink_ClosesBoth(t *testing.T) {
	t.Parallel()

	eventSink := &stubEventSink{}
	chunkSink := NewStubSink()

	composite := NewCompositeSink(eventSink, chunkSink)

	if err := composite.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !eventSink.closed {
		t.Error("event sink not closed")
	}
	if !chunkSink.Closed {
		t.Error("chunk sink not closed")
	}
}

func TestCompositeSink_PanicsOnNilEventSink(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil event sink")
		}
	}()
	NewCompositeSink(nil, NewStubSink())
}

func TestCompositeSink_SharedLodeSinkClosedOnce(t *testing.T) {
	t.Parallel()

	// When Lode is both the chunk sink and an event sink (via fanout),
	// CompositeSink.Close() calls close on both paths. The underlying Lode
	// sink must tolerate being closed through both the event and chunk paths.
	// This test makes the behavior explicit.
	sharedSink := NewStubSink()

	fanout := NewFanoutEventSink([]SinkEntry{
		{Sink: sharedSink, Delivery: DeliveryMandatory, Label: "lode"},
	})

	composite := NewCompositeSink(fanout, sharedSink)

	if err := composite.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// StubSink.Closed should be true — the key property is that Close()
	// doesn't error or panic when the same sink is closed twice.
	if !sharedSink.Closed {
		t.Error("shared sink should be closed")
	}
}

func TestCompositeSink_PanicsOnNilChunkSink(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil chunk sink")
		}
	}()
	NewCompositeSink(&stubEventSink{}, nil)
}
