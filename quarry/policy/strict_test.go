package policy_test

import (
	"errors"
	"testing"

	"github.com/pithecene/quarry/policy"
	"github.com/pithecene/quarry/types"
)

func TestStrictPolicy_IngestEvent_ImmediateWrite(t *testing.T) {
	sink := policy.NewStubSink()
	pol := policy.NewStrictPolicy(sink)

	envelope := &types.EventEnvelope{
		EventID: "e1",
		Type:    types.EventTypeItem,
		RunID:   "run-1",
		Seq:     1,
	}

	err := pol.IngestEvent(t.Context(), envelope)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify immediate write (batch of 1)
	sinkStats := sink.Stats()
	if sinkStats.EventsWritten != 1 {
		t.Errorf("expected 1 event written immediately, got %d", sinkStats.EventsWritten)
	}
	if sinkStats.EventBatches != 1 {
		t.Errorf("expected 1 batch, got %d", sinkStats.EventBatches)
	}

	// Verify policy stats
	stats := pol.Stats()
	if stats.TotalEvents != 1 {
		t.Errorf("expected TotalEvents=1, got %d", stats.TotalEvents)
	}
	if stats.EventsPersisted != 1 {
		t.Errorf("expected EventsPersisted=1, got %d", stats.EventsPersisted)
	}
	if stats.EventsDropped != 0 {
		t.Errorf("expected EventsDropped=0, got %d", stats.EventsDropped)
	}
}

func TestStrictPolicy_NoDrops(t *testing.T) {
	sink := policy.NewStubSink()
	pol := policy.NewStrictPolicy(sink)

	// Ingest all event types - strict policy should never drop
	eventTypes := []types.EventType{
		types.EventTypeItem,
		types.EventTypeArtifact,
		types.EventTypeCheckpoint,
		types.EventTypeLog,
		types.EventTypeEnqueue,
		types.EventTypeRotateProxy,
		types.EventTypeRunComplete,
	}

	for i, et := range eventTypes {
		envelope := &types.EventEnvelope{
			EventID: "e" + string(rune('0'+i)),
			Type:    et,
			RunID:   "run-1",
			Seq:     int64(i + 1),
		}
		if err := pol.IngestEvent(t.Context(), envelope); err != nil {
			t.Fatalf("unexpected error for %s: %v", et, err)
		}
	}

	stats := pol.Stats()
	if stats.EventsDropped != 0 {
		t.Errorf("strict policy should never drop, got %d drops", stats.EventsDropped)
	}
	if stats.EventsPersisted != int64(len(eventTypes)) {
		t.Errorf("expected %d persisted, got %d", len(eventTypes), stats.EventsPersisted)
	}
}

func TestStrictPolicy_IngestArtifactChunk(t *testing.T) {
	sink := policy.NewStubSink()
	pol := policy.NewStrictPolicy(sink)

	chunk := &types.ArtifactChunk{
		ArtifactID: "a1",
		Seq:        1,
		Data:       []byte("test data"),
		IsLast:     true,
	}

	err := pol.IngestArtifactChunk(t.Context(), chunk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify immediate write
	sinkStats := sink.Stats()
	if sinkStats.ChunksWritten != 1 {
		t.Errorf("expected 1 chunk written, got %d", sinkStats.ChunksWritten)
	}

	stats := pol.Stats()
	if stats.TotalChunks != 1 {
		t.Errorf("expected TotalChunks=1, got %d", stats.TotalChunks)
	}
	if stats.ChunksPersisted != 1 {
		t.Errorf("expected ChunksPersisted=1, got %d", stats.ChunksPersisted)
	}
}

func TestStrictPolicy_SinkError(t *testing.T) {
	sink := policy.NewStubSink()
	expectedErr := errors.New("sink failure")
	sink.ErrorOnWrite = expectedErr

	pol := policy.NewStrictPolicy(sink)

	envelope := &types.EventEnvelope{
		EventID: "e1",
		Type:    types.EventTypeItem,
	}

	err := pol.IngestEvent(t.Context(), envelope)
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	stats := pol.Stats()
	if stats.Errors != 1 {
		t.Errorf("expected Errors=1, got %d", stats.Errors)
	}
}

func TestStrictPolicy_Flush_NoOp(t *testing.T) {
	sink := policy.NewStubSink()
	pol := policy.NewStrictPolicy(sink)

	// Ingest an event
	envelope := &types.EventEnvelope{EventID: "e1", Type: types.EventTypeItem}
	_ = pol.IngestEvent(t.Context(), envelope)

	// Flush should be a no-op (nothing buffered)
	beforeBatches := sink.Stats().EventBatches

	err := pol.Flush(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	afterBatches := sink.Stats().EventBatches
	if afterBatches != beforeBatches {
		t.Errorf("flush should not write additional batches")
	}

	stats := pol.Stats()
	if stats.FlushCount != 1 {
		t.Errorf("expected FlushCount=1, got %d", stats.FlushCount)
	}
}

func TestStrictPolicy_OrderingPreserved(t *testing.T) {
	sink := policy.NewStubSink()
	pol := policy.NewStrictPolicy(sink)

	// Ingest events in order
	for i := 1; i <= 5; i++ {
		envelope := &types.EventEnvelope{
			EventID: "e" + string(rune('0'+i)),
			Type:    types.EventTypeItem,
			Seq:     int64(i),
		}
		if err := pol.IngestEvent(t.Context(), envelope); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// Verify ordering in sink
	if len(sink.WrittenEvents) != 5 {
		t.Fatalf("expected 5 events, got %d", len(sink.WrittenEvents))
	}
	for i, event := range sink.WrittenEvents {
		expectedSeq := int64(i + 1)
		if event.Seq != expectedSeq {
			t.Errorf("event %d: expected seq %d, got %d", i, expectedSeq, event.Seq)
		}
	}
}

func TestStrictPolicy_Close(t *testing.T) {
	sink := policy.NewStubSink()
	pol := policy.NewStrictPolicy(sink)

	err := pol.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !sink.Stats().Closed {
		t.Error("sink should be closed after policy Close()")
	}
}
