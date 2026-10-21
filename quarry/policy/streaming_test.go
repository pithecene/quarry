package policy_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/policy"
	"github.com/pithecene/quarry/types"
)

// helper to create streaming policy or fail test
func mustNewStreamingPolicy(t *testing.T, sink policy.Sink, config policy.StreamingConfig) *policy.StreamingPolicy {
	t.Helper()
	pol, err := policy.NewStreamingPolicy(sink, config)
	if err != nil {
		t.Fatalf("NewStreamingPolicy failed: %v", err)
	}
	t.Cleanup(iox.CloseFunc(pol))
	return pol
}

func TestStreamingPolicy_InvalidConfig_BothZero(t *testing.T) {
	sink := policy.NewStubSink()
	_, err := policy.NewStreamingPolicy(sink, policy.StreamingConfig{
		FlushCount:    0,
		FlushInterval: 0,
	})
	if !errors.Is(err, policy.ErrStreamingInvalidConfig) {
		t.Errorf("expected ErrStreamingInvalidConfig, got %v", err)
	}
}

func TestStreamingPolicy_ValidConfig_OnlyCount(t *testing.T) {
	sink := policy.NewStubSink()
	pol, err := policy.NewStreamingPolicy(sink, policy.StreamingConfig{FlushCount: 5})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	iox.DiscardClose(pol)
}

func TestStreamingPolicy_ValidConfig_OnlyInterval(t *testing.T) {
	sink := policy.NewStubSink()
	pol, err := policy.NewStreamingPolicy(sink, policy.StreamingConfig{FlushInterval: time.Second})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	iox.DiscardClose(pol)
}

func TestStreamingPolicy_ValidConfig_Both(t *testing.T) {
	sink := policy.NewStubSink()
	pol, err := policy.NewStreamingPolicy(sink, policy.StreamingConfig{
		FlushCount:    10,
		FlushInterval: time.Second,
	})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	iox.DiscardClose(pol)
}

func TestStreamingPolicy_CountTrigger_FlushesAtThreshold(t *testing.T) {
	sink := policy.NewStubSink()
	pol := mustNewStreamingPolicy(t, sink, policy.StreamingConfig{FlushCount: 3})

	// Ingest 2 events — below threshold, no flush
	for i := 1; i <= 2; i++ {
		if err := pol.IngestEvent(t.Context(), &types.EventEnvelope{
			EventID: "e" + string(rune('0'+i)),
			Type:    types.EventTypeItem,
			Seq:     int64(i),
		}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if sink.Stats().EventsWritten != 0 {
		t.Errorf("expected 0 events written below threshold, got %d", sink.Stats().EventsWritten)
	}

	// 3rd event should trigger flush
	if err := pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e3",
		Type:    types.EventTypeItem,
		Seq:     3,
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if sink.Stats().EventsWritten != 3 {
		t.Errorf("expected 3 events written at threshold, got %d", sink.Stats().EventsWritten)
	}
}

func TestStreamingPolicy_NoDrops_AllEventTypes(t *testing.T) {
	sink := policy.NewStubSink()
	pol := mustNewStreamingPolicy(t, sink, policy.StreamingConfig{FlushCount: 100})

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
		if err := pol.IngestEvent(t.Context(), &types.EventEnvelope{
			EventID: "e" + string(rune('0'+i)),
			Type:    et,
			Seq:     int64(i + 1),
		}); err != nil {
			t.Fatalf("unexpected error for %s: %v", et, err)
		}
	}

	// Flush and verify all persisted
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	stats := pol.Stats()
	if stats.EventsDropped != 0 {
		t.Errorf("streaming policy should never drop, got %d drops", stats.EventsDropped)
	}
	if stats.EventsPersisted != int64(len(eventTypes)) {
		t.Errorf("expected %d persisted, got %d", len(eventTypes), stats.EventsPersisted)
	}
}

func TestStreamingPolicy_OrderingPreserved(t *testing.T) {
	sink := policy.NewStubSink()
	pol := mustNewStreamingPolicy(t, sink, policy.StreamingConfig{FlushCount: 100})

	for i := 1; i <= 5; i++ {
		_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
			EventID: "e" + string(rune('0'+i)),
			Type:    types.EventTypeItem,
			Seq:     int64(i),
		})
	}

	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	if len(sink.WrittenEvents) != 5 {
		t.Fatalf("expected 5 events, got %d", len(sink.WrittenEvents))
	}
	for i, ev := range sink.WrittenEvents {
		expectedSeq := int64(i + 1)
		if ev.Seq != expectedSeq {
			t.Errorf("event %d: expected seq %d, got %d", i, expectedSeq, ev.Seq)
		}
	}
}

func TestStreamingPolicy_ArtifactChunks_Buffered(t *testing.T) {
	sink := policy.NewStubSink()
	pol := mustNewStreamingPolicy(t, sink, policy.StreamingConfig{FlushCount: 100})

	for i := 0; i < 3; i++ {
		if err := pol.IngestArtifactChunk(t.Context(), &types.ArtifactChunk{
			ArtifactID: "a1",
			Seq:        int64(i + 1),
			Data:       []byte("test data"),
			IsLast:     i == 2,
		}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// Not written yet
	if sink.Stats().ChunksWritten != 0 {
		t.Errorf("expected 0 chunks written before flush, got %d", sink.Stats().ChunksWritten)
	}

	// Flush writes chunks
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	if sink.Stats().ChunksWritten != 3 {
		t.Errorf("expected 3 chunks written after flush, got %d", sink.Stats().ChunksWritten)
	}

	stats := pol.Stats()
	if stats.TotalChunks != 3 {
		t.Errorf("expected TotalChunks=3, got %d", stats.TotalChunks)
	}
	if stats.ChunksPersisted != 3 {
		t.Errorf("expected ChunksPersisted=3, got %d", stats.ChunksPersisted)
	}
}

func TestStreamingPolicy_ChunksFirstOrdering(t *testing.T) {
	sink := policy.NewStubSink()
	pol := mustNewStreamingPolicy(t, sink, policy.StreamingConfig{FlushCount: 100})

	// Buffer both events and chunks
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem, Seq: 1,
	})
	_ = pol.IngestArtifactChunk(t.Context(), &types.ArtifactChunk{
		ArtifactID: "a1", Seq: 1, Data: []byte("data"),
	})

	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Verify write order: chunks first, then events
	if len(sink.WriteOrder) != 2 {
		t.Fatalf("expected 2 write ops, got %d", len(sink.WriteOrder))
	}
	if sink.WriteOrder[0].Type != "chunks" {
		t.Errorf("expected first write to be chunks, got %s", sink.WriteOrder[0].Type)
	}
	if sink.WriteOrder[1].Type != "events" {
		t.Errorf("expected second write to be events, got %s", sink.WriteOrder[1].Type)
	}
}

func TestStreamingPolicy_FlushFailure_PreservesBuffers(t *testing.T) {
	sink := policy.NewStubSink()
	pol := mustNewStreamingPolicy(t, sink, policy.StreamingConfig{FlushCount: 100})

	// Buffer events
	for i := 1; i <= 3; i++ {
		_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
			EventID: "e" + string(rune('0'+i)),
			Type:    types.EventTypeItem,
			Seq:     int64(i),
		})
	}

	// Make sink fail
	sink.ErrorOnWrite = errors.New("write failed")

	err := pol.Flush(t.Context())
	if err == nil {
		t.Fatal("expected flush to fail")
	}

	// Buffer should still have data
	stats := pol.Stats()
	if stats.BufferSize == 0 {
		t.Error("buffer should not be cleared on flush failure")
	}
	if stats.Errors != 1 {
		t.Errorf("expected Errors=1, got %d", stats.Errors)
	}

	// Fix sink and retry
	sink.ErrorOnWrite = nil
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("retry flush failed: %v", err)
	}

	if sink.Stats().EventsWritten != 3 {
		t.Errorf("expected 3 events written after retry, got %d", sink.Stats().EventsWritten)
	}
}

func TestStreamingPolicy_ChunkWriteFailure_PreservesBothBuffers(t *testing.T) {
	sink := policy.NewStubSink()
	pol := mustNewStreamingPolicy(t, sink, policy.StreamingConfig{FlushCount: 100})

	// Buffer both
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem,
	})
	_ = pol.IngestArtifactChunk(t.Context(), &types.ArtifactChunk{
		ArtifactID: "a1", Seq: 1, Data: []byte("data"),
	})

	// Make sink fail (affects chunks first in chunks_first semantics)
	sink.ErrorOnWrite = errors.New("chunk write failed")

	err := pol.Flush(t.Context())
	if err == nil {
		t.Fatal("expected flush to fail")
	}

	// Events should NOT have been written (chunks fail first)
	if sink.Stats().EventsWritten != 0 {
		t.Errorf("expected 0 events written when chunks fail, got %d", sink.Stats().EventsWritten)
	}

	// Both buffers preserved
	stats := pol.Stats()
	if stats.BufferSize == 0 {
		t.Error("buffers should be preserved on failure")
	}

	// Fix and retry
	sink.ErrorOnWrite = nil
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("retry failed: %v", err)
	}

	if sink.Stats().ChunksWritten != 1 {
		t.Errorf("expected 1 chunk written, got %d", sink.Stats().ChunksWritten)
	}
	if sink.Stats().EventsWritten != 1 {
		t.Errorf("expected 1 event written, got %d", sink.Stats().EventsWritten)
	}
}

func TestStreamingPolicy_EventWriteFailure_ChunksAlreadySucceeded(t *testing.T) {
	// Use a selective fail sink to fail only on events
	baseSink := policy.NewStubSink()
	failingSink := &streamingSelectiveFailSink{
		StubSink:     baseSink,
		failOnEvents: true,
	}

	pol, err := policy.NewStreamingPolicy(failingSink, policy.StreamingConfig{FlushCount: 100})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(iox.CloseFunc(pol))

	// Buffer both
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem,
	})
	_ = pol.IngestArtifactChunk(t.Context(), &types.ArtifactChunk{
		ArtifactID: "a1", Seq: 1, Data: []byte("data"),
	})

	// Flush: chunks succeed, events fail
	err = pol.Flush(t.Context())
	if err == nil {
		t.Fatal("expected flush to fail on events")
	}

	// Chunks were written
	if baseSink.Stats().ChunksWritten != 1 {
		t.Errorf("expected 1 chunk written, got %d", baseSink.Stats().ChunksWritten)
	}
	// Events not written
	if baseSink.Stats().EventsWritten != 0 {
		t.Errorf("expected 0 events written, got %d", baseSink.Stats().EventsWritten)
	}

	// Fix events and retry
	failingSink.failOnEvents = false
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("retry should succeed: %v", err)
	}

	// Events now written, chunks may duplicate (chunks_first semantics)
	if baseSink.Stats().EventsWritten != 1 {
		t.Errorf("expected 1 event written, got %d", baseSink.Stats().EventsWritten)
	}
}

func TestStreamingPolicy_EmptyFlush_NoWriteCalls(t *testing.T) {
	sink := policy.NewStubSink()
	pol := mustNewStreamingPolicy(t, sink, policy.StreamingConfig{FlushCount: 10})

	// Flush with no data
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// No writes should occur
	if sink.Stats().EventBatches != 0 {
		t.Errorf("expected 0 event batches, got %d", sink.Stats().EventBatches)
	}
	if sink.Stats().ChunkBatches != 0 {
		t.Errorf("expected 0 chunk batches, got %d", sink.Stats().ChunkBatches)
	}
}

func TestStreamingPolicy_BufferSize_TracksCorrectly(t *testing.T) {
	sink := policy.NewStubSink()
	pol := mustNewStreamingPolicy(t, sink, policy.StreamingConfig{FlushCount: 100})

	// Empty buffer
	if pol.Stats().BufferSize != 0 {
		t.Errorf("expected BufferSize=0 initially, got %d", pol.Stats().BufferSize)
	}

	// Add event
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem,
	})
	sizeAfterEvent := pol.Stats().BufferSize
	if sizeAfterEvent == 0 {
		t.Error("BufferSize should be >0 after ingesting event")
	}

	// Add chunk with known data
	_ = pol.IngestArtifactChunk(t.Context(), &types.ArtifactChunk{
		ArtifactID: "a1", Seq: 1, Data: make([]byte, 100),
	})
	sizeAfterChunk := pol.Stats().BufferSize
	if sizeAfterChunk != sizeAfterEvent+100 {
		t.Errorf("expected BufferSize=%d, got %d", sizeAfterEvent+100, sizeAfterChunk)
	}

	// Flush should reset buffer size
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	if pol.Stats().BufferSize != 0 {
		t.Errorf("expected BufferSize=0 after flush, got %d", pol.Stats().BufferSize)
	}
}

func TestStreamingPolicy_Stats_CountersAccurate(t *testing.T) {
	sink := policy.NewStubSink()
	pol := mustNewStreamingPolicy(t, sink, policy.StreamingConfig{FlushCount: 100})

	// Ingest 3 events and 2 chunks
	for i := 1; i <= 3; i++ {
		_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
			EventID: "e" + string(rune('0'+i)),
			Type:    types.EventTypeItem,
			Seq:     int64(i),
		})
	}
	for i := 1; i <= 2; i++ {
		_ = pol.IngestArtifactChunk(t.Context(), &types.ArtifactChunk{
			ArtifactID: "a1", Seq: int64(i), Data: []byte("data"),
		})
	}

	// Before flush
	stats := pol.Stats()
	if stats.TotalEvents != 3 {
		t.Errorf("expected TotalEvents=3, got %d", stats.TotalEvents)
	}
	if stats.TotalChunks != 2 {
		t.Errorf("expected TotalChunks=2, got %d", stats.TotalChunks)
	}
	if stats.EventsPersisted != 0 {
		t.Errorf("expected EventsPersisted=0 before flush, got %d", stats.EventsPersisted)
	}

	// Flush
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	stats = pol.Stats()
	if stats.EventsPersisted != 3 {
		t.Errorf("expected EventsPersisted=3, got %d", stats.EventsPersisted)
	}
	if stats.ChunksPersisted != 2 {
		t.Errorf("expected ChunksPersisted=2, got %d", stats.ChunksPersisted)
	}
	if stats.FlushCount != 1 {
		t.Errorf("expected FlushCount=1, got %d", stats.FlushCount)
	}
	if stats.EventsDropped != 0 {
		t.Errorf("expected EventsDropped=0, got %d", stats.EventsDropped)
	}
}

func TestStreamingPolicy_FlushTriggerStats(t *testing.T) {
	sink := policy.NewStubSink()
	pol := mustNewStreamingPolicy(t, sink, policy.StreamingConfig{FlushCount: 2})

	// Count trigger (ingest 2 events to reach threshold)
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem, Seq: 1,
	})
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e2", Type: types.EventTypeItem, Seq: 2,
	})

	// Termination trigger
	_ = pol.Flush(t.Context())

	triggerStats := pol.FlushTriggerStats()
	if triggerStats[policy.FlushTriggerCount] != 1 {
		t.Errorf("expected 1 count trigger, got %d", triggerStats[policy.FlushTriggerCount])
	}
	if triggerStats[policy.FlushTriggerTermination] != 1 {
		t.Errorf("expected 1 termination trigger, got %d", triggerStats[policy.FlushTriggerTermination])
	}
}

func TestStreamingPolicy_IntervalTrigger(t *testing.T) {
	sink := policy.NewStubSink()
	pol := mustNewStreamingPolicy(t, sink, policy.StreamingConfig{
		FlushInterval: 50 * time.Millisecond,
	})

	// Ingest an event
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem, Seq: 1,
	})

	// Wait for interval to fire
	time.Sleep(150 * time.Millisecond)

	// Event should have been flushed by interval
	if sink.Stats().EventsWritten != 1 {
		t.Errorf("expected 1 event written by interval flush, got %d", sink.Stats().EventsWritten)
	}

	triggerStats := pol.FlushTriggerStats()
	if triggerStats[policy.FlushTriggerInterval] < 1 {
		t.Errorf("expected at least 1 interval trigger, got %d", triggerStats[policy.FlushTriggerInterval])
	}
}

func TestStreamingPolicy_IntervalSkipsEmptyBuffer(t *testing.T) {
	sink := policy.NewStubSink()
	_ = mustNewStreamingPolicy(t, sink, policy.StreamingConfig{
		FlushInterval: 50 * time.Millisecond,
	})

	// Don't ingest anything; wait for interval to pass
	time.Sleep(150 * time.Millisecond)

	// No writes should occur (interval skips when buffer empty)
	if sink.Stats().EventBatches != 0 {
		t.Errorf("expected 0 event batches on empty buffer, got %d", sink.Stats().EventBatches)
	}
}

func TestStreamingPolicy_Close_FlushesAndStops(t *testing.T) {
	sink := policy.NewStubSink()
	pol, err := policy.NewStreamingPolicy(sink, policy.StreamingConfig{
		FlushCount:    100,
		FlushInterval: time.Hour, // Won't fire during test
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Buffer an event
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem,
	})

	// Close should flush and close sink
	if err := pol.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	if sink.Stats().EventsWritten != 1 {
		t.Errorf("expected 1 event written on close, got %d", sink.Stats().EventsWritten)
	}
	if !sink.Stats().Closed {
		t.Error("sink should be closed after policy Close()")
	}
}

func TestStreamingPolicy_Close_Idempotent(t *testing.T) {
	sink := policy.NewStubSink()
	pol, err := policy.NewStreamingPolicy(sink, policy.StreamingConfig{FlushCount: 10})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Close twice should not panic
	if err := pol.Close(); err != nil {
		t.Fatalf("first close failed: %v", err)
	}
	if err := pol.Close(); err != nil {
		t.Fatalf("second close failed: %v", err)
	}
}

func TestStreamingPolicy_CountTrigger_MultipleCycles(t *testing.T) {
	sink := policy.NewStubSink()
	pol := mustNewStreamingPolicy(t, sink, policy.StreamingConfig{FlushCount: 2})

	// First cycle
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{EventID: "e1", Type: types.EventTypeItem, Seq: 1})
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{EventID: "e2", Type: types.EventTypeItem, Seq: 2})

	if sink.Stats().EventsWritten != 2 {
		t.Errorf("first cycle: expected 2 events, got %d", sink.Stats().EventsWritten)
	}

	// Second cycle
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{EventID: "e3", Type: types.EventTypeItem, Seq: 3})
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{EventID: "e4", Type: types.EventTypeItem, Seq: 4})

	if sink.Stats().EventsWritten != 4 {
		t.Errorf("second cycle: expected 4 events total, got %d", sink.Stats().EventsWritten)
	}
	if sink.Stats().EventBatches != 2 {
		t.Errorf("expected 2 batches, got %d", sink.Stats().EventBatches)
	}
}

func TestStreamingPolicy_MixedEventsAndChunks_CountTrigger(t *testing.T) {
	sink := policy.NewStubSink()
	pol := mustNewStreamingPolicy(t, sink, policy.StreamingConfig{FlushCount: 2})

	// Chunk + event — chunk count doesn't trigger flush
	_ = pol.IngestArtifactChunk(t.Context(), &types.ArtifactChunk{
		ArtifactID: "a1", Seq: 1, Data: []byte("data"),
	})
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem, Seq: 1,
	})

	// 1 event in buffer — not at threshold
	if sink.Stats().EventsWritten != 0 {
		t.Errorf("expected 0 events written with 1 event, got %d", sink.Stats().EventsWritten)
	}

	// 2nd event triggers flush — both event and chunk should be written
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e2", Type: types.EventTypeItem, Seq: 2,
	})

	if sink.Stats().EventsWritten != 2 {
		t.Errorf("expected 2 events written at threshold, got %d", sink.Stats().EventsWritten)
	}
	if sink.Stats().ChunksWritten != 1 {
		t.Errorf("expected 1 chunk written with flush, got %d", sink.Stats().ChunksWritten)
	}

	// Verify chunks-first ordering
	if len(sink.WriteOrder) < 2 {
		t.Fatalf("expected at least 2 write ops, got %d", len(sink.WriteOrder))
	}
	if sink.WriteOrder[0].Type != "chunks" {
		t.Errorf("expected chunks first, got %s", sink.WriteOrder[0].Type)
	}
	if sink.WriteOrder[1].Type != "events" {
		t.Errorf("expected events second, got %s", sink.WriteOrder[1].Type)
	}
}

func TestStreamingPolicy_FlushFailure_NewEventsPreservedWithOld(t *testing.T) {
	sink := policy.NewStubSink()
	pol := mustNewStreamingPolicy(t, sink, policy.StreamingConfig{FlushCount: 100})

	// Buffer initial events
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem, Seq: 1,
	})

	// Fail flush
	sink.ErrorOnWrite = errors.New("write failed")
	_ = pol.Flush(t.Context())

	// Add new event while old data is restored
	sink.ErrorOnWrite = nil
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e2", Type: types.EventTypeItem, Seq: 2,
	})

	// Retry flush — should write both old and new events in order
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("retry failed: %v", err)
	}

	if sink.Stats().EventsWritten != 2 {
		t.Errorf("expected 2 events written, got %d", sink.Stats().EventsWritten)
	}

	// Verify ordering: e1 before e2
	if len(sink.WrittenEvents) != 2 {
		t.Fatalf("expected 2 written events, got %d", len(sink.WrittenEvents))
	}
	if sink.WrittenEvents[0].Seq != 1 || sink.WrittenEvents[1].Seq != 2 {
		t.Errorf("expected seq order [1,2], got [%d,%d]",
			sink.WrittenEvents[0].Seq, sink.WrittenEvents[1].Seq)
	}
}

// streamingSelectiveFailSink allows controlling which operations fail
type streamingSelectiveFailSink struct {
	*policy.StubSink
	failOnEvents bool
	failOnChunks bool
}

func (s *streamingSelectiveFailSink) WriteEvents(ctx context.Context, events []*types.EventEnvelope) error {
	if s.failOnEvents {
		return errors.New("event write failed")
	}
	return s.StubSink.WriteEvents(ctx, events)
}

func (s *streamingSelectiveFailSink) WriteChunks(ctx context.Context, chunks []*types.ArtifactChunk) error {
	if s.failOnChunks {
		return errors.New("chunk write failed")
	}
	return s.StubSink.WriteChunks(ctx, chunks)
}

func TestStreamingPolicy_Backpressure_BlocksWhenFull(t *testing.T) {
	// Use a slow sink so flush takes time, allowing buffer to fill
	baseSink := policy.NewStubSink()
	slowSink := &streamingSlowSink{
		StubSink:   baseSink,
		writeDelay: 50 * time.Millisecond,
	}

	pol, err := policy.NewStreamingPolicy(slowSink, policy.StreamingConfig{
		FlushCount: 10,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(iox.CloseFunc(pol))

	// Fill buffer rapidly — we use a goroutine pool and check that
	// the stats show bounded behavior (no unbounded growth)
	ctx := t.Context()
	done := make(chan struct{})
	const totalEvents = 200

	go func() {
		defer close(done)
		for i := range totalEvents {
			if err := pol.IngestEvent(ctx, &types.EventEnvelope{
				EventID: "e",
				Type:    types.EventTypeItem,
				Seq:     int64(i + 1),
			}); err != nil {
				return
			}
		}
	}()

	select {
	case <-done:
		// All events eventually ingested (flushes drained the buffer)
	case <-time.After(10 * time.Second):
		t.Fatal("ingestion timed out — likely deadlocked on backpressure")
	}

	// Final flush to drain remaining
	if err := pol.Flush(ctx); err != nil {
		t.Fatalf("final flush failed: %v", err)
	}

	// All events should be persisted (no drops)
	stats := pol.Stats()
	if stats.TotalEvents != totalEvents {
		t.Errorf("expected TotalEvents=%d, got %d", totalEvents, stats.TotalEvents)
	}
	if stats.EventsDropped != 0 {
		t.Errorf("expected EventsDropped=0, got %d", stats.EventsDropped)
	}
}

func TestStreamingPolicy_Stats_FlushTriggers_InStats(t *testing.T) {
	sink := policy.NewStubSink()
	pol := mustNewStreamingPolicy(t, sink, policy.StreamingConfig{FlushCount: 2})

	// Count trigger
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem, Seq: 1,
	})
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e2", Type: types.EventTypeItem, Seq: 2,
	})

	// Termination trigger
	_ = pol.Flush(t.Context())

	stats := pol.Stats()
	if stats.FlushTriggers == nil {
		t.Fatal("expected FlushTriggers to be populated in Stats")
	}
	if stats.FlushTriggers["count"] != 1 {
		t.Errorf("expected FlushTriggers[count]=1, got %d", stats.FlushTriggers["count"])
	}
	if stats.FlushTriggers["termination"] != 1 {
		t.Errorf("expected FlushTriggers[termination]=1, got %d", stats.FlushTriggers["termination"])
	}
}

func TestStreamingPolicy_Stats_FlushTriggers_NilForOtherPolicies(t *testing.T) {
	sink := policy.NewStubSink()
	pol := policy.NewStrictPolicy(sink)

	stats := pol.Stats()
	if stats.FlushTriggers != nil {
		t.Errorf("expected FlushTriggers=nil for strict policy, got %v", stats.FlushTriggers)
	}
}

func TestStreamingPolicy_ChunkOnlyCountMode_NoDeadlock(t *testing.T) {
	// Regression test: in count-only mode (no FlushInterval), chunks alone
	// would fill the buffer and block forever since only events contribute
	// to the count threshold. After the fix, IngestArtifactChunk triggers
	// a capacity-based flush when the buffer is full.
	baseSink := policy.NewStubSink()
	slowSink := &streamingSlowSink{
		StubSink:   baseSink,
		writeDelay: 10 * time.Millisecond,
	}

	pol, err := policy.NewStreamingPolicy(slowSink, policy.StreamingConfig{
		FlushCount: 1000, // high threshold — chunks won't trigger count-based flush
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(iox.CloseFunc(pol))

	ctx := t.Context()
	done := make(chan struct{})
	const totalChunks = 100

	go func() {
		defer close(done)
		for i := range totalChunks {
			chunk := &types.ArtifactChunk{
				ArtifactID: "a1",
				Seq:        int64(i),
				Data:       make([]byte, 2*1024*1024), // 2 MiB each → fills 128 MiB buffer quickly
			}
			if err := pol.IngestArtifactChunk(ctx, chunk); err != nil {
				return
			}
		}
	}()

	select {
	case <-done:
		// All chunks eventually ingested (capacity flush drained the buffer)
	case <-time.After(10 * time.Second):
		t.Fatal("chunk ingestion timed out — likely deadlocked (count-only mode with chunks)")
	}

	if err := pol.Flush(ctx); err != nil {
		t.Fatalf("final flush failed: %v", err)
	}

	stats := pol.Stats()
	if stats.TotalChunks != totalChunks {
		t.Errorf("expected TotalChunks=%d, got %d", totalChunks, stats.TotalChunks)
	}
	if stats.ChunksPersisted != totalChunks {
		t.Errorf("expected ChunksPersisted=%d, got %d", totalChunks, stats.ChunksPersisted)
	}
	// Capacity flushes should be reported as "capacity", not "count"
	if stats.FlushTriggers["capacity"] == 0 {
		t.Error("expected FlushTriggers[capacity] > 0 for chunk-capacity flush")
	}
	if stats.FlushTriggers["count"] != 0 {
		t.Errorf("expected FlushTriggers[count]=0 (no event count threshold), got %d", stats.FlushTriggers["count"])
	}
}

func TestStreamingPolicy_EventOnlyCapacity_NoDeadlock(t *testing.T) {
	// Regression test: when flush_count is set above streamingMaxBufferEvents
	// and no interval is configured, events fill the buffer and block forever.
	// After the fix, IngestEvent triggers a capacity flush when the buffer is full.
	baseSink := policy.NewStubSink()
	slowSink := &streamingSlowSink{
		StubSink:   baseSink,
		writeDelay: 10 * time.Millisecond,
	}

	pol, err := policy.NewStreamingPolicy(slowSink, policy.StreamingConfig{
		FlushCount: 100_000, // above internal cap of 50,000
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(iox.CloseFunc(pol))

	ctx := t.Context()
	done := make(chan struct{})
	const totalEvents = 60_000 // exceeds 50k internal cap

	go func() {
		defer close(done)
		for i := range totalEvents {
			if err := pol.IngestEvent(ctx, &types.EventEnvelope{
				EventID: "e",
				Type:    types.EventTypeItem,
				Seq:     int64(i + 1),
			}); err != nil {
				return
			}
		}
	}()

	select {
	case <-done:
		// All events eventually ingested (capacity flush drained the buffer)
	case <-time.After(10 * time.Second):
		t.Fatal("event ingestion timed out — likely deadlocked (flush_count > internal cap)")
	}

	if err := pol.Flush(ctx); err != nil {
		t.Fatalf("final flush failed: %v", err)
	}

	stats := pol.Stats()
	if stats.TotalEvents != totalEvents {
		t.Errorf("expected TotalEvents=%d, got %d", totalEvents, stats.TotalEvents)
	}
	if stats.EventsDropped != 0 {
		t.Errorf("expected EventsDropped=0, got %d", stats.EventsDropped)
	}
	// Should have triggered capacity flushes, not count flushes
	if stats.FlushTriggers["capacity"] == 0 {
		t.Error("expected FlushTriggers[capacity] > 0 for event-capacity flush")
	}
}

func TestStreamingPolicy_OversizedEntry_AcceptedWhenBufferEmpty(t *testing.T) {
	// A single entry larger than streamingMaxBufferBytes should be accepted
	// when the buffer is empty (not deadlock), and flushed normally.
	sink := policy.NewStubSink()
	pol, err := policy.NewStreamingPolicy(sink, policy.StreamingConfig{
		FlushCount: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Cleanup(iox.CloseFunc(pol))

	ctx := t.Context()

	// 200 MiB chunk — larger than the 128 MiB internal cap
	bigChunk := &types.ArtifactChunk{
		ArtifactID: "a-big",
		Seq:        0,
		Data:       make([]byte, 200*1024*1024),
	}

	done := make(chan error, 1)
	go func() {
		done <- pol.IngestArtifactChunk(ctx, bigChunk)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("IngestArtifactChunk failed for oversized chunk: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("oversized chunk ingestion timed out — likely deadlocked")
	}

	// Flush should write the oversized chunk
	if err := pol.Flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	stats := pol.Stats()
	if stats.TotalChunks != 1 {
		t.Errorf("expected TotalChunks=1, got %d", stats.TotalChunks)
	}
	if stats.ChunksPersisted != 1 {
		t.Errorf("expected ChunksPersisted=1, got %d", stats.ChunksPersisted)
	}
}

// streamingSlowSink adds a delay to writes for backpressure testing.
type streamingSlowSink struct {
	*policy.StubSink
	writeDelay time.Duration
}

func (s *streamingSlowSink) WriteEvents(ctx context.Context, events []*types.EventEnvelope) error {
	time.Sleep(s.writeDelay)
	return s.StubSink.WriteEvents(ctx, events)
}

func (s *streamingSlowSink) WriteChunks(ctx context.Context, chunks []*types.ArtifactChunk) error {
	time.Sleep(s.writeDelay)
	return s.StubSink.WriteChunks(ctx, chunks)
}
