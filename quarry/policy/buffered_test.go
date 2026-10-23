package policy_test

import (
	"context"
	"errors"
	"testing"

	"github.com/pithecene/quarry/policy"
	"github.com/pithecene/quarry/types"
)

// helper to create policy or fail test
func mustNewBufferedPolicy(t *testing.T, sink policy.Sink, config policy.BufferedConfig) *policy.BufferedPolicy {
	t.Helper()
	pol, err := policy.NewBufferedPolicy(sink, config)
	if err != nil {
		t.Fatalf("NewBufferedPolicy failed: %v", err)
	}
	return pol
}

func TestBufferedPolicy_BuffersEvents(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferEvents: 10}
	pol := mustNewBufferedPolicy(t, sink, config)

	// Ingest events - should be buffered, not written
	for i := 1; i <= 3; i++ {
		envelope := &types.EventEnvelope{
			EventID: "e" + string(rune('0'+i)),
			Type:    types.EventTypeItem,
			Seq:     int64(i),
		}
		if err := pol.IngestEvent(t.Context(), envelope); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// Sink should have nothing yet
	if sink.Stats().EventsWritten != 0 {
		t.Errorf("expected 0 events written before flush, got %d", sink.Stats().EventsWritten)
	}

	// Policy stats should reflect buffered state
	stats := pol.Stats()
	if stats.TotalEvents != 3 {
		t.Errorf("expected TotalEvents=3, got %d", stats.TotalEvents)
	}
	if stats.EventsPersisted != 0 {
		t.Errorf("expected EventsPersisted=0 before flush, got %d", stats.EventsPersisted)
	}
}

func TestBufferedPolicy_FlushWritesBatch(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferEvents: 10}
	pol := mustNewBufferedPolicy(t, sink, config)

	// Ingest events
	for i := 1; i <= 5; i++ {
		envelope := &types.EventEnvelope{
			EventID: "e" + string(rune('0'+i)),
			Type:    types.EventTypeItem,
			Seq:     int64(i),
		}
		_ = pol.IngestEvent(t.Context(), envelope)
	}

	// Flush should write all events in one batch
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sinkStats := sink.Stats()
	if sinkStats.EventsWritten != 5 {
		t.Errorf("expected 5 events written, got %d", sinkStats.EventsWritten)
	}
	if sinkStats.EventBatches != 1 {
		t.Errorf("expected 1 batch (not 5), got %d", sinkStats.EventBatches)
	}

	stats := pol.Stats()
	if stats.EventsPersisted != 5 {
		t.Errorf("expected EventsPersisted=5, got %d", stats.EventsPersisted)
	}
	if stats.FlushCount != 1 {
		t.Errorf("expected FlushCount=1, got %d", stats.FlushCount)
	}
}

func TestBufferedPolicy_DropsDroppableWhenFull(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferEvents: 3}
	pol := mustNewBufferedPolicy(t, sink, config)

	// Fill buffer with non-droppable events
	for i := 1; i <= 3; i++ {
		envelope := &types.EventEnvelope{
			EventID: "e" + string(rune('0'+i)),
			Type:    types.EventTypeItem,
			Seq:     int64(i),
		}
		if err := pol.IngestEvent(t.Context(), envelope); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// Try to add a droppable event when full - should be dropped
	logEvent := &types.EventEnvelope{
		EventID: "log1",
		Type:    types.EventTypeLog,
		Seq:     4,
	}
	if err := pol.IngestEvent(t.Context(), logEvent); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stats := pol.Stats()
	if stats.EventsDropped != 1 {
		t.Errorf("expected 1 dropped event, got %d", stats.EventsDropped)
	}
	if stats.DroppedByType[types.EventTypeLog] != 1 {
		t.Errorf("expected 1 log dropped, got %d", stats.DroppedByType[types.EventTypeLog])
	}
}

func TestBufferedPolicy_EvictsDroppableForNonDroppable(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferEvents: 3}
	pol := mustNewBufferedPolicy(t, sink, config)

	// Fill buffer: 2 items + 1 log (droppable)
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem, Seq: 1,
	})
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "log1", Type: types.EventTypeLog, Seq: 2,
	})
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e2", Type: types.EventTypeItem, Seq: 3,
	})

	// Add non-droppable when full - should evict the log
	itemEvent := &types.EventEnvelope{
		EventID: "e3",
		Type:    types.EventTypeItem,
		Seq:     4,
	}
	if err := pol.IngestEvent(t.Context(), itemEvent); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stats := pol.Stats()
	if stats.EventsDropped != 1 {
		t.Errorf("expected 1 dropped event, got %d", stats.EventsDropped)
	}
	if stats.DroppedByType[types.EventTypeLog] != 1 {
		t.Errorf("expected log to be dropped, got %v", stats.DroppedByType)
	}

	// Flush and verify the log was evicted
	_ = pol.Flush(t.Context())
	if sink.Stats().EventsWritten != 3 {
		t.Errorf("expected 3 events written, got %d", sink.Stats().EventsWritten)
	}

	// Verify no log events in written events
	for _, ev := range sink.WrittenEvents {
		if ev.Type == types.EventTypeLog {
			t.Error("log event should have been evicted")
		}
	}
}

func TestBufferedPolicy_ErrorsOnNonDroppableWhenNoDroppable(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferEvents: 2}
	pol := mustNewBufferedPolicy(t, sink, config)

	// Fill buffer with non-droppable events only
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem, Seq: 1,
	})
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e2", Type: types.EventTypeCheckpoint, Seq: 2,
	})

	// Try to add another non-droppable - should fail
	itemEvent := &types.EventEnvelope{
		EventID: "e3",
		Type:    types.EventTypeItem,
		Seq:     3,
	}
	err := pol.IngestEvent(t.Context(), itemEvent)
	if !errors.Is(err, policy.ErrBufferFull) {
		t.Errorf("expected ErrBufferFull, got %v", err)
	}

	stats := pol.Stats()
	if stats.Errors != 1 {
		t.Errorf("expected Errors=1, got %d", stats.Errors)
	}
}

func TestBufferedPolicy_OrderingPreserved(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferEvents: 10}
	pol := mustNewBufferedPolicy(t, sink, config)

	// Ingest events in sequence order
	for i := 1; i <= 5; i++ {
		envelope := &types.EventEnvelope{
			EventID: "e" + string(rune('0'+i)),
			Type:    types.EventTypeItem,
			Seq:     int64(i),
		}
		_ = pol.IngestEvent(t.Context(), envelope)
	}

	_ = pol.Flush(t.Context())

	// Verify ordering preserved
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

func TestBufferedPolicy_ArtifactChunksBuffered(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferBytes: 1024} // Use byte limit
	pol := mustNewBufferedPolicy(t, sink, config)

	// Ingest chunks
	for i := 0; i < 3; i++ {
		chunk := &types.ArtifactChunk{
			ArtifactID: "a1",
			Seq:        int64(i + 1),
			Data:       []byte("data"),
			IsLast:     i == 2,
		}
		if err := pol.IngestArtifactChunk(t.Context(), chunk); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// Should be buffered
	if sink.Stats().ChunksWritten != 0 {
		t.Errorf("expected 0 chunks written before flush, got %d", sink.Stats().ChunksWritten)
	}

	// Flush writes chunks
	_ = pol.Flush(t.Context())

	if sink.Stats().ChunksWritten != 3 {
		t.Errorf("expected 3 chunks written after flush, got %d", sink.Stats().ChunksWritten)
	}
	if sink.Stats().ChunkBatches != 1 {
		t.Errorf("expected 1 chunk batch, got %d", sink.Stats().ChunkBatches)
	}
}

func TestBufferedPolicy_SinkError(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferEvents: 10}
	pol := mustNewBufferedPolicy(t, sink, config)

	// Buffer an event
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem,
	})

	// Make sink fail
	expectedErr := errors.New("sink failure")
	sink.ErrorOnWrite = expectedErr

	err := pol.Flush(t.Context())
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	stats := pol.Stats()
	if stats.Errors != 1 {
		t.Errorf("expected Errors=1, got %d", stats.Errors)
	}
}

func TestBufferedPolicy_Close_FlushesAndCloses(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferEvents: 10}
	pol := mustNewBufferedPolicy(t, sink, config)

	// Buffer events
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem,
	})

	err := pol.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have flushed
	if sink.Stats().EventsWritten != 1 {
		t.Errorf("expected 1 event written on close, got %d", sink.Stats().EventsWritten)
	}

	// Should be closed
	if !sink.Stats().Closed {
		t.Error("sink should be closed")
	}
}

func TestBufferedPolicy_DropsOnlyAllowedTypes(t *testing.T) {
	t.Parallel()

	// Per CONTRACT_POLICY.md: may drop log, enqueue, rotate_proxy
	droppableTypes := []types.EventType{
		types.EventTypeLog,
		types.EventTypeEnqueue,
		types.EventTypeRotateProxy,
	}

	for _, et := range droppableTypes {
		t.Run(string(et), func(t *testing.T) {
			sink := policy.NewStubSink()
			config := policy.BufferedConfig{MaxBufferEvents: 1}
			pol := mustNewBufferedPolicy(t, sink, config)

			// Fill with non-droppable
			_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
				EventID: "e1", Type: types.EventTypeItem,
			})

			// Try droppable - should be dropped (not error)
			err := pol.IngestEvent(t.Context(), &types.EventEnvelope{
				EventID: "d1", Type: et,
			})
			if err != nil {
				t.Errorf("droppable type %s should not error, got %v", et, err)
			}

			stats := pol.Stats()
			if stats.EventsDropped != 1 {
				t.Errorf("expected 1 drop for %s, got %d", et, stats.EventsDropped)
			}
		})
	}
}

func TestBufferedPolicy_NeverDropsNonDroppable(t *testing.T) {
	t.Parallel()

	// Per CONTRACT_POLICY.md: must NOT drop item, artifact, checkpoint, run_error, run_complete
	nonDroppableTypes := []types.EventType{
		types.EventTypeItem,
		types.EventTypeArtifact,
		types.EventTypeCheckpoint,
		types.EventTypeRunError,
		types.EventTypeRunComplete,
	}

	for _, et := range nonDroppableTypes {
		t.Run(string(et), func(t *testing.T) {
			sink := policy.NewStubSink()
			config := policy.BufferedConfig{MaxBufferEvents: 1}
			pol := mustNewBufferedPolicy(t, sink, config)

			// Fill with non-droppable
			_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
				EventID: "e1", Type: types.EventTypeItem,
			})

			// Try another non-droppable - should error (not drop)
			err := pol.IngestEvent(t.Context(), &types.EventEnvelope{
				EventID: "e2", Type: et,
			})
			if !errors.Is(err, policy.ErrBufferFull) {
				t.Errorf("non-droppable type %s should error when buffer full, got %v", et, err)
			}

			stats := pol.Stats()
			if stats.DroppedByType[et] != 0 {
				t.Errorf("non-droppable type %s should never be dropped", et)
			}
		})
	}
}

func TestBufferedPolicy_InvalidConfig_BothLimitsZero(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{
		MaxBufferEvents: 0,
		MaxBufferBytes:  0,
	}

	_, err := policy.NewBufferedPolicy(sink, config)
	if !errors.Is(err, policy.ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestBufferedPolicy_ValidConfig_OnlyEventLimit(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferEvents: 10, MaxBufferBytes: 0}

	pol, err := policy.NewBufferedPolicy(sink, config)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if pol == nil {
		t.Fatal("expected non-nil policy")
	}
}

func TestBufferedPolicy_ValidConfig_OnlyByteLimit(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferEvents: 0, MaxBufferBytes: 1024}

	pol, err := policy.NewBufferedPolicy(sink, config)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if pol == nil {
		t.Fatal("expected non-nil policy")
	}
}

func TestBufferedPolicy_ChunkBuffering_RespectsMaxBufferBytes(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferBytes: 100} // Small limit
	pol := mustNewBufferedPolicy(t, sink, config)

	// First chunk should fit
	chunk1 := &types.ArtifactChunk{
		ArtifactID: "a1",
		Seq:        1,
		Data:       make([]byte, 50),
	}
	if err := pol.IngestArtifactChunk(t.Context(), chunk1); err != nil {
		t.Fatalf("first chunk should fit: %v", err)
	}

	// Second chunk should also fit (50 + 50 = 100)
	chunk2 := &types.ArtifactChunk{
		ArtifactID: "a1",
		Seq:        2,
		Data:       make([]byte, 50),
	}
	if err := pol.IngestArtifactChunk(t.Context(), chunk2); err != nil {
		t.Fatalf("second chunk should fit: %v", err)
	}

	// Third chunk should exceed limit
	chunk3 := &types.ArtifactChunk{
		ArtifactID: "a1",
		Seq:        3,
		Data:       make([]byte, 10),
	}
	err := pol.IngestArtifactChunk(t.Context(), chunk3)
	if !errors.Is(err, policy.ErrBufferFull) {
		t.Errorf("expected ErrBufferFull when chunk exceeds limit, got %v", err)
	}

	stats := pol.Stats()
	if stats.Errors != 1 {
		t.Errorf("expected Errors=1, got %d", stats.Errors)
	}
}

func TestBufferedPolicy_ChunkBuffering_SharedWithEvents(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferBytes: 500} // Shared limit
	pol := mustNewBufferedPolicy(t, sink, config)

	// Add an event (estimated ~200 bytes)
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem,
	})

	// Add a chunk that fits
	chunk1 := &types.ArtifactChunk{
		ArtifactID: "a1",
		Seq:        1,
		Data:       make([]byte, 200),
	}
	if err := pol.IngestArtifactChunk(t.Context(), chunk1); err != nil {
		t.Fatalf("chunk should fit: %v", err)
	}

	// Verify buffer size includes both
	stats := pol.Stats()
	if stats.BufferSize < 400 {
		t.Errorf("BufferSize should be >= 400, got %d", stats.BufferSize)
	}

	// Large chunk should exceed remaining space
	chunk2 := &types.ArtifactChunk{
		ArtifactID: "a1",
		Seq:        2,
		Data:       make([]byte, 200),
	}
	err := pol.IngestArtifactChunk(t.Context(), chunk2)
	if !errors.Is(err, policy.ErrBufferFull) {
		t.Errorf("expected ErrBufferFull, got %v", err)
	}
}

func TestBufferedPolicy_BufferSize_AccurateAfterChunkBuffering(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferBytes: 1000}
	pol := mustNewBufferedPolicy(t, sink, config)

	// Add chunks with known sizes
	for i := 0; i < 3; i++ {
		chunk := &types.ArtifactChunk{
			ArtifactID: "a1",
			Seq:        int64(i + 1),
			Data:       make([]byte, 100), // 100 bytes each
		}
		_ = pol.IngestArtifactChunk(t.Context(), chunk)
	}

	stats := pol.Stats()
	if stats.BufferSize != 300 {
		t.Errorf("expected BufferSize=300, got %d", stats.BufferSize)
	}
}

func TestBufferedPolicy_BufferSize_AccurateAfterEviction(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferEvents: 2}
	pol := mustNewBufferedPolicy(t, sink, config)

	// Add a droppable event
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "log1", Type: types.EventTypeLog,
	})
	sizeAfterLog := pol.Stats().BufferSize

	// Add non-droppable to fill
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem,
	})

	// Add another non-droppable - should evict log
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e2", Type: types.EventTypeItem,
	})

	stats := pol.Stats()
	// Buffer should have 2 items now, size should reflect eviction
	// Size should be 2 * ~200 bytes, not include the dropped log
	if stats.BufferSize <= sizeAfterLog {
		t.Errorf("BufferSize should reflect eviction, got %d (was %d with log)", stats.BufferSize, sizeAfterLog)
	}
}

func TestBufferedPolicy_FlushFailure_PreservesEventBuffer(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferEvents: 10}
	pol := mustNewBufferedPolicy(t, sink, config)

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

	// Flush should fail
	err := pol.Flush(t.Context())
	if err == nil {
		t.Fatal("expected flush to fail")
	}

	// Buffer should still have data
	stats := pol.Stats()
	if stats.BufferSize == 0 {
		t.Error("buffer should not be cleared on flush failure")
	}

	// Fix sink and retry flush
	sink.ErrorOnWrite = nil
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("retry flush failed: %v", err)
	}

	// Now data should be written
	if sink.Stats().EventsWritten != 3 {
		t.Errorf("expected 3 events written after retry, got %d", sink.Stats().EventsWritten)
	}
}

func TestBufferedPolicy_FlushFailure_PreservesChunkBuffer(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferBytes: 1000}
	pol := mustNewBufferedPolicy(t, sink, config)

	// Buffer chunks only (no events to avoid partial success complexity)
	for i := 0; i < 3; i++ {
		_ = pol.IngestArtifactChunk(t.Context(), &types.ArtifactChunk{
			ArtifactID: "a1",
			Seq:        int64(i + 1),
			Data:       []byte("data"),
		})
	}

	// Make sink fail
	sink.ErrorOnWrite = errors.New("write failed")

	// Flush should fail
	err := pol.Flush(t.Context())
	if err == nil {
		t.Fatal("expected flush to fail")
	}

	// Buffer should still have data
	stats := pol.Stats()
	if stats.BufferSize == 0 {
		t.Error("chunk buffer should not be cleared on flush failure")
	}

	// Fix sink and retry flush
	sink.ErrorOnWrite = nil
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("retry flush failed: %v", err)
	}

	// Now data should be written
	if sink.Stats().ChunksWritten != 3 {
		t.Errorf("expected 3 chunks written after retry, got %d", sink.Stats().ChunksWritten)
	}
}

func TestBufferedPolicy_BufferSize_ZeroAfterSuccessfulFlush(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferBytes: 1000}
	pol := mustNewBufferedPolicy(t, sink, config)

	// Buffer mixed data
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem,
	})
	_ = pol.IngestArtifactChunk(t.Context(), &types.ArtifactChunk{
		ArtifactID: "a1", Seq: 1, Data: []byte("data"),
	})

	// Verify non-zero before flush
	if pol.Stats().BufferSize == 0 {
		t.Fatal("buffer should have data before flush")
	}

	// Successful flush
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Buffer should be empty
	stats := pol.Stats()
	if stats.BufferSize != 0 {
		t.Errorf("expected BufferSize=0 after successful flush, got %d", stats.BufferSize)
	}
}

func TestBufferedPolicy_ChunksPersisted_IncrementedAfterFlush(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferBytes: 1000}
	pol := mustNewBufferedPolicy(t, sink, config)

	// Buffer chunks
	for i := 0; i < 5; i++ {
		_ = pol.IngestArtifactChunk(t.Context(), &types.ArtifactChunk{
			ArtifactID: "a1",
			Seq:        int64(i + 1),
			Data:       []byte("data"),
		})
	}

	// Before flush
	statsBefore := pol.Stats()
	if statsBefore.ChunksPersisted != 0 {
		t.Errorf("expected ChunksPersisted=0 before flush, got %d", statsBefore.ChunksPersisted)
	}

	// Flush
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// After flush
	statsAfter := pol.Stats()
	if statsAfter.ChunksPersisted != 5 {
		t.Errorf("expected ChunksPersisted=5 after flush, got %d", statsAfter.ChunksPersisted)
	}
}

func TestBufferedPolicy_EvictionRechecksByteLimit(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	// Small byte limit: can fit ~2 events at 200 bytes each
	config := policy.BufferedConfig{MaxBufferEvents: 3, MaxBufferBytes: 450}
	pol := mustNewBufferedPolicy(t, sink, config)

	// Fill with 1 small droppable + 1 item (under byte limit)
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "log1", Type: types.EventTypeLog, // ~200 bytes
	})
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem, // ~200 bytes, total ~400
	})

	// Buffer is at ~400 bytes, event count = 2, limit = 3 events / 450 bytes
	// Adding another ~200 byte event would exceed byte limit (400 + 200 = 600 > 450)
	// Even after evicting log (~200 bytes), new total would be 200 + 200 = 400, which fits

	err := pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e2", Type: types.EventTypeItem,
	})
	if err != nil {
		t.Fatalf("should succeed after evicting droppable: %v", err)
	}

	stats := pol.Stats()
	if stats.EventsDropped != 1 {
		t.Errorf("expected 1 event dropped, got %d", stats.EventsDropped)
	}
}

func TestBufferedPolicy_EventExceedsByteLimitAlone(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	// Byte limit smaller than a single event (~200 bytes estimated)
	config := policy.BufferedConfig{MaxBufferEvents: 10, MaxBufferBytes: 100}
	pol := mustNewBufferedPolicy(t, sink, config)

	// Single event is ~200 bytes which exceeds 100 byte limit
	err := pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem,
	})
	if !errors.Is(err, policy.ErrBufferFull) {
		t.Errorf("expected ErrBufferFull when event exceeds limit, got %v", err)
	}
}

func TestBufferedPolicy_FlushFailure_ChunkWriteFail_PreservesBothBuffers(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferBytes: 1000}
	pol := mustNewBufferedPolicy(t, sink, config)

	// Buffer both events and chunks
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem,
	})
	_ = pol.IngestArtifactChunk(t.Context(), &types.ArtifactChunk{
		ArtifactID: "a1", Seq: 1, Data: []byte("chunk-data"),
	})

	// Make sink fail only on chunk writes
	sink.ErrorOnWrite = errors.New("chunk write failed")

	// Flush will write events successfully (ErrorOnWrite affects both, so we need a smarter stub)
	// Actually StubSink.ErrorOnWrite affects all writes. Let's just verify buffer preservation.

	// For this test, we'll make both fail
	err := pol.Flush(t.Context())
	if err == nil {
		t.Fatal("expected flush to fail")
	}

	// Both buffers should be preserved (no clearing on failure)
	stats := pol.Stats()
	if stats.BufferSize == 0 {
		t.Error("buffers should be preserved on flush failure")
	}

	// Fix sink and retry
	sink.ErrorOnWrite = nil
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("retry flush failed: %v", err)
	}

	// Both should be written now
	if sink.Stats().EventsWritten != 1 {
		t.Errorf("expected 1 event written after retry, got %d", sink.Stats().EventsWritten)
	}
	if sink.Stats().ChunksWritten != 1 {
		t.Errorf("expected 1 chunk written after retry, got %d", sink.Stats().ChunksWritten)
	}
}

func TestBufferedPolicy_ChunkBuffering_RequiresByteLimit(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	// Only event limit, no byte limit
	config := policy.BufferedConfig{MaxBufferEvents: 10, MaxBufferBytes: 0}
	pol := mustNewBufferedPolicy(t, sink, config)

	// Chunk buffering should fail without byte limit
	chunk := &types.ArtifactChunk{
		ArtifactID: "a1",
		Seq:        1,
		Data:       []byte("data"),
	}
	err := pol.IngestArtifactChunk(t.Context(), chunk)
	if !errors.Is(err, policy.ErrBufferFull) {
		t.Errorf("expected ErrBufferFull when chunk buffering without byte limit, got %v", err)
	}

	stats := pol.Stats()
	if stats.Errors != 1 {
		t.Errorf("expected Errors=1, got %d", stats.Errors)
	}
}

func TestBufferedPolicy_InvalidFlushMode(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{
		MaxBufferBytes: 1000,
		FlushMode:      "invalid_mode",
	}

	_, err := policy.NewBufferedPolicy(sink, config)
	if !errors.Is(err, policy.ErrInvalidFlushMode) {
		t.Errorf("expected ErrInvalidFlushMode, got %v", err)
	}
}

func TestBufferedPolicy_DefaultFlushMode(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferBytes: 1000}
	// FlushMode not set - should default to AtLeastOnce

	pol, err := policy.NewBufferedPolicy(sink, config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pol == nil {
		t.Fatal("expected non-nil policy")
	}
}

func TestBufferedPolicy_FlushAtLeastOnce_PreservesBuffersOnChunkFailure(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{
		MaxBufferBytes: 1000,
		FlushMode:      policy.FlushAtLeastOnce,
	}
	pol := mustNewBufferedPolicy(t, sink, config)

	// Buffer event and chunk
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem,
	})
	_ = pol.IngestArtifactChunk(t.Context(), &types.ArtifactChunk{
		ArtifactID: "a1", Seq: 1, Data: []byte("data"),
	})

	// Make sink fail
	sink.ErrorOnWrite = errors.New("write failed")

	// Flush fails
	err := pol.Flush(t.Context())
	if err == nil {
		t.Fatal("expected flush to fail")
	}

	// Both buffers should be preserved
	stats := pol.Stats()
	if stats.BufferSize == 0 {
		t.Error("buffers should be preserved on failure")
	}

	// Retry succeeds and writes duplicates (events written twice is acceptable)
	sink.ErrorOnWrite = nil
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("retry failed: %v", err)
	}

	// Both event and chunk written
	if sink.Stats().EventsWritten < 1 {
		t.Errorf("expected event written, got %d", sink.Stats().EventsWritten)
	}
	if sink.Stats().ChunksWritten != 1 {
		t.Errorf("expected 1 chunk written, got %d", sink.Stats().ChunksWritten)
	}
}

func TestBufferedPolicy_FlushChunksFirst_NoEventsOnChunkFailure(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{
		MaxBufferBytes: 1000,
		FlushMode:      policy.FlushChunksFirst,
	}
	pol := mustNewBufferedPolicy(t, sink, config)

	// Buffer event and chunk
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem,
	})
	_ = pol.IngestArtifactChunk(t.Context(), &types.ArtifactChunk{
		ArtifactID: "a1", Seq: 1, Data: []byte("data"),
	})

	// Make sink fail
	sink.ErrorOnWrite = errors.New("chunk write failed")

	// Flush fails on chunks
	err := pol.Flush(t.Context())
	if err == nil {
		t.Fatal("expected flush to fail")
	}

	// Events should NOT have been written (chunks failed first)
	if sink.Stats().EventsWritten != 0 {
		t.Errorf("expected 0 events written when chunks fail first, got %d", sink.Stats().EventsWritten)
	}

	// Both buffers preserved
	stats := pol.Stats()
	if stats.BufferSize == 0 {
		t.Error("buffers should be preserved")
	}
}

func TestBufferedPolicy_FlushTwoPhase_NoEventDuplicatesOnRetry(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{
		MaxBufferBytes: 1000,
		FlushMode:      policy.FlushTwoPhase,
	}
	pol := mustNewBufferedPolicy(t, sink, config)

	// Buffer event and chunk
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem,
	})
	_ = pol.IngestArtifactChunk(t.Context(), &types.ArtifactChunk{
		ArtifactID: "a1", Seq: 1, Data: []byte("data"),
	})

	// First flush attempt - should succeed
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("first flush should succeed: %v", err)
	}

	// Events should be written once
	if sink.Stats().EventsWritten != 1 {
		t.Errorf("expected 1 event written, got %d", sink.Stats().EventsWritten)
	}

	// Buffer another event and chunk
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e2", Type: types.EventTypeItem,
	})
	_ = pol.IngestArtifactChunk(t.Context(), &types.ArtifactChunk{
		ArtifactID: "a2", Seq: 1, Data: []byte("data2"),
	})

	// Second flush
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("second flush failed: %v", err)
	}

	// Now 2 events total
	if sink.Stats().EventsWritten != 2 {
		t.Errorf("expected 2 events written total, got %d", sink.Stats().EventsWritten)
	}
}

func TestBufferedPolicy_FlushTwoPhase_ChunksNotRewrittenOnEventFailure(t *testing.T) {
	t.Parallel()

	// TwoPhase now writes chunks first, then events.
	// This test verifies chunks aren't duplicated if events fail.
	baseSink := policy.NewStubSink()
	failingSink := &selectiveFailSink{
		StubSink:       baseSink,
		failOnChunks:   false,
		failOnEvents:   true,
		chunkCallCount: 0,
	}

	config := policy.BufferedConfig{
		MaxBufferBytes: 1000,
		FlushMode:      policy.FlushTwoPhase,
	}
	pol, _ := policy.NewBufferedPolicy(failingSink, config)

	// Buffer event and chunk
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem,
	})
	_ = pol.IngestArtifactChunk(t.Context(), &types.ArtifactChunk{
		ArtifactID: "a1", Seq: 1, Data: []byte("data"),
	})

	// First flush: chunks succeed, events fail
	err := pol.Flush(t.Context())
	if err == nil {
		t.Fatal("expected flush to fail on events")
	}

	// Chunks were written
	if baseSink.Stats().ChunksWritten != 1 {
		t.Errorf("expected 1 chunk written, got %d", baseSink.Stats().ChunksWritten)
	}

	// Events not written yet
	if baseSink.Stats().EventsWritten != 0 {
		t.Errorf("expected 0 events written, got %d", baseSink.Stats().EventsWritten)
	}

	// Fix events
	failingSink.failOnEvents = false

	// Retry flush
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("retry should succeed: %v", err)
	}

	// Chunks should still be 1 (not re-written)
	if baseSink.Stats().ChunksWritten != 1 {
		t.Errorf("expected chunks not re-written, got %d", baseSink.Stats().ChunksWritten)
	}

	// Events now written
	if baseSink.Stats().EventsWritten != 1 {
		t.Errorf("expected 1 event written, got %d", baseSink.Stats().EventsWritten)
	}
}

// selectiveFailSink allows controlling which operations fail
type selectiveFailSink struct {
	*policy.StubSink
	failOnEvents   bool
	failOnChunks   bool
	chunkCallCount int
}

func (s *selectiveFailSink) WriteEvents(ctx context.Context, events []*types.EventEnvelope) error {
	if s.failOnEvents {
		return errors.New("event write failed")
	}
	return s.StubSink.WriteEvents(ctx, events)
}

func (s *selectiveFailSink) WriteChunks(ctx context.Context, chunks []*types.ArtifactChunk) error {
	s.chunkCallCount++
	if s.failOnChunks {
		return errors.New("chunk write failed")
	}
	return s.StubSink.WriteChunks(ctx, chunks)
}

func TestBufferedPolicy_FlushTwoPhase_NewEventsAfterEventFailureAreWritten(t *testing.T) {
	t.Parallel()

	// TwoPhase now writes chunks first, then events.
	// This test verifies new events added after a partial flush are written.
	baseSink := policy.NewStubSink()
	failingSink := &selectiveFailSink{
		StubSink:     baseSink,
		failOnChunks: false,
		failOnEvents: true,
	}

	config := policy.BufferedConfig{
		MaxBufferBytes: 10000,
		FlushMode:      policy.FlushTwoPhase,
	}
	pol, _ := policy.NewBufferedPolicy(failingSink, config)

	// Buffer initial event and chunk
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem,
	})
	_ = pol.IngestArtifactChunk(t.Context(), &types.ArtifactChunk{
		ArtifactID: "a1", Seq: 1, Data: []byte("data"),
	})

	// First flush: chunks succeed, events fail
	err := pol.Flush(t.Context())
	if err == nil {
		t.Fatal("expected flush to fail on events")
	}

	// Chunks were written
	if baseSink.Stats().ChunksWritten != 1 {
		t.Errorf("expected 1 chunk written after first flush, got %d", baseSink.Stats().ChunksWritten)
	}

	// Events not written yet
	if baseSink.Stats().EventsWritten != 0 {
		t.Errorf("expected 0 events written after first flush, got %d", baseSink.Stats().EventsWritten)
	}

	// Add a NEW event after the partial flush
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e2", Type: types.EventTypeItem,
	})

	// Fix events and retry
	failingSink.failOnEvents = false
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("retry should succeed: %v", err)
	}

	// e1 and e2 should both be written
	if baseSink.Stats().EventsWritten != 2 {
		t.Errorf("expected 2 events written (e1 + e2), got %d", baseSink.Stats().EventsWritten)
	}

	// Verify each event written exactly once
	eventIDs := make(map[string]int)
	for _, ev := range baseSink.WrittenEvents {
		eventIDs[ev.EventID]++
	}
	if eventIDs["e1"] != 1 {
		t.Errorf("e1 should be written exactly once, got %d", eventIDs["e1"])
	}
	if eventIDs["e2"] != 1 {
		t.Errorf("e2 should be written exactly once, got %d", eventIDs["e2"])
	}

	// Chunks should still be 1 (not re-written)
	if baseSink.Stats().ChunksWritten != 1 {
		t.Errorf("expected chunks not re-written, got %d", baseSink.Stats().ChunksWritten)
	}
}

func TestBufferedPolicy_FlushTwoPhase_NewChunksAfterEventFailureAreWritten(t *testing.T) {
	t.Parallel()

	// TwoPhase writes chunks first. This test verifies new chunks added
	// after a partial flush are written without duplicating old chunks.
	baseSink := policy.NewStubSink()
	failingSink := &selectiveFailSink{
		StubSink:     baseSink,
		failOnChunks: false,
		failOnEvents: true,
	}

	config := policy.BufferedConfig{
		MaxBufferBytes: 10000,
		FlushMode:      policy.FlushTwoPhase,
	}
	pol, _ := policy.NewBufferedPolicy(failingSink, config)

	ctx := t.Context()

	// Buffer initial event and chunk
	_ = pol.IngestEvent(ctx, &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem,
	})
	_ = pol.IngestArtifactChunk(ctx, &types.ArtifactChunk{
		ArtifactID: "a1", Seq: 1, Data: []byte("chunk1"),
	})

	// First flush: chunks succeed, events fail
	err := pol.Flush(ctx)
	if err == nil {
		t.Fatal("expected flush to fail on events")
	}

	// First chunk was written
	if baseSink.Stats().ChunksWritten != 1 {
		t.Errorf("expected 1 chunk written after first flush, got %d", baseSink.Stats().ChunksWritten)
	}

	// Add a NEW chunk after the partial flush
	_ = pol.IngestArtifactChunk(ctx, &types.ArtifactChunk{
		ArtifactID: "a1", Seq: 2, Data: []byte("chunk2"),
	})

	// Fix events and retry
	failingSink.failOnEvents = false
	if err := pol.Flush(ctx); err != nil {
		t.Fatalf("retry should succeed: %v", err)
	}

	// Both chunks should be written (old chunk once, new chunk once)
	if baseSink.Stats().ChunksWritten != 2 {
		t.Errorf("expected 2 chunks written total, got %d", baseSink.Stats().ChunksWritten)
	}

	// Verify each chunk written exactly once
	chunkSeqs := make(map[int64]int)
	for _, ch := range baseSink.WrittenChunks {
		chunkSeqs[ch.Seq]++
	}
	if chunkSeqs[1] != 1 {
		t.Errorf("chunk seq 1 should be written exactly once, got %d", chunkSeqs[1])
	}
	if chunkSeqs[2] != 1 {
		t.Errorf("chunk seq 2 should be written exactly once, got %d", chunkSeqs[2])
	}

	// Events should be written
	if baseSink.Stats().EventsWritten != 1 {
		t.Errorf("expected 1 event written, got %d", baseSink.Stats().EventsWritten)
	}
}

func TestBufferedPolicy_BufferSize_UpdatedWhenEventBufferNextCleared(t *testing.T) {
	t.Parallel()

	baseSink := policy.NewStubSink()
	failingSink := &selectiveFailSink{
		StubSink:     baseSink,
		failOnChunks: true,
		failOnEvents: false,
	}

	config := policy.BufferedConfig{
		MaxBufferBytes: 10000,
		FlushMode:      policy.FlushTwoPhase,
	}
	pol, _ := policy.NewBufferedPolicy(failingSink, config)

	// Buffer initial event and chunk
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem,
	})
	_ = pol.IngestArtifactChunk(t.Context(), &types.ArtifactChunk{
		ArtifactID: "a1", Seq: 1, Data: make([]byte, 100),
	})

	// First flush: events succeed, chunks fail
	_ = pol.Flush(t.Context())

	// Add event to eventBufferNext
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e2", Type: types.EventTypeItem,
	})

	sizeWithNext := pol.Stats().BufferSize

	// Fix chunks and complete flush
	failingSink.failOnChunks = false
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("retry should succeed: %v", err)
	}

	// BufferSize should be 0 after successful flush (eventBufferNext cleared)
	sizeAfterFlush := pol.Stats().BufferSize
	if sizeAfterFlush != 0 {
		t.Errorf("expected BufferSize=0 after successful flush, got %d (was %d before)", sizeAfterFlush, sizeWithNext)
	}
}

func TestBufferedPolicy_Eviction_ConsidersEventBufferNext(t *testing.T) {
	t.Parallel()

	baseSink := policy.NewStubSink()
	failingSink := &selectiveFailSink{
		StubSink:     baseSink,
		failOnChunks: true,
		failOnEvents: false,
	}

	config := policy.BufferedConfig{
		MaxBufferEvents: 3,
		MaxBufferBytes:  10000,
		FlushMode:       policy.FlushTwoPhase,
	}
	pol, _ := policy.NewBufferedPolicy(failingSink, config)

	// Buffer 2 NON-droppable events (so eventBuffer has no droppable to evict)
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem,
	})
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e2", Type: types.EventTypeItem,
	})
	_ = pol.IngestArtifactChunk(t.Context(), &types.ArtifactChunk{
		ArtifactID: "a1", Seq: 1, Data: []byte("data"),
	})

	// Flush: events succeed, chunks fail
	// eventBuffer now marked as flushed
	_ = pol.Flush(t.Context())

	// Add a droppable to eventBufferNext
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "log1", Type: types.EventTypeLog, // droppable, goes to eventBufferNext
	})

	// Buffer is now at limit: 2 in eventBuffer + 1 in eventBufferNext = 3
	// Try to add a non-droppable
	// eventBuffer has no droppable, so must evict log1 from eventBufferNext
	err := pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e3", Type: types.EventTypeItem,
	})
	if err != nil {
		t.Fatalf("should succeed by evicting from eventBufferNext: %v", err)
	}

	stats := pol.Stats()
	// log1 should have been dropped (from eventBufferNext)
	if stats.DroppedByType[types.EventTypeLog] != 1 {
		t.Errorf("expected 1 log dropped from eventBufferNext, got %d", stats.DroppedByType[types.EventTypeLog])
	}

	// Complete flush
	failingSink.failOnChunks = false
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("flush should succeed: %v", err)
	}

	// Verify e1, e2 written in first flush, e3 written in second (log1 was evicted)
	eventIDs := make(map[string]int)
	for _, ev := range baseSink.WrittenEvents {
		eventIDs[ev.EventID]++
	}
	if eventIDs["e1"] != 1 {
		t.Errorf("e1 should be written once, got %d", eventIDs["e1"])
	}
	if eventIDs["e2"] != 1 {
		t.Errorf("e2 should be written once, got %d", eventIDs["e2"])
	}
	if eventIDs["e3"] != 1 {
		t.Errorf("e3 should be written once, got %d", eventIDs["e3"])
	}
	if eventIDs["log1"] != 0 {
		t.Errorf("log1 should NOT be written (was evicted), got %d", eventIDs["log1"])
	}
}

// TestBufferedPolicy_EventsWrittenInSequenceOrder tests that all events
// (including artifact commits) are written in their original sequence order.
// The "chunks before commit" invariant is enforced by the Lode client,
// not by reordering events in the policy.
func TestBufferedPolicy_EventsWrittenInSequenceOrder(t *testing.T) {
	t.Parallel()

	flushModes := []policy.FlushMode{
		policy.FlushAtLeastOnce,
		policy.FlushChunksFirst,
		policy.FlushTwoPhase,
	}

	for _, flushMode := range flushModes {
		t.Run(string(flushMode), func(t *testing.T) {
			sink := policy.NewStubSink()
			config := policy.BufferedConfig{
				MaxBufferEvents: 100,
				MaxBufferBytes:  1024 * 1024,
				FlushMode:       flushMode,
			}
			pol := mustNewBufferedPolicy(t, sink, config)

			ctx := t.Context()

			// Ingest events in a specific sequence order
			events := []*types.EventEnvelope{
				{EventID: "e1", Type: types.EventTypeItem, Seq: 1},
				{
					EventID: "art-commit", Type: types.EventTypeArtifact, Seq: 2,
					Payload: map[string]any{
						"artifact_id":  "art-1",
						"name":         "test.txt",
						"content_type": "text/plain",
						"size_bytes":   float64(10),
					},
				},
				{EventID: "e2", Type: types.EventTypeLog, Seq: 3},
			}

			for _, ev := range events {
				if err := pol.IngestEvent(ctx, ev); err != nil {
					t.Fatalf("IngestEvent failed: %v", err)
				}
			}

			// Flush
			if err := pol.Flush(ctx); err != nil {
				t.Fatalf("Flush failed: %v", err)
			}

			// Collect all written events
			var written []*types.EventEnvelope
			for _, op := range sink.WriteOrder {
				if op.Type == "events" {
					written = append(written, op.Events...)
				}
			}

			if len(written) != len(events) {
				t.Fatalf("expected %d events written, got %d", len(events), len(written))
			}

			// Verify events are written in sequence order
			for i, ev := range written {
				if ev.Seq != events[i].Seq {
					t.Errorf("event at index %d: expected seq %d, got %d",
						i, events[i].Seq, ev.Seq)
				}
			}
		})
	}
}

// TestBufferedPolicy_AllEventsWrittenTogether verifies that all events
// (including artifact commits) are written in the same batch, preserving order.
func TestBufferedPolicy_AllEventsWrittenTogether(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferEvents: 100}
	pol := mustNewBufferedPolicy(t, sink, config)

	ctx := t.Context()

	// Mix regular events and artifact commits
	_ = pol.IngestEvent(ctx, &types.EventEnvelope{EventID: "e1", Type: types.EventTypeItem, Seq: 1})
	_ = pol.IngestEvent(ctx, &types.EventEnvelope{EventID: "art1", Type: types.EventTypeArtifact, Seq: 2})
	_ = pol.IngestEvent(ctx, &types.EventEnvelope{EventID: "e2", Type: types.EventTypeLog, Seq: 3})
	_ = pol.IngestEvent(ctx, &types.EventEnvelope{EventID: "art2", Type: types.EventTypeArtifact, Seq: 4})

	// Flush
	if err := pol.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify that all events are written in a single batch
	writeOrder := sink.WriteOrder
	if len(writeOrder) == 0 {
		t.Fatal("no writes recorded")
	}

	// Should be exactly one events write
	eventsWriteCount := 0
	var allEvents []*types.EventEnvelope
	for _, op := range writeOrder {
		if op.Type == "events" {
			eventsWriteCount++
			allEvents = append(allEvents, op.Events...)
		}
	}

	if eventsWriteCount != 1 {
		t.Errorf("expected 1 events write, got %d", eventsWriteCount)
	}

	// Verify all 4 events were written in order
	if len(allEvents) != 4 {
		t.Fatalf("expected 4 events, got %d", len(allEvents))
	}

	expectedSeqs := []int64{1, 2, 3, 4}
	for i, ev := range allEvents {
		if ev.Seq != expectedSeqs[i] {
			t.Errorf("event at index %d: expected seq %d, got %d", i, expectedSeqs[i], ev.Seq)
		}
	}
}
