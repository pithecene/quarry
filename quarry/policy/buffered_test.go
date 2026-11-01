package policy_test

import (
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

func TestBufferedPolicy_ValidConfig_OneLimit(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		config policy.BufferedConfig
	}{
		{"event limit only", policy.BufferedConfig{MaxBufferEvents: 10, MaxBufferBytes: 0}},
		{"byte limit only", policy.BufferedConfig{MaxBufferEvents: 0, MaxBufferBytes: 1024}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pol, err := policy.NewBufferedPolicy(policy.NewStubSink(), tc.config)
			if err != nil {
				t.Fatalf("expected success, got %v", err)
			}
			if pol == nil {
				t.Fatal("expected non-nil policy")
			}
		})
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

func TestBufferedPolicy_BufferSize_ZeroAfterSuccessfulFlush(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferBytes: 1000}
	pol := mustNewBufferedPolicy(t, sink, config)

	// Buffer an event
	_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem,
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

// TestBufferedPolicy_EventsWrittenInSequenceOrder tests that all events
// (including artifact commits) are written in their original sequence order.
func TestBufferedPolicy_EventsWrittenInSequenceOrder(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{
		MaxBufferEvents: 100,
		MaxBufferBytes:  1024 * 1024,
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
		written = append(written, op.Events...)
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
		eventsWriteCount++
		allEvents = append(allEvents, op.Events...)
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
