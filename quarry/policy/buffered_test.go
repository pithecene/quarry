package policy_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

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
	if !errors.Is(err, policy.ErrSinkWrite) {
		t.Errorf("expected the error to carry ErrSinkWrite, got %v", err)
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

// pausingSink blocks its first WriteEvents until released, recording every
// batch it writes, so a test can ingest while a flush is in flight. A non-nil firstErr
// fails that first write, which then records nothing.
type pausingSink struct {
	mu       sync.Mutex
	started  chan struct{}
	release  chan struct{}
	firstErr error
	paused   bool
	batches  [][]string
}

func (s *pausingSink) WriteEvents(_ context.Context, events []*types.EventEnvelope) error {
	ids := make([]string, 0, len(events))
	for _, e := range events {
		ids = append(ids, e.EventID)
	}
	s.mu.Lock()
	pause := !s.paused
	s.paused = true
	s.mu.Unlock()
	if pause {
		close(s.started)
		<-s.release
		if s.firstErr != nil {
			return s.firstErr
		}
	}
	s.mu.Lock()
	s.batches = append(s.batches, ids)
	s.mu.Unlock()
	return nil
}

func (s *pausingSink) Close() error { return nil }

// A decoded event counts the bytes of the frame it arrived in, so a large
// payload with few fields still fills the byte limit.
func TestBufferedPolicy_CountsTheFrameBytes(t *testing.T) {
	t.Parallel()
	pol := mustNewBufferedPolicy(t, policy.NewStubSink(), policy.BufferedConfig{MaxBufferBytes: 1 << 20})
	big := &types.EventEnvelope{EventID: "e1", Type: types.EventTypeLog, Seq: 1, Payload: map[string]any{"message": "…"}, RawFrameBytes: 600 << 10}
	if err := pol.IngestEvent(t.Context(), big); err != nil {
		t.Fatal(err)
	}
	if got := pol.Stats().BufferSize; got != 600<<10 {
		t.Fatalf("BufferSize = %d, want the frame's %d bytes", got, 600<<10)
	}
	// A second frame of that size does not fit, so the droppable log is dropped.
	next := *big
	next.EventID, next.Seq = "e2", 2
	if err := pol.IngestEvent(t.Context(), &next); err != nil {
		t.Fatal(err)
	}
	if stats := pol.Stats(); stats.BufferSize != 600<<10 || stats.EventsDropped != 1 {
		t.Errorf("BufferSize %d, dropped %d; want %d and 1", stats.BufferSize, stats.EventsDropped, 600<<10)
	}
}

// startPausedFlush starts a flush that the sink holds until release runs, and
// returns release and the flush's result. Cleanup releases the sink before
// the policy's own cleanup closes it, so a failing test never hangs.
func startPausedFlush(t *testing.T, pol *policy.BufferedPolicy, sink *pausingSink) (release func(), flushed <-chan error) {
	t.Helper()
	release = sync.OnceFunc(func() { close(sink.release) })
	t.Cleanup(release)
	result := make(chan error, 1)
	go func() { result <- pol.Flush(t.Context()) }()
	<-sink.started
	return release, result
}

// Eviction never takes an event that a flush is writing, so each event is
// counted once, as persisted or as dropped.
func TestBufferedPolicy_NeverEvictsAnEventInFlight(t *testing.T) {
	t.Parallel()
	sink := &pausingSink{started: make(chan struct{}), release: make(chan struct{})}
	pol := mustNewBufferedPolicy(t, sink, policy.BufferedConfig{MaxBufferEvents: 2})
	event := func(id string, seq int64, typ types.EventType) *types.EventEnvelope {
		return &types.EventEnvelope{EventID: id, Type: typ, Seq: seq}
	}
	if err := pol.IngestEvent(t.Context(), event("log-1", 1, types.EventTypeLog)); err != nil {
		t.Fatal(err)
	}
	release, flushed := startPausedFlush(t, pol, sink)
	for _, e := range []*types.EventEnvelope{event("log-2", 2, types.EventTypeLog), event("item-3", 3, types.EventTypeItem)} {
		if err := pol.IngestEvent(t.Context(), e); err != nil {
			t.Fatalf("ingest %s: %v", e.EventID, err)
		}
	}
	release()
	if err := <-flushed; err != nil {
		t.Fatalf("first flush: %v", err)
	}
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("second flush: %v", err)
	}
	stats := pol.Stats()
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if fmt.Sprint(sink.batches) != "[[log-1] [item-3]]" || stats.TotalEvents != 3 || stats.EventsPersisted != 2 || stats.EventsDropped != 1 {
		t.Errorf("batches %v, received %d, persisted %d, dropped %d; want [[log-1] [item-3]], 3, 2, 1",
			sink.batches, stats.TotalEvents, stats.EventsPersisted, stats.EventsDropped)
	}
}

// An event ingested while a flush writes is kept for the next flush: the
// flush removes the entries it wrote, so neither a new envelope nor the same
// envelope ingested again is lost with them.
func TestBufferedPolicy_KeepsEventsIngestedDuringAFlush(t *testing.T) {
	t.Parallel()
	first := &types.EventEnvelope{EventID: "e1", Type: types.EventTypeItem, Seq: 1}
	cases := []struct {
		name string
		late *types.EventEnvelope
		want string
	}{
		{"a new envelope", &types.EventEnvelope{EventID: "e2", Type: types.EventTypeItem, Seq: 2}, "[[e1] [e2]]"},
		{"the same envelope again", first, "[[e1] [e1]]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sink := &pausingSink{started: make(chan struct{}), release: make(chan struct{})}
			pol := mustNewBufferedPolicy(t, sink, policy.BufferedConfig{MaxBufferEvents: 10})
			if err := pol.IngestEvent(t.Context(), first); err != nil {
				t.Fatal(err)
			}
			release, flushed := startPausedFlush(t, pol, sink)
			if err := pol.IngestEvent(t.Context(), tc.late); err != nil {
				t.Fatal(err)
			}
			release()
			if err := <-flushed; err != nil {
				t.Fatalf("first flush: %v", err)
			}
			if err := pol.Flush(t.Context()); err != nil {
				t.Fatalf("second flush: %v", err)
			}
			sink.mu.Lock()
			defer sink.mu.Unlock()
			if got := fmt.Sprint(sink.batches); got != tc.want {
				t.Errorf("batches %s, want %s", got, tc.want)
			}
		})
	}
}

// A flush waiting for another flush returns when its own context ends, and
// counts the failure. In synctest the deadline fires only once the flush is
// blocked on the other, and a flush that ignored it would be reported as a
// deadlock.
func TestBufferedPolicy_FlushWaitHonorsCancellation(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		sink := &pausingSink{started: make(chan struct{}), release: make(chan struct{})}
		pol := mustNewBufferedPolicy(t, sink, policy.BufferedConfig{MaxBufferEvents: 10})
		if err := pol.IngestEvent(t.Context(), &types.EventEnvelope{EventID: "e1", Type: types.EventTypeItem, Seq: 1}); err != nil {
			t.Fatal(err)
		}
		startPausedFlush(t, pol, sink)
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		if err := pol.Flush(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Flush = %v, want context.DeadlineExceeded", err)
		}
		if errs := pol.Stats().Errors; errs != 1 {
			t.Errorf("Stats.Errors = %d, want 1 for the abandoned flush", errs)
		}
	})
}
