package policy

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pithecene/quarry/types"
)

// blockingSink stalls its first WriteEvents; later writes succeed at once.
// When honourCtx is true the stalled write returns on context cancellation;
// otherwise it ignores the context, like a hung store.
type blockingSink struct {
	*StubSink
	honourCtx bool
	entered   chan struct{}
	once      sync.Once
	release   chan struct{}
}

func (s *blockingSink) WriteEvents(ctx context.Context, _ []*types.EventEnvelope) error {
	first := false
	s.once.Do(func() { first = true; close(s.entered) })
	if !first {
		return nil
	}
	if s.honourCtx {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.release:
			return nil
		}
	}
	<-s.release
	return nil
}

func newStalledStreamingPolicy(t *testing.T, honourCtx bool) (*StreamingPolicy, *blockingSink) {
	t.Helper()
	sink := &blockingSink{StubSink: NewStubSink(), honourCtx: honourCtx, entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-sink.release:
		default:
			close(sink.release)
		}
	})
	pol, err := NewStreamingPolicy(sink, StreamingConfig{FlushInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewStreamingPolicy: %v", err)
	}
	if err := pol.IngestEvent(t.Context(), &types.EventEnvelope{RunID: "r", Seq: 1, Type: types.EventTypeItem}); err != nil {
		t.Fatalf("IngestEvent: %v", err)
	}
	select {
	case <-sink.entered: // the interval flush is now stalled in the sink
	case <-time.After(5 * time.Second):
		t.Fatal("interval flush never reached the sink")
	}
	return pol, sink
}

// A stalled interval flush that honours its context is cancelled by Close.
func TestStreamingPolicy_CloseCancelsStalledIntervalFlush(t *testing.T) {
	pol, _ := newStalledStreamingPolicy(t, true)
	done := make(chan struct{})
	go func() { _ = pol.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked behind a stalled interval flush")
	}
}

// An interval flush that ignores its context is joined, never abandoned:
// Close does not return while that write is still in flight.
func TestStreamingPolicy_CloseJoinsUncancellableIntervalFlush(t *testing.T) {
	pol, sink := newStalledStreamingPolicy(t, false)
	done := make(chan struct{})
	go func() { _ = pol.Close(); close(done) }()
	select {
	case <-done:
		t.Fatal("Close returned while the interval write was still in flight")
	case <-time.After(200 * time.Millisecond):
	}
	sink.release <- struct{}{} // the write returns
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the interval write finished")
	}
}

// A flush with an already-cancelled context does nothing, even when the
// flush lock is free.
func TestStreamingPolicy_CancelledFlushWritesNothing(t *testing.T) {
	sink := NewStubSink()
	pol, err := NewStreamingPolicy(sink, StreamingConfig{FlushCount: 100})
	if err != nil {
		t.Fatalf("NewStreamingPolicy: %v", err)
	}
	if err := pol.IngestEvent(t.Context(), &types.EventEnvelope{RunID: "r", Seq: 1, Type: types.EventTypeItem}); err != nil {
		t.Fatalf("IngestEvent: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for range 50 {
		if err := pol.Flush(ctx); err == nil {
			t.Fatal("Flush with a cancelled context returned nil")
		}
	}
	if got := sink.Stats().EventsWritten; got != 0 {
		t.Fatalf("cancelled flushes wrote %d events, want 0", got)
	}
}

// A count-triggered flush still writing when Close's own flush times out is
// joined before the sink closes, never abandoned.
func TestStreamingPolicy_CloseJoinsInFlightCountFlush(t *testing.T) {
	prev := closeFlushTimeout
	closeFlushTimeout = 50 * time.Millisecond
	t.Cleanup(func() { closeFlushTimeout = prev })

	sink := &closeTrackingSink{blockingSink: &blockingSink{StubSink: NewStubSink(), entered: make(chan struct{}), release: make(chan struct{})}}
	pol, err := NewStreamingPolicy(sink, StreamingConfig{FlushCount: 1})
	if err != nil {
		t.Fatalf("NewStreamingPolicy: %v", err)
	}
	ingestDone := make(chan struct{})
	go func() {
		_ = pol.IngestEvent(t.Context(), &types.EventEnvelope{RunID: "r", Seq: 1, Type: types.EventTypeItem})
		close(ingestDone)
	}()
	<-sink.entered // the count-triggered flush is writing

	closeDone := make(chan struct{})
	go func() { _ = pol.Close(); close(closeDone) }()
	time.Sleep(200 * time.Millisecond) // well past closeFlushTimeout
	if sink.closed() {
		t.Fatal("sink closed while a flush was still writing")
	}
	sink.release <- struct{}{}
	<-ingestDone
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the in-flight flush finished")
	}
	if !sink.closed() {
		t.Fatal("sink not closed")
	}
}

type closeTrackingSink struct {
	*blockingSink
	mu       sync.Mutex
	isClosed bool
}

func (s *closeTrackingSink) Close() error {
	s.mu.Lock()
	s.isClosed = true
	s.mu.Unlock()
	return nil
}

func (s *closeTrackingSink) closed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isClosed
}

// When a flush in flight holds the sink past the close deadline, events
// accepted meanwhile are not persisted; Close reports them instead of
// returning nil.
func TestStreamingPolicy_CloseReportsUnflushedData(t *testing.T) {
	prev := closeFlushTimeout
	closeFlushTimeout = 50 * time.Millisecond
	t.Cleanup(func() { closeFlushTimeout = prev })

	sink := &blockingSink{StubSink: NewStubSink(), entered: make(chan struct{}), release: make(chan struct{})}
	pol, err := NewStreamingPolicy(sink, StreamingConfig{FlushCount: 100})
	if err != nil {
		t.Fatalf("NewStreamingPolicy: %v", err)
	}
	ctx := t.Context()
	if err := pol.IngestEvent(ctx, &types.EventEnvelope{RunID: "r", Seq: 1, Type: types.EventTypeItem}); err != nil {
		t.Fatalf("IngestEvent: %v", err)
	}
	flushDone := make(chan struct{})
	go func() { _ = pol.Flush(ctx); close(flushDone) }()
	<-sink.entered // the caller's flush holds the sink
	if err := pol.IngestEvent(ctx, &types.EventEnvelope{RunID: "r", Seq: 2, Type: types.EventTypeItem}); err != nil {
		t.Fatalf("IngestEvent: %v", err)
	}

	closeErr := make(chan error, 1)
	go func() { closeErr <- pol.Close() }()
	time.Sleep(150 * time.Millisecond) // the close-time flush has timed out
	sink.release <- struct{}{}
	<-flushDone
	select {
	case err := <-closeErr:
		if !errors.Is(err, ErrCloseUnflushed) {
			t.Fatalf("Close err = %v, want ErrCloseUnflushed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return")
	}
	if pol.Stats().Errors == 0 {
		t.Error("the timed-out close flush was not counted as an error")
	}
}
