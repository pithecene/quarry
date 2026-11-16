// Package policy defines the ingestion policy interface per CONTRACT_POLICY.md.
package policy

import (
	"context"
	"errors"
	"sync"

	"github.com/pithecene/quarry/types"
)

// ErrSinkWrite marks an error that a policy received from its sink's
// WriteEvents. The runtime reports a run that fails with it as
// storage_error, which a retry may resolve, and not as policy_failure.
var ErrSinkWrite = errors.New("sink write failed")

// Sink abstracts persistence for policies.
// Implementations may write to storage, forward to a queue, or stub for testing.
//
// Methods are batch-oriented to support both strict (batch of 1) and buffered policies.
//
// Write methods must return once ctx is done. Policies bound their close-time
// flush with a context deadline and never abandon a write in progress (it
// could otherwise be persisted after the caller considers the policy closed), so that
// bound holds only for sinks that honor cancellation.
type Sink interface {
	// WriteEvents persists a batch of event envelopes.
	// Must preserve ordering within the batch.
	// Returns error on failure; caller decides whether to retry or fail.
	WriteEvents(ctx context.Context, events []*types.EventEnvelope) error

	// Close releases any resources held by the sink.
	Close() error
}

// WriteOp represents a write operation for ordering verification.
type WriteOp struct {
	Events []*types.EventEnvelope
}

// StubSink is a test sink that accepts writes without persisting.
// Tracks write statistics for test assertions.
type StubSink struct {
	mu sync.Mutex

	// EventsWritten is the total count of events written.
	EventsWritten int64
	// EventBatches is the number of WriteEvents calls.
	EventBatches int64
	// Closed indicates whether Close was called.
	Closed bool

	// WrittenEvents stores all written events for inspection.
	WrittenEvents []*types.EventEnvelope

	// WriteOrder tracks the order of write operations for ordering tests.
	WriteOrder []WriteOp

	// ErrorOnWrite, if non-nil, is returned by WriteEvents.
	ErrorOnWrite error
}

// NewStubSink creates a new stub sink for testing.
func NewStubSink() *StubSink {
	return &StubSink{
		WrittenEvents: make([]*types.EventEnvelope, 0),
		WriteOrder:    make([]WriteOp, 0),
	}
}

// WriteEvents records the events without persisting.
func (s *StubSink) WriteEvents(_ context.Context, events []*types.EventEnvelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ErrorOnWrite != nil {
		return s.ErrorOnWrite
	}

	s.EventBatches++
	s.EventsWritten += int64(len(events))
	s.WrittenEvents = append(s.WrittenEvents, events...)
	s.WriteOrder = append(s.WriteOrder, WriteOp{Events: events})

	return nil
}

// Close marks the sink as closed.
func (s *StubSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Closed = true
	return nil
}

// Stats returns a snapshot of sink statistics.
func (s *StubSink) Stats() StubSinkStats {
	s.mu.Lock()
	defer s.mu.Unlock()

	return StubSinkStats{
		EventsWritten: s.EventsWritten,
		EventBatches:  s.EventBatches,
		Closed:        s.Closed,
	}
}

// StubSinkStats is a snapshot of StubSink statistics.
type StubSinkStats struct {
	EventsWritten int64
	EventBatches  int64
	Closed        bool
}
