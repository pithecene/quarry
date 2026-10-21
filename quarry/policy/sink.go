// Package policy defines the ingestion policy interface per CONTRACT_POLICY.md.
package policy

import (
	"context"
	"sync"

	"github.com/pithecene/quarry/types"
)

// Sink abstracts persistence for policies.
// Implementations may write to storage, forward to a queue, or stub for testing.
//
// Methods are batch-oriented to support both strict (batch of 1) and buffered policies.
type Sink interface {
	// WriteEvents persists a batch of event envelopes.
	// Must preserve ordering within the batch.
	// Returns error on failure; caller decides whether to retry or fail.
	WriteEvents(ctx context.Context, events []*types.EventEnvelope) error

	// WriteChunks persists a batch of artifact chunks.
	// Must preserve ordering within the batch.
	// Returns error on failure; caller decides whether to retry or fail.
	WriteChunks(ctx context.Context, chunks []*types.ArtifactChunk) error

	// Close releases any resources held by the sink.
	Close() error
}

// WriteOp represents a write operation for ordering verification.
type WriteOp struct {
	Type   string // "events" or "chunks"
	Events []*types.EventEnvelope
	Chunks []*types.ArtifactChunk
}

// StubSink is a test sink that accepts writes without persisting.
// Tracks write statistics for test assertions.
type StubSink struct {
	mu sync.Mutex

	// EventsWritten is the total count of events written.
	EventsWritten int64
	// ChunksWritten is the total count of chunks written.
	ChunksWritten int64
	// EventBatches is the number of WriteEvents calls.
	EventBatches int64
	// ChunkBatches is the number of WriteChunks calls.
	ChunkBatches int64
	// Closed indicates whether Close was called.
	Closed bool

	// WrittenEvents stores all written events for inspection.
	WrittenEvents []*types.EventEnvelope
	// WrittenChunks stores all written chunks for inspection.
	WrittenChunks []*types.ArtifactChunk

	// WriteOrder tracks the order of write operations for ordering tests.
	WriteOrder []WriteOp

	// ErrorOnWrite, if non-nil, is returned by WriteEvents/WriteChunks.
	ErrorOnWrite error
}

// NewStubSink creates a new stub sink for testing.
func NewStubSink() *StubSink {
	return &StubSink{
		WrittenEvents: make([]*types.EventEnvelope, 0),
		WrittenChunks: make([]*types.ArtifactChunk, 0),
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
	s.WriteOrder = append(s.WriteOrder, WriteOp{Type: "events", Events: events})

	return nil
}

// WriteChunks records the chunks without persisting.
func (s *StubSink) WriteChunks(_ context.Context, chunks []*types.ArtifactChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ErrorOnWrite != nil {
		return s.ErrorOnWrite
	}

	s.ChunkBatches++
	s.ChunksWritten += int64(len(chunks))
	s.WrittenChunks = append(s.WrittenChunks, chunks...)
	s.WriteOrder = append(s.WriteOrder, WriteOp{Type: "chunks", Chunks: chunks})

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
		ChunksWritten: s.ChunksWritten,
		EventBatches:  s.EventBatches,
		ChunkBatches:  s.ChunkBatches,
		Closed:        s.Closed,
	}
}

// StubSinkStats is a snapshot of StubSink statistics.
type StubSinkStats struct {
	EventsWritten int64
	ChunksWritten int64
	EventBatches  int64
	ChunkBatches  int64
	Closed        bool
}
