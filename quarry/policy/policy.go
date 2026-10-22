// Package policy defines the ingestion policy interface per CONTRACT_POLICY.md.
package policy

import (
	"context"
	"sync/atomic"

	"github.com/pithecene/quarry/types"
)

// Policy defines the ingestion policy interface.
// Policies control buffering, dropping, and persistence behavior.
//
// Per CONTRACT_POLICY.md:
//   - May drop: log, enqueue, rotate_proxy
//   - Must NOT drop: item, artifact, checkpoint, run_error, run_complete
//   - Policy must not alter event shapes
//   - Policy failure terminates the run
type Policy interface {
	// IngestEvent handles an event envelope.
	// May drop droppable event types (log, enqueue, rotate_proxy).
	// Must not drop non-droppable types; return error to terminate run.
	IngestEvent(ctx context.Context, envelope *types.EventEnvelope) error

	// IngestArtifactChunk handles an artifact chunk.
	// Must buffer/persist chunks in order.
	// Returns error on failure (terminates run).
	IngestArtifactChunk(ctx context.Context, chunk *types.ArtifactChunk) error

	// Flush flushes any buffered data.
	// Called on run_complete, run_error, or runtime termination.
	Flush(ctx context.Context) error

	// Close flushes what it can (within a deadline the sink must honour; see
	// Sink) and releases policy resources.
	// It is one-shot: any later Close returns the first result without
	// flushing or writing.
	Close() error

	// Stats returns policy statistics for observability.
	// Returns an atomic snapshot of policy metrics at a point in time.
	// All counters in the returned Stats are consistent with each other.
	Stats() Stats
}

// Stats represents policy observability metrics per CONTRACT_POLICY.md.
type Stats struct {
	// TotalEvents is the total number of events received.
	TotalEvents int64
	// EventsPersisted is the number of events persisted.
	EventsPersisted int64
	// EventsDropped is the total number of events dropped.
	EventsDropped int64
	// DroppedByType maps event types to drop counts.
	DroppedByType map[types.EventType]int64
	// TotalChunks is the total number of artifact chunks received.
	TotalChunks int64
	// ChunksPersisted is the number of chunks persisted.
	ChunksPersisted int64
	// BufferSize is the current buffer size in bytes (if buffered).
	BufferSize int64
	// FlushCount is the number of flush operations.
	FlushCount int64
	// Errors is the count of non-fatal errors encountered.
	Errors int64
	// FlushTriggers is a per-trigger-type flush counter.
	// Only populated by streaming policy; nil for strict/buffered.
	// Keys are trigger names: "count", "interval", "termination", "capacity".
	FlushTriggers map[string]int64
}

// droppableTypes defines which event types may be dropped per CONTRACT_POLICY.md.
var droppableTypes = map[types.EventType]bool{
	types.EventTypeLog:         true,
	types.EventTypeEnqueue:     true,
	types.EventTypeRotateProxy: true,
}

// IsDroppable returns true if the event type may be dropped by policy.
func IsDroppable(eventType types.EventType) bool {
	return droppableTypes[eventType]
}

// estimateEventSize returns an estimated size in bytes for an event envelope.
// Used by buffered and streaming policies for buffer management.
func estimateEventSize(envelope *types.EventEnvelope) int64 {
	// Base size for envelope structure
	size := int64(200)
	if envelope.Payload != nil {
		size += int64(len(envelope.Payload) * 50) // rough estimate per field
	}
	return size
}

// DroppableTypes returns the set of event types that may be dropped.
func DroppableTypes() map[types.EventType]bool {
	// Return a copy to prevent mutation
	result := make(map[types.EventType]bool, len(droppableTypes))
	for k, v := range droppableTypes {
		result[k] = v
	}
	return result
}

// statsRecorder is an internal helper for thread-safe stats management.
// Uses atomic.Int64 for hot-path counters to eliminate lock contention.
//
// Synchronization model:
//   - Counter methods use atomics (lock-free): incTotalEvents, incFlush, etc.
//   - droppedByType map requires caller-provided synchronization:
//     StrictPolicy never writes (safe without lock);
//     Buffered/StreamingPolicy guard under their own mu.
//   - snapshot() reads atomics individually (relaxed consistency, suitable for observability).
//   - snapshotLocked() reads atomics under caller's mu (consistent with buffer state).
type statsRecorder struct {
	totalEvents     atomic.Int64
	eventsPersisted atomic.Int64
	eventsDropped   atomic.Int64
	totalChunks     atomic.Int64
	chunksPersisted atomic.Int64
	bufferSize      atomic.Int64
	flushCount      atomic.Int64
	errors          atomic.Int64

	// droppedByType is a map requiring external synchronization.
	// StrictPolicy never writes to it (snapshot is safe without locking).
	// Buffered/StreamingPolicy guard it under their own mu.
	droppedByType map[types.EventType]int64
}

// newStatsRecorder creates a new recorder with initialized droppedByType map.
func newStatsRecorder() *statsRecorder {
	return &statsRecorder{
		droppedByType: make(map[types.EventType]int64),
	}
}

func (r *statsRecorder) incTotalEvents()            { r.totalEvents.Add(1) }
func (r *statsRecorder) incEventsPersisted(n int64)  { r.eventsPersisted.Add(n) }
func (r *statsRecorder) incTotalChunks()             { r.totalChunks.Add(1) }
func (r *statsRecorder) incChunksPersisted(n int64)  { r.chunksPersisted.Add(n) }
func (r *statsRecorder) incErrors()                  { r.errors.Add(1) }
func (r *statsRecorder) incFlush()                   { r.flushCount.Add(1) }

func (r *statsRecorder) snapshot() Stats {
	s := Stats{
		TotalEvents:     r.totalEvents.Load(),
		EventsPersisted: r.eventsPersisted.Load(),
		EventsDropped:   r.eventsDropped.Load(),
		TotalChunks:     r.totalChunks.Load(),
		ChunksPersisted: r.chunksPersisted.Load(),
		BufferSize:      r.bufferSize.Load(),
		FlushCount:      r.flushCount.Load(),
		Errors:          r.errors.Load(),
		DroppedByType:   make(map[types.EventType]int64, len(r.droppedByType)),
	}
	for k, v := range r.droppedByType {
		s.DroppedByType[k] = v
	}
	return s
}

// --- Caller-locked methods ---
// These use atomic operations for counters (lock-free). Map operations on
// droppedByType still require the caller to hold its policy mu.

func (r *statsRecorder) incTotalEventsLocked()            { r.totalEvents.Add(1) }
func (r *statsRecorder) incEventsPersistedLocked(n int64) { r.eventsPersisted.Add(n) }

func (r *statsRecorder) incEventsDroppedLocked(eventType types.EventType) {
	r.eventsDropped.Add(1)
	r.droppedByType[eventType]++
}

func (r *statsRecorder) incTotalChunksLocked()            { r.totalChunks.Add(1) }
func (r *statsRecorder) incChunksPersistedLocked(n int64) { r.chunksPersisted.Add(n) }
func (r *statsRecorder) incErrorsLocked()                 { r.errors.Add(1) }
func (r *statsRecorder) incFlushLocked()                  { r.flushCount.Add(1) }
func (r *statsRecorder) setBufferSizeLocked(bytes int64)  { r.bufferSize.Store(bytes) }

// snapshotLocked returns a snapshot of stats with the given bufferSize.
// Caller must hold its policy mu (required for droppedByType map access).
func (r *statsRecorder) snapshotLocked(bufferSize int64) Stats {
	s := Stats{
		TotalEvents:     r.totalEvents.Load(),
		EventsPersisted: r.eventsPersisted.Load(),
		EventsDropped:   r.eventsDropped.Load(),
		TotalChunks:     r.totalChunks.Load(),
		ChunksPersisted: r.chunksPersisted.Load(),
		BufferSize:      bufferSize,
		FlushCount:      r.flushCount.Load(),
		Errors:          r.errors.Load(),
		DroppedByType:   make(map[types.EventType]int64, len(r.droppedByType)),
	}
	for k, v := range r.droppedByType {
		s.DroppedByType[k] = v
	}
	return s
}
