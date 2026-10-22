package policy

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/pithecene/quarry/log"
	"github.com/pithecene/quarry/types"
)

// FlushMode controls flush semantics for BufferedPolicy.
type FlushMode string

const (
	// FlushAtLeastOnce preserves all buffers on any failure.
	// May cause duplicate event writes on retry, but guarantees no data loss.
	// This is the default and safest mode.
	FlushAtLeastOnce FlushMode = "at_least_once"

	// FlushChunksFirst writes chunks before events.
	// If chunks fail, events are not written (no duplicates).
	// If chunks succeed but events fail, chunks may be duplicated on retry.
	FlushChunksFirst FlushMode = "chunks_first"

	// FlushTwoPhase tracks per-buffer success to avoid duplicates.
	// Events written successfully are not re-written on retry even if chunks fail.
	// Requires internal state tracking; most complex mode.
	FlushTwoPhase FlushMode = "two_phase"
)

// BufferedConfig configures a BufferedPolicy.
type BufferedConfig struct {
	// MaxBufferEvents is the maximum number of events to buffer.
	// Zero means no limit (use MaxBufferBytes instead).
	MaxBufferEvents int

	// MaxBufferBytes is the maximum buffer size in bytes (estimated).
	// Zero means no limit (use MaxBufferEvents instead).
	// At least one limit must be set.
	MaxBufferBytes int64

	// FlushMode controls flush failure semantics.
	// Default is FlushAtLeastOnce (safest, may duplicate on retry).
	FlushMode FlushMode

	// Logger is an optional logger for policy observability.
	// If nil, no logging is emitted.
	Logger *log.Logger
}

// DefaultBufferedConfig returns sensible defaults for buffered policy.
func DefaultBufferedConfig() BufferedConfig {
	return BufferedConfig{
		MaxBufferEvents: 1000,
		MaxBufferBytes:  10 * 1024 * 1024, // 10 MB
		FlushMode:       FlushAtLeastOnce,
	}
}

// ErrBufferFull is returned when buffer is full and event is non-droppable.
var ErrBufferFull = errors.New("buffer full: cannot accept non-droppable event")

// ErrInvalidConfig is returned when BufferedConfig is invalid.
var ErrInvalidConfig = errors.New("invalid config: at least one of MaxBufferEvents or MaxBufferBytes must be set")

// ErrInvalidFlushMode is returned when FlushMode is unknown.
var ErrInvalidFlushMode = errors.New("invalid flush mode")

// BufferedPolicy implements buffered persistence with drop rules.
//
// Per CONTRACT_POLICY.md:
//   - Bounded buffer with explicit limits
//   - May drop: log, enqueue, rotate_proxy
//   - Must NOT drop: item, artifact, checkpoint, run_error, run_complete
//   - Batch writes on flush
//   - Flush on run_complete, run_error, runtime termination
//
// The "chunks before commit" invariant is enforced by the Lode client,
// not by reordering events in the policy. Events are written in seq order.
type BufferedPolicy struct {
	sink   Sink
	config BufferedConfig
	logger *log.Logger

	closer onceCloser
	closed bool // set under mu when Close begins; ingestion is refused after

	mu              sync.Mutex // guards buffer state only
	eventBuffer     []*types.EventEnvelope
	eventBufferNext []*types.EventEnvelope // TwoPhase: events added after eventsFlushed=true
	chunkBuffer     []*types.ArtifactChunk
	chunkBufferNext []*types.ArtifactChunk // TwoPhase: chunks added after chunksFlushed=true
	bufferBytes     int64
	chunksFlushed   bool // TwoPhase: chunkBuffer written, awaiting events success
	eventsFlushed   bool // TwoPhase: eventBuffer written, awaiting full success
	stats           *statsRecorder
}

// NewBufferedPolicy creates a new buffered policy.
// Returns error if config is invalid.
func NewBufferedPolicy(sink Sink, config BufferedConfig) (*BufferedPolicy, error) {
	if config.MaxBufferEvents <= 0 && config.MaxBufferBytes <= 0 {
		return nil, ErrInvalidConfig
	}

	// Default flush mode
	if config.FlushMode == "" {
		config.FlushMode = FlushAtLeastOnce
	}

	// Validate flush mode
	switch config.FlushMode {
	case FlushAtLeastOnce, FlushChunksFirst, FlushTwoPhase:
		// valid
	default:
		return nil, fmt.Errorf("%w: %s", ErrInvalidFlushMode, config.FlushMode)
	}

	return &BufferedPolicy{
		sink:            sink,
		config:          config,
		logger:          config.Logger,
		eventBuffer:     make([]*types.EventEnvelope, 0, max(config.MaxBufferEvents, 100)),
		eventBufferNext: make([]*types.EventEnvelope, 0),
		chunkBuffer:     make([]*types.ArtifactChunk, 0),
		chunkBufferNext: make([]*types.ArtifactChunk, 0),
		stats:           newStatsRecorder(),
	}, nil
}

// IngestEvent buffers the event, applying drop rules if buffer is full.
//
// Drop strategy when full:
//   - If incoming event is droppable: drop it, record in stats
//   - If incoming event is non-droppable and buffer has droppable events: drop oldest droppable
//   - If incoming event is non-droppable and no droppable events: return error (fail run)
//
// In TwoPhase mode, events added after a partial flush go to eventBufferNext.
func (p *BufferedPolicy) IngestEvent(_ context.Context, envelope *types.EventEnvelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrPolicyClosed
	}

	p.stats.incTotalEventsLocked()

	eventSize := p.estimateEventSize(envelope)

	// Check if buffer has room
	if p.hasRoomForEvent(eventSize) {
		p.appendEvent(envelope, eventSize)
		return nil
	}

	// Buffer is full - apply drop rules
	if IsDroppable(envelope.Type) {
		// Drop the incoming event
		p.stats.incEventsDroppedLocked(envelope.Type)
		p.logDrop(envelope.Type, "buffer_full")
		return nil
	}

	// Non-droppable event - try to make room by dropping oldest droppable
	if p.dropOldestDroppable() && p.hasRoomForBytes(eventSize) {
		p.appendEvent(envelope, eventSize)
		return nil
	}

	// No room even after eviction, or no droppable events to evict
	p.stats.incErrorsLocked()
	p.logBufferOverflow(envelope.Type)
	return ErrBufferFull
}

// appendEvent adds an event to the appropriate buffer. Caller must hold mu.
// In TwoPhase mode with eventsFlushed=true, appends to eventBufferNext.
func (p *BufferedPolicy) appendEvent(envelope *types.EventEnvelope, eventSize int64) {
	if p.config.FlushMode == FlushTwoPhase && p.eventsFlushed {
		// Events added after partial flush go to next buffer
		p.eventBufferNext = append(p.eventBufferNext, envelope)
	} else {
		p.eventBuffer = append(p.eventBuffer, envelope)
	}
	p.bufferBytes += eventSize
	p.stats.setBufferSizeLocked(p.bufferBytes)
}

// IngestArtifactChunk buffers the chunk.
// Artifact chunks are never dropped per CONTRACT_POLICY.md.
// Returns error if chunk would exceed buffer limits (policy failure).
// Requires MaxBufferBytes to be set; chunks cannot be bounded by event count alone.
// In TwoPhase mode, chunks added after a partial flush go to chunkBufferNext.
func (p *BufferedPolicy) IngestArtifactChunk(_ context.Context, chunk *types.ArtifactChunk) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrPolicyClosed
	}

	p.stats.incTotalChunksLocked()

	// Chunks require byte limit for bounded buffering
	if p.config.MaxBufferBytes <= 0 {
		p.stats.incErrorsLocked()
		return fmt.Errorf("%w: chunk buffering requires MaxBufferBytes to be set", ErrBufferFull)
	}

	chunkSize := int64(len(chunk.Data))

	// Chunks are non-droppable; if buffer is full, fail the run
	if p.bufferBytes+chunkSize > p.config.MaxBufferBytes {
		p.stats.incErrorsLocked()
		return fmt.Errorf("%w: chunk size %d would exceed buffer limit", ErrBufferFull, chunkSize)
	}

	// In TwoPhase mode, route to chunkBufferNext if chunks already flushed
	if p.config.FlushMode == FlushTwoPhase && p.chunksFlushed {
		p.chunkBufferNext = append(p.chunkBufferNext, chunk)
	} else {
		p.chunkBuffer = append(p.chunkBuffer, chunk)
	}
	p.bufferBytes += chunkSize
	p.stats.setBufferSizeLocked(p.bufferBytes)

	return nil
}

// Flush writes all buffered events and chunks to the sink.
// Behavior depends on FlushMode configuration.
func (p *BufferedPolicy) Flush(ctx context.Context) error {
	switch p.config.FlushMode {
	case FlushChunksFirst:
		return p.flushChunksFirst(ctx)
	case FlushTwoPhase:
		return p.flushTwoPhase(ctx)
	default:
		return p.flushAtLeastOnce(ctx)
	}
}

// flushAtLeastOnce writes chunks then events; preserves all buffers on any failure.
// Chunks are written first to satisfy the "chunks before commit" invariant.
func (p *BufferedPolicy) flushAtLeastOnce(ctx context.Context) error {
	p.mu.Lock()
	p.stats.incFlushLocked()
	events := p.eventBuffer
	chunks := p.chunkBuffer
	p.mu.Unlock()

	// Write chunks first (required for "chunks before commit" invariant)
	if len(chunks) > 0 {
		if err := p.sink.WriteChunks(ctx, chunks); err != nil {
			p.mu.Lock()
			p.stats.incErrorsLocked()
			p.mu.Unlock()
			p.logFlushFailure("chunks", err)
			// Keep all buffers intact - prefer duplicates over loss
			return err
		}
		p.mu.Lock()
		p.stats.incChunksPersistedLocked(int64(len(chunks)))
		p.mu.Unlock()
	}

	// Write events after chunks
	if len(events) > 0 {
		if err := p.sink.WriteEvents(ctx, events); err != nil {
			p.mu.Lock()
			p.stats.incErrorsLocked()
			p.mu.Unlock()
			p.logFlushFailure("events", err)
			// Keep all buffers intact - prefer duplicates over loss
			return err
		}
		p.mu.Lock()
		p.stats.incEventsPersistedLocked(int64(len(events)))
		p.mu.Unlock()
	}

	// Clear buffers only after full success
	p.mu.Lock()
	p.clearEventBuffer()
	p.clearChunkBuffer()
	p.recalculateBufferBytes()
	p.mu.Unlock()

	return nil
}

// flushChunksFirst writes chunks, then events.
// If chunks fail, events are not written.
func (p *BufferedPolicy) flushChunksFirst(ctx context.Context) error {
	p.mu.Lock()
	p.stats.incFlushLocked()
	events := p.eventBuffer
	chunks := p.chunkBuffer
	p.mu.Unlock()

	// Write chunks first
	if len(chunks) > 0 {
		if err := p.sink.WriteChunks(ctx, chunks); err != nil {
			p.mu.Lock()
			p.stats.incErrorsLocked()
			p.mu.Unlock()
			// Keep all buffers - events not attempted
			return err
		}
		p.mu.Lock()
		p.stats.incChunksPersistedLocked(int64(len(chunks)))
		p.mu.Unlock()
	}

	// Write events after chunks succeed
	if len(events) > 0 {
		if err := p.sink.WriteEvents(ctx, events); err != nil {
			p.mu.Lock()
			p.stats.incErrorsLocked()
			// Chunks succeeded, events failed - clear chunks only
			p.clearChunkBuffer()
			p.recalculateBufferBytes()
			p.mu.Unlock()
			return err
		}
		p.mu.Lock()
		p.stats.incEventsPersistedLocked(int64(len(events)))
		p.mu.Unlock()
	}

	// Clear all buffers after full success
	p.mu.Lock()
	p.clearEventBuffer()
	p.clearChunkBuffer()
	p.recalculateBufferBytes()
	p.mu.Unlock()

	return nil
}

// flushTwoPhase tracks per-buffer success to avoid duplicates on retry.
// Order: chunks → chunksNext → events → eventsNext (chunks first for "chunks before commit").
// Handles chunks/events added after a partial flush via chunkBufferNext/eventBufferNext.
func (p *BufferedPolicy) flushTwoPhase(ctx context.Context) error {
	p.mu.Lock()
	p.stats.incFlushLocked()
	events := p.eventBuffer
	eventsNext := p.eventBufferNext
	chunks := p.chunkBuffer
	chunksNext := p.chunkBufferNext
	chunksFlushed := p.chunksFlushed
	eventsFlushed := p.eventsFlushed
	p.mu.Unlock()

	// Write original chunks first (required for "chunks before commit" invariant)
	if len(chunks) > 0 && !chunksFlushed {
		if err := p.sink.WriteChunks(ctx, chunks); err != nil {
			p.mu.Lock()
			p.stats.incErrorsLocked()
			p.mu.Unlock()
			return err
		}
		p.mu.Lock()
		p.stats.incChunksPersistedLocked(int64(len(chunks)))
		p.chunksFlushed = true
		p.mu.Unlock()
	}

	// Write new chunks added after partial flush
	if len(chunksNext) > 0 {
		if err := p.sink.WriteChunks(ctx, chunksNext); err != nil {
			p.mu.Lock()
			p.stats.incErrorsLocked()
			p.mu.Unlock()
			return err
		}
		p.mu.Lock()
		p.stats.incChunksPersistedLocked(int64(len(chunksNext)))
		p.clearChunkBufferNext()
		p.recalculateBufferBytes()
		p.mu.Unlock()
	}

	// Write original events if not already flushed
	if len(events) > 0 && !eventsFlushed {
		if err := p.sink.WriteEvents(ctx, events); err != nil {
			p.mu.Lock()
			p.stats.incErrorsLocked()
			p.mu.Unlock()
			return err
		}
		p.mu.Lock()
		p.stats.incEventsPersistedLocked(int64(len(events)))
		p.eventsFlushed = true
		p.mu.Unlock()
	}

	// Write new events added after partial flush
	if len(eventsNext) > 0 {
		if err := p.sink.WriteEvents(ctx, eventsNext); err != nil {
			p.mu.Lock()
			p.stats.incErrorsLocked()
			p.mu.Unlock()
			return err
		}
		p.mu.Lock()
		p.stats.incEventsPersistedLocked(int64(len(eventsNext)))
		p.mu.Unlock()
	}

	// Clear all buffers and reset state after full success
	p.mu.Lock()
	p.clearEventBuffer()
	p.clearEventBufferNext()
	p.clearChunkBuffer()
	p.clearChunkBufferNext()
	p.recalculateBufferBytes()
	p.chunksFlushed = false
	p.eventsFlushed = false
	p.mu.Unlock()

	return nil
}

// clearEventBuffer resets the event buffer. Caller must hold mu.
// Call recalculateBufferBytes after all buffer clears are complete.
func (p *BufferedPolicy) clearEventBuffer() {
	p.eventBuffer = make([]*types.EventEnvelope, 0, max(p.config.MaxBufferEvents, 100))
}

// clearEventBufferNext resets the next event buffer (TwoPhase). Caller must hold mu.
// Call recalculateBufferBytes after all buffer clears are complete.
func (p *BufferedPolicy) clearEventBufferNext() {
	p.eventBufferNext = make([]*types.EventEnvelope, 0)
}

// clearChunkBuffer resets the chunk buffer. Caller must hold mu.
// Call recalculateBufferBytes after all buffer clears are complete.
func (p *BufferedPolicy) clearChunkBuffer() {
	p.chunkBuffer = make([]*types.ArtifactChunk, 0)
}

// clearChunkBufferNext resets the next chunk buffer (TwoPhase). Caller must hold mu.
// Call recalculateBufferBytes after all buffer clears are complete.
func (p *BufferedPolicy) clearChunkBufferNext() {
	p.chunkBufferNext = make([]*types.ArtifactChunk, 0)
}

// recalculateBufferBytes recalculates bufferBytes from all buffers. Caller must hold mu.
func (p *BufferedPolicy) recalculateBufferBytes() {
	var total int64
	for _, event := range p.eventBuffer {
		total += p.estimateEventSize(event)
	}
	for _, event := range p.eventBufferNext {
		total += p.estimateEventSize(event)
	}
	for _, chunk := range p.chunkBuffer {
		total += int64(len(chunk.Data))
	}
	for _, chunk := range p.chunkBufferNext {
		total += int64(len(chunk.Data))
	}
	p.bufferBytes = total
	p.stats.setBufferSizeLocked(p.bufferBytes)
}

// Close flushes remaining data and closes the sink.
func (p *BufferedPolicy) Close() error {
	return p.closer.close(func() error {
		// Best-effort flush on close, bounded by a deadline the sink must
		// honour (see Sink); the write is never abandoned.
		ctx, cancel := context.WithTimeout(context.Background(), closeFlushTimeout)
		defer cancel()
		// Refuse ingestion from here on, so nothing is accepted after the final
		// flush takes its snapshot.
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()

		// A failed close-time flush leaves accepted data unpersisted: report it
		// rather than closing over it silently. (Buffers may also hold data
		// already written and kept for retry, so the flush result, not buffer
		// lengths, is what says whether anything was lost.)
		var lossErr error
		if err := p.Flush(ctx); err != nil {
			lossErr = fmt.Errorf("%w: close-time flush failed: %w", ErrCloseUnflushed, err)
		}
		return errors.Join(lossErr, p.sink.Close())
	})
}

// Stats returns policy statistics.
// Returns an atomic snapshot: the buffer mutex is held while taking the
// snapshot, ensuring all counters and buffer size are captured from the
// same point in time.
func (p *BufferedPolicy) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.stats.snapshotLocked(p.bufferBytes)
}

// hasRoomForEvent checks if the buffer can accept an event of the given size.
func (p *BufferedPolicy) hasRoomForEvent(eventSize int64) bool {
	// Check event count limit (all event buffers combined)
	totalEvents := len(p.eventBuffer) + len(p.eventBufferNext)
	if p.config.MaxBufferEvents > 0 && totalEvents >= p.config.MaxBufferEvents {
		return false
	}

	return p.hasRoomForBytes(eventSize)
}

// hasRoomForBytes checks if adding bytes would exceed the byte limit.
func (p *BufferedPolicy) hasRoomForBytes(size int64) bool {
	if p.config.MaxBufferBytes > 0 && p.bufferBytes+size > p.config.MaxBufferBytes {
		return false
	}
	return true
}

// dropOldestDroppable removes the oldest droppable event from the buffer.
// Scans eventBuffer first, then eventBufferNext (TwoPhase mode).
// Returns true if an event was dropped, false if no droppable events exist.
// Caller must hold mu.
func (p *BufferedPolicy) dropOldestDroppable() bool {
	// First scan eventBuffer
	for i, event := range p.eventBuffer {
		if IsDroppable(event.Type) {
			eventType := event.Type
			eventSize := p.estimateEventSize(event)
			p.eventBuffer = append(p.eventBuffer[:i], p.eventBuffer[i+1:]...)
			p.bufferBytes -= eventSize
			p.stats.setBufferSizeLocked(p.bufferBytes)
			p.stats.incEventsDroppedLocked(eventType)
			p.logDrop(eventType, "evicted_for_non_droppable")
			return true
		}
	}

	// Then scan eventBufferNext (TwoPhase mode)
	for i, event := range p.eventBufferNext {
		if IsDroppable(event.Type) {
			eventType := event.Type
			eventSize := p.estimateEventSize(event)
			p.eventBufferNext = append(p.eventBufferNext[:i], p.eventBufferNext[i+1:]...)
			p.bufferBytes -= eventSize
			p.stats.setBufferSizeLocked(p.bufferBytes)
			p.stats.incEventsDroppedLocked(eventType)
			p.logDrop(eventType, "evicted_for_non_droppable")
			return true
		}
	}

	return false
}

// estimateEventSize delegates to the package-level estimateEventSize.
func (p *BufferedPolicy) estimateEventSize(envelope *types.EventEnvelope) int64 {
	return estimateEventSize(envelope)
}

// --- Logging helpers (per CONTRACT_POLICY.md observability requirements) ---

// logDrop logs an event drop per CONTRACT_POLICY.md observability.
func (p *BufferedPolicy) logDrop(eventType types.EventType, reason string) {
	if p.logger == nil {
		return
	}
	p.logger.Warn("event dropped", map[string]any{
		"event_type": string(eventType),
		"reason":     reason,
		"policy":     "buffered",
	})
}

// logBufferOverflow logs a buffer overflow error per CONTRACT_POLICY.md.
func (p *BufferedPolicy) logBufferOverflow(eventType types.EventType) {
	if p.logger == nil {
		return
	}
	p.logger.Error("buffer overflow", map[string]any{
		"event_type": string(eventType),
		"policy":     "buffered",
	})
}

// logFlushFailure logs a flush failure per CONTRACT_POLICY.md.
func (p *BufferedPolicy) logFlushFailure(bufferType string, err error) {
	if p.logger == nil {
		return
	}
	p.logger.Error("flush failed", map[string]any{
		"buffer_type": bufferType,
		"error":       err.Error(),
		"policy":      "buffered",
	})
}
