package policy

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/pithecene/quarry/log"
	"github.com/pithecene/quarry/types"
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

	// Logger is an optional logger for policy observability.
	// If nil, no logging is emitted.
	Logger *log.Logger
}

// DefaultBufferedConfig returns sensible defaults for buffered policy.
func DefaultBufferedConfig() BufferedConfig {
	return BufferedConfig{
		MaxBufferEvents: 1000,
		MaxBufferBytes:  10 * 1024 * 1024, // 10 MB
	}
}

// ErrBufferFull is returned when buffer is full and event is non-droppable.
var ErrBufferFull = errors.New("buffer full: cannot accept non-droppable event")

// ErrInvalidConfig is returned when BufferedConfig is invalid.
var ErrInvalidConfig = errors.New("invalid config: at least one of MaxBufferEvents or MaxBufferBytes must be set")

// BufferedPolicy implements buffered persistence with drop rules.
//
// Per CONTRACT_POLICY.md:
//   - Bounded buffer with explicit limits
//   - May drop: log, enqueue, rotate_proxy
//   - Must NOT drop: item, artifact, checkpoint, run_error, run_complete
//   - Batch writes on flush
//   - Flush on run_complete, run_error, runtime termination
//
// A flush writes the buffered events at least once: on failure the buffer
// is kept whole, so a retry may write an event twice but never loses one.
// Events are written in seq order.
type BufferedPolicy struct {
	sink   Sink
	config BufferedConfig
	logger *log.Logger

	closer onceCloser
	closed bool // set under mu when Close begins; ingestion is refused afterward

	mu          sync.Mutex // guards buffer state only
	eventBuffer []*types.EventEnvelope
	bufferBytes int64
	stats       *statsRecorder
}

// NewBufferedPolicy creates a new buffered policy.
// Returns error if config is invalid.
func NewBufferedPolicy(sink Sink, config BufferedConfig) (*BufferedPolicy, error) {
	if config.MaxBufferEvents <= 0 && config.MaxBufferBytes <= 0 {
		return nil, ErrInvalidConfig
	}

	return &BufferedPolicy{
		sink:        sink,
		config:      config,
		logger:      config.Logger,
		eventBuffer: make([]*types.EventEnvelope, 0, max(config.MaxBufferEvents, 100)),
		stats:       newStatsRecorder(),
	}, nil
}

// IngestEvent buffers the event, applying drop rules if buffer is full.
//
// Drop strategy when full:
//   - If incoming event is droppable: drop it, record in stats
//   - If incoming event is non-droppable and buffer has droppable events: drop oldest droppable
//   - If incoming event is non-droppable and no droppable events: return error (fail run)
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

// appendEvent adds an event to the buffer. Caller must hold mu.
func (p *BufferedPolicy) appendEvent(envelope *types.EventEnvelope, eventSize int64) {
	p.eventBuffer = append(p.eventBuffer, envelope)
	p.bufferBytes += eventSize
	p.stats.setBufferSizeLocked(p.bufferBytes)
}

// Flush writes the buffered events to the sink at least once. On failure
// the buffer is kept whole, so a retry prefers a duplicate over a loss.
func (p *BufferedPolicy) Flush(ctx context.Context) error {
	p.mu.Lock()
	p.stats.incFlushLocked()
	events := p.eventBuffer
	p.mu.Unlock()

	if len(events) > 0 {
		if err := p.sink.WriteEvents(ctx, events); err != nil {
			p.mu.Lock()
			p.stats.incErrorsLocked()
			p.mu.Unlock()
			p.logFlushFailure(err)
			return err
		}
		p.mu.Lock()
		p.stats.incEventsPersistedLocked(int64(len(events)))
		p.mu.Unlock()
	}

	// Clear the buffer only after success
	p.mu.Lock()
	p.clearEventBuffer()
	p.recalculateBufferBytes()
	p.mu.Unlock()

	return nil
}

// clearEventBuffer resets the event buffer. Caller must hold mu.
// Call recalculateBufferBytes after the buffer clear.
func (p *BufferedPolicy) clearEventBuffer() {
	p.eventBuffer = make([]*types.EventEnvelope, 0, max(p.config.MaxBufferEvents, 100))
}

// recalculateBufferBytes recalculates bufferBytes from the buffer. Caller must hold mu.
func (p *BufferedPolicy) recalculateBufferBytes() {
	var total int64
	for _, event := range p.eventBuffer {
		total += p.estimateEventSize(event)
	}
	p.bufferBytes = total
	p.stats.setBufferSizeLocked(p.bufferBytes)
}

// Close flushes remaining data and closes the sink.
func (p *BufferedPolicy) Close() error {
	return p.closer.close(func() error {
		// Best-effort flush on close, bounded by a deadline the sink must
		// honor (see Sink); the write is never abandoned.
		ctx, cancel := context.WithTimeout(context.Background(), closeFlushTimeout)
		defer cancel()
		// Refuse ingestion from this point, so nothing is accepted after the final
		// flush takes its snapshot.
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()

		// A failed close-time flush leaves accepted data unpersisted: report it
		// rather than closing without reporting it. (The buffer may also retain
		// data already written and kept for retry, so the flush result, not the
		// buffer length, determines whether any data was lost.)
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
	if p.config.MaxBufferEvents > 0 && len(p.eventBuffer) >= p.config.MaxBufferEvents {
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
// Returns true if an event was dropped, false if no droppable events exist.
// Caller must hold mu.
func (p *BufferedPolicy) dropOldestDroppable() bool {
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
func (p *BufferedPolicy) logFlushFailure(err error) {
	if p.logger == nil {
		return
	}
	p.logger.Error("flush failed", map[string]any{
		"error":  err.Error(),
		"policy": "buffered",
	})
}
