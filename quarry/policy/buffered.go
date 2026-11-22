package policy

import (
	"context"
	"errors"
	"fmt"
	"slices"
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

	// flushSem serializes flushes (capacity 1), so two never write the same
	// events; a flush waiting for it gives up when its context ends.
	flushSem chan struct{}

	mu          sync.Mutex // guards buffer state only
	eventBuffer []*types.EventEnvelope
	// inFlight is how many leading eventBuffer entries a flush is writing.
	// Eviction skips them and appends follow them, so on success the flush
	// removes exactly that prefix.
	inFlight    int
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
		flushSem:    make(chan struct{}, 1),
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
	// A canceled context never starts a flush; select alone would pick the
	// free semaphore at random even when the context is already done.
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case p.flushSem <- struct{}{}:
	case <-ctx.Done():
		// Another flush still holds the sink: this one never started.
		p.mu.Lock()
		p.stats.incErrorsLocked()
		p.mu.Unlock()
		p.logFlushFailure(ctx.Err())
		return ctx.Err()
	}
	defer func() { <-p.flushSem }()

	// The flush writes a copy of the buffer and marks it in flight, so eviction
	// leaves it alone and events that arrive during the write stay buffered.
	p.mu.Lock()
	p.stats.incFlushLocked()
	events := slices.Clone(p.eventBuffer)
	p.inFlight = len(events)
	p.mu.Unlock()

	if len(events) > 0 {
		if err := markedWrite(ctx, p.sink, events); err != nil {
			p.mu.Lock()
			p.inFlight = 0
			p.stats.incErrorsLocked()
			p.mu.Unlock()
			p.logFlushFailure(err)
			return err
		}
		p.mu.Lock()
		p.stats.incEventsPersistedLocked(int64(len(events)))
		p.mu.Unlock()
	}

	// Remove only the written prefix, after success. slices.Delete zeroes the
	// vacated slots, so the backing array keeps no written payload.
	p.mu.Lock()
	p.eventBuffer = slices.Delete(p.eventBuffer, 0, p.inFlight)
	p.inFlight = 0
	p.recalculateBufferBytes()
	p.mu.Unlock()

	return nil
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

// dropOldestDroppable removes the oldest droppable event that no flush is
// writing. Returns true if an event was dropped, false if none can be.
// Caller must hold mu.
func (p *BufferedPolicy) dropOldestDroppable() bool {
	for i := p.inFlight; i < len(p.eventBuffer); i++ {
		if event := p.eventBuffer[i]; IsDroppable(event.Type) {
			eventType := event.Type
			eventSize := p.estimateEventSize(event)
			p.eventBuffer = slices.Delete(p.eventBuffer, i, i+1)
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
