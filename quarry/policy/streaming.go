package policy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/pithecene/quarry/log"
	"github.com/pithecene/quarry/types"
)

// Internal buffer capacity defaults.
// These are safety bounds, not user-facing config. The buffer rarely fills
// under normal operation because flush triggers drain it periodically.
// If ingestion outpaces the sink, backpressure (blocking) prevents OOM.
const (
	// streamingMaxBufferEvents is the maximum events buffered before backpressure.
	streamingMaxBufferEvents = 50_000
	// streamingMaxBufferBytes is the maximum buffer size in bytes before backpressure.
	streamingMaxBufferBytes = 128 * 1024 * 1024 // 128 MiB
)

// StreamingConfig configures a StreamingPolicy.
type StreamingConfig struct {
	// FlushCount triggers a flush after N events accumulate.
	// Zero means count-based flush is disabled.
	FlushCount int

	// FlushInterval triggers a flush every interval.
	// Zero means interval-based flush is disabled.
	FlushInterval time.Duration

	// Logger is an optional logger for policy observability.
	Logger *log.Logger
}

// FlushTrigger identifies which trigger caused a flush.
type FlushTrigger string

const (
	// FlushTriggerCount indicates a count-threshold flush.
	FlushTriggerCount FlushTrigger = "count"
	// FlushTriggerInterval indicates an interval-based flush.
	FlushTriggerInterval FlushTrigger = "interval"
	// FlushTriggerTermination indicates a run termination flush.
	FlushTriggerTermination FlushTrigger = "termination"
	// FlushTriggerCapacity indicates a buffer-capacity emergency flush.
	// Fired when buffer is at internal capacity after an append, regardless
	// of whether the event count threshold was reached.
	FlushTriggerCapacity FlushTrigger = "capacity"
)

// ErrStreamingInvalidConfig is returned when StreamingConfig is invalid.
var ErrStreamingInvalidConfig = errors.New("invalid streaming config: at least one of FlushCount or FlushInterval must be set")

// StreamingPolicy implements continuous persistence with batched writes.
//
// Per CONTRACT_POLICY.md streaming section:
//   - No drops: all event types are persisted (same guarantee as strict)
//   - Bounded buffer: events accumulate in a bounded in-memory buffer
//   - Periodic flush: buffer flushed to storage when any trigger fires
//   - Blocking on full: if buffer full and no trigger fired, ingestion blocks
//     until next flush completes — events are never dropped
//
// Flush semantics: chunks first, then events (equivalent to chunks_first).
// On flush failure, buffer is preserved and retried on next trigger.
//
// Thread safety:
//   - mu guards buffer state (append, size tracking, stats)
//   - flushSem serializes flush operations to prevent concurrent writes;
//     acquiring it honors the caller's context, so a flush stalled in the
//     sink cannot block another flush past its deadline
//   - IngestEvent/IngestArtifactChunk hold mu briefly to append
//   - triggerFlush holds flushSem for the duration of the write,
//     and mu briefly to swap/restore buffers
type StreamingPolicy struct {
	sink   Sink
	config StreamingConfig
	logger *log.Logger

	closer onceCloser

	mu          sync.Mutex // guards buffer state and stats
	bufferCond  *sync.Cond // signaled after flush drains buffer; uses mu as locker
	eventBuffer []*types.EventEnvelope
	chunkBuffer []*types.ArtifactChunk
	bufferBytes int64
	stats       *statsRecorder

	// flushSem serializes flush operations (capacity 1).
	// Prevents concurrent flushes from interval goroutine and count trigger.
	flushSem chan struct{}

	// flushTriggerCounts tracks how many times each trigger type fired.
	// Guarded by mu.
	flushByCount       int64
	flushByInterval    int64
	flushByTermination int64
	flushByCapacity    int64

	// stopCh signals the interval goroutine to stop.
	stopCh chan struct{}
	// loopCtx is the context of interval flushes; Close cancels it.
	loopCtx    context.Context
	loopCancel context.CancelFunc
	// loopDone is closed when the interval goroutine exits (or never ran).
	loopDone chan struct{}
	// stopped indicates Close has been called. Guarded by mu.
	stopped bool
}

// NewStreamingPolicy creates a new streaming policy.
// Returns error if config is invalid.
func NewStreamingPolicy(sink Sink, config StreamingConfig) (*StreamingPolicy, error) {
	if config.FlushCount <= 0 && config.FlushInterval <= 0 {
		return nil, ErrStreamingInvalidConfig
	}

	p := &StreamingPolicy{
		sink:        sink,
		config:      config,
		logger:      config.Logger,
		eventBuffer: make([]*types.EventEnvelope, 0, 128),
		chunkBuffer: make([]*types.ArtifactChunk, 0),
		stats:       newStatsRecorder(),
		stopCh:      make(chan struct{}),
		flushSem:    make(chan struct{}, 1),
		loopDone:    make(chan struct{}),
	}
	p.bufferCond = sync.NewCond(&p.mu)
	p.loopCtx, p.loopCancel = context.WithCancel(context.Background())

	// Start interval flush goroutine if configured
	if config.FlushInterval > 0 {
		go p.intervalLoop()
	} else {
		close(p.loopDone)
	}

	return p, nil
}

// IngestEvent adds the event to the buffer.
// Never drops events. Blocks if buffer is at capacity until a flush drains it.
// If count threshold is reached after append, triggers a flush.
func (p *StreamingPolicy) IngestEvent(ctx context.Context, envelope *types.EventEnvelope) error {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return ErrPolicyClosed
	}

	eventSize := p.estimateEventSize(envelope)

	// Block until buffer has room for this item (backpressure per CONTRACT_POLICY.md).
	// Allow single-item overflow when buffer is empty to prevent deadlock for oversized entries.
	p.waitForRoomLocked(eventSize)
	if p.stopped { // Close began while this call waited for room
		p.mu.Unlock()
		return ErrPolicyClosed
	}

	p.stats.incTotalEventsLocked()
	p.eventBuffer = append(p.eventBuffer, envelope)
	p.bufferBytes += eventSize
	p.stats.setBufferSizeLocked(p.bufferBytes)

	// Check count trigger first, then capacity trigger as safety valve.
	// Capacity flush prevents deadlock when flush_count > streamingMaxBufferEvents
	// or when no interval is configured and the buffer fills from events alone.
	countTriggered := p.config.FlushCount > 0 && len(p.eventBuffer) >= p.config.FlushCount
	capacityTriggered := !countTriggered && p.isBufferFullLocked()
	p.mu.Unlock()

	if countTriggered {
		return p.triggerFlush(ctx, FlushTriggerCount)
	}
	if capacityTriggered {
		return p.triggerFlush(ctx, FlushTriggerCapacity)
	}

	return nil
}

// IngestArtifactChunk adds the chunk to the buffer.
// Artifact chunks are never dropped. Blocks if buffer is at capacity.
// Triggers a flush if buffer is at capacity after append, preventing deadlock
// in count-only mode where chunks don't contribute to the event count trigger.
func (p *StreamingPolicy) IngestArtifactChunk(ctx context.Context, chunk *types.ArtifactChunk) error {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return ErrPolicyClosed
	}

	chunkSize := int64(len(chunk.Data))

	// Block until buffer has room for this chunk (backpressure per CONTRACT_POLICY.md).
	// Allow single-item overflow when buffer is empty to prevent deadlock for oversized chunks.
	p.waitForRoomLocked(chunkSize)
	if p.stopped { // Close began while this call waited for room
		p.mu.Unlock()
		return ErrPolicyClosed
	}

	p.stats.incTotalChunksLocked()
	p.chunkBuffer = append(p.chunkBuffer, chunk)
	p.bufferBytes += chunkSize
	p.stats.setBufferSizeLocked(p.bufferBytes)

	// Trigger capacity flush if buffer is at capacity after this append.
	// This prevents deadlock in count-only mode: without an interval trigger,
	// chunks alone would fill the buffer and block forever since only events
	// contribute to the count threshold.
	shouldFlush := p.isBufferFullLocked()
	p.mu.Unlock()

	if shouldFlush {
		return p.triggerFlush(ctx, FlushTriggerCapacity)
	}

	return nil
}

// Flush flushes all buffered data (run termination trigger).
// Called on run_complete, run_error, or runtime termination.
func (p *StreamingPolicy) Flush(ctx context.Context) error {
	return p.triggerFlush(ctx, FlushTriggerTermination)
}

// triggerFlush performs a flush with the given trigger reason.
// Serialized by flushSem to prevent concurrent writes.
//
// Strategy: swap buffers under mu, write outside mu, restore on failure.
// This allows IngestEvent/IngestArtifactChunk to continue appending to
// fresh buffers during a write, without blocking on the sink.
func (p *StreamingPolicy) triggerFlush(ctx context.Context, trigger FlushTrigger) error {
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
		p.logFlushFailure("all", trigger, ctx.Err())
		return ctx.Err()
	}
	defer func() { <-p.flushSem }()

	// Swap buffers under mu
	p.mu.Lock()

	// Record trigger type
	switch trigger {
	case FlushTriggerCount:
		p.flushByCount++
	case FlushTriggerInterval:
		p.flushByInterval++
	case FlushTriggerTermination:
		p.flushByTermination++
	case FlushTriggerCapacity:
		p.flushByCapacity++
	}

	p.stats.incFlushLocked()

	events := p.eventBuffer
	chunks := p.chunkBuffer

	// Nothing to flush
	if len(events) == 0 && len(chunks) == 0 {
		p.mu.Unlock()
		return nil
	}

	// Install fresh buffers so ingestion can continue during write
	p.eventBuffer = make([]*types.EventEnvelope, 0, 128)
	p.chunkBuffer = make([]*types.ArtifactChunk, 0)
	p.recalculateBufferBytes()

	// Wake blocked ingestors — buffer has room now
	p.bufferCond.Broadcast()

	p.mu.Unlock()

	// Write chunks first (chunks_first semantics per CONTRACT_POLICY.md)
	if len(chunks) > 0 {
		if err := p.sink.WriteChunks(ctx, chunks); err != nil {
			// Restore both buffers: prepend old data before any new data
			p.mu.Lock()
			p.stats.incErrorsLocked()
			p.eventBuffer = append(events, p.eventBuffer...)
			p.chunkBuffer = append(chunks, p.chunkBuffer...)
			p.recalculateBufferBytes()
			p.mu.Unlock()
			p.logFlushFailure("chunks", trigger, err)
			return err
		}
		p.mu.Lock()
		p.stats.incChunksPersistedLocked(int64(len(chunks)))
		p.mu.Unlock()
	}

	// Write events
	if len(events) > 0 {
		if err := p.sink.WriteEvents(ctx, events); err != nil {
			// Chunks succeeded; restore only events
			p.mu.Lock()
			p.stats.incErrorsLocked()
			p.eventBuffer = append(events, p.eventBuffer...)
			p.recalculateBufferBytes()
			p.mu.Unlock()
			p.logFlushFailure("events", trigger, err)
			return err
		}
		p.mu.Lock()
		p.stats.incEventsPersistedLocked(int64(len(events)))
		p.mu.Unlock()
	}

	p.logFlush(trigger, len(events), len(chunks))

	return nil
}

// Close stops and joins the interval goroutine, flushes once more, and
// closes the sink. An interval flush in progress is canceled; every flush
// in progress is joined before the sink closes, so no write of this policy
// outlives Close. The final flush is bounded by closeFlushTimeout for sinks
// that honor cancellation (see Sink).
func (p *StreamingPolicy) Close() error {
	return p.closer.close(func() error {
		p.mu.Lock()
		if !p.stopped {
			p.stopped = true
			close(p.stopCh)
		}
		// Wake ingestion waiting for buffer room: it now refuses the data.
		p.bufferCond.Broadcast()
		p.mu.Unlock()
		p.loopCancel()

		<-p.loopDone

		ctx, cancel := context.WithTimeout(context.Background(), closeFlushTimeout)
		defer cancel()
		flushErr := p.Flush(ctx)

		// Join any flush still in progress (count-, capacity-, or caller-
		// triggered) before closing the sink: none is abandoned.
		p.flushSem <- struct{}{}
		defer func() { <-p.flushSem }()

		// Data still buffered now was accepted but never persisted: report it
		// rather than closing without reporting it.
		p.mu.Lock()
		events, chunks := len(p.eventBuffer), len(p.chunkBuffer)
		p.mu.Unlock()
		var lossErr error
		if events > 0 || chunks > 0 {
			lossErr = fmt.Errorf("%w: %d events and %d chunks not persisted at close: %w", ErrCloseUnflushed, events, chunks, flushErr)
		}
		return errors.Join(lossErr, p.sink.Close())
	})
}

// Stats returns policy statistics.
// Returns an atomic snapshot: the buffer mutex is held while taking the
// snapshot, ensuring all counters and buffer size are consistent.
// Includes FlushTriggers per CONTRACT_POLICY.md streaming observability.
func (p *StreamingPolicy) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()

	s := p.stats.snapshotLocked(p.bufferBytes)
	s.FlushTriggers = map[string]int64{
		string(FlushTriggerCount):       p.flushByCount,
		string(FlushTriggerInterval):    p.flushByInterval,
		string(FlushTriggerTermination): p.flushByTermination,
		string(FlushTriggerCapacity):    p.flushByCapacity,
	}
	return s
}

// FlushTriggerStats returns per-trigger flush counts for observability.
// These are additive to the base Stats per CONTRACT_POLICY.md streaming section.
func (p *StreamingPolicy) FlushTriggerStats() map[FlushTrigger]int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	return map[FlushTrigger]int64{
		FlushTriggerCount:       p.flushByCount,
		FlushTriggerInterval:    p.flushByInterval,
		FlushTriggerTermination: p.flushByTermination,
		FlushTriggerCapacity:    p.flushByCapacity,
	}
}

// intervalLoop runs in a goroutine and triggers flushes on the configured interval.
func (p *StreamingPolicy) intervalLoop() {
	defer close(p.loopDone)
	ticker := time.NewTicker(p.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.mu.Lock()
			hasData := len(p.eventBuffer) > 0 || len(p.chunkBuffer) > 0
			p.mu.Unlock()

			if hasData {
				// Best-effort interval flush — errors logged but not fatal
				_ = p.triggerFlush(p.loopCtx, FlushTriggerInterval)
			}
		case <-p.stopCh:
			return
		}
	}
}

// estimateEventSize delegates to the package-level estimateEventSize.
func (p *StreamingPolicy) estimateEventSize(envelope *types.EventEnvelope) int64 {
	return estimateEventSize(envelope)
}

// recalculateBufferBytes recalculates bufferBytes from all buffers. Caller must hold mu.
func (p *StreamingPolicy) recalculateBufferBytes() {
	var total int64
	for _, event := range p.eventBuffer {
		total += p.estimateEventSize(event)
	}
	for _, chunk := range p.chunkBuffer {
		total += int64(len(chunk.Data))
	}
	p.bufferBytes = total
	p.stats.setBufferSizeLocked(p.bufferBytes)
}

// waitForRoomLocked blocks until the buffer can accept an item of additionalBytes.
// If the buffer is non-empty and would exceed capacity with the new item, blocks
// on bufferCond until a flush drains enough space.
// If the buffer is empty, always proceeds — a single oversized item is accepted
// rather than deadlocking (it will be flushed on the next trigger).
// Caller must hold mu.
func (p *StreamingPolicy) waitForRoomLocked(additionalBytes int64) {
	for !p.stopped && p.bufferBytes > 0 && p.wouldExceedCapacityLocked(additionalBytes) {
		p.bufferCond.Wait()
	}
}

// wouldExceedCapacityLocked returns true if adding additionalBytes would exceed
// internal buffer capacity.
// Caller must hold mu.
func (p *StreamingPolicy) wouldExceedCapacityLocked(additionalBytes int64) bool {
	if len(p.eventBuffer)+len(p.chunkBuffer) >= streamingMaxBufferEvents {
		return true
	}
	return p.bufferBytes+additionalBytes > streamingMaxBufferBytes
}

// isBufferFullLocked returns true if the buffer has reached internal capacity.
// Used after an append to decide if a capacity-triggered flush is needed.
// Caller must hold mu.
func (p *StreamingPolicy) isBufferFullLocked() bool {
	if len(p.eventBuffer)+len(p.chunkBuffer) >= streamingMaxBufferEvents {
		return true
	}
	return p.bufferBytes >= streamingMaxBufferBytes
}

// --- Logging helpers ---

func (p *StreamingPolicy) logFlush(trigger FlushTrigger, events, chunks int) {
	if p.logger == nil {
		return
	}
	p.logger.Info("streaming flush", map[string]any{
		"trigger": string(trigger),
		"events":  events,
		"chunks":  chunks,
		"policy":  "streaming",
	})
}

func (p *StreamingPolicy) logFlushFailure(bufferType string, trigger FlushTrigger, err error) {
	if p.logger == nil {
		return
	}
	p.logger.Error("streaming flush failed", map[string]any{
		"buffer_type": bufferType,
		"trigger":     string(trigger),
		"error":       err.Error(),
		"policy":      "streaming",
	})
}

// Verify StreamingPolicy implements Policy.
var _ Policy = (*StreamingPolicy)(nil)
