package policy

import (
	"context"
	"errors"
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
//   - flushMu serializes flush operations to prevent concurrent writes
//   - IngestEvent/IngestArtifactChunk hold mu briefly to append
//   - triggerFlush holds flushMu for the duration of the write,
//     and mu briefly to swap/restore buffers
type StreamingPolicy struct {
	sink   Sink
	config StreamingConfig
	logger *log.Logger

	mu          sync.Mutex // guards buffer state and stats
	bufferCond  *sync.Cond // signaled after flush drains buffer; uses mu as locker
	eventBuffer []*types.EventEnvelope
	chunkBuffer []*types.ArtifactChunk
	bufferBytes int64
	stats       *statsRecorder

	// flushMu serializes flush operations.
	// Prevents concurrent flushes from interval goroutine and count trigger.
	flushMu sync.Mutex

	// flushTriggerCounts tracks how many times each trigger type fired.
	// Guarded by mu.
	flushByCount       int64
	flushByInterval    int64
	flushByTermination int64
	flushByCapacity    int64

	// stopCh signals the interval goroutine to stop.
	stopCh chan struct{}
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
	}
	p.bufferCond = sync.NewCond(&p.mu)

	// Start interval flush goroutine if configured
	if config.FlushInterval > 0 {
		go p.intervalLoop()
	}

	return p, nil
}

// IngestEvent adds the event to the buffer.
// Never drops events. Blocks if buffer is at capacity until a flush drains it.
// If count threshold is reached after append, triggers a flush.
func (p *StreamingPolicy) IngestEvent(ctx context.Context, envelope *types.EventEnvelope) error {
	p.mu.Lock()

	eventSize := p.estimateEventSize(envelope)

	// Block until buffer has room for this item (backpressure per CONTRACT_POLICY.md).
	// Allow single-item overflow when buffer is empty to prevent deadlock for oversized entries.
	p.waitForRoomLocked(eventSize)

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

	chunkSize := int64(len(chunk.Data))

	// Block until buffer has room for this chunk (backpressure per CONTRACT_POLICY.md).
	// Allow single-item overflow when buffer is empty to prevent deadlock for oversized chunks.
	p.waitForRoomLocked(chunkSize)

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
// Serialized by flushMu to prevent concurrent writes.
//
// Strategy: swap buffers under mu, write outside mu, restore on failure.
// This allows IngestEvent/IngestArtifactChunk to continue appending to
// fresh buffers during a write, without blocking on the sink.
func (p *StreamingPolicy) triggerFlush(ctx context.Context, trigger FlushTrigger) error {
	p.flushMu.Lock()
	defer p.flushMu.Unlock()

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

// Close stops the interval goroutine and closes the sink.
func (p *StreamingPolicy) Close() error {
	p.mu.Lock()
	if !p.stopped {
		p.stopped = true
		close(p.stopCh)
	}
	p.mu.Unlock()

	// Best-effort flush on close
	_ = p.Flush(context.Background())
	return p.sink.Close()
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
				_ = p.triggerFlush(context.Background(), FlushTriggerInterval)
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
	for p.bufferBytes > 0 && p.wouldExceedCapacityLocked(additionalBytes) {
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
