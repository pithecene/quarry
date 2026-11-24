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
//   - Blocking on full: when the next event would exceed the bounds, ingestion
//     flushes and blocks until the flush completes — events are never dropped
//
// On flush failure, the buffer is preserved and retried on the next trigger.
//
// Thread safety:
//   - mu guards buffer state (append, size tracking, stats)
//   - flushSem serializes flush operations to prevent concurrent writes;
//     acquiring it honors the caller's context, so a flush stalled in the
//     sink cannot block another flush past its deadline
//   - IngestEvent holds mu briefly to append
//   - triggerFlush holds flushSem for the duration of the write,
//     and mu briefly to swap/restore buffers
type StreamingPolicy struct {
	sink   Sink
	config StreamingConfig
	logger *log.Logger

	closer onceCloser

	mu          sync.Mutex // guards buffer state and stats
	eventBuffer []*types.EventEnvelope
	bufferBytes int64
	// writing and writingBytes are the batch that a flush holds outside
	// eventBuffer. A failed write returns it, so the bounds count it.
	writing      int
	writingBytes int64
	stats        *statsRecorder

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
		stats:       newStatsRecorder(),
		stopCh:      make(chan struct{}),
		flushSem:    make(chan struct{}, 1),
		loopDone:    make(chan struct{}),
	}
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
// Never drops events. While the buffer has no room it flushes, so ingestion
// blocks on the flush (backpressure per CONTRACT_POLICY.md). A call whose
// context ends before its event is admitted does not buffer it and returns
// an error: the context's, or the flush's when a flush it started fails.
// If count threshold is reached after append, triggers a flush.
func (p *StreamingPolicy) IngestEvent(ctx context.Context, envelope *types.EventEnvelope) error {
	// A closed policy refuses before it inspects the envelope, and the envelope
	// is sized outside the lock, so an invalid one cannot leave mu held.
	p.mu.Lock()
	stopped := p.stopped
	p.mu.Unlock()
	if stopped {
		return ErrPolicyClosed
	}
	eventSize := p.estimateEventSize(envelope)
	p.mu.Lock()
	if err := p.admitLocked(ctx, eventSize); err != nil {
		p.mu.Unlock()
		return err
	}

	p.stats.incTotalEventsLocked()
	p.eventBuffer = append(p.eventBuffer, envelope)
	p.bufferBytes += eventSize
	p.stats.setBufferSizeLocked(p.retainedBytesLocked())

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

// Flush flushes all buffered data (run termination trigger).
// Called on run_complete, run_error, or runtime termination.
func (p *StreamingPolicy) Flush(ctx context.Context) error {
	return p.triggerFlush(ctx, FlushTriggerTermination)
}

// triggerFlush performs a flush with the given trigger reason.
// Serialized by flushSem to prevent concurrent writes.
//
// Strategy: swap the buffer under mu, write outside mu, restore on failure.
// This allows IngestEvent to continue appending to a fresh buffer during a
// write, without blocking on the sink.
func (p *StreamingPolicy) triggerFlush(ctx context.Context, trigger FlushTrigger) error {
	return p.flushUnless(ctx, trigger, func() bool { return false })
}

// flushUnless is triggerFlush, except that it writes nothing when settled, which
// it calls with mu held once no other flush is writing, reports true.
func (p *StreamingPolicy) flushUnless(ctx context.Context, trigger FlushTrigger, settled func() bool) error {
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
		p.logFlushFailure(trigger, ctx.Err())
		return ctx.Err()
	}
	defer func() { <-p.flushSem }()

	// Swap buffers under mu
	p.mu.Lock()
	if settled() {
		p.mu.Unlock()
		return nil
	}

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

	// Nothing to flush
	if len(events) == 0 {
		p.mu.Unlock()
		return nil
	}

	// Install a fresh buffer so ingestion can continue during write
	p.eventBuffer = make([]*types.EventEnvelope, 0, 128)
	p.writing, p.writingBytes = len(events), p.bufferBytes
	p.recalculateBufferBytes()

	p.mu.Unlock()

	if err := markedWrite(ctx, p.sink, events); err != nil {
		// Restore the buffer: prepend old data before any new data
		p.mu.Lock()
		p.stats.incErrorsLocked()
		p.eventBuffer = append(events, p.eventBuffer...)
		p.writing, p.writingBytes = 0, 0
		p.recalculateBufferBytes()
		p.mu.Unlock()
		p.logFlushFailure(trigger, err)
		return err
	}
	p.mu.Lock()
	p.writing, p.writingBytes = 0, 0
	p.stats.setBufferSizeLocked(p.retainedBytesLocked())
	p.stats.incEventsPersistedLocked(int64(len(events)))
	p.mu.Unlock()

	p.logFlush(trigger, len(events))

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
		events := len(p.eventBuffer)
		p.mu.Unlock()
		var lossErr error
		if events > 0 {
			lossErr = fmt.Errorf("%w: %d events not persisted at close: %w", ErrCloseUnflushed, events, flushErr)
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

	s := p.stats.snapshotLocked(p.retainedBytesLocked())
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
			hasData := len(p.eventBuffer) > 0
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

// recalculateBufferBytes recalculates bufferBytes from the buffer. Caller must hold mu.
func (p *StreamingPolicy) recalculateBufferBytes() {
	var total int64
	for _, event := range p.eventBuffer {
		total += p.estimateEventSize(event)
	}
	p.bufferBytes = total
	p.stats.setBufferSizeLocked(p.retainedBytesLocked())
}

// retainedBytesLocked is what the byte bound counts: the buffer and the batch
// being written. Caller must hold mu.
func (p *StreamingPolicy) retainedBytesLocked() int64 {
	return p.bufferBytes + p.writingBytes
}

// hasRoomLocked reports whether an event of eventSize fits the bounds, as the
// only one when it is oversized. Caller must hold mu.
func (p *StreamingPolicy) hasRoomLocked(eventSize int64) bool {
	return p.retainedBytesLocked() == 0 || !p.wouldExceedCapacityLocked(eventSize)
}

// admitLocked returns once the buffer has room for an event of eventSize, with
// mu held, or the reason the event is refused. While there is no room it
// flushes the partial batch and checks again: the capacity trigger after an
// append never fires for an event that does not fit, and one flush need not
// make room, since other calls can refill the buffer while it writes. An
// empty buffer always has room when no flush holds a batch, so an oversized event
// is admitted alone.
func (p *StreamingPolicy) admitLocked(ctx context.Context, eventSize int64) error {
	for {
		if p.stopped {
			return ErrPolicyClosed
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if p.hasRoomLocked(eventSize) {
			return nil
		}
		p.mu.Unlock()
		// A write that this call waits for can make the room.
		err := p.flushUnless(ctx, FlushTriggerCapacity, func() bool { return p.hasRoomLocked(eventSize) })
		p.mu.Lock()
		if err != nil {
			return err
		}
	}
}

// wouldExceedCapacityLocked returns true if adding additionalBytes would exceed
// internal capacity, which the buffer shares with the batch being written.
// Caller must hold mu.
func (p *StreamingPolicy) wouldExceedCapacityLocked(additionalBytes int64) bool {
	if len(p.eventBuffer)+p.writing >= streamingMaxBufferEvents {
		return true
	}
	return p.retainedBytesLocked()+additionalBytes > streamingMaxBufferBytes
}

// isBufferFullLocked returns true if the buffer and the batch being written have
// reached internal capacity.
// Used after an append to decide if a capacity-triggered flush is needed.
// Caller must hold mu.
func (p *StreamingPolicy) isBufferFullLocked() bool {
	if len(p.eventBuffer)+p.writing >= streamingMaxBufferEvents {
		return true
	}
	return p.retainedBytesLocked() >= streamingMaxBufferBytes
}

// --- Logging helpers ---

func (p *StreamingPolicy) logFlush(trigger FlushTrigger, events int) {
	if p.logger == nil {
		return
	}
	p.logger.Info("streaming flush", map[string]any{
		"trigger": string(trigger),
		"events":  events,
		"policy":  "streaming",
	})
}

func (p *StreamingPolicy) logFlushFailure(trigger FlushTrigger, err error) {
	if p.logger == nil {
		return
	}
	p.logger.Error("streaming flush failed", map[string]any{
		"trigger": string(trigger),
		"error":   err.Error(),
		"policy":  "streaming",
	})
}

// Verify StreamingPolicy implements Policy.
var _ Policy = (*StreamingPolicy)(nil)
