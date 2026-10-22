package policy

import (
	"context"
	"sync/atomic"

	"github.com/pithecene/quarry/types"
)

// StrictPolicy implements synchronous, unbuffered persistence.
//
// Per CONTRACT_POLICY.md:
//   - No buffering: each event/chunk is written immediately
//   - No drops: all events are persisted
//   - Backpressure: caller blocks on sink latency
//   - Sink errors fail the run
type StrictPolicy struct {
	sink Sink

	closer onceCloser
	closed atomic.Bool // ingestion after Close is refused

	stats *statsRecorder
}

// NewStrictPolicy creates a new strict policy writing to the given sink.
func NewStrictPolicy(sink Sink) *StrictPolicy {
	return &StrictPolicy{
		sink:  sink,
		stats: newStatsRecorder(),
	}
}

// IngestEvent writes the event immediately to the sink.
// Returns error on sink failure (terminates run).
func (p *StrictPolicy) IngestEvent(ctx context.Context, envelope *types.EventEnvelope) error {
	if p.closed.Load() {
		return ErrPolicyClosed
	}
	p.stats.incTotalEvents()

	// Write immediately (batch of 1)
	if err := p.sink.WriteEvents(ctx, []*types.EventEnvelope{envelope}); err != nil {
		p.stats.incErrors()
		return err
	}

	p.stats.incEventsPersisted(1)

	return nil
}

// IngestArtifactChunk writes the chunk immediately to the sink.
// Returns error on sink failure (terminates run).
func (p *StrictPolicy) IngestArtifactChunk(ctx context.Context, chunk *types.ArtifactChunk) error {
	if p.closed.Load() {
		return ErrPolicyClosed
	}
	p.stats.incTotalChunks()

	// Write immediately (batch of 1)
	if err := p.sink.WriteChunks(ctx, []*types.ArtifactChunk{chunk}); err != nil {
		p.stats.incErrors()
		return err
	}

	p.stats.incChunksPersisted(1)

	return nil
}

// Flush is a no-op for strict policy (nothing is buffered).
func (p *StrictPolicy) Flush(_ context.Context) error {
	p.stats.incFlush()
	return nil
}

// Close refuses further ingestion and closes the underlying sink. See the
// Policy.Close precondition: the caller has no ingestion in progress.
func (p *StrictPolicy) Close() error {
	return p.closer.close(func() error {
		p.closed.Store(true)
		return p.sink.Close()
	})
}

// Stats returns policy statistics.
func (p *StrictPolicy) Stats() Stats {
	return p.stats.snapshot()
}
