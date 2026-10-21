package policy

import (
	"context"

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

// Close closes the underlying sink.
func (p *StrictPolicy) Close() error {
	return p.sink.Close()
}

// Stats returns policy statistics.
func (p *StrictPolicy) Stats() Stats {
	return p.stats.snapshot()
}
