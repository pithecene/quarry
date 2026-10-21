package policy

import (
	"context"
	"sync"

	"github.com/pithecene/quarry/types"
)

// NoopPolicy is a no-op policy for testing.
// Accepts all events but does not actually persist them.
//
// Per CONTRACT_POLICY.md, stats reflect droppable vs non-droppable semantics:
//   - Droppable events (log, enqueue, rotate_proxy) are counted as dropped
//   - Non-droppable events (item, artifact, checkpoint, run_error, run_complete)
//     are counted as persisted (even though noop doesn't actually persist)
type NoopPolicy struct {
	mu    sync.Mutex
	stats Stats
}

// NewNoopPolicy creates a new no-op policy.
func NewNoopPolicy() *NoopPolicy {
	return &NoopPolicy{
		stats: Stats{
			DroppedByType: make(map[types.EventType]int64),
		},
	}
}

// IngestEvent accepts the event but does not persist it.
// Per CONTRACT_POLICY.md, only droppable events are counted as dropped.
func (p *NoopPolicy) IngestEvent(_ context.Context, envelope *types.EventEnvelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.stats.TotalEvents++

	if IsDroppable(envelope.Type) {
		// Droppable events are dropped (not persisted)
		p.stats.EventsDropped++
		p.stats.DroppedByType[envelope.Type]++
	} else {
		// Non-droppable events are "persisted" (even though noop doesn't actually persist)
		// This maintains correct stats semantics per CONTRACT_POLICY.md
		p.stats.EventsPersisted++
	}

	return nil
}

// IngestArtifactChunk accepts the chunk but does not persist it.
func (p *NoopPolicy) IngestArtifactChunk(_ context.Context, _ *types.ArtifactChunk) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.stats.TotalChunks++

	return nil
}

// Flush is a no-op.
func (p *NoopPolicy) Flush(_ context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.stats.FlushCount++

	return nil
}

// Close is a no-op.
func (p *NoopPolicy) Close() error {
	return nil
}

// Stats returns the policy statistics.
func (p *NoopPolicy) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Return a copy with a copied map
	stats := p.stats
	stats.DroppedByType = make(map[types.EventType]int64, len(p.stats.DroppedByType))
	for k, v := range p.stats.DroppedByType {
		stats.DroppedByType[k] = v
	}

	return stats
}
