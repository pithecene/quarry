package policy

import (
	"context"

	"github.com/pithecene/quarry/types"
)

// CompositeSink implements [Sink] by routing events and chunks to separate
// destinations. Events go to a pluggable [EventSink] (which may fan out to
// multiple backends), while artifact chunks always go to a dedicated chunk
// sink (typically Lode).
//
// This enables configurations where events are persisted to Redis Streams,
// Lode, or both — while artifact chunks always route to durable storage.
type CompositeSink struct {
	events EventSink
	chunks Sink
}

// NewCompositeSink creates a composite that routes events to eventSink and
// chunks to chunkSink. Both are required.
func NewCompositeSink(eventSink EventSink, chunkSink Sink) *CompositeSink {
	if eventSink == nil {
		panic("composite sink requires a non-nil event sink")
	}
	if chunkSink == nil {
		panic("composite sink requires a non-nil chunk sink")
	}
	return &CompositeSink{
		events: eventSink,
		chunks: chunkSink,
	}
}

// WriteEvents delegates to the event sink.
func (c *CompositeSink) WriteEvents(ctx context.Context, events []*types.EventEnvelope) error {
	return c.events.WriteEvents(ctx, events)
}

// WriteChunks delegates to the chunk sink (always Lode).
func (c *CompositeSink) WriteChunks(ctx context.Context, chunks []*types.ArtifactChunk) error {
	return c.chunks.WriteChunks(ctx, chunks)
}

// Close closes both the event sink and the chunk sink.
// Returns the first error encountered.
func (c *CompositeSink) Close() error {
	eventErr := c.events.Close()
	chunkErr := c.chunks.Close()
	if eventErr != nil {
		return eventErr
	}
	return chunkErr
}

// Verify CompositeSink implements Sink.
var _ Sink = (*CompositeSink)(nil)
