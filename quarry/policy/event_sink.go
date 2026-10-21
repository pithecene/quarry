package policy

import (
	"context"

	"github.com/pithecene/quarry/types"
)

// DeliveryMode controls how sink write failures are handled.
type DeliveryMode int

const (
	// DeliveryMandatory means write failures propagate to the policy.
	// A failed mandatory sink fails the overall write.
	DeliveryMandatory DeliveryMode = iota

	// DeliveryBestEffort means write failures are logged but not propagated.
	// A failed best-effort sink does not affect other sinks or the policy.
	DeliveryBestEffort
)

// EventSink abstracts event persistence without artifact chunk handling.
// Implementations write events to a backing store (Lode, Redis Streams, etc.).
//
// Unlike [Sink], EventSink does not handle artifact chunks — those always
// route to Lode regardless of event sink configuration.
type EventSink interface {
	// WriteEvents persists a batch of event envelopes.
	// Must preserve ordering within the batch.
	WriteEvents(ctx context.Context, events []*types.EventEnvelope) error

	// Close releases any resources held by the sink.
	Close() error
}
