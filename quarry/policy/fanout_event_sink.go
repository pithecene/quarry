package policy

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/pithecene/quarry/types"
)

// SinkEntry pairs an EventSink with its delivery mode.
type SinkEntry struct {
	Sink     EventSink
	Delivery DeliveryMode
	// Label identifies this sink in log messages (e.g. "lode", "redisstream").
	Label string
}

// classifyError returns nil if the error should be swallowed (best-effort),
// or the original error if it should propagate (mandatory).
// Best-effort failures are logged with the sink label.
func (e SinkEntry) classifyError(err error) error {
	if err == nil {
		return nil
	}
	if e.Delivery == DeliveryBestEffort {
		log.Printf("Warning: best-effort event sink %q: %v", e.Label, err)
		return nil
	}
	return err
}

// FanoutEventSink dispatches WriteEvents to multiple EventSink implementations
// concurrently. Delivery mode per sink controls error propagation:
//   - DeliveryMandatory: write failure propagates to the caller.
//   - DeliveryBestEffort: write failure is logged, not propagated.
//
// If multiple mandatory sinks fail, all errors are joined and returned.
type FanoutEventSink struct {
	sinks []SinkEntry
}

// NewFanoutEventSink creates a fanout dispatcher for the given sink entries.
// Panics if sinks is empty — at least one event sink is required.
func NewFanoutEventSink(sinks []SinkEntry) *FanoutEventSink {
	if len(sinks) == 0 {
		panic("fanout event sink requires at least one sink entry")
	}
	entries := make([]SinkEntry, len(sinks))
	copy(entries, sinks)
	return &FanoutEventSink{sinks: entries}
}

// WriteEvents dispatches events to all sinks concurrently.
// Every mandatory sink failure stays matchable with errors.Is in the returned
// error. Best-effort failures are logged.
func (f *FanoutEventSink) WriteEvents(ctx context.Context, events []*types.EventEnvelope) error {
	if len(f.sinks) == 1 {
		return f.writeSingle(ctx, events)
	}
	return f.writeFanout(ctx, events)
}

// eventWriter is what markedWrite writes to: a policy's Sink or an EventSink.
type eventWriter interface {
	WriteEvents(ctx context.Context, events []*types.EventEnvelope) error
}

// markedWrite writes events to sink and marks a failure as soon as the write
// returns, before the fan-out waits for its other sinks, while ctx still says
// what ended this write.
func markedWrite(ctx context.Context, sink eventWriter, events []*types.EventEnvelope) error {
	if err := sink.WriteEvents(ctx, events); err != nil {
		return writeFailure(ctx, err)
	}
	return nil
}

// writeSingle is the fast path for a single sink — no goroutines.
func (f *FanoutEventSink) writeSingle(ctx context.Context, events []*types.EventEnvelope) error {
	entry := f.sinks[0]
	return entry.classifyError(markedWrite(ctx, entry.Sink, events))
}

// writeFanout dispatches to multiple sinks concurrently.
func (f *FanoutEventSink) writeFanout(ctx context.Context, events []*types.EventEnvelope) error {
	type result struct {
		idx int
		err error
	}

	var wg sync.WaitGroup
	results := make([]result, len(f.sinks))

	for i, entry := range f.sinks {
		wg.Add(1)
		go func(idx int, e SinkEntry) {
			defer wg.Done()
			results[idx] = result{idx: idx, err: markedWrite(ctx, e.Sink, events)}
		}(i, entry)
	}
	wg.Wait()

	// Each mandatory error stays in the chain, so errors.Is finds a sentinel
	// such as a Lode conflict behind the fan-out, and each is marked on its own,
	// so one sink's refusal stays a refusal beside another's cancellation.
	var mandatoryErrs []error
	for i, r := range results {
		if classified := f.sinks[i].classifyError(r.err); classified != nil {
			mandatoryErrs = append(mandatoryErrs, fmt.Errorf("%s: %w", f.sinks[i].Label, classified))
		}
	}

	switch len(mandatoryErrs) {
	case 0:
		return nil
	case 1:
		return fmt.Errorf("event sink write failed: %w", mandatoryErrs[0])
	default:
		return fmt.Errorf("event sink writes failed: %w", errors.Join(mandatoryErrs...))
	}
}

// Close closes all sinks and returns every close error, joined.
func (f *FanoutEventSink) Close() error {
	var errs []error
	for _, entry := range f.sinks {
		if err := entry.Sink.Close(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", entry.Label, err))
		}
	}
	return errors.Join(errs...)
}

// Verify FanoutEventSink implements EventSink.
var _ EventSink = (*FanoutEventSink)(nil)
