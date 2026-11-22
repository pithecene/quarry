// Package policy defines the ingestion policy interface per CONTRACT_POLICY.md.
package policy

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"

	"github.com/pithecene/quarry/types"
)

// ErrSinkWrite marks an error that a policy received from its sink's
// WriteEvents. The runtime reports a run that fails with it as
// storage_error, which a retry may resolve, and not as policy_failure.
var ErrSinkWrite = errors.New("sink write failed")

// ErrWriteCanceled marks a sink write that failed because the run's context
// ended: the run's own cancellation or deadline, not a store's refusal. The
// runtime reports such a run as cancelled.
var ErrWriteCanceled = errors.New("sink write ended with the run")

// runContextKey is the context key under which WithRunContext records the run.
type runContextKey struct{}

// runRecord is what WithRunContext records: the run's context. A nil record,
// from WithoutRunContext, records that no run governs the context.
type runRecord struct {
	run context.Context
}

// WithRunContext returns ctx recorded as a run's context. A sink write under
// it, or under a context derived from it, that fails because the run ended is
// marked ErrWriteCanceled. Every other failed write is a store's refusal.
func WithRunContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, runContextKey{}, &runRecord{run: ctx})
}

// WithoutRunContext returns ctx with no run recorded, for work that outlives
// the run under a deadline of its own, such as a detached termination flush:
// a write under it that fails is a store's refusal, whatever ended the run.
func WithoutRunContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, runContextKey{}, (*runRecord)(nil))
}

// writeFailure marks err, a failed sink write under ctx, with the reason the
// runtime reports. Callers mark the failure as soon as the write returns, while
// ctx still says what ended it: ErrWriteCanceled when the run ended it,
// ErrSinkWrite otherwise. An error already marked keeps its marks.
func writeFailure(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, ErrSinkWrite) || errors.Is(err, ErrWriteCanceled):
		return err
	case endedWithRun(ctx, err):
		return fmt.Errorf("%w: %w", ErrWriteCanceled, err)
	default:
		return fmt.Errorf("%w: %w", ErrSinkWrite, err)
	}
}

// endedWithRun reports whether the run that ctx records ended the write: ctx
// records a run, so it is attached to it and not detached with a deadline of
// its own, the run and ctx have both ended, and every branch of err shows that
// end (ctx's error or cause, or the run's).
func endedWithRun(ctx context.Context, err error) bool {
	record, _ := ctx.Value(runContextKey{}).(*runRecord)
	if record == nil || record.run.Err() == nil || ctx.Err() == nil {
		return false
	}
	// ctx ended with the run's cause, so the run's end ended it, and not a
	// deadline of ctx's own that passed first.
	if !causeMatches(context.Cause(ctx), context.Cause(record.run)) { //nolint:contextcheck // reads the recorded run's cause; starts no work under it
		return false
	}
	return showsOnly(err, []error{ctx.Err(), context.Cause(ctx)})
}

// showsOnly reports whether every branch of err's tree shows one of ends. Each
// node is matched before it is unwrapped, so an end given as a joined error
// still matches, and a join shows an end only when each of its branches does.
func showsOnly(err error, ends []error) bool {
	if slices.ContainsFunc(ends, func(end error) bool { return isEnd(err, end) }) {
		return true
	}
	switch x := err.(type) { //nolint:errorlint // the walk inspects each node itself
	case interface{ Unwrap() []error }:
		children := x.Unwrap()
		if len(children) == 0 {
			return false
		}
		return !slices.ContainsFunc(children, func(c error) bool { return !showsOnly(c, ends) })
	case interface{ Unwrap() error }:
		if inner := x.Unwrap(); inner != nil {
			return showsOnly(inner, ends)
		}
	}
	return false
}

// causeMatches reports whether a and b are the same cause: the same value, or
// for a cause that cannot be compared, a deeply equal one, as a value child
// reports its run's cause.
func causeMatches(a, b error) bool {
	return sameCause(a, b) || (!isComparableTarget(a) && a != nil && reflect.DeepEqual(a, b))
}

// isEnd reports whether the node err itself is end: the same value, or a node
// whose own Is method claims it. It does not unwrap err.
func isEnd(err, end error) bool {
	if sameCause(err, end) {
		return true
	}
	// A node that joins several errors shows an end only through each branch,
	// so its own Is method cannot vouch for a sibling refusal.
	if isJoin(err) {
		return false
	}
	matcher, ok := err.(interface{ Is(error) bool })
	return ok && isComparableTarget(end) && matcher.Is(end)
}

// isJoin reports whether err directly joins at least one error. A multi-error
// with no children has no branch to show an end, so it is matched as a leaf.
func isJoin(err error) bool {
	join, ok := err.(interface{ Unwrap() []error })
	return ok && len(join.Unwrap()) > 0
}

// sameCause reports whether a and b are the same error value, without
// panicking on one that cannot be compared.
func sameCause(a, b error) bool {
	return isComparableTarget(a) && isComparableTarget(b) && reflect.TypeOf(a) == reflect.TypeOf(b) && a == b //nolint:errorlint // identity of the node is the point
}

// isComparableTarget reports whether errors.Is can compare target without
// panicking: a host can cancel with an error of a comparable type whose value
// holds a slice.
func isComparableTarget(target error) bool {
	return target != nil && reflect.ValueOf(target).Comparable()
}

// Sink abstracts persistence for policies.
// Implementations may write to storage, forward to a queue, or stub for testing.
//
// Methods are batch-oriented to support both strict (batch of 1) and buffered policies.
//
// Write methods must return once ctx is done. Policies bound their close-time
// flush with a context deadline and never abandon a write in progress (it
// could otherwise be persisted after the caller considers the policy closed), so that
// bound holds only for sinks that honor cancellation.
type Sink interface {
	// WriteEvents persists a batch of event envelopes.
	// Must preserve ordering within the batch.
	// Returns error on failure; caller decides whether to retry or fail.
	WriteEvents(ctx context.Context, events []*types.EventEnvelope) error

	// Close releases any resources held by the sink.
	Close() error
}

// WriteOp represents a write operation for ordering verification.
type WriteOp struct {
	Events []*types.EventEnvelope
}

// StubSink is a test sink that accepts writes without persisting.
// Tracks write statistics for test assertions.
type StubSink struct {
	mu sync.Mutex

	// EventsWritten is the total count of events written.
	EventsWritten int64
	// EventBatches is the number of WriteEvents calls.
	EventBatches int64
	// Closed indicates whether Close was called.
	Closed bool

	// WrittenEvents stores all written events for inspection.
	WrittenEvents []*types.EventEnvelope

	// WriteOrder tracks the order of write operations for ordering tests.
	WriteOrder []WriteOp

	// ErrorOnWrite, if non-nil, is returned by WriteEvents.
	ErrorOnWrite error
}

// NewStubSink creates a new stub sink for testing.
func NewStubSink() *StubSink {
	return &StubSink{
		WrittenEvents: make([]*types.EventEnvelope, 0),
		WriteOrder:    make([]WriteOp, 0),
	}
}

// WriteEvents records the events without persisting.
func (s *StubSink) WriteEvents(_ context.Context, events []*types.EventEnvelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ErrorOnWrite != nil {
		return s.ErrorOnWrite
	}

	s.EventBatches++
	s.EventsWritten += int64(len(events))
	s.WrittenEvents = append(s.WrittenEvents, events...)
	s.WriteOrder = append(s.WriteOrder, WriteOp{Events: events})

	return nil
}

// Close marks the sink as closed.
func (s *StubSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Closed = true
	return nil
}

// Stats returns a snapshot of sink statistics.
func (s *StubSink) Stats() StubSinkStats {
	s.mu.Lock()
	defer s.mu.Unlock()

	return StubSinkStats{
		EventsWritten: s.EventsWritten,
		EventBatches:  s.EventBatches,
		Closed:        s.Closed,
	}
}

// StubSinkStats is a snapshot of StubSink statistics.
type StubSinkStats struct {
	EventsWritten int64
	EventBatches  int64
	Closed        bool
}
