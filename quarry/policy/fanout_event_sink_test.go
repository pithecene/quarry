package policy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/pithecene/quarry/types"
)

// stubEventSink is a test double for EventSink.
type stubEventSink struct {
	mu            sync.Mutex
	eventsWritten int
	closed        bool
	writeErr      error
}

func (s *stubEventSink) WriteEvents(_ context.Context, events []*types.EventEnvelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writeErr != nil {
		return s.writeErr
	}
	s.eventsWritten += len(events)
	return nil
}

func (s *stubEventSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// closerFailSink is a test double whose Close returns an error.
type closerFailSink struct {
	stubEventSink
	closeErr error
}

func (s *closerFailSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return s.closeErr
}

func testEnvelopes(n int) []*types.EventEnvelope {
	out := make([]*types.EventEnvelope, n)
	for i := range n {
		out[i] = &types.EventEnvelope{
			RunID: "run-001",
			Seq:   int64(i + 1),
			Type:  types.EventTypeItem,
		}
	}
	return out
}

func TestFanout_SingleSink(t *testing.T) {
	t.Parallel()

	sink := &stubEventSink{}
	fanout := NewFanoutEventSink([]SinkEntry{
		{Sink: sink, Delivery: DeliveryMandatory, Label: "test"},
	})

	events := testEnvelopes(3)
	if err := fanout.WriteEvents(t.Context(), events); err != nil {
		t.Fatalf("write: %v", err)
	}
	if sink.eventsWritten != 3 {
		t.Errorf("expected 3 events, got %d", sink.eventsWritten)
	}
}

func TestFanout_MultipleSinks(t *testing.T) {
	t.Parallel()

	s1 := &stubEventSink{}
	s2 := &stubEventSink{}
	fanout := NewFanoutEventSink([]SinkEntry{
		{Sink: s1, Delivery: DeliveryMandatory, Label: "s1"},
		{Sink: s2, Delivery: DeliveryMandatory, Label: "s2"},
	})

	events := testEnvelopes(2)
	if err := fanout.WriteEvents(t.Context(), events); err != nil {
		t.Fatalf("write: %v", err)
	}
	if s1.eventsWritten != 2 {
		t.Errorf("s1: expected 2 events, got %d", s1.eventsWritten)
	}
	if s2.eventsWritten != 2 {
		t.Errorf("s2: expected 2 events, got %d", s2.eventsWritten)
	}
}

func TestFanout_MandatoryFailurePropagates(t *testing.T) {
	t.Parallel()

	s1 := &stubEventSink{writeErr: errors.New("disk full")}
	s2 := &stubEventSink{}
	fanout := NewFanoutEventSink([]SinkEntry{
		{Sink: s1, Delivery: DeliveryMandatory, Label: "s1"},
		{Sink: s2, Delivery: DeliveryMandatory, Label: "s2"},
	})

	err := fanout.WriteEvents(t.Context(), testEnvelopes(1))
	if err == nil {
		t.Fatal("expected error from mandatory sink failure")
	}
	// s2 should still have received the events
	if s2.eventsWritten != 1 {
		t.Errorf("s2: expected 1 event, got %d", s2.eventsWritten)
	}
}

func TestFanout_BestEffortFailureSwallowed(t *testing.T) {
	t.Parallel()

	s1 := &stubEventSink{}
	s2 := &stubEventSink{writeErr: errors.New("redis down")}
	fanout := NewFanoutEventSink([]SinkEntry{
		{Sink: s1, Delivery: DeliveryMandatory, Label: "lode"},
		{Sink: s2, Delivery: DeliveryBestEffort, Label: "redis"},
	})

	err := fanout.WriteEvents(t.Context(), testEnvelopes(1))
	if err != nil {
		t.Fatalf("expected no error (best-effort failure should be swallowed): %v", err)
	}
	if s1.eventsWritten != 1 {
		t.Errorf("s1: expected 1 event, got %d", s1.eventsWritten)
	}
}

func TestFanout_AllBestEffortFailures(t *testing.T) {
	t.Parallel()

	s1 := &stubEventSink{writeErr: errors.New("fail 1")}
	s2 := &stubEventSink{writeErr: errors.New("fail 2")}
	fanout := NewFanoutEventSink([]SinkEntry{
		{Sink: s1, Delivery: DeliveryBestEffort, Label: "s1"},
		{Sink: s2, Delivery: DeliveryBestEffort, Label: "s2"},
	})

	// All best-effort failures are swallowed
	err := fanout.WriteEvents(t.Context(), testEnvelopes(1))
	if err != nil {
		t.Fatalf("expected no error for all best-effort: %v", err)
	}
}

func TestFanout_CloseAll(t *testing.T) {
	t.Parallel()

	s1 := &stubEventSink{}
	s2 := &stubEventSink{}
	fanout := NewFanoutEventSink([]SinkEntry{
		{Sink: s1, Delivery: DeliveryMandatory, Label: "s1"},
		{Sink: s2, Delivery: DeliveryBestEffort, Label: "s2"},
	})

	if err := fanout.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !s1.closed {
		t.Error("s1 not closed")
	}
	if !s2.closed {
		t.Error("s2 not closed")
	}
}

func TestFanout_ClosePartialFailure(t *testing.T) {
	t.Parallel()

	s1 := &stubEventSink{} // close succeeds
	s2 := &closerFailSink{closeErr: errors.New("close failed")}
	s3 := &stubEventSink{} // should still be closed

	fanout := NewFanoutEventSink([]SinkEntry{
		{Sink: s1, Delivery: DeliveryMandatory, Label: "s1"},
		{Sink: s2, Delivery: DeliveryMandatory, Label: "s2"},
		{Sink: s3, Delivery: DeliveryMandatory, Label: "s3"},
	})

	err := fanout.Close()
	if err == nil {
		t.Fatal("expected error from partial close failure")
	}
	if !s1.closed {
		t.Error("s1 should still be closed")
	}
	if !s3.closed {
		t.Error("s3 should still be closed despite s2 failure")
	}
}

func TestFanout_BestEffortDoesNotFailPolicy(t *testing.T) {
	t.Parallel()

	// Simulate: Lode mandatory + Redis best-effort through a StrictPolicy.
	// Redis fails → policy write should still succeed.
	lodeSink := NewStubSink()
	redisSink := &stubEventSink{writeErr: errors.New("redis down")}

	fanout := NewFanoutEventSink([]SinkEntry{
		{Sink: lodeSink, Delivery: DeliveryMandatory, Label: "lode"},
		{Sink: redisSink, Delivery: DeliveryBestEffort, Label: "redis"},
	})

	pol := NewStrictPolicy(fanout)

	events := testEnvelopes(2)
	for _, ev := range events {
		if err := pol.IngestEvent(t.Context(), ev); err != nil {
			t.Fatalf("ingest should succeed despite best-effort failure: %v", err)
		}
	}

	stats := pol.Stats()
	if stats.TotalEvents != 2 {
		t.Errorf("expected 2 events ingested, got %d", stats.TotalEvents)
	}
	if stats.EventsPersisted != 2 {
		t.Errorf("expected 2 events persisted, got %d", stats.EventsPersisted)
	}
}

func TestFanout_MandatoryFailureFailsPolicy(t *testing.T) {
	t.Parallel()

	// Mandatory Redis failure should propagate through the policy.
	lodeSink := NewStubSink()
	redisSink := &stubEventSink{writeErr: errors.New("redis down")}

	fanout := NewFanoutEventSink([]SinkEntry{
		{Sink: lodeSink, Delivery: DeliveryMandatory, Label: "lode"},
		{Sink: redisSink, Delivery: DeliveryMandatory, Label: "redis"},
	})

	pol := NewStrictPolicy(fanout)

	ev := testEnvelopes(1)[0]
	err := pol.IngestEvent(t.Context(), ev)
	if err == nil {
		t.Fatal("expected policy failure when mandatory sink fails")
	}
}

func TestFanout_PanicsOnEmpty(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on empty sinks")
		}
	}()
	NewFanoutEventSink(nil)
}

// Every mandatory sink's error stays in the fan-out's error chain, on write
// and on close, so a caller can match each one with errors.Is.
func TestFanout_KeepsEachErrorInTheChain(t *testing.T) {
	t.Parallel()
	errA, errB := errors.New("sink a failed"), errors.New("sink b failed")
	cases := []struct {
		name string
		call func(*FanoutEventSink) error
		a, b EventSink
	}{
		{
			name: "write",
			call: func(f *FanoutEventSink) error { return f.WriteEvents(t.Context(), testEnvelopes(1)) },
			a:    &stubEventSink{writeErr: errA},
			b:    &stubEventSink{writeErr: errB},
		},
		{
			name: "close",
			call: func(f *FanoutEventSink) error { return f.Close() },
			a:    &closerFailSink{closeErr: errA},
			b:    &closerFailSink{closeErr: errB},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fanout := NewFanoutEventSink([]SinkEntry{
				{Sink: tc.a, Delivery: DeliveryMandatory, Label: "a"},
				{Sink: tc.b, Delivery: DeliveryMandatory, Label: "b"},
			})
			err := tc.call(fanout)
			if !errors.Is(err, errA) || !errors.Is(err, errB) {
				t.Errorf("error %v, want both sink errors in its chain", err)
			}
		})
	}
}

// uncomparableCauseError is a host cause of a comparable type whose value,
// holding a slice, cannot be compared: == on it panics.
type uncomparableCauseError struct{ detail any }

func (c uncomparableCauseError) Error() string { return fmt.Sprint("host stopped: ", c.detail) }

func (c uncomparableCauseError) Unwrap() error { return context.Canceled }

// multiIsError joins errors and claims to be context.Canceled through its own
// Is method, whatever its branches hold.
type multiIsError struct{ errs []error }

func (e *multiIsError) Error() string   { return errors.Join(e.errs...).Error() }
func (e *multiIsError) Unwrap() []error { return e.errs }
func (e *multiIsError) Is(t error) bool { return t == context.Canceled }

// valueProbeKey is an unrelated context key, for a value child of a run.
type valueProbeKey struct{}

// bareUncomparableError is an uncomparable cause that wraps nothing, so a
// write showing it can be matched only by comparing the cause itself.
type bareUncomparableError struct{ detail any }

func (c bareUncomparableError) Error() string { return fmt.Sprint("host stopped: ", c.detail) }

// A failed write is marked where it fails: as ended with the run when the run
// that its context records had ended the write and every branch of the error
// shows that end, and as a refusal otherwise, including a store timeout under
// a live run, a context that records no run, and a detached flush's own
// deadline.
func TestWriteFailure(t *testing.T) {
	t.Parallel()
	refusal := errors.New("bucket refused the write")
	live := func() context.Context { return WithRunContext(t.Context()) }
	canceled := func() context.Context {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		return WithRunContext(ctx)
	}
	unrecorded := func() context.Context {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		return ctx
	}
	// detached is a termination flush's context as the runtime builds it: the
	// run has ended, and the flush's own deadline, detached from the run and
	// recording no run, has passed too.
	detached := func() context.Context {
		ctx, cancel := context.WithTimeout(WithoutRunContext(context.WithoutCancel(canceled())), -time.Second)
		t.Cleanup(cancel)
		return ctx
	}
	withCause := func(cause error) func() context.Context {
		return func() context.Context {
			ctx, cancel := context.WithCancelCause(t.Context())
			cancel(cause)
			return WithRunContext(ctx)
		}
	}
	hostDeadline := func() context.Context {
		ctx, cancel := context.WithTimeout(t.Context(), -time.Second)
		t.Cleanup(cancel)
		return WithRunContext(ctx)
	}
	// detachedAfterDeadline is a termination flush whose own deadline passed
	// after a host deadline: both end with context.DeadlineExceeded.
	detachedAfterDeadline := func() context.Context {
		ctx, cancel := context.WithTimeout(WithoutRunContext(context.WithoutCancel(hostDeadline())), -time.Second)
		t.Cleanup(cancel)
		return ctx
	}
	shutdown := errors.New("operator requested shutdown")
	hostCause := fmt.Errorf("host: %w", shutdown)
	uncomparable := uncomparableCauseError{detail: []string{"drain"}}
	bareUncomparable := bareUncomparableError{detail: []string{"drain"}}
	joinedCause := errors.Join(errors.New("drain"), errors.New("rotate"))
	// alsoMarked is set where the input already carries both marks.
	cases := []struct {
		name       string
		ctx        func() context.Context
		err        error
		want       error
		alsoMarked bool
	}{
		{"a refusal under a live context", live, refusal, ErrSinkWrite, false},
		{"a store timeout under a live context", live, fmt.Errorf("put: %w", context.DeadlineExceeded), ErrSinkWrite, false},
		{"the run's cancellation", canceled, fmt.Errorf("put: %w", context.Canceled), ErrWriteCanceled, false},
		{"a refusal after the run ended", canceled, refusal, ErrSinkWrite, false},
		{"the host's cause", withCause(shutdown), fmt.Errorf("put: %w", shutdown), ErrWriteCanceled, false},
		{"a wrapped host cause", withCause(hostCause), fmt.Errorf("put: %w", hostCause), ErrWriteCanceled, false},
		{"only the leaf of a wrapped host cause", withCause(hostCause), fmt.Errorf("put: %w", shutdown), ErrSinkWrite, false},
		{"an uncomparable host cause", withCause(uncomparable), fmt.Errorf("put: %w", uncomparable), ErrWriteCanceled, false},
		// A cause that cannot be compared is matched only through ctx's error, so
		// a write that shows it alone counts as a refusal, without a panic.
		{"a bare uncomparable host cause", withCause(bareUncomparable), fmt.Errorf("put: %w", bareUncomparable), ErrSinkWrite, false},
		{"a cancellation without a recorded run", unrecorded, fmt.Errorf("put: %w", context.Canceled), ErrSinkWrite, false},
		{"a detached flush that ran out of its own time", detached, fmt.Errorf("put: %w", context.DeadlineExceeded), ErrSinkWrite, false},
		{"a refusal joined with the run's end in one sink", canceled, errors.Join(refusal, context.Canceled), ErrSinkWrite, false},
		{"a refusal already marked under an ended run", canceled, fmt.Errorf("%w: %w", ErrSinkWrite, context.DeadlineExceeded), ErrSinkWrite, false},
		{"a detached flush after a host deadline", detachedAfterDeadline, fmt.Errorf("put: %w", context.DeadlineExceeded), ErrSinkWrite, false},
		{"a value child of a run with an uncomparable cause", func() context.Context {
			return context.WithValue(withCause(uncomparable)(), valueProbeKey{}, true)
		}, fmt.Errorf("put: %w", context.Canceled), ErrWriteCanceled, false},
		{"a wrapped refusal joined with the run's end", canceled, fmt.Errorf("put: %w", errors.Join(refusal, context.Canceled)), ErrSinkWrite, false},
		{"a host cause that is itself a join", withCause(joinedCause), joinedCause, ErrWriteCanceled, false},
		{"a cancellation already marked under an unrecorded context", unrecorded, fmt.Errorf("%w: %w", ErrWriteCanceled, context.Canceled), ErrWriteCanceled, false},
		{"a run child whose own deadline passed first", func() context.Context {
			runCtx, cancel := context.WithCancel(t.Context())
			child, stop := context.WithTimeout(WithRunContext(runCtx), -time.Second)
			t.Cleanup(stop)
			cancel()
			return child
		}, fmt.Errorf("put: %w", context.DeadlineExceeded), ErrSinkWrite, false},
		{"a multi-error whose Is claims the cancellation", canceled, &multiIsError{errs: []error{context.Canceled, refusal}}, ErrSinkWrite, false},
		{"a childless multi-error whose Is claims the cancellation", canceled, &multiIsError{}, ErrWriteCanceled, false},
		{"an error a fan-out already marked", canceled, errors.Join(fmt.Errorf("%w: %w", ErrWriteCanceled, context.Canceled), fmt.Errorf("%w: %w", ErrSinkWrite, refusal)), ErrSinkWrite, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := writeFailure(tc.ctx(), tc.err)
			other := map[error]error{ErrSinkWrite: ErrWriteCanceled, ErrWriteCanceled: ErrSinkWrite}[tc.want]
			if !errors.Is(got, tc.want) || !errors.Is(got, tc.err) || (!tc.alsoMarked && errors.Is(got, other)) {
				t.Errorf("writeFailure = %v, want it marked %v around the original error", got, tc.want)
			}
		})
	}
}

// A fan-out marks each mandatory sink on its own, so one sink's refusal stays
// a refusal beside another's cancellation.
func TestFanout_MarksEachSinkWrite(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	ctx = WithRunContext(ctx)
	refusal := errors.New("bucket refused the write")
	fanout := NewFanoutEventSink([]SinkEntry{
		{Sink: &stubEventSink{writeErr: fmt.Errorf("put: %w", context.Canceled)}, Delivery: DeliveryMandatory, Label: "a"},
		{Sink: &stubEventSink{writeErr: refusal}, Delivery: DeliveryMandatory, Label: "b"},
	})
	err := fanout.WriteEvents(ctx, testEnvelopes(1))
	if !errors.Is(err, ErrSinkWrite) || !errors.Is(err, ErrWriteCanceled) || !errors.Is(err, refusal) {
		t.Errorf("fan-out error %v, want a refusal and a cancellation, each marked", err)
	}
}

// gatedSink waits for its gate, then fails with the error that fail builds
// from the write's context.
type gatedSink struct {
	stubEventSink
	gate <-chan struct{}
	fail func(ctx context.Context) error
}

func (s *gatedSink) WriteEvents(ctx context.Context, _ []*types.EventEnvelope) error {
	<-s.gate
	return s.fail(ctx)
}

// Each fan-out sink is marked when its own write returns: a sink that failed
// while the run was live stays a refusal although the run ends before the
// other sink returns. synctest.Wait establishes that the first sink has
// returned, and been marked, before the run is cancelled.
func TestFanout_MarksEachSinkWhenItReturns(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		runCtx, cancel := context.WithCancel(t.Context())
		ctx := WithRunContext(runCtx)
		open := make(chan struct{})
		closed := make(chan struct{})
		close(open)
		fanout := NewFanoutEventSink([]SinkEntry{
			{Sink: &gatedSink{gate: open, fail: func(context.Context) error { return fmt.Errorf("own timeout: %w", context.Canceled) }}, Delivery: DeliveryMandatory, Label: "early"},
			{Sink: &gatedSink{gate: closed, fail: func(ctx context.Context) error { return fmt.Errorf("put: %w", ctx.Err()) }}, Delivery: DeliveryMandatory, Label: "late"},
		})
		result := make(chan error, 1)
		go func() { result <- fanout.WriteEvents(ctx, testEnvelopes(1)) }()
		synctest.Wait()
		cancel()
		close(closed)
		err := <-result
		if !errors.Is(err, ErrSinkWrite) || !errors.Is(err, ErrWriteCanceled) {
			t.Errorf("fan-out error %v, want the early refusal and the late cancellation, each marked", err)
		}
	})
}
