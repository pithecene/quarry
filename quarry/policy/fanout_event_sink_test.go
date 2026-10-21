package policy

import (
	"context"
	"errors"
	"sync"
	"testing"

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
	// Simulate: Lode mandatory + Redis best-effort through a StrictPolicy.
	// Redis fails → policy write should still succeed.
	lodeSink := NewStubSink()
	redisSink := &stubEventSink{writeErr: errors.New("redis down")}

	fanout := NewFanoutEventSink([]SinkEntry{
		{Sink: lodeSink, Delivery: DeliveryMandatory, Label: "lode"},
		{Sink: redisSink, Delivery: DeliveryBestEffort, Label: "redis"},
	})

	composite := NewCompositeSink(fanout, lodeSink)
	pol := NewStrictPolicy(composite)

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
	// Mandatory Redis failure should propagate through the policy.
	lodeSink := NewStubSink()
	redisSink := &stubEventSink{writeErr: errors.New("redis down")}

	fanout := NewFanoutEventSink([]SinkEntry{
		{Sink: lodeSink, Delivery: DeliveryMandatory, Label: "lode"},
		{Sink: redisSink, Delivery: DeliveryMandatory, Label: "redis"},
	})

	composite := NewCompositeSink(fanout, lodeSink)
	pol := NewStrictPolicy(composite)

	ev := testEnvelopes(1)[0]
	err := pol.IngestEvent(t.Context(), ev)
	if err == nil {
		t.Fatal("expected policy failure when mandatory sink fails")
	}
}

func TestFanout_PanicsOnEmpty(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on empty sinks")
		}
	}()
	NewFanoutEventSink(nil)
}
