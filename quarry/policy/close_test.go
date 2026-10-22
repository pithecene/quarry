package policy_test

import (
	"errors"
	"testing"

	"github.com/pithecene/quarry/policy"
	"github.com/pithecene/quarry/types"
)

// Close is one-shot: a later Close (a deferred one) must write nothing,
// even when events are still buffered after a failed flush.
func TestBufferedPolicy_SecondCloseWritesNothing(t *testing.T) {
	sink := policy.NewStubSink()
	pol := mustNewBufferedPolicy(t, sink, policy.BufferedConfig{MaxBufferEvents: 10})

	for i := range 2 {
		ev := &types.EventEnvelope{RunID: "run-001", Seq: int64(i + 1), Type: types.EventTypeItem}
		if err := pol.IngestEvent(t.Context(), ev); err != nil {
			t.Fatalf("IngestEvent: %v", err)
		}
	}

	sink.ErrorOnWrite = errors.New("store unavailable")
	// The flush fails, so the events stay buffered: Close reports the loss.
	if err := pol.Close(); !errors.Is(err, policy.ErrCloseUnflushed) {
		t.Fatalf("first Close err = %v, want ErrCloseUnflushed", err)
	}
	sink.ErrorOnWrite = nil

	_ = pol.Close()
	if got := sink.Stats().EventsWritten; got != 0 {
		t.Fatalf("second Close wrote %d events, want 0", got)
	}
}

func TestStrictPolicy_CloseIsOneShot(t *testing.T) {
	sink := &countingCloseSink{StubSink: policy.NewStubSink()}
	pol := policy.NewStrictPolicy(sink)
	_ = pol.Close()
	_ = pol.Close()
	if sink.closes != 1 {
		t.Fatalf("sink closed %d times, want 1", sink.closes)
	}
}

type countingCloseSink struct {
	*policy.StubSink
	closes int
}

func (s *countingCloseSink) Close() error {
	s.closes++
	return s.StubSink.Close()
}

// Data offered after Close began is refused, not accepted and lost behind
// the final flush.
func TestPolicies_RefuseIngestAfterClose(t *testing.T) {
	ev := &types.EventEnvelope{RunID: "run-001", Seq: 1, Type: types.EventTypeItem}
	buffered := mustNewBufferedPolicy(t, policy.NewStubSink(), policy.BufferedConfig{MaxBufferEvents: 10, MaxBufferBytes: 1 << 20})
	streaming, err := policy.NewStreamingPolicy(policy.NewStubSink(), policy.StreamingConfig{FlushCount: 10})
	if err != nil {
		t.Fatalf("NewStreamingPolicy: %v", err)
	}
	for name, pol := range map[string]policy.Policy{"buffered": buffered, "streaming": streaming} {
		_ = pol.Close()
		if err := pol.IngestEvent(t.Context(), ev); !errors.Is(err, policy.ErrPolicyClosed) {
			t.Errorf("%s IngestEvent after Close: %v, want ErrPolicyClosed", name, err)
		}
		if err := pol.IngestArtifactChunk(t.Context(), &types.ArtifactChunk{ArtifactID: "a", Data: []byte("x")}); !errors.Is(err, policy.ErrPolicyClosed) {
			t.Errorf("%s IngestArtifactChunk after Close: %v, want ErrPolicyClosed", name, err)
		}
	}
}
