package policy_test

import (
	"errors"
	"testing"

	"github.com/pithecene/quarry/policy"
	"github.com/pithecene/quarry/types"
)

func TestStubSink_WriteEvents(t *testing.T) {
	sink := policy.NewStubSink()

	events := []*types.EventEnvelope{
		{EventID: "e1", Type: types.EventTypeItem},
		{EventID: "e2", Type: types.EventTypeLog},
	}

	err := sink.WriteEvents(t.Context(), events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stats := sink.Stats()
	if stats.EventsWritten != 2 {
		t.Errorf("expected 2 events written, got %d", stats.EventsWritten)
	}
	if stats.EventBatches != 1 {
		t.Errorf("expected 1 batch, got %d", stats.EventBatches)
	}
	if len(sink.WrittenEvents) != 2 {
		t.Errorf("expected 2 stored events, got %d", len(sink.WrittenEvents))
	}
}

func TestStubSink_WriteChunks(t *testing.T) {
	sink := policy.NewStubSink()

	chunks := []*types.ArtifactChunk{
		{ArtifactID: "a1", Seq: 1, Data: []byte("data1")},
		{ArtifactID: "a1", Seq: 2, Data: []byte("data2"), IsLast: true},
	}

	err := sink.WriteChunks(t.Context(), chunks)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stats := sink.Stats()
	if stats.ChunksWritten != 2 {
		t.Errorf("expected 2 chunks written, got %d", stats.ChunksWritten)
	}
	if stats.ChunkBatches != 1 {
		t.Errorf("expected 1 batch, got %d", stats.ChunkBatches)
	}
}

func TestStubSink_ErrorOnWrite(t *testing.T) {
	sink := policy.NewStubSink()
	expectedErr := errors.New("write failed")
	sink.ErrorOnWrite = expectedErr

	err := sink.WriteEvents(t.Context(), []*types.EventEnvelope{{EventID: "e1"}})
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	err = sink.WriteChunks(t.Context(), []*types.ArtifactChunk{{ArtifactID: "a1"}})
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestStubSink_Close(t *testing.T) {
	sink := policy.NewStubSink()

	if sink.Stats().Closed {
		t.Error("sink should not be closed initially")
	}

	err := sink.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !sink.Stats().Closed {
		t.Error("sink should be closed after Close()")
	}
}
