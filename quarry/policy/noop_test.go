package policy_test

import (
	"testing"

	"github.com/pithecene/quarry/policy"
	"github.com/pithecene/quarry/types"
)

func TestNoopPolicy_AcceptReturnsNil(t *testing.T) {
	pol := policy.NewNoopPolicy()

	// Every event type should be accepted without error
	eventTypes := []types.EventType{
		types.EventTypeItem,
		types.EventTypeArtifact,
		types.EventTypeCheckpoint,
		types.EventTypeLog,
		types.EventTypeEnqueue,
		types.EventTypeRotateProxy,
		types.EventTypeRunError,
		types.EventTypeRunComplete,
	}

	for _, et := range eventTypes {
		t.Run(string(et), func(t *testing.T) {
			envelope := &types.EventEnvelope{
				EventID: "e1",
				Type:    et,
				RunID:   "run-1",
				Seq:     1,
			}
			err := pol.IngestEvent(t.Context(), envelope)
			if err != nil {
				t.Errorf("IngestEvent(%s) = %v, want nil", et, err)
			}
		})
	}
}

func TestNoopPolicy_AcceptChunkReturnsNil(t *testing.T) {
	pol := policy.NewNoopPolicy()

	chunk := &types.ArtifactChunk{
		ArtifactID: "a1",
		Seq:        1,
		Data:       []byte("data"),
		IsLast:     true,
	}

	err := pol.IngestArtifactChunk(t.Context(), chunk)
	if err != nil {
		t.Errorf("IngestArtifactChunk() = %v, want nil", err)
	}
}

func TestNoopPolicy_StatsDefensiveCopy(t *testing.T) {
	pol := policy.NewNoopPolicy()

	// Ingest an event so stats are non-zero
	envelope := &types.EventEnvelope{
		EventID: "e1",
		Type:    types.EventTypeLog,
		RunID:   "run-1",
		Seq:     1,
	}
	if err := pol.IngestEvent(t.Context(), envelope); err != nil {
		t.Fatalf("IngestEvent failed: %v", err)
	}

	// Get stats and mutate the returned copy
	stats1 := pol.Stats()
	stats1.TotalEvents = 999
	stats1.DroppedByType[types.EventTypeLog] = 999

	// Get stats again — should reflect original values, not the mutation
	stats2 := pol.Stats()
	if stats2.TotalEvents != 1 {
		t.Errorf("TotalEvents = %d after mutation, want 1 (defensive copy broken)", stats2.TotalEvents)
	}
	if stats2.DroppedByType[types.EventTypeLog] != 1 {
		t.Errorf("DroppedByType[log] = %d after mutation, want 1 (map copy broken)", stats2.DroppedByType[types.EventTypeLog])
	}
}

func TestNoopPolicy_CloseReturnsNil(t *testing.T) {
	pol := policy.NewNoopPolicy()

	err := pol.Close()
	if err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}
}

func TestNoopPolicy_FlushReturnsNil(t *testing.T) {
	pol := policy.NewNoopPolicy()

	err := pol.Flush(t.Context())
	if err != nil {
		t.Errorf("Flush() = %v, want nil", err)
	}
}

func TestNoopPolicy_DroppableVsNonDroppableStats(t *testing.T) {
	pol := policy.NewNoopPolicy()

	// Ingest a non-droppable event
	item := &types.EventEnvelope{
		EventID: "e1",
		Type:    types.EventTypeItem,
		RunID:   "run-1",
		Seq:     1,
	}
	if err := pol.IngestEvent(t.Context(), item); err != nil {
		t.Fatalf("IngestEvent(item) failed: %v", err)
	}

	// Ingest a droppable event
	log := &types.EventEnvelope{
		EventID: "e2",
		Type:    types.EventTypeLog,
		RunID:   "run-1",
		Seq:     2,
	}
	if err := pol.IngestEvent(t.Context(), log); err != nil {
		t.Fatalf("IngestEvent(log) failed: %v", err)
	}

	stats := pol.Stats()

	if stats.TotalEvents != 2 {
		t.Errorf("TotalEvents = %d, want 2", stats.TotalEvents)
	}
	if stats.EventsPersisted != 1 {
		t.Errorf("EventsPersisted = %d, want 1 (non-droppable only)", stats.EventsPersisted)
	}
	if stats.EventsDropped != 1 {
		t.Errorf("EventsDropped = %d, want 1 (droppable only)", stats.EventsDropped)
	}
	if stats.DroppedByType[types.EventTypeLog] != 1 {
		t.Errorf("DroppedByType[log] = %d, want 1", stats.DroppedByType[types.EventTypeLog])
	}
}
