package policy_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/pithecene/quarry/policy"
	"github.com/pithecene/quarry/types"
)

// TestBufferedPolicy_Stats_ConcurrentAccess verifies that Stats() is safe
// under concurrent ingestion and flush operations. Run with -race.
func TestBufferedPolicy_Stats_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{
		MaxBufferEvents: 1000,
		MaxBufferBytes:  100 * 1024,
	}
	pol, err := policy.NewBufferedPolicy(sink, config)
	if err != nil {
		t.Fatalf("NewBufferedPolicy failed: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var wg sync.WaitGroup
	const numIngesters = 4
	const numEventsPerIngester = 100

	// Spawn ingesters
	for i := range numIngesters {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range numEventsPerIngester {
				select {
				case <-ctx.Done():
					return
				default:
				}
				envelope := &types.EventEnvelope{
					EventID: "e",
					Type:    types.EventTypeItem,
					Seq:     int64(id*numEventsPerIngester + j),
				}
				_ = pol.IngestEvent(ctx, envelope)
			}
		}(i)
	}

	// Spawn chunk ingesters
	wg.Go(func() {
		for i := range 50 {
			select {
			case <-ctx.Done():
				return
			default:
			}
			chunk := &types.ArtifactChunk{
				ArtifactID: "a1",
				Seq:        int64(i),
				Data:       []byte("chunk-data"),
			}
			_ = pol.IngestArtifactChunk(ctx, chunk)
		}
	})

	// Spawn stats readers
	statsResults := make(chan policy.Stats, 1000)
	wg.Go(func() {
		for range 200 {
			select {
			case <-ctx.Done():
				return
			default:
			}
			stats := pol.Stats()
			statsResults <- stats
		}
	})

	// Spawn flushers
	wg.Go(func() {
		for range 10 {
			select {
			case <-ctx.Done():
				return
			default:
			}
			_ = pol.Flush(ctx)
		}
	})

	wg.Wait()
	close(statsResults)

	// Validate all snapshots have non-negative values
	for stats := range statsResults {
		if stats.BufferSize < 0 {
			t.Errorf("BufferSize should never be negative, got %d", stats.BufferSize)
		}
		if stats.TotalEvents < 0 {
			t.Errorf("TotalEvents should never be negative, got %d", stats.TotalEvents)
		}
		if stats.EventsPersisted < 0 {
			t.Errorf("EventsPersisted should never be negative, got %d", stats.EventsPersisted)
		}
	}
}

// TestBufferedPolicy_Stats_BufferSizeZeroAfterFlush verifies that BufferSize
// is zero after a successful flush when all ingestion has completed.
func TestBufferedPolicy_Stats_BufferSizeZeroAfterFlush(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferBytes: 10000}
	pol, _ := policy.NewBufferedPolicy(sink, config)

	ctx := t.Context()

	// Ingest some data
	for i := range 10 {
		_ = pol.IngestEvent(ctx, &types.EventEnvelope{
			EventID: "e",
			Type:    types.EventTypeItem,
			Seq:     int64(i),
		})
	}
	_ = pol.IngestArtifactChunk(ctx, &types.ArtifactChunk{
		ArtifactID: "a1",
		Seq:        1,
		Data:       []byte("chunk"),
	})

	// Verify non-zero before flush
	statsBefore := pol.Stats()
	if statsBefore.BufferSize == 0 {
		t.Fatal("BufferSize should be non-zero before flush")
	}

	// Flush
	if err := pol.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify zero after flush
	statsAfter := pol.Stats()
	if statsAfter.BufferSize != 0 {
		t.Errorf("BufferSize should be 0 after successful flush, got %d", statsAfter.BufferSize)
	}
}

// TestPolicy_Stats_CrossPolicyConsistency verifies that stats semantics are
// uniform across policy implementations (interface-level contract).
func TestPolicy_Stats_CrossPolicyConsistency(t *testing.T) {
	t.Parallel()

	type policyFactory func(policy.Sink) policy.Policy

	factories := map[string]policyFactory{
		"StrictPolicy": func(sink policy.Sink) policy.Policy {
			return policy.NewStrictPolicy(sink)
		},
		"BufferedPolicy": func(sink policy.Sink) policy.Policy {
			pol, _ := policy.NewBufferedPolicy(sink, policy.BufferedConfig{
				MaxBufferEvents: 100,
				MaxBufferBytes:  10000,
			})
			return pol
		},
	}

	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			sink := policy.NewStubSink()
			pol := factory(sink)
			ctx := t.Context()

			// Ingest events
			for i := range 5 {
				err := pol.IngestEvent(ctx, &types.EventEnvelope{
					EventID: "e",
					Type:    types.EventTypeItem,
					Seq:     int64(i),
				})
				if err != nil {
					t.Fatalf("IngestEvent failed: %v", err)
				}
			}

			// Ingest chunks
			for i := range 3 {
				err := pol.IngestArtifactChunk(ctx, &types.ArtifactChunk{
					ArtifactID: "a1",
					Seq:        int64(i),
					Data:       []byte("data"),
				})
				if err != nil {
					t.Fatalf("IngestArtifactChunk failed: %v", err)
				}
			}

			// Flush
			if err := pol.Flush(ctx); err != nil {
				t.Fatalf("Flush failed: %v", err)
			}

			stats := pol.Stats()

			// Common invariants across all policies
			if stats.TotalEvents != 5 {
				t.Errorf("expected TotalEvents=5, got %d", stats.TotalEvents)
			}
			if stats.EventsPersisted != 5 {
				t.Errorf("expected EventsPersisted=5, got %d", stats.EventsPersisted)
			}
			if stats.TotalChunks != 3 {
				t.Errorf("expected TotalChunks=3, got %d", stats.TotalChunks)
			}
			if stats.ChunksPersisted != 3 {
				t.Errorf("expected ChunksPersisted=3, got %d", stats.ChunksPersisted)
			}
			if stats.FlushCount != 1 {
				t.Errorf("expected FlushCount=1, got %d", stats.FlushCount)
			}
			if stats.EventsDropped != 0 {
				t.Errorf("expected EventsDropped=0, got %d", stats.EventsDropped)
			}
			if stats.Errors != 0 {
				t.Errorf("expected Errors=0, got %d", stats.Errors)
			}
			if stats.DroppedByType == nil {
				t.Error("DroppedByType should never be nil")
			}
		})
	}
}

// TestPolicy_Stats_ErrorsOnSinkFailure verifies that Errors counter increments
// on sink failures across policy implementations.
func TestPolicy_Stats_ErrorsOnSinkFailure(t *testing.T) {
	t.Parallel()

	type policyFactory func(policy.Sink) policy.Policy

	factories := map[string]policyFactory{
		"StrictPolicy": func(sink policy.Sink) policy.Policy {
			return policy.NewStrictPolicy(sink)
		},
		"BufferedPolicy": func(sink policy.Sink) policy.Policy {
			pol, _ := policy.NewBufferedPolicy(sink, policy.BufferedConfig{
				MaxBufferBytes: 10000,
			})
			return pol
		},
	}

	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			sink := policy.NewStubSink()
			sink.ErrorOnWrite = errors.New("sink failure")
			pol := factory(sink)
			ctx := t.Context()

			// Attempt to ingest (will fail on StrictPolicy, buffer on BufferedPolicy)
			_ = pol.IngestEvent(ctx, &types.EventEnvelope{
				EventID: "e1",
				Type:    types.EventTypeItem,
			})

			// For BufferedPolicy, flush to trigger the error
			_ = pol.Flush(ctx)

			stats := pol.Stats()
			if stats.Errors < 1 {
				t.Errorf("expected Errors >= 1 on sink failure, got %d", stats.Errors)
			}
		})
	}
}

// TestStats_DroppedByType_SnapshotIsolation verifies that the DroppedByType map
// in Stats is a deep copy, not a reference to internal state.
func TestStats_DroppedByType_SnapshotIsolation(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferEvents: 1}
	pol, _ := policy.NewBufferedPolicy(sink, config)
	ctx := t.Context()

	// Fill buffer with non-droppable
	_ = pol.IngestEvent(ctx, &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem,
	})

	// Drop a droppable event
	_ = pol.IngestEvent(ctx, &types.EventEnvelope{
		EventID: "log1", Type: types.EventTypeLog,
	})

	// Take first snapshot
	stats1 := pol.Stats()
	if stats1.DroppedByType[types.EventTypeLog] != 1 {
		t.Fatalf("expected 1 log dropped, got %d", stats1.DroppedByType[types.EventTypeLog])
	}

	// Drop another event
	_ = pol.IngestEvent(ctx, &types.EventEnvelope{
		EventID: "log2", Type: types.EventTypeLog,
	})

	// Take second snapshot
	stats2 := pol.Stats()
	if stats2.DroppedByType[types.EventTypeLog] != 2 {
		t.Errorf("expected 2 logs dropped in stats2, got %d", stats2.DroppedByType[types.EventTypeLog])
	}

	// First snapshot should be unchanged (isolation)
	if stats1.DroppedByType[types.EventTypeLog] != 1 {
		t.Errorf("stats1 should be isolated, expected 1 log, got %d", stats1.DroppedByType[types.EventTypeLog])
	}

	// Mutating returned map should not affect internal state
	stats2.DroppedByType[types.EventTypeLog] = 999
	stats3 := pol.Stats()
	if stats3.DroppedByType[types.EventTypeLog] != 2 {
		t.Errorf("internal state should be isolated from mutations, got %d", stats3.DroppedByType[types.EventTypeLog])
	}
}

// TestStats_FlushCount_IncrementsOnEachFlush verifies FlushCount increments
// exactly once per Flush call.
func TestStats_FlushCount_IncrementsOnEachFlush(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferBytes: 10000}
	pol, _ := policy.NewBufferedPolicy(sink, config)
	ctx := t.Context()

	if pol.Stats().FlushCount != 0 {
		t.Errorf("expected FlushCount=0 initially, got %d", pol.Stats().FlushCount)
	}

	for i := 1; i <= 5; i++ {
		_ = pol.Flush(ctx)
		if pol.Stats().FlushCount != int64(i) {
			t.Errorf("expected FlushCount=%d after %d flushes, got %d", i, i, pol.Stats().FlushCount)
		}
	}
}

// TestStats_FlushCount_IncrementsEvenOnFailure verifies that FlushCount
// increments even when the flush operation fails.
func TestStats_FlushCount_IncrementsEvenOnFailure(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferBytes: 10000}
	pol, _ := policy.NewBufferedPolicy(sink, config)
	ctx := t.Context()

	// Add data to buffer
	_ = pol.IngestEvent(ctx, &types.EventEnvelope{
		EventID: "e1", Type: types.EventTypeItem,
	})

	// Make sink fail
	sink.ErrorOnWrite = errors.New("write failed")

	// Flush fails
	_ = pol.Flush(ctx)

	stats := pol.Stats()
	if stats.FlushCount != 1 {
		t.Errorf("expected FlushCount=1 even on failure, got %d", stats.FlushCount)
	}
	if stats.Errors != 1 {
		t.Errorf("expected Errors=1, got %d", stats.Errors)
	}
}

// TestStats_EventsPersisted_OnlyOnSuccess verifies that EventsPersisted
// only increments after successful writes.
func TestStats_EventsPersisted_OnlyOnSuccess(t *testing.T) {
	t.Parallel()

	sink := policy.NewStubSink()
	config := policy.BufferedConfig{MaxBufferBytes: 10000}
	pol, _ := policy.NewBufferedPolicy(sink, config)
	ctx := t.Context()

	// Add events
	for range 3 {
		_ = pol.IngestEvent(ctx, &types.EventEnvelope{
			EventID: "e", Type: types.EventTypeItem,
		})
	}

	// Fail flush
	sink.ErrorOnWrite = errors.New("write failed")
	_ = pol.Flush(ctx)

	stats := pol.Stats()
	if stats.EventsPersisted != 0 {
		t.Errorf("expected EventsPersisted=0 after failed flush, got %d", stats.EventsPersisted)
	}

	// Succeed flush
	sink.ErrorOnWrite = nil
	_ = pol.Flush(ctx)

	stats = pol.Stats()
	if stats.EventsPersisted != 3 {
		t.Errorf("expected EventsPersisted=3 after successful flush, got %d", stats.EventsPersisted)
	}
}
