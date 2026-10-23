package runtime

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pithecene/quarry/types"
)

func TestComputeDedupKey_Deterministic(t *testing.T) {
	t.Parallel()

	params := map[string]any{"url": "https://example.com", "page": float64(1)}

	key1 := computeDedupKey("script.ts", params)
	key2 := computeDedupKey("script.ts", params)

	if key1 != key2 {
		t.Errorf("dedup keys should be deterministic, got %s and %s", key1, key2)
	}
}

func TestComputeDedupKey_DifferentTarget(t *testing.T) {
	t.Parallel()

	params := map[string]any{"url": "https://example.com"}

	key1 := computeDedupKey("script-a.ts", params)
	key2 := computeDedupKey("script-b.ts", params)

	if key1 == key2 {
		t.Error("different targets should produce different dedup keys")
	}
}

func TestComputeDedupKey_DifferentParams(t *testing.T) {
	t.Parallel()

	key1 := computeDedupKey("script.ts", map[string]any{"page": float64(1)})
	key2 := computeDedupKey("script.ts", map[string]any{"page": float64(2)})

	if key1 == key2 {
		t.Error("different params should produce different dedup keys")
	}
}

func TestComputeDedupKey_MapKeyOrdering(t *testing.T) {
	t.Parallel()

	// json.Marshal sorts map keys; verify ordering doesn't affect key
	params1 := map[string]any{"a": "1", "b": "2", "c": "3"}
	params2 := map[string]any{"c": "3", "a": "1", "b": "2"}

	key1 := computeDedupKey("script.ts", params1)
	key2 := computeDedupKey("script.ts", params2)

	if key1 != key2 {
		t.Errorf("map key ordering should not affect dedup key, got %s and %s", key1, key2)
	}
}

func TestComputeDedupKey_NilParams(t *testing.T) {
	t.Parallel()

	key1 := computeDedupKey("script.ts", nil)
	key2 := computeDedupKey("script.ts", map[string]any{})

	// nil marshals to "null", empty map marshals to "{}" — these should differ
	// but both are valid and deterministic
	_ = key1
	_ = key2
}

// successFactory returns a ChildRunFactory that records calls and returns success.
func successFactory(calls *atomic.Int64) ChildRunFactory {
	return func(ctx context.Context, item WorkItem, observer EnqueueObserver) (*RunResult, error) {
		calls.Add(1)
		return &RunResult{
			RunMeta: &types.RunMeta{RunID: item.RunID, Attempt: 1},
			Outcome: &types.RunOutcome{Status: types.OutcomeSuccess, Message: "ok"},
		}, nil
	}
}

func TestOperator_DepthLimit(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	operator := NewOperator(FanOutConfig{
		MaxDepth: 1,
		MaxRuns:  10,
		Parallel: 1,
	}, successFactory(&calls))

	// Create observer at depth 1 (already at max depth)
	observer := operator.NewObserver(1)

	// This enqueue should be skipped (depth 1 + 1 = 2 > MaxDepth 1)
	observer(&types.EventEnvelope{
		Type:    types.EventTypeEnqueue,
		Payload: map[string]any{"target": "script.ts", "params": map[string]any{}},
	})

	rootDone := make(chan struct{})
	close(rootDone)
	operator.Run(t.Context(), rootDone)

	result := operator.Results()
	if result.RunsTotal != 0 {
		t.Errorf("expected 0 runs at max depth, got %d", result.RunsTotal)
	}
	if result.EnqueueSkipped != 1 {
		t.Errorf("expected 1 skipped, got %d", result.EnqueueSkipped)
	}
}

func TestOperator_MaxRunsCap(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	operator := NewOperator(FanOutConfig{
		MaxDepth: 5,
		MaxRuns:  3,
		Parallel: 1,
	}, successFactory(&calls))

	observer := operator.NewObserver(0)

	// Submit 5 items but max-runs is 3
	for i := range 5 {
		observer(&types.EventEnvelope{
			Type: types.EventTypeEnqueue,
			Payload: map[string]any{
				"target": "script.ts",
				"params": map[string]any{"page": float64(i)},
			},
		})
	}

	rootDone := make(chan struct{})
	close(rootDone)
	operator.Run(t.Context(), rootDone)

	result := operator.Results()
	if result.RunsTotal != 3 {
		t.Errorf("expected 3 runs (max-runs cap), got %d", result.RunsTotal)
	}
	if result.EnqueueSkipped != 2 {
		t.Errorf("expected 2 skipped, got %d", result.EnqueueSkipped)
	}
}

func TestOperator_DedupSkips(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	operator := NewOperator(FanOutConfig{
		MaxDepth: 3,
		MaxRuns:  10,
		Parallel: 1,
	}, successFactory(&calls))

	observer := operator.NewObserver(0)

	// Submit same target+params twice
	for range 2 {
		observer(&types.EventEnvelope{
			Type: types.EventTypeEnqueue,
			Payload: map[string]any{
				"target": "script.ts",
				"params": map[string]any{"url": "https://example.com"},
			},
		})
	}

	rootDone := make(chan struct{})
	close(rootDone)
	operator.Run(t.Context(), rootDone)

	result := operator.Results()
	if result.RunsTotal != 1 {
		t.Errorf("expected 1 run after dedup, got %d", result.RunsTotal)
	}
	if result.EnqueueDeduped != 1 {
		t.Errorf("expected 1 deduped, got %d", result.EnqueueDeduped)
	}
}

func TestOperator_MissingTarget(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	operator := NewOperator(FanOutConfig{
		MaxDepth: 3,
		MaxRuns:  10,
		Parallel: 1,
	}, successFactory(&calls))

	observer := operator.NewObserver(0)

	// Enqueue with no target
	observer(&types.EventEnvelope{
		Type:    types.EventTypeEnqueue,
		Payload: map[string]any{"params": map[string]any{}},
	})

	// Enqueue with empty target
	observer(&types.EventEnvelope{
		Type:    types.EventTypeEnqueue,
		Payload: map[string]any{"target": "", "params": map[string]any{}},
	})

	rootDone := make(chan struct{})
	close(rootDone)
	operator.Run(t.Context(), rootDone)

	result := operator.Results()
	if result.RunsTotal != 0 {
		t.Errorf("expected 0 runs for missing target, got %d", result.RunsTotal)
	}
	if result.EnqueueSkipped != 2 {
		t.Errorf("expected 2 skipped for missing target, got %d", result.EnqueueSkipped)
	}
}

func TestOperator_Concurrency(t *testing.T) {
	t.Parallel()

	var maxConcurrent atomic.Int64
	var currentConcurrent atomic.Int64

	factory := func(ctx context.Context, item WorkItem, observer EnqueueObserver) (*RunResult, error) {
		cur := currentConcurrent.Add(1)
		for {
			old := maxConcurrent.Load()
			if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
				break
			}
		}

		// Simulate work
		time.Sleep(10 * time.Millisecond)
		currentConcurrent.Add(-1)

		return &RunResult{
			RunMeta: &types.RunMeta{RunID: item.RunID, Attempt: 1},
			Outcome: &types.RunOutcome{Status: types.OutcomeSuccess, Message: "ok"},
		}, nil
	}

	operator := NewOperator(FanOutConfig{
		MaxDepth: 3,
		MaxRuns:  6,
		Parallel: 3,
	}, factory)

	observer := operator.NewObserver(0)
	for i := range 6 {
		observer(&types.EventEnvelope{
			Type: types.EventTypeEnqueue,
			Payload: map[string]any{
				"target": "script.ts",
				"params": map[string]any{"i": float64(i)},
			},
		})
	}

	rootDone := make(chan struct{})
	close(rootDone)
	operator.Run(t.Context(), rootDone)

	result := operator.Results()
	if result.RunsTotal != 6 {
		t.Errorf("expected 6 runs, got %d", result.RunsTotal)
	}
	if maxConcurrent.Load() > 3 {
		t.Errorf("expected max concurrency <= 3, got %d", maxConcurrent.Load())
	}
}

func TestOperator_ContextCancellation(t *testing.T) {
	t.Parallel()

	factory := func(ctx context.Context, item WorkItem, observer EnqueueObserver) (*RunResult, error) {
		// Block until context canceled
		<-ctx.Done()
		return &RunResult{
			RunMeta: &types.RunMeta{RunID: item.RunID, Attempt: 1},
			Outcome: &types.RunOutcome{Status: types.OutcomeExecutorCrash, Message: "canceled"},
		}, nil
	}

	operator := NewOperator(FanOutConfig{
		MaxDepth: 3,
		MaxRuns:  5,
		Parallel: 2,
	}, factory)

	observer := operator.NewObserver(0)
	observer(&types.EventEnvelope{
		Type:    types.EventTypeEnqueue,
		Payload: map[string]any{"target": "script.ts", "params": map[string]any{"id": "a"}},
	})

	ctx, cancel := context.WithCancel(t.Context())
	rootDone := make(chan struct{})
	close(rootDone)

	done := make(chan struct{})
	go func() {
		operator.Run(ctx, rootDone)
		close(done)
	}()

	// Cancel context after brief delay
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// OK — operator terminated
	case <-time.After(2 * time.Second):
		t.Fatal("operator did not terminate after context cancellation")
	}
}

func TestOperator_RecursiveFanOut(t *testing.T) {
	t.Parallel()

	// Level 0 observer enqueues targets at depth 1.
	// The factory, when running a depth-1 item, uses its observer to enqueue depth-2 items.
	// With MaxDepth=2, depth-2 items should execute. Depth-3 should be skipped.

	var totalCalls atomic.Int64

	var factory ChildRunFactory //nolint:staticcheck // split decl+assign needed for recursive self-reference
	factory = func(ctx context.Context, item WorkItem, observer EnqueueObserver) (*RunResult, error) {
		totalCalls.Add(1)

		// At depth 1, enqueue a child (which will be depth 2)
		if item.Depth == 1 {
			observer(&types.EventEnvelope{
				Type: types.EventTypeEnqueue,
				Payload: map[string]any{
					"target": "leaf.ts",
					"params": map[string]any{"parent": item.RunID},
				},
			})
		}

		// At depth 2, try to enqueue (should be skipped at depth 3 > MaxDepth 2)
		if item.Depth == 2 {
			observer(&types.EventEnvelope{
				Type: types.EventTypeEnqueue,
				Payload: map[string]any{
					"target": "too-deep.ts",
					"params": map[string]any{"parent": item.RunID},
				},
			})
		}

		return &RunResult{
			RunMeta: &types.RunMeta{RunID: item.RunID, Attempt: 1},
			Outcome: &types.RunOutcome{Status: types.OutcomeSuccess, Message: "ok"},
		}, nil
	}

	operator := NewOperator(FanOutConfig{
		MaxDepth: 2,
		MaxRuns:  10,
		Parallel: 1,
	}, factory)

	observer := operator.NewObserver(0)
	observer(&types.EventEnvelope{
		Type: types.EventTypeEnqueue,
		Payload: map[string]any{
			"target": "list.ts",
			"params": map[string]any{},
		},
	})

	rootDone := make(chan struct{})
	close(rootDone)
	operator.Run(t.Context(), rootDone)

	result := operator.Results()

	// Expect: 1 (depth 1: list.ts) + 1 (depth 2: leaf.ts) = 2 runs
	if result.RunsTotal != 2 {
		t.Errorf("expected 2 recursive runs, got %d", result.RunsTotal)
	}
	if result.RunsSucceeded != 2 {
		t.Errorf("expected 2 succeeded, got %d", result.RunsSucceeded)
	}
	// The depth-3 enqueue should be skipped
	if result.EnqueueSkipped != 1 {
		t.Errorf("expected 1 skipped (depth limit), got %d", result.EnqueueSkipped)
	}
	if totalCalls.Load() != 2 {
		t.Errorf("expected 2 factory calls, got %d", totalCalls.Load())
	}
}

func TestOperator_NoEnqueueEvents(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	operator := NewOperator(FanOutConfig{
		MaxDepth: 3,
		MaxRuns:  10,
		Parallel: 1,
	}, successFactory(&calls))

	// Don't submit any items — bounds are not requirements

	rootDone := make(chan struct{})
	close(rootDone)
	operator.Run(t.Context(), rootDone)

	result := operator.Results()
	if result.RunsTotal != 0 {
		t.Errorf("expected 0 runs with no enqueue events, got %d", result.RunsTotal)
	}
	if calls.Load() != 0 {
		t.Errorf("expected 0 factory calls, got %d", calls.Load())
	}
}

func TestOperator_EnqueueSourceCategoryOverride(t *testing.T) {
	t.Parallel()

	var capturedItems []WorkItem

	factory := func(ctx context.Context, item WorkItem, observer EnqueueObserver) (*RunResult, error) {
		capturedItems = append(capturedItems, item)
		return &RunResult{
			RunMeta: &types.RunMeta{RunID: item.RunID, Attempt: 1},
			Outcome: &types.RunOutcome{Status: types.OutcomeSuccess, Message: "ok"},
		}, nil
	}

	operator := NewOperator(FanOutConfig{
		MaxDepth: 3,
		MaxRuns:  10,
		Parallel: 1,
	}, factory)

	observer := operator.NewObserver(0)

	// Enqueue with source/category overrides
	observer(&types.EventEnvelope{
		Type: types.EventTypeEnqueue,
		Payload: map[string]any{
			"target":   "detail.ts",
			"params":   map[string]any{"url": "https://example.com"},
			"source":   "other-source",
			"category": "premium",
		},
	})

	// Enqueue without overrides (should have empty strings)
	observer(&types.EventEnvelope{
		Type: types.EventTypeEnqueue,
		Payload: map[string]any{
			"target": "basic.ts",
			"params": map[string]any{"url": "https://basic.com"},
		},
	})

	rootDone := make(chan struct{})
	close(rootDone)
	operator.Run(t.Context(), rootDone)

	if len(capturedItems) != 2 {
		t.Fatalf("expected 2 items, got %d", len(capturedItems))
	}

	// First item should have overrides
	if capturedItems[0].Source != "other-source" {
		t.Errorf("expected source=other-source, got %q", capturedItems[0].Source)
	}
	if capturedItems[0].Category != "premium" {
		t.Errorf("expected category=premium, got %q", capturedItems[0].Category)
	}

	// Second item should have empty (inherit from parent)
	if capturedItems[1].Source != "" {
		t.Errorf("expected empty source for no-override item, got %q", capturedItems[1].Source)
	}
	if capturedItems[1].Category != "" {
		t.Errorf("expected empty category for no-override item, got %q", capturedItems[1].Category)
	}
}

func TestOperator_FailedChildRun(t *testing.T) {
	t.Parallel()

	factory := func(ctx context.Context, item WorkItem, observer EnqueueObserver) (*RunResult, error) {
		return &RunResult{
			RunMeta: &types.RunMeta{RunID: item.RunID, Attempt: 1},
			Outcome: &types.RunOutcome{Status: types.OutcomeScriptError, Message: "script failed"},
		}, nil
	}

	operator := NewOperator(FanOutConfig{
		MaxDepth: 3,
		MaxRuns:  5,
		Parallel: 1,
	}, factory)

	observer := operator.NewObserver(0)
	observer(&types.EventEnvelope{
		Type:    types.EventTypeEnqueue,
		Payload: map[string]any{"target": "failing.ts", "params": map[string]any{}},
	})

	rootDone := make(chan struct{})
	close(rootDone)
	operator.Run(t.Context(), rootDone)

	result := operator.Results()
	if result.RunsTotal != 1 {
		t.Errorf("expected 1 run, got %d", result.RunsTotal)
	}
	if result.RunsFailed != 1 {
		t.Errorf("expected 1 failed, got %d", result.RunsFailed)
	}
	if result.RunsSucceeded != 0 {
		t.Errorf("expected 0 succeeded, got %d", result.RunsSucceeded)
	}
}
