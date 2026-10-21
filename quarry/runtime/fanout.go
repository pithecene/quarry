package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/pithecene/quarry/types"
)

// FanOutConfig configures the fan-out operator.
type FanOutConfig struct {
	// MaxDepth is the maximum recursion depth for fan-out (root = depth 0).
	MaxDepth int
	// MaxRuns is the maximum total child runs to execute.
	MaxRuns int
	// Parallel is the maximum concurrent child runs.
	Parallel int
}

// FanOutResult aggregates fan-out execution statistics.
type FanOutResult struct {
	// RunsTotal is the number of child runs executed.
	RunsTotal int64
	// RunsSucceeded is the number of child runs that completed successfully.
	RunsSucceeded int64
	// RunsFailed is the number of child runs that failed.
	RunsFailed int64
	// EnqueueReceived is the total number of enqueue events observed.
	EnqueueReceived int64
	// EnqueueDeduped is the number of enqueue events skipped due to dedup.
	EnqueueDeduped int64
	// EnqueueSkipped is the number of enqueue events skipped due to depth/max-runs limits.
	EnqueueSkipped int64
	// ChildResults holds the result of each child run, keyed by run_id.
	ChildResults map[string]*RunResult
}

// WorkItem represents a unit of derived work to execute.
type WorkItem struct {
	// Target is the script path for the child run.
	Target string
	// Params is the job payload for the child run.
	Params map[string]any
	// Depth is the recursion depth of this item (root children = 1).
	Depth int
	// DedupKey is the precomputed dedup key.
	DedupKey string
	// RunID is the assigned run_id for the child run.
	RunID string
	// Source is an optional partition override for the child run's source.
	// Empty string means inherit from parent.
	Source string
	// Category is an optional partition override for the child run's category.
	// Empty string means inherit from parent.
	Category string
}

// ChildRunFactory creates and executes a child run, returning the result.
// The factory is responsible for building a RunConfig, creating a RunOrchestrator,
// and executing it. The observer parameter enables recursive fan-out.
type ChildRunFactory func(ctx context.Context, item WorkItem, observer EnqueueObserver) (*RunResult, error)

// Operator manages the fan-out work queue, dedup, and worker pool.
type Operator struct {
	config  FanOutConfig
	factory ChildRunFactory

	queue chan WorkItem
	seen  map[string]struct{}
	mu    sync.Mutex

	runsStarted  atomic.Int64
	runsFinished atomic.Int64
	succeeded    atomic.Int64
	failed       atomic.Int64
	received     atomic.Int64
	deduped      atomic.Int64
	skipped      atomic.Int64

	resultsMu    sync.Mutex
	childResults map[string]*RunResult
}

// NewOperator creates a new fan-out operator.
func NewOperator(config FanOutConfig, factory ChildRunFactory) *Operator {
	return &Operator{
		config:       config,
		factory:      factory,
		queue:        make(chan WorkItem, config.MaxRuns),
		seen:         make(map[string]struct{}),
		childResults: make(map[string]*RunResult),
	}
}

// NewObserver returns an EnqueueObserver bound to a specific depth.
// The observer intercepts enqueue events, computes a dedup key, and
// submits work items to the operator queue.
func (s *Operator) NewObserver(depth int) EnqueueObserver {
	return func(envelope *types.EventEnvelope) {
		s.received.Add(1)

		target, _ := envelope.Payload["target"].(string)
		if target == "" {
			s.skipped.Add(1)
			return
		}

		params, _ := envelope.Payload["params"].(map[string]any)
		if params == nil {
			params = map[string]any{}
		}

		childDepth := depth + 1
		if childDepth > s.config.MaxDepth {
			s.skipped.Add(1)
			return
		}

		dedupKey := computeDedupKey(target, params)

		s.mu.Lock()
		if _, exists := s.seen[dedupKey]; exists {
			s.mu.Unlock()
			s.deduped.Add(1)
			return
		}

		// Check max-runs before committing the slot.
		// Both dedup and slot reservation are under the same mutex,
		// so a single check is sufficient.
		if s.runsStarted.Load() >= int64(s.config.MaxRuns) {
			s.mu.Unlock()
			s.skipped.Add(1)
			return
		}

		s.seen[dedupKey] = struct{}{}
		s.runsStarted.Add(1)
		s.mu.Unlock()

		source, _ := envelope.Payload["source"].(string)
		category, _ := envelope.Payload["category"].(string)

		item := WorkItem{
			Target:   target,
			Params:   params,
			Depth:    childDepth,
			DedupKey: dedupKey,
			RunID:    uuid.New().String(),
			Source:   source,
			Category: category,
		}

		// Non-blocking send; queue is sized to MaxRuns.
		select {
		case s.queue <- item:
		default:
			// Queue full — should not happen since queue capacity == MaxRuns
			s.skipped.Add(1)
			s.runsStarted.Add(-1)
		}
	}
}

// Run executes the operator worker pool.
// It reads from the work queue and spawns child runs up to the concurrency limit.
// Terminates when rootDone is closed AND the queue is drained AND all workers are idle.
func (s *Operator) Run(ctx context.Context, rootDone <-chan struct{}) {
	sem := make(chan struct{}, s.config.Parallel)
	var wg sync.WaitGroup

	// workerDone is signaled each time a worker completes, used to
	// re-check termination conditions without busy-spinning.
	workerDone := make(chan struct{}, s.config.MaxRuns)

	dispatch := func(item WorkItem) {
		wg.Add(1)
		go func(wi WorkItem) {
			defer wg.Done()
			defer func() {
				<-sem
				// Signal that a worker finished so the main loop can re-check.
				select {
				case workerDone <- struct{}{}:
				default:
				}
			}()

			childObserver := s.NewObserver(wi.Depth)
			result, err := s.factory(ctx, wi, childObserver)
			s.runsFinished.Add(1)

			s.resultsMu.Lock()
			if err != nil || result == nil {
				s.failed.Add(1)
				if result != nil {
					s.childResults[wi.RunID] = result
				}
			} else {
				s.childResults[wi.RunID] = result
				if result.Outcome.Status == types.OutcomeSuccess {
					s.succeeded.Add(1)
				} else {
					s.failed.Add(1)
				}
			}
			s.resultsMu.Unlock()
		}(item)
	}

	rootFinished := false
	for {
		// Try to drain the queue non-blocking first.
		drained := false
		for !drained {
			select {
			case item := <-s.queue:
				// Acquire semaphore (bounded concurrency).
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					wg.Wait()
					return
				}
				dispatch(item)
			default:
				drained = true
			}
		}

		// Queue is empty. Check termination conditions.
		if !rootFinished {
			select {
			case <-rootDone:
				rootFinished = true
			default:
			}
		}

		if rootFinished {
			// Root is done. Wait for all workers, then check if they enqueued more.
			wg.Wait()
			if len(s.queue) == 0 {
				return
			}
			// Workers enqueued more items — continue draining.
			continue
		}

		// Root still running — block until new work, root completion, worker completion, or cancel.
		select {
		case item := <-s.queue:
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				wg.Wait()
				return
			}
			dispatch(item)
		case <-rootDone:
			rootFinished = true
		case <-workerDone:
			// A worker finished — re-check queue (it may have enqueued items).
		case <-ctx.Done():
			wg.Wait()
			return
		}
	}
}

// Results returns the aggregate fan-out statistics.
func (s *Operator) Results() FanOutResult {
	s.resultsMu.Lock()
	defer s.resultsMu.Unlock()

	// Copy the map to avoid external mutation
	results := make(map[string]*RunResult, len(s.childResults))
	for k, v := range s.childResults {
		results[k] = v
	}

	return FanOutResult{
		RunsTotal:       s.runsFinished.Load(),
		RunsSucceeded:   s.succeeded.Load(),
		RunsFailed:      s.failed.Load(),
		EnqueueReceived: s.received.Load(),
		EnqueueDeduped:  s.deduped.Load(),
		EnqueueSkipped:  s.skipped.Load(),
		ChildResults:    results,
	}
}

// computeDedupKey produces a deterministic key from target + params.
// Dedup is by (target, params) only. source/category are partition hints,
// not work identity — the same work is not re-executed for different partitions.
// Go's json.Marshal sorts map keys deterministically since Go 1.12.
func computeDedupKey(target string, params map[string]any) string {
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		// Fallback: use target only (should not happen for map[string]any)
		paramsJSON = []byte("{}")
	}

	h := sha256.New()
	h.Write([]byte(target))
	h.Write([]byte{0x00}) // separator
	h.Write(paramsJSON)
	return hex.EncodeToString(h.Sum(nil))
}

// PrintFanOutSummary prints a human-readable fan-out summary to stdout.
func PrintFanOutSummary(result FanOutResult) {
	fmt.Printf("\n=== Fan-Out Summary ===\n")
	fmt.Printf("Child Runs:       %d total, %d succeeded, %d failed\n",
		result.RunsTotal, result.RunsSucceeded, result.RunsFailed)
	fmt.Printf("Enqueue Events:   %d received, %d deduped, %d skipped\n",
		result.EnqueueReceived, result.EnqueueDeduped, result.EnqueueSkipped)

	if len(result.ChildResults) > 0 {
		fmt.Printf("\n--- Child Run Results ---\n")
		runIDs := make([]string, 0, len(result.ChildResults))
		for id := range result.ChildResults {
			runIDs = append(runIDs, id)
		}
		sort.Strings(runIDs)
		for _, runID := range runIDs {
			res := result.ChildResults[runID]
			fmt.Printf("  %s: outcome=%s, events=%d, duration=%s\n",
				runID, res.Outcome.Status, res.EventCount, res.Duration)
		}
	}
}
