// Package metrics provides per-run metrics collection per CONTRACT_METRICS.md.
//
// The Collector accumulates counters during a single run. It is a leaf package
// with no internal dependencies. Ingestion policy metrics are absorbed from
// policy.Stats at run completion rather than recorded live, avoiding double-counting.
package metrics

import (
	"maps"
	"sync"
)

// Snapshot is an immutable point-in-time view of all contract-required metrics.
// Returned by Collector.Snapshot(). Safe to read concurrently after creation.
type Snapshot struct {
	// Run lifecycle
	RunsStarted   int64
	RunsCompleted int64
	RunsFailed    int64
	RunsCrashed   int64

	// Ingestion (absorbed from policy.Stats at run completion)
	EventsReceived  int64
	EventsPersisted int64
	EventsDropped   int64
	DroppedByType   map[string]int64
	FlushTriggers   map[string]int64 // streaming policy per-trigger flush counts; nil for non-streaming

	// Executor
	ExecutorLaunchSuccess int64
	ExecutorLaunchFailure int64
	ExecutorCrash         int64
	IPCDecodeErrors       int64

	// Lode / Storage
	LodeWriteSuccess int64
	LodeWriteFailure int64
	LodeWriteRetry   int64 // CAS retries observed via lode.WithRetryObserver

	// Dimensions (informational, set at construction)
	Policy         string
	Executor       string
	StorageBackend string
	RunID          string
	JobID          string
}

// Collector accumulates metrics during a single run.
// Thread-safe via sync.Mutex. All increment methods are nil-receiver safe.
type Collector struct {
	mu sync.Mutex

	// Run lifecycle
	runsStarted   int64
	runsCompleted int64
	runsFailed    int64
	runsCrashed   int64

	// Executor
	executorLaunchSuccess int64
	executorLaunchFailure int64
	executorCrash         int64
	ipcDecodeErrors       int64

	// Lode / Storage
	lodeWriteSuccess int64
	lodeWriteFailure int64
	lodeWriteRetry   int64

	// Ingestion (set once via AbsorbPolicyStats)
	eventsReceived  int64
	eventsPersisted int64
	eventsDropped   int64
	droppedByType   map[string]int64
	flushTriggers   map[string]int64

	// Dimensions
	policy         string
	executor       string
	storageBackend string
	runID          string
	jobID          string
}

// NewCollector creates a Collector with dimension labels.
// All dimensions are required per CONTRACT_METRICS.md (policy, executor, storage_backend).
// runID and jobID are optional dimensions.
func NewCollector(policy, executor, storageBackend, runID, jobID string) *Collector {
	return &Collector{
		droppedByType:  make(map[string]int64),
		policy:         policy,
		executor:       executor,
		storageBackend: storageBackend,
		runID:          runID,
		jobID:          jobID,
	}
}

// --- Run lifecycle ---

// IncRunStarted records a run start.
func (c *Collector) IncRunStarted() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.runsStarted++
	c.mu.Unlock()
}

// IncRunCompleted records a successful run completion.
func (c *Collector) IncRunCompleted() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.runsCompleted++
	c.mu.Unlock()
}

// IncRunFailed records a run failure (script_error or policy_failure).
func (c *Collector) IncRunFailed() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.runsFailed++
	c.mu.Unlock()
}

// IncRunCrashed records a run crash (executor_crash).
func (c *Collector) IncRunCrashed() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.runsCrashed++
	c.mu.Unlock()
}

// --- Executor ---

// IncExecutorLaunchSuccess records a successful executor launch.
func (c *Collector) IncExecutorLaunchSuccess() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.executorLaunchSuccess++
	c.mu.Unlock()
}

// IncExecutorLaunchFailure records a failed executor launch.
func (c *Collector) IncExecutorLaunchFailure() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.executorLaunchFailure++
	c.mu.Unlock()
}

// IncExecutorCrash records an executor crash detected during ingestion.
func (c *Collector) IncExecutorCrash() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.executorCrash++
	c.mu.Unlock()
}

// IncIPCDecodeErrors records an IPC frame decode error.
func (c *Collector) IncIPCDecodeErrors() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.ipcDecodeErrors++
	c.mu.Unlock()
}

// --- Lode / Storage ---
// Lode counters are per-call, not per-record. A single WriteEvents call
// with N events counts as 1 success. Per-event granularity is tracked
// separately by policy.Stats (events_persisted_total).

// IncLodeWriteSuccess records a successful Lode write operation (per-call).
func (c *Collector) IncLodeWriteSuccess() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.lodeWriteSuccess++
	c.mu.Unlock()
}

// IncLodeWriteFailure records a failed Lode write operation (per-call).
func (c *Collector) IncLodeWriteFailure() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.lodeWriteFailure++
	c.mu.Unlock()
}

// IncLodeWriteRetry records one Lode CAS retry attempt. Lode invokes the
// retry observer once per retry (never for the initial attempt), so this
// counts retries, not write calls.
func (c *Collector) IncLodeWriteRetry() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.lodeWriteRetry++
	c.mu.Unlock()
}

// --- Ingestion (absorbed from policy.Stats) ---

// AbsorbPolicyStats copies ingestion counters from policy.Stats into the collector.
// Called once after run completion with the final policy stats snapshot.
// The droppedByType and flushTriggers map keys are string-typed to keep this
// package free of dependencies on the types package.
// flushTriggers may be nil (non-streaming policies).
func (c *Collector) AbsorbPolicyStats(totalEvents, persisted, dropped int64, droppedByType, flushTriggers map[string]int64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.eventsReceived = totalEvents
	c.eventsPersisted = persisted
	c.eventsDropped = dropped
	c.droppedByType = make(map[string]int64, len(droppedByType))
	maps.Copy(c.droppedByType, droppedByType)
	if flushTriggers != nil {
		c.flushTriggers = make(map[string]int64, len(flushTriggers))
		maps.Copy(c.flushTriggers, flushTriggers)
	} else {
		c.flushTriggers = nil
	}
	c.mu.Unlock()
}

// --- Snapshot ---

// Snapshot returns an immutable point-in-time view of all metrics.
// The returned Snapshot is safe to read concurrently; the Collector can
// continue to be mutated independently.
func (c *Collector) Snapshot() Snapshot {
	if c == nil {
		return Snapshot{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	dropped := make(map[string]int64, len(c.droppedByType))
	maps.Copy(dropped, c.droppedByType)

	var triggers map[string]int64
	if c.flushTriggers != nil {
		triggers = make(map[string]int64, len(c.flushTriggers))
		maps.Copy(triggers, c.flushTriggers)
	}

	return Snapshot{
		RunsStarted:   c.runsStarted,
		RunsCompleted: c.runsCompleted,
		RunsFailed:    c.runsFailed,
		RunsCrashed:   c.runsCrashed,

		EventsReceived:  c.eventsReceived,
		EventsPersisted: c.eventsPersisted,
		EventsDropped:   c.eventsDropped,
		DroppedByType:   dropped,
		FlushTriggers:   triggers,

		ExecutorLaunchSuccess: c.executorLaunchSuccess,
		ExecutorLaunchFailure: c.executorLaunchFailure,
		ExecutorCrash:         c.executorCrash,
		IPCDecodeErrors:       c.ipcDecodeErrors,

		LodeWriteSuccess: c.lodeWriteSuccess,
		LodeWriteFailure: c.lodeWriteFailure,
		LodeWriteRetry:   c.lodeWriteRetry,

		Policy:         c.policy,
		Executor:       c.executor,
		StorageBackend: c.storageBackend,
		RunID:          c.runID,
		JobID:          c.jobID,
	}
}
