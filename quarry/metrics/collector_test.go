package metrics

import (
	"sync"
	"testing"
)

func TestCollector_IncrementMethods(t *testing.T) {
	c := NewCollector("strict", "node", "fs", "run-001", "job-001")

	c.IncRunStarted()
	c.IncRunCompleted()
	c.IncRunFailed()
	c.IncRunFailed()
	c.IncRunCrashed()
	c.IncExecutorLaunchSuccess()
	c.IncExecutorLaunchFailure()
	c.IncExecutorLaunchFailure()
	c.IncExecutorCrash()
	c.IncIPCDecodeErrors()
	c.IncIPCDecodeErrors()
	c.IncIPCDecodeErrors()
	c.IncLodeWriteSuccess()
	c.IncLodeWriteSuccess()
	c.IncLodeWriteFailure()

	s := c.Snapshot()

	if s.RunsStarted != 1 {
		t.Errorf("RunsStarted = %d, want 1", s.RunsStarted)
	}
	if s.RunsCompleted != 1 {
		t.Errorf("RunsCompleted = %d, want 1", s.RunsCompleted)
	}
	if s.RunsFailed != 2 {
		t.Errorf("RunsFailed = %d, want 2", s.RunsFailed)
	}
	if s.RunsCrashed != 1 {
		t.Errorf("RunsCrashed = %d, want 1", s.RunsCrashed)
	}
	if s.ExecutorLaunchSuccess != 1 {
		t.Errorf("ExecutorLaunchSuccess = %d, want 1", s.ExecutorLaunchSuccess)
	}
	if s.ExecutorLaunchFailure != 2 {
		t.Errorf("ExecutorLaunchFailure = %d, want 2", s.ExecutorLaunchFailure)
	}
	if s.ExecutorCrash != 1 {
		t.Errorf("ExecutorCrash = %d, want 1", s.ExecutorCrash)
	}
	if s.IPCDecodeErrors != 3 {
		t.Errorf("IPCDecodeErrors = %d, want 3", s.IPCDecodeErrors)
	}
	if s.LodeWriteSuccess != 2 {
		t.Errorf("LodeWriteSuccess = %d, want 2", s.LodeWriteSuccess)
	}
	if s.LodeWriteFailure != 1 {
		t.Errorf("LodeWriteFailure = %d, want 1", s.LodeWriteFailure)
	}
	if s.LodeWriteRetry != 0 {
		t.Errorf("LodeWriteRetry = %d, want 0", s.LodeWriteRetry)
	}
}

func TestCollector_Dimensions(t *testing.T) {
	c := NewCollector("buffered", "node", "s3", "run-42", "job-7")
	s := c.Snapshot()

	if s.Policy != "buffered" {
		t.Errorf("Policy = %q, want %q", s.Policy, "buffered")
	}
	if s.Executor != "node" {
		t.Errorf("Executor = %q, want %q", s.Executor, "node")
	}
	if s.StorageBackend != "s3" {
		t.Errorf("StorageBackend = %q, want %q", s.StorageBackend, "s3")
	}
	if s.RunID != "run-42" {
		t.Errorf("RunID = %q, want %q", s.RunID, "run-42")
	}
	if s.JobID != "job-7" {
		t.Errorf("JobID = %q, want %q", s.JobID, "job-7")
	}
}

func TestCollector_AbsorbPolicyStats(t *testing.T) {
	c := NewCollector("strict", "node", "fs", "run-001", "")

	droppedByType := map[string]int64{
		"log":          5,
		"enqueue":      2,
		"rotate_proxy": 1,
	}
	c.AbsorbPolicyStats(100, 92, 8, droppedByType, nil)

	s := c.Snapshot()

	if s.EventsReceived != 100 {
		t.Errorf("EventsReceived = %d, want 100", s.EventsReceived)
	}
	if s.EventsPersisted != 92 {
		t.Errorf("EventsPersisted = %d, want 92", s.EventsPersisted)
	}
	if s.EventsDropped != 8 {
		t.Errorf("EventsDropped = %d, want 8", s.EventsDropped)
	}
	if len(s.DroppedByType) != 3 {
		t.Errorf("DroppedByType has %d entries, want 3", len(s.DroppedByType))
	}
	if s.DroppedByType["log"] != 5 {
		t.Errorf("DroppedByType[log] = %d, want 5", s.DroppedByType["log"])
	}
	if s.DroppedByType["enqueue"] != 2 {
		t.Errorf("DroppedByType[enqueue] = %d, want 2", s.DroppedByType["enqueue"])
	}
	if s.DroppedByType["rotate_proxy"] != 1 {
		t.Errorf("DroppedByType[rotate_proxy] = %d, want 1", s.DroppedByType["rotate_proxy"])
	}
	if s.FlushTriggers != nil {
		t.Errorf("FlushTriggers should be nil when nil passed, got %v", s.FlushTriggers)
	}
}

func TestCollector_AbsorbPolicyStats_FlushTriggers(t *testing.T) {
	c := NewCollector("streaming", "node", "fs", "run-001", "")

	triggers := map[string]int64{"count": 3, "interval": 7, "termination": 1}
	c.AbsorbPolicyStats(100, 100, 0, nil, triggers)

	s := c.Snapshot()
	if s.FlushTriggers == nil {
		t.Fatal("FlushTriggers should be populated")
	}
	if s.FlushTriggers["count"] != 3 {
		t.Errorf("FlushTriggers[count] = %d, want 3", s.FlushTriggers["count"])
	}
	if s.FlushTriggers["interval"] != 7 {
		t.Errorf("FlushTriggers[interval] = %d, want 7", s.FlushTriggers["interval"])
	}
	if s.FlushTriggers["termination"] != 1 {
		t.Errorf("FlushTriggers[termination] = %d, want 1", s.FlushTriggers["termination"])
	}

	// Mutate original — collector should be isolated
	triggers["count"] = 999
	s2 := c.Snapshot()
	if s2.FlushTriggers["count"] != 3 {
		t.Errorf("FlushTriggers[count] = %d, want 3 (should be isolated)", s2.FlushTriggers["count"])
	}
}

func TestCollector_AbsorbPolicyStats_MapIsolation(t *testing.T) {
	c := NewCollector("strict", "node", "fs", "run-001", "")

	original := map[string]int64{"log": 5}
	c.AbsorbPolicyStats(10, 5, 5, original, nil)

	// Mutate the original map after absorption
	original["log"] = 999
	original["new_type"] = 100

	s := c.Snapshot()
	if s.DroppedByType["log"] != 5 {
		t.Errorf("DroppedByType[log] = %d, want 5 (should be isolated from caller mutation)", s.DroppedByType["log"])
	}
	if _, exists := s.DroppedByType["new_type"]; exists {
		t.Error("DroppedByType should not contain new_type added after absorption")
	}
}

func TestCollector_SnapshotImmutability(t *testing.T) {
	c := NewCollector("strict", "node", "fs", "run-001", "")
	c.IncRunStarted()
	c.IncLodeWriteSuccess()

	s1 := c.Snapshot()

	// Mutate collector after snapshot
	c.IncRunCompleted()
	c.IncLodeWriteSuccess()
	c.IncLodeWriteSuccess()

	// s1 should be unchanged
	if s1.RunsCompleted != 0 {
		t.Errorf("s1.RunsCompleted = %d, want 0 (snapshot should be frozen)", s1.RunsCompleted)
	}
	if s1.LodeWriteSuccess != 1 {
		t.Errorf("s1.LodeWriteSuccess = %d, want 1 (snapshot should be frozen)", s1.LodeWriteSuccess)
	}

	// New snapshot should reflect mutations
	s2 := c.Snapshot()
	if s2.RunsCompleted != 1 {
		t.Errorf("s2.RunsCompleted = %d, want 1", s2.RunsCompleted)
	}
	if s2.LodeWriteSuccess != 3 {
		t.Errorf("s2.LodeWriteSuccess = %d, want 3", s2.LodeWriteSuccess)
	}
}

func TestCollector_SnapshotDroppedByTypeIsolation(t *testing.T) {
	c := NewCollector("strict", "node", "fs", "run-001", "")
	c.AbsorbPolicyStats(10, 5, 5, map[string]int64{"log": 3}, nil)

	s := c.Snapshot()

	// Mutate the snapshot's map
	s.DroppedByType["log"] = 999
	s.DroppedByType["injected"] = 1

	// Collector should be unaffected
	s2 := c.Snapshot()
	if s2.DroppedByType["log"] != 3 {
		t.Errorf("DroppedByType[log] = %d, want 3 (collector should be isolated from snapshot mutation)", s2.DroppedByType["log"])
	}
	if _, exists := s2.DroppedByType["injected"]; exists {
		t.Error("DroppedByType should not contain injected key from snapshot mutation")
	}
}

func TestCollector_NilReceiverSafety(t *testing.T) {
	var c *Collector

	// None of these should panic
	c.IncRunStarted()
	c.IncRunCompleted()
	c.IncRunFailed()
	c.IncRunCrashed()
	c.IncExecutorLaunchSuccess()
	c.IncExecutorLaunchFailure()
	c.IncExecutorCrash()
	c.IncIPCDecodeErrors()
	c.IncLodeWriteSuccess()
	c.IncLodeWriteFailure()
	c.AbsorbPolicyStats(10, 8, 2, map[string]int64{"log": 2}, nil)

	s := c.Snapshot()
	if s.RunsStarted != 0 {
		t.Errorf("nil collector snapshot RunsStarted = %d, want 0", s.RunsStarted)
	}
	if s.DroppedByType != nil {
		t.Errorf("nil collector snapshot DroppedByType should be nil, got %v", s.DroppedByType)
	}
}

func TestCollector_ConcurrentAccess(t *testing.T) {
	c := NewCollector("strict", "node", "fs", "run-001", "")
	const goroutines = 10
	const iterations = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			for range iterations {
				c.IncRunStarted()
				c.IncLodeWriteSuccess()
				c.IncIPCDecodeErrors()
			}
		}()
	}

	wg.Wait()

	s := c.Snapshot()
	want := int64(goroutines * iterations)

	if s.RunsStarted != want {
		t.Errorf("RunsStarted = %d, want %d", s.RunsStarted, want)
	}
	if s.LodeWriteSuccess != want {
		t.Errorf("LodeWriteSuccess = %d, want %d", s.LodeWriteSuccess, want)
	}
	if s.IPCDecodeErrors != want {
		t.Errorf("IPCDecodeErrors = %d, want %d", s.IPCDecodeErrors, want)
	}
}

func TestCollector_ZeroValueSnapshot(t *testing.T) {
	c := NewCollector("strict", "node", "fs", "run-001", "")
	s := c.Snapshot()

	// All counters should be zero
	if s.RunsStarted != 0 || s.RunsCompleted != 0 || s.RunsFailed != 0 || s.RunsCrashed != 0 {
		t.Error("fresh collector should have zero run lifecycle counters")
	}
	if s.EventsReceived != 0 || s.EventsPersisted != 0 || s.EventsDropped != 0 {
		t.Error("fresh collector should have zero ingestion counters")
	}
	if s.ExecutorLaunchSuccess != 0 || s.ExecutorLaunchFailure != 0 || s.ExecutorCrash != 0 || s.IPCDecodeErrors != 0 {
		t.Error("fresh collector should have zero executor counters")
	}
	if s.LodeWriteSuccess != 0 || s.LodeWriteFailure != 0 || s.LodeWriteRetry != 0 {
		t.Error("fresh collector should have zero Lode counters")
	}
	if len(s.DroppedByType) != 0 {
		t.Errorf("fresh collector DroppedByType should be empty, got %v", s.DroppedByType)
	}
}

func TestCollector_IncLodeWriteRetry(t *testing.T) {
	c := NewCollector("strict", "test", "fs", "run-1", "")
	c.IncLodeWriteRetry()
	c.IncLodeWriteRetry()

	s := c.Snapshot()
	if s.LodeWriteRetry != 2 {
		t.Errorf("LodeWriteRetry = %d, want 2", s.LodeWriteRetry)
	}
	if s.LodeWriteSuccess != 0 || s.LodeWriteFailure != 0 {
		t.Errorf("retry must not touch success/failure: success=%d failure=%d", s.LodeWriteSuccess, s.LodeWriteFailure)
	}
}
