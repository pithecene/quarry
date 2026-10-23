package reader

import (
	"testing"
)

// TestInspectRunResponse verifies the response shape matches CONTRACT_CLI.md.
func TestInspectRunResponse(t *testing.T) {
	t.Parallel()

	resp := InspectRun("test-run")

	if resp.RunID != "test-run" {
		t.Errorf("RunID = %q, want %q", resp.RunID, "test-run")
	}
	if resp.JobID == "" {
		t.Error("JobID should not be empty")
	}
	if resp.State == "" {
		t.Error("State should not be empty")
	}
	if resp.Attempt < 1 {
		t.Errorf("Attempt = %d, should be >= 1", resp.Attempt)
	}
	if resp.Policy == "" {
		t.Error("Policy should not be empty")
	}
	if resp.StartedAt.IsZero() {
		t.Error("StartedAt should not be zero")
	}
}

// TestInspectJobResponse verifies the response shape.
func TestInspectJobResponse(t *testing.T) {
	t.Parallel()

	resp := InspectJob("test-job")

	if resp.JobID != "test-job" {
		t.Errorf("JobID = %q, want %q", resp.JobID, "test-job")
	}
	if resp.State == "" {
		t.Error("State should not be empty")
	}
	// RunIDs may be empty but should not be nil
	if resp.RunIDs == nil {
		t.Error("RunIDs should not be nil")
	}
}

// TestInspectProxyResponse verifies proxy pool response shape.
func TestInspectProxyResponse(t *testing.T) {
	t.Parallel()

	resp := InspectProxy("test-pool")

	if resp.Name != "test-pool" {
		t.Errorf("Name = %q, want %q", resp.Name, "test-pool")
	}
	if resp.Strategy == "" {
		t.Error("Strategy should not be empty")
	}
	if resp.EndpointCnt < 0 {
		t.Errorf("EndpointCnt = %d, should be >= 0", resp.EndpointCnt)
	}
}

// TestStatsRunsResponse verifies stats response shape.
func TestStatsRunsResponse(t *testing.T) {
	t.Parallel()

	resp := StatsRuns()

	if resp.Total < 0 {
		t.Errorf("Total = %d, should be >= 0", resp.Total)
	}
	if resp.Running < 0 {
		t.Errorf("Running = %d, should be >= 0", resp.Running)
	}
	if resp.Succeeded < 0 {
		t.Errorf("Succeeded = %d, should be >= 0", resp.Succeeded)
	}
	if resp.Failed < 0 {
		t.Errorf("Failed = %d, should be >= 0", resp.Failed)
	}
}

// TestStatsMetricsResponse verifies metrics snapshot response shape.
func TestStatsMetricsResponse(t *testing.T) {
	t.Parallel()

	resp := StatsMetrics()

	if resp == nil {
		t.Fatal("StatsMetrics() returned nil")
	}

	// Ts must not be empty per CONTRACT_CLI.md
	if resp.Ts == "" {
		t.Error("Ts should not be empty")
	}

	// All counters must be non-negative
	if resp.RunsStarted < 0 {
		t.Errorf("RunsStarted = %d, should be >= 0", resp.RunsStarted)
	}
	if resp.RunsCompleted < 0 {
		t.Errorf("RunsCompleted = %d, should be >= 0", resp.RunsCompleted)
	}
	if resp.RunsFailed < 0 {
		t.Errorf("RunsFailed = %d, should be >= 0", resp.RunsFailed)
	}
	if resp.RunsCrashed < 0 {
		t.Errorf("RunsCrashed = %d, should be >= 0", resp.RunsCrashed)
	}
	if resp.EventsReceived < 0 {
		t.Errorf("EventsReceived = %d, should be >= 0", resp.EventsReceived)
	}
	if resp.EventsPersisted < 0 {
		t.Errorf("EventsPersisted = %d, should be >= 0", resp.EventsPersisted)
	}
	if resp.EventsDropped < 0 {
		t.Errorf("EventsDropped = %d, should be >= 0", resp.EventsDropped)
	}
	if resp.ExecutorLaunchSuccess < 0 {
		t.Errorf("ExecutorLaunchSuccess = %d, should be >= 0", resp.ExecutorLaunchSuccess)
	}
	if resp.ExecutorLaunchFailure < 0 {
		t.Errorf("ExecutorLaunchFailure = %d, should be >= 0", resp.ExecutorLaunchFailure)
	}
	if resp.ExecutorCrash < 0 {
		t.Errorf("ExecutorCrash = %d, should be >= 0", resp.ExecutorCrash)
	}
	if resp.IPCDecodeErrors < 0 {
		t.Errorf("IPCDecodeErrors = %d, should be >= 0", resp.IPCDecodeErrors)
	}
	if resp.LodeWriteSuccess < 0 {
		t.Errorf("LodeWriteSuccess = %d, should be >= 0", resp.LodeWriteSuccess)
	}
	if resp.LodeWriteFailure < 0 {
		t.Errorf("LodeWriteFailure = %d, should be >= 0", resp.LodeWriteFailure)
	}
	if resp.LodeWriteRetry < 0 {
		t.Errorf("LodeWriteRetry = %d, should be >= 0", resp.LodeWriteRetry)
	}

	// DroppedByType must not be nil (stub returns populated map)
	if resp.DroppedByType == nil {
		t.Error("DroppedByType should not be nil")
	}
}

// TestListRunsLimit verifies that a limit is applied and that limit=0
// returns every stub item.
func TestListRunsLimit(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		limit, count int
	}{
		{"no limit", 0, 4},
		{"limit", 2, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := len(ListRuns(ListRunsOptions{Limit: tc.limit})); got != tc.count {
				t.Errorf("ListRuns with limit=%d returned %d items, expected %d", tc.limit, got, tc.count)
			}
		})
	}
}

// TestListRunsWithStateFilter verifies state filtering.
func TestListRunsWithStateFilter(t *testing.T) {
	t.Parallel()

	opts := ListRunsOptions{State: "succeeded"}
	results := ListRuns(opts)

	for _, r := range results {
		if r.State != "succeeded" {
			t.Errorf("Expected state 'succeeded', got %q", r.State)
		}
	}
}

// TestListRunItemShape verifies list item response shape.
func TestListRunItemShape(t *testing.T) {
	t.Parallel()

	results := ListRuns(ListRunsOptions{})

	if len(results) == 0 {
		t.Fatal("Expected at least one result")
	}

	item := results[0]
	if item.RunID == "" {
		t.Error("RunID should not be empty")
	}
	if item.State == "" {
		t.Error("State should not be empty")
	}
	if item.StartedAt.IsZero() {
		t.Error("StartedAt should not be zero")
	}
}

// TestDebugResolveProxyCommitted verifies committed flag is set.
func TestDebugResolveProxyCommitted(t *testing.T) {
	t.Parallel()

	// Without commit
	resp, err := DebugResolveProxy("test-pool", false)
	if err != nil {
		t.Fatalf("DebugResolveProxy failed: %v", err)
	}
	if resp.Committed {
		t.Error("Committed should be false when commit=false")
	}

	// With commit
	resp, err = DebugResolveProxy("test-pool", true)
	if err != nil {
		t.Fatalf("DebugResolveProxy failed: %v", err)
	}
	if !resp.Committed {
		t.Error("Committed should be true when commit=true")
	}
}

// TestDebugResolveProxyRequiresPool verifies pool validation.
func TestDebugResolveProxyRequiresPool(t *testing.T) {
	t.Parallel()

	_, err := DebugResolveProxy("", false)
	if err == nil {
		t.Error("Expected error for empty pool name")
	}
}

// TestDebugIPCResponse verifies IPC debug response shape.
func TestDebugIPCResponse(t *testing.T) {
	t.Parallel()

	resp := DebugIPC(false)

	if resp.Transport == "" {
		t.Error("Transport should not be empty")
	}
	if resp.Encoding == "" {
		t.Error("Encoding should not be empty")
	}
	if resp.MessagesSent < 0 {
		t.Errorf("MessagesSent = %d, should be >= 0", resp.MessagesSent)
	}
	if resp.Errors < 0 {
		t.Errorf("Errors = %d, should be >= 0", resp.Errors)
	}
}
