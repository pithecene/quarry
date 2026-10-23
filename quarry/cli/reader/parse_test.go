package reader

import (
	"strings"
	"testing"
)

func TestParseMetricsRecord(t *testing.T) {
	t.Parallel()

	// Simulate a JSON-round-tripped record (float64 values)
	record := map[string]any{
		"record_kind":                   "metrics",
		"ts":                            "2026-02-03T15:00:00Z",
		"runs_started_total":            float64(5),
		"runs_completed_total":          float64(4),
		"runs_failed_total":             float64(1),
		"runs_crashed_total":            float64(0),
		"events_received_total":         float64(100),
		"events_persisted_total":        float64(98),
		"events_dropped_total":          float64(2),
		"executor_launch_success_total": float64(5),
		"executor_launch_failure_total": float64(0),
		"executor_crash_total":          float64(0),
		"ipc_decode_errors_total":       float64(1),
		"lode_write_success_total":      float64(50),
		"lode_write_failure_total":      float64(0),
		"lode_write_retry_total":        float64(3),
		"policy":                        "buffered",
		"executor":                      "my-executor.js",
		"storage_backend":               "s3",
		"run_id":                        "run-abc",
		"job_id":                        "job-def",
		"dropped_by_type":               map[string]any{"log": float64(2)},
	}

	parsed, err := ParseMetricsRecord(record)
	if err != nil {
		t.Fatalf("ParseMetricsRecord failed: %v", err)
	}

	if parsed.Ts != "2026-02-03T15:00:00Z" {
		t.Errorf("Ts = %q, want %q", parsed.Ts, "2026-02-03T15:00:00Z")
	}
	if parsed.RunsStarted != 5 {
		t.Errorf("RunsStarted = %d, want 5", parsed.RunsStarted)
	}
	if parsed.RunsCompleted != 4 {
		t.Errorf("RunsCompleted = %d, want 4", parsed.RunsCompleted)
	}
	if parsed.RunsFailed != 1 {
		t.Errorf("RunsFailed = %d, want 1", parsed.RunsFailed)
	}
	if parsed.EventsReceived != 100 {
		t.Errorf("EventsReceived = %d, want 100", parsed.EventsReceived)
	}
	if parsed.EventsPersisted != 98 {
		t.Errorf("EventsPersisted = %d, want 98", parsed.EventsPersisted)
	}
	if parsed.EventsDropped != 2 {
		t.Errorf("EventsDropped = %d, want 2", parsed.EventsDropped)
	}
	if parsed.IPCDecodeErrors != 1 {
		t.Errorf("IPCDecodeErrors = %d, want 1", parsed.IPCDecodeErrors)
	}
	if parsed.LodeWriteSuccess != 50 {
		t.Errorf("LodeWriteSuccess = %d, want 50", parsed.LodeWriteSuccess)
	}
	if parsed.LodeWriteRetry != 3 {
		t.Errorf("LodeWriteRetry = %d, want 3", parsed.LodeWriteRetry)
	}
	if parsed.Policy != "buffered" {
		t.Errorf("Policy = %q, want %q", parsed.Policy, "buffered")
	}
	if parsed.Executor != "my-executor.js" {
		t.Errorf("Executor = %q, want %q", parsed.Executor, "my-executor.js")
	}
	if parsed.StorageBackend != "s3" {
		t.Errorf("StorageBackend = %q, want %q", parsed.StorageBackend, "s3")
	}
	if parsed.RunID != "run-abc" {
		t.Errorf("RunID = %q, want %q", parsed.RunID, "run-abc")
	}
	if parsed.JobID != "job-def" {
		t.Errorf("JobID = %q, want %q", parsed.JobID, "job-def")
	}
	if parsed.DroppedByType == nil {
		t.Fatal("DroppedByType should not be nil")
	}
	if parsed.DroppedByType["log"] != 2 {
		t.Errorf("DroppedByType[log] = %d, want 2", parsed.DroppedByType["log"])
	}
}

func TestParseMetricsRecord_NilRecord(t *testing.T) {
	t.Parallel()

	_, err := ParseMetricsRecord(nil)
	if err == nil {
		t.Error("expected error for nil record")
	}
}

func TestParseMetricsRecord_MissingRequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		record map[string]any
		errMsg string
	}{
		{
			name:   "missing ts",
			record: map[string]any{"record_kind": "metrics", "run_id": "run-1", "policy": "strict", "executor": "e.js", "storage_backend": "fs"},
			errMsg: "ts",
		},
		{
			name:   "missing run_id",
			record: map[string]any{"record_kind": "metrics", "ts": "2026-02-03T15:00:00Z", "policy": "strict", "executor": "e.js", "storage_backend": "fs"},
			errMsg: "run_id",
		},
		{
			name:   "missing policy",
			record: map[string]any{"record_kind": "metrics", "ts": "2026-02-03T15:00:00Z", "run_id": "run-1", "executor": "e.js", "storage_backend": "fs"},
			errMsg: "policy",
		},
		{
			name:   "missing executor",
			record: map[string]any{"record_kind": "metrics", "ts": "2026-02-03T15:00:00Z", "run_id": "run-1", "policy": "strict", "storage_backend": "fs"},
			errMsg: "executor",
		},
		{
			name:   "missing storage_backend",
			record: map[string]any{"record_kind": "metrics", "ts": "2026-02-03T15:00:00Z", "run_id": "run-1", "policy": "strict", "executor": "e.js"},
			errMsg: "storage_backend",
		},
		{
			name:   "all required missing",
			record: map[string]any{"record_kind": "metrics"},
			errMsg: "ts",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseMetricsRecord(tt.record)
			if err == nil {
				t.Fatal("expected error for missing required field, got nil")
			}
			if !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("error = %q, want it to mention %q", err.Error(), tt.errMsg)
			}
		})
	}
}

func TestParseMetricsRecord_Ts(t *testing.T) {
	t.Parallel()

	record := map[string]any{
		"record_kind":     "metrics",
		"ts":              "2026-02-03T15:30:00Z",
		"run_id":          "run-1",
		"policy":          "strict",
		"executor":        "executor.js",
		"storage_backend": "fs",
	}

	parsed, err := ParseMetricsRecord(record)
	if err != nil {
		t.Fatalf("ParseMetricsRecord failed: %v", err)
	}
	if parsed.Ts != "2026-02-03T15:30:00Z" {
		t.Errorf("Ts = %q, want %q", parsed.Ts, "2026-02-03T15:30:00Z")
	}
}
