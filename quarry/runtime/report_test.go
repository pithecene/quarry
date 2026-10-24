package runtime

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pithecene/quarry/internal/testutil"
	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/policy"
	"github.com/pithecene/quarry/types"
)

func newTestRunResult() *RunResult {
	jobID := "job-001"
	return &RunResult{
		RunMeta: &types.RunMeta{
			RunID:   "run-001",
			JobID:   &jobID,
			Attempt: 1,
		},
		Outcome: &types.RunOutcome{
			Status:  types.OutcomeSuccess,
			Message: "run completed successfully",
		},
		Duration:   5 * time.Second,
		EventCount: 42,
		PolicyStats: policy.Stats{
			TotalEvents:     42,
			EventsPersisted: 42,
			EventsDropped:   0,
			FlushTriggers:   map[string]int64{"interval": 3, "termination": 1},
		},
		ArtifactStats: ArtifactStats{
			TotalArtifacts:     5,
			CommittedArtifacts: 5,
			OrphanedArtifacts:  0,
			TotalChunks:        10,
			TotalBytes:         524288,
		},
		StderrOutput: "",
		TerminalSummary: map[string]any{
			"items": float64(42),
		},
	}
}

func newTestSnapshot() metrics.Snapshot {
	return metrics.Snapshot{
		RunsStarted:           1,
		RunsCompleted:         1,
		EventsReceived:        42,
		EventsPersisted:       42,
		ExecutorLaunchSuccess: 1,
		LodeWriteSuccess:      5,
		Policy:                "streaming",
		Executor:              "executor.mjs",
		StorageBackend:        "fs",
		RunID:                 "run-001",
		JobID:                 "job-001",
	}
}

func TestBuildRunReport_Success(t *testing.T) {
	t.Parallel()

	result := newTestRunResult()
	snap := newTestSnapshot()

	report := BuildRunReport(result, snap, "streaming", 0)

	if report.RunID != "run-001" {
		t.Errorf("RunID = %q, want %q", report.RunID, "run-001")
	}
	if report.JobID != "job-001" {
		t.Errorf("JobID = %q, want %q", report.JobID, "job-001")
	}
	if report.Attempt != 1 {
		t.Errorf("Attempt = %d, want 1", report.Attempt)
	}
	if report.Outcome != types.OutcomeSuccess {
		t.Errorf("Outcome = %q, want %q", report.Outcome, types.OutcomeSuccess)
	}
	if report.ExitCode != 0 {
		t.Errorf("ExitCode = %d, want 0", report.ExitCode)
	}
	if report.DurationMs != 5000 {
		t.Errorf("DurationMs = %d, want 5000", report.DurationMs)
	}
	if report.EventCount != 42 {
		t.Errorf("EventCount = %d, want 42", report.EventCount)
	}
	if report.Policy.Name != "streaming" {
		t.Errorf("Policy.Name = %q, want %q", report.Policy.Name, "streaming")
	}
	if report.Policy.EventsReceived != 42 {
		t.Errorf("Policy.EventsReceived = %d, want 42", report.Policy.EventsReceived)
	}
	if report.Artifacts.Total != 5 {
		t.Errorf("Artifacts.Total = %d, want 5", report.Artifacts.Total)
	}
	if report.Artifacts.Committed != 5 {
		t.Errorf("Artifacts.Committed = %d, want 5", report.Artifacts.Committed)
	}
	if report.TerminalSummary == nil {
		t.Error("TerminalSummary is nil, want non-nil")
	}
	if (*report.TerminalSummary)["items"] != float64(42) {
		t.Errorf("TerminalSummary[items] = %v, want 42", (*report.TerminalSummary)["items"])
	}
}

func TestBuildRunReport_ScriptError(t *testing.T) {
	t.Parallel()

	result := newTestRunResult()
	errType := "TypeError"
	stack := "Error: oops\n  at script.ts:10"
	result.Outcome = &types.RunOutcome{
		Status:    types.OutcomeScriptError,
		Message:   "script error: oops",
		ErrorType: &errType,
		Stack:     &stack,
	}
	result.StderrOutput = "some stderr output"

	snap := newTestSnapshot()
	report := BuildRunReport(result, snap, "strict", 1)

	if report.Outcome != types.OutcomeScriptError {
		t.Errorf("Outcome = %q, want %q", report.Outcome, types.OutcomeScriptError)
	}
	if report.ExitCode != 1 {
		t.Errorf("ExitCode = %d, want 1", report.ExitCode)
	}
	if report.Stderr != "some stderr output" {
		t.Errorf("Stderr = %q, want %q", report.Stderr, "some stderr output")
	}
}

func TestBuildRunReport_NoJobID(t *testing.T) {
	t.Parallel()

	result := newTestRunResult()
	result.RunMeta.JobID = nil

	snap := newTestSnapshot()
	report := BuildRunReport(result, snap, "strict", 0)

	if report.JobID != "" {
		t.Errorf("JobID = %q, want empty", report.JobID)
	}

	// Verify omitempty: marshal and check no job_id key
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if _, exists := raw["job_id"]; exists {
		t.Error("job_id should be omitted when empty")
	}
}

func TestBuildRunReport_ProxyUsed(t *testing.T) {
	t.Parallel()

	result := newTestRunResult()
	username := "user1"
	result.ProxyUsed = &types.ProxyEndpointRedacted{
		Protocol: types.ProxyProtocolHTTP,
		Host:     "proxy.example.com",
		Port:     8080,
		Username: &username,
	}

	snap := newTestSnapshot()
	report := BuildRunReport(result, snap, "strict", 0)

	if report.ProxyUsed == nil {
		t.Fatal("ProxyUsed is nil, want non-nil")
	}
	if report.ProxyUsed.Host != "proxy.example.com" {
		t.Errorf("ProxyUsed.Host = %q, want %q", report.ProxyUsed.Host, "proxy.example.com")
	}
	if report.ProxyUsed.Port != 8080 {
		t.Errorf("ProxyUsed.Port = %d, want 8080", report.ProxyUsed.Port)
	}
}

func TestWriteRunReport_File(t *testing.T) {
	t.Parallel()

	result := newTestRunResult()
	snap := newTestSnapshot()
	report := BuildRunReport(result, snap, "strict", 0)

	dir := t.TempDir()
	path := filepath.Join(dir, "report.json")

	if err := WriteRunReport(report, path); err != nil {
		t.Fatalf("WriteRunReport failed: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read report: %v", err)
	}

	var decoded RunReport
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal report: %v", err)
	}

	if decoded.RunID != "run-001" {
		t.Errorf("decoded RunID = %q, want %q", decoded.RunID, "run-001")
	}
	if decoded.Outcome != types.OutcomeSuccess {
		t.Errorf("decoded Outcome = %q, want %q", decoded.Outcome, types.OutcomeSuccess)
	}
}

func TestWriteRunReport_EmptyPath(t *testing.T) {
	t.Parallel()

	report := &RunReport{}
	err := WriteRunReport(report, "")
	if err == nil {
		t.Fatal("expected error for empty path")
	}
}

func TestWriteRunReportTo_Writer(t *testing.T) {
	t.Parallel()

	result := newTestRunResult()
	snap := newTestSnapshot()
	report := BuildRunReport(result, snap, "strict", 0)

	var buf bytes.Buffer
	if err := writeRunReportTo(report, &buf); err != nil {
		t.Fatalf("writeRunReportTo failed: %v", err)
	}

	var decoded RunReport
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if decoded.RunID != "run-001" {
		t.Errorf("decoded RunID = %q, want %q", decoded.RunID, "run-001")
	}
}

func TestRunReport_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	result := newTestRunResult()
	snap := newTestSnapshot()
	report := BuildRunReport(result, snap, "streaming", 0)

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	// Verify key fields exist in JSON
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	requiredKeys := []string{
		"run_id", "attempt", "outcome", "message", "exit_code",
		"duration_ms", "event_count", "policy", "artifacts", "metrics",
	}
	for _, key := range requiredKeys {
		if _, exists := raw[key]; !exists {
			t.Errorf("missing required key %q in report JSON", key)
		}
	}

	// Verify policy sub-object
	policyObj, ok := raw["policy"].(map[string]any)
	if !ok {
		t.Fatal("policy is not an object")
	}
	policyKeys := []string{"name", "events_received", "events_persisted", "events_dropped"}
	for _, key := range policyKeys {
		if _, exists := policyObj[key]; !exists {
			t.Errorf("missing required key %q in policy sub-object", key)
		}
	}

	// Verify artifacts sub-object
	artifactsObj, ok := raw["artifacts"].(map[string]any)
	if !ok {
		t.Fatal("artifacts is not an object")
	}
	artifactKeys := []string{"total", "committed", "orphaned", "chunks", "bytes"}
	for _, key := range artifactKeys {
		if _, exists := artifactsObj[key]; !exists {
			t.Errorf("missing required key %q in artifacts sub-object", key)
		}
	}
}

func TestBuildRunReport_EmptyTerminalPayload(t *testing.T) {
	t.Parallel()

	// A terminal event with an empty payload ({}) must produce
	// "terminal_summary": {} in JSON, not omit the field.
	result := newTestRunResult()
	result.TerminalSummary = map[string]any{}

	snap := newTestSnapshot()
	report := BuildRunReport(result, snap, "strict", 0)

	if report.TerminalSummary == nil {
		t.Fatal("TerminalSummary pointer is nil; empty payload should produce non-nil pointer")
	}

	data, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	ts, exists := raw["terminal_summary"]
	if !exists {
		t.Fatal("terminal_summary key missing from JSON; empty payload must still be present")
	}
	tsMap, ok := ts.(map[string]any)
	if !ok {
		t.Fatalf("terminal_summary is %T, want map", ts)
	}
	if len(tsMap) != 0 {
		t.Errorf("terminal_summary has %d keys, want 0", len(tsMap))
	}
}

func TestBuildRunReport_NoTerminalEvent(t *testing.T) {
	t.Parallel()

	// When no terminal event was received, terminal_summary must be omitted.
	result := newTestRunResult()
	result.TerminalSummary = nil

	snap := newTestSnapshot()
	report := BuildRunReport(result, snap, "strict", 0)

	if report.TerminalSummary != nil {
		t.Fatal("TerminalSummary pointer should be nil when no terminal event")
	}

	data, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if _, exists := raw["terminal_summary"]; exists {
		t.Error("terminal_summary should be omitted when no terminal event was received")
	}
}

func TestWriteRunReport_Stderr(t *testing.T) {
	testutil.Serial(t, "swaps the process-wide os.Stderr")
	// Verify the "--report -" code path writes to stderr without error.
	// Redirect os.Stderr to a pipe so we can capture and verify output.
	origStderr := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create pipe: %v", err)
	}
	os.Stderr = w

	result := newTestRunResult()
	snap := newTestSnapshot()
	report := BuildRunReport(result, snap, "strict", 0)

	writeErr := WriteRunReport(report, "-")

	// Restore stderr before any assertions (so test failures print correctly)
	iox.DiscardClose(w)
	os.Stderr = origStderr

	if writeErr != nil {
		t.Fatalf("WriteRunReport to stderr failed: %v", writeErr)
	}

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(r); err != nil {
		t.Fatalf("failed to read from pipe: %v", err)
	}

	var decoded RunReport
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("stderr output is not valid JSON: %v\noutput: %s", err, buf.String())
	}

	if decoded.RunID != "run-001" {
		t.Errorf("decoded RunID = %q, want %q", decoded.RunID, "run-001")
	}
}
