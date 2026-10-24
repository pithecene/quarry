package runtime

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/types"
)

// RunReport is the structured JSON report written by --report.
// All fields use json tags matching the documented contract.
type RunReport struct {
	RunID      string              `json:"run_id"`
	JobID      string              `json:"job_id,omitempty"`
	Attempt    int                 `json:"attempt"`
	Outcome    types.OutcomeStatus `json:"outcome"`
	Message    string              `json:"message"`
	ExitCode   int                 `json:"exit_code"`
	DurationMs int64               `json:"duration_ms"`
	EventCount int64               `json:"event_count"`

	Policy    *ReportPolicy     `json:"policy"`
	Artifacts *ReportArtifacts  `json:"artifacts"`
	Metrics   *metrics.Snapshot `json:"metrics"`

	TerminalSummary *map[string]any              `json:"terminal_summary,omitempty"`
	ProxyUsed       *types.ProxyEndpointRedacted `json:"proxy_used,omitempty"`
	Stderr          string                       `json:"stderr,omitempty"`
	RunRecordError  string                       `json:"run_record_error,omitempty"`
}

// ReportPolicy holds policy stats in the report.
type ReportPolicy struct {
	Name            string           `json:"name"`
	EventsReceived  int64            `json:"events_received"`
	EventsPersisted int64            `json:"events_persisted"`
	EventsDropped   int64            `json:"events_dropped"`
	FlushTriggers   map[string]int64 `json:"flush_triggers,omitempty"`
}

// ReportArtifacts holds artifact stats in the report.
type ReportArtifacts struct {
	Total     int64 `json:"total"`
	Committed int64 `json:"committed"`
	Orphaned  int64 `json:"orphaned"`
	Chunks    int64 `json:"chunks"`
	Bytes     int64 `json:"bytes"`
}

// BuildRunReport composes a RunReport from a RunResult and metrics snapshot.
// The policyName is the policy name string (e.g. "strict", "buffered", "streaming").
// The exitCode is the process exit code that will be returned to the caller.
func BuildRunReport(result *RunResult, snap metrics.Snapshot, policyName string, exitCode int) *RunReport {
	report := &RunReport{
		RunID:      result.RunMeta.RunID,
		Attempt:    result.RunMeta.Attempt,
		Outcome:    result.Outcome.Status,
		Message:    result.Outcome.Message,
		ExitCode:   exitCode,
		DurationMs: result.Duration.Milliseconds(),
		EventCount: result.EventCount,
		Policy: &ReportPolicy{
			Name:            policyName,
			EventsReceived:  result.PolicyStats.TotalEvents,
			EventsPersisted: result.PolicyStats.EventsPersisted,
			EventsDropped:   result.PolicyStats.EventsDropped,
			FlushTriggers:   result.PolicyStats.FlushTriggers,
		},
		Artifacts: &ReportArtifacts{
			Total:     result.ArtifactStats.TotalArtifacts,
			Committed: result.ArtifactStats.CommittedArtifacts,
			Orphaned:  result.ArtifactStats.OrphanedArtifacts,
			Chunks:    result.ArtifactStats.TotalChunks,
			Bytes:     result.ArtifactStats.TotalBytes,
		},
		Metrics:        &snap,
		ProxyUsed:      result.ProxyUsed,
		Stderr:         result.StderrOutput,
		RunRecordError: result.RunRecordError,
	}

	if result.RunMeta.JobID != nil {
		report.JobID = *result.RunMeta.JobID
	}

	// Pointer indirection: nil = no terminal event (omitted via omitempty),
	// non-nil pointer to empty map = terminal event with empty payload (serialized as {}).
	if result.TerminalSummary != nil {
		ts := result.TerminalSummary
		report.TerminalSummary = &ts
	}

	return report
}

// WriteRunReport writes the report as JSON to the specified path.
// If path is "-", writes to stderr.
func WriteRunReport(report *RunReport, path string) error {
	if path == "" {
		return errors.New("report path must not be empty")
	}

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal report: %w", err)
	}
	data = append(data, '\n')

	if path == "-" {
		_, err = os.Stderr.Write(data)
		if err != nil {
			return fmt.Errorf("failed to write report to stderr: %w", err)
		}
		return nil
	}

	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("failed to write report to %s: %w", path, err)
	}
	return nil
}

// writeRunReportTo writes report JSON to any writer (for testing).
func writeRunReportTo(report *RunReport, w io.Writer) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal report: %w", err)
	}
	data = append(data, '\n')
	_, err = w.Write(data)
	return err
}
