// Package types defines core domain types for the Quarry runtime.
// Types are designed to match the TypeScript SDK and conform to CONTRACT_*.md.
//
//nolint:revive // types is a common Go package naming convention
package types

import (
	"errors"
	"fmt"
)

// RunMeta contains run identity and lineage metadata per CONTRACT_RUN.md.
type RunMeta struct {
	// RunID is the canonical run identifier. Must be globally unique.
	RunID string
	// JobID is the logical job identifier. May be nil if unknown at runtime.
	JobID *string
	// ParentRunID links retry runs to their predecessor. Nil for initial runs.
	ParentRunID *string
	// Attempt is the attempt number. Starts at 1 for initial runs.
	Attempt int
}

// Validate validates lineage rules per CONTRACT_RUN.md:
//   - attempt >= 1
//   - attempt == 1 => parent_run_id must be nil (initial run)
//   - attempt > 1 => parent_run_id must be present (retry run)
func (r *RunMeta) Validate() error {
	if r.RunID == "" {
		return errors.New("run_id must be non-empty")
	}

	if r.Attempt < 1 {
		return fmt.Errorf("attempt must be >= 1, got %d", r.Attempt)
	}

	if r.Attempt == 1 && r.ParentRunID != nil {
		return errors.New("initial run (attempt=1) must not have parent_run_id")
	}

	if r.Attempt > 1 && r.ParentRunID == nil {
		return fmt.Errorf("retry run (attempt=%d) must have parent_run_id", r.Attempt)
	}

	return nil
}

// OutcomeStatus represents the final status of a run per CONTRACT_RUN.md.
type OutcomeStatus string

const (
	// OutcomeSuccess indicates the run completed successfully (run_complete).
	OutcomeSuccess OutcomeStatus = "success"
	// OutcomeScriptError indicates the script emitted run_error.
	OutcomeScriptError OutcomeStatus = "script_error"
	// OutcomeExecutorCrash indicates the executor crashed or exited abnormally.
	OutcomeExecutorCrash OutcomeStatus = "executor_crash"
	// OutcomePolicyFailure indicates the ingestion policy failed.
	OutcomePolicyFailure OutcomeStatus = "policy_failure"
	// OutcomeVersionMismatch indicates an SDK/CLI contract version mismatch.
	OutcomeVersionMismatch OutcomeStatus = "version_mismatch"
	// OutcomeStorageError indicates that a sink failed to persist the run's
	// events. A retry of the run may succeed.
	OutcomeStorageError OutcomeStatus = "storage_error"
)

// RunOutcome represents the final outcome of a run.
type RunOutcome struct {
	// Status is the outcome classification.
	Status OutcomeStatus
	// Message is a human-readable description.
	Message string
	// ErrorType is populated for script errors (from run_error payload).
	ErrorType *string
	// Stack is populated for script errors (from run_error payload).
	Stack *string
}
