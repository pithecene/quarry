package runtime

import (
	"fmt"

	"github.com/pithecene/quarry/types"
)

// Exit codes per executor contract (matches TypeScript executor).
const (
	ExitCodeCompleted    = 0 // run_complete emitted
	ExitCodeError        = 1 // run_error emitted
	ExitCodeCrash        = 2 // executor crash (no terminal event)
	ExitCodeInvalidInput = 3 // invalid arguments or input
)

// outcomeFromExitCode maps exit code directly to outcome status.
// This is the authoritative mapping per CONTRACT_RUN.md.
func outcomeFromExitCode(exitCode int) types.OutcomeStatus {
	switch exitCode {
	case ExitCodeCompleted:
		return types.OutcomeSuccess
	case ExitCodeError:
		return types.OutcomeScriptError
	case ExitCodeCrash, ExitCodeInvalidInput:
		return types.OutcomeExecutorCrash
	default:
		return types.OutcomeExecutorCrash
	}
}

// DetermineOutcome determines the run outcome based on exit code and terminal event.
// Per CONTRACT_RUN.md, outcome is determined by:
//  1. Exit code from executor
//  2. Presence and type of terminal event
//
// Exit code mapping:
//   - 0: completed (should have run_complete)
//   - 1: error (should have run_error)
//   - 2: crash
//   - 3: invalid input (treated as crash)
func DetermineOutcome(exitCode int, hasTerminal bool, terminalEvent *types.EventEnvelope) *types.RunOutcome {
	switch exitCode {
	case ExitCodeCompleted:
		// Normal exit - should have run_complete
		if hasTerminal && terminalEvent.Type == types.EventTypeRunComplete {
			return &types.RunOutcome{
				Status:  types.OutcomeSuccess,
				Message: "run completed successfully",
			}
		}
		// Exit 0 without terminal = anomaly, treat as crash
		return &types.RunOutcome{
			Status:  types.OutcomeExecutorCrash,
			Message: "executor exited cleanly without terminal event",
		}

	case ExitCodeError:
		// Script error - should have run_error
		if hasTerminal && terminalEvent.Type == types.EventTypeRunError {
			return extractRunErrorOutcome(terminalEvent)
		}
		// Exit 1 without terminal = anomaly, treat as crash
		return &types.RunOutcome{
			Status:  types.OutcomeExecutorCrash,
			Message: "executor exited with error without terminal event",
		}

	case ExitCodeCrash:
		return &types.RunOutcome{
			Status:  types.OutcomeExecutorCrash,
			Message: "executor crashed",
		}

	case ExitCodeInvalidInput:
		return &types.RunOutcome{
			Status:  types.OutcomeExecutorCrash,
			Message: "executor rejected invalid input",
		}

	default:
		return &types.RunOutcome{
			Status:  types.OutcomeExecutorCrash,
			Message: fmt.Sprintf("executor exited with unexpected code %d", exitCode),
		}
	}
}

// extractRunErrorOutcome extracts outcome details from a run_error event.
func extractRunErrorOutcome(event *types.EventEnvelope) *types.RunOutcome {
	outcome := &types.RunOutcome{
		Status:  types.OutcomeScriptError,
		Message: "script error",
	}

	if event.Payload != nil {
		if msg, ok := event.Payload["message"].(string); ok {
			outcome.Message = msg
		}
		if errType, ok := event.Payload["error_type"].(string); ok {
			outcome.ErrorType = &errType
		}
		if stack, ok := event.Payload["stack"].(string); ok {
			outcome.Stack = &stack
		}
	}

	return outcome
}

// reduceOutcome is the one rule that classifies a run whose ingestion and
// policy flush succeeded (CONTRACT_IPC.md §Outcome Status Mapping). The exit
// code decides the category, and the terminal event must agree with it, as
// DetermineOutcome checks: exit 0 without run_complete, or exit 1 without
// run_error, is an executor crash. A run_result frame, when the executor sent
// one, only adds the executor's own message, error type, and stack to an
// outcome that the check confirmed. conflict reports a run_result whose
// status differs from the outcome, which the caller logs.
func reduceOutcome(exitCode int, hasTerminal bool, terminal *types.EventEnvelope, frame *types.RunResultFrame) (outcome *types.RunOutcome, conflict bool) {
	outcome = DetermineOutcome(exitCode, hasTerminal, terminal)
	if frame == nil {
		return outcome, false
	}
	reported := runResultOutcomeToRunOutcome(frame)
	conflict = reported.Status != outcome.Status
	if outcome.Status != outcomeFromExitCode(exitCode) {
		// The terminal event disagrees with the exit code, so the run is a
		// crash whatever the executor reported.
		return outcome, conflict
	}
	if conflict {
		outcome.Message = fmt.Sprintf("exit code %d but run_result reported %s: %s", exitCode, frame.Outcome.Status, reported.Message)
		return outcome, true
	}
	if frame.Outcome.Message != nil {
		outcome.Message = reported.Message
	}
	if reported.ErrorType != nil {
		outcome.ErrorType = reported.ErrorType
	}
	if reported.Stack != nil {
		outcome.Stack = reported.Stack
	}
	return outcome, false
}
