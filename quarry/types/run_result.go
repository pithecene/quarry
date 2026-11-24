// Package types defines core domain types for the Quarry runtime.
//

package types

// RunResultStatus is the status of a run result.
type RunResultStatus string

const (
	// RunResultStatusCompleted indicates the run completed successfully.
	RunResultStatusCompleted RunResultStatus = "completed"
	// RunResultStatusError indicates the run ended with a script error.
	RunResultStatusError RunResultStatus = "error"
	// RunResultStatusCrash indicates the executor crashed.
	RunResultStatusCrash RunResultStatus = "crash"
)

// RunResultOutcome is the outcome from a run result frame.
// Per CONTRACT_IPC.md, this describes the final outcome.
type RunResultOutcome struct {
	// Status is the outcome status.
	Status RunResultStatus `msgpack:"status" json:"status"`
	// Message is a human-readable description.
	Message *string `msgpack:"message,omitempty" json:"message,omitempty"`
	// ErrorType is the error type (for error status).
	ErrorType *string `msgpack:"error_type,omitempty" json:"error_type,omitempty"`
	// Stack is the stack trace (for error status).
	Stack *string `msgpack:"stack,omitempty" json:"stack,omitempty"`
}

// RunResultFrame is a run result control frame per CONTRACT_IPC.md.
// This is a control frame, NOT an event, and does not affect seq ordering.
// Discriminated from other frames by Type == "run_result".
type RunResultFrame struct {
	// Type is always "run_result" for run result frames.
	Type string `msgpack:"type"`
	// Outcome is the run outcome.
	Outcome RunResultOutcome `msgpack:"outcome"`
	// ProxyUsed is the redacted proxy endpoint (no password).
	ProxyUsed *ProxyEndpointRedacted `msgpack:"proxy_used,omitempty"`
}
