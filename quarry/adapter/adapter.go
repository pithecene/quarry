// Package adapter defines the event-bus adapter boundary per CONTRACT_INTEGRATION.md.
//
// Adapters publish run completion notifications to downstream systems.
// The runtime owns adapter lifecycle; users provide configuration only.
package adapter

import "context"

// RunCompletedEvent is the payload published when a run finishes.
// Shape matches the event payload defined in docs/guides/integration.md.
type RunCompletedEvent struct {
	ContractVersion string `json:"contract_version"`
	EventType       string `json:"event_type"` // always "run_completed"
	RunID           string `json:"run_id"`
	Source          string `json:"source"`
	Category        string `json:"category"`
	Day             string `json:"day"`
	Outcome         string `json:"outcome"` // success, script_error, etc.
	StoragePath     string `json:"storage_path"`
	Timestamp       string `json:"timestamp"` // ISO 8601
	JobID           string `json:"job_id,omitempty"`
	Attempt         int    `json:"attempt"`
	EventCount      int64  `json:"event_count"`
	DurationMs      int64  `json:"duration_ms"`
}

// Adapter publishes run completion events to a downstream system.
// Implementations must be safe for single-use per run.
type Adapter interface {
	// Publish sends a run completion event to the downstream system.
	// Must respect context cancellation and deadlines.
	Publish(ctx context.Context, event *RunCompletedEvent) error

	// Close releases adapter resources.
	Close() error
}
