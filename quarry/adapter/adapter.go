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
	// Locator is where a reader finds the run (CONTRACT_INTEGRATION.md §Run
	// Locator).
	Locator Locator `json:"locator"`
}

// Locator is the run and the storage as the SDK's readRun takes them, whose
// field names it therefore has.
type Locator struct {
	Run     RunLocator     `json:"run"`
	Storage StorageLocator `json:"storage"`
}

// RunLocator identifies the run in its dataset.
type RunLocator struct {
	Source   string `json:"source"`
	Category string `json:"category"`
	Day      string `json:"day"`
	RunID    string `json:"runId"`
}

// StorageLocator is the storage that holds the run. It carries no credentials.
type StorageLocator struct {
	Backend     string `json:"backend"` // fs or s3
	Path        string `json:"path"`    // fs: absolute directory, s3: bucket/prefix
	Dataset     string `json:"dataset"`
	Region      string `json:"region,omitempty"`
	Endpoint    string `json:"endpoint,omitempty"`
	S3PathStyle bool   `json:"s3PathStyle,omitempty"`
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
