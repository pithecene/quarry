// Package reader provides the read-side data access layer for the quarry CLI.
//
// This package isolates all read operations from runtime internals.
// All read-only commands use this wrapper exclusively per the implementation plan.
//
// Current implementation returns stub data. When Lode integration is ready,
// these will be wired to actual data sources.
package reader

import "time"

// SidecarFileSummary describes a sidecar file discovered from snapshot metadata.
type SidecarFileSummary struct {
	Filename    string `json:"filename"`
	ContentType string `json:"content_type"`
	Size        int64  `json:"size"`
	Path        string `json:"path"`
}

// InspectRunResponse per CONTRACT_CLI.md.
type InspectRunResponse struct {
	RunID        string               `json:"run_id"`
	JobID        string               `json:"job_id"`
	State        string               `json:"state"`
	Attempt      int                  `json:"attempt"`
	ParentRun    *string              `json:"parent_run"`
	Policy       string               `json:"policy"`
	StartedAt    time.Time            `json:"started_at"`
	EndedAt      *time.Time           `json:"ended_at"`
	SidecarFiles []SidecarFileSummary `json:"sidecar_files,omitempty"`
}

// ProxySticky represents proxy sticky configuration per CONTRACT_PROXY.md.
// Scope must be one of: "job", "domain", "origin".
type ProxySticky struct {
	Scope string `json:"scope"`
	TTLMs *int64 `json:"ttl_ms,omitempty"`
}

// ProxyRuntime represents runtime proxy state.
type ProxyRuntime struct {
	RoundRobinIndex int        `json:"round_robin_index"`
	StickyEntries   int        `json:"sticky_entries"`
	RecencyWindow   *int       `json:"recency_window,omitempty"`
	RecencyFill     int        `json:"recency_fill"`
	LastUsedAt      *time.Time `json:"last_used_at"`
}

// InspectProxyPoolResponse per CONTRACT_CLI.md.
type InspectProxyPoolResponse struct {
	Name        string       `json:"name"`
	Strategy    string       `json:"strategy"`
	EndpointCnt int          `json:"endpoint_cnt"`
	Sticky      *ProxySticky `json:"sticky"`
	Runtime     ProxyRuntime `json:"runtime"`
}

// RunStats per CONTRACT_CLI.md.
type RunStats struct {
	Total     int `json:"total"`
	Running   int `json:"running"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
}

// ProxyStats per CONTRACT_CLI.md.
type ProxyStats struct {
	Pool       string     `json:"pool"`
	Requests   int        `json:"requests"`
	Failures   int        `json:"failures"`
	LastUsedAt *time.Time `json:"last_used_at"`
}

// ListRunItem per CONTRACT_CLI.md.
type ListRunItem struct {
	RunID     string    `json:"run_id"`
	State     string    `json:"state"`
	StartedAt time.Time `json:"started_at"`
}

// ListPoolItem per CONTRACT_CLI.md.
type ListPoolItem struct {
	Name     string `json:"name"`
	Strategy string `json:"strategy"`
}

// MetricsSnapshot per CONTRACT_METRICS.md and CONTRACT_CLI.md.
// Field names use _total suffix to match CONTRACT_METRICS naming.
type MetricsSnapshot struct {
	// Timestamp of the metrics snapshot (run completion time)
	Ts string `json:"ts"`

	// Run lifecycle
	RunsStarted   int64 `json:"runs_started_total"`
	RunsCompleted int64 `json:"runs_completed_total"`
	RunsFailed    int64 `json:"runs_failed_total"`
	RunsCrashed   int64 `json:"runs_crashed_total"`

	// Ingestion
	EventsReceived  int64            `json:"events_received_total"`
	EventsPersisted int64            `json:"events_persisted_total"`
	EventsDropped   int64            `json:"events_dropped_total"`
	DroppedByType   map[string]int64 `json:"dropped_by_type,omitempty"`

	// Executor
	ExecutorLaunchSuccess int64 `json:"executor_launch_success_total"`
	ExecutorLaunchFailure int64 `json:"executor_launch_failure_total"`
	ExecutorCrash         int64 `json:"executor_crash_total"`
	IPCDecodeErrors       int64 `json:"ipc_decode_errors_total"`

	// Lode / Storage
	LodeWriteSuccess int64 `json:"lode_write_success_total"`
	LodeWriteFailure int64 `json:"lode_write_failure_total"`
	LodeWriteRetry   int64 `json:"lode_write_retry_total"`

	// Dimensions per CONTRACT_METRICS.md
	Policy         string `json:"policy"`
	Executor       string `json:"executor"`
	StorageBackend string `json:"storage_backend"`
	RunID          string `json:"run_id"`
	JobID          string `json:"job_id,omitempty"`
}

// ListRunsOptions for filtering list runs.
type ListRunsOptions struct {
	State string
	Limit int
}

// ProxyEndpoint represents a resolved proxy endpoint.
type ProxyEndpoint struct {
	Host     string  `json:"host"`
	Port     int     `json:"port"`
	Protocol string  `json:"protocol"`
	Username *string `json:"username,omitempty"`
}

// ResolveProxyResponse per CONTRACT_CLI.md.
type ResolveProxyResponse struct {
	Endpoint  ProxyEndpoint `json:"endpoint"`
	Committed bool          `json:"committed"`
}

// IPCDebugResponse per CONTRACT_CLI.md.
type IPCDebugResponse struct {
	Transport    string  `json:"transport"`
	Encoding     string  `json:"encoding"`
	MessagesSent int     `json:"messages_sent"`
	Errors       int     `json:"errors"`
	LastError    *string `json:"last_error"`
}
