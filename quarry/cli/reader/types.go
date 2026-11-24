// Package reader holds the response types of the quarry CLI's read commands
// and parses metrics records into them. The commands read storage through the
// run reader (package github.com/pithecene/quarry/reader) and the Lode queries.
package reader

import runreader "github.com/pithecene/quarry/reader"

// SidecarFileSummary describes a sidecar file discovered from snapshot metadata.
type SidecarFileSummary struct {
	Filename    string `json:"filename" yaml:"filename"`
	ContentType string `json:"content_type" yaml:"content_type"`
	Size        int64  `json:"size" yaml:"size"`
	Path        string `json:"path" yaml:"path"`
}

// InspectRunResponse per CONTRACT_CLI.md §inspect run: the run as its
// records describe it, the policy that its metrics record names, and its
// sidecar files. Policy is null when the run persisted no metrics record.
type InspectRunResponse struct {
	Run          runreader.Run        `json:"run" yaml:"run"`
	Policy       *string              `json:"policy" yaml:"policy"`
	SidecarFiles []SidecarFileSummary `json:"sidecar_files,omitempty" yaml:"sidecar_files,omitempty"`
}

// MetricsSnapshot per CONTRACT_METRICS.md and CONTRACT_CLI.md.
// Field names use _total suffix to match CONTRACT_METRICS naming.
type MetricsSnapshot struct {
	// Timestamp of the metrics snapshot (run completion time)
	Ts string `json:"ts" yaml:"ts"`

	// Run lifecycle
	RunsStarted   int64 `json:"runs_started_total" yaml:"runs_started_total"`
	RunsCompleted int64 `json:"runs_completed_total" yaml:"runs_completed_total"`
	RunsFailed    int64 `json:"runs_failed_total" yaml:"runs_failed_total"`
	RunsCrashed   int64 `json:"runs_crashed_total" yaml:"runs_crashed_total"`

	// Ingestion
	EventsReceived  int64            `json:"events_received_total" yaml:"events_received_total"`
	EventsPersisted int64            `json:"events_persisted_total" yaml:"events_persisted_total"`
	EventsDropped   int64            `json:"events_dropped_total" yaml:"events_dropped_total"`
	DroppedByType   map[string]int64 `json:"dropped_by_type,omitempty" yaml:"dropped_by_type,omitempty"`

	// Executor
	ExecutorLaunchSuccess int64 `json:"executor_launch_success_total" yaml:"executor_launch_success_total"`
	ExecutorLaunchFailure int64 `json:"executor_launch_failure_total" yaml:"executor_launch_failure_total"`
	ExecutorCrash         int64 `json:"executor_crash_total" yaml:"executor_crash_total"`
	IPCDecodeErrors       int64 `json:"ipc_decode_errors_total" yaml:"ipc_decode_errors_total"`

	// Lode / Storage
	LodeWriteSuccess          int64 `json:"lode_write_success_total" yaml:"lode_write_success_total"`
	LodeWriteFailure          int64 `json:"lode_write_failure_total" yaml:"lode_write_failure_total"`
	LodeWriteRetry            int64 `json:"lode_write_retry_total" yaml:"lode_write_retry_total"`
	ArtifactIndexWriteFailure int64 `json:"artifact_index_write_failure_total" yaml:"artifact_index_write_failure_total"`

	// Dimensions per CONTRACT_METRICS.md
	Policy         string `json:"policy" yaml:"policy"`
	Executor       string `json:"executor" yaml:"executor"`
	StorageBackend string `json:"storage_backend" yaml:"storage_backend"`
	RunID          string `json:"run_id" yaml:"run_id"`
	JobID          string `json:"job_id,omitempty" yaml:"job_id,omitempty"`
}

// ProxyEndpoint represents a resolved proxy endpoint.
type ProxyEndpoint struct {
	Host     string  `json:"host" yaml:"host"`
	Port     int     `json:"port" yaml:"port"`
	Protocol string  `json:"protocol" yaml:"protocol"`
	Username *string `json:"username,omitempty" yaml:"username,omitempty"`
}

// ResolveProxyResponse per CONTRACT_CLI.md.
type ResolveProxyResponse struct {
	Endpoint ProxyEndpoint `json:"endpoint" yaml:"endpoint"`
}
