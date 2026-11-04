package reader

import "errors"

// ParseMetricsRecord converts a Lode record (map[string]any) to a MetricsSnapshot.
// Handles both int64 (direct writes) and float64 (JSON round-trips) for numeric fields.
func ParseMetricsRecord(record map[string]any) (*MetricsSnapshot, error) {
	if record == nil {
		return nil, errors.New("nil record")
	}

	snap := &MetricsSnapshot{
		Ts: toString(record["ts"]),

		// Run lifecycle
		RunsStarted:   toInt64(record["runs_started_total"]),
		RunsCompleted: toInt64(record["runs_completed_total"]),
		RunsFailed:    toInt64(record["runs_failed_total"]),
		RunsCrashed:   toInt64(record["runs_crashed_total"]),

		// Ingestion
		EventsReceived:  toInt64(record["events_received_total"]),
		EventsPersisted: toInt64(record["events_persisted_total"]),
		EventsDropped:   toInt64(record["events_dropped_total"]),

		// Executor
		ExecutorLaunchSuccess: toInt64(record["executor_launch_success_total"]),
		ExecutorLaunchFailure: toInt64(record["executor_launch_failure_total"]),
		ExecutorCrash:         toInt64(record["executor_crash_total"]),
		IPCDecodeErrors:       toInt64(record["ipc_decode_errors_total"]),

		// Lode / Storage
		LodeWriteSuccess: toInt64(record["lode_write_success_total"]),
		LodeWriteFailure: toInt64(record["lode_write_failure_total"]),
		LodeWriteRetry:   toInt64(record["lode_write_retry_total"]),
		ArtifactIndexWriteFailure: toInt64(record["artifact_index_write_failure_total"]),

		// Dimensions
		Policy:         toString(record["policy"]),
		Executor:       toString(record["executor"]),
		StorageBackend: toString(record["storage_backend"]),
		RunID:          toString(record["run_id"]),
		JobID:          toString(record["job_id"]),
	}

	// Parse dropped_by_type if present
	if dbt, ok := record["dropped_by_type"]; ok && dbt != nil {
		snap.DroppedByType = parseDroppedByType(dbt)
	}

	// Validate contract-required fields per CONTRACT_CLI.md.
	// The write path always populates these; missing values indicate
	// data corruption or a malformed record.
	if snap.Ts == "" {
		return nil, errors.New("metrics record missing required field: ts")
	}
	if snap.RunID == "" {
		return nil, errors.New("metrics record missing required field: run_id")
	}
	if snap.Policy == "" {
		return nil, errors.New("metrics record missing required field: policy")
	}
	if snap.Executor == "" {
		return nil, errors.New("metrics record missing required field: executor")
	}
	if snap.StorageBackend == "" {
		return nil, errors.New("metrics record missing required field: storage_backend")
	}

	return snap, nil
}

// toInt64 converts a value to int64, handling float64 from JSON and int64 from direct writes.
func toInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case float64:
		return int64(n)
	case int:
		return int64(n)
	default:
		return 0
	}
}

// toString converts a value to string, returning empty string for nil/non-string.
func toString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// parseDroppedByType converts dropped_by_type from Lode record format.
// Handles both map[string]int64 (direct) and map[string]any (JSON round-trip).
func parseDroppedByType(v any) map[string]int64 {
	switch m := v.(type) {
	case map[string]int64:
		return m
	case map[string]any:
		result := make(map[string]int64, len(m))
		for k, val := range m {
			result[k] = toInt64(val)
		}
		return result
	default:
		return nil
	}
}
