package lode

import (
	"maps"
	"math"

	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/types"
)

// RecordKind discriminator values per CONTRACT_LODE.md.
const (
	RecordKindEvent         = "event"
	RecordKindArtifactEvent = "artifact_event"
	RecordKindArtifactChunk = "artifact_chunk"
	RecordKindMetrics       = "metrics"
)

// artifactPayload holds fields extracted from an artifact event's payload.
type artifactPayload struct {
	artifactID  string
	name        string
	contentType string
	sizeBytes   int64
}

// extractArtifactPayload extracts typed artifact fields from a payload map.
func extractArtifactPayload(payload map[string]any) artifactPayload {
	var p artifactPayload
	if payload == nil {
		return p
	}
	if v, ok := payload["artifact_id"].(string); ok {
		p.artifactID = v
	}
	if v, ok := payload["name"].(string); ok {
		p.name = v
	}
	if v, ok := payload["content_type"].(string); ok {
		p.contentType = v
	}
	if v, ok := int64Of(payload["size_bytes"]); ok {
		p.sizeBytes = v
	}
	return p
}

// int64Of reads an integer payload field. JSON decodes a number as float64,
// and msgpack decodes one as the narrowest integer type that holds it.
func int64Of(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		// A size is a whole number of bytes within int64. -2^63 and 2^63 are exact
		// in float64, so the bounds compare without rounding.
		if n != math.Trunc(n) || n < -(1<<63) || n >= 1<<63 {
			return 0, false
		}
		return int64(n), true
	case int:
		return int64(n), true
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case uint:
		if n > math.MaxInt64 {
			return 0, false
		}
		return int64(n), true
	case uint8:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint64:
		if n > math.MaxInt64 {
			return 0, false
		}
		return int64(n), true
	}
	return 0, false
}

// addOptionalEnvelopeFields adds optional envelope fields (job_id, parent_run_id) to a record map.
func addOptionalEnvelopeFields(m map[string]any, e *types.EventEnvelope) {
	if e.JobID != nil {
		m["job_id"] = *e.JobID
	}
	if e.ParentRunID != nil {
		m["parent_run_id"] = *e.ParentRunID
	}
}

// toEventRecordMap converts an EventEnvelope to a map for Lode storage.
// Lode HiveLayout requires records as map[string]any.
func toEventRecordMap(e *types.EventEnvelope, cfg Config) map[string]any {
	m := map[string]any{
		"record_kind":      RecordKindEvent,
		"contract_version": e.ContractVersion,
		"event_id":         e.EventID,
		"run_id":           e.RunID,
		"seq":              e.Seq,
		"type":             string(e.Type),
		"event_type":       string(e.Type), // partition key
		"ts":               e.Ts,
		"payload":          e.Payload,
		"attempt":          e.Attempt,
		"source":           cfg.Source,
		"category":         cfg.Category,
		"day":              cfg.Day,
		"policy":           cfg.Policy,
	}
	addOptionalEnvelopeFields(m, e)
	return m
}

// toArtifactCommitRecordMap converts an artifact EventEnvelope to a map for storage.
func toArtifactCommitRecordMap(e *types.EventEnvelope, cfg Config) map[string]any {
	ap := extractArtifactPayload(e.Payload)
	m := map[string]any{
		"record_kind":      RecordKindArtifactEvent,
		"artifact_id":      ap.artifactID,
		"name":             ap.name,
		"content_type":     ap.contentType,
		"size_bytes":       ap.sizeBytes,
		"contract_version": e.ContractVersion,
		"event_id":         e.EventID,
		"run_id":           e.RunID,
		"seq":              e.Seq,
		"event_type":       string(e.Type), // partition key
		"ts":               e.Ts,
		"attempt":          e.Attempt,
		"source":           cfg.Source,
		"category":         cfg.Category,
		"day":              cfg.Day,
		"policy":           cfg.Policy,
	}
	addOptionalEnvelopeFields(m, e)
	addArtifactStorageFields(m, e.Storage)
	return m
}

// addArtifactStorageFields adds the storage fields of a commit record
// (CONTRACT_LODE.md §Commit Record Schema). A record without them, as
// earlier releases wrote, has its bytes in chunk records.
func addArtifactStorageFields(m map[string]any, s *types.ArtifactStorage) {
	if s == nil {
		return
	}
	m["storage"] = string(s.Kind)
	m["sha256"] = s.SHA256
	if s.Kind == types.ArtifactStorageVolume {
		m["volume_snapshot_id"] = s.VolumeSnapshotID
	}
}

// metricsFields returns the snapshot's counters and dimensions under their
// CONTRACT_METRICS.md names.
func metricsFields(snap metrics.Snapshot) map[string]any {
	m := map[string]any{
		// Run lifecycle (CONTRACT_METRICS names)
		"runs_started_total":   snap.RunsStarted,
		"runs_completed_total": snap.RunsCompleted,
		"runs_failed_total":    snap.RunsFailed,
		"runs_crashed_total":   snap.RunsCrashed,

		// Ingestion
		"events_received_total":  snap.EventsReceived,
		"events_persisted_total": snap.EventsPersisted,
		"events_dropped_total":   snap.EventsDropped,

		// Executor
		"executor_launch_success_total": snap.ExecutorLaunchSuccess,
		"executor_launch_failure_total": snap.ExecutorLaunchFailure,
		"executor_crash_total":          snap.ExecutorCrash,
		"ipc_decode_errors_total":       snap.IPCDecodeErrors,

		// Lode / Storage
		"lode_write_success_total":           snap.LodeWriteSuccess,
		"lode_write_failure_total":           snap.LodeWriteFailure,
		"lode_write_retry_total":             snap.LodeWriteRetry,
		"artifact_index_write_failure_total": snap.ArtifactIndexWriteFailure,

		// Dimensions
		"policy":          snap.Policy,
		"executor":        snap.Executor,
		"storage_backend": snap.StorageBackend,
		"run_id":          snap.RunID,
	}

	// Optional: only include job_id when non-empty
	if snap.JobID != "" {
		m["job_id"] = snap.JobID
	}

	// Copy dropped_by_type if non-empty
	if len(snap.DroppedByType) > 0 {
		dropped := make(map[string]int64, len(snap.DroppedByType))
		maps.Copy(dropped, snap.DroppedByType)
		m["dropped_by_type"] = dropped
	}

	return m
}
