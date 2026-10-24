package lode

import (
	"maps"
	"time"

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

// EventRecord is the storage format for non-artifact events.
// Includes record_kind discriminator and partition keys per CONTRACT_LODE.md.
type EventRecord struct {
	// Record discriminator
	RecordKind string `json:"record_kind"`

	// Event envelope fields
	ContractVersion string         `json:"contract_version"`
	EventID         string         `json:"event_id"`
	RunID           string         `json:"run_id"`
	Seq             int64          `json:"seq"`
	Type            string         `json:"type"`
	Ts              string         `json:"ts"`
	Payload         map[string]any `json:"payload"`
	JobID           *string        `json:"job_id,omitempty"`
	ParentRunID     *string        `json:"parent_run_id,omitempty"`
	Attempt         int            `json:"attempt"`

	// Partition keys (used by Lode HiveLayout)
	Source   string `json:"source"`
	Category string `json:"category"`
	Day      string `json:"day"`
}

// ArtifactCommitRecord is the storage format for artifact commit events.
// Marks an artifact as complete per CONTRACT_LODE.md.
type ArtifactCommitRecord struct {
	// Record discriminator
	RecordKind string `json:"record_kind"`

	// Artifact commit fields
	ArtifactID  string `json:"artifact_id"`
	Name        string `json:"name"`
	ContentType string `json:"content_type"`
	SizeBytes   int64  `json:"size_bytes"`

	// Event metadata (from envelope)
	ContractVersion string  `json:"contract_version"`
	EventID         string  `json:"event_id"`
	RunID           string  `json:"run_id"`
	Seq             int64   `json:"seq"`
	Ts              string  `json:"ts"`
	JobID           *string `json:"job_id,omitempty"`
	ParentRunID     *string `json:"parent_run_id,omitempty"`
	Attempt         int     `json:"attempt"`

	// Partition keys
	Source   string `json:"source"`
	Category string `json:"category"`
	Day      string `json:"day"`
}

// ArtifactChunkRecord is the storage format for artifact binary chunks.
// Written to event_type=artifact partition per CONTRACT_LODE.md.
type ArtifactChunkRecord struct {
	// Record discriminator
	RecordKind string `json:"record_kind"`

	// Chunk fields
	ArtifactID string `json:"artifact_id"`
	Seq        int64  `json:"seq"`
	IsLast     bool   `json:"is_last"`
	Offset     int64  `json:"offset"`
	Length     int64  `json:"length"`
	Data       []byte `json:"data"` // base64 encoded in JSON

	// Optional checksum per CONTRACT_LODE.md
	Checksum     *string `json:"checksum,omitempty"`
	ChecksumAlgo *string `json:"checksum_algo,omitempty"`

	// Partition keys
	Source   string `json:"source"`
	Category string `json:"category"`
	Day      string `json:"day"`
	RunID    string `json:"run_id"`
}

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
	if v, ok := payload["size_bytes"].(float64); ok {
		p.sizeBytes = int64(v)
	}
	return p
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
	return m
}

// toChunkRecordMap converts an ArtifactChunk to a map for storage.
func toChunkRecordMap(chunk *types.ArtifactChunk, offset int64, cfg Config) map[string]any {
	return map[string]any{
		"record_kind": RecordKindArtifactChunk,
		"artifact_id": chunk.ArtifactID,
		"seq":         chunk.Seq,
		"is_last":     chunk.IsLast,
		"offset":      offset,
		"length":      int64(len(chunk.Data)),
		"data":        chunk.Data,
		"event_type":  "artifact", // partition key for chunks
		"source":      cfg.Source,
		"category":    cfg.Category,
		"day":         cfg.Day,
		"run_id":      cfg.RunID,
	}
}

// toMetricsRecordMap converts a metrics.Snapshot to a map for Lode storage.
// Written to event_type=metrics partition with record_kind=metrics.
// Key names use _total suffix to match CONTRACT_METRICS.md naming.
func toMetricsRecordMap(snap metrics.Snapshot, cfg Config, completedAt time.Time) map[string]any {
	m := map[string]any{
		"record_kind": RecordKindMetrics,
		"event_type":  "metrics", // partition key
		"ts":          completedAt.UTC().Format(time.RFC3339),

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
		"lode_write_success_total": snap.LodeWriteSuccess,
		"lode_write_failure_total": snap.LodeWriteFailure,
		"lode_write_retry_total":   snap.LodeWriteRetry,

		// Dimensions
		"policy":          snap.Policy,
		"executor":        snap.Executor,
		"storage_backend": snap.StorageBackend,
		"run_id":          snap.RunID,

		// Partition keys
		"source":   cfg.Source,
		"category": cfg.Category,
		"day":      cfg.Day,
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

// Legacy struct-based conversion for testing (not used with Lode)
func toEventRecord(e *types.EventEnvelope, cfg Config) EventRecord {
	return EventRecord{
		RecordKind:      RecordKindEvent,
		ContractVersion: e.ContractVersion,
		EventID:         e.EventID,
		RunID:           e.RunID,
		Seq:             e.Seq,
		Type:            string(e.Type),
		Ts:              e.Ts,
		Payload:         e.Payload,
		JobID:           e.JobID,
		ParentRunID:     e.ParentRunID,
		Attempt:         e.Attempt,
		Source:          cfg.Source,
		Category:        cfg.Category,
		Day:             cfg.Day,
	}
}

func toArtifactCommitRecord(e *types.EventEnvelope, cfg Config) ArtifactCommitRecord {
	ap := extractArtifactPayload(e.Payload)
	return ArtifactCommitRecord{
		RecordKind:      RecordKindArtifactEvent,
		ArtifactID:      ap.artifactID,
		Name:            ap.name,
		ContentType:     ap.contentType,
		SizeBytes:       ap.sizeBytes,
		ContractVersion: e.ContractVersion,
		EventID:         e.EventID,
		RunID:           e.RunID,
		Seq:             e.Seq,
		Ts:              e.Ts,
		JobID:           e.JobID,
		ParentRunID:     e.ParentRunID,
		Attempt:         e.Attempt,
		Source:          cfg.Source,
		Category:        cfg.Category,
		Day:             cfg.Day,
	}
}

func toChunkRecord(chunk *types.ArtifactChunk, offset int64, cfg Config) ArtifactChunkRecord {
	return ArtifactChunkRecord{
		RecordKind: RecordKindArtifactChunk,
		ArtifactID: chunk.ArtifactID,
		Seq:        chunk.Seq,
		IsLast:     chunk.IsLast,
		Offset:     offset,
		Length:     int64(len(chunk.Data)),
		Data:       chunk.Data,
		Source:     cfg.Source,
		Category:   cfg.Category,
		Day:        cfg.Day,
		RunID:      cfg.RunID,
	}
}
