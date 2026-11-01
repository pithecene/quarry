package types

// ContractVersion is the emit contract version per CONTRACT_EMIT.md.
// This is an alias for the canonical Version to maintain backward compatibility.
const ContractVersion = Version

// EventType represents the type of event per CONTRACT_EMIT.md.
type EventType string

// Event type constants per CONTRACT_EMIT.md.
const (
	EventTypeItem        EventType = "item"
	EventTypeArtifact    EventType = "artifact"
	EventTypeCheckpoint  EventType = "checkpoint"
	EventTypeEnqueue     EventType = "enqueue"
	EventTypeRotateProxy EventType = "rotate_proxy"
	EventTypeLog         EventType = "log"
	EventTypeRunError    EventType = "run_error"
	EventTypeRunComplete EventType = "run_complete"
)

// IsTerminal returns true if this event type is a terminal event.
func (e EventType) IsTerminal() bool {
	return e == EventTypeRunComplete || e == EventTypeRunError
}

// LogLevel represents log severity per CONTRACT_EMIT.md.
type LogLevel string

// Log level constants per CONTRACT_EMIT.md.
const (
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
)

// EventEnvelope is the envelope for all events per CONTRACT_EMIT.md.
// All fields use msgpack tags to match the TypeScript SDK wire format.
type EventEnvelope struct {
	// ContractVersion is the semantic version of the emit contract.
	ContractVersion string `msgpack:"contract_version"`
	// EventID is a unique identifier for this event, scoped to the run.
	EventID string `msgpack:"event_id"`
	// RunID is the canonical run identifier.
	RunID string `msgpack:"run_id"`
	// Seq is the monotonic sequence number, starts at 1.
	Seq int64 `msgpack:"seq"`
	// Type is the event type discriminator.
	Type EventType `msgpack:"type"`
	// Ts is the event timestamp in ISO 8601 UTC format.
	Ts string `msgpack:"ts"`
	// Payload is the type-specific payload.
	Payload map[string]any `msgpack:"payload"`
	// JobID is the job identifier, included when known.
	JobID *string `msgpack:"job_id,omitempty"`
	// ParentRunID is the parent run ID for retries.
	ParentRunID *string `msgpack:"parent_run_id,omitempty"`
	// Attempt is the attempt number, always present, starts at 1.
	Attempt int `msgpack:"attempt"`
	// Storage is where the runtime stored an artifact's bytes, set on an
	// artifact event before policy dispatch and never on the wire. The
	// storage client writes its fields into the commit record.
	Storage *ArtifactStorage `msgpack:"-" json:"-"`
}

// ItemPayload represents an item event payload per CONTRACT_EMIT.md.
type ItemPayload struct {
	// ItemType is a caller-defined type label.
	ItemType string `msgpack:"item_type"`
	// Data is the record payload.
	Data map[string]any `msgpack:"data"`
}

// ArtifactPayload represents an artifact event payload per CONTRACT_EMIT.md.
// This is the commit record for an artifact; bytes are transmitted separately.
type ArtifactPayload struct {
	// ArtifactID is the unique identifier for the artifact.
	ArtifactID string `msgpack:"artifact_id"`
	// Name is a human-readable name.
	Name string `msgpack:"name"`
	// ContentType is the MIME content type.
	ContentType string `msgpack:"content_type"`
	// SizeBytes is the total size in bytes.
	SizeBytes int64 `msgpack:"size_bytes"`
}

// CheckpointPayload represents a checkpoint event payload per CONTRACT_EMIT.md.
type CheckpointPayload struct {
	// CheckpointID is the unique identifier for the checkpoint.
	CheckpointID string `msgpack:"checkpoint_id"`
	// Note is an optional human-readable note.
	Note *string `msgpack:"note,omitempty"`
}

// EnqueuePayload represents an enqueue event payload per CONTRACT_EMIT.md.
// This is advisory only; not guaranteed or required.
type EnqueuePayload struct {
	// Target is the target identifier for the work.
	Target string `msgpack:"target"`
	// Params is the parameters for the work.
	Params map[string]any `msgpack:"params"`
	// Source is an optional partition override for the child run's source.
	Source string `msgpack:"source,omitempty"`
	// Category is an optional partition override for the child run's category.
	Category string `msgpack:"category,omitempty"`
}

// RotateProxyPayload represents a rotate_proxy event payload per CONTRACT_EMIT.md.
// This is advisory only; not guaranteed or required.
type RotateProxyPayload struct {
	// Reason is an optional reason for the rotation request.
	Reason *string `msgpack:"reason,omitempty"`
}

// LogPayload represents a log event payload per CONTRACT_EMIT.md.
type LogPayload struct {
	// Level is the log level.
	Level LogLevel `msgpack:"level"`
	// Message is the log message.
	Message string `msgpack:"message"`
	// Fields is optional structured fields.
	Fields map[string]any `msgpack:"fields,omitempty"`
}

// RunErrorPayload represents a run_error event payload per CONTRACT_EMIT.md.
type RunErrorPayload struct {
	// ErrorType is the error type/category.
	ErrorType string `msgpack:"error_type"`
	// Message is the error message.
	Message string `msgpack:"message"`
	// Stack is an optional stack trace.
	Stack *string `msgpack:"stack,omitempty"`
}

// RunCompletePayload represents a run_complete event payload per CONTRACT_EMIT.md.
type RunCompletePayload struct {
	// Summary is an optional summary object.
	Summary map[string]any `msgpack:"summary,omitempty"`
}
