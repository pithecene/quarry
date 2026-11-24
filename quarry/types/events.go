package types

// ContractVersion is the emit contract version per CONTRACT_EMIT.md.
// It is the canonical Version.
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
	// RawFrameBytes is the length of the IPC frame this envelope was decoded
	// from, length prefix included, set by the decoder and never on the wire.
	// Zero means the envelope was built in process.
	RawFrameBytes int64 `msgpack:"-" json:"-"`
}
