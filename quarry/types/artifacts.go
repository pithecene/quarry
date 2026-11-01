//nolint:revive // types is a common Go package naming convention
package types

// ArtifactChunkFrame represents an artifact chunk frame per CONTRACT_IPC.md.
// This is a stream-level construct, not a normal emit event.
// Discriminated from event envelopes by Type == "artifact_chunk".
type ArtifactChunkFrame struct {
	// Type is always "artifact_chunk" for chunk frames.
	Type string `msgpack:"type"`
	// ArtifactID identifies the artifact this chunk belongs to.
	ArtifactID string `msgpack:"artifact_id"`
	// Seq is the sequence number, starts at 1.
	Seq int64 `msgpack:"seq"`
	// IsLast is true if this is the final chunk.
	IsLast bool `msgpack:"is_last"`
	// Data is the raw binary data.
	Data []byte `msgpack:"data"`
}

// ArtifactOpenFrame opens an artifact per CONTRACT_IPC.md §Artifact Frames.
// The runtime creates the artifact's Volume with SizeBytes as its total
// length before any byte arrives, or no Volume when SizeBytes is zero.
// Discriminated from event envelopes by Type == "artifact_open".
type ArtifactOpenFrame struct {
	// Type is always "artifact_open".
	Type string `msgpack:"type"`
	// ArtifactID identifies the artifact, one canonical path segment.
	ArtifactID string `msgpack:"artifact_id"`
	// SizeBytes is the artifact's length in bytes, zero for an empty artifact.
	SizeBytes int64 `msgpack:"size_bytes"`
	// ExpectedSHA256 is the hex SHA-256 the script expects of the bytes.
	// Empty when the script declares none.
	ExpectedSHA256 string `msgpack:"expected_sha256,omitempty"`
}

// ArtifactRangeFrame carries one range of an opened artifact's bytes.
// Ranges may arrive in any order and must not overlap.
// Discriminated from event envelopes by Type == "artifact_range".
type ArtifactRangeFrame struct {
	// Type is always "artifact_range".
	Type string `msgpack:"type"`
	// ArtifactID identifies the artifact the range belongs to.
	ArtifactID string `msgpack:"artifact_id"`
	// RangeID is a correlation ID the executor assigns, counting up from 1
	// within the run, which the artifact_range_ack repeats.
	RangeID uint32 `msgpack:"range_id"`
	// Offset is the byte offset of the range within the artifact.
	Offset int64 `msgpack:"offset"`
	// Data is the range's bytes, at most ipc.MaxChunkSize.
	Data []byte `msgpack:"data"`
}

// ArtifactRangeAckFrame acknowledges an artifact_range frame. The runtime
// sends it to the executor's stdin once the range is committed, or once the
// range is refused.
type ArtifactRangeAckFrame struct {
	// Type is always "artifact_range_ack".
	Type string `msgpack:"type"`
	// RangeID is the correlation ID of the artifact_range frame.
	RangeID uint32 `msgpack:"range_id"`
	// OK is true once the range is committed.
	OK bool `msgpack:"ok"`
	// Error is the error message when OK is false. Nil on success.
	Error *string `msgpack:"error,omitempty"`
}

// ArtifactChunk is an internal representation of a chunk (after decoding).
type ArtifactChunk struct {
	ArtifactID string
	Seq        int64
	IsLast     bool
	Data       []byte
}

// ArtifactAccumulator tracks chunks for a single artifact.
type ArtifactAccumulator struct {
	// ArtifactID is the artifact identifier.
	ArtifactID string
	// Chunks holds the accumulated chunks in order.
	Chunks []*ArtifactChunk
	// TotalBytes is the sum of all chunk data lengths.
	TotalBytes int64
	// Committed is true if the artifact event has been received and validated.
	Committed bool
	// NextSeq is the expected next sequence number.
	NextSeq int64
	// Complete is true if is_last has been seen.
	Complete bool
	// ErrorState is true if the artifact encountered an unrecoverable error (e.g., size mismatch).
	ErrorState bool
}
