//nolint:revive // types is a common Go package naming convention
package types

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

// ArtifactStorageKind is where an artifact's bytes are stored.
type ArtifactStorageKind string

const (
	// ArtifactStorageVolume is an artifact stored as one Lode Volume under
	// the run's artifact directory.
	ArtifactStorageVolume ArtifactStorageKind = "volume"
	// ArtifactStorageEmpty is an artifact of zero bytes, which has no Volume.
	ArtifactStorageEmpty ArtifactStorageKind = "empty"
)

// ArtifactStorage is the storage result of one artifact, which the runtime
// passes beside the artifact event and the commit record names
// (CONTRACT_LODE.md §Artifact Storage).
type ArtifactStorage struct {
	// Kind is where the bytes are.
	Kind ArtifactStorageKind
	// VolumeSnapshotID names the Volume snapshot whose manifest covers the
	// artifact. Empty unless Kind is ArtifactStorageVolume.
	VolumeSnapshotID string
	// SHA256 is the hex digest the runtime computed of the artifact's bytes.
	SHA256 string
}
