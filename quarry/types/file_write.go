package types

// FileWriteFrame represents a file_write IPC frame.
// This is a single-frame transport for sidecar file uploads via Lode Store.
// It bypasses the Dataset segment/manifest machinery entirely.
//
// Discriminated from event envelopes by Type == "file_write".
// Does not participate in seq numbering or the policy pipeline.
type FileWriteFrame struct {
	// Type is always "file_write" for file write frames.
	Type string `msgpack:"type"`
	// WriteID is a monotonic correlation ID assigned by the executor, starting at 1.
	// Used to match file_write_ack responses. Zero means no ack expected (legacy).
	WriteID uint32 `msgpack:"write_id"`
	// Filename is the target filename (no path separators, no "..").
	Filename string `msgpack:"filename"`
	// ContentType is the MIME content type.
	ContentType string `msgpack:"content_type"`
	// Data is the raw binary data (max 8 MiB).
	Data []byte `msgpack:"data"`
}

// FileWriteAckFrame represents a file_write_ack IPC frame.
// Sent by the runtime to the executor via stdin after processing a file_write.
// Correlates to a file_write frame via WriteID.
type FileWriteAckFrame struct {
	// Type is always "file_write_ack" for ack frames.
	Type string `msgpack:"type"`
	// WriteID is the correlation ID from the file_write frame.
	WriteID uint32 `msgpack:"write_id"`
	// OK is true if the write succeeded.
	OK bool `msgpack:"ok"`
	// Error is the error message when OK is false. Nil on success.
	Error *string `msgpack:"error,omitempty"`
}
