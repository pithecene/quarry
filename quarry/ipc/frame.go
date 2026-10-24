// Package ipc implements IPC framing per CONTRACT_IPC.md.
package ipc

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/pithecene/quarry/types"
)

// Frame size constants per CONTRACT_IPC.md.
const (
	// MaxFrameSize is the maximum frame size (16 MiB), including length prefix.
	MaxFrameSize = 16 * 1024 * 1024
	// MaxPayloadSize is the maximum payload size (MaxFrameSize - 4 bytes).
	MaxPayloadSize = MaxFrameSize - LengthPrefixSize
	// MaxChunkSize is the maximum artifact chunk size (8 MiB raw bytes).
	MaxChunkSize = 8 * 1024 * 1024
	// LengthPrefixSize is the size of the length prefix in bytes.
	LengthPrefixSize = 4
)

// ArtifactChunkType is the type discriminant for artifact chunk frames.
const ArtifactChunkType = "artifact_chunk"

// RunResultType is the type discriminant for run result control frames.
const RunResultType = "run_result"

// FileWriteType is the type discriminant for file write frames.
// File writes bypass seq numbering and the policy pipeline.
const FileWriteType = "file_write"

// FileWriteAckType is the type discriminant for file write acknowledgement frames.
// Sent runtime→executor via stdin after processing a file_write frame.
const FileWriteAckType = "file_write_ack"

// FrameErrorKind classifies frame decoding errors.
type FrameErrorKind int

const (
	// FrameErrorPartial indicates a truncated or incomplete frame.
	FrameErrorPartial FrameErrorKind = iota
	// FrameErrorTooLarge indicates a frame exceeding MaxFrameSize.
	FrameErrorTooLarge
	// FrameErrorDecode indicates a msgpack decoding error.
	FrameErrorDecode
)

// FrameError represents a frame decoding error.
type FrameError struct {
	Kind FrameErrorKind
	Msg  string
	Err  error
}

func (e *FrameError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Msg, e.Err)
	}
	return e.Msg
}

func (e *FrameError) Unwrap() error {
	return e.Err
}

// IsFatal returns true if this error is fatal (terminate run).
// Per CONTRACT_IPC.md, partial and oversized frames are fatal.
func (e *FrameError) IsFatal() bool {
	return e.Kind == FrameErrorPartial || e.Kind == FrameErrorTooLarge
}

// IsFatalFrameError returns true if the error is a fatal frame error.
func IsFatalFrameError(err error) bool {
	if frameErr, ok := errors.AsType[*FrameError](err); ok {
		return frameErr.IsFatal()
	}
	return false
}

// FrameDecoder decodes length-prefixed msgpack frames from a stream.
type FrameDecoder struct {
	reader io.Reader
}

// NewFrameDecoder creates a new frame decoder.
// Wraps the reader with bufio.Reader to reduce syscall overhead
// on unbuffered sources (e.g., OS pipes from child processes).
func NewFrameDecoder(r io.Reader) *FrameDecoder {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}
	return &FrameDecoder{reader: br}
}

// ReadFrame reads a single frame from the stream.
// Returns the raw payload bytes (msgpack-encoded).
//
// Errors:
//   - io.EOF: stream ended cleanly (no more frames)
//   - *FrameError with Kind=FrameErrorPartial: incomplete frame (fatal)
//   - *FrameError with Kind=FrameErrorTooLarge: frame exceeds limit (fatal)
func (d *FrameDecoder) ReadFrame() ([]byte, error) {
	// Read 4-byte big-endian length prefix
	var lengthBuf [LengthPrefixSize]byte
	_, err := io.ReadFull(d.reader, lengthBuf[:])
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		// Partial read of length prefix
		return nil, &FrameError{
			Kind: FrameErrorPartial,
			Msg:  "failed to read length prefix",
			Err:  err,
		}
	}

	payloadSize := binary.BigEndian.Uint32(lengthBuf[:])

	// Validate frame size per CONTRACT_IPC.md
	if payloadSize > MaxPayloadSize {
		return nil, &FrameError{
			Kind: FrameErrorTooLarge,
			Msg:  fmt.Sprintf("payload size %d exceeds maximum %d", payloadSize, MaxPayloadSize),
		}
	}

	// Read payload
	payload := make([]byte, payloadSize)
	_, err = io.ReadFull(d.reader, payload)
	if err != nil {
		return nil, &FrameError{
			Kind: FrameErrorPartial,
			Msg:  "failed to read payload",
			Err:  err,
		}
	}

	return payload, nil
}

// probeFrameType extracts the "type" field from a msgpack map without
// fully unmarshaling the payload. Falls back to full probe on error.
func probeFrameType(payload []byte) (string, error) {
	dec := msgpack.NewDecoder(bytes.NewReader(payload))
	n, err := dec.DecodeMapLen()
	if err != nil {
		return "", err
	}
	for range n {
		key, err := dec.DecodeString()
		if err != nil {
			return "", err
		}
		if key == "type" {
			return dec.DecodeString()
		}
		if err := dec.Skip(); err != nil {
			return "", err
		}
	}
	return "", errors.New("missing type field")
}

// DecodeFrame decodes a payload and returns a typed frame.
// Discriminates based on the type field: "artifact_chunk", "run_result",
// "file_write", or event types.
func DecodeFrame(payload []byte) (any, error) {
	frameType, err := probeFrameType(payload)
	if err != nil {
		return nil, &FrameError{
			Kind: FrameErrorDecode,
			Msg:  "failed to decode frame type",
			Err:  err,
		}
	}

	switch frameType {
	case ArtifactChunkType:
		return DecodeArtifactChunk(payload)
	case RunResultType:
		return DecodeRunResult(payload)
	case FileWriteType:
		return DecodeFileWrite(payload)
	case FileWriteAckType:
		return DecodeFileWriteAck(payload)
	default:
		return DecodeEventEnvelope(payload)
	}
}

// DecodeEventEnvelope decodes a payload as an EventEnvelope.
func DecodeEventEnvelope(payload []byte) (*types.EventEnvelope, error) {
	var envelope types.EventEnvelope
	if err := msgpack.Unmarshal(payload, &envelope); err != nil {
		return nil, &FrameError{
			Kind: FrameErrorDecode,
			Msg:  "failed to decode event envelope",
			Err:  err,
		}
	}
	return &envelope, nil
}

// DecodeArtifactChunk decodes a payload as an ArtifactChunkFrame.
func DecodeArtifactChunk(payload []byte) (*types.ArtifactChunkFrame, error) {
	var chunk types.ArtifactChunkFrame
	if err := msgpack.Unmarshal(payload, &chunk); err != nil {
		return nil, &FrameError{
			Kind: FrameErrorDecode,
			Msg:  "failed to decode artifact chunk",
			Err:  err,
		}
	}
	return &chunk, nil
}

// DecodeRunResult decodes a payload as a RunResultFrame.
func DecodeRunResult(payload []byte) (*types.RunResultFrame, error) {
	var result types.RunResultFrame
	if err := msgpack.Unmarshal(payload, &result); err != nil {
		return nil, &FrameError{
			Kind: FrameErrorDecode,
			Msg:  "failed to decode run result",
			Err:  err,
		}
	}
	return &result, nil
}

// DecodeFileWrite decodes a payload as a FileWriteFrame.
func DecodeFileWrite(payload []byte) (*types.FileWriteFrame, error) {
	var frame types.FileWriteFrame
	if err := msgpack.Unmarshal(payload, &frame); err != nil {
		return nil, &FrameError{
			Kind: FrameErrorDecode,
			Msg:  "failed to decode file write",
			Err:  err,
		}
	}
	return &frame, nil
}

// DecodeFileWriteAck decodes a payload as a FileWriteAckFrame.
func DecodeFileWriteAck(payload []byte) (*types.FileWriteAckFrame, error) {
	var frame types.FileWriteAckFrame
	if err := msgpack.Unmarshal(payload, &frame); err != nil {
		return nil, &FrameError{
			Kind: FrameErrorDecode,
			Msg:  "failed to decode file write ack",
			Err:  err,
		}
	}
	return &frame, nil
}

// EncodeFrame encodes a payload with a 4-byte big-endian length prefix.
// This is the public encoder counterpart to FrameDecoder.ReadFrame.
func EncodeFrame(payload []byte) []byte {
	buf := make([]byte, LengthPrefixSize+len(payload))
	binary.BigEndian.PutUint32(buf[:LengthPrefixSize], uint32(len(payload)))
	copy(buf[LengthPrefixSize:], payload)
	return buf
}

// EncodeFileWriteAck encodes a FileWriteAckFrame as a length-prefixed msgpack frame.
func EncodeFileWriteAck(ack *types.FileWriteAckFrame) ([]byte, error) {
	payload, err := msgpack.Marshal(ack)
	if err != nil {
		return nil, fmt.Errorf("failed to encode file write ack: %w", err)
	}
	return EncodeFrame(payload), nil
}
