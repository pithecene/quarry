package ipc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/pithecene/quarry/types"
)

// encodeFrame encodes a payload with length prefix (matches Node executor output).
func encodeFrame(payload []byte) []byte {
	buf := make([]byte, LengthPrefixSize+len(payload))
	binary.BigEndian.PutUint32(buf[:LengthPrefixSize], uint32(len(payload)))
	copy(buf[LengthPrefixSize:], payload)
	return buf
}

// encodeEventFrame encodes an event envelope as a framed msgpack payload.
func encodeEventFrame(envelope *types.EventEnvelope) ([]byte, error) {
	payload, err := msgpack.Marshal(envelope)
	if err != nil {
		return nil, err
	}
	return encodeFrame(payload), nil
}

// encodeArtifactChunkFrame encodes an artifact chunk as a framed msgpack payload.
func encodeArtifactChunkFrame(chunk *types.ArtifactChunkFrame) ([]byte, error) {
	payload, err := msgpack.Marshal(chunk)
	if err != nil {
		return nil, err
	}
	return encodeFrame(payload), nil
}

func TestFrameDecoder_SingleEvent(t *testing.T) {
	t.Parallel()

	envelope := &types.EventEnvelope{
		ContractVersion: types.Version,
		EventID:         "evt-001",
		RunID:           "run-001",
		Seq:             1,
		Type:            types.EventTypeItem,
		Ts:              "2024-01-15T10:00:00Z",
		Attempt:         1,
		Payload: map[string]any{
			"item_type": "product",
			"data":      map[string]any{"name": "test"},
		},
	}

	frame, err := encodeEventFrame(envelope)
	if err != nil {
		t.Fatalf("encodeEventFrame failed: %v", err)
	}

	decoder := NewFrameDecoder(bytes.NewReader(frame))
	payload, err := decoder.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	decoded, err := DecodeEventEnvelope(payload)
	if err != nil {
		t.Fatalf("DecodeEventEnvelope failed: %v", err)
	}

	if decoded.EventID != envelope.EventID {
		t.Errorf("EventID = %q, want %q", decoded.EventID, envelope.EventID)
	}
	if decoded.Type != envelope.Type {
		t.Errorf("Type = %q, want %q", decoded.Type, envelope.Type)
	}
	if decoded.Seq != envelope.Seq {
		t.Errorf("Seq = %d, want %d", decoded.Seq, envelope.Seq)
	}
}

func TestFrameDecoder_MultipleEvents(t *testing.T) {
	t.Parallel()

	events := []*types.EventEnvelope{
		{
			ContractVersion: types.Version,
			EventID:         "evt-001",
			RunID:           "run-001",
			Seq:             1,
			Type:            types.EventTypeItem,
			Ts:              "2024-01-15T10:00:00Z",
			Attempt:         1,
			Payload:         map[string]any{"item_type": "product"},
		},
		{
			ContractVersion: types.Version,
			EventID:         "evt-002",
			RunID:           "run-001",
			Seq:             2,
			Type:            types.EventTypeLog,
			Ts:              "2024-01-15T10:00:01Z",
			Attempt:         1,
			Payload:         map[string]any{"level": "info", "message": "test"},
		},
		{
			ContractVersion: types.Version,
			EventID:         "evt-003",
			RunID:           "run-001",
			Seq:             3,
			Type:            types.EventTypeRunComplete,
			Ts:              "2024-01-15T10:00:02Z",
			Attempt:         1,
			Payload:         map[string]any{},
		},
	}

	// Encode all events into a single buffer
	var buf bytes.Buffer
	for _, env := range events {
		frame, err := encodeEventFrame(env)
		if err != nil {
			t.Fatalf("encodeEventFrame failed: %v", err)
		}
		buf.Write(frame)
	}

	// Decode all events
	decoder := NewFrameDecoder(&buf)
	decoded := make([]*types.EventEnvelope, 0, len(events))

	for {
		payload, err := decoder.ReadFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadFrame failed: %v", err)
		}

		env, err := DecodeEventEnvelope(payload)
		if err != nil {
			t.Fatalf("DecodeEventEnvelope failed: %v", err)
		}
		decoded = append(decoded, env)
	}

	if len(decoded) != len(events) {
		t.Fatalf("decoded %d events, want %d", len(decoded), len(events))
	}

	for i, env := range decoded {
		if env.EventID != events[i].EventID {
			t.Errorf("events[%d].EventID = %q, want %q", i, env.EventID, events[i].EventID)
		}
		if env.Type != events[i].Type {
			t.Errorf("events[%d].Type = %q, want %q", i, env.Type, events[i].Type)
		}
		if env.Seq != events[i].Seq {
			t.Errorf("events[%d].Seq = %d, want %d", i, env.Seq, events[i].Seq)
		}
	}
}

func TestFrameDecoder_TerminalEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		envelope *types.EventEnvelope
		terminal bool
	}{
		{
			name: "run_complete is terminal",
			envelope: &types.EventEnvelope{
				ContractVersion: types.Version,
				EventID:         "evt-001",
				RunID:           "run-001",
				Seq:             1,
				Type:            types.EventTypeRunComplete,
				Ts:              "2024-01-15T10:00:00Z",
				Attempt:         1,
				Payload:         map[string]any{},
			},
			terminal: true,
		},
		{
			name: "run_error is terminal",
			envelope: &types.EventEnvelope{
				ContractVersion: types.Version,
				EventID:         "evt-001",
				RunID:           "run-001",
				Seq:             1,
				Type:            types.EventTypeRunError,
				Ts:              "2024-01-15T10:00:00Z",
				Attempt:         1,
				Payload: map[string]any{
					"error_type": "script_error",
					"message":    "test error",
				},
			},
			terminal: true,
		},
		{
			name: "item is not terminal",
			envelope: &types.EventEnvelope{
				ContractVersion: types.Version,
				EventID:         "evt-001",
				RunID:           "run-001",
				Seq:             1,
				Type:            types.EventTypeItem,
				Ts:              "2024-01-15T10:00:00Z",
				Attempt:         1,
				Payload:         map[string]any{"item_type": "product"},
			},
			terminal: false,
		},
		{
			name: "log is not terminal",
			envelope: &types.EventEnvelope{
				ContractVersion: types.Version,
				EventID:         "evt-001",
				RunID:           "run-001",
				Seq:             1,
				Type:            types.EventTypeLog,
				Ts:              "2024-01-15T10:00:00Z",
				Attempt:         1,
				Payload:         map[string]any{"level": "info", "message": "test"},
			},
			terminal: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame, err := encodeEventFrame(tt.envelope)
			if err != nil {
				t.Fatalf("encodeEventFrame failed: %v", err)
			}

			decoder := NewFrameDecoder(bytes.NewReader(frame))
			payload, err := decoder.ReadFrame()
			if err != nil {
				t.Fatalf("ReadFrame failed: %v", err)
			}

			decoded, err := DecodeEventEnvelope(payload)
			if err != nil {
				t.Fatalf("DecodeEventEnvelope failed: %v", err)
			}

			if decoded.Type.IsTerminal() != tt.terminal {
				t.Errorf("IsTerminal() = %v, want %v", decoded.Type.IsTerminal(), tt.terminal)
			}
		})
	}
}

func TestFrameDecoder_ArtifactChunk(t *testing.T) {
	t.Parallel()

	chunk := &types.ArtifactChunkFrame{
		Type:       "artifact_chunk",
		ArtifactID: "art-001",
		Seq:        1,
		IsLast:     true,
		Data:       []byte("hello world"),
	}

	frame, err := encodeArtifactChunkFrame(chunk)
	if err != nil {
		t.Fatalf("encodeArtifactChunkFrame failed: %v", err)
	}

	decoder := NewFrameDecoder(bytes.NewReader(frame))
	payload, err := decoder.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	// Use DecodeFrame to discriminate
	result, err := DecodeFrame(payload)
	if err != nil {
		t.Fatalf("DecodeFrame failed: %v", err)
	}

	decoded, ok := result.(*types.ArtifactChunkFrame)
	if !ok {
		t.Fatalf("DecodeFrame returned %T, want *types.ArtifactChunkFrame", result)
	}

	if decoded.ArtifactID != chunk.ArtifactID {
		t.Errorf("ArtifactID = %q, want %q", decoded.ArtifactID, chunk.ArtifactID)
	}
	if decoded.Seq != chunk.Seq {
		t.Errorf("Seq = %d, want %d", decoded.Seq, chunk.Seq)
	}
	if decoded.IsLast != chunk.IsLast {
		t.Errorf("IsLast = %v, want %v", decoded.IsLast, chunk.IsLast)
	}
	if !bytes.Equal(decoded.Data, chunk.Data) {
		t.Errorf("Data = %q, want %q", decoded.Data, chunk.Data)
	}
}

func TestFrameDecoder_MixedEventsAndChunks(t *testing.T) {
	t.Parallel()

	// Simulate a typical run: item event, artifact event, artifact chunks, run_complete
	var buf bytes.Buffer

	// 1. Item event
	itemEnv := &types.EventEnvelope{
		ContractVersion: types.Version,
		EventID:         "evt-001",
		RunID:           "run-001",
		Seq:             1,
		Type:            types.EventTypeItem,
		Ts:              "2024-01-15T10:00:00Z",
		Attempt:         1,
		Payload:         map[string]any{"item_type": "product"},
	}
	frame, _ := encodeEventFrame(itemEnv)
	buf.Write(frame)

	// 2. Artifact event (metadata)
	artifactEnv := &types.EventEnvelope{
		ContractVersion: types.Version,
		EventID:         "evt-002",
		RunID:           "run-001",
		Seq:             2,
		Type:            types.EventTypeArtifact,
		Ts:              "2024-01-15T10:00:01Z",
		Attempt:         1,
		Payload: map[string]any{
			"artifact_id":  "art-001",
			"name":         "screenshot.png",
			"content_type": "image/png",
			"size_bytes":   1024,
		},
	}
	frame, _ = encodeEventFrame(artifactEnv)
	buf.Write(frame)

	// 3. Artifact chunks
	chunk1 := &types.ArtifactChunkFrame{
		Type:       "artifact_chunk",
		ArtifactID: "art-001",
		Seq:        1,
		IsLast:     false,
		Data:       []byte("chunk1"),
	}
	frame, _ = encodeArtifactChunkFrame(chunk1)
	buf.Write(frame)

	chunk2 := &types.ArtifactChunkFrame{
		Type:       "artifact_chunk",
		ArtifactID: "art-001",
		Seq:        2,
		IsLast:     true,
		Data:       []byte("chunk2"),
	}
	frame, _ = encodeArtifactChunkFrame(chunk2)
	buf.Write(frame)

	// 4. Run complete
	completeEnv := &types.EventEnvelope{
		ContractVersion: types.Version,
		EventID:         "evt-003",
		RunID:           "run-001",
		Seq:             3,
		Type:            types.EventTypeRunComplete,
		Ts:              "2024-01-15T10:00:02Z",
		Attempt:         1,
		Payload:         map[string]any{},
	}
	frame, _ = encodeEventFrame(completeEnv)
	buf.Write(frame)

	// Decode and verify order
	decoder := NewFrameDecoder(&buf)
	var events []*types.EventEnvelope
	var chunks []*types.ArtifactChunkFrame

	for {
		payload, err := decoder.ReadFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadFrame failed: %v", err)
		}

		result, err := DecodeFrame(payload)
		if err != nil {
			t.Fatalf("DecodeFrame failed: %v", err)
		}

		switch v := result.(type) {
		case *types.EventEnvelope:
			events = append(events, v)
		case *types.ArtifactChunkFrame:
			chunks = append(chunks, v)
		default:
			t.Fatalf("unexpected type: %T", v)
		}
	}

	if len(events) != 3 {
		t.Errorf("got %d events, want 3", len(events))
	}
	if len(chunks) != 2 {
		t.Errorf("got %d chunks, want 2", len(chunks))
	}

	// Verify terminal event is last
	if len(events) > 0 && !events[len(events)-1].Type.IsTerminal() {
		t.Error("last event should be terminal")
	}

	// Verify chunk ordering
	if len(chunks) >= 2 {
		if chunks[0].Seq != 1 || chunks[1].Seq != 2 {
			t.Errorf("chunks out of order: seq %d, %d", chunks[0].Seq, chunks[1].Seq)
		}
		if chunks[0].IsLast || !chunks[1].IsLast {
			t.Error("IsLast flags incorrect")
		}
	}
}

// TestFrameDecoder_PartialFrame validates fatal error for truncated frames.
// Per CONTRACT_IPC.md: "If a frame is truncated or malformed, the runtime must
// treat it as a fatal stream error and terminate the run."
func TestFrameDecoder_PartialFrame(t *testing.T) {
	t.Parallel()

	// Create a valid frame but truncate it
	envelope := &types.EventEnvelope{
		ContractVersion: types.Version,
		EventID:         "evt-001",
		RunID:           "run-001",
		Seq:             1,
		Type:            types.EventTypeItem,
		Ts:              "2024-01-15T10:00:00Z",
		Attempt:         1,
		Payload:         map[string]any{},
	}

	frame, _ := encodeEventFrame(envelope)

	// Truncate the frame (keep only length prefix + half payload)
	truncated := frame[:LengthPrefixSize+len(frame[LengthPrefixSize:])/2]

	decoder := NewFrameDecoder(bytes.NewReader(truncated))
	_, err := decoder.ReadFrame()

	if err == nil {
		t.Fatal("expected error for truncated frame")
	}

	if !IsFatalFrameError(err) {
		t.Errorf("expected fatal frame error, got: %v", err)
	}

	var frameErr *FrameError
	if !errors.As(err, &frameErr) {
		t.Fatalf("expected *FrameError, got %T", err)
	}

	if frameErr.Kind != FrameErrorPartial {
		t.Errorf("Kind = %v, want FrameErrorPartial", frameErr.Kind)
	}

	// Verify IsFatal() directly
	if !frameErr.IsFatal() {
		t.Error("FrameErrorPartial.IsFatal() should return true")
	}
}

// TestFrameDecoder_OversizedFrame validates fatal error for frames exceeding max size.
// Per CONTRACT_IPC.md: "Maximum frame size: 16 MiB. Frames exceeding this limit
// are invalid and must be rejected."
func TestFrameDecoder_OversizedFrame(t *testing.T) {
	t.Parallel()

	// Create a length prefix claiming a payload larger than MaxPayloadSize
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, uint32(MaxPayloadSize+1))

	decoder := NewFrameDecoder(&buf)
	_, err := decoder.ReadFrame()

	if err == nil {
		t.Fatal("expected error for oversized frame")
	}

	if !IsFatalFrameError(err) {
		t.Errorf("expected fatal frame error, got: %v", err)
	}

	var frameErr *FrameError
	if !errors.As(err, &frameErr) {
		t.Fatalf("expected *FrameError, got %T", err)
	}

	if frameErr.Kind != FrameErrorTooLarge {
		t.Errorf("Kind = %v, want FrameErrorTooLarge", frameErr.Kind)
	}

	// Verify IsFatal() directly
	if !frameErr.IsFatal() {
		t.Error("FrameErrorTooLarge.IsFatal() should return true")
	}
}

func TestFrameDecoder_EmptyStream(t *testing.T) {
	t.Parallel()

	decoder := NewFrameDecoder(bytes.NewReader(nil))
	_, err := decoder.ReadFrame()

	if !errors.Is(err, io.EOF) {
		t.Errorf("expected io.EOF, got: %v", err)
	}
}

// TestFrameDecoder_TruncatedLengthPrefix validates fatal error when length prefix is incomplete.
// Per CONTRACT_IPC.md: partial frames are fatal.
func TestFrameDecoder_TruncatedLengthPrefix(t *testing.T) {
	t.Parallel()

	// Only 2 bytes instead of 4
	partial := []byte{0x00, 0x00}

	decoder := NewFrameDecoder(bytes.NewReader(partial))
	_, err := decoder.ReadFrame()

	if err == nil {
		t.Fatal("expected error for truncated length prefix")
	}

	if !IsFatalFrameError(err) {
		t.Errorf("expected fatal frame error, got: %v", err)
	}

	var frameErr *FrameError
	if !errors.As(err, &frameErr) {
		t.Fatalf("expected *FrameError, got %T", err)
	}

	if frameErr.Kind != FrameErrorPartial {
		t.Errorf("Kind = %v, want FrameErrorPartial", frameErr.Kind)
	}
}

// TestFrameDecoder_MalformedMsgpack validates decode error for invalid msgpack.
// Decode errors are non-fatal (the frame was read correctly, just couldn't decode).
func TestFrameDecoder_MalformedMsgpack(t *testing.T) {
	t.Parallel()

	// Valid frame length prefix but garbage msgpack payload
	garbage := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	frame := encodeFrame(garbage)

	decoder := NewFrameDecoder(bytes.NewReader(frame))
	payload, err := decoder.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	// Decoding should fail
	_, err = DecodeFrame(payload)
	if err == nil {
		t.Fatal("expected decode error for malformed msgpack")
	}

	var frameErr *FrameError
	if !errors.As(err, &frameErr) {
		t.Fatalf("expected *FrameError, got %T", err)
	}

	if frameErr.Kind != FrameErrorDecode {
		t.Errorf("Kind = %v, want FrameErrorDecode", frameErr.Kind)
	}

	// Decode errors are NOT fatal (frame was valid, content wasn't)
	if IsFatalFrameError(err) {
		t.Error("decode errors should not be fatal")
	}
}

// TestFrameError_ErrorMessage validates error message formatting.
func TestFrameError_ErrorMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      *FrameError
		contains string
	}{
		{
			name:     "partial without underlying error",
			err:      &FrameError{Kind: FrameErrorPartial, Msg: "truncated"},
			contains: "truncated",
		},
		{
			name: "partial with underlying error",
			err: &FrameError{
				Kind: FrameErrorPartial,
				Msg:  "read failed",
				Err:  io.ErrUnexpectedEOF,
			},
			contains: "unexpected EOF",
		},
		{
			name:     "oversized",
			err:      &FrameError{Kind: FrameErrorTooLarge, Msg: "payload too big"},
			contains: "too big",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := tt.err.Error()
			if !bytes.Contains([]byte(msg), []byte(tt.contains)) {
				t.Errorf("error message %q does not contain %q", msg, tt.contains)
			}
		})
	}
}

// TestFrameError_Unwrap validates error unwrapping.
func TestFrameError_Unwrap(t *testing.T) {
	t.Parallel()

	underlying := io.ErrUnexpectedEOF
	err := &FrameError{
		Kind: FrameErrorPartial,
		Msg:  "test",
		Err:  underlying,
	}

	if !errors.Is(err, underlying) {
		t.Error("Unwrap should allow errors.Is to find underlying error")
	}
}

// encodeFileWriteFrame encodes a file write frame as a framed msgpack payload.
func encodeFileWriteFrame(fw *types.FileWriteFrame) ([]byte, error) {
	payload, err := msgpack.Marshal(fw)
	if err != nil {
		return nil, err
	}
	return encodeFrame(payload), nil
}

// TestDecodeFrame_FileWrite validates file_write frame decoding with write_id.
func TestDecodeFrame_FileWrite(t *testing.T) {
	t.Parallel()

	fw := &types.FileWriteFrame{
		Type:        "file_write",
		WriteID:     42,
		Filename:    "image.png",
		ContentType: "image/png",
		Data:        []byte("fake png data"),
	}

	frame, err := encodeFileWriteFrame(fw)
	if err != nil {
		t.Fatalf("encodeFileWriteFrame failed: %v", err)
	}

	decoder := NewFrameDecoder(bytes.NewReader(frame))
	payload, err := decoder.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	result, err := DecodeFrame(payload)
	if err != nil {
		t.Fatalf("DecodeFrame failed: %v", err)
	}

	decoded, ok := result.(*types.FileWriteFrame)
	if !ok {
		t.Fatalf("DecodeFrame returned %T, want *types.FileWriteFrame", result)
	}

	if decoded.WriteID != 42 {
		t.Errorf("WriteID = %d, want 42", decoded.WriteID)
	}
	if decoded.Filename != "image.png" {
		t.Errorf("Filename = %q, want %q", decoded.Filename, "image.png")
	}
	if decoded.ContentType != "image/png" {
		t.Errorf("ContentType = %q, want %q", decoded.ContentType, "image/png")
	}
	if !bytes.Equal(decoded.Data, fw.Data) {
		t.Errorf("Data = %q, want %q", decoded.Data, fw.Data)
	}
}

// TestDecodeFrame_FileWrite_ZeroWriteID validates backward compat with write_id=0.
func TestDecodeFrame_FileWrite_ZeroWriteID(t *testing.T) {
	t.Parallel()

	fw := &types.FileWriteFrame{
		Type:        "file_write",
		WriteID:     0,
		Filename:    "data.csv",
		ContentType: "text/csv",
		Data:        []byte("a,b,c"),
	}

	frame, err := encodeFileWriteFrame(fw)
	if err != nil {
		t.Fatalf("encodeFileWriteFrame failed: %v", err)
	}

	decoder := NewFrameDecoder(bytes.NewReader(frame))
	payload, err := decoder.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	decoded, err := DecodeFileWrite(payload)
	if err != nil {
		t.Fatalf("DecodeFileWrite failed: %v", err)
	}

	if decoded.WriteID != 0 {
		t.Errorf("WriteID = %d, want 0", decoded.WriteID)
	}
}

// TestEncodeDecodeFileWriteAck_Success validates roundtrip for success ack.
func TestEncodeDecodeFileWriteAck_Success(t *testing.T) {
	t.Parallel()

	ack := &types.FileWriteAckFrame{
		Type:    "file_write_ack",
		WriteID: 7,
		OK:      true,
	}

	frame, err := EncodeFileWriteAck(ack)
	if err != nil {
		t.Fatalf("EncodeFileWriteAck failed: %v", err)
	}

	decoder := NewFrameDecoder(bytes.NewReader(frame))
	payload, err := decoder.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	decoded, err := DecodeFileWriteAck(payload)
	if err != nil {
		t.Fatalf("DecodeFileWriteAck failed: %v", err)
	}

	if decoded.Type != "file_write_ack" {
		t.Errorf("Type = %q, want %q", decoded.Type, "file_write_ack")
	}
	if decoded.WriteID != 7 {
		t.Errorf("WriteID = %d, want 7", decoded.WriteID)
	}
	if !decoded.OK {
		t.Error("OK = false, want true")
	}
	if decoded.Error != nil {
		t.Errorf("Error = %v, want nil", decoded.Error)
	}
}

// TestEncodeDecodeFileWriteAck_Error validates roundtrip for error ack.
func TestEncodeDecodeFileWriteAck_Error(t *testing.T) {
	t.Parallel()

	errMsg := "S3 PutObject failed: 500 Internal Server Error"
	ack := &types.FileWriteAckFrame{
		Type:    "file_write_ack",
		WriteID: 3,
		OK:      false,
		Error:   &errMsg,
	}

	frame, err := EncodeFileWriteAck(ack)
	if err != nil {
		t.Fatalf("EncodeFileWriteAck failed: %v", err)
	}

	decoder := NewFrameDecoder(bytes.NewReader(frame))
	payload, err := decoder.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	decoded, err := DecodeFileWriteAck(payload)
	if err != nil {
		t.Fatalf("DecodeFileWriteAck failed: %v", err)
	}

	if decoded.OK {
		t.Error("OK = true, want false")
	}
	if decoded.WriteID != 3 {
		t.Errorf("WriteID = %d, want 3", decoded.WriteID)
	}
	if decoded.Error == nil {
		t.Fatal("Error = nil, want error message")
	}
	if *decoded.Error != errMsg {
		t.Errorf("Error = %q, want %q", *decoded.Error, errMsg)
	}
}

// TestDecodeFrame_FileWriteAck validates DecodeFrame routes file_write_ack correctly.
func TestDecodeFrame_FileWriteAck(t *testing.T) {
	t.Parallel()

	ack := &types.FileWriteAckFrame{
		Type:    "file_write_ack",
		WriteID: 1,
		OK:      true,
	}

	frame, err := EncodeFileWriteAck(ack)
	if err != nil {
		t.Fatalf("EncodeFileWriteAck failed: %v", err)
	}

	decoder := NewFrameDecoder(bytes.NewReader(frame))
	payload, err := decoder.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	result, err := DecodeFrame(payload)
	if err != nil {
		t.Fatalf("DecodeFrame failed: %v", err)
	}

	decoded, ok := result.(*types.FileWriteAckFrame)
	if !ok {
		t.Fatalf("DecodeFrame returned %T, want *types.FileWriteAckFrame", result)
	}

	if decoded.WriteID != 1 {
		t.Errorf("WriteID = %d, want 1", decoded.WriteID)
	}
	if !decoded.OK {
		t.Error("OK = false, want true")
	}
}

// TestIsFatalFrameError_NonFrameError validates IsFatalFrameError with non-FrameError.
func TestIsFatalFrameError_NonFrameError(t *testing.T) {
	t.Parallel()

	regularErr := errors.New("regular error")
	if IsFatalFrameError(regularErr) {
		t.Error("regular errors should not be fatal frame errors")
	}

	if IsFatalFrameError(nil) {
		t.Error("nil should not be a fatal frame error")
	}

	if IsFatalFrameError(io.EOF) {
		t.Error("io.EOF should not be a fatal frame error")
	}
}

// encodeRunResultFrame encodes a run result frame as a framed msgpack payload.
func encodeRunResultFrame(result *types.RunResultFrame) ([]byte, error) {
	payload, err := msgpack.Marshal(result)
	if err != nil {
		return nil, err
	}
	return encodeFrame(payload), nil
}

// TestDecodeFrame_RunResult validates run_result frame decoding.
func TestDecodeFrame_RunResult(t *testing.T) {
	t.Parallel()

	message := "run completed successfully"
	result := &types.RunResultFrame{
		Type: "run_result",
		Outcome: types.RunResultOutcome{
			Status:  types.RunResultStatusCompleted,
			Message: &message,
		},
	}

	frame, err := encodeRunResultFrame(result)
	if err != nil {
		t.Fatalf("encodeRunResultFrame failed: %v", err)
	}

	decoder := NewFrameDecoder(bytes.NewReader(frame))
	payload, err := decoder.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	decoded, err := DecodeFrame(payload)
	if err != nil {
		t.Fatalf("DecodeFrame failed: %v", err)
	}

	resultFrame, ok := decoded.(*types.RunResultFrame)
	if !ok {
		t.Fatalf("expected *types.RunResultFrame, got %T", decoded)
	}

	if resultFrame.Type != "run_result" {
		t.Errorf("result.Type = %q, want %q", resultFrame.Type, "run_result")
	}

	if resultFrame.Outcome.Status != types.RunResultStatusCompleted {
		t.Errorf("result.Outcome.Status = %q, want %q", resultFrame.Outcome.Status, types.RunResultStatusCompleted)
	}

	if resultFrame.Outcome.Message == nil || *resultFrame.Outcome.Message != message {
		t.Errorf("result.Outcome.Message = %v, want %q", resultFrame.Outcome.Message, message)
	}
}

// TestDecodeFrame_RunResult_WithProxy validates run_result frame with proxy_used.
func TestDecodeFrame_RunResult_WithProxy(t *testing.T) {
	t.Parallel()

	username := "user"
	result := &types.RunResultFrame{
		Type: "run_result",
		Outcome: types.RunResultOutcome{
			Status: types.RunResultStatusCompleted,
		},
		ProxyUsed: &types.ProxyEndpointRedacted{
			Protocol: types.ProxyProtocolHTTP,
			Host:     "proxy.example.com",
			Port:     8080,
			Username: &username,
		},
	}

	frame, err := encodeRunResultFrame(result)
	if err != nil {
		t.Fatalf("encodeRunResultFrame failed: %v", err)
	}

	decoder := NewFrameDecoder(bytes.NewReader(frame))
	payload, err := decoder.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	decoded, err := DecodeFrame(payload)
	if err != nil {
		t.Fatalf("DecodeFrame failed: %v", err)
	}

	resultFrame, ok := decoded.(*types.RunResultFrame)
	if !ok {
		t.Fatalf("expected *types.RunResultFrame, got %T", decoded)
	}

	if resultFrame.ProxyUsed == nil {
		t.Fatal("expected ProxyUsed to be set")
	}

	if resultFrame.ProxyUsed.Protocol != types.ProxyProtocolHTTP {
		t.Errorf("ProxyUsed.Protocol = %q, want %q", resultFrame.ProxyUsed.Protocol, types.ProxyProtocolHTTP)
	}

	if resultFrame.ProxyUsed.Host != "proxy.example.com" {
		t.Errorf("ProxyUsed.Host = %q, want %q", resultFrame.ProxyUsed.Host, "proxy.example.com")
	}

	if resultFrame.ProxyUsed.Port != 8080 {
		t.Errorf("ProxyUsed.Port = %d, want %d", resultFrame.ProxyUsed.Port, 8080)
	}

	if resultFrame.ProxyUsed.Username == nil || *resultFrame.ProxyUsed.Username != username {
		t.Errorf("ProxyUsed.Username = %v, want %q", resultFrame.ProxyUsed.Username, username)
	}
}

// TestDecodeFrame_RunResult_Error validates run_result frame with error status.
func TestDecodeFrame_RunResult_Error(t *testing.T) {
	t.Parallel()

	message := "Something went wrong"
	errorType := "script_error"
	stack := "Error: something failed\n  at main.ts:42"
	result := &types.RunResultFrame{
		Type: "run_result",
		Outcome: types.RunResultOutcome{
			Status:    types.RunResultStatusError,
			Message:   &message,
			ErrorType: &errorType,
			Stack:     &stack,
		},
	}

	frame, err := encodeRunResultFrame(result)
	if err != nil {
		t.Fatalf("encodeRunResultFrame failed: %v", err)
	}

	decoder := NewFrameDecoder(bytes.NewReader(frame))
	payload, err := decoder.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	decoded, err := DecodeFrame(payload)
	if err != nil {
		t.Fatalf("DecodeFrame failed: %v", err)
	}

	resultFrame, ok := decoded.(*types.RunResultFrame)
	if !ok {
		t.Fatalf("expected *types.RunResultFrame, got %T", decoded)
	}

	if resultFrame.Outcome.Status != types.RunResultStatusError {
		t.Errorf("result.Outcome.Status = %q, want %q", resultFrame.Outcome.Status, types.RunResultStatusError)
	}

	if resultFrame.Outcome.ErrorType == nil || *resultFrame.Outcome.ErrorType != errorType {
		t.Errorf("result.Outcome.ErrorType = %v, want %q", resultFrame.Outcome.ErrorType, errorType)
	}

	if resultFrame.Outcome.Stack == nil || *resultFrame.Outcome.Stack != stack {
		t.Errorf("result.Outcome.Stack = %v, want %q", resultFrame.Outcome.Stack, stack)
	}
}

// TestDecodeFrame_RunResult_Crash validates run_result frame with crash status.
func TestDecodeFrame_RunResult_Crash(t *testing.T) {
	t.Parallel()

	message := "executor crashed"
	result := &types.RunResultFrame{
		Type: "run_result",
		Outcome: types.RunResultOutcome{
			Status:  types.RunResultStatusCrash,
			Message: &message,
		},
	}

	frame, err := encodeRunResultFrame(result)
	if err != nil {
		t.Fatalf("encodeRunResultFrame failed: %v", err)
	}

	decoder := NewFrameDecoder(bytes.NewReader(frame))
	payload, err := decoder.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	decoded, err := DecodeFrame(payload)
	if err != nil {
		t.Fatalf("DecodeFrame failed: %v", err)
	}

	resultFrame, ok := decoded.(*types.RunResultFrame)
	if !ok {
		t.Fatalf("expected *types.RunResultFrame, got %T", decoded)
	}

	if resultFrame.Outcome.Status != types.RunResultStatusCrash {
		t.Errorf("result.Outcome.Status = %q, want %q", resultFrame.Outcome.Status, types.RunResultStatusCrash)
	}
}

// TestDecodeFrame_MixedWithRunResult validates mixed stream with run_result.
// Per CONTRACT_IPC.md, run_result is a control frame that does not affect seq.
func TestDecodeFrame_MixedWithRunResult(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	// 1. Item event
	itemEnv := &types.EventEnvelope{
		ContractVersion: types.Version,
		EventID:         "evt-001",
		RunID:           "run-001",
		Seq:             1,
		Type:            types.EventTypeItem,
		Ts:              "2024-01-15T10:00:00Z",
		Attempt:         1,
		Payload:         map[string]any{"item_type": "product"},
	}
	frame, _ := encodeEventFrame(itemEnv)
	buf.Write(frame)

	// 2. Run complete
	completeEnv := &types.EventEnvelope{
		ContractVersion: types.Version,
		EventID:         "evt-002",
		RunID:           "run-001",
		Seq:             2,
		Type:            types.EventTypeRunComplete,
		Ts:              "2024-01-15T10:00:01Z",
		Attempt:         1,
		Payload:         map[string]any{},
	}
	frame, _ = encodeEventFrame(completeEnv)
	buf.Write(frame)

	// 3. Run result (control frame, after terminal)
	message := "run completed"
	result := &types.RunResultFrame{
		Type: "run_result",
		Outcome: types.RunResultOutcome{
			Status:  types.RunResultStatusCompleted,
			Message: &message,
		},
	}
	frame, _ = encodeRunResultFrame(result)
	buf.Write(frame)

	// Decode and verify
	decoder := NewFrameDecoder(&buf)
	var events []*types.EventEnvelope
	var runResults []*types.RunResultFrame

	for {
		payload, err := decoder.ReadFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadFrame failed: %v", err)
		}

		decoded, err := DecodeFrame(payload)
		if err != nil {
			t.Fatalf("DecodeFrame failed: %v", err)
		}

		switch v := decoded.(type) {
		case *types.EventEnvelope:
			events = append(events, v)
		case *types.RunResultFrame:
			runResults = append(runResults, v)
		default:
			t.Fatalf("unexpected type: %T", v)
		}
	}

	if len(events) != 2 {
		t.Errorf("got %d events, want 2", len(events))
	}
	if len(runResults) != 1 {
		t.Errorf("got %d run results, want 1", len(runResults))
	}

	// Verify run_result content
	if len(runResults) > 0 {
		if runResults[0].Outcome.Status != types.RunResultStatusCompleted {
			t.Errorf("run result status = %q, want %q", runResults[0].Outcome.Status, types.RunResultStatusCompleted)
		}
	}
}
