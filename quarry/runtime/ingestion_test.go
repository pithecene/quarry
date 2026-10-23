package runtime

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	lodepkg "github.com/pithecene/lode/lode"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/log"
	"github.com/pithecene/quarry/policy"
	"github.com/pithecene/quarry/types"
)

// encodeFrame encodes a payload with length prefix
func encodeFrame(payload []byte) []byte {
	buf := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(payload)))
	copy(buf[4:], payload)
	return buf
}

// encodeEventFrame creates a framed event envelope
func encodeEventFrame(envelope *types.EventEnvelope) []byte {
	payload, _ := msgpack.Marshal(envelope)
	return encodeFrame(payload)
}

func TestIngestionEngine_EnvelopeValidation_ContractVersionMismatch(t *testing.T) {
	runMeta := &types.RunMeta{
		RunID:   "run-123",
		Attempt: 1,
	}

	envelope := &types.EventEnvelope{
		ContractVersion: "0.99.0", // Wrong version
		EventID:         "evt-1",
		RunID:           "run-123",
		Seq:             1,
		Type:            types.EventTypeLog,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{"level": "info", "message": "test"},
		Attempt:         1,
	}

	data := encodeEventFrame(envelope)
	reader := bytes.NewReader(data)

	logger := log.NewLogger(runMeta)
	engine := NewIngestionEngine(reader, policy.NewNoopPolicy(), NewArtifactManager(), nil, logger, runMeta, nil, nil, nil)

	err := engine.Run(t.Context())
	if err == nil {
		t.Fatal("expected error for contract version mismatch")
	}
	if !IsVersionMismatchError(err) {
		t.Error("contract version mismatch should be version mismatch error")
	}
	if IsStreamError(err) {
		t.Error("contract version mismatch should not be stream error")
	}
	if IsPolicyError(err) {
		t.Error("contract version mismatch should not be policy error")
	}
}

func TestIngestionEngine_EnvelopeValidation_RunIDMismatch(t *testing.T) {
	runMeta := &types.RunMeta{
		RunID:   "run-123",
		Attempt: 1,
	}

	envelope := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-1",
		RunID:           "run-WRONG", // Wrong run_id
		Seq:             1,
		Type:            types.EventTypeLog,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{"level": "info", "message": "test"},
		Attempt:         1,
	}

	data := encodeEventFrame(envelope)
	reader := bytes.NewReader(data)

	logger := log.NewLogger(runMeta)
	engine := NewIngestionEngine(reader, policy.NewNoopPolicy(), NewArtifactManager(), nil, logger, runMeta, nil, nil, nil)

	err := engine.Run(t.Context())
	if err == nil {
		t.Fatal("expected error for run_id mismatch")
	}
	if IsPolicyError(err) {
		t.Error("run_id mismatch should be stream error, not policy error")
	}
}

func TestIngestionEngine_EnvelopeValidation_AttemptMismatch(t *testing.T) {
	runMeta := &types.RunMeta{
		RunID:   "run-123",
		Attempt: 1,
	}

	envelope := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-1",
		RunID:           "run-123",
		Seq:             1,
		Type:            types.EventTypeLog,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{"level": "info", "message": "test"},
		Attempt:         2, // Wrong attempt
	}

	data := encodeEventFrame(envelope)
	reader := bytes.NewReader(data)

	logger := log.NewLogger(runMeta)
	engine := NewIngestionEngine(reader, policy.NewNoopPolicy(), NewArtifactManager(), nil, logger, runMeta, nil, nil, nil)

	err := engine.Run(t.Context())
	if err == nil {
		t.Fatal("expected error for attempt mismatch")
	}
	if IsPolicyError(err) {
		t.Error("attempt mismatch should be stream error, not policy error")
	}
}

func TestIngestionEngine_SequenceViolation(t *testing.T) {
	runMeta := &types.RunMeta{
		RunID:   "run-123",
		Attempt: 1,
	}

	// Create two events with wrong sequence
	envelope1 := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-1",
		RunID:           "run-123",
		Seq:             1,
		Type:            types.EventTypeLog,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{"level": "info", "message": "test"},
		Attempt:         1,
	}
	envelope2 := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-2",
		RunID:           "run-123",
		Seq:             3, // Wrong: should be 2
		Type:            types.EventTypeLog,
		Ts:              "2024-01-01T00:00:01Z",
		Payload:         map[string]any{"level": "info", "message": "test2"},
		Attempt:         1,
	}

	var buf bytes.Buffer
	buf.Write(encodeEventFrame(envelope1))
	buf.Write(encodeEventFrame(envelope2))

	logger := log.NewLogger(runMeta)
	engine := NewIngestionEngine(&buf, policy.NewNoopPolicy(), NewArtifactManager(), nil, logger, runMeta, nil, nil, nil)

	err := engine.Run(t.Context())
	if err == nil {
		t.Fatal("expected error for sequence violation")
	}
	if IsPolicyError(err) {
		t.Error("sequence violation should be stream error, not policy error")
	}
}

func TestIngestionEngine_FrameDecodeError(t *testing.T) {
	runMeta := &types.RunMeta{
		RunID:   "run-123",
		Attempt: 1,
	}

	// Create invalid msgpack data
	invalidPayload := []byte{0xFF, 0xFF, 0xFF} // Invalid msgpack
	data := encodeFrame(invalidPayload)

	logger := log.NewLogger(runMeta)
	engine := NewIngestionEngine(bytes.NewReader(data), policy.NewNoopPolicy(), NewArtifactManager(), nil, logger, runMeta, nil, nil, nil)

	err := engine.Run(t.Context())
	if err == nil {
		t.Fatal("expected error for frame decode error")
	}
	if IsPolicyError(err) {
		t.Error("frame decode error should be stream error, not policy error")
	}
}

// failingPolicy is a policy that fails on certain events
type failingPolicy struct {
	*policy.NoopPolicy
	failOnType types.EventType
}

func (p *failingPolicy) IngestEvent(ctx context.Context, envelope *types.EventEnvelope) error {
	if envelope.Type == p.failOnType {
		return errors.New("policy failure")
	}
	return p.NoopPolicy.IngestEvent(ctx, envelope)
}

// trackingPolicy tracks flush calls for testing
type trackingPolicy struct {
	*policy.NoopPolicy
	FlushCalled bool
}

func newTrackingPolicy() *trackingPolicy {
	return &trackingPolicy{
		NoopPolicy: policy.NewNoopPolicy(),
	}
}

func (p *trackingPolicy) Flush(ctx context.Context) error {
	p.FlushCalled = true
	return p.NoopPolicy.Flush(ctx)
}

// TestIngestionEngine_PolicyFlushAfterError verifies that policy tracking works
func TestIngestionEngine_PolicyFlushAfterError(t *testing.T) {
	// This test verifies that the trackingPolicy works correctly
	// The actual flush-on-error behavior is tested at the RunOrchestrator level
	pol := newTrackingPolicy()

	// Flush should set the flag
	if err := pol.Flush(t.Context()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !pol.FlushCalled {
		t.Error("expected FlushCalled to be true")
	}
}

func TestIngestionEngine_PolicyFailure(t *testing.T) {
	runMeta := &types.RunMeta{
		RunID:   "run-123",
		Attempt: 1,
	}

	envelope := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-1",
		RunID:           "run-123",
		Seq:             1,
		Type:            types.EventTypeItem,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{"item_type": "test", "data": map[string]any{}},
		Attempt:         1,
	}

	data := encodeEventFrame(envelope)
	reader := bytes.NewReader(data)

	failPolicy := &failingPolicy{
		NoopPolicy: policy.NewNoopPolicy(),
		failOnType: types.EventTypeItem,
	}

	logger := log.NewLogger(runMeta)
	engine := NewIngestionEngine(reader, failPolicy, NewArtifactManager(), nil, logger, runMeta, nil, nil, nil)

	err := engine.Run(t.Context())
	if err == nil {
		t.Fatal("expected error for policy failure")
	}
	if !IsPolicyError(err) {
		t.Error("policy failure should be classified as policy error")
	}
}

func TestIngestionEngine_ValidEvent(t *testing.T) {
	runMeta := &types.RunMeta{
		RunID:   "run-123",
		Attempt: 1,
	}

	envelope := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-1",
		RunID:           "run-123",
		Seq:             1,
		Type:            types.EventTypeRunComplete,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{},
		Attempt:         1,
	}

	data := encodeEventFrame(envelope)
	reader := bytes.NewReader(data)

	logger := log.NewLogger(runMeta)
	engine := NewIngestionEngine(reader, policy.NewNoopPolicy(), NewArtifactManager(), nil, logger, runMeta, nil, nil, nil)

	err := engine.Run(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	terminal, hasTerminal := engine.GetTerminalEvent()
	if !hasTerminal {
		t.Error("expected terminal event to be recorded")
	}
	if terminal.Type != types.EventTypeRunComplete {
		t.Errorf("expected run_complete, got %s", terminal.Type)
	}
}

// encodeRunResultFrame creates a framed run_result frame
func encodeRunResultFrame(frame *types.RunResultFrame) []byte {
	payload, _ := msgpack.Marshal(frame)
	return encodeFrame(payload)
}

func TestIngestionEngine_RunResult_Completed(t *testing.T) {
	runMeta := &types.RunMeta{
		RunID:   "run-123",
		Attempt: 1,
	}

	// Create stream with event + run_result
	var buf bytes.Buffer

	// First: an event
	envelope := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-1",
		RunID:           "run-123",
		Seq:             1,
		Type:            types.EventTypeRunComplete,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{},
		Attempt:         1,
	}
	buf.Write(encodeEventFrame(envelope))

	// Then: run_result frame
	message := "run completed successfully"
	runResult := &types.RunResultFrame{
		Type: "run_result",
		Outcome: types.RunResultOutcome{
			Status:  types.RunResultStatusCompleted,
			Message: &message,
		},
	}
	buf.Write(encodeRunResultFrame(runResult))

	logger := log.NewLogger(runMeta)
	engine := NewIngestionEngine(&buf, policy.NewNoopPolicy(), NewArtifactManager(), nil, logger, runMeta, nil, nil, nil)

	err := engine.Run(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check run_result was captured
	result := engine.GetRunResult()
	if result == nil {
		t.Fatal("expected run_result to be captured")
	}
	if result.Outcome.Status != types.RunResultStatusCompleted {
		t.Errorf("expected status completed, got %s", result.Outcome.Status)
	}
	if result.Outcome.Message == nil || *result.Outcome.Message != message {
		t.Errorf("expected message %q, got %v", message, result.Outcome.Message)
	}

	// run_result should NOT affect seq
	if engine.CurrentSeq() != 1 {
		t.Errorf("expected seq 1, got %d (run_result should not affect seq)", engine.CurrentSeq())
	}
}

func TestIngestionEngine_RunResult_WithProxy(t *testing.T) {
	runMeta := &types.RunMeta{
		RunID:   "run-123",
		Attempt: 1,
	}

	var buf bytes.Buffer

	// Event
	envelope := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-1",
		RunID:           "run-123",
		Seq:             1,
		Type:            types.EventTypeRunComplete,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{},
		Attempt:         1,
	}
	buf.Write(encodeEventFrame(envelope))

	// Run result with proxy
	username := "user"
	runResult := &types.RunResultFrame{
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
	buf.Write(encodeRunResultFrame(runResult))

	logger := log.NewLogger(runMeta)
	engine := NewIngestionEngine(&buf, policy.NewNoopPolicy(), NewArtifactManager(), nil, logger, runMeta, nil, nil, nil)

	err := engine.Run(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := engine.GetRunResult()
	if result == nil {
		t.Fatal("expected run_result to be captured")
	}
	if result.ProxyUsed == nil {
		t.Fatal("expected proxy_used to be set")
	}
	if result.ProxyUsed.Host != "proxy.example.com" {
		t.Errorf("expected host proxy.example.com, got %s", result.ProxyUsed.Host)
	}
	if result.ProxyUsed.Port != 8080 {
		t.Errorf("expected port 8080, got %d", result.ProxyUsed.Port)
	}
}

func TestIngestionEngine_RunResult_DuplicateIgnored(t *testing.T) {
	runMeta := &types.RunMeta{
		RunID:   "run-123",
		Attempt: 1,
	}

	var buf bytes.Buffer

	// Event
	envelope := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-1",
		RunID:           "run-123",
		Seq:             1,
		Type:            types.EventTypeRunComplete,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{},
		Attempt:         1,
	}
	buf.Write(encodeEventFrame(envelope))

	// First run_result
	message1 := "first"
	runResult1 := &types.RunResultFrame{
		Type: "run_result",
		Outcome: types.RunResultOutcome{
			Status:  types.RunResultStatusCompleted,
			Message: &message1,
		},
	}
	buf.Write(encodeRunResultFrame(runResult1))

	// Second run_result (should be ignored)
	message2 := "second"
	runResult2 := &types.RunResultFrame{
		Type: "run_result",
		Outcome: types.RunResultOutcome{
			Status:  types.RunResultStatusError,
			Message: &message2,
		},
	}
	buf.Write(encodeRunResultFrame(runResult2))

	logger := log.NewLogger(runMeta)
	engine := NewIngestionEngine(&buf, policy.NewNoopPolicy(), NewArtifactManager(), nil, logger, runMeta, nil, nil, nil)

	err := engine.Run(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := engine.GetRunResult()
	if result == nil {
		t.Fatal("expected run_result to be captured")
	}
	// First one should win
	if result.Outcome.Status != types.RunResultStatusCompleted {
		t.Errorf("expected first run_result (completed), got %s", result.Outcome.Status)
	}
	if result.Outcome.Message == nil || *result.Outcome.Message != "first" {
		t.Errorf("expected first message, got %v", result.Outcome.Message)
	}
}

func TestIngestionEngine_RunResult_NotCountedInSeq(t *testing.T) {
	runMeta := &types.RunMeta{
		RunID:   "run-123",
		Attempt: 1,
	}

	var buf bytes.Buffer

	// Event 1
	envelope1 := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-1",
		RunID:           "run-123",
		Seq:             1,
		Type:            types.EventTypeItem,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{"item_type": "test", "data": map[string]any{}},
		Attempt:         1,
	}
	buf.Write(encodeEventFrame(envelope1))

	// Run result (should not affect seq)
	runResult := &types.RunResultFrame{
		Type: "run_result",
		Outcome: types.RunResultOutcome{
			Status: types.RunResultStatusCompleted,
		},
	}
	buf.Write(encodeRunResultFrame(runResult))

	// Event 2 (should still expect seq 2, not 3)
	envelope2 := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-2",
		RunID:           "run-123",
		Seq:             2, // Should be 2, not 3
		Type:            types.EventTypeRunComplete,
		Ts:              "2024-01-01T00:00:01Z",
		Payload:         map[string]any{},
		Attempt:         1,
	}
	buf.Write(encodeEventFrame(envelope2))

	logger := log.NewLogger(runMeta)
	engine := NewIngestionEngine(&buf, policy.NewNoopPolicy(), NewArtifactManager(), nil, logger, runMeta, nil, nil, nil)

	err := engine.Run(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if engine.CurrentSeq() != 2 {
		t.Errorf("expected final seq 2, got %d", engine.CurrentSeq())
	}

	// run_result should still be captured
	if engine.GetRunResult() == nil {
		t.Error("expected run_result to be captured")
	}
}

// encodeFileWriteFrame creates a framed file_write frame
func encodeFileWriteFrame(frame *types.FileWriteFrame) []byte {
	payload, _ := msgpack.Marshal(frame)
	return encodeFrame(payload)
}

func TestIngestionEngine_FileWrite_NoFileWriter_FailsFast(t *testing.T) {
	runMeta := &types.RunMeta{
		RunID:   "run-123",
		Attempt: 1,
	}

	var buf bytes.Buffer

	// First: a valid event so ingestion is underway
	envelope := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-1",
		RunID:           "run-123",
		Seq:             1,
		Type:            types.EventTypeItem,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{"item_type": "test", "data": map[string]any{}},
		Attempt:         1,
	}
	buf.Write(encodeEventFrame(envelope))

	// Then: a file_write frame (no FileWriter configured)
	fileWrite := &types.FileWriteFrame{
		Type:        "file_write",
		Filename:    "screenshot.png",
		ContentType: "image/png",
		Data:        []byte("fake-png-data"),
	}
	buf.Write(encodeFileWriteFrame(fileWrite))

	logger := log.NewLogger(runMeta)
	// nil FileWriter — this should now fail fast
	engine := NewIngestionEngine(&buf, policy.NewNoopPolicy(), NewArtifactManager(), nil, logger, runMeta, nil, nil, nil)

	err := engine.Run(t.Context())
	if err == nil {
		t.Fatal("expected error when file_write received without FileWriter")
	}
	if !IsStreamError(err) {
		t.Errorf("expected stream error, got: %v", err)
	}

	var ingErr *IngestionError
	if !errors.As(err, &ingErr) {
		t.Fatalf("expected *IngestionError, got %T", err)
	}
	if ingErr.Kind != IngestionErrorStream {
		t.Errorf("expected IngestionErrorStream, got %d", ingErr.Kind)
	}
}

// pipeCloseReader simulates a pipe that returns data, then returns a pipe close error
// (not io.EOF) - mimicking the race condition when executor exits quickly
type pipeCloseReader struct {
	data    []byte
	offset  int
	errOnce bool
}

func (r *pipeCloseReader) Read(p []byte) (n int, err error) {
	if r.offset >= len(r.data) {
		if !r.errOnce {
			r.errOnce = true
			// Return an error that is NOT io.EOF, simulating pipe closure
			return 0, errors.New("read |0: file already closed")
		}
		return 0, errors.New("read |0: file already closed")
	}
	n = copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}

// TestIngestionEngine_PipeCloseAfterTerminal verifies that pipe closure errors
// after a terminal event are treated as acceptable completion, not crashes.
// This tests the fix for issue #56 (IPC race condition).
func TestIngestionEngine_PipeCloseAfterTerminal(t *testing.T) {
	runMeta := &types.RunMeta{
		RunID:   "run-123",
		Attempt: 1,
	}

	// Create a terminal event
	envelope := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-1",
		RunID:           "run-123",
		Seq:             1,
		Type:            types.EventTypeRunComplete,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{},
		Attempt:         1,
	}

	data := encodeEventFrame(envelope)

	// Use pipeCloseReader that returns "file already closed" instead of EOF
	reader := &pipeCloseReader{data: data}

	logger := log.NewLogger(runMeta)
	engine := NewIngestionEngine(reader, policy.NewNoopPolicy(), NewArtifactManager(), nil, logger, runMeta, nil, nil, nil)

	err := engine.Run(t.Context())

	// Should NOT error - pipe closure after terminal is acceptable
	if err != nil {
		t.Fatalf("expected no error after pipe close following terminal, got: %v", err)
	}

	// Terminal event should be recorded
	terminal, hasTerminal := engine.GetTerminalEvent()
	if !hasTerminal {
		t.Error("expected terminal event to be recorded")
	}
	if terminal.Type != types.EventTypeRunComplete {
		t.Errorf("expected run_complete, got %s", terminal.Type)
	}
}

// failingFileWriter is a FileWriter that always fails PutFile.
type failingFileWriter struct {
	err error
}

func (w *failingFileWriter) PutFile(_ context.Context, _, _ string, _ []byte) error {
	return w.err
}

func TestIngestionEngine_FileWrite_SuccessAck(t *testing.T) {
	runMeta := &types.RunMeta{
		RunID:   "run-123",
		Attempt: 1,
	}

	var buf bytes.Buffer

	// A valid event so ingestion is underway
	envelope := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-1",
		RunID:           "run-123",
		Seq:             1,
		Type:            types.EventTypeItem,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{"item_type": "test", "data": map[string]any{}},
		Attempt:         1,
	}
	buf.Write(encodeEventFrame(envelope))

	// File write with write_id
	fileWrite := &types.FileWriteFrame{
		Type:        "file_write",
		WriteID:     1,
		Filename:    "screenshot.png",
		ContentType: "image/png",
		Data:        []byte("fake-png-data"),
	}
	buf.Write(encodeFileWriteFrame(fileWrite))

	// Terminal event
	terminal := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-2",
		RunID:           "run-123",
		Seq:             2,
		Type:            types.EventTypeRunComplete,
		Ts:              "2024-01-01T00:00:01Z",
		Payload:         map[string]any{},
		Attempt:         1,
	}
	buf.Write(encodeEventFrame(terminal))

	var ackBuf bytes.Buffer
	logger := log.NewLogger(runMeta)
	fw := lode.NewStubFileWriter()
	engine := NewIngestionEngine(&buf, policy.NewNoopPolicy(), NewArtifactManager(), fw, logger, runMeta, nil, nil, &ackBuf)

	err := engine.Run(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify success ack was written
	if ackBuf.Len() == 0 {
		t.Fatal("expected ack frame to be written")
	}

	// Decode the ack: 4-byte length prefix + payload
	ackData := ackBuf.Bytes()
	if len(ackData) < 4 {
		t.Fatalf("ack data too short: %d bytes", len(ackData))
	}
	payloadLen := binary.BigEndian.Uint32(ackData[:4])
	payload := ackData[4 : 4+payloadLen]

	var ack types.FileWriteAckFrame
	if err := msgpack.Unmarshal(payload, &ack); err != nil {
		t.Fatalf("failed to decode ack: %v", err)
	}

	if ack.WriteID != 1 {
		t.Errorf("WriteID = %d, want 1", ack.WriteID)
	}
	if !ack.OK {
		t.Error("OK = false, want true")
	}
	if ack.Error != nil {
		t.Errorf("Error = %v, want nil", ack.Error)
	}
}

func TestIngestionEngine_FileWrite_ErrorAck_NoStreamError(t *testing.T) {
	runMeta := &types.RunMeta{
		RunID:   "run-123",
		Attempt: 1,
	}

	var buf bytes.Buffer

	// A valid event
	envelope := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-1",
		RunID:           "run-123",
		Seq:             1,
		Type:            types.EventTypeItem,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{"item_type": "test", "data": map[string]any{}},
		Attempt:         1,
	}
	buf.Write(encodeEventFrame(envelope))

	// File write with write_id
	fileWrite := &types.FileWriteFrame{
		Type:        "file_write",
		WriteID:     5,
		Filename:    "screenshot.png",
		ContentType: "image/png",
		Data:        []byte("fake-png-data"),
	}
	buf.Write(encodeFileWriteFrame(fileWrite))

	// Terminal event — should still be processed (ingestion continues)
	terminal := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-2",
		RunID:           "run-123",
		Seq:             2,
		Type:            types.EventTypeRunComplete,
		Ts:              "2024-01-01T00:00:01Z",
		Payload:         map[string]any{},
		Attempt:         1,
	}
	buf.Write(encodeEventFrame(terminal))

	var ackBuf bytes.Buffer
	logger := log.NewLogger(runMeta)
	fw := &failingFileWriter{err: errors.New("S3 PutObject failed: 500")}
	engine := NewIngestionEngine(&buf, policy.NewNoopPolicy(), NewArtifactManager(), fw, logger, runMeta, nil, nil, &ackBuf)

	err := engine.Run(t.Context())
	// PutFile failure is recoverable — no stream error
	if err != nil {
		t.Fatalf("expected nil error (recoverable), got: %v", err)
	}

	// Terminal event should have been processed
	if !engine.HasTerminal() {
		t.Error("expected terminal event to be recorded")
	}

	// Verify error ack was written
	if ackBuf.Len() == 0 {
		t.Fatal("expected ack frame to be written")
	}

	ackData := ackBuf.Bytes()
	payloadLen := binary.BigEndian.Uint32(ackData[:4])
	payload := ackData[4 : 4+payloadLen]

	var ack types.FileWriteAckFrame
	if err := msgpack.Unmarshal(payload, &ack); err != nil {
		t.Fatalf("failed to decode ack: %v", err)
	}

	if ack.WriteID != 5 {
		t.Errorf("WriteID = %d, want 5", ack.WriteID)
	}
	if ack.OK {
		t.Error("OK = true, want false")
	}
	if ack.Error == nil {
		t.Fatal("Error = nil, want error message")
	}
	if *ack.Error != "S3 PutObject failed: 500" {
		t.Errorf("Error = %q, want %q", *ack.Error, "S3 PutObject failed: 500")
	}
}

func TestIngestionEngine_FileWrite_NilAckWriter_BackwardCompat(t *testing.T) {
	runMeta := &types.RunMeta{
		RunID:   "run-123",
		Attempt: 1,
	}

	var buf bytes.Buffer

	envelope := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-1",
		RunID:           "run-123",
		Seq:             1,
		Type:            types.EventTypeItem,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{"item_type": "test", "data": map[string]any{}},
		Attempt:         1,
	}
	buf.Write(encodeEventFrame(envelope))

	fileWrite := &types.FileWriteFrame{
		Type:        "file_write",
		WriteID:     1,
		Filename:    "screenshot.png",
		ContentType: "image/png",
		Data:        []byte("fake-png-data"),
	}
	buf.Write(encodeFileWriteFrame(fileWrite))

	terminal := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-2",
		RunID:           "run-123",
		Seq:             2,
		Type:            types.EventTypeRunComplete,
		Ts:              "2024-01-01T00:00:01Z",
		Payload:         map[string]any{},
		Attempt:         1,
	}
	buf.Write(encodeEventFrame(terminal))

	logger := log.NewLogger(runMeta)
	fw := lode.NewStubFileWriter()
	// nil ackWriter — backward compat, no panic
	engine := NewIngestionEngine(&buf, policy.NewNoopPolicy(), NewArtifactManager(), fw, logger, runMeta, nil, nil, nil)

	err := engine.Run(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// epipeWriter simulates a broken pipe (EPIPE).
type epipeWriter struct{}

func (epipeWriter) Write(_ []byte) (int, error) {
	return 0, errors.New("write |1: broken pipe")
}

func TestIngestionEngine_FileWrite_AckWriteEPIPE_NonFatal(t *testing.T) {
	runMeta := &types.RunMeta{
		RunID:   "run-123",
		Attempt: 1,
	}

	var buf bytes.Buffer

	envelope := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-1",
		RunID:           "run-123",
		Seq:             1,
		Type:            types.EventTypeItem,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{"item_type": "test", "data": map[string]any{}},
		Attempt:         1,
	}
	buf.Write(encodeEventFrame(envelope))

	fileWrite := &types.FileWriteFrame{
		Type:        "file_write",
		WriteID:     1,
		Filename:    "screenshot.png",
		ContentType: "image/png",
		Data:        []byte("fake-png-data"),
	}
	buf.Write(encodeFileWriteFrame(fileWrite))

	terminal := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-2",
		RunID:           "run-123",
		Seq:             2,
		Type:            types.EventTypeRunComplete,
		Ts:              "2024-01-01T00:00:01Z",
		Payload:         map[string]any{},
		Attempt:         1,
	}
	buf.Write(encodeEventFrame(terminal))

	logger := log.NewLogger(runMeta)
	fw := lode.NewStubFileWriter()
	// EPIPE ack writer — should not cause stream error
	engine := NewIngestionEngine(&buf, policy.NewNoopPolicy(), NewArtifactManager(), fw, logger, runMeta, nil, nil, &epipeWriter{})

	err := engine.Run(t.Context())
	if err != nil {
		t.Fatalf("expected nil error (EPIPE non-fatal), got: %v", err)
	}
}

// TestIngestionEngine_FileWrite_PostTerminal_ErrorAck verifies that a file_write
// received after a terminal event sends an error ack to guarantee executor-side
// promise settlement. Per CONTRACT_IPC.md §Terminal boundary.
func TestIngestionEngine_FileWrite_PostTerminal_ErrorAck(t *testing.T) {
	runMeta := &types.RunMeta{
		RunID:   "run-123",
		Attempt: 1,
	}

	var buf bytes.Buffer

	// Terminal event first
	terminal := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-1",
		RunID:           "run-123",
		Seq:             1,
		Type:            types.EventTypeRunComplete,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{},
		Attempt:         1,
	}
	buf.Write(encodeEventFrame(terminal))

	// File write AFTER terminal — should get error ack
	fileWrite := &types.FileWriteFrame{
		Type:        "file_write",
		WriteID:     7,
		Filename:    "late-file.png",
		ContentType: "image/png",
		Data:        []byte("late-data"),
	}
	buf.Write(encodeFileWriteFrame(fileWrite))

	var ackBuf bytes.Buffer
	logger := log.NewLogger(runMeta)
	fw := lode.NewStubFileWriter()
	engine := NewIngestionEngine(&buf, policy.NewNoopPolicy(), NewArtifactManager(), fw, logger, runMeta, nil, nil, &ackBuf)

	err := engine.Run(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify error ack was sent for the post-terminal file_write
	if ackBuf.Len() == 0 {
		t.Fatal("expected error ack frame for post-terminal file_write")
	}

	ackData := ackBuf.Bytes()
	payloadLen := binary.BigEndian.Uint32(ackData[:4])
	payload := ackData[4 : 4+payloadLen]

	var ack types.FileWriteAckFrame
	if err := msgpack.Unmarshal(payload, &ack); err != nil {
		t.Fatalf("failed to decode ack: %v", err)
	}

	if ack.WriteID != 7 {
		t.Errorf("WriteID = %d, want 7", ack.WriteID)
	}
	if ack.OK {
		t.Error("OK = true, want false (post-terminal rejection)")
	}
	if ack.Error == nil || *ack.Error != "run already terminated" {
		t.Errorf("Error = %v, want %q", ack.Error, "run already terminated")
	}
}

// TestIngestionEngine_FileWrite_PostTerminal_NoAckWriter verifies that a
// post-terminal file_write with nil ackWriter does not panic (backward compat).
func TestIngestionEngine_FileWrite_PostTerminal_NoAckWriter(t *testing.T) {
	runMeta := &types.RunMeta{
		RunID:   "run-123",
		Attempt: 1,
	}

	var buf bytes.Buffer

	terminal := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-1",
		RunID:           "run-123",
		Seq:             1,
		Type:            types.EventTypeRunComplete,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{},
		Attempt:         1,
	}
	buf.Write(encodeEventFrame(terminal))

	fileWrite := &types.FileWriteFrame{
		Type:        "file_write",
		WriteID:     1,
		Filename:    "late-file.png",
		ContentType: "image/png",
		Data:        []byte("late-data"),
	}
	buf.Write(encodeFileWriteFrame(fileWrite))

	logger := log.NewLogger(runMeta)
	fw := lode.NewStubFileWriter()
	// nil ackWriter — should not panic
	engine := NewIngestionEngine(&buf, policy.NewNoopPolicy(), NewArtifactManager(), fw, logger, runMeta, nil, nil, nil)

	err := engine.Run(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestIngestionEngine_PipeCloseBeforeTerminal verifies that pipe closure errors
// BEFORE a terminal event are still treated as stream errors (executor crash).
func TestIngestionEngine_PipeCloseBeforeTerminal(t *testing.T) {
	runMeta := &types.RunMeta{
		RunID:   "run-123",
		Attempt: 1,
	}

	// Create a non-terminal event (item)
	envelope := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-1",
		RunID:           "run-123",
		Seq:             1,
		Type:            types.EventTypeItem,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{"item_type": "test", "data": map[string]any{}},
		Attempt:         1,
	}

	data := encodeEventFrame(envelope)

	// Use pipeCloseReader that returns "file already closed" instead of EOF
	reader := &pipeCloseReader{data: data}

	logger := log.NewLogger(runMeta)
	engine := NewIngestionEngine(reader, policy.NewNoopPolicy(), NewArtifactManager(), nil, logger, runMeta, nil, nil, nil)

	err := engine.Run(t.Context())

	// SHOULD error - pipe closure before terminal is a stream error
	if err == nil {
		t.Fatal("expected error for pipe close before terminal")
	}
	if !IsStreamError(err) {
		t.Errorf("expected stream error, got: %v", err)
	}
}

// =============================================================================
// End-to-End: file_write → event flush → sidecar_files in snapshot Metadata
//
// This test verifies the full pipeline: executor emits file_write frame,
// ingestion engine calls PutFile on a real LodeClient, strict policy
// flushes events via the same client's WriteEvents, and the resulting
// snapshot contains sidecar_files metadata.
// =============================================================================

func TestIngestionEngine_FileWrite_MetadataInventory_EndToEnd(t *testing.T) {
	runMeta := &types.RunMeta{
		RunID:   "run-e2e",
		Attempt: 1,
	}

	// Set up a real LodeClient backed by in-memory storage.
	// This is both the FileWriter (for PutFile) and the sink client
	// (for WriteEvents), so pending file refs flow through correctly.
	store := lodepkg.NewMemory()
	factory := func() (lodepkg.Store, error) { return store, nil }

	cfg := lode.Config{
		Dataset:  "quarry",
		Source:   "e2e-source",
		Category: "e2e-category",
		Day:      "2026-03-22",
		RunID:    "run-e2e",
		Policy:   "strict",
	}

	client, err := lode.NewLodeClientWithFactory(cfg, factory)
	if err != nil {
		t.Fatalf("NewLodeClientWithFactory failed: %v", err)
	}

	// StrictPolicy writes each event immediately via the sink.
	sink := lode.NewSink(cfg, client)
	pol := policy.NewStrictPolicy(sink)

	// Build IPC frame sequence: file_write → item event → terminal event
	var buf bytes.Buffer

	// 1. file_write frame
	fileWrite := &types.FileWriteFrame{
		Type:        "file_write",
		WriteID:     1,
		Filename:    "product-image.png",
		ContentType: "image/png",
		Data:        []byte("fake-png-bytes"),
	}
	buf.Write(encodeFileWriteFrame(fileWrite))

	// 2. Item event (this triggers strict policy → WriteEvents → flushes pending files)
	itemEvent := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-1",
		RunID:           "run-e2e",
		Seq:             1,
		Type:            types.EventTypeItem,
		Ts:              "2026-03-22T10:00:00Z",
		Payload:         map[string]any{"item_type": "product", "data": map[string]any{"name": "Widget"}},
		Attempt:         1,
	}
	buf.Write(encodeEventFrame(itemEvent))

	// 3. Terminal event
	terminal := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-2",
		RunID:           "run-e2e",
		Seq:             2,
		Type:            types.EventTypeRunComplete,
		Ts:              "2026-03-22T10:00:01Z",
		Payload:         map[string]any{},
		Attempt:         1,
	}
	buf.Write(encodeEventFrame(terminal))

	// Run ingestion
	var ackBuf bytes.Buffer
	logger := log.NewLogger(runMeta)
	engine := NewIngestionEngine(&buf, pol, NewArtifactManager(), client, logger, runMeta, nil, nil, &ackBuf)

	if err := engine.Run(t.Context()); err != nil {
		t.Fatalf("ingestion failed: %v", err)
	}

	// Verify file_write_ack was sent
	if ackBuf.Len() == 0 {
		t.Fatal("expected file_write_ack frame")
	}

	// Read snapshots from the dataset and verify sidecar_files metadata
	ds, err := lode.NewReadDataset("quarry", factory)
	if err != nil {
		t.Fatalf("NewReadDataset failed: %v", err)
	}

	snapshots, err := ds.Snapshots(t.Context())
	if err != nil {
		t.Fatalf("Snapshots failed: %v", err)
	}

	if len(snapshots) == 0 {
		t.Fatal("expected at least one snapshot")
	}

	// Verify exactly one snapshot carries sidecar_files metadata, and that
	// it is the item/event flush snapshot (not duplicated onto the terminal
	// snapshot or any other). This pins down the "flush on event write and
	// drain afterward" contract.
	metadataCount := 0
	for _, snap := range snapshots {
		raw, ok := snap.Manifest.Metadata[lode.MetadataKeySidecarFiles]
		if !ok {
			continue
		}
		files, ok := raw.([]any)
		if !ok || len(files) == 0 {
			continue
		}
		metadataCount++

		// Verify it carries the correct file ref
		if len(files) != 1 {
			t.Fatalf("sidecar_files has %d entries, want 1", len(files))
		}
		ref, ok := files[0].(map[string]any)
		if !ok {
			t.Fatalf("file ref is %T, want map[string]any", files[0])
		}
		if ref["filename"] != "product-image.png" {
			t.Errorf("filename = %v, want product-image.png", ref["filename"])
		}
		if ref["content_type"] != "image/png" {
			t.Errorf("content_type = %v, want image/png", ref["content_type"])
		}
		// Size should match len("fake-png-bytes") = 14
		if size, ok := ref["size"].(float64); !ok || int64(size) != 14 {
			t.Errorf("size = %v, want 14", ref["size"])
		}

		// Verify this snapshot contains the item event (the first event
		// flush), not the terminal event. Read the snapshot data and
		// check for item_type in the record.
		data, err := ds.Read(t.Context(), snap.ID)
		if err != nil {
			t.Fatalf("ds.Read failed: %v", err)
		}
		if len(data) != 1 {
			t.Fatalf("snapshot has %d records, want 1", len(data))
		}
		record, ok := data[0].(map[string]any)
		if !ok {
			t.Fatalf("record is %T, want map[string]any", data[0])
		}
		if record["event_type"] != "item" {
			t.Errorf("snapshot with sidecar_files has event_type=%v, want item", record["event_type"])
		}
	}
	if metadataCount == 0 {
		t.Fatal("no snapshot contains sidecar_files metadata — file_write refs did not survive to event flush")
	}
	if metadataCount > 1 {
		t.Errorf("sidecar_files metadata found on %d snapshots, want exactly 1 (should only appear on item event flush)", metadataCount)
	}
}
