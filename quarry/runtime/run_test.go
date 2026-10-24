package runtime

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/pithecene/quarry/policy"
	"github.com/pithecene/quarry/types"
)

// mockExecutor is a test executor that produces configurable stdout.
// It simulates a real executor by blocking Wait() until killed or released.
type mockExecutor struct {
	mu          sync.Mutex
	stdout      *bytes.Buffer
	started     bool
	killed      bool
	exitCode    int
	startErr    error
	waitErr     error         // error to return from Wait
	killChan    chan struct{} // signals Wait to return when Kill is called
	releaseChan chan struct{} // signals Wait to return for normal completion
	blockOnWait bool          // if true, Wait blocks until kill or release
}

func newMockExecutor(stdout []byte, exitCode int) *mockExecutor {
	return &mockExecutor{
		stdout:      bytes.NewBuffer(stdout),
		exitCode:    exitCode,
		killChan:    make(chan struct{}),
		releaseChan: make(chan struct{}),
		blockOnWait: false,
	}
}

// newBlockingMockExecutor creates a mock that blocks Wait() until killed.
// This simulates a long-running executor process.
func newBlockingMockExecutor(stdout []byte, exitCode int) *mockExecutor {
	return &mockExecutor{
		stdout:      bytes.NewBuffer(stdout),
		exitCode:    exitCode,
		killChan:    make(chan struct{}),
		releaseChan: make(chan struct{}),
		blockOnWait: true,
	}
}

func (m *mockExecutor) Start(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.startErr != nil {
		return m.startErr
	}
	m.started = true
	return nil
}

func (m *mockExecutor) Stdout() io.Reader {
	return m.stdout
}

func (m *mockExecutor) Stdin() io.WriteCloser {
	return &nopWriteCloser{}
}

// nopWriteCloser is an io.WriteCloser that discards all writes.
type nopWriteCloser struct{}

func (nopWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (nopWriteCloser) Close() error                { return nil }

func (m *mockExecutor) Wait() (*ExecutorResult, error) {
	if m.blockOnWait {
		// Block until killed or released
		select {
		case <-m.killChan:
			// Killed - return with configured exit code
		case <-m.releaseChan:
			// Released for normal completion
		}
	}
	if m.waitErr != nil {
		return nil, m.waitErr
	}
	return &ExecutorResult{
		ExitCode:    m.exitCode,
		StderrBytes: []byte{},
	}, nil
}

func (m *mockExecutor) Kill() error {
	m.mu.Lock()
	alreadyKilled := m.killed
	m.killed = true
	m.mu.Unlock()

	// Signal Wait to return (only once)
	if !alreadyKilled {
		close(m.killChan)
	}
	return nil
}

func (m *mockExecutor) Release() {
	// Signal Wait to return for normal completion
	select {
	case <-m.releaseChan:
		// already closed
	default:
		close(m.releaseChan)
	}
}

func (m *mockExecutor) WasKilled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.killed
}

// flushTrackingPolicy wraps a policy and tracks flush calls.
type flushTrackingPolicy struct {
	policy.Policy
	mu          sync.Mutex
	flushCalled bool
	flushErr    error
}

func newFlushTrackingPolicy() *flushTrackingPolicy {
	return &flushTrackingPolicy{
		Policy: policy.NewNoopPolicy(),
	}
}

func (p *flushTrackingPolicy) Flush(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.flushCalled = true
	if p.flushErr != nil {
		return p.flushErr
	}
	return p.Policy.Flush(ctx)
}

func (p *flushTrackingPolicy) WasFlushed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.flushCalled
}

// encodeTestFrame encodes a payload with length prefix for IPC.
func encodeTestFrame(payload []byte) []byte {
	buf := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(payload)))
	copy(buf[4:], payload)
	return buf
}

// encodeTestEventFrame creates a framed event envelope.
func encodeTestEventFrame(envelope *types.EventEnvelope) []byte {
	payload, _ := msgpack.Marshal(envelope)
	return encodeTestFrame(payload)
}

// makeValidEventStream creates a valid event stream with run_complete.
func makeValidEventStream(runMeta *types.RunMeta) []byte {
	envelope := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-1",
		RunID:           runMeta.RunID,
		Seq:             1,
		Type:            types.EventTypeRunComplete,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{},
		Attempt:         runMeta.Attempt,
	}
	return encodeTestEventFrame(envelope)
}

// makeInvalidFrame creates an invalid msgpack frame (stream error).
func makeInvalidFrame() []byte {
	return encodeTestFrame([]byte{0xFF, 0xFF, 0xFF})
}

func TestRunOrchestrator_FlushCalledOnStreamError(t *testing.T) {
	t.Parallel()

	runMeta := &types.RunMeta{
		RunID:   "run-flush-stream",
		Attempt: 1,
	}

	// Create executor that produces invalid frame (causes stream error)
	mockExec := newMockExecutor(makeInvalidFrame(), 1)

	// Create tracking policy
	trackingPol := newFlushTrackingPolicy()

	config := &RunConfig{
		ExecutorPath: "/fake/executor",
		ScriptPath:   "/fake/script.js",
		Job:          map[string]any{},
		RunMeta:      runMeta,
		Policy:       trackingPol,
		ExecutorFactory: func(_ *ExecutorConfig) Executor {
			return mockExec
		},
	}

	orchestrator, err := NewRunOrchestrator(config)
	if err != nil {
		t.Fatalf("failed to create orchestrator: %v", err)
	}

	result, err := orchestrator.Execute(t.Context())
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// Verify outcome is executor crash (stream error)
	if result.Outcome.Status != types.OutcomeExecutorCrash {
		t.Errorf("expected OutcomeExecutorCrash, got %s", result.Outcome.Status)
	}

	// Verify flush was called despite the error
	if !trackingPol.WasFlushed() {
		t.Error("expected policy Flush to be called on stream error path")
	}
}

func TestRunOrchestrator_FlushCalledOnPolicyError(t *testing.T) {
	t.Parallel()

	runMeta := &types.RunMeta{
		RunID:   "run-flush-policy",
		Attempt: 1,
	}

	// Create executor that produces a valid event
	eventData := makeValidEventStream(runMeta)
	mockExec := newMockExecutor(eventData, 0)

	// Create policy that fails on ingestion
	failingPol := &failingIngestPolicy{
		flushTrackingPolicy: newFlushTrackingPolicy(),
	}

	config := &RunConfig{
		ExecutorPath: "/fake/executor",
		ScriptPath:   "/fake/script.js",
		Job:          map[string]any{},
		RunMeta:      runMeta,
		Policy:       failingPol,
		ExecutorFactory: func(_ *ExecutorConfig) Executor {
			return mockExec
		},
	}

	orchestrator, err := NewRunOrchestrator(config)
	if err != nil {
		t.Fatalf("failed to create orchestrator: %v", err)
	}

	result, err := orchestrator.Execute(t.Context())
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// Verify outcome is policy failure
	if result.Outcome.Status != types.OutcomePolicyFailure {
		t.Errorf("expected OutcomePolicyFailure, got %s", result.Outcome.Status)
	}

	// Verify flush was called despite the error
	if !failingPol.WasFlushed() {
		t.Error("expected policy Flush to be called on policy error path")
	}
}

// failingIngestPolicy fails on IngestEvent.
type failingIngestPolicy struct {
	*flushTrackingPolicy
}

func (p *failingIngestPolicy) IngestEvent(_ context.Context, _ *types.EventEnvelope) error {
	return &policyError{msg: "simulated policy failure"}
}

type policyError struct {
	msg string
}

func (e *policyError) Error() string {
	return e.msg
}

func TestRunOrchestrator_ExecutorKilledOnStreamError(t *testing.T) {
	t.Parallel()

	runMeta := &types.RunMeta{
		RunID:   "run-kill-stream",
		Attempt: 1,
	}

	// Create blocking executor that produces invalid frame
	// The executor blocks on Wait() until killed, simulating a long-running process
	mockExec := newBlockingMockExecutor(makeInvalidFrame(), 1)

	config := &RunConfig{
		ExecutorPath: "/fake/executor",
		ScriptPath:   "/fake/script.js",
		Job:          map[string]any{},
		RunMeta:      runMeta,
		Policy:       policy.NewNoopPolicy(),
		ExecutorFactory: func(_ *ExecutorConfig) Executor {
			return mockExec
		},
	}

	orchestrator, err := NewRunOrchestrator(config)
	if err != nil {
		t.Fatalf("failed to create orchestrator: %v", err)
	}

	_, err = orchestrator.Execute(t.Context())
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// Verify executor was killed due to stream error
	if !mockExec.WasKilled() {
		t.Error("expected executor to be killed on stream error")
	}
}

func TestRunOrchestrator_ExecutorKilledOnPolicyError(t *testing.T) {
	t.Parallel()

	runMeta := &types.RunMeta{
		RunID:   "run-kill-policy",
		Attempt: 1,
	}

	// Create blocking executor that produces a valid event
	// The executor blocks on Wait() until killed, simulating a long-running process
	eventData := makeValidEventStream(runMeta)
	mockExec := newBlockingMockExecutor(eventData, 0)

	// Create policy that fails on ingestion
	failingPol := &failingIngestPolicy{
		flushTrackingPolicy: newFlushTrackingPolicy(),
	}

	config := &RunConfig{
		ExecutorPath: "/fake/executor",
		ScriptPath:   "/fake/script.js",
		Job:          map[string]any{},
		RunMeta:      runMeta,
		Policy:       failingPol,
		ExecutorFactory: func(_ *ExecutorConfig) Executor {
			return mockExec
		},
	}

	orchestrator, err := NewRunOrchestrator(config)
	if err != nil {
		t.Fatalf("failed to create orchestrator: %v", err)
	}

	_, err = orchestrator.Execute(t.Context())
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// Verify executor was killed due to policy error
	if !mockExec.WasKilled() {
		t.Error("expected executor to be killed on policy error")
	}
}

func TestRunOrchestrator_SuccessfulRun(t *testing.T) {
	t.Parallel()

	runMeta := &types.RunMeta{
		RunID:   "run-success",
		Attempt: 1,
	}

	// Create executor that produces valid run_complete event
	eventData := makeValidEventStream(runMeta)
	mockExec := newMockExecutor(eventData, 0)

	trackingPol := newFlushTrackingPolicy()

	config := &RunConfig{
		ExecutorPath: "/fake/executor",
		ScriptPath:   "/fake/script.js",
		Job:          map[string]any{},
		RunMeta:      runMeta,
		Policy:       trackingPol,
		ExecutorFactory: func(_ *ExecutorConfig) Executor {
			return mockExec
		},
	}

	orchestrator, err := NewRunOrchestrator(config)
	if err != nil {
		t.Fatalf("failed to create orchestrator: %v", err)
	}

	result, err := orchestrator.Execute(t.Context())
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// Verify successful outcome
	if result.Outcome.Status != types.OutcomeSuccess {
		t.Errorf("expected OutcomeSuccess, got %s: %s", result.Outcome.Status, result.Outcome.Message)
	}

	// Verify flush was called on success path too
	if !trackingPol.WasFlushed() {
		t.Error("expected policy Flush to be called on success path")
	}

	// Verify executor was NOT killed on success
	if mockExec.WasKilled() {
		t.Error("executor should not be killed on successful run")
	}
}

func TestRunOrchestrator_FlushCalledOnExecutorWaitError(t *testing.T) {
	t.Parallel()

	runMeta := &types.RunMeta{
		RunID:   "run-flush-wait-err",
		Attempt: 1,
	}

	// Create executor that produces valid stream but fails on Wait
	eventData := makeValidEventStream(runMeta)
	mockExec := newMockExecutor(eventData, 0)
	mockExec.waitErr = io.ErrUnexpectedEOF // Simulate wait failure

	// Create tracking policy
	trackingPol := newFlushTrackingPolicy()

	config := &RunConfig{
		ExecutorPath: "/fake/executor",
		ScriptPath:   "/fake/script.js",
		Job:          map[string]any{},
		RunMeta:      runMeta,
		Policy:       trackingPol,
		ExecutorFactory: func(_ *ExecutorConfig) Executor {
			return mockExec
		},
	}

	orchestrator, err := NewRunOrchestrator(config)
	if err != nil {
		t.Fatalf("failed to create orchestrator: %v", err)
	}

	result, err := orchestrator.Execute(t.Context())
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// Verify outcome is executor crash (wait error)
	if result.Outcome.Status != types.OutcomeExecutorCrash {
		t.Errorf("expected OutcomeExecutorCrash, got %s", result.Outcome.Status)
	}

	// Verify flush was called despite the wait error
	if !trackingPol.WasFlushed() {
		t.Error("expected policy Flush to be called on executor wait error path")
	}
}

func TestIsStreamError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "stream error",
			err:      &IngestionError{Kind: IngestionErrorStream, Err: io.EOF},
			expected: true,
		},
		{
			name:     "policy error",
			err:      &IngestionError{Kind: IngestionErrorPolicy, Err: io.EOF},
			expected: false,
		},
		{
			name:     "canceled error",
			err:      &IngestionError{Kind: IngestionErrorCanceled, Err: context.Canceled},
			expected: false,
		},
		{
			name:     "version mismatch error",
			err:      &IngestionError{Kind: IngestionErrorVersionMismatch, Err: io.EOF},
			expected: false,
		},
		{
			name:     "non-ingestion error",
			err:      io.EOF,
			expected: false,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsStreamError(tt.err)
			if got != tt.expected {
				t.Errorf("IsStreamError(%v) = %v, want %v", tt.err, got, tt.expected)
			}
		})
	}
}

func TestIsVersionMismatchError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "version mismatch error",
			err:      &IngestionError{Kind: IngestionErrorVersionMismatch, Err: io.EOF},
			expected: true,
		},
		{
			name:     "stream error",
			err:      &IngestionError{Kind: IngestionErrorStream, Err: io.EOF},
			expected: false,
		},
		{
			name:     "policy error",
			err:      &IngestionError{Kind: IngestionErrorPolicy, Err: io.EOF},
			expected: false,
		},
		{
			name:     "canceled error",
			err:      &IngestionError{Kind: IngestionErrorCanceled, Err: context.Canceled},
			expected: false,
		},
		{
			name:     "non-ingestion error",
			err:      io.EOF,
			expected: false,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsVersionMismatchError(tt.err)
			if got != tt.expected {
				t.Errorf("IsVersionMismatchError(%v) = %v, want %v", tt.err, got, tt.expected)
			}
		})
	}
}

// makeVersionMismatchStream creates a stream with a contract version mismatch.
func makeVersionMismatchStream(runMeta *types.RunMeta) []byte {
	envelope := &types.EventEnvelope{
		ContractVersion: "0.99.0", // Wrong version
		EventID:         "evt-1",
		RunID:           runMeta.RunID,
		Seq:             1,
		Type:            types.EventTypeLog,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{"level": "info", "message": "test"},
		Attempt:         runMeta.Attempt,
	}
	return encodeTestEventFrame(envelope)
}

func TestRunOrchestrator_VersionMismatch_OutcomeMapping(t *testing.T) {
	t.Parallel()

	runMeta := &types.RunMeta{
		RunID:   "run-version-mismatch",
		Attempt: 1,
	}

	// Create executor that produces a version-mismatched event
	eventData := makeVersionMismatchStream(runMeta)
	mockExec := newMockExecutor(eventData, 1)

	trackingPol := newFlushTrackingPolicy()

	config := &RunConfig{
		ExecutorPath: "/fake/executor",
		ScriptPath:   "/fake/script.js",
		Job:          map[string]any{},
		RunMeta:      runMeta,
		Policy:       trackingPol,
		ExecutorFactory: func(_ *ExecutorConfig) Executor {
			return mockExec
		},
	}

	orchestrator, err := NewRunOrchestrator(config)
	if err != nil {
		t.Fatalf("failed to create orchestrator: %v", err)
	}

	result, err := orchestrator.Execute(t.Context())
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// Verify outcome is version_mismatch, NOT executor_crash
	if result.Outcome.Status != types.OutcomeVersionMismatch {
		t.Errorf("expected OutcomeVersionMismatch, got %s: %s", result.Outcome.Status, result.Outcome.Message)
	}

	// Verify actionable message
	if result.Outcome.Message == "" {
		t.Error("expected actionable message for version mismatch")
	}
}

// encodeTestRunResultFrame creates a framed run_result control frame.
func encodeTestRunResultFrame(status types.RunResultStatus, message *string) []byte {
	frame := &types.RunResultFrame{
		Type: types.RunResultType,
		Outcome: types.RunResultOutcome{
			Status:  status,
			Message: message,
		},
	}
	payload, _ := msgpack.Marshal(frame)
	return encodeTestFrame(payload)
}

// makeEventStreamWithRunResult creates a stream with event + run_result frame.
func makeEventStreamWithRunResult(runMeta *types.RunMeta, status types.RunResultStatus) []byte {
	return makeEventStreamWithRunResultMessage(runMeta, status, nil)
}

// makeEventStreamWithRunResultMessage creates a stream with event + run_result frame including message.
func makeEventStreamWithRunResultMessage(runMeta *types.RunMeta, status types.RunResultStatus, message *string) []byte {
	// Event frame
	envelope := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-1",
		RunID:           runMeta.RunID,
		Seq:             1,
		Type:            types.EventTypeRunComplete,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{},
		Attempt:         runMeta.Attempt,
	}
	eventFrame := encodeTestEventFrame(envelope)

	// run_result frame
	runResultFrame := encodeTestRunResultFrame(status, message)

	// Concatenate
	result := make([]byte, len(eventFrame)+len(runResultFrame))
	copy(result, eventFrame)
	copy(result[len(eventFrame):], runResultFrame)
	return result
}

func TestRunOrchestrator_ExitCodeConflictWithRunResult(t *testing.T) {
	t.Parallel()

	runMeta := &types.RunMeta{
		RunID:   "run-exit-conflict",
		Attempt: 1,
	}

	// Create executor that:
	// - Emits run_result with status "completed"
	// - But exits with non-zero code (1)
	// This simulates a misbehaving executor that reports success but crashes
	eventData := makeEventStreamWithRunResult(runMeta, types.RunResultStatusCompleted)
	mockExec := newMockExecutor(eventData, 1) // Exit code 1 = script_error

	trackingPol := newFlushTrackingPolicy()

	config := &RunConfig{
		ExecutorPath: "/fake/executor",
		ScriptPath:   "/fake/script.js",
		Job:          map[string]any{},
		RunMeta:      runMeta,
		Policy:       trackingPol,
		ExecutorFactory: func(_ *ExecutorConfig) Executor {
			return mockExec
		},
	}

	orchestrator, err := NewRunOrchestrator(config)
	if err != nil {
		t.Fatalf("failed to create orchestrator: %v", err)
	}

	result, err := orchestrator.Execute(t.Context())
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// Exit code is authoritative: exit code 1 = script_error
	// (even though run_result said "completed")
	if result.Outcome.Status != types.OutcomeScriptError {
		t.Errorf("expected OutcomeScriptError (exit code authoritative), got %s: %s",
			result.Outcome.Status, result.Outcome.Message)
	}
}

func TestRunOrchestrator_ExitCodeCrashOverridesRunResult(t *testing.T) {
	t.Parallel()

	runMeta := &types.RunMeta{
		RunID:   "run-exit-crash",
		Attempt: 1,
	}

	// Create executor that:
	// - Emits run_result with status "error" (script error)
	// - But exits with code 2 (executor crash)
	// Exit code should win - this is a crash, not a script error
	eventData := makeEventStreamWithRunResult(runMeta, types.RunResultStatusError)
	mockExec := newMockExecutor(eventData, 2) // Exit code 2 = executor_crash

	trackingPol := newFlushTrackingPolicy()

	config := &RunConfig{
		ExecutorPath: "/fake/executor",
		ScriptPath:   "/fake/script.js",
		Job:          map[string]any{},
		RunMeta:      runMeta,
		Policy:       trackingPol,
		ExecutorFactory: func(_ *ExecutorConfig) Executor {
			return mockExec
		},
	}

	orchestrator, err := NewRunOrchestrator(config)
	if err != nil {
		t.Fatalf("failed to create orchestrator: %v", err)
	}

	result, err := orchestrator.Execute(t.Context())
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// Exit code is authoritative: exit code 2 = executor_crash
	// (even though run_result said "error")
	if result.Outcome.Status != types.OutcomeExecutorCrash {
		t.Errorf("expected OutcomeExecutorCrash (exit code authoritative), got %s: %s",
			result.Outcome.Status, result.Outcome.Message)
	}
}

func TestRunOrchestrator_RunResultContextPreserved(t *testing.T) {
	t.Parallel()

	runMeta := &types.RunMeta{
		RunID:   "run-result-context",
		Attempt: 1,
	}

	// Create executor that:
	// - Emits run_result with status "error" and context (message, error_type)
	// - Exits with code 1 (script_error)
	// Exit code and run_result are consistent; context should be preserved
	msg := "TypeError: Cannot read property 'foo' of undefined"
	eventData := makeEventStreamWithRunResultMessage(runMeta, types.RunResultStatusError, &msg)
	mockExec := newMockExecutor(eventData, 1)

	trackingPol := newFlushTrackingPolicy()

	config := &RunConfig{
		ExecutorPath: "/fake/executor",
		ScriptPath:   "/fake/script.js",
		Job:          map[string]any{},
		RunMeta:      runMeta,
		Policy:       trackingPol,
		ExecutorFactory: func(_ *ExecutorConfig) Executor {
			return mockExec
		},
	}

	orchestrator, err := NewRunOrchestrator(config)
	if err != nil {
		t.Fatalf("failed to create orchestrator: %v", err)
	}

	result, err := orchestrator.Execute(t.Context())
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// Exit code determines category, run_result provides context
	if result.Outcome.Status != types.OutcomeScriptError {
		t.Errorf("expected OutcomeScriptError, got %s", result.Outcome.Status)
	}
	if result.Outcome.Message != msg {
		t.Errorf("expected message %q, got %q", msg, result.Outcome.Message)
	}
}

// =============================================================================
// Phase 4: Runtime & Ingestion Resilience Tests
// =============================================================================

// makePartialEventStream creates a stream with valid events followed by invalid data.
// This simulates executor crash mid-stream.
func makePartialEventStream(runMeta *types.RunMeta, validEventCount int) []byte {
	var buf bytes.Buffer

	// Write valid events
	for i := 1; i <= validEventCount; i++ {
		envelope := &types.EventEnvelope{
			ContractVersion: types.ContractVersion,
			EventID:         fmt.Sprintf("evt-%d", i),
			RunID:           runMeta.RunID,
			Seq:             int64(i),
			Type:            types.EventTypeItem,
			Ts:              "2024-01-01T00:00:00Z",
			Payload:         map[string]any{"item_type": "test", "data": map[string]any{"i": i}},
			Attempt:         runMeta.Attempt,
		}
		buf.Write(encodeTestEventFrame(envelope))
	}

	// Append truncated/invalid data (simulating crash mid-frame)
	buf.Write([]byte{0x00, 0x00, 0x00, 0x10}) // Length prefix for 16 bytes
	buf.Write([]byte{0xFF, 0xFE})             // Only 2 bytes instead of 16

	return buf.Bytes()
}

// makeEventStreamWithTruncatedLength creates a stream with a length prefix but EOF before payload.
func makeEventStreamWithTruncatedLength(runMeta *types.RunMeta) []byte {
	var buf bytes.Buffer

	// Valid event first
	envelope := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-1",
		RunID:           runMeta.RunID,
		Seq:             1,
		Type:            types.EventTypeItem,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{"item_type": "test", "data": map[string]any{}},
		Attempt:         runMeta.Attempt,
	}
	buf.Write(encodeTestEventFrame(envelope))

	// Truncated frame: length says 100 bytes but stream ends
	buf.Write([]byte{0x00, 0x00, 0x00, 0x64}) // Length prefix for 100 bytes
	// No payload - EOF

	return buf.Bytes()
}

func TestRunOrchestrator_ExecutorCrashMidStream(t *testing.T) {
	t.Parallel()

	runMeta := &types.RunMeta{
		RunID:   "run-crash-midstream",
		Attempt: 1,
	}

	// Create executor that produces 3 valid events then crashes (invalid data)
	eventData := makePartialEventStream(runMeta, 3)
	mockExec := newMockExecutor(eventData, 2) // Exit code 2 = crash

	trackingPol := newFlushTrackingPolicy()

	config := &RunConfig{
		ExecutorPath: "/fake/executor",
		ScriptPath:   "/fake/script.js",
		Job:          map[string]any{},
		RunMeta:      runMeta,
		Policy:       trackingPol,
		ExecutorFactory: func(_ *ExecutorConfig) Executor {
			return mockExec
		},
	}

	orchestrator, err := NewRunOrchestrator(config)
	if err != nil {
		t.Fatalf("failed to create orchestrator: %v", err)
	}

	result, err := orchestrator.Execute(t.Context())
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// Verify outcome is executor crash
	if result.Outcome.Status != types.OutcomeExecutorCrash {
		t.Errorf("expected OutcomeExecutorCrash, got %s: %s", result.Outcome.Status, result.Outcome.Message)
	}

	// Verify flush was still called (best effort)
	if !trackingPol.WasFlushed() {
		t.Error("expected policy Flush to be called even after mid-stream crash")
	}

	// Verify event count reflects partial processing
	// We sent 3 valid events before the crash, so at least 3 should be ingested
	const expectedMinEvents = 3
	if result.EventCount < expectedMinEvents {
		t.Errorf("expected at least %d events ingested before crash, got %d",
			expectedMinEvents, result.EventCount)
	}
}

func TestRunOrchestrator_ExecutorCrashTruncatedFrame(t *testing.T) {
	t.Parallel()

	runMeta := &types.RunMeta{
		RunID:   "run-crash-truncated",
		Attempt: 1,
	}

	// Create executor that produces valid event then truncated frame (simulates sudden death)
	eventData := makeEventStreamWithTruncatedLength(runMeta)
	mockExec := newMockExecutor(eventData, 2)

	trackingPol := newFlushTrackingPolicy()

	config := &RunConfig{
		ExecutorPath: "/fake/executor",
		ScriptPath:   "/fake/script.js",
		Job:          map[string]any{},
		RunMeta:      runMeta,
		Policy:       trackingPol,
		ExecutorFactory: func(_ *ExecutorConfig) Executor {
			return mockExec
		},
	}

	orchestrator, err := NewRunOrchestrator(config)
	if err != nil {
		t.Fatalf("failed to create orchestrator: %v", err)
	}

	result, err := orchestrator.Execute(t.Context())
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// Verify outcome is executor crash (stream error from truncated frame)
	if result.Outcome.Status != types.OutcomeExecutorCrash {
		t.Errorf("expected OutcomeExecutorCrash, got %s: %s", result.Outcome.Status, result.Outcome.Message)
	}

	// Verify flush was called
	if !trackingPol.WasFlushed() {
		t.Error("expected policy Flush to be called on truncated frame")
	}
}

func TestRunOrchestrator_PolicyFlushFailure_OutcomeMapping(t *testing.T) {
	t.Parallel()

	runMeta := &types.RunMeta{
		RunID:   "run-flush-fail-outcome",
		Attempt: 1,
	}

	// Create executor that produces valid complete stream
	eventData := makeValidEventStream(runMeta)
	mockExec := newMockExecutor(eventData, 0)

	// Create policy that succeeds on ingestion but fails on flush
	flushFailPol := newFlushTrackingPolicy()
	flushFailPol.flushErr = io.ErrUnexpectedEOF

	config := &RunConfig{
		ExecutorPath: "/fake/executor",
		ScriptPath:   "/fake/script.js",
		Job:          map[string]any{},
		RunMeta:      runMeta,
		Policy:       flushFailPol,
		ExecutorFactory: func(_ *ExecutorConfig) Executor {
			return mockExec
		},
	}

	orchestrator, err := NewRunOrchestrator(config)
	if err != nil {
		t.Fatalf("failed to create orchestrator: %v", err)
	}

	result, err := orchestrator.Execute(t.Context())
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// Flush failure should result in policy failure outcome
	if result.Outcome.Status != types.OutcomePolicyFailure {
		t.Errorf("expected OutcomePolicyFailure, got %s: %s", result.Outcome.Status, result.Outcome.Message)
	}
}

func TestRunOrchestrator_ExecutorStartFailure(t *testing.T) {
	t.Parallel()

	runMeta := &types.RunMeta{
		RunID:   "run-start-fail",
		Attempt: 1,
	}

	// Create executor that fails on Start
	mockExec := newMockExecutor(nil, 0)
	mockExec.startErr = io.ErrUnexpectedEOF

	trackingPol := newFlushTrackingPolicy()

	config := &RunConfig{
		ExecutorPath: "/fake/executor",
		ScriptPath:   "/fake/script.js",
		Job:          map[string]any{},
		RunMeta:      runMeta,
		Policy:       trackingPol,
		ExecutorFactory: func(_ *ExecutorConfig) Executor {
			return mockExec
		},
	}

	orchestrator, err := NewRunOrchestrator(config)
	if err != nil {
		t.Fatalf("failed to create orchestrator: %v", err)
	}

	result, err := orchestrator.Execute(t.Context())
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// Executor start failure should be executor crash
	if result.Outcome.Status != types.OutcomeExecutorCrash {
		t.Errorf("expected OutcomeExecutorCrash, got %s: %s", result.Outcome.Status, result.Outcome.Message)
	}

	// Flush should still be called (best effort)
	if !trackingPol.WasFlushed() {
		t.Error("expected policy Flush to be called even on executor start failure")
	}
}

// =============================================================================
// Outcome Mapping Verification Tests
// =============================================================================

func TestOutcomeMapping_ExitCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		exitCode       int
		expectedStatus types.OutcomeStatus
	}{
		{"exit 0 with terminal", ExitCodeCompleted, types.OutcomeSuccess},
		{"exit 1 script error", ExitCodeError, types.OutcomeScriptError},
		{"exit 2 crash", ExitCodeCrash, types.OutcomeExecutorCrash},
		{"exit 3 invalid input", ExitCodeInvalidInput, types.OutcomeExecutorCrash},
		{"exit 127 unknown", 127, types.OutcomeExecutorCrash},
		{"exit 255 unknown", 255, types.OutcomeExecutorCrash},
		{"exit -1 signal", -1, types.OutcomeExecutorCrash},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := outcomeFromExitCode(tt.exitCode)
			if got != tt.expectedStatus {
				t.Errorf("outcomeFromExitCode(%d) = %s, want %s", tt.exitCode, got, tt.expectedStatus)
			}
		})
	}
}

func TestOutcomeMapping_DetermineOutcome(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		exitCode       int
		hasTerminal    bool
		terminalType   types.EventType
		expectedStatus types.OutcomeStatus
	}{
		{
			name:           "exit 0 with run_complete",
			exitCode:       ExitCodeCompleted,
			hasTerminal:    true,
			terminalType:   types.EventTypeRunComplete,
			expectedStatus: types.OutcomeSuccess,
		},
		{
			name:           "exit 0 without terminal (anomaly)",
			exitCode:       ExitCodeCompleted,
			hasTerminal:    false,
			terminalType:   "",
			expectedStatus: types.OutcomeExecutorCrash,
		},
		{
			name:           "exit 1 with run_error",
			exitCode:       ExitCodeError,
			hasTerminal:    true,
			terminalType:   types.EventTypeRunError,
			expectedStatus: types.OutcomeScriptError,
		},
		{
			name:           "exit 1 without terminal (anomaly)",
			exitCode:       ExitCodeError,
			hasTerminal:    false,
			terminalType:   "",
			expectedStatus: types.OutcomeExecutorCrash,
		},
		{
			name:           "exit 2 crash",
			exitCode:       ExitCodeCrash,
			hasTerminal:    false,
			terminalType:   "",
			expectedStatus: types.OutcomeExecutorCrash,
		},
		{
			name:           "exit 3 invalid input",
			exitCode:       ExitCodeInvalidInput,
			hasTerminal:    false,
			terminalType:   "",
			expectedStatus: types.OutcomeExecutorCrash,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var terminal *types.EventEnvelope
			if tt.hasTerminal {
				terminal = &types.EventEnvelope{Type: tt.terminalType}
			}

			outcome := DetermineOutcome(tt.exitCode, tt.hasTerminal, terminal)
			if outcome.Status != tt.expectedStatus {
				t.Errorf("DetermineOutcome(%d, %v, %v) = %s, want %s",
					tt.exitCode, tt.hasTerminal, tt.terminalType, outcome.Status, tt.expectedStatus)
			}
		})
	}
}

// =============================================================================
// Sink Write Failure Tests (Orchestrator Level)
// =============================================================================

// sinkFailingPolicy fails writes to the underlying sink.
type sinkFailingPolicy struct {
	*flushTrackingPolicy
	failOnEvent bool
	failOnChunk bool
	writeErr    error
}

func newSinkFailingPolicy(failOnEvent, failOnChunk bool, writeErr error) *sinkFailingPolicy {
	return &sinkFailingPolicy{
		flushTrackingPolicy: newFlushTrackingPolicy(),
		failOnEvent:         failOnEvent,
		failOnChunk:         failOnChunk,
		writeErr:            writeErr,
	}
}

func (p *sinkFailingPolicy) IngestEvent(ctx context.Context, envelope *types.EventEnvelope) error {
	if p.failOnEvent {
		return &policyError{msg: "sink write failed: " + p.writeErr.Error()}
	}
	return p.flushTrackingPolicy.IngestEvent(ctx, envelope)
}

func (p *sinkFailingPolicy) IngestArtifactChunk(ctx context.Context, chunk *types.ArtifactChunk) error {
	if p.failOnChunk {
		return &policyError{msg: "chunk write failed: " + p.writeErr.Error()}
	}
	return p.flushTrackingPolicy.IngestArtifactChunk(ctx, chunk)
}

func TestRunOrchestrator_SinkWriteFailure_BeforeChunks(t *testing.T) {
	t.Parallel()

	runMeta := &types.RunMeta{
		RunID:   "run-sink-fail-event",
		Attempt: 1,
	}

	// Create executor that produces a valid event
	eventData := makeValidEventStream(runMeta)
	mockExec := newMockExecutor(eventData, 0)

	// Policy that fails on event write (before any chunks)
	sinkFailPol := newSinkFailingPolicy(true, false, io.ErrShortWrite)

	config := &RunConfig{
		ExecutorPath: "/fake/executor",
		ScriptPath:   "/fake/script.js",
		Job:          map[string]any{},
		RunMeta:      runMeta,
		Policy:       sinkFailPol,
		ExecutorFactory: func(_ *ExecutorConfig) Executor {
			return mockExec
		},
	}

	orchestrator, err := NewRunOrchestrator(config)
	if err != nil {
		t.Fatalf("failed to create orchestrator: %v", err)
	}

	result, err := orchestrator.Execute(t.Context())
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// Sink write failure during event ingestion should be policy failure
	if result.Outcome.Status != types.OutcomePolicyFailure {
		t.Errorf("expected OutcomePolicyFailure, got %s: %s", result.Outcome.Status, result.Outcome.Message)
	}
}

// makeEventStreamWithArtifactChunk creates a stream with an artifact chunk frame.
func makeEventStreamWithArtifactChunk(runMeta *types.RunMeta) []byte {
	var buf bytes.Buffer

	// Item event first
	itemEnvelope := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-1",
		RunID:           runMeta.RunID,
		Seq:             1,
		Type:            types.EventTypeItem,
		Ts:              "2024-01-01T00:00:00Z",
		Payload:         map[string]any{"item_type": "test", "data": map[string]any{}},
		Attempt:         runMeta.Attempt,
	}
	buf.Write(encodeTestEventFrame(itemEnvelope))

	// Artifact chunk frame
	chunk := &types.ArtifactChunkFrame{
		Type:       "artifact_chunk",
		ArtifactID: "art-1",
		Seq:        1,
		Data:       []byte("test data"),
		IsLast:     true,
	}
	chunkPayload, _ := msgpack.Marshal(chunk)
	buf.Write(encodeTestFrame(chunkPayload))

	// run_complete event
	completeEnvelope := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-2",
		RunID:           runMeta.RunID,
		Seq:             2,
		Type:            types.EventTypeRunComplete,
		Ts:              "2024-01-01T00:00:01Z",
		Payload:         map[string]any{},
		Attempt:         runMeta.Attempt,
	}
	buf.Write(encodeTestEventFrame(completeEnvelope))

	return buf.Bytes()
}

func TestRunOrchestrator_SinkWriteFailure_OnChunks(t *testing.T) {
	t.Parallel()

	runMeta := &types.RunMeta{
		RunID:   "run-sink-fail-chunk",
		Attempt: 1,
	}

	// Create executor that produces event + artifact chunk
	eventData := makeEventStreamWithArtifactChunk(runMeta)
	mockExec := newMockExecutor(eventData, 0)

	// Policy that fails on chunk write (after events succeed)
	sinkFailPol := newSinkFailingPolicy(false, true, io.ErrShortWrite)

	config := &RunConfig{
		ExecutorPath: "/fake/executor",
		ScriptPath:   "/fake/script.js",
		Job:          map[string]any{},
		RunMeta:      runMeta,
		Policy:       sinkFailPol,
		ExecutorFactory: func(_ *ExecutorConfig) Executor {
			return mockExec
		},
	}

	orchestrator, err := NewRunOrchestrator(config)
	if err != nil {
		t.Fatalf("failed to create orchestrator: %v", err)
	}

	result, err := orchestrator.Execute(t.Context())
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// Chunk write failure should be policy failure
	if result.Outcome.Status != types.OutcomePolicyFailure {
		t.Errorf("expected OutcomePolicyFailure, got %s: %s", result.Outcome.Status, result.Outcome.Message)
	}
}

// =============================================================================
// No Silent Data Loss Tests
// =============================================================================

// eventCountingPolicy counts events for verification.
type eventCountingPolicy struct {
	policy.Policy
	mu             sync.Mutex
	eventsIngested int
}

func newEventCountingPolicy() *eventCountingPolicy {
	return &eventCountingPolicy{
		Policy: policy.NewNoopPolicy(),
	}
}

func (p *eventCountingPolicy) IngestEvent(ctx context.Context, envelope *types.EventEnvelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.eventsIngested++
	return p.Policy.IngestEvent(ctx, envelope)
}

func (p *eventCountingPolicy) Stats() policy.Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	stats := p.Policy.Stats()
	stats.TotalEvents = int64(p.eventsIngested)
	return stats
}

func TestRunOrchestrator_NoSilentDataLoss_AllEventsIngested(t *testing.T) {
	t.Parallel()

	runMeta := &types.RunMeta{
		RunID:   "run-no-silent-loss",
		Attempt: 1,
	}

	// Create stream with multiple events
	var buf bytes.Buffer
	eventCount := 5
	for i := 1; i <= eventCount; i++ {
		envelope := &types.EventEnvelope{
			ContractVersion: types.ContractVersion,
			EventID:         fmt.Sprintf("evt-%d", i),
			RunID:           runMeta.RunID,
			Seq:             int64(i),
			Type:            types.EventTypeItem,
			Ts:              "2024-01-01T00:00:00Z",
			Payload:         map[string]any{"item_type": "test", "data": map[string]any{}},
			Attempt:         runMeta.Attempt,
		}
		buf.Write(encodeTestEventFrame(envelope))
	}
	// Terminal event
	terminal := &types.EventEnvelope{
		ContractVersion: types.ContractVersion,
		EventID:         "evt-terminal",
		RunID:           runMeta.RunID,
		Seq:             int64(eventCount + 1),
		Type:            types.EventTypeRunComplete,
		Ts:              "2024-01-01T00:00:01Z",
		Payload:         map[string]any{},
		Attempt:         runMeta.Attempt,
	}
	buf.Write(encodeTestEventFrame(terminal))

	mockExec := newMockExecutor(buf.Bytes(), 0)

	countingPol := newEventCountingPolicy()

	config := &RunConfig{
		ExecutorPath: "/fake/executor",
		ScriptPath:   "/fake/script.js",
		Job:          map[string]any{},
		RunMeta:      runMeta,
		Policy:       countingPol,
		ExecutorFactory: func(_ *ExecutorConfig) Executor {
			return mockExec
		},
	}

	orchestrator, err := NewRunOrchestrator(config)
	if err != nil {
		t.Fatalf("failed to create orchestrator: %v", err)
	}

	result, err := orchestrator.Execute(t.Context())
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// Verify success
	if result.Outcome.Status != types.OutcomeSuccess {
		t.Errorf("expected OutcomeSuccess, got %s: %s", result.Outcome.Status, result.Outcome.Message)
	}

	// Verify all events were ingested (no silent drops)
	expectedTotal := int64(eventCount + 1) // items + terminal
	stats := countingPol.Stats()
	if stats.TotalEvents != expectedTotal {
		t.Errorf("expected %d events ingested, got %d (silent data loss detected)",
			expectedTotal, stats.TotalEvents)
	}

	// Verify event count in result matches
	if result.EventCount != expectedTotal {
		t.Errorf("result.EventCount=%d, expected %d", result.EventCount, expectedTotal)
	}
}

func TestRunOrchestrator_NoSilentDataLoss_ErrorSurfaced(t *testing.T) {
	t.Parallel()

	runMeta := &types.RunMeta{
		RunID:   "run-error-surfaced",
		Attempt: 1,
	}

	// Create stream with one event then invalid data
	eventData := makePartialEventStream(runMeta, 1)
	mockExec := newMockExecutor(eventData, 2)

	config := &RunConfig{
		ExecutorPath: "/fake/executor",
		ScriptPath:   "/fake/script.js",
		Job:          map[string]any{},
		RunMeta:      runMeta,
		Policy:       policy.NewNoopPolicy(),
		ExecutorFactory: func(_ *ExecutorConfig) Executor {
			return mockExec
		},
	}

	orchestrator, err := NewRunOrchestrator(config)
	if err != nil {
		t.Fatalf("failed to create orchestrator: %v", err)
	}

	result, err := orchestrator.Execute(t.Context())
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// Error must be surfaced in outcome - not silently ignored
	if result.Outcome.Status == types.OutcomeSuccess {
		t.Error("expected non-success outcome when stream has errors (no silent failure)")
	}

	// Outcome message should indicate the problem
	if result.Outcome.Message == "" {
		t.Error("outcome message should describe the failure (not empty)")
	}
}
