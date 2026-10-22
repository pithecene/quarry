package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/policy"
	"github.com/pithecene/quarry/types"
)

// recordingStorage records RunStorage calls, in order, alongside executor
// starts, so tests can assert CONTRACT_RUN.md §Run Finalization ordering.
type recordingStorage struct {
	mu        sync.Mutex
	calls     []string
	startErr  error
	endErr    error
	metricErr error
	end       *lode.RunEnd
	startedAt time.Time
}

func (s *recordingStorage) record(call string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, call)
}

func (s *recordingStorage) WriteStartRecord(_ context.Context, _ *types.RunMeta, startedAt time.Time) error {
	s.record("start")
	s.startedAt = startedAt
	return s.startErr
}

func (s *recordingStorage) WriteMetrics(_ context.Context, _ metrics.Snapshot, _ time.Time) error {
	s.record("metrics")
	return s.metricErr
}

func (s *recordingStorage) WriteEndRecord(_ context.Context, end lode.RunEnd) error {
	s.record("end")
	s.end = &end
	return s.endErr
}

func (s *recordingStorage) sequence() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.calls, ",")
}

// startRecordingExecutor wraps the mock executor so its Start is recorded in
// the same sequence as the storage calls.
type startRecordingExecutor struct {
	*mockExecutor
	storage *recordingStorage
}

func (e *startRecordingExecutor) Start(ctx context.Context) error {
	e.storage.record("executor")
	return e.mockExecutor.Start(ctx)
}

// closeRecordingPolicy records Close in the storage call sequence.
type closeRecordingPolicy struct {
	*flushTrackingPolicy
	storage *recordingStorage
}

func (p *closeRecordingPolicy) Close() error {
	p.storage.record("close")
	return p.flushTrackingPolicy.Close()
}

func finalizeConfig(t *testing.T, storage *recordingStorage, exec *mockExecutor) *RunConfig {
	t.Helper()
	runMeta := &types.RunMeta{RunID: "run-fin", Attempt: 1}
	return &RunConfig{
		ExecutorPath:   "/fake/executor",
		ScriptPath:     "/fake/script.js",
		Job:            map[string]any{},
		RunMeta:        runMeta,
		Policy:         &closeRecordingPolicy{flushTrackingPolicy: newFlushTrackingPolicy(), storage: storage},
		Storage:        storage,
		StorageDataset: "quarry",
		Source:         "src",
		Category:       "default",
		StartedAt:      time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC),
		StorageDay:     "2026-10-21",
		ExecutorFactory: func(_ *ExecutorConfig) Executor {
			return &startRecordingExecutor{mockExecutor: exec, storage: storage}
		},
	}
}

func executeRun(t *testing.T, config *RunConfig) *RunResult {
	t.Helper()
	orchestrator, err := NewRunOrchestrator(config)
	if err != nil {
		t.Fatalf("NewRunOrchestrator: %v", err)
	}
	result, err := orchestrator.Execute(t.Context())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	return result
}

func TestExecute_FinalizationOrder(t *testing.T) {
	storage := &recordingStorage{}
	exec := newMockExecutor(makeValidEventStream(&types.RunMeta{RunID: "run-fin", Attempt: 1}), 0)
	config := finalizeConfig(t, storage, exec)
	startedAt := time.Date(2026, 10, 21, 23, 59, 59, 0, time.UTC)
	config.StartedAt = startedAt

	result := executeRun(t, config)

	// The policy is closed (its last chance to flush) before the end record.
	if got, want := storage.sequence(), "start,executor,close,metrics,end"; got != want {
		t.Fatalf("call order = %q, want %q", got, want)
	}
	if result.Outcome.Status != types.OutcomeSuccess {
		t.Fatalf("outcome = %s, want success", result.Outcome.Status)
	}
	if !storage.startedAt.Equal(startedAt) {
		t.Errorf("start record started_at = %v, want the configured %v", storage.startedAt, startedAt)
	}
	end := storage.end
	if end.Outcome != types.OutcomeSuccess || end.Terminal != types.EventTypeRunComplete {
		t.Errorf("end record = outcome %q terminal %q, want success/run_complete", end.Outcome, end.Terminal)
	}
	if !end.MetricsPersisted {
		t.Error("end record should report metrics persisted")
	}
	if result.RunRecordError != "" {
		t.Errorf("unexpected RunRecordError %q", result.RunRecordError)
	}
}

func TestExecute_StartRecordFailure_RunNeverStarts(t *testing.T) {
	storage := &recordingStorage{startErr: errors.New("store unavailable")}
	exec := newMockExecutor(makeValidEventStream(&types.RunMeta{RunID: "run-fin", Attempt: 1}), 0)

	result := executeRun(t, finalizeConfig(t, storage, exec))

	// No executor and no run data; the end record is still attempted,
	// because a failed remote write may have been persisted.
	if got, want := storage.sequence(), "start,close,end"; got != want {
		t.Fatalf("call order = %q, want %q", got, want)
	}
	if result.Outcome.Status != types.OutcomeExecutorCrash {
		t.Fatalf("outcome = %s, want executor_crash (retryable)", result.Outcome.Status)
	}
	if !strings.Contains(result.Outcome.Message, "start record") {
		t.Errorf("message %q should name the start record", result.Outcome.Message)
	}
	if storage.end.Terminal != "" {
		t.Errorf("end record terminal = %q, want none", storage.end.Terminal)
	}
}

func TestExecute_EndRecordFailure_ReportedOutcomeUnchanged(t *testing.T) {
	storage := &recordingStorage{endErr: errors.New("put failed")}
	exec := newMockExecutor(makeValidEventStream(&types.RunMeta{RunID: "run-fin", Attempt: 1}), 0)

	result := executeRun(t, finalizeConfig(t, storage, exec))

	if result.Outcome.Status != types.OutcomeSuccess {
		t.Fatalf("outcome = %s, want success (end-record failure must not change it)", result.Outcome.Status)
	}
	if !strings.Contains(result.RunRecordError, "put failed") {
		t.Errorf("RunRecordError = %q, want the write error", result.RunRecordError)
	}
}

func TestExecute_MetricsFailure_RecordedInEndRecord(t *testing.T) {
	storage := &recordingStorage{metricErr: errors.New("metrics write failed")}
	exec := newMockExecutor(makeValidEventStream(&types.RunMeta{RunID: "run-fin", Attempt: 1}), 0)

	result := executeRun(t, finalizeConfig(t, storage, exec))

	if result.Outcome.Status != types.OutcomeSuccess {
		t.Fatalf("outcome = %s, want success", result.Outcome.Status)
	}
	if storage.end.MetricsPersisted {
		t.Error("end record should report metrics not persisted")
	}
}

func TestExecute_NoStorage_WritesNothing(t *testing.T) {
	exec := newMockExecutor(makeValidEventStream(&types.RunMeta{RunID: "run-fin", Attempt: 1}), 0)
	config := finalizeConfig(t, &recordingStorage{}, exec)
	config.Storage = nil

	result := executeRun(t, config)
	if result.Outcome.Status != types.OutcomeSuccess {
		t.Fatalf("outcome = %s, want success", result.Outcome.Status)
	}
}

// A run ID that already has a start record belongs to another execution:
// nothing at all may be written for it.
func TestExecute_DuplicateRunID_WritesNothingElse(t *testing.T) {
	storage := &recordingStorage{startErr: fmt.Errorf("write failed: %w", lode.ErrPathExists)}
	exec := newMockExecutor(makeValidEventStream(&types.RunMeta{RunID: "run-fin", Attempt: 1}), 0)

	result := executeRun(t, finalizeConfig(t, storage, exec))

	if got, want := storage.sequence(), "start,close"; got != want {
		t.Fatalf("call order = %q, want %q", got, want)
	}
	if result.Outcome.Status != types.OutcomeExecutorCrash {
		t.Fatalf("outcome = %s, want executor_crash", result.Outcome.Status)
	}
}

func TestNewRunOrchestrator_RejectsUnsafeStorageKey(t *testing.T) {
	config := finalizeConfig(t, &recordingStorage{}, newMockExecutor(nil, 0))
	config.StorageDataset = "../other"
	if _, err := NewRunOrchestrator(config); !errors.Is(err, lode.ErrInvalidRunKey) {
		t.Fatalf("NewRunOrchestrator err = %v, want ErrInvalidRunKey", err)
	}
}

// unflushedClosePolicy fails its close-time flush, leaving data unpersisted.
type unflushedClosePolicy struct {
	*flushTrackingPolicy
}

func (p *unflushedClosePolicy) Close() error {
	return fmt.Errorf("%w: 3 events not persisted", policy.ErrCloseUnflushed)
}

// Data left unpersisted at close fails the run before the end record is
// written, rather than being reported after a "success" outcome.
func TestExecute_UnflushedCloseFailsRun(t *testing.T) {
	storage := &recordingStorage{}
	exec := newMockExecutor(makeValidEventStream(&types.RunMeta{RunID: "run-fin", Attempt: 1}), 0)
	config := finalizeConfig(t, storage, exec)
	config.Policy = &unflushedClosePolicy{flushTrackingPolicy: newFlushTrackingPolicy()}

	result := executeRun(t, config)

	if result.Outcome.Status != types.OutcomePolicyFailure {
		t.Fatalf("outcome = %s, want policy_failure", result.Outcome.Status)
	}
	if storage.end == nil || storage.end.Outcome != types.OutcomePolicyFailure {
		t.Fatalf("end record outcome = %+v, want policy_failure", storage.end)
	}
}

func TestNewRunOrchestrator_StartedAtMustMatchDay(t *testing.T) {
	config := finalizeConfig(t, &recordingStorage{}, newMockExecutor(nil, 0))
	config.StartedAt = time.Time{}
	if _, err := NewRunOrchestrator(config); err == nil {
		t.Error("zero StartedAt accepted with Storage")
	}
	config.StartedAt = time.Date(2026, 10, 21, 23, 59, 0, 0, time.UTC)
	config.StorageDay = "2026-10-22"
	if _, err := NewRunOrchestrator(config); err == nil {
		t.Error("StartedAt on another day than StorageDay accepted")
	}
}
