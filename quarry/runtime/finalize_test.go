package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/pithecene/quarry/internal/testutil"
	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/policy"
	"github.com/pithecene/quarry/types"
)

// recordingStorage records RunStorage calls, in order, alongside executor
// starts, so tests can assert CONTRACT_RUN.md §Run Finalization ordering.
type recordingStorage struct {
	mu         sync.Mutex
	calls      []string
	startErr   error
	endErr     error
	endErrs    []error // of the first writes of the end record, before endErr
	ends       []lode.RunEnd
	onEnd      func() // called by each write of the end record
	end        *lode.RunEnd
	startedAt  time.Time
	deadlineAt *time.Time
}

func (s *recordingStorage) record(call string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, call)
}

func (s *recordingStorage) WriteStartRecord(ctx context.Context, _ *types.RunMeta, startedAt time.Time, deadlineAt *time.Time) error {
	s.record("start")
	// A store honors its context, so a write after the context ends fails.
	if err := ctx.Err(); err != nil {
		return err
	}
	s.startedAt = startedAt
	s.deadlineAt = deadlineAt
	return s.startErr
}

func (s *recordingStorage) WriteEndRecord(_ context.Context, end lode.RunEnd) error {
	s.record("end")
	s.end = &end
	s.ends = append(s.ends, end)
	if s.onEnd != nil {
		s.onEnd()
	}
	if len(s.endErrs) > 0 {
		err := s.endErrs[0]
		s.endErrs = s.endErrs[1:]
		return err
	}
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
	t.Parallel()

	storage := &recordingStorage{}
	exec := newMockExecutor(makeValidEventStream(&types.RunMeta{RunID: "run-fin", Attempt: 1}), 0)
	config := finalizeConfig(t, storage, exec)
	startedAt := time.Date(2026, 10, 21, 23, 59, 59, 0, time.UTC)
	config.StartedAt = startedAt
	config.Collector = metrics.NewCollector("strict", "/fake/executor", "fs", "run-fin", "")

	result := executeRun(t, config)

	// The policy is closed (its last chance to flush) before the end record.
	if got, want := storage.sequence(), "start,executor,close,end"; got != want {
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
	if end.Metrics == nil || !reflect.DeepEqual(*end.Metrics, config.Collector.Snapshot()) || end.Metrics.RunsCompleted != 1 {
		t.Errorf("end record metrics = %+v, want the run's final snapshot", end.Metrics)
	}
	if want, err := json.Marshal(result.TerminalSummary); err != nil || string(end.TerminalSummary) != string(want) {
		t.Errorf("end record terminal summary = %s, want the result's %s (%v)", end.TerminalSummary, want, err)
	}
	if result.RunRecordError != "" {
		t.Errorf("unexpected RunRecordError %q", result.RunRecordError)
	}
}

// An end record is written without a payload that it cannot hold, with a warning.
func TestWriteEndRecord_OmitsAnUnencodableSummary(t *testing.T) {
	testutil.Serial(t, "swaps the process-wide os.Stderr")
	// The logger takes os.Stderr when it is built, with the run's identity.
	logFile, err := os.Create(filepath.Join(t.TempDir(), "log"))
	if err != nil {
		t.Fatal(err)
	}
	original := os.Stderr
	os.Stderr = logFile
	t.Cleanup(func() {
		os.Stderr = original
		iox.DiscardClose(logFile)
	})
	storage := &recordingStorage{}
	config := finalizeConfig(t, storage, newMockExecutor(makeValidEventStream(&types.RunMeta{RunID: "run-fin", Attempt: 1}), 0))
	orchestrator, err := NewRunOrchestrator(config)
	if err != nil {
		t.Fatal(err)
	}
	result := &RunResult{Outcome: &types.RunOutcome{}, TerminalSummary: map[string]any{"ratio": math.NaN()}}
	orchestrator.writeEndRecord(t.Context(), result)
	if storage.end == nil || storage.end.TerminalSummary != nil || result.RunRecordError != "" {
		t.Errorf("end = %+v, error %q, want a record without a summary", storage.end, result.RunRecordError)
	}
	logged, err := os.ReadFile(logFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	var line struct {
		Level   string `json:"level"`
		Message string `json:"message"`
		RunID   string `json:"run_id"`
		Attempt int    `json:"attempt"`
		Fields  struct {
			Error string `json:"error"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(logged, &line); err != nil {
		t.Fatalf("log %q: %v", logged, err)
	}
	if line.Level != "warn" || !strings.Contains(line.Message, "omits the terminal summary") ||
		line.RunID != "run-fin" || line.Attempt != 1 || !strings.Contains(line.Fields.Error, "NaN") {
		t.Errorf("logged %q, want one warning with the run's identity and the encoding error", logged)
	}
}

func TestExecute_StartRecordFailure_RunNeverStarts(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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

// The end record is written again with the same end, until storage holds it
// or the attempts run out.
func TestExecute_EndRecordAttempts(t *testing.T) {
	t.Parallel()
	refused := errors.New("put failed")
	for _, tc := range []struct {
		name     string
		startErr error
		endErrs  []error
		endErr   error
		calls    string
		recorded bool
		failed   bool
	}{
		{"first", nil, nil, nil, "start,executor,close,end", true, false},
		{"third", nil, []error{refused, refused}, nil, "start,executor,close,end,end,end", true, false},
		{"none", nil, nil, refused, "start,executor,close,end,end,end", false, true},
		// A record that exists can be this one, unreadable so far.
		{"an existing record", nil, []error{refused, lode.ErrPathExists}, nil, "start,executor,close,end,end,end", true, false},
		{"no start record", refused, nil, nil, "start,close,end", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storage := &recordingStorage{startErr: tc.startErr, endErrs: tc.endErrs, endErr: tc.endErr}
			exec := newMockExecutor(makeValidEventStream(&types.RunMeta{RunID: "run-fin", Attempt: 1}), 0)
			result := executeRun(t, finalizeConfig(t, storage, exec))
			if got := storage.sequence(); got != tc.calls {
				t.Errorf("calls = %s, want %s", got, tc.calls)
			}
			if result.Recorded != tc.recorded || (result.RunRecordError != "") != tc.failed {
				t.Errorf("recorded = %t, error %q, want %t and an error %t", result.Recorded, result.RunRecordError, tc.recorded, tc.failed)
			}
			for _, end := range storage.ends {
				if !reflect.DeepEqual(end, storage.ends[0]) {
					t.Errorf("a later attempt wrote %+v, want %+v", end, storage.ends[0])
				}
			}
		})
	}
}

// No attempt starts after the bound, whether or not the store honors it.
func TestWriteEndRecord_StartsNoAttemptAfterTheBound(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		refused := errors.New("put failed")
		storage := &recordingStorage{endErr: refused, onEnd: func() { time.Sleep(finalizeTimeout - endRecordDelay/2) }}
		config := finalizeConfig(t, storage, newMockExecutor(nil, 0))
		orchestrator, err := NewRunOrchestrator(config)
		if err != nil {
			t.Fatal(err)
		}
		result := &RunResult{RunMeta: config.RunMeta, Outcome: &types.RunOutcome{Status: types.OutcomeSuccess}}
		if orchestrator.writeEndRecord(t.Context(), result) || len(storage.ends) != 1 || !strings.Contains(result.RunRecordError, refused.Error()) {
			t.Errorf("attempts = %d, error %q, want one attempt and its error", len(storage.ends), result.RunRecordError)
		}
	})
}

func TestExecute_NoStorage_WritesNothing(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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

// openStdoutExecutor keeps its stdout open until the runtime kills it, as a
// live executor does, so a run ends only when the runtime stops it.
type openStdoutExecutor struct {
	*mockExecutor
	stdout *io.PipeReader
	writer *io.PipeWriter
}

func newOpenStdoutExecutor() *openStdoutExecutor {
	r, w := io.Pipe()
	return &openStdoutExecutor{mockExecutor: newBlockingMockExecutor(nil, 0), stdout: r, writer: w}
}

func (e *openStdoutExecutor) Stdout() io.Reader { return e.stdout }

// Start kills the executor when ctx ends, as exec.CommandContext does for
// the real executor process.
func (e *openStdoutExecutor) Start(ctx context.Context) error {
	if err := e.mockExecutor.Start(ctx); err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		_ = e.Kill()
	}()
	return nil
}

func (e *openStdoutExecutor) Kill() error {
	_ = e.writer.Close()
	return e.mockExecutor.Kill()
}

func deadlineConfig(t *testing.T, storage *recordingStorage, exec Executor, maxDuration time.Duration) *RunConfig {
	t.Helper()
	startedAt := time.Now().UTC()
	return &RunConfig{
		ExecutorPath:    "/fake/executor",
		ScriptPath:      "/fake/script.js",
		Job:             map[string]any{},
		RunMeta:         &types.RunMeta{RunID: "run-deadline", Attempt: 1},
		Policy:          newFlushTrackingPolicy(),
		Storage:         storage,
		StorageDataset:  "quarry",
		Source:          "src",
		Category:        "default",
		StartedAt:       startedAt,
		StorageDay:      lode.DeriveDay(startedAt),
		MaxDuration:     maxDuration,
		ExecutorFactory: func(_ *ExecutorConfig) Executor { return exec },
	}
}

// The runtime stops a run that outlives its max duration at the deadline
// and ends it as deadline_exceeded, and its start record states the deadline.
func TestExecute_StopsTheRunAtItsDeadline(t *testing.T) {
	t.Parallel()

	storage := &recordingStorage{}
	exec := newOpenStdoutExecutor()
	cfg := deadlineConfig(t, storage, exec, 50*time.Millisecond)
	o, err := NewRunOrchestrator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	result, err := o.Execute(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome.Status != types.OutcomeDeadlineExceeded {
		t.Fatalf("outcome = %s (%s), want %s", result.Outcome.Status, result.Outcome.Message, types.OutcomeDeadlineExceeded)
	}
	if !exec.WasKilled() {
		t.Error("the executor was not stopped at the deadline")
	}
	want := cfg.StartedAt.Add(cfg.MaxDuration)
	if storage.deadlineAt == nil || !storage.deadlineAt.Equal(want) {
		t.Errorf("start record deadline = %v, want %v", storage.deadlineAt, want)
	}
	if storage.end == nil || storage.end.Outcome != types.OutcomeDeadlineExceeded {
		t.Errorf("end record outcome = %v, want %s", storage.end, types.OutcomeDeadlineExceeded)
	}
}

// A host's cancellation before the deadline keeps its own outcome.
func TestExecute_HostCancellationBeforeTheDeadline(t *testing.T) {
	t.Parallel()

	storage := &recordingStorage{}
	exec := newOpenStdoutExecutor()
	cfg := deadlineConfig(t, storage, exec, time.Hour)
	o, err := NewRunOrchestrator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	time.AfterFunc(50*time.Millisecond, cancel)
	result, err := o.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome.Status == types.OutcomeDeadlineExceeded {
		t.Fatalf("outcome = %s, want the outcome of a canceled run", result.Outcome.Status)
	}
}

// A run without a max duration declares no deadline.
func TestExecute_NoMaxDurationDeclaresNoDeadline(t *testing.T) {
	t.Parallel()

	storage := &recordingStorage{}
	exec := newMockExecutor(makeValidEventStream(&types.RunMeta{RunID: "run-deadline", Attempt: 1}), 0)
	o, err := NewRunOrchestrator(deadlineConfig(t, storage, exec, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.Execute(t.Context()); err != nil {
		t.Fatal(err)
	}
	if storage.deadlineAt != nil {
		t.Errorf("start record deadline = %v, want none", storage.deadlineAt)
	}
}

func TestNewRunOrchestrator_RejectsANegativeMaxDuration(t *testing.T) {
	t.Parallel()

	cfg := deadlineConfig(t, &recordingStorage{}, newMockExecutor(nil, 0), -time.Second)
	if _, err := NewRunOrchestrator(cfg); err == nil {
		t.Fatal("NewRunOrchestrator accepted a negative max duration")
	}
}

// The deadline bounds the start record's write, so a run whose deadline
// passes first never starts, and it ends as deadline_exceeded.
func TestExecute_DeadlinePastAtStart(t *testing.T) {
	t.Parallel()

	storage := &recordingStorage{}
	exec := newOpenStdoutExecutor()
	o, err := NewRunOrchestrator(deadlineConfig(t, storage, exec, time.Nanosecond))
	if err != nil {
		t.Fatal(err)
	}
	result, err := o.Execute(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome.Status != types.OutcomeDeadlineExceeded {
		t.Fatalf("outcome = %s (%s), want %s", result.Outcome.Status, result.Outcome.Message, types.OutcomeDeadlineExceeded)
	}
	if exec.WasKilled() || exec.started {
		t.Error("the executor started although the run's deadline had passed")
	}
}

// slowFlushPolicy fails ingestion at once and then flushes slowly, so its
// cleanup crosses the run's deadline after execution has decided the outcome.
type slowFlushPolicy struct {
	*flushTrackingPolicy
	delay time.Duration
}

func (p *slowFlushPolicy) IngestEvent(_ context.Context, _ *types.EventEnvelope) error {
	return &policyError{msg: "simulated policy failure"}
}

func (p *slowFlushPolicy) Flush(ctx context.Context) error {
	time.Sleep(p.delay)
	return p.flushTrackingPolicy.Flush(ctx)
}

// A failure that execution reached before the deadline keeps its outcome when
// the cleanup that follows crosses the deadline.
func TestExecute_CleanupPastTheDeadlineKeepsTheOutcome(t *testing.T) {
	t.Parallel()

	storage := &recordingStorage{}
	runMeta := &types.RunMeta{RunID: "run-deadline", Attempt: 1}
	exec := newMockExecutor(makeValidEventStream(runMeta), 0)
	cfg := deadlineConfig(t, storage, exec, 100*time.Millisecond)
	cfg.Policy = &slowFlushPolicy{flushTrackingPolicy: newFlushTrackingPolicy(), delay: 300 * time.Millisecond}
	o, err := NewRunOrchestrator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	result, err := o.Execute(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome.Status != types.OutcomePolicyFailure {
		t.Fatalf("outcome = %s (%s), want %s", result.Outcome.Status, result.Outcome.Message, types.OutcomePolicyFailure)
	}
}
