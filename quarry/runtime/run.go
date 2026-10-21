package runtime

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/log"
	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/policy"
	"github.com/pithecene/quarry/types"
)

// Executor abstracts executor process lifecycle for testing.
type Executor interface {
	Start(ctx context.Context) error
	Stdout() io.Reader
	Stdin() io.WriteCloser
	Wait() (*ExecutorResult, error)
	Kill() error
}

// ExecutorFactory creates an Executor. Used for test injection.
type ExecutorFactory func(config *ExecutorConfig) Executor

// RunConfig configures a single run.
type RunConfig struct {
	// ExecutorPath is the path to the executor binary.
	ExecutorPath string
	// ScriptPath is the path to the script file.
	ScriptPath string
	// Job is the job payload.
	Job any
	// RunMeta is the run identity and lineage metadata.
	RunMeta *types.RunMeta
	// Proxy is the optional resolved proxy endpoint per CONTRACT_PROXY.md.
	// If nil, executor launches without a proxy.
	Proxy *types.ProxyEndpoint
	// Policy is the ingestion policy.
	Policy policy.Policy
	// ExecutorFactory overrides executor creation (for testing).
	// If nil, uses NewExecutorManager.
	ExecutorFactory ExecutorFactory
	// FileWriter handles sidecar file writes via Lode Store.
	// If nil, file_write frames are logged and ignored.
	FileWriter lode.FileWriter
	// EnqueueObserver is an optional callback for fan-out scheduling.
	// When set, called synchronously on each enqueue event before policy dispatch.
	EnqueueObserver EnqueueObserver
	// BrowserWSEndpoint is the optional WebSocket URL of an externally managed browser.
	// When set, the executor connects instead of launching a new Chromium instance.
	BrowserWSEndpoint string
	// ResolveFrom is the optional path to a node_modules directory used for
	// bare-specifier ESM resolution fallback in workspace/monorepo setups.
	ResolveFrom string
	// Source is the partition key for origin system/provider.
	Source string
	// Category is the partition key for logical data type (default: "default").
	Category string
	// StorageDataset is the Lode dataset ID (e.g. "quarry").
	// Used to construct StoragePartition for executor input.
	StorageDataset string
	// StorageDay is the partition day (YYYY-MM-DD UTC).
	// Used to construct StoragePartition for executor input.
	StorageDay string
	// Collector is the metrics collector for this run per CONTRACT_METRICS.md.
	// If nil, no metrics are recorded (all Collector methods are nil-safe).
	Collector *metrics.Collector
}

// RunResult represents the result of a run.
type RunResult struct {
	// RunMeta is the run identity and lineage.
	RunMeta *types.RunMeta
	// Outcome is the run outcome.
	Outcome *types.RunOutcome
	// Duration is the total run duration.
	Duration time.Duration
	// PolicyStats is the policy statistics.
	PolicyStats policy.Stats
	// ArtifactStats is the artifact accumulation statistics.
	ArtifactStats ArtifactStats
	// OrphanIDs is the list of orphaned artifact IDs.
	OrphanIDs []string
	// StderrOutput is the captured executor stderr.
	StderrOutput string
	// EventCount is the total number of events processed.
	EventCount int64
	// ProxyUsed is the proxy endpoint used (redacted, no password).
	// Nil if no proxy was configured.
	ProxyUsed *types.ProxyEndpointRedacted
	// TerminalSummary is the payload from the terminal event (run_complete or run_error).
	// Nil if no terminal event was received.
	TerminalSummary map[string]any
}

// RunOrchestrator orchestrates a single run.
type RunOrchestrator struct {
	config    *RunConfig
	logger    *log.Logger
	startTime time.Time
}

// NewRunOrchestrator creates a new run orchestrator.
// Returns error if run metadata is invalid.
func NewRunOrchestrator(config *RunConfig) (*RunOrchestrator, error) {
	// Validate run metadata per CONTRACT_RUN.md
	if err := config.RunMeta.Validate(); err != nil {
		return nil, fmt.Errorf("invalid run metadata: %w", err)
	}

	// Create logger with run context
	logger := log.NewLogger(config.RunMeta)

	return &RunOrchestrator{
		config: config,
		logger: logger,
	}, nil
}

// Execute executes the run end-to-end.
// This is the main entry point for run orchestration.
//
// Execution flow:
//  1. Start executor process
//  2. Run IPC ingestion loop (concurrent)
//  3. Wait for executor exit
//  4. Flush policy
//  5. Determine outcome
//  6. Return result
func (r *RunOrchestrator) Execute(ctx context.Context) (*RunResult, error) {
	r.startTime = time.Now()
	r.config.Collector.IncRunStarted()

	r.logger.Info("starting run", map[string]any{
		"script":   r.config.ScriptPath,
		"executor": r.config.ExecutorPath,
	})

	// Pre-run health gate: verify external browser is reachable before launching
	// the executor. Best-effort: warns on failure but does not block the run,
	// because the health probe assumes /json/version at the host root, which is
	// not guaranteed for tokenized or path-prefixed external browser services.
	if r.config.BrowserWSEndpoint != "" {
		if err := HealthCheckBrowser(r.config.BrowserWSEndpoint); err != nil {
			r.logger.Warn("external browser health check failed (continuing anyway)", map[string]any{
				"endpoint": r.config.BrowserWSEndpoint,
				"error":    err.Error(),
			})
		} else {
			r.logger.Info("external browser health check passed", map[string]any{
				"endpoint": r.config.BrowserWSEndpoint,
			})
		}
	}

	// Create executor
	execConfig := &ExecutorConfig{
		ExecutorPath:      r.config.ExecutorPath,
		ScriptPath:        r.config.ScriptPath,
		Job:               r.config.Job,
		RunMeta:           r.config.RunMeta,
		Proxy:             r.config.Proxy,
		BrowserWSEndpoint: r.config.BrowserWSEndpoint,
		ResolveFrom:       r.config.ResolveFrom,
	}

	// Attach storage partition metadata for SDK-side key computation
	if r.config.StorageDataset != "" && r.config.StorageDay != "" {
		execConfig.Storage = &StoragePartition{
			Dataset:  r.config.StorageDataset,
			Source:   r.config.Source,
			Category: r.config.Category,
			Day:      r.config.StorageDay,
			RunID:    r.config.RunMeta.RunID,
		}
	}

	var executor Executor
	if r.config.ExecutorFactory != nil {
		executor = r.config.ExecutorFactory(execConfig)
	} else {
		executor = NewExecutorManager(execConfig)
	}

	// Start executor
	if err := executor.Start(ctx); err != nil {
		r.config.Collector.IncExecutorLaunchFailure()
		r.logger.Error("failed to start executor", map[string]any{
			"error": err.Error(),
		})
		// Best-effort flush even on start failure for strict termination semantics
		flushCtx, flushCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		if flushErr := r.config.Policy.Flush(flushCtx); flushErr != nil {
			r.logger.Warn("policy flush failed (best effort)", map[string]any{
				"error": flushErr.Error(),
			})
		}
		flushCancel()
		return r.buildResult(&types.RunOutcome{
			Status:  types.OutcomeExecutorCrash,
			Message: fmt.Sprintf("failed to start executor: %v", err),
		}, "", nil, nil), nil
	}

	r.config.Collector.IncExecutorLaunchSuccess()

	// Create artifact manager
	artifacts := NewArtifactManager()

	// Create ingestion engine with ack writer for file_write_ack frames.
	// executor.Stdin() is kept open after metadata delivery for this purpose.
	ingestion := NewIngestionEngine(
		executor.Stdout(),
		r.config.Policy,
		artifacts,
		r.config.FileWriter,
		r.logger,
		r.config.RunMeta,
		r.config.Collector,
		r.config.EnqueueObserver,
		executor.Stdin(),
	)

	// Run ingestion in goroutine
	ingestionDone := make(chan error, 1)
	go func() {
		ingestionDone <- ingestion.Run(ctx)
	}()

	// Wait for ingestion to complete FIRST
	// IMPORTANT: We must wait for ingestion before calling executor.Wait() because
	// Go's exec.Cmd.Wait() closes StdoutPipe, which would cause ingestion reads to
	// fail with "file already closed" even if data is still in the pipe buffer.
	ingErr := <-ingestionDone

	// Close stdin after ingestion completes — signals EOF to executor's AckReader.
	// Errors are expected if the executor already exited.
	if err := executor.Stdin().Close(); err != nil {
		r.logger.Debug("stdin close after ingestion (expected if executor exited)", map[string]any{
			"error": err.Error(),
		})
	}

	// On ANY ingestion error (policy, stream, or canceled), kill executor immediately
	// This prevents the executor from continuing to emit after we've decided to terminate
	if ingErr != nil {
		r.logger.Warn("killing executor due to ingestion error", map[string]any{
			"error":     ingErr.Error(),
			"is_policy": IsPolicyError(ingErr),
		})
		_ = executor.Kill()
	}

	// NOW call Wait() to reap the child process
	// This is safe because ingestion has already read all data from the pipe
	execResult, execErr := executor.Wait()

	// Always attempt policy flush (best effort) on all termination paths
	// Per CONTRACT_POLICY.md: "Buffered events must be flushed on run_complete, run_error, runtime termination (best effort)"
	// Use WithoutCancel to preserve context values (tracing) while ignoring parent cancellation
	flushCtx, flushCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	flushErr := r.config.Policy.Flush(flushCtx)
	flushCancel()
	if flushErr != nil {
		r.logger.Warn("policy flush failed (best effort)", map[string]any{
			"error": flushErr.Error(),
		})
	}

	// Handle executor wait error
	if execErr != nil {
		r.logger.Error("executor wait failed", map[string]any{
			"error": execErr.Error(),
		})
		return r.buildResult(&types.RunOutcome{
			Status:  types.OutcomeExecutorCrash,
			Message: fmt.Sprintf("executor wait failed: %v", execErr),
		}, "", artifacts, ingestion), nil
	}

	// Handle ingestion errors
	if ingErr != nil {
		r.logger.Error("ingestion failed", map[string]any{
			"error":     ingErr.Error(),
			"exit_code": execResult.ExitCode,
		})

		// Classify the error
		var outcome *types.RunOutcome
		switch {
		case IsPolicyError(ingErr):
			outcome = &types.RunOutcome{
				Status:  types.OutcomePolicyFailure,
				Message: fmt.Sprintf("policy failure: %v", ingErr),
			}
		case IsVersionMismatchError(ingErr):
			outcome = &types.RunOutcome{
				Status:  types.OutcomeVersionMismatch,
				Message: fmt.Sprintf("SDK/CLI version mismatch: %v. Update the quarry CLI to match your SDK version, or pin the SDK to match your CLI.", ingErr),
			}
		case IsCanceledError(ingErr):
			outcome = &types.RunOutcome{
				Status:  types.OutcomeExecutorCrash,
				Message: fmt.Sprintf("run canceled: %v", ingErr),
			}
		default:
			// Stream/frame errors are executor crash
			outcome = &types.RunOutcome{
				Status:  types.OutcomeExecutorCrash,
				Message: fmt.Sprintf("stream error: %v", ingErr),
			}
		}

		return r.buildResult(outcome, string(execResult.StderrBytes), artifacts, ingestion), nil
	}

	// If flush failed and there were no other errors, report policy failure
	if flushErr != nil {
		return r.buildResult(&types.RunOutcome{
			Status:  types.OutcomePolicyFailure,
			Message: fmt.Sprintf("policy flush failed: %v", flushErr),
		}, string(execResult.StderrBytes), artifacts, ingestion), nil
	}

	// Determine outcome based on exit code and run_result frame.
	//
	// IMPORTANT: Exit codes are AUTHORITATIVE for outcome classification.
	// The run_result frame provides supplementary context (message, error_type, stack)
	// but does not override the exit code's determination of outcome category.
	//
	// Executor exit code mapping (see runtime/outcome.go):
	//   0 = success (run_complete emitted)
	//   1 = script_error (run_error emitted)
	//   2 = executor_crash
	//   3 = invalid_input (treated as executor_crash)
	var outcome *types.RunOutcome
	runResultFrame := ingestion.GetRunResult()

	// Exit code determines outcome category
	exitOutcome := outcomeFromExitCode(execResult.ExitCode)

	if runResultFrame != nil {
		// Check for exit code / run_result consistency
		runResultOutcome := runResultOutcomeToRunOutcome(runResultFrame)

		if exitOutcome != runResultOutcome.Status {
			r.logger.Warn("exit code conflicts with run_result", map[string]any{
				"exit_code":          execResult.ExitCode,
				"exit_outcome":       exitOutcome,
				"run_result_status":  runResultFrame.Outcome.Status,
				"run_result_outcome": runResultOutcome.Status,
			})
		}

		// Exit code is authoritative for outcome category
		// run_result provides supplementary context (message, error_type, stack)
		outcome = &types.RunOutcome{
			Status:    exitOutcome,
			Message:   runResultOutcome.Message,
			ErrorType: runResultOutcome.ErrorType,
			Stack:     runResultOutcome.Stack,
		}

		// If exit code says success but run_result has more specific failure info,
		// use run_result's message but keep exit code's status
		if exitOutcome == types.OutcomeSuccess && runResultOutcome.Status != types.OutcomeSuccess {
			outcome.Message = fmt.Sprintf("exit code 0 but run_result reported %s: %s",
				runResultFrame.Outcome.Status, runResultOutcome.Message)
		}

		r.logger.Info("run completed (from run_result)", map[string]any{
			"outcome":   outcome.Status,
			"exit_code": execResult.ExitCode,
			"duration":  time.Since(r.startTime).String(),
		})
	} else {
		// Fall back to exit code + terminal event analysis
		terminalEvent, hasTerminal := ingestion.GetTerminalEvent()
		outcome = DetermineOutcome(execResult.ExitCode, hasTerminal, terminalEvent)
		r.logger.Info("run completed", map[string]any{
			"outcome":      outcome.Status,
			"exit_code":    execResult.ExitCode,
			"duration":     time.Since(r.startTime).String(),
			"has_terminal": hasTerminal,
		})
	}

	return r.buildResult(outcome, string(execResult.StderrBytes), artifacts, ingestion), nil
}

// runResultOutcomeToRunOutcome converts a RunResultFrame to a RunOutcome.
func runResultOutcomeToRunOutcome(frame *types.RunResultFrame) *types.RunOutcome {
	var status types.OutcomeStatus
	switch frame.Outcome.Status {
	case types.RunResultStatusCompleted:
		status = types.OutcomeSuccess
	case types.RunResultStatusError:
		status = types.OutcomeScriptError
	case types.RunResultStatusCrash:
		status = types.OutcomeExecutorCrash
	default:
		status = types.OutcomeExecutorCrash
	}

	var message string
	if frame.Outcome.Message != nil {
		message = *frame.Outcome.Message
	} else {
		message = string(frame.Outcome.Status)
	}

	outcome := &types.RunOutcome{
		Status:    status,
		Message:   message,
		ErrorType: frame.Outcome.ErrorType,
		Stack:     frame.Outcome.Stack,
	}

	return outcome
}

// buildResult constructs the final run result.
func (r *RunOrchestrator) buildResult(
	outcome *types.RunOutcome,
	stderrOutput string,
	artifacts *ArtifactManager,
	ingestion *IngestionEngine,
) *RunResult {
	result := &RunResult{
		RunMeta:      r.config.RunMeta,
		Outcome:      outcome,
		Duration:     time.Since(r.startTime),
		PolicyStats:  r.config.Policy.Stats(),
		StderrOutput: stderrOutput,
	}

	// Set redacted proxy (per CONTRACT_PROXY.md: exclude password)
	// Prefer run_result.proxy_used if available, otherwise use config.Proxy
	if ingestion != nil {
		if runResult := ingestion.GetRunResult(); runResult != nil && runResult.ProxyUsed != nil {
			result.ProxyUsed = runResult.ProxyUsed
		}
	}
	if result.ProxyUsed == nil && r.config.Proxy != nil {
		redacted := r.config.Proxy.Redact()
		result.ProxyUsed = &redacted
	}

	if artifacts != nil {
		result.ArtifactStats = artifacts.Stats()
		result.OrphanIDs = artifacts.GetOrphanIDs()
	}

	if ingestion != nil {
		result.EventCount = ingestion.CurrentSeq()
		if termEvent, hasTerm := ingestion.GetTerminalEvent(); hasTerm {
			if termEvent.Payload != nil {
				result.TerminalSummary = termEvent.Payload
			} else {
				result.TerminalSummary = map[string]any{}
			}
		}
	}

	// Record run outcome metrics per CONTRACT_METRICS.md
	switch outcome.Status {
	case types.OutcomeSuccess:
		r.config.Collector.IncRunCompleted()
	case types.OutcomeScriptError, types.OutcomePolicyFailure, types.OutcomeVersionMismatch:
		r.config.Collector.IncRunFailed()
	case types.OutcomeExecutorCrash:
		r.config.Collector.IncRunCrashed()
	}

	// Absorb policy stats into the metrics collector
	ps := result.PolicyStats
	droppedByType := make(map[string]int64, len(ps.DroppedByType))
	for k, v := range ps.DroppedByType {
		droppedByType[string(k)] = v
	}
	r.config.Collector.AbsorbPolicyStats(ps.TotalEvents, ps.EventsPersisted, ps.EventsDropped, droppedByType, ps.FlushTriggers)

	return result
}
