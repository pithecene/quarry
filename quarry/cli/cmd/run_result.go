package cmd

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/pithecene/quarry/iox"
	"github.com/pithecene/quarry/lode"
	"github.com/pithecene/quarry/metrics"
	"github.com/pithecene/quarry/runtime"
	"github.com/pithecene/quarry/types"
)

// runFinalizer handles post-execution concerns: adapter notification, the
// exit report, and result printing. Metrics and run records are persisted by
// Execute itself (CONTRACT_RUN.md §Run Finalization). Built once per run
// invocation and shared across fan-out and single-run paths.
type runFinalizer struct {
	collector      *metrics.Collector
	adapter        *adapterChoice
	storage        storageChoice
	storageDataset string
	source         string
	category       string
	policyChoice   policyChoice
	startTime      time.Time
	quiet          bool
	reportPath     string
}

// Finalize notifies the adapter, writes the report, and prints results.
// duration is computed from startTime internally.
//
// Note: run_completed events reach all configured event sinks (including Redis Streams)
// through the normal policy path — no separate terminal publish is needed.
func (f *runFinalizer) Finalize(ctx context.Context, result *runtime.RunResult) {
	duration := time.Since(f.startTime)
	if result.RunRecordError != "" {
		fmt.Fprintf(os.Stderr, "Warning: %s\n", result.RunRecordError)
	}
	f.notifyAdapter(ctx, result, duration)
	f.writeReport(result)
	f.printResults(result, duration)
}

// notifyAdapter publishes the completion event on a context that keeps ctx's
// values but not its cancellation, so a run that its deadline or a signal ended
// still reports that it completed. It publishes nothing for a run whose
// records are not confirmed in storage.
func (f *runFinalizer) notifyAdapter(ctx context.Context, result *runtime.RunResult, duration time.Duration) {
	if f.adapter == nil {
		return
	}
	if !result.Recorded {
		fmt.Fprintln(os.Stderr, "Warning: adapter notification skipped: the run's records are not confirmed in storage")
		return
	}
	adpt, err := buildAdapter(*f.adapter)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: adapter creation failed: %v\n", err)
		return
	}
	defer iox.DiscardClose(adpt)

	event := buildRunCompletedEvent(result, f.storage, f.storageDataset, f.source, f.category, lode.DeriveDay(f.startTime), duration)
	event.Locator.Storage, err = buildStorageLocator(f.storage, f.storageDataset)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: adapter notification skipped: %v\n", err)
		return
	}
	publishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), f.adapter.timeout)
	defer cancel()
	if err := adpt.Publish(publishCtx, event); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: adapter notification failed: %v\n", err)
	}
}

func (f *runFinalizer) writeReport(result *runtime.RunResult) {
	if f.reportPath == "" {
		return
	}
	exitCode := runExitCode(result)
	report := runtime.BuildRunReport(result, f.collector.Snapshot(), f.policyChoice.name, exitCode)
	if err := runtime.WriteRunReport(report, f.reportPath); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to write report: %v\n", err)
	}
}

func (f *runFinalizer) printResults(result *runtime.RunResult, duration time.Duration) {
	if f.quiet {
		return
	}
	printRunResult(result, f.policyChoice, duration)
	printMetrics(f.collector.Snapshot())
}

// runExitCode is the exit code of a run: its outcome's, or a storage error's when
// it succeeded and its records are not confirmed in storage.
func runExitCode(result *runtime.RunResult) int {
	if result.Outcome.Status == types.OutcomeSuccess && !result.Recorded {
		return exitStorageError
	}
	return outcomeToExitCode(result.Outcome.Status)
}

func outcomeToExitCode(status types.OutcomeStatus) int {
	switch status {
	case types.OutcomeSuccess:
		return exitSuccess
	case types.OutcomeScriptError:
		return exitScriptError
	case types.OutcomeExecutorCrash:
		return exitExecutorCrash
	case types.OutcomePolicyFailure:
		return exitPolicyFailure
	case types.OutcomeVersionMismatch:
		return exitPolicyFailure // non-retryable configuration error, same as policy_failure
	case types.OutcomeStorageError:
		return exitStorageError
	case types.OutcomeDeadlineExceeded:
		return exitDeadline
	default:
		return exitScriptError
	}
}

func printRunResult(result *runtime.RunResult, choice policyChoice, duration time.Duration) {
	fmt.Printf("\nrun_id=%s, attempt=%d, outcome=%s, duration=%s\n",
		result.RunMeta.RunID,
		result.RunMeta.Attempt,
		result.Outcome.Status,
		duration.Round(time.Millisecond),
	)

	switch choice.name {
	case "buffered":
		fmt.Printf("policy=%s, drops=%d, buffer_bytes=%d\n",
			choice.name,
			result.PolicyStats.EventsDropped,
			result.PolicyStats.BufferSize,
		)
	case "streaming":
		fmt.Printf("policy=%s, flush_count=%d, flush_interval=%s, flushes=%d\n",
			choice.name,
			choice.flushCount,
			choice.flushInterval,
			result.PolicyStats.FlushCount,
		)
	default:
		fmt.Printf("policy=%s\n", choice.name)
	}

	fmt.Printf("\n=== Run Result ===\n")
	fmt.Printf("Run ID:       %s\n", result.RunMeta.RunID)
	if result.RunMeta.JobID != nil {
		fmt.Printf("Job ID:       %s\n", *result.RunMeta.JobID)
	}
	if result.RunMeta.ParentRunID != nil {
		fmt.Printf("Parent Run:   %s\n", *result.RunMeta.ParentRunID)
	}
	fmt.Printf("Attempt:      %d\n", result.RunMeta.Attempt)
	fmt.Printf("Outcome:      %s\n", result.Outcome.Status)
	fmt.Printf("Message:      %s\n", result.Outcome.Message)
	fmt.Printf("Duration:     %s\n", result.Duration)
	fmt.Printf("Events:       %d\n", result.EventCount)

	if result.ProxyUsed != nil {
		fmt.Printf("\n=== Proxy ===\n")
		fmt.Printf("Protocol:     %s\n", result.ProxyUsed.Protocol)
		fmt.Printf("Host:         %s\n", result.ProxyUsed.Host)
		fmt.Printf("Port:         %d\n", result.ProxyUsed.Port)
		if result.ProxyUsed.Username != nil {
			fmt.Printf("Username:     %s\n", *result.ProxyUsed.Username)
		}
	}

	fmt.Printf("\n=== Policy Stats ===\n")
	fmt.Printf("Events Total:     %d\n", result.PolicyStats.TotalEvents)
	fmt.Printf("Events Persisted: %d\n", result.PolicyStats.EventsPersisted)
	fmt.Printf("Events Dropped:   %d\n", result.PolicyStats.EventsDropped)
	fmt.Printf("Flushes:          %d\n", result.PolicyStats.FlushCount)

	if result.ArtifactStats.TotalArtifacts > 0 {
		fmt.Printf("\n=== Artifact Stats ===\n")
		fmt.Printf("Total Artifacts:   %d\n", result.ArtifactStats.TotalArtifacts)
		fmt.Printf("Committed:         %d\n", result.ArtifactStats.CommittedArtifacts)
		fmt.Printf("Orphaned:          %d\n", result.ArtifactStats.OrphanedArtifacts)
		fmt.Printf("Total Ranges:      %d\n", result.ArtifactStats.TotalRanges)
		fmt.Printf("Total Bytes:       %d\n", result.ArtifactStats.TotalBytes)
	}

	if len(result.OrphanIDs) > 0 {
		fmt.Printf("\n=== Orphan Artifacts ===\n")
		for _, id := range result.OrphanIDs {
			fmt.Printf("  - %s\n", id)
		}
	}

	if result.StderrOutput != "" {
		fmt.Printf("\n=== Executor Stderr ===\n")
		fmt.Printf("%s", result.StderrOutput)
	}
}

// printMetrics prints the CONTRACT_METRICS.md metrics surface via CLI.
// Uses contract metric names for stable, machine-parseable output.
func printMetrics(snap metrics.Snapshot) {
	fmt.Printf("\n=== Metrics (CONTRACT_METRICS) ===\n")

	// Run lifecycle
	fmt.Printf("runs_started_total:              %d\n", snap.RunsStarted)
	fmt.Printf("runs_completed_total:            %d\n", snap.RunsCompleted)
	fmt.Printf("runs_failed_total:               %d\n", snap.RunsFailed)
	fmt.Printf("runs_crashed_total:              %d\n", snap.RunsCrashed)

	// Ingestion policy
	fmt.Printf("events_received_total:           %d\n", snap.EventsReceived)
	fmt.Printf("events_persisted_total:          %d\n", snap.EventsPersisted)
	fmt.Printf("events_dropped_total:            %d\n", snap.EventsDropped)
	// Deterministic output order for dropped-by-type breakdown
	droppedTypes := sortedKeys(snap.DroppedByType)
	for _, eventType := range droppedTypes {
		fmt.Printf("  events_dropped{type=%s}:      %d\n", eventType, snap.DroppedByType[eventType])
	}

	// Executor
	fmt.Printf("executor_launch_success_total:   %d\n", snap.ExecutorLaunchSuccess)
	fmt.Printf("executor_launch_failure_total:   %d\n", snap.ExecutorLaunchFailure)
	fmt.Printf("executor_crash_total:            %d\n", snap.ExecutorCrash)
	fmt.Printf("ipc_decode_errors_total:         %d\n", snap.IPCDecodeErrors)

	// Lode / Storage (per-call granularity)
	fmt.Printf("lode_write_success_total:        %d\n", snap.LodeWriteSuccess)
	fmt.Printf("lode_write_failure_total:        %d\n", snap.LodeWriteFailure)
	fmt.Printf("lode_write_retry_total:          %d\n", snap.LodeWriteRetry)
	fmt.Printf("artifact_index_write_failure_total: %d\n", snap.ArtifactIndexWriteFailure)

	// Dimensions
	fmt.Printf("\n  policy=%s executor=%s storage_backend=%s\n", snap.Policy, snap.Executor, snap.StorageBackend)
}

// sortedKeys returns map keys in sorted order for deterministic output.
func sortedKeys(m map[string]int64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
