# Temporal Integration Guide

This guide describes how Quarry integrates with [Temporal](https://temporal.io)
as an **orchestration integration**. Temporal wraps the Quarry runtime as
a Temporal activity — it does not use the `--adapter` notification pathway.

This guide is **non-normative**. The authoritative contract for orchestration
integration semantics is [`docs/contracts/CONTRACT_INTEGRATION.md`](../contracts/CONTRACT_INTEGRATION.md).

---

## Overview

| Aspect | Standalone CLI | Temporal Orchestration |
|--------|---------------|----------------------|
| Scheduling | cron / CI / shell | Temporal schedules + workflows |
| Retries | Manual (re-run command) | Workflow-level with lineage |
| Delivery guarantees | Best-effort (or at-least-once with outbox) | At-least-once (durable execution) |
| Downstream notification | `--adapter webhook` / `--adapter redis` | Separate workflow activity |
| Package | `quarry` CLI binary | `quarry-temporal/` worker binary |

---

## Architecture

```
Temporal Worker (quarry-temporal/)
  -> Workflow: QuarryExtractionWorkflow
    -> Activity: RunQuarryExtraction
      -> runtime.NewRunOrchestrator(config).Execute(ctx)
        -> [executor -> IPC -> policy -> Lode]
      <- outcome, run_id, duration
    <- Workflow decides: retry? notify? fan-out?
```

The activity is a thin in-process wrapper. It calls `NewRunOrchestrator`
directly — no subprocess, no CLI parsing, no shell invocation. The Quarry
runtime runs inside the Temporal worker process.

---

## Activity Wrapper

The activity wrapper is intentionally minimal. It translates between
Temporal's activity contract and Quarry's `RunOrchestrator` API.

```go
// Pseudocode — illustrative, not compilable.
func RunQuarryExtraction(ctx workflow.Context, params ExtractionParams) (*ExtractionResult, error) {
    activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
        StartToCloseTimeout: 4 * time.Hour,
        HeartbeatTimeout:    60 * time.Second,
    })

    var result ExtractionResult
    err := workflow.ExecuteActivity(activityCtx, runQuarryActivity, params).Get(ctx, &result)
    return &result, err
}

func runQuarryActivity(ctx context.Context, params ExtractionParams) (*ExtractionResult, error) {
    config := &runtime.RunConfig{
        ExecutorPath: params.ExecutorPath,
        ScriptPath:   params.ScriptPath,
        Job:          params.Job,
        RunMeta: &types.RunMeta{
            RunID:       params.RunID,
            JobID:       &params.JobID,
            ParentRunID: params.ParentRunID,
            Attempt:     params.Attempt,
        },
        // ... policy, proxy, source, category
    }

    orchestrator, err := runtime.NewRunOrchestrator(config)
    if err != nil {
        return nil, temporal.NewNonRetryableApplicationError("invalid config", "CONFIG_ERROR", err)
    }

    // Heartbeat goroutine — reports liveness to Temporal.
    done := make(chan struct{})
    go func() {
        ticker := time.NewTicker(30 * time.Second)
        defer ticker.Stop()
        for {
            select {
            case <-ticker.C:
                activity.RecordHeartbeat(ctx, "running")
            case <-done:
                return
            }
        }
    }()

    result, execErr := orchestrator.Execute(ctx)
    close(done)

    if execErr != nil {
        return nil, execErr
    }

    // Map outcome to Temporal error semantics.
    return mapOutcome(result)
}
```

Key design choices:

- **In-process** (Strategy A): the activity calls Go functions directly.
  No subprocess, no CLI parsing overhead, no serialization boundary.
- **Heartbeat goroutine**: runs concurrently with `Execute()`. Context
  cancellation from Temporal propagates through `ctx`, triggering executor
  kill and best-effort policy flush.
- **`MaximumAttempts: 1` on the activity**: retries happen at the workflow
  level so Quarry can record explicit lineage in Lode (each retry gets a
  new `run_id`, incremented `attempt`, and linked `parent_run_id`).

---

## Lineage Mapping

Quarry's lineage model maps to Temporal concepts:

| Quarry Field | Temporal Equivalent | Who Sets It |
|-------------|-------------------|-------------|
| `run_id` | (generated per execution) | Workflow |
| `job_id` | Workflow ID | Workflow |
| `attempt` | Workflow retry counter + 1 | Workflow |
| `parent_run_id` | Previous activity's `run_id` | Workflow |

The workflow generates a new `run_id` for each execution attempt and
threads `parent_run_id` through from the previous attempt's result.
This satisfies `RunMeta.Validate()` rules:

- `attempt == 1` → `parent_run_id` is nil (initial run).
- `attempt > 1` → `parent_run_id` is the previous execution's `run_id`.

Retries are workflow-level (not activity-level) so each Quarry run gets
its own `run_id` recorded in Lode, preserving full lineage.

---

## Heartbeat and Cancellation

### Recommended Timeouts

| Timeout | Value | Rationale |
|---------|-------|-----------|
| `StartToCloseTimeout` | 4 hours | Upper bound for longest extraction |
| `HeartbeatTimeout` | 60 seconds | Detect stuck activities quickly |
| `ScheduleToStartTimeout` | 5 minutes | Detect worker unavailability |

### Cancellation Behavior

When Temporal cancels an activity (heartbeat timeout, workflow cancellation,
or explicit termination):

1. `ctx.Done()` fires inside `RunOrchestrator.Execute()`.
2. Runtime kills the executor process (best-effort).
3. Policy flush runs (best-effort) to persist any buffered events.
4. Activity returns an error; Temporal marks the activity as failed.

The heartbeat goroutine should check `ctx.Err()` to stop cleanly.

---

## Error Mapping

The activity maps `OutcomeStatus` to Temporal error types per
[`CONTRACT_INTEGRATION.md`](../contracts/CONTRACT_INTEGRATION.md):

| OutcomeStatus | Temporal Behavior | Implementation |
|---------------|------------------|----------------|
| `success` | Activity completes normally | Return result, nil error |
| `script_error` | Retryable error | Return `temporal.NewApplicationError(...)` |
| `executor_crash` | Retryable error | Return `temporal.NewApplicationError(...)` |
| `policy_failure` | Non-retryable error | Return `temporal.NewNonRetryableApplicationError(...)` |

`policy_failure` is non-retryable because it indicates a configuration
problem (e.g., invalid policy settings) that retries cannot resolve.

---

## Data Flow

Temporal has payload size limits and serialization overhead. Quarry keeps
orchestrator payloads small:

| What | Where | Size |
|------|-------|------|
| Extraction config (script path, job, proxy, run meta) | Temporal activity input | ~1–5 KB |
| Result summary (outcome, run_id, duration, event count) | Temporal activity output | ~1 KB |
| Events, artifacts, metrics | Lode (direct write) | Unbounded |

Run data never flows through Temporal. The activity writes directly to
Lode during execution. Downstream consumers read from Lode, not from
Temporal's payload store.

---

## Batch Patterns

### Small Batches (<500 runs)

Execute extractions as parallel activities within a single workflow:

```
Workflow: BatchExtraction
  -> Activity: RunQuarryExtraction (run 1)
  -> Activity: RunQuarryExtraction (run 2)
  -> ...
  -> Activity: RunQuarryExtraction (run N)
  <- Aggregate results
```

### Large Batches (>500 runs)

Use child workflows to avoid Temporal history size limits:

```
Workflow: LargeBatchExtraction
  -> ChildWorkflow: BatchChunk (runs 1–500)
    -> Activity: RunQuarryExtraction (run 1)
    -> ...
  -> ChildWorkflow: BatchChunk (runs 501–1000)
    -> Activity: RunQuarryExtraction (run 501)
    -> ...
  <- Aggregate child results
```

Each child workflow has its own history, avoiding the ~50K event limit
per workflow execution.

---

## Outbox Pattern and Temporal

| Deployment | Notification Mechanism | Delivery Guarantee |
|-----------|----------------------|-------------------|
| Standalone CLI | `--adapter webhook` + outbox | Best-effort (or at-least-once with outbox) |
| Temporal | Separate workflow activity | At-least-once (durable execution) |

In a Temporal deployment, downstream notification becomes a separate
activity in the workflow — e.g., a webhook POST or SNS publish activity
that runs after the extraction activity completes. Temporal's durable
execution guarantees at-least-once delivery without the outbox pattern.

The outbox pattern (see [`docs/plans/IMPLEMENTATION_PLAN.md`](../plans/IMPLEMENTATION_PLAN.md)) remains relevant
for standalone CLI deployments where no external orchestrator provides
delivery guarantees.

---

## Module Structure

The Temporal integration requires a Go module split so `quarry-temporal/`
can depend on core runtime types without pulling in CLI/TUI dependencies.

| Module | Contains | Depends On |
|--------|----------|-----------|
| `quarry-core/` | types, runtime, policy, lode, adapter, proxy, metrics | (standalone) |
| `quarry-cli/` | CLI commands, TUI, config | `quarry-core/` |
| `quarry-temporal/` | activity, workflow, worker | `quarry-core/`, `go.temporal.io/sdk` |

See [`docs/plans/IMPLEMENTATION_PLAN.md`](../plans/IMPLEMENTATION_PLAN.md) for the module split roadmap and
sequencing.

---

## Related Documents

- [`docs/contracts/CONTRACT_INTEGRATION.md`](../contracts/CONTRACT_INTEGRATION.md) — normative orchestration integration semantics
- [`docs/contracts/CONTRACT_RUN.md`](../contracts/CONTRACT_RUN.md) — run lifecycle, lineage, and outcome rules
- [`docs/guides/integration.md`](integration.md) — downstream notification patterns (event-bus, polling)
- [`docs/plans/IMPLEMENTATION_PLAN.md`](../plans/IMPLEMENTATION_PLAN.md) — module split and Temporal roadmap
