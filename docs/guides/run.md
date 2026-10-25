# Runs, Jobs, and Attempts

A run is one execution of a script against one job. This guide explains
how runs relate to jobs and attempts, which lifecycle hooks a script can
export, how a run ends, and how fan-out starts child runs. The
[run contract](../contracts/CONTRACT_RUN.md) states the rules.

---

## Definitions

- **Job**: logical unit of work.
- **Run**: a single execution attempt of a job.
- **Attempt**: the number of a run among the runs of one job, starting at 1.
  Quarry does not retry a run. A caller that retries a job starts a new run
  with the next attempt number (`--attempt`) and the failed run as its
  parent (`--parent-run-id`).

---

## Lineage

A retry is a **new run** that names the failed run as its parent. The job
ID stays the same across retries.

---

## Lifecycle Hooks

Scripts may export **optional lifecycle hooks** alongside the default function.
Hooks allow scripts to intercept the run at well-defined points without
building external wrappers.

**Execution order (happy path):**

Hooks run in this order: `prepare` → acquire browser → `beforeRun` → `script()` → `afterRun` → `beforeTerminal` → auto-emit terminal → `cleanup`

**Error path:** replaces `afterRun` with `onError`. Other hooks run normally.

**Skip path:** `prepare` returns `{ action: 'skip' }` → emit `run_complete({skipped})` → done. No browser, no context, no other hooks.

See the [SDK and setup guide](../../PUBLIC_API.md) for the full hook reference with signatures and examples.

---

## Outcomes

Runs end in one of a few outcomes:
- success (normal completion)
- script error (emitted `run_error`)
- executor crash
- policy failure
- version mismatch (SDK/CLI contract version mismatch)

Each outcome is observable in runtime metadata.

---

## Child Runs (Fan-Out)

When `--depth > 0` is set, scripts can trigger **child runs** via
`emit.enqueue()`. Child runs differ from retry runs:

| | Retry Run | Child Run |
|-|-----------|-----------|
| Trigger | Failure of previous run | `emit.enqueue()` from parent |
| Script | Same as parent | Specified by `target` field |
| `attempt` | Incremented | Always 1 |
| `job_id` | Same as parent | New (derived from parent) |

Child runs are tracked internally by the fan-out operator. The root run's
exit code determines the overall outcome. Child run results appear in the
fan-out summary.
