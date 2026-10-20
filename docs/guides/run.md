# Runs, Jobs, and Attempts

This document explains Quarry’s run lifecycle at a user level.
The authoritative contract is [`docs/contracts/CONTRACT_RUN.md`](../contracts/CONTRACT_RUN.md).

---

## Definitions

- **Job**: logical unit of work.
- **Run**: a single execution attempt of a job.
- **Attempt**: a counter for retries of the same job.

---

## Lineage

Retries create **new runs** with a parent run reference. The original job
ID stays the same across retries.

---

## Lifecycle Hooks

Scripts may export **optional lifecycle hooks** alongside the default function.
Hooks allow scripts to intercept the run at well-defined points without
building external wrappers.

**Execution order (happy path):**

`prepare` → acquire browser → `beforeRun` → `script()` → `afterRun` → `beforeTerminal` → auto-emit terminal → `cleanup`

**Error path:** replaces `afterRun` with `onError`; other hooks run normally.

**Skip path:** `prepare` returns `{ action: 'skip' }` → emit `run_complete({skipped})` → done. No browser, no context, no other hooks.

See [`PUBLIC_API.md`](../../PUBLIC_API.md) for the full hook reference with signatures and examples.

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
exit code determines the overall outcome; child run results appear in the
fan-out summary.
