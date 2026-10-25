# Quarry — Implementation Plan (Contract-First, Status-Tracked)

This plan reflects Quarry’s clarified architecture and introduces an explicit **contract-freezing phase**
to prevent drift between SDK, executor, runtime, ingestion policies, and Lode.
Completed items are checked off. Unchecked items are planned or unverified.
Status statements refer to released behavior where possible.

Quarry’s core principle:

> **Quarry defines the runtime boundary.  
> Ingestion Policy defines tradeoffs.  
> Lode defines structure.**

Scripts and executors remain **policy-agnostic**.

## Current Status (as of v0.14.0)
- Latest release: v0.14.0 (see CHANGELOG.md).
- Phases 0–5 complete. Phase 6 (dogfooding) in progress.
- All v1.0 readiness items resolved (see docs/RELEASE_READINESS_v1.0.md).
- Roadmap: v0.13.6 (Lode v0.10.0) → v0.14.0 (read side & retention) →
  v1.0 (contract freeze) → v1.x (first-party host). See the roadmap
  sections below; #245 is the umbrella issue, #239 and #240 the drivers.

---

## Conceptual Stack

```
User Script (Puppeteer, imperative)
        ↓
emit.*  (stable contract)
        ↓
Quarry Runtime (execution + observability)
        ↓
Ingestion Policy (configurable behavior)
        ↓
Lode (partitioned persistence substrate)
```

---

## Phase 0 — Foundations & Guardrails

### Goal
Establish development discipline before any architectural commitments harden.

### Deliverables
- Repo skeleton with internal modules:
  - `runtime/`
  - `executor-node/`
  - `sdk/`
  - `docs/`
  - `examples/`
- [`AGENTS.md`](../AGENTS.md) defining style and collaboration guardrails
- Taskfile scaffolding for lint / test / build

### Mini-milestones
- [x] Repo structure committed
- [x] Taskfile targets exist (even if stubbed)
- [x] Guardrails reviewed and accepted

---

## Phase 0.5 — Contract Definitions (NO CODE)

### Goal
Freeze **interfaces and invariants** required for parallel implementation,
without committing to internal implementations.

This phase produces **authoritative documents**, not code.

### Deliverables

#### 0.5.1 Emit Contract (`docs/contracts/CONTRACT_EMIT.md`)
Defines:
- Event envelope fields (e.g. `event_id`, `run_id`, `seq`, `type`, `payload`, `ts`)
- Initial event types:
  - `item`
  - `artifact`
  - `checkpoint`
  - `enqueue` (optional advisory; not required or guaranteed)
  - `rotate_proxy` (optional advisory; not required or guaranteed)
  - `log`
  - `run_error`
  - `run_complete`
- Ordering guarantees:
  - Total order **per run**
  - No reordering across event types
- Versioning rules:
  - Contract version in envelope
  - Backward-compatible additive changes only
- Error semantics:
  - Script abort vs executor crash vs policy failure

#### 0.5.2 IPC & Streaming Contract (`docs/contracts/CONTRACT_IPC.md`)
Defines:
- Framing format (length prefix + payload)
- Payload encoding (single choice, e.g. msgpack)
- Maximum frame size
- Artifact chunking rules:
  - chunk size
  - ordering
  - reassembly expectations
- Backpressure semantics:
  - blocking writes (emit blocks on backpressure)
  - no executor-side buffering
  - dropping is policy-layer only
- Failure behavior on partial frames or pipe closure

#### 0.5.3 Run Identity & Lineage (`docs/contracts/CONTRACT_RUN.md`)
Defines:
- Canonical `run_id`
- Relationship: job → run → retry run
- Correlation fields:
  - `job_id`
  - `parent_run_id`
  - `attempt`
- Idempotency expectations:
  - runs are append-only
  - deduplication is downstream responsibility

#### 0.5.4 Ingestion Policy Semantics (`docs/contracts/CONTRACT_POLICY.md`)
Defines:
- What constitutes a “drop”
- Which event types may be dropped
- What “buffered” means (bounded memory, flush guarantees)
- Required observability:
  - counters
  - logs
  - events
- Invariants:
  - no silent loss
  - policy does not alter event shapes

#### 0.5.5 Lode Expectations (`docs/contracts/CONTRACT_LODE.md`)
Defines Quarry’s **minimal expectations** of Lode:
- Required partition keys (source / category / day / run_id / event_type)
- Append-only semantics
- Policy-independent layout invariants
- Concrete meaning of “consistency across policies”
- Lineage/metadata surfaced; dedup elimination is downstream and out of scope

#### 0.5.6 Proxy Contract (`docs/contracts/CONTRACT_PROXY.md`)
Defines:
- Shared proxy data model (endpoint, pool, strategy, sticky scope)
- Runtime selection and rotation rules
- Executor application requirements (launch + auth)
- IPC payload fields and redaction rules
- Validation rules and observability expectations

#### 0.5.7 CLI Contract (`docs/contracts/CONTRACT_CLI.md`)
Defines:
- CLI invariants and command topology
- Read-only guarantees and non-goals
- Output format selection rules
- Request/response shapes for inspect/stats/list/debug

### Exit criteria
- [x] All five contract documents exist
- [x] SDK, executor, runtime, and policy work can proceed independently
- [x] Any ambiguity is resolved by pointing to a contract doc

---

## Phase 1 — TypeScript SDK (Script Contract)

### Goal
Define the stable authoring surface for extraction scripts.

### Deliverables
- `QuarryContext<Job>`
- `EmitAPI`
- Optional hooks
- Proxy config types + validation helpers
- Minimal SDK README with example

### Mini-milestones
- [x] Context exposes real Puppeteer objects
- [x] `emit.*` is the sole output mechanism
- [x] No ingestion or durability concepts leak into the SDK

---

## Phase 2 — Node Executor (Minimal Host)

### Goal
Execute scripts without distortion and stream events immediately.

### Deliverables
- Executor entrypoint (`stdio` mode)
- Script loader
- IPC implementation conforming to contracts
- Emit forwarding implementation
- Proxy application utilities (launch args + auth)

### Mini-milestones
- [x] Events streamed incrementally and in order
- [x] Large artifacts supported via chunking
- [x] Executor has zero knowledge of ingestion policy

---

## Phase 3 — Go Runtime Core

### Goal
Supervise execution and route events to an ingestion policy.

### Deliverables
- Executor lifecycle management
- Event ingestion loop
- Retry boundaries (retry = new run)
- Run metadata and logging
- Proxy selection + rotation state in runtime

### Mini-milestones
- [x] One job runs end-to-end
- [x] Executor crashes detected and recorded
- [x] Runtime delegates ingestion exclusively to policy layer

---

## Phase 4 — Ingestion Policy Layer

### Goal
Make tradeoffs explicit and swappable.

### Required policies

#### Strict Policy
- Synchronous writes
- Immediate backpressure
- No drops

#### Buffered Policy
- Bounded in-memory buffering
- Batched writes
- Explicit, observable drops allowed

### Mini-milestones
- [x] Common `IngestionPolicy` interface
- [x] Policies selectable per run
- [x] Drops and buffering visible in stats/logs

---

## Phase 5 — CLI & Configuration

### Goal
Expose policy selection without leaking complexity.

### Deliverables
- `quarry run`
- Read-only CLI surface (`inspect`, `stats`, `list`, `debug`, `version`)
- Output formatting layer (`json|table|yaml` with TTY defaults)
- Policy selection flags or config
- Clear run summaries

### Mini-milestones
- [x] Policy recorded in run metadata
- [x] Policy effects visible in output

---

## Phase 6 — Dogfooding (Two Postures)

> **Prerequisite**: v0.3.0 must be released before Phase 6 begins.
> Phase 6 is a post-release validation exercise — Quarry is used E2E
> on a real project, and feedback is captured as concrete follow-ups.

### Goal
Validate Quarry across different ETL philosophies.

### Mandatory runs
1. Precision-first ETL (strict policy)
2. Volume-first ETL (buffered/loss-tolerant policy)

### Mini-milestones
- [ ] Same script structure used in both
- [ ] Same emission contract used in both
- [ ] Only ingestion policy differs

---

## Exit Criteria

Quarry is ready to expand when:
- Contracts prevent cross-component drift
- Scripts are policy-agnostic
- Loss (if any) is intentional and observable
- Lode partitions are consistent across policies
- No escape hatches were required during dogfooding

---

## v0.3.0 Roadmap — ETL Ingress Hardening

### Goal
Treat Quarry as the first ingress of an ETL pipeline and harden the operational surface
before adding event bus integrations.

### Deliverables
- Lode upgrade to v0.4.1 with compatibility notes
- CLI/Stats/Metrics hardening for run visibility and ingestion effects
- v0.3.0 release readiness checklist and acceptance criteria
- Dogfooding prerequisites validated (CLI surface functional for real-project use)

### Mini-milestones
- [x] Lode dependency updated and validated against CONTRACT_LODE.md
- [x] CLI stats output includes policy effects and run outcome clarity
- [x] Metrics coverage for run lifecycle, ingestion drops, and executor failures
- [x] Dogfooding prerequisites met (Phase 6 can begin immediately post-release)

---

## v0.4.0 Roadmap — Storage Mechanics

### Goal
Expose sidecar file upload capabilities to scripts via `ctx.storage.put()`,
enabling direct writes to addressable storage paths outside the event pipeline.

### Deliverables
- `StorageAPI` on `QuarryContext` with `put()` method
- `file_write` IPC frame type for executor→runtime file transfer
- Content type persistence via companion `.meta.json`
- Terminal guard enforcement on `storage.put()`
- Contract updates (CONTRACT_IPC.md, CONTRACT_EMIT.md)

### Mini-milestones
- [x] SDK `StorageAPI` type and `storage.put()` implementation
- [x] `file_write` IPC frame in executor and runtime
- [x] Lode file writer for sidecar persistence
- [x] Terminal guard prevents writes after run completion
- [x] Contract and guide updates
- [x] Tests: SDK, executor IPC, runtime ingestion

---

## v0.5.0 Roadmap — Event Bus Adapters

### Goal
Provide runtime-integrated downstream notification so consumers do not need
to poll Lode or wire external plumbing.

### Deliverables
- `Adapter` interface and `RunCompletedEvent` type ([`quarry/adapter/`](../quarry/adapter/))
- Webhook adapter: HTTP POST with retries, custom headers, timeout ([`quarry/adapter/webhook/`](../quarry/adapter/webhook/))
- Redis pub/sub adapter: PUBLISH with retries, configurable channel ([`quarry/adapter/redis/`](../quarry/adapter/redis/))
- CLI flags: `--adapter`, `--adapter-url`, `--adapter-header`, `--adapter-channel`, `--adapter-timeout`, `--adapter-retries`
- Hook in `runAction()` after metrics persist (best-effort, does not fail run)
- Contract and guide updates

### Mini-milestones
- [x] Adapter interface and event type ([`quarry/adapter/adapter.go`](../quarry/adapter/adapter.go))
- [x] Webhook adapter implementation ([`quarry/adapter/webhook/webhook.go`](../quarry/adapter/webhook/webhook.go))
- [x] Webhook tests with httptest ([`quarry/adapter/webhook/webhook_test.go`](../quarry/adapter/webhook/webhook_test.go))
- [x] CLI flags and hook in [`quarry/cli/cmd/run.go`](../quarry/cli/cmd/run.go)
- [x] CONTRACT_INTEGRATION.md updated with runtime adapter reference
- [x] CONTRACT_CLI.md updated with adapter flags
- [x] Integration guide updated with webhook example
- [x] CLI_PARITY.json updated

### Redis Pub/Sub Adapter (v0.5.0)
- [x] Redis adapter implementation ([`quarry/adapter/redis/redis.go`](../quarry/adapter/redis/redis.go))
- [x] Redis adapter tests with miniredis ([`quarry/adapter/redis/redis_test.go`](../quarry/adapter/redis/redis_test.go))
- [x] `--adapter-channel` CLI flag
- [x] Config YAML `channel` field in adapter stanza
- [x] CLI wiring in `run.go` (import, flag, parse, build)
- [x] CLI_PARITY.json updated
- [x] CONTRACT_INTEGRATION.md and CONTRACT_CLI.md updated

### Future adapters (separate PRs)
- NATS
- SNS

### Future: At-Least-Once Delivery (Outbox Pattern)

v0.5.0 delivery is best-effort with retries. If all retries exhaust, the
notification is lost. For workloads that require at-least-once guarantees,
a Lode-backed outbox pattern can be added without breaking the existing
contract (strengthening from best-effort to at-least-once is additive).

#### Design sketch
1. Write a `notification_pending` record to Lode **before** attempting
   publish (durable intent).
2. Attempt publish with retries (existing webhook logic).
3. On success, mark the record `delivered`.
4. Undelivered records are retryable via a CLI command
   (`quarry adapter drain`) or detected by the next run.

Outbox records would live at:
```
datasets/<dataset>/partitions/source=<s>/category=<c>/day=<d>/run_id=<r>/event_type=adapter_outbox/
```

#### Relationship to external orchestrators

The outbox pattern applies only to standalone/CLI usage where no external
orchestrator provides delivery guarantees. When Quarry runs inside an
orchestrator (Temporal, Airflow, etc.), the outbox is redundant — the
orchestrator owns durable execution and downstream notification becomes
a separate step with the orchestrator's built-in retry semantics.

Standalone CLI usage remains a first-class paradigm. The outbox pattern
is relevant for users running Quarry directly (cron, CI, shell scripts)
without an external orchestrator.

See [`docs/guides/temporal.md`](guides/temporal.md) for the full Temporal integration design.

#### Open questions
- Retry ownership: CLI command, background process, or next-run piggyback?
- TTL for stale outbox entries (when to give up permanently).
- Whether `quarry adapter drain` should be a new CLI command or a
  subcommand of `quarry run`.

This is deferred until best-effort proves insufficient in standalone
production usage.

---

## v0.6.0 Roadmap — Derived Work Execution

### Goal

Enable `quarry run` to execute derived work discovered via `emit.enqueue()`,
turning a single CLI invocation into bounded fan-out — without introducing
new commands, modes, or abstractions.

### Design Principle

There is no "crawl mode." Fan-out is an **emergent runtime behavior** that
occurs when the user provides explicit fan-out flags. No new top-level
verbs, no umbrella "scope" or "budget" concepts. Only independent, literal
flags that state facts about execution bounds.

This preserves [`CONTRACT_CLI.md`](contracts/CONTRACT_CLI.md)'s invariant: `quarry run` is the only
execution entrypoint.

### Motivation

Discovery-driven extraction is the most common Quarry workload shape that
the current runtime cannot serve natively. A listing script emits `enqueue`
events for detail pages, but the runtime records them without acting — forcing
users to build external orchestrators that read enqueue records from storage,
loop, and spawn `quarry run` per target.

Fan-out flags close this loop inside the runtime:

```
quarry run --source list.ts --job '{"url":"..."}' --depth 2 --max-runs 500 --parallel 8
```

This fills the gap between single-target `quarry run` (zero infrastructure)
and external orchestrators (complex DAGs, heavy infrastructure):

```
Complexity of work
──────────────────────────────────────────────────────
Single target        Discovered fan-out      Complex DAG

quarry run           quarry run              External orchestrator
(no fan-out flags)   (with fan-out flags)    (Temporal, Airflow, etc.)
```

### Default Behavior (no fan-out flags)

```bash
quarry run --source list.ts --job '{"url":"..."}' ...
```

- Exactly one run executes.
- `emit.enqueue()` events are persisted as normal events.
- Enqueue events **never cause execution** of child runs.
- Process exits deterministically.

This is unchanged from current behavior. The `enqueue` event type remains
advisory per [`CONTRACT_EMIT.md`](contracts/CONTRACT_EMIT.md).

### Fan-Out Behavior (any fan-out flag present)

```bash
quarry run --source list.ts --job '{"url":"..."}' --depth 3 --max-runs 50000 --parallel 12
```

When any fan-out flag is provided:

- `emit.enqueue({ target, params })` events are **scheduled and executed**
  as child runs in addition to being persisted.
- Fan-out continues until the work queue is exhausted or bounds are reached.
- The process remains one-shot: it starts, exhausts, exits.

### Fan-Out Flags

| Flag | Meaning | Required for fan-out |
|------|---------|---------------------|
| `--depth N` | Maximum derivation depth from root run | Yes (at least one fan-out flag) |
| `--max-runs N` | Hard cap on total runs executed | No (unbounded if omitted) |
| `--parallel N` | Maximum concurrent child runs | No (default: 1) |

Flags are **bounds**, not requirements. A run with `--depth 3` that emits
zero enqueue events completes successfully with zero fan-out.

`--parallel` without any other fan-out flag is a no-op (single run has
nothing to parallelize).

### Depth Semantics

Depth is **derivation depth**, not same-script recursion depth:

- Root run starts at depth 0
- Each `enqueue` spawns a child at `depth + 1`
- `--depth N` means: do not execute runs where `depth > N`

This supports heterogeneous script chains naturally:

```
list.ts (depth 0) → detail.ts (depth 1) → asset.ts (depth 2)
```

### Script Chaining

Chaining is explicit in the `enqueue` payload. The `target` field identifies
the script to execute; `params` carries the job data:

```ts
// list.ts discovers detail pages
for (const url of discoveredUrls) {
  emit.enqueue({ target: "detail.ts", params: { url } })
}
```

The runtime resolves `target` and constructs a child run with
`--source <target>` and `--job <params>`.

### Deduplication

When fan-out is enabled, dedup is mandatory:

- Key: `(target, canonical_json(params))`
- Canonical JSON: sorted-key serialization (deterministic key ordering)
- Same `(target, params)` pair encountered again → silently skipped
- Dedup is per-invocation (no cross-invocation persistence)

### Scheduling Independence

The scheduler intercepts `enqueue` events **before** the ingestion
pipeline. The control path (what to execute next) is independent of
the data path (event persistence). If ingestion policy drops an enqueue
event from storage, the scheduler must still have received it.

This ensures fan-out behavior cannot be accidentally altered by policy
configuration.

### Exit Semantics

- Exit 0 if the root run succeeded and the process completed normally
- Non-zero only for infrastructure failures (executor crash, runtime error)
- Individual child run failures are recorded in metrics and a summary event
- Callers who need per-child outcome granularity use `quarry inspect`
- Summary record emitted: runs attempted, runs deduplicated, failures

### Bound Exhaustion

When `--max-runs` is reached mid-execution:

- Runs already in-flight complete normally
- Pending enqueue events are **not executed** but remain persisted
- The summary records how many enqueue events were not executed due to
  the cap

### Contract Impact

- CONTRACT_EMIT.md — additive: specify that when fan-out flags are
  present, the runtime **may treat enqueue as actionable** (schedule
  derived work). Default behavior (advisory) is unchanged.
- CONTRACT_RUN.md — additive: `parent_run_id` and `depth` threading
  for fan-out lineage.
- CONTRACT_CLI.md — additive: fan-out flags on `quarry run`. No new
  commands.

### Deliverables

- Fan-out flags on `quarry run`: `--depth`, `--max-runs`, `--parallel`
- In-process scheduler: bounded work queue, dedup store, concurrency executor
- Lineage fields: `parent_run_id`, `depth` on child run envelopes
- Summary output: runs attempted, deduplicated, failed, not-executed
- `quarry inspect` support for fan-out lineage trees
- Contract and guide updates

### Mini-milestones

- [x] Contract updates (CONTRACT_EMIT.md enqueue semantics, CONTRACT_RUN.md
      lineage fields, CONTRACT_CLI.md fan-out flags)
- [x] In-process scheduler: enqueue event interception, dedup, depth tracking
- [x] Child run spawning with concurrency limiter
- [x] `parent_run_id` and `depth` threading through run metadata
- [x] Fan-out flag wiring in [`quarry/cli/cmd/run.go`](../quarry/cli/cmd/run.go)
- [x] Summary output and exit code semantics
- [ ] `quarry inspect` fan-out lineage view
- [x] Tests: depth bounds, max-runs cap, concurrency, dedup, scheduling
      independence from ingestion policy

### Scope Boundary

Fan-out handles bounded, discovered work. It does **not** handle:
- Scheduling priority (depth-first vs breadth-first is implementation-chosen)
- Cross-invocation dedup (don't re-scrape URLs seen in previous runs)
- Per-target rate limiting beyond proxy rotation
- Resume of interrupted fan-out
- Conditional branching (if X then enqueue Y)

These belong in external orchestrators. If fan-out starts accumulating
these features, it has exceeded its scope.

### Open Questions

- **`target` resolution**: Should `target` be a file path (relative to
  CWD, relative to root script, or absolute) or a logical name resolved
  from `--config` YAML? Must be frozen before implementation.
- **`--parallel` default**: 1 (sequential) is safest but may surprise
  users who expect fan-out to be concurrent by default.
- **Proxy pool sharing**: When N child runs execute concurrently against
  the same proxy pool, recency tracking (v0.7.0) must be pool-aware
  across concurrent runs. The fan-out design must not preclude this.

### Design Context

See [`docs/ingress-models.md`](ingress-models.md) for the broader analysis of ingress models
and use case taxonomy that motivated this approach.

---

## v0.7.0 Roadmap — Streaming Policy & Advanced Proxy Rotation

### Goal
Add a `streaming` ingestion policy for crawl workloads and harden proxy
selection for production use.

### Streaming Policy

Add a third ingestion policy (`streaming`) that combines strict's no-drop
guarantee with buffered's batched write efficiency. Designed for long-running
crawl workloads where downstream consumers need near-real-time visibility
into emitted items without per-event R2 write overhead.

#### Motivation

With `policy=strict`, each event creates its own Lode snapshot (data file +
manifest = 2+ R2 PUTs). For a crawl emitting hundreds of items, this means
hundreds of small R2 round trips that the executor blocks on. The streaming
policy batches events and flushes on configurable triggers (count threshold,
time interval), reducing R2 PUTs by an order of magnitude while preserving
near-real-time downstream visibility.

#### Deliverables
- `StreamingPolicy` implementation in [`quarry/policy/`](../quarry/policy/)
- CLI flags: `--flush-count`, `--flush-interval` (valid only with `--policy=streaming`)
- Config YAML: `policy.flush_count`, `policy.flush_interval`
- Flush trigger loop in runtime run orchestration
- CONTRACT_POLICY.md streaming section (already committed)

#### Mini-milestones
- [x] Contract sketch (CONTRACT_POLICY.md streaming section)
- [x] `StreamingPolicy` struct with count + interval flush triggers
- [x] Runtime integration: flush trigger goroutine in `Execute()`
- [x] CLI flag wiring and validation (`--policy=streaming` requires triggers)
- [x] Config YAML support (`policy.flush_count`, `policy.flush_interval`)
- [x] Tests: count trigger, interval trigger, combined, ordering, artifact integrity
- [x] Performance benchmarks: strict, buffered, streaming contention analysis
- [ ] Guide and configuration doc updates

### Advanced Proxy Rotation

#### Goal
Harden proxy selection for production workloads that require recency-aware
rotation to reduce endpoint reuse and improve scraping reliability.

### Phase 1: In-Memory Recency Window

Add an opt-in `recency_window` pool option that maintains a ring buffer of
recently-used endpoint indices and excludes them from random selection.

#### Deliverables
- `RecencyWindow *int` field on `ProxyPool` (Go types + SDK types)
- Ring buffer in selector `poolState`, recency-aware `selectRandom`
- LRU fallback when window >= endpoint count (never blocks)
- Peek/commit semantics (peek does not advance ring)
- Validation: hard-reject if <= 0; soft-warn if set on non-random strategy
- Contract, guide, and SDK validation updates

#### Implementation scope
- [`quarry/types/proxy.go`](../quarry/types/proxy.go) — field, validation, warnings
- [`quarry/proxy/selector.go`](../quarry/proxy/selector.go) — ring buffer, modified `selectRandom`, stats
- [`sdk/src/types/proxy.ts`](../sdk/src/types/proxy.ts) — `recencyWindow` field
- [`sdk/src/proxy.ts`](../sdk/src/proxy.ts) — validation in `validateProxyPool`
- [`quarry/cli/reader/types.go`](../quarry/cli/reader/types.go) — `RecencyWindow`/`RecencyFill` in `ProxyRuntime`

#### Mini-milestones
- [x] Contract updated ([`CONTRACT_PROXY.md`](contracts/CONTRACT_PROXY.md) — recency semantics)
- [x] Guide updated ([`docs/guides/proxy.md`](guides/proxy.md) — user-facing docs)
- [x] Go types and validation ([`quarry/types/proxy.go`](../quarry/types/proxy.go))
- [x] Selector ring buffer and recency-aware random ([`quarry/proxy/selector.go`](../quarry/proxy/selector.go))
- [x] SDK types and validation ([`sdk/src/types/proxy.ts`](../sdk/src/types/proxy.ts), [`sdk/src/proxy.ts`](../sdk/src/proxy.ts))
- [x] Tests: type validation, selector avoidance, LRU fallback, peek semantics

### Phase 2: Pluggable Recency Backend (Future)

Replace in-memory ring buffer with a pluggable backend interface to enable
cross-run, cross-process proxy coordination.

- Interface: `RecencyStore` with `Mark(idx)` / `Exclude() []int` / `LRU() int`
- In-memory implementation (default, matches Phase 1)
- Redis implementation (atomic choose+mark via Lua script)
- Enables concurrent workers sharing proxy state

Phase 2 is deferred until Phase 1 is validated in production.

---

## v0.13.6 Roadmap — Lode v0.10.0

### Goal

Take Lode v0.10.0 as a dependency-only patch release. v0.10.0 adds the
listing primitives the v0.14.0 read side is built on; nothing in Quarry's
public surface changes.

### Deliverables

- `github.com/pithecene-io/lode` → v0.10.0 in [`quarry/go.mod`](../quarry/go.mod).
- Record the additive listing API in [`CONTRACT_LODE.md`](contracts/CONTRACT_LODE.md) as Quarry-visible
  behavior: `PartitionListOptions.Prefix`, `StartAfter` cursors, sorted
  `ListManifests`, `Store.List` string-prefix semantics, and the
  `ErrPartitionPrefixNotSupported` sentinel.
- Note any Lode retry-schedule fixes from its hardening pass in the
  changelog. `WithRetryObserver` semantics are contractual on the Lode side
  and do not move, so `lode_write_retry_total` counts are unaffected.

### Mini-milestones

- [x] Lode v0.10.0 tagged upstream
- [x] `go.mod` bump, tests green
- [x] [`CONTRACT_LODE.md`](contracts/CONTRACT_LODE.md) listing notes (§ Listing Order and Cursors)
- [x] Lockstep release: `task version:bump V=0.13.6`, [`CHANGELOG.md`](../CHANGELOG.md)
      promoted, single version-bump commit (AGENTS.md § Version Policy)

---

## v0.14.0 Roadmap — Read Side & Retention

### Goal

Give Quarry's own storage layout a reader that Quarry maintains, and a
retention command that expresses run outcomes in Quarry's terms. Both are
prerequisites for the v1.x host and stand on their own for batch
consumers (#240) and long-lived datasets (#239).

Quarry owns the Hive layout, the event envelope, and the meaning of a run's
terminal event. Today a downstream consumer must reimplement all three to
enumerate runs, which couples it to layout details Quarry may change.

**Status:** shipped in v0.14.0 (2026-10-24), except the items under
Carried Forward. This section keeps the decisions below as the operator
made them. Where the shipped design differs, Mini-milestones states the
difference, and the contracts are authoritative.

### Decisions

Decided with the operator; the reasoning, the code facts behind each, and
the measurements they are judged by are recorded on #245. Normative text
lands in the contract PRs under Mini-milestones; the API is specified and
reviewed there, against code, not here.

**A. The runtime owns a run's whole lifecycle.** `Execute` writes a start
record before any other write, runs, performs finalization (metrics
persistence and the end record) and returns only after it. Today metrics
are persisted by the CLI after `Execute` returns, so any host other than
the CLI gets no finalization at all; [`CONTRACT_INTEGRATION.md`](contracts/CONTRACT_INTEGRATION.md)'s "atomic
unit of work" becomes true for every host, and the end record is a real
quiescence barrier because nothing writes after it.

**B. Quarry-written objects have one builder and one home.**
B1 — the runtime alone computes storage keys, using Lode's Hive encoding.
Today `file_write_ack` carries only `type`, `write_id`, `ok`, `error`, and
the SDK fills `StoragePutResult.key` from its own `buildStorageKey`; so
B1 is a [`CONTRACT_IPC.md`](contracts/CONTRACT_IPC.md) change first — the ack gains the resolved key —
and only then is the SDK predictor removed, deleting the raw-vs-encoded
path divergence in both writers rather than fixing it twice. B2 — run
records live in a Quarry-owned namespace inside the dataset,
`datasets/<dataset>/runs/<source>/<category>/<day>/<run_id>/`
(`start.json`, `end.json`), outside Lode's partition tree and scoped by
`--storage-dataset` like everything else; Lode's sweep is contractually
blind to it. Every component is produced by the same B1 builder with
Lode's Hive value encoding — never raw concatenation — so a value such as
`a/b` is stored escaped, exactly as the partition tree stores it; `run`
rejects only what encoding cannot make safe (empty values and the
literal `.` / `..`), so a record can neither escape the namespace nor
create an ambiguous prefix. B3 — sidecars stay at
`…/run_id=<r>/files/`, where consumers already read them; `gc` knows the
legacy raw-interpolated prefix for runs that predate B1.

**C. Runs are found from run records, never from partitions.** Listing
enumerates the namespace prefix and keeps only `start.json` keys — exactly
one per run, so a page cut on that key list is a run boundary by
construction — and reads `end.json` per returned run, where a missing
`end.json` is a normal state (a live run before finalization, or a legacy
run with no terminal event) reported as "no terminal evidence", never a
read error; that is two objects
per run against the ~5 partitions per run the Hive tree holds, and
cross-source queries are a shorter prefix. Event reads address Lode
partitions by the ref stored in the record. `Store.List` guarantees
neither order nor continuation today, so the reader sorts the filtered
keys lexicographically before cutting a page — the cursor is the last
`start.json` key returned, and the same prefix always yields the same
order on every backend — until Lode adds storage-native continuation
(a Lode ask, not a prerequisite); the measurement below is what the
change is judged by. C1 — index order is `source/category/day/run_id`,
matching the layout and #240's query; a time-first secondary index is
added only when the v1.x host has evidence it needs one. C2 — the ~11k
runs that predate records are backfilled by an explicit, operator-run,
idempotent upgrade step (`quarry index rebuild`, dry-run first) that
writes records carrying only what storage proves, all marked
`backfilled: true`: a start record for every legacy run (`day`, the run's
partition prefix `source=…/category=…/day=…/run_id=…/` under which every
`event_type` partition sits, null `started_at`), and an end record for
every legacy run
that has a terminal event (`ended_at` from that event's `ts`, terminal
type, null `outcome` — storage cannot prove the runtime's classification).
A backfilled end record is a valid retention barrier under D: its run is
provably over. A legacy run with no terminal event gets no end record and
no deadline, so it is reclaimable only through the operator's
`--presume-dead-after` assertion, measured from the end of its `day`.
The reader never falls back to partition enumeration.

**D. Retention selects from run records.** The end record is the
deletion barrier; Lode expires the snapshots (refusing when the run owns
the dataset head — `gc` resolves the pointer, excludes that run, reports
it skipped); Quarry deletes its own namespace and sidecar objects; every
failure is reported, nothing partial is silent. D1 — a run may declare a
maximum duration, enforced by the runtime (kill, `run_error`, end record);
there is no default, and an unterminated run without one is never
presumed dead unless the operator asserts a bound explicitly with
`gc --presume-dead-after <duration>`. D2 — the "ingested" acknowledgement
is a consumer-supplied list (`--runs-from`); Quarry holds no ack state.
D3 — deletion writes a tombstone first and clears it last, so a crashed
`gc` leaves a state the next run resumes from and `list runs` can flag.

**E. [`CONTRACT_CLI.md`](contracts/CONTRACT_CLI.md)'s read side is rewritten from this data model**, as
a versioned breaking change: partition selectors become required,
`started_at` nullable, the `running` state is retired in favour of
`unterminated` ("no terminal evidence" — a live run, a killed process, a
host crash, and a legacy run without a terminal event are all this state,
and nothing in storage distinguishes them until an enforceable deadline
or a liveness signal exists, so no listing or statistic may present it as
running), pagination and error semantics are specified, and the stub-backed views
with no data model (jobs, tasks, executors) are removed rather than
reconciled.

**F. This roadmap carries decisions; the design lives elsewhere.** The
design record is #245 until the v0.14.0 contract PRs supersede it.
Command and flag names used above (`quarry index rebuild`,
`--presume-dead-after`, `--runs-from`, `--storage-compression`) are
provisional labels for the decisions, not API; the contract PRs fix the
names and shapes.

**Compression** is a property of the dataset, not of an invocation. Lode
v0.12.0 enforces this itself: each manifest records its compressor, a
read decompresses each snapshot with the compressor its manifest names,
and a writer whose compressor differs from the head manifest's is
refused with `ErrSettingsMismatch` before it writes anything. A
Quarry-side pin (`datasets/<dataset>/quarry.json`) is therefore
unnecessary and is dropped. Today every Quarry writer opens a dataset
without a compressor, and no option selects one. The planned change,
carried forward, has every writer, `run` and `prune` alike, open a
dataset that has snapshots with the compressor its head manifest
records, since Lode refuses any other. A compressor option will choose
the compressor only for a dataset without snapshots, and `run` will
refuse a value that differs from an existing head's. The option will
ship with the benchmark below.
Recompressing an existing dataset waits on a Lode rewrite primitive. No
lockstep or contract-version impact.

**Known constraints, recorded not hidden:** a Lode dataset is one linear
history with one `latest` pointer (commit rate is bounded per dataset;
relief is dataset partitioning and burst smoothing in the host); Lode
v0.10.0 listing applies its cursor after enumerating the prefix, and
`Store.List` has no continuation, so all paging is client-side until Lode
adds storage-native continuation; the sidecar writers' raw interpolation
(fixed by B1).

### Measurements

Each decision is judged by a number in its PR, not by argument:

- B1: key builders 2 → 1; the `--source 'a/b'` reproduction (events and
  sidecars under different prefixes) goes from failing to passing.
- B2/C: store calls per `list runs` page on the dogfood dataset
  (~11k runs): partition enumeration vs. record listing, in
  [`docs/guides/benchmarks.md`](guides/benchmarks.md).
- A/D: zero runs without an end record among runs that completed
  finalization; `quarry prune --dry-run` candidate counts by outcome on the dogfood
  dataset before any deletion ships.
- Compression: write throughput, per-event latency under `strict`, and
  bytes stored for none/gzip/zstd, same benchmark harness.

### Prerequisites

- [x] Lode v0.10.0 (listing), taken in v0.13.6
- [x] pithecene/lode#174 (deletion primitive) and Lode's retention
      contract out of Draft, in Lode v0.12.0 (`Expire`, `Sweep`)
- [x] Lode: decompressor dispatch on `manifest.Compressor`, in Lode v0.12.0
- [ ] Lode: snapshot rewrite primitive, which blocks `--recompress` only

### Mini-milestones

- [x] [`CONTRACT_RUN.md`](contracts/CONTRACT_RUN.md) / [`CONTRACT_INTEGRATION.md`](contracts/CONTRACT_INTEGRATION.md): runtime-owned
      finalization, and start and end records (A). The run deadline (D1)
      is carried forward.
- [x] [`CONTRACT_LODE.md`](contracts/CONTRACT_LODE.md): the `runs/` namespace with Hive-encoded
      components, the unchanged sidecar inventory, whole-run deletion,
      Quarry-side deletion of its own objects, and head-owner exclusion
      (B2, B3, D). Sidecar keys remain unencoded (§ Key Encoding, known
      limitation), so the single key builder (B1) is carried forward.
- [x] [`CONTRACT_CLI.md`](contracts/CONTRACT_CLI.md): `list runs`, `list events`, `index rebuild`, and
      `prune` specified from the data model, with `unterminated` in place
      of `running` (E). Removing the stub-backed views and aligning
      [`CONTRACT_METRICS.md`](contracts/CONTRACT_METRICS.md) § Data Source Progression are carried forward.
- [x] Runtime: finalization moved into `Execute`, which writes the start
      and end records (A)
- [x] `quarry/reader` over run records, which the CLI commands use, with
      its [`ARCH_INDEX.md`](ARCH_INDEX.md) boundary entry (C). Module Split records its
      placement in `quarry-core/`, and that extraction remains planned
      there. `cli/reader` remains as it was, not an adapter over
      `quarry/reader`.
- [x] `quarry index rebuild`, the operator-run backfill, dry run first (C2)
- [x] `sdk/` reader module over CLI JSON, exported from the public entrypoint
      (`listRuns`, `readRun`)
- [x] Retention shipped as `quarry prune` rather than `gc`, with
      `--dry-run`, selection by state and age (`--succeeded-days`,
      `--failed-days`, `--unterminated-days`) or by `--run-id`, and
      `--sweep-grace` (D). The
      operator's bound on unterminated runs is `--unterminated-days` in
      place of `--presume-dead-after`. Tombstones (D3) were replaced by
      idempotent steps: an interrupted prune resumes on the next prune
      that selects the run.
- [x] [`PUBLIC_API.md`](../PUBLIC_API.md) Known Limitation 7 updated
- [x] `list runs` benchmark on a 200-run fixture published in
      [`docs/guides/benchmarks.md`](guides/benchmarks.md). The dogfood measurements are carried
      forward.

### Carried Forward

Not shipped in v0.14.0. The items that change a run contract outside the
experimental artifact surface (v1.0 § Experimental Artifact Surface) must
land before the v1.0 freeze.

- [ ] [`CONTRACT_IPC.md`](contracts/CONTRACT_IPC.md): `file_write_ack` carries the resolved storage
      key, the runtime alone builds keys with Hive encoding for sidecar
      keys, and SDK `buildStorageKey` is removed (B1). This is part of the
      experimental artifact surface (v1.0 § Experimental Artifact
      Surface), so it does not gate the freeze.
- [ ] Measurements on the dogfood dataset (about 11k runs): store calls
      per `list runs` page (B2/C) and `quarry prune --dry-run` candidate
      counts by outcome (A/D)
- [ ] Run deadline declared per run and enforced by the runtime (D1)
- [ ] Consumer-supplied run list for retention (`--runs-from`, D2)
- [ ] [`CONTRACT_CLI.md`](contracts/CONTRACT_CLI.md): stub-backed views with no data model (jobs,
      tasks, executors) removed, and [`CONTRACT_METRICS.md`](contracts/CONTRACT_METRICS.md) § Data
      Source Progression aligned (E)
- [ ] Writers that open a dataset with its head's compressor, a
      compressor option for datasets without snapshots, and the
      none/gzip/zstd benchmark, with `--recompress` once the Lode
      primitive exists

### Resolved Questions

- The `sdk/` reader runs `quarry` from `PATH` by default and accepts an
  explicit path (`quarryPath`).
- The event reader returns events only. Artifact bytes remain Known
  Limitation 3. Their replacement streams them into Lode Volumes and is
  designed on #255 (v1.0 § Experimental Artifact Surface).

---

## v1.0 — Contract Freeze

### Goal

Freeze the run contracts (`CONTRACT_RUN`, `CONTRACT_EMIT`, `CONTRACT_IPC`,
`CONTRACT_LODE`, `CONTRACT_POLICY`) and the SDK surface. v1.0 adds nothing;
it is the point after which the v1.x host is built *on* the contracts
rather than *in* them.

### Experimental Artifact Surface

**Decision:** the artifact byte path is experimental by default
and excluded from the v1.0 compatibility commitment, as Lode v1.0
excludes Volume (Lode `V1_API_DECISIONS.md` D9).
[`PUBLIC_API.md`](../PUBLIC_API.md) § Experimental Surface defines the surface.

The main use case, Panthalassa, acquires raw provider files that the
current path cannot carry, and the replacement, designed on #255, depends
on measurements from Panthalassa's source samples. Experimental status is
not a target: if the replacement has run against those samples, with
tests and usage sufficient for the freeze, before v1.0 is tagged, v1.0
freezes it with the other contracts. Lode's side follows its own gate.

### Deliverables

- [`docs/RELEASE_READINESS_v1.0.md`](RELEASE_READINESS_v1.0.md) taken from Draft to signed off: the
  v1.0 blockers (§1–§5) are resolved; the Release Gates checklist and the
  Sign-Off table are still open and are completed as part of this
  milestone, not assumed.
- [`PUBLIC_API.md`](../PUBLIC_API.md) Known Limitations rewritten as decisions: the read side
  limitation is retired by the v0.14.0 reader (shipped before v1.0); the
  scheduling limitation stays in force, restated as "planned: the v1.x
  first-party host", and is retired only when `quarry serve` ships.
- `.grove.yaml` intent revised deliberately if the host changes the
  project's stated shape; not by drift.

### Mini-milestones

- [x] v0.14.0 shipped
- [ ] v0.14.0 Carried Forward items that change a run contract outside
      the experimental artifact surface landed
- [x] Experimental artifact surface marked in [`PUBLIC_API.md`](../PUBLIC_API.md) and the contracts
- [ ] Readiness sign-off
- [ ] Known Limitations and `.grove.yaml` revised
- [ ] v1.0.0 tagged

---

## v1.x Roadmap — First-Party Host (`quarry serve`)

### Goal

Provide the machinery users expect around a crawler — job definitions,
cadence, visibility, retry, retention — as a first-party host of the
existing runtime, without changing what a run is.

Ingestion cadence ranges from sparse, to always-on, to multi-hour bursts
followed by idleness. The host is a scheduler and bounded executor pool
that is allowed to idle, not a service that must be up.

### Position

- The runtime is already an embeddable library: the seam is
  `runtime.NewRunOrchestrator(config)`, which returns
  `(*RunOrchestrator, error)`, followed by `Execute(ctx)` on the result,
  and [`CONTRACT_INTEGRATION.md`](contracts/CONTRACT_INTEGRATION.md) § Orchestration Integration Semantics
  already defines its boundary. `quarry serve` is one host of that seam; a
  Temporal worker is another. Quarry must not become specific to either.
- Lode remains the data plane and the system of record for everything a
  run produces. The host never duplicates it.
- Known scale constraint: a Lode dataset is one linear history with a
  single `latest` pointer, so every snapshot commit in a dataset
  serialises on one CAS round trip (~50–100 ms on S3/R2, i.e. on the
  order of 10–20 commits/s per dataset before `WithRetryCount` exhausts).
  Lode will not shard the pointer — per-partition histories are
  per-partition datasets by design — so the relief is Quarry's: the host
  partitions writers across dataset IDs (per crawl family or shard),
  smooths scheduled bursts instead of firing N runs at :00, and sizes its
  pool against the per-dataset ceiling. Quarry will ask Lode to document
  the contention bound in its write-API contract.
- Single-operator trust model, same as the CLI. Multi-user, tenancy, and
  auth are out of scope; the HTTP surface binds to localhost or sits behind
  the operator's own proxy.
- The CLI remains the full surface; `serve` composes it.

### Control-Plane State

Lode is append-only and immutable — the right home for runs, the wrong
home for mutable job state (paused, next fire, concurrency leases).

1. Declarative first: jobs and schedules are a file alongside the existing
   config YAML; all run state is derived from Lode through the v0.14.0
   reader. No new persistence. Pause/resume is an edit.
2. Embedded store (single file, e.g. SQLite or bbolt) only when leases or
   acknowledgements are needed, holding only what Lode cannot, and
   rebuildable from Lode plus the job file.

The store must never hold anything Lode already has.

### Deliverables

- `quarry serve`: job file, scheduler, bounded pool, HTTP read API over the
  v0.14.0 reader, scheduled `gc`.
- `CONTRACT_JOBS.md` (job definition and lifecycle) and `CONTRACT_SERVE.md`
  (HTTP read surface, process lifecycle).
- Run actions with lineage: re-run / retry a run as a new attempt per
  [`CONTRACT_INTEGRATION.md`](contracts/CONTRACT_INTEGRATION.md) § Lineage Preservation.
- Proxy pool health over time. `inspect proxy` exposes selection state
  only (round-robin index, sticky entries, recency, last-used) and
  [`CONTRACT_PROXY.md`](contracts/CONTRACT_PROXY.md) excludes health checking, so this needs new
  per-endpoint success/failure instrumentation and persistence — a
  [`CONTRACT_PROXY.md`](contracts/CONTRACT_PROXY.md) change, scoped when the host exists.
- A GUI, if built, is a consumer of the HTTP read API in its own package
  or repository — never a second reader.

### Mini-milestones

- [ ] Job definition format decided (extend config YAML vs. separate file)
- [ ] [`CONTRACT_CLI.md`](contracts/CONTRACT_CLI.md) revised: `serve` leaves the forbidden list and is
      defined as the one long-lived, work-executing command besides `run`
- [ ] `CONTRACT_JOBS.md` / `CONTRACT_SERVE.md` drafted
- [ ] `quarry serve` with scheduler and pool (declarative state only)
- [ ] HTTP read API
- [ ] Run retry action with lineage
- [ ] Embedded store, if and when leases/acks are needed
- [ ] GUI as a separate consumer

### Open Questions

- Job definition: extend the config YAML schema, or a separate jobs file.
- HTTP surface in the first cut: read-only, or also job control
  (pause/trigger).
- Whether the module split is a prerequisite for the host or follows it.

---

## Module Split — Multi-Module Restructure

### Goal

Split the `quarry/` Go module into independent modules so that hosts
(`quarry serve`, `quarry-temporal/`, future integrations) can depend on
core runtime types without pulling in CLI, TUI, or unrelated dependencies.

### Agreed Layout

| Module | Go Module Path | Contains |
|--------|---------------|----------|
| `quarry-core/` | `github.com/pithecene/quarry-core` | types, runtime, policy, lode, reader (v0.14.0 layout owner), adapter, proxy, metrics, log |
| `quarry-cli/` | `github.com/pithecene/quarry-cli` | CLI commands, TUI, config, `cli/reader` (response-shaping adapter over core `reader`) |
| `quarry-temporal/` | `github.com/pithecene/quarry-temporal` | activity, workflow, worker binary |
| `quarry-sdk/` | (npm: `@aspect/quarry-sdk`) | TypeScript SDK (rename from `sdk/`) |
| `quarry-executor/` | (npm: `@aspect/quarry-executor`) | Node executor (rename from `executor-node/`) |

### Dependency Graph

```
quarry-cli/      -> quarry-core/
quarry-temporal/  -> quarry-core/, go.temporal.io/sdk
quarry-core/      (no quarry-* dependencies)
```

### Prerequisites

- [x] Decouple lode from cli/reader dependency (PR #113)
- [ ] Zero cli/tui imports from core packages

### Sequencing

1. Extract `quarry-core/` — move types, runtime, policy, lode, reader,
   adapter, proxy, metrics, log into standalone module.
2. Extract `quarry-cli/` — CLI, TUI, config, and the `cli/reader`
   adapter depend on `quarry-core/` (which owns the layout `reader`).
3. Create `quarry-temporal/` — new module depending on `quarry-core/`
   and `go.temporal.io/sdk`.
4. Rename `sdk/` → `quarry-sdk/`, `executor-node/` → `quarry-executor/`
   — update package names and CI references.
5. Update CI — build matrix, release workflows, go.work for local dev.

### Mini-milestones

- [ ] Audit: identify all cross-package imports that block extraction
- [ ] Extract `quarry-core/` module with passing tests
- [ ] Extract `quarry-cli/` module with passing tests
- [ ] Create `quarry-temporal/` module (empty scaffold)
- [ ] Rename SDK and executor packages
- [ ] CI builds all modules independently

---

## Temporal Orchestration Integration (Deferred Host)

> Temporal is not being dogfooded. It is one host of the runtime seam
> defined in [`CONTRACT_INTEGRATION.md`](contracts/CONTRACT_INTEGRATION.md), alongside the first-party
> `quarry serve` host in the v1.x roadmap. Nothing in Quarry may become
> Temporal-specific; this section is built when a Temporal deployment
> exists to validate it against.

### Goal

Enable Quarry to run as a Temporal activity, with workflow-level retries,
lineage tracking, and durable execution guarantees.

### Prerequisites

- [ ] A Temporal deployment to dogfood against
- [ ] Module split complete (`quarry-core/` extracted)
- [ ] `quarry-temporal/` module created

### Deliverables

- Activity wrapper: thin in-process wrapper calling
  `runtime.NewRunOrchestrator(config).Execute(ctx)`
- Heartbeat goroutine: concurrent liveness reporting (30s interval)
- Outcome mapping: `OutcomeStatus` → Temporal error semantics per
  [`CONTRACT_INTEGRATION.md`](contracts/CONTRACT_INTEGRATION.md)
- Reference workflow: `QuarryExtractionWorkflow` with lineage-aware
  retry logic (new `run_id` per attempt, `parent_run_id` threading)
- Worker binary: standalone Temporal worker for `quarry-temporal/`
- Batch patterns: parallel activities for small batches, child workflows
  for large batches (>500 runs)

### Mini-milestones

- [ ] Activity wrapper with outcome mapping
- [ ] Heartbeat goroutine with cancellation propagation
- [ ] Reference workflow with lineage-aware retries
- [ ] Worker binary and registration
- [ ] Batch workflow patterns (parallel + child workflow)
- [ ] Integration tests with Temporal test framework

See [`docs/guides/temporal.md`](guides/temporal.md) for detailed design and pseudocode.

---

## Additional Event Bus Adapters (Staggered)

Order of support:
- ~~Redis Pub/Sub~~ (shipped in v0.5.0)
- NATS
- SNS/SQS

> Temporal is an orchestration integration, not a notification adapter.
> See [Temporal Orchestration Integration (Deferred Host)](#temporal-orchestration-integration-deferred-host)
> and [`docs/guides/temporal.md`](guides/temporal.md).

Principles:
- Integrations must not change contracts.
- Event envelope remains stable across adapters.
- CLI/Stats/Metrics hardening is a prerequisite for each new adapter.

---

## Contract Change Protocol

Any change to SDK, executor, runtime, or policy that affects contract behavior
must follow this protocol.

### Pre-merge checklist

**For SDK envelope/event type changes:**
- [ ] Update [`docs/contracts/CONTRACT_EMIT.md`](contracts/CONTRACT_EMIT.md)
- [ ] Update impacted contract docs (if any)
- [ ] Verify SDK types match contract definitions

**For IPC/streaming changes:**
- [ ] Update [`docs/contracts/CONTRACT_IPC.md`](contracts/CONTRACT_IPC.md)
- [ ] Verify executor and runtime implementations align

**For run identity/lineage changes:**
- [ ] Update [`docs/contracts/CONTRACT_RUN.md`](contracts/CONTRACT_RUN.md)

### PR requirements

PRs that modify contract behavior must:
1. Reference the specific contract section changed.
2. Include a **compatibility note** (breaking vs additive).
3. Update `contract_version` if the change is breaking.
