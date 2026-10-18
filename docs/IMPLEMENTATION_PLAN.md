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

## Current Status (as of v0.13.5)
- Latest release: v0.13.5 (see CHANGELOG.md).
- Phases 0–5 complete. Phase 6 (dogfooding) in progress.
- All v1.0 readiness items resolved (see docs/RELEASE_READINESS_v1.0.md).

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
- `AGENTS.md` defining style and collaboration guardrails
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
- `Adapter` interface and `RunCompletedEvent` type (`quarry/adapter/`)
- Webhook adapter: HTTP POST with retries, custom headers, timeout (`quarry/adapter/webhook/`)
- Redis pub/sub adapter: PUBLISH with retries, configurable channel (`quarry/adapter/redis/`)
- CLI flags: `--adapter`, `--adapter-url`, `--adapter-header`, `--adapter-channel`, `--adapter-timeout`, `--adapter-retries`
- Hook in `runAction()` after metrics persist (best-effort, does not fail run)
- Contract and guide updates

### Mini-milestones
- [x] Adapter interface and event type (`quarry/adapter/adapter.go`)
- [x] Webhook adapter implementation (`quarry/adapter/webhook/webhook.go`)
- [x] Webhook tests with httptest (`quarry/adapter/webhook/webhook_test.go`)
- [x] CLI flags and hook in `quarry/cli/cmd/run.go`
- [x] CONTRACT_INTEGRATION.md updated with runtime adapter reference
- [x] CONTRACT_CLI.md updated with adapter flags
- [x] Integration guide updated with webhook example
- [x] CLI_PARITY.json updated

### Redis Pub/Sub Adapter (v0.5.0)
- [x] Redis adapter implementation (`quarry/adapter/redis/redis.go`)
- [x] Redis adapter tests with miniredis (`quarry/adapter/redis/redis_test.go`)
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

See `docs/guides/temporal.md` for the full Temporal integration design.

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

This preserves `CONTRACT_CLI.md`'s invariant: `quarry run` is the only
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
advisory per `CONTRACT_EMIT.md`.

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
- [x] Fan-out flag wiring in `quarry/cli/cmd/run.go`
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

See `docs/ingress-models.md` for the broader analysis of ingress models
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
- `StreamingPolicy` implementation in `quarry/policy/`
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
- `quarry/types/proxy.go` — field, validation, warnings
- `quarry/proxy/selector.go` — ring buffer, modified `selectRandom`, stats
- `sdk/src/types/proxy.ts` — `recencyWindow` field
- `sdk/src/proxy.ts` — validation in `validateProxyPool`
- `quarry/cli/reader/types.go` — `RecencyWindow`/`RecencyFill` in `ProxyRuntime`

#### Mini-milestones
- [x] Contract updated (`CONTRACT_PROXY.md` — recency semantics)
- [x] Guide updated (`docs/guides/proxy.md` — user-facing docs)
- [x] Go types and validation (`quarry/types/proxy.go`)
- [x] Selector ring buffer and recency-aware random (`quarry/proxy/selector.go`)
- [x] SDK types and validation (`sdk/src/types/proxy.ts`, `sdk/src/proxy.ts`)
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

## Module Split — Multi-Module Restructure

### Goal

Split the `quarry/` Go module into independent modules so that
`quarry-temporal/` (and future integrations) can depend on core runtime
types without pulling in CLI, TUI, or unrelated dependencies.

### Agreed Layout

| Module | Go Module Path | Contains |
|--------|---------------|----------|
| `quarry-core/` | `github.com/pithecene-io/quarry-core` | types, runtime, policy, lode, adapter, proxy, metrics, log |
| `quarry-cli/` | `github.com/pithecene-io/quarry-cli` | CLI commands, TUI, config, reader |
| `quarry-temporal/` | `github.com/pithecene-io/quarry-temporal` | activity, workflow, worker binary |
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

1. Extract `quarry-core/` — move types, runtime, policy, lode, adapter,
   proxy, metrics, log into standalone module.
2. Extract `quarry-cli/` — CLI, TUI, config, reader depend on
   `quarry-core/`.
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

## Temporal Orchestration Integration

### Goal

Enable Quarry to run as a Temporal activity, with workflow-level retries,
lineage tracking, and durable execution guarantees.

### Prerequisites

- [ ] Module split complete (`quarry-core/` extracted)
- [ ] `quarry-temporal/` module created

### Deliverables

- Activity wrapper: thin in-process wrapper calling
  `runtime.NewRunOrchestrator(config).Execute(ctx)`
- Heartbeat goroutine: concurrent liveness reporting (30s interval)
- Outcome mapping: `OutcomeStatus` → Temporal error semantics per
  `CONTRACT_INTEGRATION.md`
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

See `docs/guides/temporal.md` for detailed design and pseudocode.

---

## Additional Event Bus Adapters (Staggered)

Order of support:
- ~~Redis Pub/Sub~~ (shipped in v0.5.0)
- NATS
- SNS/SQS

> Temporal is an orchestration integration, not a notification adapter.
> See [Temporal Orchestration Integration](#temporal-orchestration-integration)
> and `docs/guides/temporal.md`.

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
- [ ] Update `docs/contracts/CONTRACT_EMIT.md`
- [ ] Update impacted contract docs (if any)
- [ ] Verify SDK types match contract definitions

**For IPC/streaming changes:**
- [ ] Update `docs/contracts/CONTRACT_IPC.md`
- [ ] Verify executor and runtime implementations align

**For run identity/lineage changes:**
- [ ] Update `docs/contracts/CONTRACT_RUN.md`

### PR requirements

PRs that modify contract behavior must:
1. Reference the specific contract section changed.
2. Include a **compatibility note** (breaking vs additive).
3. Update `contract_version` if the change is breaking.
