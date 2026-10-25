# Ingress Models — Use Cases and Trade-offs

This document is a **design exploration**, not a commitment.
It catalogs the ways Quarry could accept work, maps each to the use cases
it serves, and identifies where the current model falls short.
No contracts are proposed or modified.

---

## Use Case Taxonomy

Quarry sits at the intersection of several distinct workload shapes.
They differ in how work is discovered, how many units exist, and whether
the process is exploratory or operationalized.

### 1. Ad Hoc Exploration

A developer wants to scrape a page, inspect the output, tweak the script,
and repeat. The emphasis is speed of iteration, not durability or automation.

**Characteristics:**
- Single URL or small target set
- Human in the loop
- Output consumed interactively (terminal, JSON file)
- No pipeline context — Quarry is the entire workflow

### 2. Formalized Single-Target Extraction

A script runs against a known URL (or small set) on a schedule. It emits
structured data that feeds a downstream system. Reliability and
repeatability matter; the script is stable and rarely changes.

**Characteristics:**
- Fixed, known targets
- Cron / CI / Airflow triggers the run externally
- Output consumed by ETL pipeline or data warehouse
- Retry semantics matter
- Proxy rotation often required (long-running, recurring scrapes)

### 3. Crawling (Discovered Work)

A script starts at a seed URL and discovers more work as it runs:
listing pages yield detail URLs, pagination reveals more pages,
sitemaps reference thousands of endpoints. The work graph is not
known upfront.

**Characteristics:**
- Fan-out: one run produces N follow-up targets
- Depth/breadth bounds needed to prevent runaway crawls
- Deduplication matters (same URL discovered via multiple paths)
- Total work is discovered incrementally, not declared upfront
- Output volume is proportional to crawl depth

### 4. High-Volume Batch

An external system provides thousands of targets (URLs, API endpoints,
search queries). Each target is independent. The concern is throughput,
backpressure, and resource management — not work discovery.

**Characteristics:**
- Targets known upfront (manifest, database query, queue)
- Parallelism is the primary scaling lever
- Failure isolation: one target failing should not affect others
- Progress tracking and partial completion matter
- Proxy pool sizing becomes critical

### 5. Pipeline Step (Orchestrated ETL)

Quarry is one stage in a multi-step DAG. An orchestrator (Temporal,
Airflow, Prefect, Step Functions) manages sequencing, retries, and
data flow between steps. Quarry's role is to extract; other steps
transform and load.

**Characteristics:**
- Quarry does not own retry or sequencing logic
- Orchestrator provides durable execution guarantees
- Run inputs and outputs must be addressable by the orchestrator
- Quarry must report outcomes in a machine-readable way
- Heartbeating or liveness signals may be required

### 6. Continuous / Event-Driven

New work arrives over time: a queue fills with URLs, a webhook
delivers events, a change feed produces targets. Quarry would process
items as they appear, indefinitely.

**Characteristics:**
- Long-running consumer process (not one-shot)
- Backpressure and flow control are critical
- Graceful shutdown and checkpointing matter
- Operational concerns: monitoring, restarts, scaling

---

## Ingress Models

Each model below is a way for work to enter Quarry. They are not
mutually exclusive — a project could use different models for
different workloads.

### A. CLI One-Shot (`quarry run`)

**What it is today.**

A single CLI invocation runs one script against one job.
The process exits when the script completes or fails.

```
quarry run --source script.ts --job '{"url":"..."}' --storage-backend fs --storage-path ./data
```

**Serves well:**
- Ad hoc exploration
- Formalized single-target extraction (via cron)
- Pipeline step (when wrapped by an orchestrator)

**Serves poorly:**
- Crawling (enqueue events are advisory; no follow-up)
- High-volume batch (must orchestrate N invocations externally)
- Continuous processing (no consumer loop)

**Strengths:**
- Simple mental model: one invocation, one run, one exit code
- Composable with Unix tools (pipes, xargs, GNU parallel)
- No long-running state to manage
- Easy to test and debug

**Weaknesses:**
- Process-per-run overhead for high-volume workloads
- No built-in fan-out for discovered work
- Orchestration burden falls entirely on the caller

---

### B. Crawl Mode (`quarry crawl`)

A single CLI invocation that recursively follows `enqueue` events.
Each enqueue becomes a child run. The process exits when the crawl
graph is exhausted or limits are reached.

```
quarry crawl --source listing.ts --job '{"url":"..."}' \
  --max-depth 3 --concurrency 4 --storage-backend fs --storage-path ./data
```

**How it would work:**
- Root run emits `enqueue` events with `target` (source script) and `params` (job payload)
- Runtime spawns child runs for each enqueue, up to concurrency limit
- `parent_run_id` / `attempt` envelope fields provide lineage tracking
- Depth counter increments per generation; `--max-depth` bounds it
- Deduplication by `(target, params)` hash prevents redundant runs
- All runs write to the same Lode dataset — one crawl, one output
- Exit code reflects worst-case child outcome

**Serves well:**
- Crawling (the primary target)
- Ad hoc exploration with follow-links

**Serves poorly:**
- High-volume batch (no external queue; targets must be discovered, not pre-declared)
- Pipeline step (orchestrators prefer to own the run graph)
- Continuous processing (still one-shot; terminates when graph exhausts)

**Strengths:**
- Zero infrastructure — no queue, no orchestrator, no daemon
- Stays CLI-shaped: deterministic start, deterministic end
- Natural fit for "I found more URLs, go get them"
- `enqueue` goes from advisory to actionable with a simple opt-in

**Weaknesses:**
- Unbounded fan-out risk (mitigated by `--max-depth` and `--concurrency`)
- Single-process bottleneck for very large crawls
- No cross-process coordination (one machine, one crawl)
- Failure semantics are more complex (partial crawl success)
- New CLI command and runtime behavior to maintain

**Open questions:**
- Does `target` map to `--source` (different script per depth level), or is it always the same script?
- Should dedup be exact-match on params, or pluggable (e.g. URL normalization)?
- How does `--max-depth` interact with heterogeneous targets (listing → detail → assets)?
- Should failed child runs be retried automatically, or just recorded?

---

### C. Queue Consumer (`quarry work`)

A long-running process that pulls jobs from an external queue and
invokes `quarry run` for each. The queue provides backpressure,
visibility, and retry semantics.

```
quarry work --queue redis://localhost:6379/0/quarry-jobs \
  --concurrency 4 --storage-backend s3 --storage-path s3://bucket/data
```

**How it would work:**
- Consumer connects to a queue (Redis list, SQS, etc.)
- Pulls job payloads, each containing `source` and `job` fields
- Spawns a `quarry run` for each (in-process or subprocess)
- Acknowledges the job on success; nacks/requeues on failure
- Runs until stopped (SIGTERM → graceful drain)

**Serves well:**
- High-volume batch
- Continuous / event-driven processing
- Crawling (enqueue events push back to the queue)

**Serves poorly:**
- Ad hoc exploration (too much setup for one-offs)
- Simple formalized extraction (overkill for single-target cron)

**Strengths:**
- Backpressure built-in (queue depth = work in flight)
- Horizontal scaling (run multiple `quarry work` processes)
- Retry semantics owned by queue infrastructure
- Enqueue events can feed back into the same queue (crawl without depth tracking)
- Clean separation: queue owns scheduling, Quarry owns execution

**Weaknesses:**
- Requires queue infrastructure (operational cost)
- Long-running process to monitor, restart, scale
- Queue choice becomes a coupling point (Redis vs SQS vs NATS)
- Quarry now has two operational modes with different failure characteristics

**Open questions:**
- Should `quarry work` support multiple queue backends, or just one (with adapters)?
- In-process execution (import runtime as library) or subprocess (fork `quarry run`)?
- How does graceful shutdown interact with in-flight runs?
- Should enqueue events automatically feed back to the queue, or require explicit config?

---

### D. HTTP/gRPC API (`quarry serve`)

A server that accepts job submissions over the network and returns
run IDs. Callers poll or subscribe for completion.

```
quarry serve --port 8080 --concurrency 8 \
  --storage-backend s3 --storage-path s3://bucket/data
```

**How it would work:**
- HTTP endpoint: `POST /runs` with job payload, returns `{ run_id }`
- Status endpoint: `GET /runs/{id}` returns run state
- Completion notifications via webhook callback or SSE
- Bounded concurrency with request queuing

**Serves well:**
- Microservice integration (extraction as a service)
- Multi-tenant platforms (shared extraction infrastructure)
- High-volume batch (submit many jobs, poll for results)

**Serves poorly:**
- Ad hoc exploration (CLI is faster for one-offs)
- Simple cron jobs (HTTP adds unnecessary indirection)
- Crawling (no inherent fan-out mechanism)

**Strengths:**
- Language-agnostic client surface (any HTTP client)
- Centralized resource management (one deployment, many callers)
- Natural fit for platform/SaaS products that embed extraction

**Weaknesses:**
- Significant operational surface (deployment, auth, monitoring, scaling)
- Quarry becomes a service, not a tool — fundamental identity shift
- Must handle request queuing, timeouts, health checks
- Authentication and authorization are now Quarry's problem
- Furthest departure from the current architecture

**Open questions:**
- Is this really Quarry, or a separate service that wraps Quarry?
- Who is the user? Individual developers or platform teams?
- Does this belong in core, or as a community project?

---

### E. Temporal Activity (Planned)

Quarry's runtime runs as a Temporal activity within a workflow.
Temporal owns retries, lineage, and durable execution guarantees.

```go
// Pseudocode — workflow invokes Quarry as an activity
result, err := workflow.ExecuteActivity(ctx, quarry.ExtractActivity, quarry.ActivityInput{
    Source: "script.ts",
    Job:    map[string]any{"url": "https://example.com"},
})
```

**Already planned** in the implementation roadmap (see [`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md)).

**Serves well:**
- Pipeline step (Temporal's primary use case)
- High-volume batch (Temporal manages parallelism and failure)
- Crawling (workflow can react to enqueue events and spawn child activities)

**Serves poorly:**
- Ad hoc exploration (Temporal is heavy for one-offs)
- Simple cron (Temporal is overkill; use `quarry run` + cron)

**Strengths:**
- Durable execution guarantees (retries, timeouts, heartbeating)
- Lineage and observability built-in
- Workflow logic can implement arbitrary orchestration patterns
- Quarry stays focused on extraction; Temporal handles the DAG

**Weaknesses:**
- Requires Temporal cluster (significant operational commitment)
- Go SDK dependency; not accessible to non-Go consumers
- Learning curve for teams unfamiliar with Temporal
- Adds a module to maintain (`quarry-temporal/`)

---

### F. Library / Programmatic API

Quarry's runtime exposed as an importable Go package. Callers
construct a run configuration and call `Execute()` directly —
no CLI, no subprocess.

```go
result, err := runtime.NewRunOrchestrator(config).Execute(ctx)
```

**This is partially what Temporal integration requires** — the module
split already plans to extract `quarry-core/` as an importable library.

**Serves well:**
- Embedding Quarry in larger Go applications
- Custom orchestration (callers build their own loops)
- Testing (programmatic control without subprocess overhead)

**Serves poorly:**
- Non-Go consumers (TypeScript, Python, etc.)
- Ad hoc use (CLI is more ergonomic)

**Strengths:**
- Maximum flexibility — caller controls everything
- No IPC overhead for Go consumers
- Enables custom ingress patterns without Quarry needing to support them
- Already partially planned (module split prerequisite)

**Weaknesses:**
- Go-only — limits accessibility
- API stability burden (library API is harder to evolve than CLI flags)
- Callers must understand runtime internals (config structs, lifecycle)

---

## Current State Assessment

| Use Case | Current Support | Gap |
|----------|----------------|-----|
| Ad hoc exploration | ✅ `quarry run` works | Minor: no REPL or interactive mode |
| Formalized single-target | ✅ `quarry run` + cron | None |
| Crawling | ❌ `enqueue` is advisory | No built-in follow-up; requires external orchestration |
| High-volume batch | ⚠️ Possible via external parallelism | No built-in queue consumer; N processes = N invocations |
| Pipeline step (orchestrated) | ⚠️ CLI works; Temporal planned | Temporal not yet implemented; CLI exit codes are the only API |
| Continuous / event-driven | ❌ No consumer mode | Requires external queue + loop around `quarry run` |

The CLI one-shot model is clean and sufficient for use cases 1–2.
Use cases 3–6 all require the caller to build orchestration that
Quarry does not provide. The question is which of these gaps Quarry
should close natively versus leaving to external tooling.

---

## Tension: Tool vs. Service

The models above fall on a spectrum:

```
  Tool                                                     Service
  ────────────────────────────────────────────────────────────────
  quarry run     quarry crawl     quarry work     quarry serve
  (one-shot)     (recursive)      (consumer)      (API)
```

Moving right increases Quarry's operational surface and infrastructure
requirements. Moving left keeps Quarry simple but pushes orchestration
complexity onto users.

The Temporal integration sidesteps this by keeping Quarry as a library
that a separate system (Temporal) orchestrates. But Temporal is a
heavyweight dependency that many projects cannot justify.

**The strategic question is: where on this spectrum does Quarry provide
the most value to the most users, without becoming something it isn't?**

---

## Not Decided Here

This document does not:
- Propose contract changes
- Recommend a specific model
- Commit to implementation timelines
- Deprecate `quarry run`

It exists to enumerate the option space before any design work begins.
