# ARCH_INDEX.md — Quarry Architecture Index

This file is a **navigation map** of Quarry’s subsystems.
It summarizes *what exists and where*, not how things are implemented.

Normative behavior is defined in `docs/contracts/CONTRACT_*.md`.

Maintenance rule:
Update this file only when a new subsystem boundary becomes real.
Do not update for internal refactors.

---

## Root

- [`AGENTS.md`](../AGENTS.md) — development guardrails and agent constraints
- [`CHANGELOG.md`](../CHANGELOG.md) — release history
- [`CLAUDE.md`](../CLAUDE.md) — repository constitution
- `Dockerfile` — container image definition
- `LICENSE` — project license
- [`PUBLIC_API.md`](../PUBLIC_API.md) — user-facing API and setup guide
- [`README.md`](../README.md) — project overview
- [`SUPPORT.md`](../SUPPORT.md) — support and contribution guide
- [`Taskfile.yaml`](../Taskfile.yaml) — task orchestration and developer workflows
- [`biome.json`](../biome.json) — formatting and linting configuration
- [`mise.toml`](../mise.toml) — toolchain version pinning
- [`package.json`](../package.json) — monorepo root metadata
- [`pnpm-workspace.yaml`](../pnpm-workspace.yaml) — monorepo layout
- `.grove.yaml` — grove stewardship declaration (intent, horizon, phase)

---

## docs/

### docs/guides/

User-facing guides (informational).

- [`guides/overview.md`](guides/overview.md) — user-facing system overview
- [`guides/emit.md`](guides/emit.md) — user-facing emit guide
- [`guides/streaming.md`](guides/streaming.md) — user-facing streaming and artifact behavior
- [`guides/run.md`](guides/run.md) — user-facing run lifecycle guide
- [`guides/policy.md`](guides/policy.md) — user-facing ingestion policy guide
- [`guides/lode.md`](guides/lode.md) — user-facing storage expectations
- [`guides/cli.md`](guides/cli.md) — user-facing CLI overview
- [`guides/configuration.md`](guides/configuration.md) — consolidated configuration reference (flags, env vars, config files)
- [`guides/container.md`](guides/container.md) — running Quarry via container images (docker run, Compose, sidecars)
- [`guides/proxy.md`](guides/proxy.md) — user-facing proxy guide
- [`guides/integration.md`](guides/integration.md) — downstream ETL trigger patterns (event-bus, polling)
- [`guides/temporal.md`](guides/temporal.md) — Temporal orchestration integration guide
- [`guides/benchmarks.md`](guides/benchmarks.md) — benchmark methodology and results

### docs/ (top-level)

- [`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md) — staged implementation roadmap
- [`RELEASE_READINESS_v0.3.0.md`](RELEASE_READINESS_v0.3.0.md) — historical release readiness (v0.3.0)
- [`RELEASE_READINESS_v1.0.md`](RELEASE_READINESS_v1.0.md) — v1.0 release gating and gap analysis
- [`ingress-models.md`](ingress-models.md) — data ingestion modeling
- [`CLI_PARITY.json`](CLI_PARITY.json) — CLI flag parity tracking

### docs/contracts/

Normative contracts (authoritative). These documents define **system behavior**.

- [`contracts/CONTRACT_IPC.md`](contracts/CONTRACT_IPC.md) — IPC framing, ordering, and transport semantics
- [`contracts/CONTRACT_EMIT.md`](contracts/CONTRACT_EMIT.md) — event and artifact emission rules
- [`contracts/CONTRACT_RUN.md`](contracts/CONTRACT_RUN.md) — run lifecycle and terminal states
- [`contracts/CONTRACT_POLICY.md`](contracts/CONTRACT_POLICY.md) — policy-level constraints
- [`contracts/CONTRACT_PROXY.md`](contracts/CONTRACT_PROXY.md) — proxy configuration, selection, and executor application
- [`contracts/CONTRACT_CLI.md`](contracts/CONTRACT_CLI.md) — CLI command topology and read-only guarantees
- [`contracts/CONTRACT_LODE.md`](contracts/CONTRACT_LODE.md) — persistence and storage interaction
- [`contracts/CONTRACT_METRICS.md`](contracts/CONTRACT_METRICS.md) — runtime metrics surface and CLI stats requirements
- [`contracts/CONTRACT_INTEGRATION.md`](contracts/CONTRACT_INTEGRATION.md) — event-bus adapter boundary and delivery semantics

Contracts are authoritative over code.

---

## executor-node/

Node-based executor implementation and IPC boundary.

### executor-node/src/

- [`executor.ts`](../executor-node/src/executor.ts) — core executor logic
- [`bin/executor.ts`](../executor-node/src/bin/executor.ts) — CLI entrypoint
- `ipc/` — IPC framing, sink abstraction, and instrumentation

### executor-node/test/

Unit and IPC-focused tests for executor behavior.

---

## sdk/

Public SDK consumed by executors and integrations.
Defines **stable APIs** for emitting events, artifacts, and lifecycle signals.

### sdk/src/

- [`emit.ts`](../sdk/src/emit.ts) — public emit API
- [`context.ts`](../sdk/src/context.ts) — execution context model
- [`hooks.ts`](../sdk/src/hooks.ts) — lifecycle hooks
- [`storage-batcher.ts`](../sdk/src/storage-batcher.ts) — bounded-concurrency storage pipelining
- [`memory.ts`](../sdk/src/memory.ts) — memory pressure detection API
- [`reader.ts`](../sdk/src/reader.ts) — run reader (`listRuns`, `readRun`) over the CLI's JSON output
- `types/` — shared domain types exposed by the SDK

### sdk/test/

Contract, unit, ordering, and property-based tests validating SDK behavior.

---

## examples/

Example usage and integration references. Non-normative.

- [`manifest.json`](../examples/manifest.json) — schema-driven example registry (used by [`scripts/run-examples.ts`](../scripts/run-examples.ts))
- [`README.md`](../examples/README.md) — example catalogue overview
- [`demo.ts`](../examples/demo.ts) — minimal standalone executor demo
- `static-html-list/` — static HTML scraping example
- `toy-pagination/` — paginated crawl example
- `artifact-snapshot/` — artifact snapshot capture example
- `intentional-failure/` — deliberate failure mode demonstration
- `fan-out-chain/` — fan-out execution chain example
- `integration-patterns/` — downstream integration pattern examples
- `hooks-prepare/` — prepare-phase hook example
- `hooks-before-terminal/` — before-terminal hook example

---

## quarry/

Go module root. Contains runtime, CLI, and core types.

### quarry/types/

- `types/` — domain types: events, artifacts, lineage metadata, proxy configuration, and lockstep versioning

### quarry/runtime/

- `runtime/` — core execution orchestration: run lifecycle, executor management, IPC ingestion, artifact handling, and fan-out scheduling

### quarry/ipc/

- `ipc/` — frame encoding/decoding per CONTRACT_IPC.md (msgpack, 16 MiB frame limit, artifact chunking)

### quarry/executor/

- `executor/` — embedded executor lifecycle: extraction, SHA256 checksumming, temp directory management
- `executor/bundle/` — embedded executor.mjs JavaScript bundle (non-Go asset)

### quarry/policy/

- `policy/` — ingestion policy interface: event buffering, drop rules, flush behavior, and observability stats

### quarry/proxy/

- `proxy/` — proxy pool selector: round-robin, random, and sticky strategies with recency windows

### quarry/lode/

- `lode/` — Lode-backed storage client: artifact chunking, checksumming, and Hive-partitioned dataset management

### quarry/metrics/

- `metrics/` — per-run metrics collection: lifecycle, ingestion, executor, and storage operation counters

### quarry/reader/

- `reader/` — read side over Quarry's own layout: lists runs from run records (never from partitions) and reads a run's events from the partitions its start record names

### quarry/log/

- `log/` — structured JSON logging with run context fields (run_id, attempt, job_id)

### quarry/adapter/

- `adapter/` — event-bus adapter boundary for publishing run completion notifications
- `adapter/redis/` — Redis pub/sub adapter with exponential backoff retry
- `adapter/redisstream/` — Redis Streams event sink for real-time event publishing during runs
- `adapter/webhook/` — HTTP POST adapter with retryable and non-retriable error handling

### quarry/iox/

- `iox/` — I/O helpers: deferred close with cleanup registration for resource lifecycle management

### quarry/cli/

- `cli/cmd/` — CLI command implementations and shared flags (run, inspect, stats, list, debug, version)
- `cli/config/` — YAML config file parsing with duration handling and proxy pool definitions
- `cli/reader/` — read-side data access layer abstraction with dependency injection
- `cli/render/` — output renderer: JSON, YAML, and table formats with TTY detection
- `cli/tui/` — terminal UI dispatcher for inspect and stats commands (Bubble Tea)

### quarry/cmd/quarry/

- `cmd/quarry/` — CLI entrypoint: wires urfave/cli application with all commands

---

## scripts/

Developer tooling and test harnesses.

- [`run-examples.ts`](../scripts/run-examples.ts) — manifest-driven example runner
- [`version-targets.txt`](../scripts/version-targets.txt) — single list of every place the product version is written
- [`version-bump.sh`](../scripts/version-bump.sh), [`version-check.sh`](../scripts/version-check.sh), [`version-targets.sh`](../scripts/version-targets.sh) — lockstep bump and drift check over that list
- [`release/`](../scripts/release/) — release-workflow steps (monotonic tag check, idempotent npm and JSR publishing, convergence of the npm `latest` and GHCR `:latest`/`:slim` pointers) with stubbed-registry tests in [`release/test.sh`](../scripts/release/test.sh)

---

## ai/

Repo-local AI governance: skill definitions for structural validation.

### ai/skills/repo-convention-enforcer/v1/

- [`SKILL.md`](../ai/skills/repo-convention-enforcer/v1/SKILL.md) — structural enforcement validator
- [`input.schema.json`](../ai/skills/repo-convention-enforcer/v1/input.schema.json) — skill input schema
- [`output.schema.json`](../ai/skills/repo-convention-enforcer/v1/output.schema.json) — skill output schema

### ai/out/

Skill execution output directory (generated, non-authoritative).

---

## .github/

CI/CD workflows.

### .github/workflows/

- `workflows/ci.yml` — continuous integration
- `workflows/nightly.yml` — nightly checks
- `workflows/release.yml` — release automation
- `workflows/release-dry-run.yml` — release dry-run validation

---

## Architectural Notes

- Contracts in `docs/` are **normative**
- `executor-node/` owns execution and IPC concerns
- `sdk/` is the public-facing API surface
- Generated artifacts (`dist/`, `node_modules/`, `.example-runs/`) are non-authoritative
- IPC is treated as a hard boundary

This index is intentionally stable and low-detail.
