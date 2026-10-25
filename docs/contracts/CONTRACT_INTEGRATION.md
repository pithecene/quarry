# Quarry Integration — Contract

This document defines the **integration boundary** for downstream notification
adapters and orchestration integrations used by the Quarry runtime.

This is a contract document. Implementations must conform.

---

## Scope

- Defines the distinction between notification adapters, event sinks, and orchestration integrations.
- Defines the adapter boundary and ownership.
- Defines the event sink boundary, delivery semantics, and failure behavior.
- Defines CLI/config selection rules.
- Defines delivery semantics and required observability.

Non-goals:
- Does not define executor behavior.
- Does not define user script behavior.
- Does not define provider-specific configuration details.
- Does not define orchestrator-specific workflow semantics (for Temporal,
  see [guides/temporal.md](../guides/temporal.md)).

---

## Integration Paradigms

Quarry supports three distinct integration paradigms:

| Paradigm | Relationship to Runtime | Timing | Direction | Examples |
|----------|------------------------|--------|-----------|----------|
| Event Sink | Receives events **during** a run | In-run | Runtime → External | Redis Streams, Lode |
| Notification Adapter | Invoked **by** runtime after a run | Post-run | Runtime → External | Webhook, Redis Pub/Sub |
| Orchestration Integration | **Wraps** the runtime as embedded activity | External | External → Runtime | A first-party host, Temporal |

**Event sinks** receive events in real time as the executor produces them.
They implement the `EventSink` interface, are selected via `--event-sink`
flags, and support per-sink delivery semantics (`mandatory` or
`best_effort`). The runtime owns their lifecycle. Event sinks are NOT
adapters and are NOT selected via `--adapter` flags.

**Notification adapters** are in-process modules that fire after a run
completes. They implement the `Adapter` interface, are selected via
`--adapter` flags, and follow best-effort delivery semantics. The runtime
owns their lifecycle.

**Orchestration integrations** embed the runtime as a unit of work within
a host. The host owns scheduling, retries, and delivery guarantees. A host
may be first-party (a long-lived `quarry` process) or an external
orchestrator. The runtime must not depend on which. Orchestration
integrations do NOT implement the `Adapter` interface and are NOT selected
via `--adapter` flags. They depend on the runtime as a library.

---

## Adapter Model

- Adapters are in-repo modules under [`quarry/adapter/`](../../quarry/adapter/).
- The runtime owns adapter lifecycle and selection.
- Users do not write runtime code. Users only provide configuration.
- Adapter notification is **best-effort**: failures are logged to stderr
  but do not fail the run. Run data is already persisted before the
  adapter is invoked.

### Notification Adapters

| Adapter | Package |
|---------|---------|
| Webhook (HTTP POST) | [`quarry/adapter/webhook`](../../quarry/adapter/webhook/) |
| Redis (Pub/Sub) | [`quarry/adapter/redis`](../../quarry/adapter/redis/) |

> Temporal is an orchestration integration, not a notification adapter.
> See [Orchestration Integration Semantics](#orchestration-integration-semantics)
> and [guides/temporal.md](../guides/temporal.md).

---

## Event Sink Model

Event sinks receive events **during** the run through the ingestion policy
path. They are structurally distinct from notification adapters:

- Adapters fire once, after a run completes and data is persisted.
- Event sinks receive every event as it is ingested, in real time.

Event sinks are in-repo modules that implement the `EventSink` interface
(`WriteEvents`, `Close`). The runtime owns their lifecycle and selection.

### Scope Boundary

- Event sinks handle **events only**. Artifact chunks always go to Lode
  regardless of event sink configuration.
- Event sinks are not adapters. They are not selected via `--adapter` flags.
- Event sinks are selected via `--event-sink` and related flags.
- When no event sinks are configured, events are written to Lode only.

### Delivery Semantics

Each event sink declares a **delivery mode**:

| Mode | Behavior |
|------|----------|
| `mandatory` | Write failure propagates through the policy and may fail the run. |
| `best_effort` | Write failure is logged to stderr and swallowed. The run continues. |

Default delivery mode is `mandatory`.

The runtime dispatches events to all configured sinks concurrently via a
fan-out dispatcher. Mandatory failures from any sink are collected and
returned as a combined error. Best-effort failures are observable but do
not contribute to the error result.

### Failure Behavior

- A `mandatory` sink failure propagates through the ingestion policy. Under
  `StrictPolicy`, this fails the run with `policy_failure`.
- A `best_effort` sink failure is logged to stderr with the sink label and
  error. The run continues unaffected.
- Close errors from any sink are collected. Close always attempts all sinks.

### Fan-Out Inheritance

Child runs (via `--depth > 0`) inherit the parent run's event sink
configuration. All configured sinks are propagated to child runs.

### Available Event Sinks

| Sink | Package | Description |
|------|---------|-------------|
| Lode | [`quarry/lode`](../../quarry/lode/) | Default event persistence (always available) |
| Redis Streams | [`quarry/adapter/redisstream`](../../quarry/adapter/redisstream/) | Real-time event streaming via XADD |

### Redis Streams Sink

The Redis Streams sink publishes events to a shared Redis Stream key using
pipelined XADD commands.

Configuration defaults:
- Stream key: `quarry:events`
- Max length: `100000` (approximate trimming via `MAXLEN~`)
- TTL: `24h` (applied once on first successful write via EXPIRE)
- Timeout: `2s` per operation
- Retries: `2` with exponential backoff (500ms × 2^attempt)

Sentinel values:
- `max_len: -1` disables stream trimming.
- `ttl: -1` disables key expiry.
- `max_len: 0` and `ttl: 0` apply defaults.

Each stream entry contains flat fields: `run_id`, `event_type`, `seq`,
`timestamp`, `source`, `category`, and `payload` (JSON-encoded).

---

## Selection and Configuration

### Selection
- Adapter and event sink selection is runtime-owned and CLI/config-driven.
- Per-run selection via `quarry run --adapter <type>` or `--event-sink <type>`
  flags is the baseline.
- Global defaults via config are optional and additive.
- No silent fallback to a different adapter or event sink is permitted.
- If `--adapter` is not set, no notification is sent.
- If `--event-sink` is not set, events are written to Lode only.

### Configuration
- Adapters and event sinks must accept configuration only from CLI/config inputs.
- Sensitive fields must be redacted from logs and output.
- Duplicate sink types are rejected (at most one sink of each type).

### Adapter CLI Flags

| Flag | Description |
|------|-------------|
| `--adapter <type>` | Adapter type (`webhook`, `redis`) |
| `--adapter-url <url>` | Endpoint URL (required when `--adapter` is set) |
| `--adapter-header <key=value>` | Custom HTTP header (repeatable, webhook only) |
| `--adapter-channel <name>` | Pub/sub channel name (redis only, default `quarry:run_completed`) |
| `--adapter-timeout <duration>` | Notification timeout (default `10s`) |
| `--adapter-retries <n>` | Retry attempts (default `3`) |

### Event Sink CLI Flags

| Flag | Description |
|------|-------------|
| `--event-sink <type>` | Event sink type (`lode`, `redis`), repeatable |
| `--event-sink-lode-delivery <mode>` | Delivery mode for Lode sink (default `mandatory`) |
| `--event-sink-redis-url <url>` | Redis URL (required when `--event-sink redis` is set) |
| `--event-sink-redis-stream-key <key>` | Stream key (default `quarry:events`) |
| `--event-sink-redis-max-len <n>` | Max stream length, -1 disables (default `100000`) |
| `--event-sink-redis-ttl <duration>` | Key TTL, -1 disables (default `24h`) |
| `--event-sink-redis-timeout <duration>` | Per-operation timeout (default `2s`) |
| `--event-sink-redis-retries <n>` | Retry attempts (default `2`) |
| `--event-sink-redis-delivery <mode>` | Delivery mode for Redis sink (default `mandatory`) |

Config file equivalent (`events.sinks[]` array in YAML):
```yaml
events:
  sinks:
    - type: lode
      delivery: mandatory
    - type: redis
      delivery: best_effort
      url: redis://localhost:6379
      stream_key: quarry:events
      max_len: 100000
      ttl: 24h
      timeout: 2s
      retries: 2
```

---

## Adapter Delivery Semantics

- Adapter delivery is **best-effort with retries**. The adapter retries on
  transient failures (5xx, network errors) but may ultimately fail.
  A failed publish is logged to stderr. The run outcome is unaffected.
- On success, delivery may be duplicated (retries after ambiguous
  failure). Consumers should use `run_id` as the idempotency key.

Adapters must not:
- alter the event payload,
- silently drop events without observable failure.

Event sink delivery semantics are defined per-sink. See
[Event Sink Model](#event-sink-model).

---

## Strategy Surface

Strategy is a contract-defined enum limited to batching and retries.

### Batching
Allowed values:
- `none`
- `fixed_count`
- `fixed_time`

### Retries
Allowed values:
- `none`
- `bounded`
- `infinite` (if supported)

Ordering and fan-out strategies are out of scope.

---

## Invocation Ordering

The adapter is invoked **after** `Execute` returns, so all of the following
have happened ([CONTRACT_RUN.md](CONTRACT_RUN.md) §Run Finalization):
1. Run execution completes (success or failure)
2. Policy flush completes
3. Metrics are persisted to Lode
4. The run's end record is written, or its write failed

Notification is not skipped when finalization is incomplete: a run whose
start record failed is notified as `executor_crash` (it never started),
and a run whose end record failed is notified with its real outcome while
the failure is surfaced as [CONTRACT_RUN.md](CONTRACT_RUN.md) requires.

This ensures consumers can read the data referenced in the event payload.
Adapter publish is the last step before CLI output and exit.

---

## Failure and Backpressure

- Adapter failures must be observable via stderr warnings.
- Adapter failure does not change the run exit code.
- Backpressure must block or fail explicitly. No silent loss is permitted.

---

## Security and Redaction

- Credentials must never be emitted in events, logs, or CLI output.
- Any adapter-specific secrets must be redacted at the boundary.

---

## Orchestration Integration Semantics

This section defines normative requirements for orchestration integrations
that embed the Quarry runtime as a unit of work.

### Activity Boundary

The orchestrator invokes `runtime.NewRunOrchestrator(config).Execute(ctx)`
as an atomic unit of work. The runtime owns everything inside this boundary
(executor lifecycle, IPC, policy, Lode persistence, and run
finalization: the start record, metrics persistence, and the end record, as
[CONTRACT_RUN.md](CONTRACT_RUN.md) §Run Finalization defines). The orchestrator owns everything
outside that boundary (scheduling, retries, fan-out, and notification) and
receives the same storage contents as the CLI without persisting anything
itself.

### Outcome Mapping

Orchestration integrations must map `OutcomeStatus` (from `types/lineage.go`)
to orchestrator-native error semantics:

| OutcomeStatus | Orchestrator Behavior |
|---------------|----------------------|
| `success` | Activity completes normally |
| `script_error` | Retryable error |
| `executor_crash` | Retryable error |
| `policy_failure` | Non-retryable error |
| `version_mismatch` | Non-retryable error |

The outcomes `policy_failure` and `version_mismatch` are non-retryable because they
indicate systemic configuration problems that retries cannot resolve.

### Lineage Preservation

The orchestrator must set `RunMeta` fields per `RunMeta.Validate()` rules:

- Each execution receives a unique `run_id`.
- `attempt` starts at 1 and increments on each retry.
- `parent_run_id` links retries to the previous execution's `run_id`.
- `attempt == 1` must have `parent_run_id == nil` (initial run).
- `attempt > 1` must have `parent_run_id != nil` (retry run).

### Heartbeat Contract

Orchestration integrations should heartbeat at a recommended interval of
30 seconds. Context cancellation from the orchestrator must trigger:

1. Executor kill (best-effort).
2. Policy flush (best-effort).

### Data Flow

Only orchestration metadata flows through the orchestrator (~10KB: config
in, result summary out). Run data (events, artifacts, metrics) stays in
Lode. This keeps orchestrator payloads small and avoids serialization
limits.

---

## Versioning

- Additive changes only during 0.x.
- Renames or semantic changes are breaking and forbidden in 0.x.
