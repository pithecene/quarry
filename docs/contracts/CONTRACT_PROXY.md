# Quarry Proxy — Contract

This document defines the **proxy configuration, selection, and application**
contract across the Go runtime, Node executor, and SDK.

This is a contract document. Implementations must conform.

---

## Scope

- Defines the shared proxy data model (endpoint, pool, strategy).
- Defines runtime selection/rotation rules.
- Defines executor application rules for Puppeteer.
- Defines validation and redaction requirements.

Non-goals:
- Provider-specific behavior.
- Proxy health checking or testing.

---

## Shared Data Model

### ProxyProtocol
Allowed values: `http`, `https`, `socks5`.

### ProxyEndpoint
A resolved endpoint the executor can dial.

Required fields:
- `protocol` (ProxyProtocol)
- `host` (string)
- `port` (integer)

Optional fields:
- `username` (string)
- `password` (string)

### ProxyStrategy
Allowed values: `round_robin`, `random`, `sticky`.

### ProxySticky
Sticky semantics for a pool.

Required fields:
- `scope` (string): `job` | `domain` | `origin`

Optional fields:
- `ttlMs` (integer)

### ProxyPool
Defines a pool and rotation policy.

Required fields:
- `name` (string)
- `strategy` (ProxyStrategy)
- `endpoints` (ProxyEndpoint[])

Optional fields:
- `sticky` (ProxySticky)
- `recency_window` (integer): number of recently-used endpoints to exclude from random selection. Only meaningful when strategy is `random`.

### JobProxyRequest
Job-level selection of a pool and optional overrides.

Required fields:
- `pool` (string)

Optional fields:
- `strategy` (ProxyStrategy)
- `stickyKey` (string)

---

## Validation Rules

Hard validation (must reject):
- `endpoints` length > 0
- `port` is within 1–65535
- `protocol` is one of `http|https|socks5`
- `username` and `password` must be provided together if either is set
- `recency_window` must be positive if set

Soft warnings (must surface):
- `socks5` usage with Puppeteer is best-effort
- very large endpoint lists with `round_robin` (recommend `random`)
- `recency_window` set on non-random strategy (has no effect)

---

## Responsibility Split

### Go Runtime
Owns:
- Parsing, env expansion, and validation of proxy pools
- Selection policy and state (round-robin counters, sticky maps)
- Emitting a **resolved** ProxyEndpoint in the run request

Does not:
- Apply proxy settings to Puppeteer
- Load provider-specific logic

### Node Executor
Owns:
- Translating ProxyEndpoint into Puppeteer launch arguments
- Applying proxy credentials at page-level authentication
- Sticky semantics tied to browser/page lifecycle

Does not:
- Select pools or strategies
- Read repository config

### SDK
Owns:
- Types and ergonomics for proxy configuration
- Validation helpers
- Optional compile helpers if a build step exists

---

## Selection and Rotation Rules (Runtime)

### Round-robin
- Maintain a counter per pool.
- Select endpoints by incrementing the counter atomically.

### Random
- Select uniformly at random.
- A secure RNG is optional, and a deterministic RNG is allowed.

### Random with Recency Window
- When `recency_window` is set on a pool with `random` strategy, maintain a ring buffer of the last N selected endpoint indices.
- Exclude ring buffer entries from the candidate pool before random selection.
- If all endpoints are excluded (window >= endpoint count), select the least-recently-used (oldest in ring). Never blocks.
- Ring buffer is updated only on committed selections (peek does not advance).
- Recency state is in-memory only and does not persist across process restarts.

### Sticky
- Maintain a map from **sticky key** → endpoint.
- Sticky key precedence:
  1) `JobProxyRequest.stickyKey` if provided
  2) if scope = `job`: `jobId`
  3) if scope = `domain`: `domain`
  4) if scope = `origin`: `scheme+host+port`
- If `ttlMs` is set, entries expire and are reselected on next use.

---

## Executor Application (Puppeteer)

Requirements:
- Apply proxy host/port/protocol at **browser launch**.
- Apply credentials via **page authentication** when `username` and `password` exist.
- Never log or emit proxy passwords.
- `socks5` must be accepted by the contract but treated as best-effort.

---

## IPC Payloads

### Run Request
- The run request includes an optional `proxy` field of type ProxyEndpoint.
- If `proxy` is absent, the executor must launch without a proxy.

### Run Result
- The run result may include `proxy_used` metadata.
- `proxy_used` must **exclude** password fields.
