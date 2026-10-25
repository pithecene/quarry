# Release Readiness: v1.0

**Status**: Draft
**Scope**: v1.0 release gating and gap analysis
**Basis**: Internal dogfooding — production browser-based extraction workload at scale (~100k+ targets, ~700k stored files, containerized sidecar storage to S3-compatible backend)
**Versions tested**: 0.9.0 through 0.10.0

---

## Dogfooding Assessment

### What works well

1. **Streaming flush model.** `--policy streaming --flush-interval` delivers data to storage within seconds of emission. Auto-ingest can consume runs while extraction is still in progress.

2. **Job serialization.** `--job JSON.stringify(payload)` is clean and composable. Scripts receive typed `ctx.job` with no ceremony.

3. **Browser context management.** `ctx.browserContext` supports single-page and multi-tab batch patterns. Tab pooling with shared work queues works out of the box.

4. **Exit on idle.** `QUARRY_BROWSER_IDLE_TIMEOUT` prevents lingering containers. Correct behavior for ephemeral workloads.

5. **Run ID propagation.** `--run-id` enables end-to-end correlation across extraction, storage, import, and downstream processing.

6. **Container stability (0.10.0).** `--disable-dev-shm-usage` on all Chromium launch paths eliminates renderer crashes. Zombie browser server detection, stale process group cleanup, and transient health-check retry tolerance are solid hardening.

7. **Workspace resolution (0.10.0).** `--resolve-from` eliminates the need to duplicate shared code across scripts in monorepo/container environments.

### Scorecard

| Dimension | Grade | Notes |
|-----------|-------|-------|
| **Correctness** | **A-** | ~~Exit code semantics are not fully exposed to callers.~~ Structured exit reporting (v0.11.0). ~~`ctx.storage.put()` doesn't return the resolved storage key.~~ Returns `StoragePutResult` (v0.11.0). `file_write_ack` surfaces backend errors (v0.12.0). |
| **Ergonomics** | **A-** | ~~No batch/concurrent `ctx.storage.put()`.~~ `createStorageBatcher` (v0.12.0). ~~Memory management is entirely DIY.~~ `ctx.memory` API (v0.12.0). |
| **Reliability** | **A-** | ~~Storage errors may not be fully surfaced.~~ End-to-end error propagation via `file_write_ack` (v0.12.0). |
| **Performance** | **A-** | ~~Storage throughput limited by serial `put()`.~~ Bounded-concurrency batching via `createStorageBatcher` (v0.12.0). |

**Overall: A- — all identified gaps resolved. Minor friction only.**

### Grading scale

| Grade | Meaning |
|-------|---------|
| **A** | No issues. Would recommend to external users as-is. |
| **A-** | Minor friction only. No workarounds needed. |
| **B+** | Works well with minor gaps. Trivial or documented workarounds. |
| **B** | Works but has known gaps. Workarounds are manageable. |
| **B-** | Functional but rough. Multiple workarounds required. |
| **C+** | Usable with significant effort. Non-trivial workarounds. |
| **C** | Barely usable. Major workarounds dominate integration code. |

---

## v1.0 Blockers

### 1. `ctx.storage.put()` must return the resolved storage key ✅

**Resolved in v0.11.0** (PR #190) — `storage.put()` now returns `StoragePutResult` with `key` field.

**Severity:** Blocker
**Area:** SDK (`ctx.storage` API surface)

When a script calls `ctx.storage.put({ filename, data, content_type })`, Quarry computes the final Hive-partitioned storage key and writes to the backend. But the resolved key is never returned to the script.

This means downstream consumers must independently reconstruct storage paths to locate sidecar files — a brittle sync point between the extraction and import codebases. If either side's path logic changes independently, files become unreachable.

**Current signature:**

```typescript
put(options: StoragePutOptions): Promise<void>
```

**What v1.0 needs:**

```typescript
put(options: StoragePutOptions): Promise<StoragePutResult>
// where StoragePutResult = { key: string }
```

The returned `key` is the actual storage path written. Scripts can then include it in `ctx.emit.item()` payloads, enabling consumers to locate sidecar files without path reconstruction.

**Scope:** SDK type change (`StorageAPI`), IPC protocol change (`file_write` response or `file_write_ack`), runtime change (return key from Lode write).

### 2. Structured exit reporting ✅

**Resolved in v0.11.0** (PR #189) — `--report` flag writes structured JSON report to file on exit.

**Severity:** Blocker
**Area:** Runtime (process exit behavior), Contracts

Quarry's exit codes (0 = success, 1 = script error, 2 = crash, 3 = invalid input) are well-defined internally but opaque to callers. A caller receiving exit code 0 has no way to audit _what_ succeeded — how many items were emitted, whether storage writes failed, or whether warnings occurred.

**Current behavior:**

- Exit 0: success (run_complete emitted)
- Exit 1: script error (run_error emitted)
- Exit 2: executor crash
- Exit 3: invalid input / version mismatch

Callers that tolerate multiple exit codes (e.g., treating both 0 and 3 as acceptable) have no structured way to understand the difference.

**What v1.0 needs:**

On exit, emit a structured run summary (JSON to stderr or a well-known file) containing:
- Items emitted (count by type)
- Storage puts attempted / succeeded / failed
- Warnings and errors
- Terminal event type and summary payload
- Exit code and its meaning

This makes every run auditable without parsing logs.

---

## Important (not blocking)

### 3. `ctx.storage.put()` batching or concurrency ✅

**Resolved in v0.12.0** (PR #196) — `createStorageBatcher` provides bounded-concurrency pipelining with configurable concurrency (default 16).

**Severity:** High
**Area:** SDK (`ctx.storage` API surface)

`ctx.storage.put()` is single-item and shares the emit serialization chain, meaning writes are strictly sequential. For workloads that fetch N files in parallel (e.g., all images from a page), the SDK forces them through a single-file bottleneck.

Whether the backend can handle concurrent writes is a separate question. But the SDK should either:
- Offer `ctx.storage.putBatch(items[])` that pipelines calls, or
- Document that concurrent `put()` calls are safe (if they are)

### 4. Memory pressure awareness ✅

**Resolved in v0.12.0** (PR #196) — `ctx.memory.snapshot()` and `ctx.memory.isAbove(level)` with cgroup v2/v1 auto-detection.

**Severity:** High
**Area:** Runtime / SDK

Image-heavy extraction workloads commonly need a memory watchdog that polls usage against the container's cgroup limit and drains work when thresholds are hit. This is generic infrastructure that every such workload reimplements.

**Possible API:**

```typescript
ctx.onMemoryPressure(threshold: number, handler: () => void)
// or
ctx.memoryPressure(): { used: number; limit: number; ratio: number }
```

### 5. `ctx.storage.put()` error surfacing ✅

**Resolved in v0.12.0** (PR #197) — `file_write_ack` provides end-to-end storage error propagation. Backend write failures reject the `storage.put()` promise. Validation errors remain fatal stream errors.

**Severity:** Medium (needs investigation)
**Area:** SDK / Runtime

It is unclear whether `ctx.storage.put()` propagates backend write failures to the script. If the underlying storage write fails (e.g., S3 returns 500), does the promise reject? Or is the error swallowed? Consumer-side fallback patterns suggest storage errors may not be fully surfaced, but this could also be a path-reconstruction issue rather than a write-failure issue. Needs investigation before v1.0.

---

## Release Gates

- [x] All v1.0 blockers resolved (§1–§2 in v0.11.0; §3–§5 in v0.12.0)
- [ ] CI green on main (`task lint`, `task test`, `task build`, `task examples`)
- [ ] Version lockstep passes (types.Version, SDK package.json, CONTRACT_VERSION, tag)
- [ ] Contracts updated for any behavior changes (CONTRACT_EMIT, CONTRACT_IPC, CONTRACT_RUN)
- [ ] PUBLIC_API.md updated for new API surface
- [ ] CHANGELOG.md finalized
- [ ] Golden test fixtures updated
- [ ] Dogfooding re-validated against updated build
- [ ] Release workflow dry-run passes
- [ ] Release notes drafted

---

## Sign-Off

| Role | Name | Date | Approved |
|------|------|------|----------|
| Maintainer | | | |
| Reviewer | | | |
