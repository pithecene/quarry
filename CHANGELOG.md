# Changelog

All notable changes to Quarry will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Security

- **Deps**: Remove the only path to `braces` (Dependabot #59, GHSA-vfj7-8cjw-p6xm, stack-exhaustion DoS, no patched release) by replacing `tsd` with vitest's built-in type testing; the SDK's type-level contract tests now run as `vitest run --typecheck.only` with `expectTypeOf`
- **Deps**: Upgrade puppeteer to 25.14.0 in development dependencies and the container image (Chrome for Testing 155.0.8059.79), resolving the `extract-zip` symlink advisories (Dependabot #47, #53) for the images and the default toolchain: `@puppeteer/browsers` 3.x no longer depends on `extract-zip`. Self-managed installs that stay on puppeteer 24 remain exposed (see `SUPPORT.md` § Open Security Advisories). The SDK and executor peer range widens to `puppeteer ^24 || ^25`, so existing puppeteer 24 installs keep working; puppeteer 25 requires Node 22.12+

---

## [0.13.6] - 2026-10-19

### Changed

- **Docs**: Roadmap added to `IMPLEMENTATION_PLAN.md` — v0.13.6 (Lode v0.10.0), v0.14.0 (read side #240, retention #239, compression) as six decided architecture points (runtime-owned run lifecycle, one key builder and a `runs/` namespace for Quarry-written objects, run records as the run index with an operator-run backfill, record-driven retention, a rewritten CLI read contract, design record on #245), v1.0 (contract freeze), v1.x (first-party `quarry serve` host). Temporal section demoted to a deferred host; `CONTRACT_INTEGRATION.md` orchestration paradigm generalized so the runtime stays host-agnostic; `PUBLIC_API.md` Known Limitation 7 states what reads exist today and points at the planned reader
- **Deps**: Upgrade `github.com/pithecene-io/lode` to v0.10.0. Additive listing API — sorted listings on every store, `PartitionListOptions.Prefix`, `StartAfter` cursors for dataset/partition/manifest listings, `ErrPartitionPrefixNotSupported`, `ErrInvalidArgument` for refused option values — recorded in `CONTRACT_LODE.md` § Listing Order and Cursors as the basis for the v0.14 read side. Error message text for invalid arguments/paths and nested manifest read failures changed upstream; Quarry matches on sentinels, so no behavior change. Upstream fixes carried: all-or-nothing filesystem `Put`, stream writes no longer hang when `Put` fails fast, filesystem `ReadRange` no longer allocates the full requested length, filesystem `List` matches string prefixes like memory/S3

---

## [0.13.5] - 2026-10-18

### Breaking Changes

- **Packaging**: The GitHub organization was renamed `pithecene-io` → `pithecene`. GitHub Packages and GHCR require the scope/owner to match, so the first v0.13.5 release run could not publish the npm package or the container images; the `v0.13.5` tag was re-pointed at the commit carrying the rename and the release re-run (#246). The SDK is `@pithecene/quarry-sdk` on npm (GitHub Packages) and JSR — update imports from `@pithecene-io/quarry-sdk` — and container images are `ghcr.io/pithecene/quarry` (`:latest`, `:slim`, versioned tags). `mise install github:pithecene/quarry`. The Go module path `github.com/pithecene-io/quarry` is unchanged

### Added

- **Metrics**: `lode_write_retry_total` is now populated. Quarry registers Lode's `WithRetryObserver` on the write path, so each CAS retry increments the counter; the CLI no longer prints it as "(not implemented)" and `CONTRACT_LODE.md` no longer marks it reserved

### Security

- **Deps**: Resolve open Dependabot alerts in the JS toolchain — refresh dev dependencies within range (vitest 4.1.12, puppeteer 24.43.1), add explicit `vite@^7.3.7` dev dependency (auto-installed vitest peer ignored overrides), bump executor `esbuild` to `^0.28.2`, and override `esbuild` → 0.28.2 and `basic-ftp` → 6.2.3. Patches source-map-js, brace-expansion, ip-address, js-yaml, nanoid, fflate, postcss, ws, vite, vitest, esbuild, and basic-ftp advisories. `extract-zip` (via `@puppeteer/browsers`) has no upstream fix and remains open
- **Container**: The image's `deps` stage installs its own npm tree, independent of `pnpm-lock.yaml`; it now pins `puppeteer@24.43.1` with a matching Chrome for Testing 148.0.7778.97 and restates the `basic-ftp` → 6.2.3 override as an npm root override so the shipped runtime carries the same patched dependencies
- **Deps**: Upgrade `github.com/pithecene-io/lode` to v0.9.1, which bumps `aws-sdk-go-v2/service/s3` and `eventstream` to resolve the EventStream denial-of-service advisory
- **Deps**: Bump `flatted` override to 3.4.2 (prototype pollution) (#238)

### Fixed

- **Storage**: `store.Put()` errors from sidecar file writes are now wrapped by `WrapWriteError` with path context and classified — new `ErrPathExists` sentinel covers immutability violations (`"path exists"`, `"already exists"`, `"file exists"`, `EEXIST`) (#237)
- **CLI**: `QuerySidecarFiles()` unions `sidecar_files` metadata across a run's snapshots; `InspectRunResponse` gains an `omitempty` `SidecarFileSummary` list so `inspect` can enumerate files without prefix-scanning (#237)

---

## [0.13.4] - 2026-03-22

### Added

- **Storage**: Sidecar file metadata inventory — files written via `storage.put()` are now tracked in Lode snapshot `Metadata` under the `sidecar_files` key. Downstream consumers can enumerate files for a run via `snapshot.Manifest.Metadata["sidecar_files"]` instead of prefix-scanning storage (#234)
- **Storage**: `SidecarFileRef` type — each tracked file includes `path`, `filename`, `content_type`, and `size` fields
- **Tests**: Lode sentinel error path validation — targeted integration tests for `lode.ErrPathExists` (immutability enforcement on double-write) and `lode.ErrNotFound` (stale snapshot ID), providing evidence for Lode V1_READINESS §5 (#234)
- **Tests**: End-to-end test for `file_write` → event flush → `sidecar_files` metadata pipeline, proving the full ingestion path from IPC frame to consumer-visible snapshot (#235)

### Fixed

- **Storage**: File inventory is only flushed on event and metrics writes (consumer-facing commit boundaries), not on chunk writes. Prevents file refs from being drained into artifact snapshots that consumers don't inspect for metadata (#234)

---

## [0.13.3] - 2026-03-17

### Fixed

- **Security**: Harden CI workflow permissions — add explicit `permissions: contents: read` to `ci.yml`, `nightly.yml`, and `release-dry-run.yml`, enforcing least-privilege GITHUB_TOKEN scope (resolves 19 CodeQL code-scanning alerts) (#232)
- **Security**: Patch vulnerable transitive devDependencies via `pnpm.overrides` — `flatted` 3.4.0 (DoS), `minimatch` 3.1.4 (ReDoS), `rollup` 4.59.0 (path traversal), `basic-ftp` 5.2.0 (path traversal) (#232)

---

## [0.13.2] - 2026-03-14

### Added

- **Runtime**: `QUARRY_BROWSER_ENDPOINT` env var — binds to `--browser-ws-endpoint` flag, enabling container Compose configs to set external browser endpoints via environment instead of mounting YAML config files (#229)
- **Runtime**: Pre-run browser health gate — verifies external browser reachability via `/json/version` before launching the executor; warns on failure but does not block the run, preserving compatibility with tokenized and path-prefixed browser services (#229)
- **Build**: `task version:bump V=X.Y.Z` — single command updates all lockstep version targets, rebuilds SDK and executor bundle, and verifies lockstep (#229)
- **Docs**: Multi-crawler deployment topology and browser endpoint documentation in container guide (#229)

### Fixed

- **Runtime**: Browser health gate no longer hard-fails on external WS endpoints with non-root paths or query parameters — downgraded from fatal to warning to avoid false-rejecting tokenized browser pool services
- **Build**: Release tarball now includes built SDK — added `pnpm run build` before `pnpm pack` in release workflow (#228)
- **Build**: `scripts/version-bump.sh` and `Taskfile.yaml` version tasks now use portable sed/grep — replaces GNU-specific `grep -P` and `sed -i` with POSIX-compatible alternatives for macOS support

### Changed

- **CI**: New `sdk-lockstep` job verifies built `CONTRACT_VERSION` matches source at PR time (#228)
- **Docs**: Updated cli.md, configuration.md, CONTRACT_CLI.md, PUBLIC_API.md, CLI_PARITY.json with browser endpoint and health gate semantics (#229)

---

## [0.13.1] - 2026-03-13

### Fixed

- **Build**: Embed git commit SHA in binary via ldflags — `quarry --version` now shows the short commit hash instead of `unknown` across all build paths (Taskfile, CI release, Dockerfile) (#225)
- **Docs**: Sweep stale v0.12.2 version references in README.md, SUPPORT.md, and IMPLEMENTATION_PLAN.md that were missed in the v0.13.0 release
- **Tests**: Update hardcoded `ContractVersion` in Redis Streams adapter test fixtures to match current release

### Changed

- **Build**: Add `docker:build` Taskfile task — local Docker builds now automatically embed the commit SHA via `--build-arg`
- **Build**: Extract `COMMIT` var in Taskfile — single resolution point for all Go and Docker build paths

---

## [0.13.0] - 2026-03-12

### Added

- **Integration**: Pluggable Redis Streams event sink — publishes every event in real time via `XADD` during run execution, enabling downstream consumers to watch events across all runs on a single stream (#222, #223)
- **CLI**: `--event-sink` repeatable flag to activate event sinks (`lode`, `redis`); multiple sinks fan out with independent delivery semantics (#223)
- **CLI**: Redis Streams event sink flags: `--event-sink-redis-url`, `--event-sink-redis-stream-key`, `--event-sink-redis-max-len`, `--event-sink-redis-ttl`, `--event-sink-redis-timeout`, `--event-sink-redis-retries`, `--event-sink-redis-delivery` (#223)
- **Runtime**: Fan-out event sink with per-sink delivery classification — `mandatory` sinks fail the run on error; `best_effort` sinks log warnings and continue (#223)
- **Contracts**: CONTRACT_INTEGRATION.md updated with Event Sink Model (scope, delivery semantics, failure behavior, fan-out inheritance) (#223)
- **Contracts**: CONTRACT_CLI.md updated with event sink flag specifications (#223)
- **Docs**: Integration guide updated with Redis Streams event sink section, comparison table, and YAML config example (#223)
- **Docs**: Configuration guide updated with event sink flags and YAML schema (#223)

### Changed

- **Docs**: PUBLIC_API.md updated with event sink flags and downstream integration references (#223)

---

## [0.12.2] - 2026-03-11

### Changed

- **Executor**: Annotate bare JSON import errors with actionable hint — Node ESM requires `with { type: 'json' }` but many runtimes accept bare imports; `loadScript()` now appends a clear explanation and fix (#219, #218)

### Fixed

- **Docs**: Sweep stale version references remaining after v0.12.1 (#217)

---

## [0.12.1] - 2026-03-08

### Changed

- **Lode**: Upgraded Lode dependency from v0.8.0 to v0.9.0 — adds opt-in CAS retry on snapshot conflict (#215)
- **Lode**: Write-path datasets now configured with `WithRetryCount(3)` — concurrent runs to the same partition retry up to 3 times with jittered exponential backoff instead of failing terminally (#215, #213)
- **IPC**: Reduced IPC overhead and eliminated batcher busy-wait spin loop (#205)
- **Internal**: Extracted `iox` package for deferred-close resource cleanup helpers (#209)

### Fixed

- **CI**: Release workflow preserves existing release notes (#200)
- **CI**: JSR publish step retries on transient failures (#201)

---

## [0.12.0] - 2026-02-24

### Added

- **SDK**: `createStorageBatcher` utility for bounded-concurrency `storage.put()` pipelining — wraps serial puts with configurable concurrency (default 16), fail-fast semantics, and `flush()` drain (#196)
- **SDK**: `ctx.memory` API — `snapshot()` returns heap, browser, and cgroup memory usage with composite pressure level; `isAbove(level)` provides boolean threshold check; cgroup v2/v1 auto-detection (#196)
- **IPC**: `file_write_ack` frame for end-to-end storage error propagation — first bidirectional IPC frame; backend write failures reject the `storage.put()` promise as recoverable errors (#197)

### Changed

- **Lode**: Upgraded Lode dependency from v0.7.3 to v0.8.0 (#196)

---

## [0.11.0] - 2026-02-23

### Added

- **CLI**: `--dry-run` flag for script validation without execution (#191)
- **CLI**: `--report` flag for structured exit reporting — writes JSON report to file on exit (#189)
- **SDK**: `storage.put()` now returns `StoragePutResult` with `key` field containing the storage path (#190)

---

## [0.10.0] - 2026-02-14

### Added

- **Executor**: `--resolve-from` CLI flag for workspace module resolution — registers an ESM resolve hook that retries bare-specifier resolution from a specified directory, preserving ESM import conditions (#178)
- **Executor**: `registerResolveFromHook()` extracted into `src/resolve-from.ts` — centralizes hook code to prevent copy drift between executor, test fixture, and unit tests

### Fixed

- **Runtime**: Detect zombie/dead browser server processes before attempting health check — avoids wasting 2s on a connection to a dead process; kill stale process groups only after verifying PID ownership via `/proc` cmdline check (#181, #182)
- **Executor**: Add `--disable-dev-shm-usage` to all Chromium launch paths — prevents renderer crashes from `/dev/shm` exhaustion in containerized environments (#183, #185)
- **Executor**: Tolerate transient `/json/list` polling failures in browser server idle monitor — requires 3 consecutive failures before exit instead of crashing on the first; non-OK HTTP responses now correctly count as failures (#183, #185)

### Changed

- **Testing**: Integration test fixture now imports production hook module instead of embedding a copy
- **Testing**: Resolve-from integration test uses unique OS temp directories (`mkdtempSync`) to prevent cross-run interference
- **Executor**: Chromium launch args centralized into `chromiumArgs()` helper (`browser-args.ts`); idle poll evaluation extracted into pure `evaluateIdlePoll()` function (`browser-idle.ts`) with 18 unit tests

---

## [0.9.0] - 2026-02-13

### Added

- **SDK**: `prepare` lifecycle hook — transform or filter job payloads before browser launch; return `{ action: 'skip' }` to short-circuit a run without acquiring a browser (#175)
- **SDK**: `beforeTerminal` lifecycle hook — outcome-aware hook that fires after script execution but before the terminal event, with emit still open; useful for emitting summary items or final metadata (#175)
- **SDK**: `PrepareResult`, `PrepareHook`, `TerminalSignal`, `BeforeTerminalHook` type exports (#175)
- **Executor**: Malformed `prepare` return validation — null, undefined, non-object, or missing `action` returns produce a diagnostic crash instead of an unhandled TypeError (#175)
- **Docs**: Lifecycle hooks documented in PUBLIC_API.md, run guide, and emit guide (#175)
- **Docs**: `CONTRACT_RUN.md` updated with lifecycle hook execution model and `prepare`-skip early return semantics
- **Docs**: `CONTRACT_EMIT.md` updated with `skipped` and `reason` fields on `run_complete` summary
- **Examples**: `hooks-prepare` and `hooks-before-terminal` runnable examples (#175)
- **Testing**: 24 new executor tests and 7 loader validation tests for lifecycle hooks (#175)

---

## [0.8.0] - 2026-02-13

### Added

- **SDK**: `createBatcher` utility for batching `emit.enqueue()` calls — accumulates items and emits fewer, larger enqueue events with `params.items`, reducing child run count for high fan-out workloads (#170)

### Changed

- **Container**: Go module layer split and multi-stage cache mounts for faster image builds (#167)
- **CI**: Container build added to release dry-run workflow (#168)

---

## [0.7.3] - 2026-02-12

### Changed

- **Lode**: Upgrade Lode storage library to v0.7.3

### Fixed

- **IPC**: Eliminate stdout guard proxy to fix drain deadlock — `Object.create(process.stdout)` caused `_events` divergence after listener cleanup, silently breaking drain delivery and hanging the executor under backpressure; replaced with split contract (separate stream + writeFn) (#164)

### Added

- **IPC**: Backpressure integration test that deterministically proves drain path is exercised — spawns child, withholds parent reads until pipe fills, asserts `backpressure_events > 0` (#164)
- **AGENTS.md**: Stream & EventEmitter discipline guardrail — bans `Object.create()` on EventEmitter instances, requires composition over prototype delegation (#164)

---

## [0.7.2] - 2026-02-11

### Changed

- **Container**: Full image uses Chrome for Testing tarball instead of distro Chromium — pins browser version to puppeteer compatibility matrix (#159)
- **Container**: Full image is now **amd64-only** — Chrome for Testing does not publish linux-arm64 builds; arm64 users should use the slim image with an external browser sidecar (#159)

### Fixed

- **IPC**: Protect IPC channel from stray stdout writes — third-party code (puppeteer-extra, stealth plugin) writing to `process.stdout` caused the Go `FrameDecoder` to interpret text as a length prefix, deriving huge payload sizes and blocking indefinitely; stdout guard now captures the real `write` for IPC and redirects stray writes to stderr with a diagnostic warning (#161)
- **Container**: Retain `ca-certificates` in full runtime image — previous purge of build-time download tools accidentally removed the TLS trust store, breaking HTTPS at runtime (#160)
- **Container**: Add `libasound2` to Chrome shared-library set — missing ALSA dependency could cause Chrome to fail at dynamic link time (#160)
- **Container**: Fail fast when building full image on non-amd64 — prevents silent production of images with a wrong-architecture Chrome binary (#160)

---

## [0.7.1] - 2026-02-11

### Added

- **Distribution**: Containerized distribution via GHCR — `ghcr.io/pithecene-io/quarry:0.7.1` (full, with system Chromium + fonts) and `ghcr.io/pithecene-io/quarry:0.7.1-slim` (no browser, BYO Chromium via `--browser-ws-endpoint`) (#152)
- **CI**: Dockerfile lint check (`docker build --check`) in PR and main CI workflows (#152)
- **Testing**: Integration test for terminal frame visibility at pipe boundary — pins the stdout flush race that caused false `executor_crash` classification (#153)

### Fixed

- **Executor**: Drain stdout before `process.exit()` to prevent false `executor_crash` — `process.exit()` was discarding buffered stdout data before it reached the OS pipe, causing the Go runtime to see EOF without a terminal event and classify successful runs as `executor_crash` (#153)

---

## [0.7.0] - 2026-02-10

### Added

- **Runtime**: Transparent browser reuse across `quarry run` invocations — Chromium persists as a detached process and auto-terminates after idle timeout, eliminating cold-start cost for sequential runs (#149)
- **CLI**: `--no-browser-reuse` flag and `no_browser_reuse` config option to disable transparent browser reuse (#149)
- **Runtime**: `QUARRY_BROWSER_IDLE_TIMEOUT` environment variable for configuring browser server idle timeout (default 60s) (#149)
- **Executor**: `--browser-server` mode — self-managing browser server with idle monitoring via `/json/list` polling and graceful shutdown (#149)
- **Policy**: `streaming` ingestion policy — combines strict's no-drop guarantee with buffered's batched write efficiency for long-running crawl workloads (#140)
- **Policy**: Configurable flush triggers: `--flush-count` (event count threshold) and `--flush-interval` (time-based periodic flush) (#140)
- **Policy**: Capacity-triggered emergency flush prevents deadlock when internal buffer bounds are reached before configured triggers fire (#140)
- **Policy**: Backpressure-based flow control — ingestion blocks when buffer is full rather than dropping events (#140)
- **Proxy**: `recency_window` pool option — maintains a ring buffer of recently-used endpoint indices and excludes them from random selection, reducing endpoint reuse (#143)
- **Proxy**: LRU fallback when recency window >= endpoint count (selection never blocks) (#143)
- **Testing**: E2E browser reuse tests gated behind `-e2e` flag for mutual test exclusivity (#149)
- **Testing**: Performance benchmarks for all ingestion policies (strict, buffered, streaming) with contention analysis (#144)
- **Infra**: yamllint added to lint pipeline via project-scoped mise (#149)
- **Docs**: Streaming policy contract section in CONTRACT_POLICY.md (#139)

### Changed

- **Metrics**: `flush_triggers` counter added to metrics snapshot — per-trigger-type flush counts (`count`, `interval`, `termination`, `capacity`) for streaming policy observability (#140)
- **Internal**: `statsRecorder` converted from mutex-based to `sync/atomic` counters — 3.7x improvement in 8-goroutine contention benchmarks (#144)
- **AGENTS.md**: Release checklist expanded with changelog promotion and golden fixture update steps (#148)

---

## [0.6.3] - 2026-02-09

### Fixed

- **Runtime**: SDK/CLI version mismatches are now classified as `version_mismatch` instead of `executor_crash`, with an actionable error message directing users to align their SDK and CLI versions (#132)
- **Lode**: Upgraded to Lode v0.7.1 — fixes O(n²) flush performance caused by linear snapshot ID lookups in `dataset.Write()` (pithecene-io/lode#108)

### Changed

- **Runtime**: Version mismatch errors increment `run_failed` (not `run_crashed`) and do not fire `executor_crash` metric (#132)
- **CLI**: `version_mismatch` outcome maps to exit code 3 (non-retryable, same as `policy_failure`) (#132)
- **Contracts**: `CONTRACT_RUN.md` and `CONTRACT_INTEGRATION.md` now enumerate `version_mismatch` as a fifth outcome status (#132)
- **Infra**: Migrated npm/JSR scope from `@justapithecus` to `@pithecene-io` (#137)
- **Infra**: Migrated Go module path from `github.com/justapithecus/quarry` to `github.com/pithecene-io/quarry` (#137)
- **Lode**: Upgraded to Lode v0.7.2 (`github.com/pithecene-io/lode`) (#137)

---

## [0.6.2] - 2026-02-08

### Added

- **CLI**: `--browser-ws-endpoint` flag — connect to an externally managed browser instead of launching Chromium per run, eliminating ~180x cold-start overhead for multi-run workloads (#127)
- **Emit**: Per-enqueue `source` and `category` partition overrides — child runs can target different Lode partitions via `emit.enqueue({ source, category })` (#129)
- **Docs**: Browser reuse documented in CLI guide, configuration reference, and PUBLIC_API.md (#130)
- **Docs**: Enqueue partition overrides documented in CONTRACT_EMIT.md and emit guide (#130)

### Changed

- **Runtime**: `storage.put()` now **fails fast** when FileWriter is not configured — previously logged a warning and silently discarded data (#128)
- **Runtime**: Metrics persistence timeout increased from 10s to 30s to match policy flush timeout (#128)
- **Docs**: Fan-out support level downgraded from `Supported` to `Experimental` in SUPPORT.md (#130)

### Upgrade Notes

- **Breaking**: `storage.put()` now returns an error instead of silently discarding data when storage is not configured. Scripts using `storage.put()` must ensure `--storage-backend` and `--storage-path` are set. Previously, misconfigured runs would silently lose sidecar files.

---

## [0.6.0] - 2026-02-08

### Added

- **Runtime**: Fan-out operator for derived work execution — `emit.enqueue()` events now trigger child runs at runtime, enabling discovery-driven extraction chains (list → detail → asset) without external orchestrators (#117)
- **CLI**: `--depth` flag to set maximum fan-out recursion depth (root = depth 0); required for fan-out activation (#117)
- **CLI**: `--max-runs` flag to cap total child runs (required when `--depth > 0` as a safety rail against unbounded fan-out) (#117)
- **CLI**: `--parallel` flag to control concurrent child run execution (default: 1, sequential) (#117)
- **Docs**: Derived work execution design replacing crawl mode concept (#116)
- **Docs**: Ingress models exploration document (#115)
- **Docs**: Temporal orchestration integration and module split documentation (#114)

### Changed

- **Lode**: Decoupled Lode from `cli/reader` dependency — cleaner module boundaries (#113)

---

## [0.5.1] - 2026-02-08

### Fixed

- **Release**: Published npm packages on GitHub Packages now include `dist/` directory — previous releases (v0.4.1, v0.5.0) shipped empty tarballs with no type declarations or runtime code (#110, #111)

---

## [0.5.0] - 2026-02-07

### Added

- **Adapter**: Redis pub/sub adapter — publishes `RunCompletedEvent` as JSON to a configurable Redis channel after run completion (#107)
- **Adapter**: `--adapter-channel` CLI flag for Redis pub/sub channel name (default: `quarry:run_completed`) (#107)
- **Adapter**: Redis adapter config in YAML: `adapter.channel` field (#107)
- **Adapter**: Webhook adapter — HTTP POST with retries, custom headers, timeout (#103)
- **Adapter**: `Adapter` interface and `RunCompletedEvent` type in `quarry/adapter/` (#103)
- **Adapter**: CLI flags: `--adapter`, `--adapter-url`, `--adapter-header`, `--adapter-timeout`, `--adapter-retries` (#103)
- **Contracts**: CONTRACT_INTEGRATION.md updated with runtime adapter reference, Redis and webhook (#103, #107)
- **Contracts**: CONTRACT_CLI.md updated with adapter flags including `--adapter-channel` (#103, #107)
- **Docs**: Integration guide updated with webhook and Redis adapter examples (#103, #107)
- **Docs**: Configuration guide updated with adapter section (#107)

### Changed

- **Docs**: CLI_PARITY.json updated with adapter flags and validation (#103, #107)

---

## [0.4.1] - 2026-02-07

### Added

- **Config**: `--config` flag for YAML project-level defaults — reduces CLI flag repetition across invocations (#104)
- **Config**: `${VAR}` and `${VAR:-default}` environment variable expansion in YAML config files (#104)
- **Config**: Inline proxy pool definitions via `proxies:` key in YAML config, replacing deprecated `--proxy-config` JSON file (#104)
- **Config**: Unknown YAML keys rejected via `KnownFields(true)` to catch typos early (#104)
- **Testing**: Hardened config edge cases — env expansion boundaries, whitespace/comments-only YAML, Duration validation, retries nil vs zero

### Changed

- **CLI**: `--source`, `--storage-backend`, `--storage-path` no longer require CLI flags when provided via `--config` file (#104)
- **CLI**: `--proxy-config` deprecated in favor of `proxies:` key in YAML config (#104)
- **Docs**: Removed premature adapter schema from configuration guide
- **Docs**: Clarified proxy job-level selection as CLI flags, not YAML config keys

### Fixed

- **Runtime**: Adapter config in YAML now emits a warning instead of being silently ignored

---

## [0.4.0] - 2026-02-07

### Added

- **Storage**: `ctx.storage.put()` — sidecar file uploads via Lode Store, enabling scripts to write files directly to addressable storage paths outside the event pipeline (#99)
- **IPC**: New `file_write` IPC frame type for executor→runtime file transfer (#99)
- **SDK**: `StorageAPI` on `QuarryContext` — `put({ filename, content_type, data })` (#99)
- **Storage**: Content type persistence via companion `.meta.json` files (#99)
- **SDK**: Terminal guard enforcement on `storage.put()` — writes rejected after `run_complete` / `run_error` (#99)
- **Contracts**: Updated CONTRACT_IPC.md and CONTRACT_EMIT.md for storage mechanics (#99)

---

## [0.3.5] - 2026-02-07

### Added

- **CLI**: `--storage-dataset` flag to override Lode dataset ID (#98)
- **Docs**: Recency window contract documentation and v0.4.0 roadmap (#95)

---

## [0.3.4] - 2026-02-07

### Fixed

- **Lode**: Fixed nil-map panic in S3 client constructor — `NewLodeS3Client` failed to initialize artifact offset and chunk-tracking maps, causing runtime panic on first `WriteChunks` call (#92)

### Changed

- **Lode**: Extracted shared `newClient()` constructor helper to prevent future initialization drift between FS and S3 client paths (#92)

### Added

- **Testing**: Regression test `TestNewClient_InitializesMaps` guards against nil-map constructor bugs (#92)

---

## [0.3.3] - 2026-02-07

### Added

- **Storage**: S3-compatible provider support — new `--storage-endpoint` and `--storage-s3-path-style` CLI flags for Cloudflare R2, MinIO, and other S3-compatible backends (#87)
- **Docs**: S3-compatible storage flags and R2 examples added to cli.md, configuration.md, and PUBLIC_API.md (#89)
- **Docs**: S3-compatible provider support section added to CONTRACT_LODE.md (#87)

### Fixed

- **Release**: JSR now publishes TypeScript source instead of compiled dist, preserving full type information (#88)
- **CLI**: Fixed duplicate run-id in usage text (#89)

---

## [0.3.2] - 2026-02-07

### Added

- **Release**: Cross-compiled platform binaries (linux/amd64, linux/arm64, darwin/amd64, darwin/arm64) included in GitHub Releases (#82)
- **Release**: mise install support — `mise install github:pithecene-io/quarry@<version>` (#82)
- **Release**: JSR publishing via OIDC (zero-secret) — `npx jsr add @pithecene-io/quarry-sdk` (#83)
- **Release**: Dual SDK distribution on JSR (public) and GitHub Packages (restricted) (#82, #83)
- **Release**: Checksums manifest (`checksums.txt`) included in every release (#82)
- **Docs**: Installation docs updated across README, PUBLIC_API.md, and SDK README (#82)

---

## [0.3.1] - 2026-02-06

### Added

- **Executor**: puppeteer-extra support with stealth and adblocker plugins (#75)
  - Stealth enabled by default (`QUARRY_STEALTH=0` to disable)
  - Adblocker opt-in via `QUARRY_ADBLOCKER=1`
  - No-sandbox mode for CI via `QUARRY_NO_SANDBOX=1`
- **Docs**: Consolidated configuration reference (`docs/guides/configuration.md`) (#81)

### Changed

- **Refactor**: Cleanup passes across SDK, executor, and Go runtime — extract helpers, reduce duplication, declarative validation (#76, #77, #78, #79)

### Fixed

- **Build**: Stabilized embedded executor bundle sync (#80)

---

## [0.3.0] - 2026-02-06

### Added

- **Metrics**: Runtime metrics surface per CONTRACT_METRICS.md — run lifecycle, ingestion drops, executor failures, storage write counters (#65)
- **Metrics**: Persist metrics snapshot to Lode at run end as `record_kind=metrics` record (#67)
- **CLI**: `stats metrics` subcommand with Lode-backed reader for querying persisted metrics (#68)
- **Lode**: Policy name recorded in event and artifact commit Lode records (#71)
- **Contracts**: CONTRACT_METRICS.md and CONTRACT_INTEGRATION.md added (#63)
- **Contracts**: Metrics stats persistence requirements defined (#66)
- **Docs**: Lode v0.2.0 → v0.4.1 compatibility guide (`docs/guides/lode-upgrade.md`) (#70)

### Changed

- **Lode**: Upgraded Lode dependency from v0.2.0 to v0.4.1 (#64)
- **Docs**: Phase 6 (dogfooding) clarified as post-release validation exercise (#70)
- **Docs**: v0.3.0 deliverables reworded — dogfooding is a prerequisite, not a gate (#70)

### Fixed

- **Go**: Prefer `errors.New` over `fmt.Errorf` for static error strings (#69)

---

## [0.2.2] - 2026-02-05

### Fixed

- **CI**: Fixed GitHub Packages publish auth failure (ENEEDAUTH) in release workflow. Root cause was missing `actions/setup-node` with registry configuration. Fix adds `packages: write` permission, proper registry-url setup, and `publishConfig` in SDK package.json.
- **CI**: Enforced pinned pnpm via Corepack in publish workflow. Replaced global `npm install -g pnpm` with Corepack-based setup that reads `packageManager` from root package.json. Added version verification step that fails fast on mismatch.

---

## [0.2.1] - 2026-02-05

### Fixed

- **IPC**: Fixed race condition where fast-completing scripts intermittently reported `executor_crash` outcome despite successful completion ([#56](https://github.com/pithecene-io/quarry/issues/56)). Root cause was Go's `exec.Cmd.Wait()` closing stdout pipe before ingestion completed reading all data.

---

## [0.2.0] - 2026-02-05

### Added

- **CLI**: `--job-json <path>` flag to load job payload from file (alternative to inline `--job`)
- **Docs**: Downstream integration guide with event-bus and polling patterns
- **CI**: CLI/config parity checker validates flag documentation against implementation
- **CI**: Strict parity gate blocks merge on CLI documentation drift
- **Examples**: Integration pattern examples (SNS, handler, filesystem polling, S3 polling)

### Changed

- **CLI**: `--job` description clarified as "inline JSON object" to distinguish from `--job-json`
- **CLI**: Job payload (`--job` and `--job-json`) now **requires** a top-level JSON object; arrays, primitives, and null are rejected with actionable error messages
- **CLI**: Normalized help text for job, storage, and policy flags for consistency
- **Docs**: CLI parity artifact (`docs/CLI_PARITY.json`) added as machine-checkable source of truth

### Known Issues

- **IPC race condition** ([#56](https://github.com/pithecene-io/quarry/issues/56)): Fast-completing scripts may intermittently report `executor_crash` outcome despite successful completion. **Fixed in v0.2.1.**

---

## [0.1.0] - 2026-02-04

### Added

- **CLI**: `quarry run` command for executing extraction scripts
- **CLI**: `quarry inspect` command for viewing run data (read-only)
- **CLI**: `quarry stats` command for run statistics
- **SDK**: `@pithecene-io/quarry-sdk` npm package with emit API
- **Emit API**: `emit.item()`, `emit.artifact()`, `emit.checkpoint()`, `emit.log()`
- **Terminal events**: `emit.runComplete()`, `emit.runError()`
- **Storage**: Filesystem backend with Hive-partitioned layout
- **Storage**: S3 backend (experimental, requires explicit opt-in)
- **Policies**: Strict and Buffered ingestion policies
- **Proxy**: Runtime proxy configuration and rotation support
- **Executor**: Embedded Node.js executor in Go binary (~28MB)
- **Contracts**: IPC, Emit, Run, Policy, Lode, CLI, Proxy contracts frozen

### Changed

- N/A (initial release)

### Fixed

- N/A (initial release)

### Breaking Changes

- None (initial release)

### Known Limitations

1. **Single executor type**: Only Node.js executor supported
2. **No built-in retries**: Retry logic is caller's responsibility
3. **No streaming reads**: Artifacts must fit in memory
4. **S3 is experimental**: No transactional guarantees across writes
5. **No job scheduling**: Quarry is an execution runtime, not a scheduler
6. **Puppeteer required**: All scripts run in a browser context

### Upgrade Notes

**Runtime Requirements:**
- Go 1.25.6 or later (for building from source)
- Node.js 23+ or 22.6+ (for script execution; 22.6 requires `--experimental-strip-types`)
- pnpm 10.28.2 (for development)

**Puppeteer:**
- Puppeteer is a peer dependency; install it in your project:
  ```bash
  npm install puppeteer
  ```

**Storage:**
- `--storage-backend` and `--storage-path` are required flags
- FS backend path must exist before running

### References

- [PUBLIC_API.md](PUBLIC_API.md) — User-facing API documentation
- [SUPPORT.md](SUPPORT.md) — Support posture and maturity level
- [docs/contracts/](docs/contracts/) — Normative contract specifications
- [docs/guides/](docs/guides/) — User guides and explanations

---

[Unreleased]: https://github.com/pithecene-io/quarry/compare/v0.13.6...HEAD
[0.13.6]: https://github.com/pithecene-io/quarry/compare/v0.13.5...v0.13.6
[0.13.5]: https://github.com/pithecene-io/quarry/compare/v0.13.4...v0.13.5
[0.13.4]: https://github.com/pithecene-io/quarry/compare/v0.13.3...v0.13.4
[0.13.3]: https://github.com/pithecene-io/quarry/compare/v0.13.2...v0.13.3
[0.13.2]: https://github.com/pithecene-io/quarry/compare/v0.13.1...v0.13.2
[0.13.1]: https://github.com/pithecene-io/quarry/compare/v0.13.0...v0.13.1
[0.13.0]: https://github.com/pithecene-io/quarry/compare/v0.12.2...v0.13.0
[0.12.2]: https://github.com/pithecene-io/quarry/releases/tag/v0.12.2
[0.12.1]: https://github.com/pithecene-io/quarry/releases/tag/v0.12.1
[0.12.0]: https://github.com/pithecene-io/quarry/releases/tag/v0.12.0
[0.11.0]: https://github.com/pithecene-io/quarry/releases/tag/v0.11.0
[0.10.0]: https://github.com/pithecene-io/quarry/releases/tag/v0.10.0
[0.9.0]: https://github.com/pithecene-io/quarry/releases/tag/v0.9.0
[0.8.0]: https://github.com/pithecene-io/quarry/releases/tag/v0.8.0
[0.7.3]: https://github.com/pithecene-io/quarry/releases/tag/v0.7.3
[0.7.2]: https://github.com/pithecene-io/quarry/releases/tag/v0.7.2
[0.7.1]: https://github.com/pithecene-io/quarry/releases/tag/v0.7.1
[0.7.0]: https://github.com/pithecene-io/quarry/releases/tag/v0.7.0
[0.6.3]: https://github.com/pithecene-io/quarry/releases/tag/v0.6.3
[0.6.2]: https://github.com/pithecene-io/quarry/releases/tag/v0.6.2
[0.6.0]: https://github.com/pithecene-io/quarry/releases/tag/v0.6.0
[0.5.1]: https://github.com/pithecene-io/quarry/releases/tag/v0.5.1
[0.5.0]: https://github.com/pithecene-io/quarry/releases/tag/v0.5.0
[0.4.1]: https://github.com/pithecene-io/quarry/releases/tag/v0.4.1
[0.4.0]: https://github.com/pithecene-io/quarry/releases/tag/v0.4.0
[0.3.5]: https://github.com/pithecene-io/quarry/releases/tag/v0.3.5
[0.3.4]: https://github.com/pithecene-io/quarry/releases/tag/v0.3.4
[0.3.3]: https://github.com/pithecene-io/quarry/releases/tag/v0.3.3
[0.3.2]: https://github.com/pithecene-io/quarry/releases/tag/v0.3.2
[0.3.1]: https://github.com/pithecene-io/quarry/releases/tag/v0.3.1
[0.3.0]: https://github.com/pithecene-io/quarry/releases/tag/v0.3.0
[0.2.2]: https://github.com/pithecene-io/quarry/releases/tag/v0.2.2
[0.2.1]: https://github.com/pithecene-io/quarry/releases/tag/v0.2.1
[0.2.0]: https://github.com/pithecene-io/quarry/releases/tag/v0.2.0
[0.1.0]: https://github.com/pithecene-io/quarry/releases/tag/v0.1.0
