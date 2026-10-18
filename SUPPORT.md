# Support Posture — Quarry v0.13.5

This document defines support expectations for Quarry v0.13.5.

---

## Maturity Level

**v0.13.5 is an early release.** APIs and behaviors may change in subsequent
minor versions. Breaking changes will be documented in release notes.

---

## Known Issues

_No known issues in v0.13.5._

---

## What Is Supported

### Supported Components

| Component | Support Level |
|-----------|---------------|
| `quarry run` command | Supported |
| `quarry inspect` command | Supported |
| `quarry stats` command | Supported |
| Node.js executor | Supported |
| Filesystem storage backend | Supported |
| S3 storage backend | Supported |
| SDK emit API | Supported |
| Lifecycle hooks (`prepare`, `beforeTerminal`) | Supported |
| Fan-out (`--depth`) | Supported |
| Storage batching (`createStorageBatcher`) | Supported |
| Memory pressure API (`ctx.memory`) | Supported |
| Structured exit reporting (`--report`) | Supported |
| Script validation (`--dry-run`) | Supported |

### Supported Platforms

| Platform | Status |
|----------|--------|
| Linux (x64, arm64) | Supported |
| macOS (x64, arm64) | Supported |
| Container (GHCR) | Supported |
| Windows | Not tested |

### Supported Runtimes

| Runtime | Version | Notes |
|---------|---------|-------|
| Go | 1.25.6 | Exact version |
| Node.js | 23+ | Native TS support |
| Node.js | 22.6+ | Requires `--experimental-strip-types` |

### Required Tooling

| Tool | Version | Notes |
|------|---------|-------|
| pnpm | 10.28.2 | Exact version |

---

## What Is Not Supported

### Explicit Non-Goals

- **Job scheduling**: Quarry is an execution runtime, not a scheduler
- **Automatic retries**: Retry logic is the caller's responsibility
- **Streaming reads**: Artifacts must fit in memory
- **Headless-only mode**: Scripts require Puppeteer (browser context)
- **Alternative executors**: Only Node.js executor is supported

### Experimental Features

The following are available but not production-hardened:

- **Proxy rotation with pluggable recency backend**: In-memory recency window is supported; cross-process recency stores are not yet available

---

## Bug Reports

Report bugs via GitHub Issues:
- https://github.com/pithecene-io/quarry/issues

### Issue Template

When reporting issues, include:

1. **Quarry version**: Output of `quarry version`
2. **Environment**: OS, Node version, Go version
3. **Reproduction steps**: Minimal script and command
4. **Expected behavior**: What should happen
5. **Actual behavior**: What happens instead
6. **Logs**: Relevant error output (sanitize credentials)

---

## Issue Severity

| Severity | Description | Response |
|----------|-------------|----------|
| P0 (Critical) | Data loss, security issue | Hotfix release |
| P1 (High) | Core functionality broken | Next minor release |
| P2 (Medium) | Non-critical bug | Backlog |
| P3 (Low) | Enhancement request | Backlog |

---

## Security

Report security vulnerabilities privately:
- Email: security@pithecene-io.com (if applicable)
- Do **not** file security issues publicly

---

## Deprecation Policy

- Deprecated features are documented in release notes
- Deprecated features remain functional for at least one minor version
- Removed features are documented in the changelog

---

## Version Compatibility

Quarry uses lockstep versioning:
- SDK version must match runtime version
- Mismatched versions produce a contract version error

Check versions:
```bash
quarry version
# 0.13.5 (commit: ...)
```

---

## No Warranty

Quarry v0.13.5 is provided "as is" without warranty of any kind.
See LICENSE for details.
