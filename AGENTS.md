# AGENTS.md — Quarry Guardrails

This file defines **non-negotiable guardrails** for working on Quarry.
It encodes *discipline and constraints*, not architecture.

---

## Core Principles

- Prefer **clarity over cleverness**
- Favor **explicit behavior** over implicit magic
- Keep abstractions **shallow and inspectable**
- Optimize for **debuggability and observability**, not elegance

---

## Scope Discipline

Agents must not:
- Invent new features unless explicitly requested
- Redesign core abstractions unprompted
- Introduce DSLs, frameworks, or configuration layers
- Optimize for scale or performance without evidence

If scope feels ambiguous or expanding, **pause and ask**.

---

## Change Discipline

- API changes are expensive; internal refactors are cheap
- Behavior changes must be observable (logs, events, metrics)
- Avoid silent fallbacks, hidden retries, or implicit recovery

---

## Structural Rules

- Small, single-purpose modules
- No premature generalization
- No “utility” dumping grounds
- Separate concerns explicitly:
  - execution vs orchestration
  - ingestion vs persistence
  - policy vs mechanism

## Stream & EventEmitter Discipline

- **Never** use `Object.create()` or prototype delegation on `EventEmitter` instances or Node.js streams.
  Prototype-chained `_events` diverge silently after listener cleanup cycles, breaking event delivery.
- Compose (separate objects for separate concerns) instead of delegating via prototype.
- If a stream's `.write()` must be intercepted, pass the original write function separately — do not create proxy objects.
- Every IPC integration test must have an explicit timeout. A test that hangs is a deadlock, not a slow test.

## Proxy Discipline

- Proxy selection is runtime-owned; executor only applies resolved endpoints.
- No provider-specific proxy code in executor or SDK.

---

## Go Rules

- Prefer `any` over `interface{}`
- Prefer `errors.New` over `fmt.Errorf` when no formatting verbs are needed
- Prefer iterators (`range`/`yield`-based or `iter.Seq`) over building intermediate slices, where appropriate
- Use `iox.DiscardClose` (not `_ = x.Close()`) when close errors are unactionable

---

## TypeScript Rules (Strict)

Quarry is **TypeScript-first, ESM-only, modern by default**.

### Language & Modules
- `.ts` only
- ESM only (`import` / `export`)
- No `require`, `module`, `exports`
- Import specifier rules (by package type):
  - **Bundled packages** (e.g. `sdk/`): extensionless imports, no `.js` specifiers
  - **Node ESM packages** (e.g. `executor-node/`): `.js` specifiers required for relative imports after TS compilation
- ES2022+ semantics

### Types
- Assume `strict: true`
- No `any` (use `unknown` + narrowing)
- Explicit return types on exported functions
- Prefer `type` aliases over `interface`
- Model domain concepts with types, not comments

### Imports & Exports
- Prefer named exports
- Default exports only for true entrypoints
- Import order:
  1. Node built-ins
  2. External deps
  3. Internal modules

### Files & Layout
- `kebab-case.ts`
- One primary concept per file
- Prefer small, composable modules

---

## Code Style & Composition

- **Declarative over imperative**: extract repeated imperative patterns into named helpers
- **Group code at similar abstraction levels**: high-level orchestration should not inline low-level mechanics
- **Extract when a pattern repeats ≥ 3 times**: error formatting, validation, resource cleanup, etc.
- **Wrap mutable or chainable behavior**: use return structs, factory functions, or monadic interfaces so callers stay declarative
- **Validation factories over object literals**: when constructing many similar objects (errors, warnings, records), extract a factory function
- **Functional iteration over imperative loops**: prefer `map`/`flatMap`/`filter` (TypeScript) or table-driven patterns (Go) over index-based loops when the intent is transformation
- **One level of nesting per function**: if a function has nested try/catch or if/else, consider extracting the inner block
- **Helpers are file-local unless shared**: do not export helpers that serve only one call site

---

## Control Flow & Async

- Pure functions where practical (not a hard requirement)
- Expression-oriented code
- Early returns over nesting
- No implicit mutation unless justified
- `async` / `await` only
- No floating promises
- Errors must be handled or propagated explicitly

---

## Errors

- Prefer explicit error types or result objects
- If throwing:
  - Throw `Error` subclasses only
  - Never throw strings

---

## Testing

- Use `t.Context()` instead of `context.Background()` in tests
- Use `errors.Is()` for error comparisons, not `==`

---

## Formatting & Comments

- Formatting is automated; do not hand-format
- Comments explain **why**, not **what**
- No commented-out code
- TSDoc only for exported APIs

---

## Barrel File Policy

- **Allowed**: One public entrypoint per package (e.g. `src/index.ts`)
- **Forbidden in SDK**: Internal barrel files (all exports in public entrypoint)
- **Allowed in internal packages** (e.g. `executor-node/`): Internal entrypoints for subsystems (e.g. `src/ipc/index.ts`)
- Public entrypoints must import directly from source files

---

## Version Policy

Quarry uses **lockstep versioning**:

- All components share a single canonical version: `quarry/types/version.go`
- SDK package version (`sdk/package.json`) must match `types.Version`
- Contract versions (`CONTRACT_VERSION` in SDK) are derived from this
- CLI `--version` output must match `types.Version`

When releasing:
1. Run `task version:bump V=X.Y.Z` — updates all lockstep targets and rebuilds artifacts
2. Promote `CHANGELOG.md`: move `[Unreleased]` entries to a dated `[X.Y.Z] - YYYY-MM-DD` section, add the `[X.Y.Z]` link reference at the bottom, and restore an empty `[Unreleased]` placeholder
3. Commit as a single version bump

**Do not skip steps. Do not split across commits.**

Every place the version is written down is listed once, in
`scripts/version-targets.txt`. `task version:bump` rewrites each target by
pattern, rebuilds the SDK and executor bundle, then runs
`task version:check`, which CI also runs: a target that matches nothing or
carries another version fails the build. Never hand-edit a version string;
if a new file must print the version, add it to the targets list.

Releases are monotonic: the release workflow rejects a tag lower than any
released tag before publishing anything, so npm `latest` and GHCR `:latest`
/ `:slim` only move forward. Re-running a release is safe (publishing skips
versions that already exist). Republishing an older version is a deliberate
manual operation, never a tag push. Release-workflow logic lives in
`scripts/release/` with tests, not inline in workflow YAML.

---

## Litmus Test

Before adding code, ask:

> Does this make the system easier to reason about for a future reader?

If not, reconsider.

## Agent Implementation Procedure

When given a task:

1. Read this file (`AGENTS.md`) in full.
2. Read only the files explicitly referenced by the task.
3. Do not infer architecture beyond what is visible in code.
4. Modify only files within the stated scope.
5. Do not introduce new dependencies unless explicitly requested.
6. Preserve existing public APIs unless the task explicitly permits changes.
7. Make all behavior changes observable.
8. Follow TypeScript and ESM rules strictly.
9. If an instruction is ambiguous, stop and ask before writing code.
10. If a change feels like scope expansion, stop and surface the concern.
11. Do not refactor unrelated code “for cleanliness.”
12. Output only the requested artifacts (code, diffs, or explanations).

