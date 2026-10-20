# Repository Constitution

## 1. Constitutional Order of Authority

1. Global dotfiles CLAUDE.md (sovereign)
2. This repository CLAUDE.md
3. AGENTS.md (behavioral expectations and guardrails)
4. docs/ARCH_INDEX.md (structural ontology)
5. docs/contracts/CONTRACT_*.md (normative behavior)
6. PUBLIC_API.md (public surface contract)

## 2. Role of AGENTS.md

AGENTS.md provides:
- Development guardrails and agent constraints
- Scope discipline and abstraction rules
- Go-specific coding conventions
- TypeScript/ESM-specific coding conventions
- Stream & EventEmitter discipline
- Proxy discipline

AGENTS.md does NOT:
- Override constitutional structural rules
- Define enforcement policy
- Weaken global prohibitions

## 3. Role of docs/ARCH_INDEX.md

ARCH_INDEX.md defines:
- Repository subsystem map
- Module responsibilities
- Directory semantics

ARCH_INDEX.md is authoritative for:
- Top-level module declarations
- Contract-to-directory mapping
- Architectural boundaries

## 4. Role of Contracts

docs/contracts/CONTRACT_*.md files define normative system behavior.
Contracts are authoritative over code.

## 5. Structural Invariants

Required top-level directories:
- `quarry/` — Go module root (runtime, CLI, core types)
- `sdk/` — public TypeScript SDK
- `executor-node/` — Node.js executor implementation
- `examples/` — usage and integration references
- `docs/` — guides, contracts, and plans
- `scripts/` — developer tooling and test harnesses

Required docs structure:
- [`docs/ARCH_INDEX.md`](docs/ARCH_INDEX.md) — subsystem navigation
- [`docs/contracts/`](docs/contracts/) — normative behavior contracts
- [`docs/guides/`](docs/guides/) — user-facing documentation

Required root files:
- [`AGENTS.md`](AGENTS.md) — agent guardrails
- [`PUBLIC_API.md`](PUBLIC_API.md) — public API contract
- [`Taskfile.yaml`](Taskfile.yaml) — task orchestration

Forbidden patterns:
- No orphan top-level directories without ARCH_INDEX entry
- No duplicate module responsibilities across directories
- No cross-language imports (Go/TypeScript boundary enforced by IPC)
- No public SDK types or functions outside `sdk/`
- No public Go API types outside `quarry/`
- No contracts outside [`docs/contracts/`](docs/contracts/)
