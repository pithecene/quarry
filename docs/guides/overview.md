# Quarry Overview

This document is a **user-facing** overview of Quarry.
Normative behavior lives in `docs/contracts/CONTRACT_*.md`.

---

## What Quarry Is

Quarry runs extraction scripts, captures structured outputs, and routes them
through a policy-controlled ingestion layer into storage.

At a high level:
- **SDK**: script authoring surface (`emit.*` and context objects).
- **Executor**: runs scripts and streams events.
- **Runtime**: manages runs, retries, and ingestion policy.
- **Lode**: persistence substrate with stable partitioning.

---

## Core Concepts

- **Job**: logical unit of work requested by a user or scheduler.
- **Run**: a single execution attempt of a job.
- **Attempt**: an integer counter for retries of the same job.
- **Policy**: ingestion behavior (strict, buffered, or streaming).

---

## Where To Start

- Authoring scripts: [`docs/guides/emit.md`](emit.md)
- Lifecycle hooks: [`PUBLIC_API.md`](../../PUBLIC_API.md) (Lifecycle Hooks section)
- Running jobs: [`docs/guides/cli.md`](cli.md)
- Configuration reference: [`docs/guides/configuration.md`](configuration.md)
- Proxies: [`docs/guides/proxy.md`](proxy.md)
- Run lifecycle: [`docs/guides/run.md`](run.md)
- Storage: [`docs/guides/lode.md`](lode.md)
- Integration: [`docs/guides/integration.md`](integration.md)

---

## Contracts vs Docs

User-facing docs explain **how to use Quarry**.
Contracts define **what must be true** for implementations.

When in doubt, treat `docs/contracts/CONTRACT_*.md` as authoritative.
