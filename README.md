# Quarry

[![CI](https://github.com/pithecene-io/quarry/actions/workflows/ci.yml/badge.svg)](https://github.com/pithecene-io/quarry/actions/workflows/ci.yml)

**A CLI-first web extraction runtime for browser-driven crawling and durable ingestion**

Quarry is a web extraction runtime for imperative, browser-backed scraping workflows. It is designed for adversarial sites, bespoke extraction logic, and long-lived ETL pipelines where correctness, observability, and durability matter more than convenience abstractions.

Quarry executes user-authored Puppeteer scripts under a strict runtime contract, streams observations incrementally, and hands off persistence to an external substrate (typically Lode). It is intentionally *not* a crawler framework, workflow engine, or low-code platform.

---

## Installation

### CLI

```bash
mise install github:pithecene/quarry@0.13.5
```

### Docker

```bash
# Full image (includes Chromium + fonts)
docker pull ghcr.io/pithecene/quarry:0.13.4

# Slim image (no browser — BYO Chromium via --browser-ws-endpoint)
docker pull ghcr.io/pithecene/quarry:0.13.4-slim
```

> **Note:** image tags are published by the release workflow per tagged
> release; the pinned tags above are the newest published images, which
> may trail the CLI version when a release's container publish failed
> (see `CHANGELOG.md`). `:latest` and `:slim` always resolve to the newest
> published images.

See [docs/guides/container.md](docs/guides/container.md) for Docker Compose examples.

### SDK

```bash
npx jsr add @pithecene/quarry-sdk
```

See [PUBLIC_API.md](PUBLIC_API.md) for full setup and usage guide.

---

## What Quarry Is

- A **runtime**, not a framework
- **CLI-first**, not embedded
- Designed for **imperative Puppeteer scripts**
- Explicit about **ordering, backpressure, and failure**
- Agnostic to storage, retries, scheduling, and downstream processing
- **Not** a crawling DSL or workflow orchestrator
- **Not** a SaaS scraper or low-code pipeline

Quarry's responsibility ends at **observing and emitting what happened**.

---

## Conceptual Model

Quarry enforces a clean boundary between extraction logic and ingestion mechanics:

```
User Script (Puppeteer, imperative)
        ↓
emit.*  (stable event contract)
        ↓
Quarry Runtime
        ↓
Ingestion Policy (strict, buffered, streaming)
        ↓
Persistence Substrate (e.g. Lode)
```

Scripts **emit observations**.  
Policies decide how those observations are handled.  
Persistence decides what survives.

---

## Pipeline Composition

Quarry is designed to be composed **around**, not extended **from**:

```bash
# Extract
quarry run \
  --script streeteasy.ts \
  --run-id "streeteasy-$(date +%s)" \
  --source nyc-rent \
  --category streeteasy \
  --job '{"url": "https://streeteasy.com/rentals"}' \
  --storage-backend fs \
  --storage-path /var/quarry/data \
  --policy buffered

# Transform (outside Quarry)
nyc-rent-transform \
  --input /var/quarry/data/source=nyc-rent \
  --output /var/quarry/normalized

# Index / analyze (outside Quarry)
nyc-rent-index \
  --input /var/quarry/normalized
```

Quarry owns **only** the extraction step.

---

## Quick Example

Quarry scripts are **freestanding programs**, not libraries.

They should:
- Accept all inputs via the job payload
- Use real Puppeteer objects (`page`, `browser`)
- Emit all outputs via `emit.*`
- Avoid shared global state
- Remain agnostic to durability and retries

### Example

```ts
import type { QuarryContext } from '@pithecene/quarry-sdk'

export default async function run(ctx: QuarryContext): Promise<void> {
  await ctx.page.goto(ctx.job.url)

  const listings = await ctx.page.evaluate(() => {
    // scrape DOM
    return []
  })

  for (const listing of listings) {
    await ctx.emit.item({
      item_type: 'listing',
      data: listing
    })
  }

  await ctx.emit.runComplete()
}
```

Scripts are imperative, explicit, and boring by design.

---

## Key Concepts

- **Emit API** — all script output flows through `emit.*` → [docs/guides/emit.md](docs/guides/emit.md)
- **Policies** — strict, buffered, or streaming ingestion control → [docs/guides/policy.md](docs/guides/policy.md)
- **Storage** — FS and S3 backends via Lode → [docs/guides/lode.md](docs/guides/lode.md)
- **Proxies** — pool-based rotation with multiple strategies → [docs/guides/proxy.md](docs/guides/proxy.md)
- **Streaming** — chunked artifacts with backpressure → [docs/guides/streaming.md](docs/guides/streaming.md)
- **Configuration** — YAML project defaults via `--config` → [docs/guides/cli.md](docs/guides/cli.md)
- **Browser Reuse** — transparent Chromium persistence across sequential runs → [docs/guides/configuration.md](docs/guides/configuration.md)
- **Container Deployment** — full/slim images, shared browser pools, multi-crawler topologies → [docs/guides/container.md](docs/guides/container.md)
- **Integration** — webhook and Redis adapters for downstream triggers → [docs/guides/integration.md](docs/guides/integration.md)
- **Fan-Out** — derived work execution via `emit.enqueue()` → [docs/guides/emit.md](docs/guides/emit.md)
- **Run Lifecycle** — terminal states and exit codes → [docs/guides/run.md](docs/guides/run.md)

---

## Documentation

| Resource | Path |
|----------|------|
| Guides | [docs/guides/](docs/guides/) |
| Contracts | [docs/contracts/](docs/contracts/) |
| Public API | [PUBLIC_API.md](PUBLIC_API.md) |
| SDK | [sdk/README.md](sdk/README.md) |
| Examples | [examples/](examples/) |
| Support | [SUPPORT.md](SUPPORT.md) |
| Changelog | [CHANGELOG.md](CHANGELOG.md) |

---

## Status

Quarry is under active development.

- Contracts frozen, SDK stable
- FS and S3 storage supported
- Platforms: linux/darwin, x64/arm64, container (GHCR)

Breaking changes are gated by contract versioning.

---

## License

Apache 2.0
