<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/quarry-banner-dark.png">
  <img alt="Quarry: browser-driven extraction, durably ingested. Banner art after Auguste Lepère, Fontainebleau Forest: Sandstone Quarry (1890), Cleveland Museum of Art, CC0" src="docs/assets/quarry-banner-light.png" width="100%">
</picture>

# Quarry

[![CI](https://github.com/pithecene/quarry/actions/workflows/ci.yml/badge.svg)](https://github.com/pithecene/quarry/actions/workflows/ci.yml)
[![Documentation](https://img.shields.io/badge/docs-pithecene.sh-1b1a17)](https://pithecene.sh/docs/quarry)

**A CLI-first web extraction runtime for browser-driven crawling and durable ingestion**

Quarry runs a Puppeteer script you write under a strict runtime contract, streams what the script observes as events, and hands persistence to an external storage layer (typically [Lode](https://github.com/pithecene/lode)). It is designed for adversarial sites, bespoke extraction logic, and long-lived ETL pipelines where correctness, observability, and durability matter more than convenience abstractions.

---

## Installation

These steps require Node.js 22.18 or later, excluding 23.0 through 23.5, which runs TypeScript scripts with native type stripping by default, and [mise](https://mise.jdx.dev) with its shell activation (`mise activate`) configured. The [SDK and setup guide](PUBLIC_API.md) lists the full toolchain, covers earlier Node 22 releases, and describes adding Quarry to an existing project.

### Script Project

The `quarry init` command creates a script project with the SDK, the Puppeteer packages the executor loads, an example script, and a storage directory. Its `mise.toml` pins the CLI and puts `quarry` on your `PATH` inside the project, and mise runs `init` itself at that version:

```bash
mise exec github:pithecene/quarry@0.14.2 -- quarry init my-scraper
cd my-scraper
npm install
```

### Docker

```bash
# Full image (includes Chromium + fonts)
docker pull ghcr.io/pithecene/quarry:0.14.2

# Slim image (no browser; bring your own Chromium through --browser-ws-endpoint)
docker pull ghcr.io/pithecene/quarry:0.14.2-slim
```

The full image runs a script without a local Node.js project, as § Run the Script in a Container shows. See the [container guide](docs/guides/container.md) for Docker Compose examples.

---

## What Quarry Is

- A **runtime**, not a framework
- **CLI-first**, not embedded
- Designed for **imperative Puppeteer scripts**
- Explicit about **ordering, backpressure, and failure**
- Agnostic to storage, retries, scheduling, and downstream processing

Quarry is not a crawler framework, a crawling DSL, a workflow orchestrator, a hosted scraping service, or a low-code pipeline.

Quarry's responsibility ends at **observing and emitting what happened**.

---

## Conceptual Model

Quarry enforces a fixed boundary between extraction logic and ingestion mechanics:

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
The ingestion policy decides how the runtime buffers and writes them, and which advisory events it may drop.
The persistence layer stores what the policy writes.

Four components carry out this model:

- **SDK**: the authoring surface for scripts, `emit.*` and the context objects.
- **Executor**: runs a script and streams its events to the runtime.
- **Runtime**: manages runs and applies the ingestion policy.
- **Lode**: stores each run's data under stable partitions.

---

## Pipeline Composition

Quarry is designed to be composed **around**, not extended **from**:

```bash
# Extract
quarry run \
  --script products.ts \
  --run-id "products-$(date +%s)" \
  --source example-store \
  --category products \
  --job '{"url": "https://shop.example.com/products"}' \
  --storage-backend fs \
  --storage-path /var/quarry/data \
  --policy buffered

# Transform (outside Quarry)
catalog-transform \
  --input /var/quarry/data/source=example-store \
  --output /var/quarry/normalized

# Index / analyze (outside Quarry)
catalog-index \
  --input /var/quarry/normalized
```

Quarry owns **only** the extraction step.

---

## Quick Example

A Quarry script is a **freestanding program**, not a library. The project that `quarry init` creates holds this one as `links.ts`. It reads a page and emits one item for each link on it.

### Script

```ts
import type { QuarryContext } from '@pithecene/quarry-sdk'

export default async function run(ctx: QuarryContext<{ url: string }>): Promise<void> {
  await ctx.page.goto(ctx.job.url)

  const links = await ctx.page.$$eval('a', (anchors) =>
    anchors.map((anchor) => ({
      text: anchor.textContent?.trim() ?? '',
      href: anchor.href
    }))
  )

  for (const link of links) {
    await ctx.emit.item({
      item_type: 'link',
      data: link
    })
  }

  await ctx.emit.runComplete()
}
```

Scripts are imperative, explicit, and simple by design. A script should:

- Accept all inputs through the job payload
- Use real Puppeteer objects (`page`, `browser`)
- Emit all outputs through `emit.*`
- Avoid shared global state
- Remain agnostic to durability and retries

### Run the Script

From the project directory, run the script with the filesystem storage backend:

```bash
quarry run \
  --script links.ts \
  --run-id links-001 \
  --source example \
  --job '{"url": "https://example.com"}' \
  --storage-backend fs \
  --storage-path ./quarry-data
```

Quarry writes the run's events under `./quarry-data`. List the run to confirm it succeeded:

```bash
quarry list runs --storage-backend fs --storage-path ./quarry-data --source example
```

The [run guide](docs/guides/run.md) describes the run lifecycle and its terminal states.

### Run the Script in a Container

The full image needs only the script and a storage directory. Save the script as `scripts/links.ts`, then run it as your own user, so that the container can write the storage directory:

```bash
mkdir -p data
docker run --rm --user "$(id -u):$(id -g)" \
  -v ./scripts:/work/scripts:ro \
  -v ./data:/work/data \
  ghcr.io/pithecene/quarry:0.14.2 \
  run \
    --script ./scripts/links.ts \
    --run-id links-001 \
    --source example \
    --job '{"url": "https://example.com"}' \
    --storage-backend fs \
    --storage-path ./data
```

---

## Key Concepts

- **Emit API**: all script output flows through `emit.*` → [emit guide](docs/guides/emit.md)
- **Policies**: strict, buffered, or streaming ingestion control → [policy guide](docs/guides/policy.md)
- **Storage**: FS and S3 backends via Lode → [storage guide](docs/guides/lode.md)
- **Proxies**: pool-based rotation with multiple strategies → [proxy guide](docs/guides/proxy.md)
- **Streaming**: chunked artifacts with backpressure → [emit guide](docs/guides/emit.md#streaming-and-backpressure)
- **Configuration**: YAML project defaults via `--config` → [configuration reference](docs/guides/configuration.md)
- **Browser Reuse**: transparent Chromium persistence across sequential runs → [configuration reference](docs/guides/configuration.md)
- **Container Deployment**: full/slim images, shared browser pools, multi-crawler topologies → [container guide](docs/guides/container.md)
- **Integration**: webhook and Redis adapters for downstream triggers → [integration guide](docs/guides/integration.md)
- **Fan-Out**: derived work execution via `emit.enqueue()` → [emit guide](docs/guides/emit.md)
- **Run Lifecycle**: terminal states and exit codes → [run guide](docs/guides/run.md)

---

## Documentation

Pithecene publishes the guides, the public API reference, the contracts, and
the changelog at [pithecene.sh/docs/quarry](https://pithecene.sh/docs/quarry).
The repository holds those documents and the material that the site does not
publish, such as the SDK reference, the examples, and the support guide:

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

Apache License 2.0 (see [LICENSE](LICENSE)). Copyright © 2026 Pithecene LLC. Contact: contact@pithecene.sh.
