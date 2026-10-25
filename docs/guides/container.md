# Container Usage

Quarry publishes two container images: a full image that includes
Chromium, and a slim image that connects to a browser running elsewhere.
This guide shows how to run them with `docker run` and Docker Compose,
including S3 storage, a Redis adapter, and a shared browser pool. The
[CLI guide](cli.md) and the [configuration reference](configuration.md)
describe the flags.

---

## Images

Quarry ships two container images via GHCR:

| Image | Tag | Arch | Includes |
|-------|-----|------|----------|
| Full | `ghcr.io/pithecene/quarry:0.14.0` | amd64 | Quarry CLI, Node.js, Puppeteer, Chrome for Testing, fonts |
| Slim | `ghcr.io/pithecene/quarry:0.14.0-slim` | amd64, arm64 | Quarry CLI, Node.js, Puppeteer (no browser) |
The **full** image is recommended for standalone usage. The **slim** image is
for environments where Chromium is provided externally (e.g., via
`--browser-ws-endpoint` pointing at a sidecar or shared browser service).

> **Note for arm64:** The full image is amd64-only because [Chrome for Testing does
> not publish linux-arm64 builds](https://github.com/nickinchrismath/chrome-for-testing/issues/1).
> arm64 users should use the slim image with an external browser sidecar
> (see [Slim image with external browser](#slim-image-with-external-browser) below).

Both images set `QUARRY_NO_SANDBOX=1` (required for containerized Chromium)
and run as a non-root `quarry` user.

---

## Standalone `docker run`

```bash
docker run --rm \
  -v ./scripts:/work/scripts:ro \
  -v ./data:/work/data \
  ghcr.io/pithecene/quarry:0.14.0 \
  run \
    --script ./scripts/my-script.ts \
    --run-id "run-$(date +%s)" \
    --source my-source \
    --storage-backend fs \
    --storage-path ./data
```

---

## Docker Compose

```yaml
services:
  quarry:
    image: ghcr.io/pithecene/quarry:0.14.0
    volumes:
      - ./scripts:/work/scripts:ro
      - ./data:/work/data
    command:
      - run
      - --script=./scripts/my-script.ts
      - --run-id=scheduled-run
      - --source=my-source
      - --storage-backend=fs
      - --storage-path=./data
      - --policy=strict
```

---

## Docker Compose with S3 storage

```yaml
services:
  quarry:
    image: ghcr.io/pithecene/quarry:0.14.0
    volumes:
      - ./scripts:/work/scripts:ro
    environment:
      - AWS_ACCESS_KEY_ID
      - AWS_SECRET_ACCESS_KEY
    command:
      - run
      - --script=./scripts/my-script.ts
      - --run-id=scheduled-run
      - --source=my-source
      - --storage-backend=s3
      - --storage-path=my-bucket/quarry-data
      - --storage-region=us-east-1
```

---

## Docker Compose with Redis adapter

```yaml
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  quarry:
    image: ghcr.io/pithecene/quarry:0.14.0
    depends_on:
      - redis
    volumes:
      - ./scripts:/work/scripts:ro
      - ./data:/work/data
    command:
      - run
      - --script=./scripts/my-script.ts
      - --run-id=scheduled-run
      - --source=my-source
      - --storage-backend=fs
      - --storage-path=./data
      - --adapter=redis
      - --adapter-url=redis://redis:6379
      - --adapter-channel=quarry:run_completed
```

A `RunCompletedEvent` JSON message is published to the `quarry:run_completed`
channel after each successful run. Subscribe with any Redis client:

```bash
redis-cli SUBSCRIBE quarry:run_completed
```

---

## Monorepo with workspace dependencies

When scripts import workspace packages (`@myorg/db`, `shared-utils`) that
live in a monorepo `node_modules` tree, use `--resolve-from` to tell the
executor where to find them:

```yaml
services:
  quarry:
    image: ghcr.io/pithecene/quarry:0.14.0
    volumes:
      - ./scripts:/work/scripts:ro
      - ./node_modules:/work/node_modules:ro
      - ./packages:/work/packages:ro
      - ./data:/work/data
    command:
      - run
      - --script=./scripts/my-script.ts
      - --run-id=scheduled-run
      - --source=my-source
      - --storage-backend=fs
      - --storage-path=./data
      - --resolve-from=./node_modules
```

The `--resolve-from` flag registers an ESM resolve hook so bare specifiers
(`import { db } from '@myorg/db'`) resolve via the specified `node_modules`
directory when they cannot be found from the script's own location.

---

## Slim image with external browser

Use the slim image when Chromium is provided externally: by a sidecar
container, a shared browser pool, or a browser on the host.

The `QUARRY_BROWSER_ENDPOINT` environment variable (or `--browser-ws-endpoint`
flag) tells Quarry to connect instead of launching its own Chromium.

```yaml
services:
  chrome:
    image: chromedp/headless-shell:latest
    ports:
      - "9222:9222"

  quarry:
    image: ghcr.io/pithecene/quarry:0.14.0-slim
    depends_on:
      - chrome
    volumes:
      - ./scripts:/work/scripts:ro
      - ./data:/work/data
    environment:
      QUARRY_BROWSER_ENDPOINT: "ws://chrome:9222"
    command:
      - run
      - --script=./scripts/my-script.ts
      - --run-id=scheduled-run
      - --source=my-source
      - --storage-backend=fs
      - --storage-path=./data
```

> **Health gate:** Quarry verifies the browser endpoint is reachable before
> launching the executor. If the browser is down, the run fails immediately
> with a clear error instead of timing out inside the script.

---

## Multi-crawler with shared browser pool

When running multiple crawlers on the same host, a shared browser pool
avoids bundling Chromium in every crawler image (~1 GB savings each) and
lets you scale crawlers and browsers independently.

On the same Docker network, the CDP WebSocket stays on the bridge
(loopback-equivalent), with no external network hop.

```yaml
services:
  browser-pool:
    image: browserless/chrome
    shm_size: "2gb"
    environment:
      MAX_CONCURRENT_SESSIONS: "10"

  crawler-products:
    image: ghcr.io/pithecene/quarry:0.14.0-slim
    depends_on:
      - browser-pool
    volumes:
      - ./scripts:/work/scripts:ro
      - ./data:/work/data
    environment:
      QUARRY_BROWSER_ENDPOINT: "ws://browser-pool:3000"
    command:
      - run
      - --script=./scripts/products.ts
      - --run-id=products-run
      - --source=products
      - --storage-backend=fs
      - --storage-path=./data

  crawler-reviews:
    image: ghcr.io/pithecene/quarry:0.14.0-slim
    depends_on:
      - browser-pool
    volumes:
      - ./scripts:/work/scripts:ro
      - ./data:/work/data
    environment:
      QUARRY_BROWSER_ENDPOINT: "ws://browser-pool:3000"
    command:
      - run
      - --script=./scripts/reviews.ts
      - --run-id=reviews-run
      - --source=reviews
      - --storage-backend=fs
      - --storage-path=./data
```

Each crawler receives an isolated `BrowserContext` within the shared browser,
so cookies, storage, and sessions do not leak between crawlers.

### When to use which pattern

| Pattern | Use when |
|---------|----------|
| **Full image** (bundled Chromium) | Single crawler, simplest setup, no external dependencies |
| **Slim + sidecar** (1 crawler, 1 browser) | arm64, or you want browser lifecycle control |
| **Slim + shared pool** (N crawlers, 1 pool) | Multiple crawlers on the same host, want smaller images and shared memory |
| **Full image per crawler** (N independent) | Crawlers on different hosts, or need full isolation |

### Limitations

- **Stealth/adblocker plugins are skipped** when connecting to an external
  browser (vanilla Puppeteer `connect()` is used). If detection evasion is
  critical, use the full image with per-crawler Chromium.
- **Cross-host CDP is not recommended.** The CDP protocol is chatty. Even
  moderate latency degrades performance. Keep crawlers and browser pools
  co-located (same host or same availability zone).
