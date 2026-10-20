# Proxy Support

This document describes proxy configuration and usage in Quarry.
It is informational; normative behavior is defined in [`docs/contracts/CONTRACT_PROXY.md`](../contracts/CONTRACT_PROXY.md).

---

## Overview

Quarry provides provider-agnostic proxy support with:
- **Runtime selection**: pools, strategies (round-robin, random, sticky)
- **Executor application**: Puppeteer launch args and page authentication
- **Validation**: hard and soft validation rules

Pools are declared once and referenced by jobs.

---

## Configuration

### Repository-level pools

Example (`quarry.yaml`):

```yaml
proxies:
  iproyal_nyc:
    strategy: round_robin
    endpoints:
      - protocol: https
        host: geo.iproyal.com
        port: 12321
        username: ${IPROYAL_USER}
        password: ${IPROYAL_PASS}
      - protocol: https
        host: geo.iproyal.com
        port: 12322
        username: ${IPROYAL_USER}
        password: ${IPROYAL_PASS}

  residential_sticky:
    strategy: sticky
    sticky:
      scope: domain    # job | domain | origin
      ttl_ms: 3600000  # 1 hour
    endpoints:
      - protocol: http
        host: proxy.example.com
        port: 8080
```

### Job-level selection

Proxy selection is configured per invocation via CLI flags (not via the
config file). The `proxy:` key in `quarry.yaml` provides defaults; CLI
flags override them.

```bash
quarry run \
  --config quarry.yaml \
  --script ./script.ts \
  --run-id run-001 \
  --proxy-pool iproyal_nyc \
  --proxy-strategy round_robin \
  --proxy-sticky-key my-custom-key
```

---

## CLI Selection

**Recommended:** Define proxy pools in a `quarry.yaml` config file (see
[`docs/guides/configuration.md`](configuration.md)) and use `--config quarry.yaml`.

**Legacy:** You can also select proxies using a separate JSON config file
via `--proxy-config`. This method is deprecated and will emit a warning:

```
quarry run \
  --script ./examples/demo.ts \
  --run-id run-001 \
  --source demo \
  --storage-backend fs \
  --storage-path ./quarry-data \
  --proxy-config ./proxies.json \
  --proxy-pool residential_sticky \
  --proxy-domain example.com \
  --job '{"source":"demo"}'
```

Relevant flags:
- `--proxy-config <path>` (JSON pool config)
- `--proxy-pool <name>`
- `--proxy-strategy round_robin|random|sticky`
- `--proxy-sticky-key <key>`
- `--proxy-domain <domain>` (when sticky scope = domain)
- `--proxy-origin <origin>` (when sticky scope = origin, format: scheme://host:port)

If a pool uses sticky scope `domain` or `origin` and you do not supply the
corresponding input, the CLI will warn and fall back to other sticky inputs.

---

## Strategies

### Round-robin

- Deterministic rotation per pool
- Counter maintained by runtime
- Suitable for load distribution

### Random

- Uniform random selection per request
- Secure RNG used
- Suitable for large pools

#### Recency Window (optional)

Set `recency_window` on a pool to avoid immediately reusing endpoints:

```yaml
proxies:
  rotating_pool:
    strategy: random
    recency_window: 3
    endpoints:
      - protocol: http
        host: proxy1.example.com
        port: 8080
      - protocol: http
        host: proxy2.example.com
        port: 8080
      - protocol: http
        host: proxy3.example.com
        port: 8080
      - protocol: http
        host: proxy4.example.com
        port: 8080
      - protocol: http
        host: proxy5.example.com
        port: 8080
```

With `recency_window: 3`, the last 3 selected endpoints are excluded from the candidate pool. This prevents immediate reuse without sacrificing randomness.

If the window is >= the number of endpoints, selection degrades to LRU (least-recently-used) ordering — effectively round-robin-like but without a fixed sequence.

Only applies to the `random` strategy; ignored for `round_robin` and `sticky`.

### Sticky

- Stable mapping by sticky key
- Key derivation precedence:
  1. Explicit `sticky_key` if provided
  2. If scope = `job`: job ID
  3. If scope = `domain`: request domain
  4. If scope = `origin`: scheme+host+port
- Optional TTL for entry expiration

---

## Validation

### Hard validation (rejects invalid config)

- Endpoints list must have at least one entry
- Port must be between 1 and 65535
- Protocol must be `http`, `https`, or `socks5`
- Username and password must be provided together

### Soft warnings (surfaced but not rejected)

- `socks5` usage with Puppeteer is best-effort
- Very large endpoint lists with `round_robin` (consider `random`)

---

## Runtime Behavior

### Selection

1. Runtime receives job with proxy request
2. Selector looks up pool by name
3. Strategy selects endpoint (updates counters/maps as needed)
4. Resolved endpoint is included in run request to executor

### Application (Executor)

1. Executor receives resolved endpoint in run input
2. Puppeteer launched with `--proxy-server=<protocol>://<host>:<port>`
3. If credentials present, `page.authenticate()` is called
4. Proxy password is never logged

### Result

- Run result includes `proxy_used` with redacted endpoint (no password)

---

## SDK Types

```typescript
import type {
  ProxyEndpoint,
  ProxyPool,
  ProxyStrategy,
  ProxySticky
} from '@pithecene/quarry-sdk'

import {
  validateProxyEndpoint,
  validateProxyPool,
  redactProxyEndpoint
} from '@pithecene/quarry-sdk'
```

---

## Security Notes

- Use environment variables for credentials
- Runtime and executor must never log passwords
- Results include `proxy_used` metadata without passwords
- Credentials are applied via page authentication, not URL

---

## Troubleshooting

| Symptom | Possible Cause | Solution |
|---------|---------------|----------|
| 403/407 responses | Invalid credentials | Verify username/password |
| Connection timeouts | Endpoint unavailable | Check proxy server status |
| Protocol errors | Wrong protocol | Verify http vs https vs socks5 |
| Socks5 failures | Limited Puppeteer support | Prefer http/https if possible |
| Sticky not working | Missing scope config | Add `sticky.scope` to pool |
