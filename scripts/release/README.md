# Release tool

The steps the release workflows run: the tag gates, checksums, the GitHub
release and its assets, idempotent publishing to npm and JSR, and convergence of the moving pointers (npm `latest`, GHCR
`:latest` and `:slim`).

```
node scripts/release/src/cli.ts <command> [arguments]
```

| Command | Used by |
|---|---|
| `assert-highest-tag <tag>` | `release.yml` Tag Check, `release-dry-run.yml` |
| `assert-release-notes <tag>` | `release.yml` Tag Check and GitHub Release |
| `write-checksums <dir>` | `release.yml` Archive and Package, `release-dry-run.yml` |
| `draft-release <tag>` | `release.yml` Draft Release |
| `upload-assets <tag> <dir>` | `release.yml` Draft Release and GitHub Release |
| `publish-release <tag>` | `release.yml` GitHub Release, last |
| `publish-npm <package-dir>` | `release.yml` publish to GitHub Packages |
| `publish-jsr <package-dir> <tag>` | `release.yml` publish to JSR |
| `converge-pointers <npm-package-dir> <image>` | `release.yml` Release Pointers |

Exit codes are 0 for success, 1 when a release step fails (reported as a
GitHub Actions `::error::` annotation), and 2 for invalid usage.

## Runtime

The tool runs directly on the Node version pinned in `mise.toml`, which strips
TypeScript types natively, so there is no build step. It imports only Node
built-ins, so a workflow job needs Node but no `pnpm install`. For the same
reason, relative imports name `.ts` files, and the code uses only erasable
TypeScript syntax (`erasableSyntaxOnly`).

## Design

- `src/commands/` holds one file per command. Each takes a `Context`
  (`src/context.ts`) for everything it does outside the process: running
  commands, fetching, sleeping, and logging.
- `src/registry/` holds one probe per registry. A probe answers `present`,
  `absent`, or `failed` (`src/lookup.ts`). Only the registry's own not-found
  answer is `absent`, and a failed lookup is retried and never read as
  "not published".
- `src/cli.ts` parses arguments and the environment tunables, wires the real
  `Context`, and maps errors to exit codes.

Environment tunables:

- Registry lookups: `PROBE_ATTEMPTS` (3) and `PROBE_RETRY_DELAY` (5 s,
  doubled per retry).
- Asset uploads: `UPLOAD_ATTEMPTS` (3) and `UPLOAD_RETRY_DELAY` (5 s, doubled
  per retry) for all files together, and `UPLOAD_ATTEMPT_TIMEOUT` (300 s) for each
  upload. A failed read of the release uses an attempt and uploads nothing.
- JSR publishing: `JSR_ATTEMPTS` (3), `JSR_ATTEMPT_TIMEOUT` (480 s), and
  `JSR_RETRY_DELAY` (10 s, doubled per retry).

## Checks

```
task scripts:lint   # tsc and biome
task scripts:test   # vitest, against a fake world of registries
```
