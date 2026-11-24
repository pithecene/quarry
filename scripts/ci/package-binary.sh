#!/usr/bin/env bash
# Build the binary for GOOS and GOARCH, and archive it in dist with its checksum.
# Usage: GOOS=linux GOARCH=amd64 scripts/ci/package-binary.sh vX.Y.Z

set -euo pipefail

commit=$(git rev-parse --short HEAD)
CGO_ENABLED=0 go -C quarry build -ldflags "-X main.commit=$commit" -o quarry ./cmd/quarry
mkdir -p dist
tar -czf "dist/quarry_$1_${GOOS}_${GOARCH}.tar.gz" -C quarry quarry
node scripts/release/src/cli.ts write-checksums dist
