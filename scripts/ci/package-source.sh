#!/usr/bin/env bash
# Package the source and the SDK in dist, with their checksums.
# Usage: scripts/ci/package-source.sh vX.Y.Z

set -euo pipefail

mkdir -p dist
git archive --format=tar.gz --output="dist/quarry-$1.tar.gz" HEAD
# pnpm names the tarball after the package version, so the only new one moves.
packed=$(mktemp -d)
trap 'rm -rf "$packed"' EXIT
pnpm -C sdk pack --pack-destination "$packed"
mv "$packed"/*.tgz "dist/quarry-sdk-$1.tgz"
node scripts/release/src/cli.ts write-checksums dist
