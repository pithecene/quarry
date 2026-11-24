#!/usr/bin/env bash
# Write dist/checksums.txt over the platform archives.
# Usage: scripts/ci/checksums-manifest.sh

set -euo pipefail

cd dist
sha256sum quarry_*.tar.gz > checksums.txt
cat checksums.txt
