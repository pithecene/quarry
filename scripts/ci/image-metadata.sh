#!/usr/bin/env bash
# Write the step outputs that the image builds use: commit, and version for a tag.
# Usage: scripts/ci/image-metadata.sh [vX.Y.Z]

set -euo pipefail

echo "commit=$(git rev-parse --short HEAD)" >> "$GITHUB_OUTPUT"
if [[ $# -gt 0 ]]; then
  echo "version=${1#v}" >> "$GITHUB_OUTPUT"
fi
