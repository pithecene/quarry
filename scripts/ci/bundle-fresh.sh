#!/usr/bin/env bash
# Fail when the embedded executor bundle differs from the committed one.
# Usage: scripts/ci/bundle-fresh.sh, after `task executor:bundle`

set -euo pipefail

if ! git diff --exit-code quarry/executor/bundle/executor.mjs; then
  echo "::error::Embedded executor bundle is stale. Run 'task executor:bundle' and commit the result."
  exit 1
fi
echo "✅ Bundle is fresh"
