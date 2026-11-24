#!/usr/bin/env bash
# Export the runner's image as MISE_RUNNER_IMAGE for the tool cache's key.
# Usage: scripts/ci/runner-image.sh

set -euo pipefail

echo "MISE_RUNNER_IMAGE=${ImageOS:?}-${ImageVersion:?}" >> "$GITHUB_ENV"
