#!/usr/bin/env bash
# Print the short commit, or "unknown" outside a Git checkout.
# Usage: scripts/ci/commit.sh

set -euo pipefail

git rev-parse --short HEAD 2> /dev/null || echo unknown
