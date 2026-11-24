#!/usr/bin/env bash
# Activate the pnpm that package.json pins, and fail when another one runs.
# Usage: scripts/ci/setup-pnpm.sh

set -euo pipefail

pinned=$(jq -r '.packageManager' package.json | sed 's/pnpm@\([^+]*\).*/\1/')
corepack enable
corepack prepare "pnpm@$pinned" --activate
actual=$(pnpm --version)
if [[ "$actual" != "$pinned" ]]; then
  echo "::error::pnpm version mismatch! Actual ($actual) != Pinned ($pinned)"
  exit 1
fi
echo "✅ Node $(node --version), pnpm $actual"
