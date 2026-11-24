#!/usr/bin/env bash
# Fail when the built SDK's CONTRACT_VERSION differs from its source.
# Usage: scripts/ci/sdk-lockstep.sh, after the SDK's build

set -euo pipefail

contract_version() { sed -n "s/.*const CONTRACT_VERSION = ['\"]\([^'\"]*\).*/\1/p" "$1"; }
source_version=$(contract_version sdk/src/types/events.ts)
built_version=$(contract_version sdk/dist/index.mjs)
if [[ -z "$source_version" || "$source_version" != "$built_version" ]]; then
  echo "::error::SDK build CONTRACT_VERSION mismatch! Source ($source_version) != Built ($built_version)"
  exit 1
fi
echo "✅ SDK build lockstep verified: $source_version"
