#!/usr/bin/env bash
# Print what a publish from a package directory would use.
# Usage: scripts/ci/publish-diagnostics.sh <package-dir>

set -euo pipefail

cd "$1"
echo "📦 Package: $(jq -r '.name' package.json)"
echo "📦 Version: $(jq -r '.version' package.json)"
echo "📦 Registry: $(npm config get registry)"
echo "📦 Scope registry: $(npm config get @pithecene:registry || echo 'not set')"
