#!/usr/bin/env bash
# Bump the lockstep version across every target in scripts/version-targets.txt.
# Usage: scripts/version-bump.sh <new-version>
#
# Rewrites the version inside each target pattern regardless of what it
# currently says, so a target that drifted still lands on <new-version>.
# Does NOT commit, edit CHANGELOG, or rebuild artifacts; the Taskfile task
# (version:bump) calls this, rebuilds, then runs version-check.sh.

set -euo pipefail
# shellcheck source=scripts/version-targets.sh
source "$(dirname "$0")/version-targets.sh"

NEW="${1:?Usage: version-bump.sh <new-version>}"
[[ "$NEW" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "not a semver: $NEW" >&2; exit 2; }
OLD=$(canonical_version)

echo "🔖 Bumping $OLD → $NEW"
echo ""

bump_target() {
  local file="$1" pattern="$2" prefix suffix
  prefix="${pattern%%__V__*}"
  suffix="${pattern#*__V__}"
  # '|' as the delimiter: patterns may contain '#' and '/', never '|'.
  # BSD sed rejects an empty group, so suffixless patterns get no group 2.
  if [[ -n "$suffix" ]]; then
    sedi -E "s|(${prefix})${SEMVER}(${suffix})|\1${NEW}\2|g" "$file"
  else
    sedi -E "s|(${prefix})${SEMVER}|\1${NEW}|g" "$file"
  fi
  echo "  ✅ $file"
}

for_each_target bump_target
