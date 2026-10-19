#!/usr/bin/env bash
# Verify that every lockstep version target carries the canonical version.
# Usage: scripts/version-check.sh
#
# Fails if any target in scripts/version-targets.txt matches nothing (the
# text it pins has moved or been deleted) or matches a version other than
# quarry/types/version.go. Run by `task version:check` and CI.

set -euo pipefail
# shellcheck source=scripts/version-targets.sh
source "$(dirname "$0")/version-targets.sh"

EXPECTED=$(canonical_version)
status=0

# Whatever sits in the version slot: the whole run of non-space characters
# (regex backtracking hands trailing pattern text such as ")" or "." back to
# the suffix). Each captured token must equal the canonical version exactly,
# or the version plus one of the target's declared suffixes (e.g. "-slim"),
# so "0.13.5x", "0.13.5-rc", ":latest", or a partial edit all fail.
TOKEN='[^[:space:]"'"'"'`|]+'

check_target() {
  local file="$1" pattern="$2" suffixes="$3" prefix suffix scan found allowed bad s
  prefix="${pattern%%__V__*}"
  suffix="${pattern#*__V__}"
  if [[ ! -f "$file" ]]; then
    echo "❌ $file: missing (target: $pattern)"
    status=1
    return
  fi
  scan="${prefix}${TOKEN}${suffix}"
  found=$(grep -oE -e "$scan" "$file" \
    | sed -E "s|^${prefix}||; s|${suffix}\$||" || true)
  if [[ -z "$found" ]]; then
    echo "❌ $file: no match for pattern: $pattern"
    status=1
    return
  fi
  allowed="$EXPECTED"
  for s in $suffixes; do allowed+=$'\n'"${EXPECTED}${s}"; done
  bad=$(printf '%s\n' "$found" | grep -vxF -f <(printf '%s\n' "$allowed") | sort -u || true)
  if [[ -n "$bad" ]]; then
    echo "❌ $file: found ${bad//$'\n'/, } for pattern: $pattern"
    status=1
  fi
}

for_each_target check_target

if [[ $status -ne 0 ]]; then
  echo "❌ Version drift: expected $EXPECTED everywhere (see scripts/version-targets.txt)"
  exit 1
fi
echo "✅ All version targets at $EXPECTED"
