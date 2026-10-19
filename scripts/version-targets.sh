#!/usr/bin/env bash
# Shared helpers for the lockstep version targets in version-targets.txt.
# Sourced by version-bump.sh and version-check.sh; not run directly.

SEMVER='[0-9]+\.[0-9]+\.[0-9]+'
TARGETS_FILE="$(dirname "${BASH_SOURCE[0]}")/version-targets.txt"

canonical_version() {
  sed -n 's/^const Version = "\([^"]*\)".*/\1/p' quarry/types/version.go
}

# for_each_target <callback>: calls <callback> <file> <pattern> <suffixes>
# for every (expanded file, pattern) pair in the targets list. <suffixes>
# is the optional third column: literal suffixes that may follow the
# version in that slot (e.g. "-slim" for image tags); empty otherwise.
for_each_target() {
  local cb="$1" files pattern suffixes f
  while IFS=$'\t' read -r files pattern suffixes; do
    [[ -z "$files" || "$files" == \#* ]] && continue
    # shellcheck disable=SC2086 # word-splitting and globbing are the point
    for f in $files; do
      "$cb" "$f" "$pattern" "${suffixes:-}"
    done
  done < "$TARGETS_FILE"
}

# Portable in-place sed (GNU vs BSD)
if sed --version 2>/dev/null | grep -q 'GNU'; then
  sedi() { sed -i "$@"; }
else
  sedi() { sed -i '' "$@"; }
fi
