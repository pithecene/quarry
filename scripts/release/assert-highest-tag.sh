#!/usr/bin/env bash
# Fail unless <tag> is at least as high as every released tag.
# Usage: scripts/release/assert-highest-tag.sh <tag> [--tags-file <file>]
#
# Releases are monotonic: a tag lower than an already-released tag is
# rejected before anything is published, so the moving pointers (npm
# `latest`, GHCR :latest and :slim) can only ever move forward. Equal is
# allowed so a re-run of the current release passes. Republishing an older
# version is a deliberate manual operation, not a workflow path.
#
# --tags-file reads the released tags from a file instead of origin (tests).

set -euo pipefail
# shellcheck source=scripts/release/lib.sh
source "$(dirname "$0")/lib.sh"

TAG="${1:-}"
[[ -n "$TAG" ]] || die "usage: assert-highest-tag.sh <tag> [--tags-file <file>]"
is_release_tag "$TAG" || die "not a release tag: ${TAG} (expected vX.Y.Z)"

if [[ "${2:-}" == "--tags-file" ]]; then
  TAGS=$(cat "${3:?--tags-file needs a path}")
else
  TAGS=$(released_tags) || die "could not list release tags on origin"
fi

HIGHEST=$(printf '%s\n' "$TAGS" | highest_tag)
if [[ -z "$HIGHEST" ]]; then
  info "No release tags yet; ${TAG} is the first"
  exit 0
fi

TOP=$(printf '%s\n%s\n' "$HIGHEST" "$TAG" | highest_tag)
[[ "$TOP" == "$TAG" ]] \
  || die "${TAG} is lower than the released ${HIGHEST}; releases must be monotonic (republishing an older version is a manual operation)"
info "${TAG} is the highest release (previous highest: ${HIGHEST})"
