#!/usr/bin/env bash
# Print whether the GitHub release for <tag> already carries its notes, as a
# GitHub Actions output line: has_notes=true or has_notes=false.
# Usage: scripts/release/release-notes-state.sh <tag>
#
# has_notes=true: a release exists with notes, so the workflow uploads the
# assets to it. has_notes=false: no release exists, or it holds only the
# draft placeholder or nothing, so the workflow creates or fills the draft.
# Any other lookup failure fails the step: a release that cannot be read is
# neither uploaded to blindly nor replaced by a draft.

set -euo pipefail
# shellcheck source=scripts/release/lib.sh
source "$(dirname "$0")/lib.sh"

TAG="${1:-}"
[[ -n "$TAG" ]] || die "usage: release-notes-state.sh <tag>"

# Kept in step with the draft body in .github/workflows/release.yml.
PLACEHOLDER="TODO: Add release notes"

ERR="$(mktemp)"
trap 'rm -f "$ERR"' EXIT

if ! BODY=$(gh release view "$TAG" --json body -q '.body' 2>"$ERR"); then
  grep -q "release not found" "$ERR" || die "could not read the release for ${TAG}: $(cat "$ERR")"
  info "No release for ${TAG} yet"
  echo "has_notes=false"
  exit 0
fi

if [[ -z "${BODY//[[:space:]]/}" ]] || grep -qF "$PLACEHOLDER" <<<"$BODY"; then
  info "The release for ${TAG} has no notes yet"
  echo "has_notes=false"
else
  info "The release for ${TAG} has its notes"
  echo "has_notes=true"
fi
