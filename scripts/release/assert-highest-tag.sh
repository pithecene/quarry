#!/usr/bin/env bash
# Fail unless <tag> is at least as high as every released tag.
# Usage: scripts/release/assert-highest-tag.sh <tag>
#
# Releases are monotonic: a tag lower than an already-released tag is
# rejected before anything is published. Equal is allowed so a re-run of the
# current release passes. Republishing an older version is a deliberate
# manual operation, not a workflow path.
#
# This is the gate on new tags. The moving pointers (npm `latest`, GHCR
# :latest and :slim) are additionally decided at the moment they are
# written (publish-npm.sh, move-image-aliases.sh), because a newer release
# can be tagged while an older release's run is still building.

set -euo pipefail
# shellcheck source=scripts/release/lib.sh
source "$(dirname "$0")/lib.sh"

TAG="${1:-}"
[[ -n "$TAG" ]] || die "usage: assert-highest-tag.sh <tag>"
is_release_tag "$TAG" || die "not a release tag: ${TAG} (expected vX.Y.Z)"

tag_is_highest "$TAG" \
  || die "${TAG} is lower than an already-released tag; releases must be monotonic (republishing an older version is a manual operation)"
info "${TAG} is the highest release"
