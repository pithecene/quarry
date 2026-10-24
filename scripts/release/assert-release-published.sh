#!/usr/bin/env bash
# Fail unless the GitHub release for <tag> exists, is published, and is not
# a prerelease.
# Usage: scripts/release/assert-release-published.sh <tag>
#
# A release is published by creating its GitHub release, notes and all,
# which pushes the tag. A tag without a published, stable release is
# rejected before anything is published.

set -euo pipefail
# shellcheck source=scripts/release/lib.sh
source "$(dirname "$0")/lib.sh"

TAG="${1:-}"
[[ -n "$TAG" ]] || die "usage: assert-release-published.sh <tag>"

ERR="$(mktemp)"
trap 'rm -f "$ERR"' EXIT

if ! STATE=$(gh release view "$TAG" --json isDraft,isPrerelease \
  -q 'if .isDraft then "a draft" elif .isPrerelease then "a prerelease" else "published" end' 2>"$ERR"); then
  grep -q "release not found" "$ERR" || die "could not read the GitHub release for ${TAG}: $(cat "$ERR")"
  STATE="missing"
fi

[[ "$STATE" == published ]] \
  || die "the GitHub release for ${TAG} is ${STATE}; create and publish the release with its notes, which pushes the tag"
info "The GitHub release for ${TAG} is published"
