#!/usr/bin/env bash
# Publish an npm package idempotently, keeping `latest` forward-only.
# Usage: scripts/release/publish-npm.sh <package-dir>
#
# The registry and auth come from the caller's npm config (actions/setup-node
# in CI). A version that is already published — a re-run, or a concurrent
# run that won the race — is a success, not a failure.
#
# `latest` must only move forward even when release runs overlap, without a
# lock (same argument as move-image-aliases.sh):
#
#   1. Publish under `latest` only if this version is the highest release,
#      otherwise under `previous`.
#   2. After publishing, re-read the highest release. If a newer one exists
#      and is published, point `latest` at it.
#
# A newer release's tag exists before its run publishes, so if our publish
# landed after the newer run's, the re-read in (2) sees it and restores it.

set -euo pipefail
# shellcheck source=scripts/release/lib.sh
source "$(dirname "$0")/lib.sh"

DIR="${1:?usage: publish-npm.sh <package-dir>}"
cd "$DIR"

NAME=$(jq -r '.name' package.json)
VERSION=$(jq -r '.version' package.json)

published() {
  npm view "${NAME}@$1" version >/dev/null 2>&1
}

if published "$VERSION"; then
  info "${NAME}@${VERSION} is already published"
else
  DIST_TAG=previous
  if tag_is_highest "v${VERSION}"; then
    DIST_TAG=latest
  fi
  if pnpm publish --access restricted --no-git-checks --tag "$DIST_TAG"; then
    info "Published ${NAME}@${VERSION} (dist-tag ${DIST_TAG})"
  else
    # The registry rejects a duplicate version; if another run published it
    # between the check and the publish, the release is still complete.
    published "$VERSION" || die "failed to publish ${NAME}@${VERSION}"
    info "${NAME}@${VERSION} was published by a concurrent run"
  fi
fi

HIGHEST=$(highest_released_version)
if [[ -n "$HIGHEST" && "$HIGHEST" != "$VERSION" ]] && published "$HIGHEST"; then
  npm dist-tag add "${NAME}@${HIGHEST}" latest
  info "latest -> ${HIGHEST}"
fi
