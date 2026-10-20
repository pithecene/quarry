#!/usr/bin/env bash
# Publish an npm package idempotently.
# Usage: scripts/release/publish-npm.sh <package-dir>
#
# The registry and auth come from the caller's npm config (actions/setup-node
# in CI). A version that is already published — a re-run, or a concurrent
# run that won the race — is a success, not a failure.
#
# The `latest` dist-tag is decided at publish time: only a version that is
# still the highest release when it is published takes `latest`; one that a
# newer release overtook mid-run publishes under `previous`, so `latest`
# never moves backwards.

set -euo pipefail
# shellcheck source=scripts/release/lib.sh
source "$(dirname "$0")/lib.sh"

DIR="${1:?usage: publish-npm.sh <package-dir>}"
cd "$DIR"

NAME=$(jq -r '.name' package.json)
VERSION=$(jq -r '.version' package.json)

published() {
  npm view "${NAME}@${VERSION}" version >/dev/null 2>&1
}

if published; then
  info "${NAME}@${VERSION} is already published"
  exit 0
fi

DIST_TAG=previous
if tag_is_highest "v${VERSION}"; then
  DIST_TAG=latest
fi

if pnpm publish --access restricted --no-git-checks --tag "$DIST_TAG"; then
  info "Published ${NAME}@${VERSION} (dist-tag ${DIST_TAG})"
  exit 0
fi

# The registry rejects a duplicate version; if another run published it
# between the check and the publish, the release is still complete.
published || die "failed to publish ${NAME}@${VERSION}"
info "${NAME}@${VERSION} was published by a concurrent run"
