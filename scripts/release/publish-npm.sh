#!/usr/bin/env bash
# Publish an npm package version idempotently, without moving `latest`.
# Usage: scripts/release/publish-npm.sh <package-dir>
#
# The registry and auth come from the caller's npm config (actions/setup-node
# in CI). A version that is already published — a re-run, or a concurrent
# run that won the race — is a success, not a failure.
#
# The version is published under the `release` dist-tag. `latest` is owned
# by converge-pointers.sh, which runs serialized after publishing, so
# overlapping release runs never race on it.

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

if pnpm publish --access restricted --no-git-checks --tag release; then
  info "Published ${NAME}@${VERSION}"
  exit 0
fi

# The registry rejects a duplicate version; if another run published it
# between the check and the publish, the release is still complete.
published || die "failed to publish ${NAME}@${VERSION}"
info "${NAME}@${VERSION} was published by a concurrent run"
