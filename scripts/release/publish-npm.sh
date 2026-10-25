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

published() { lookup npm_probe "$NAME" "$VERSION"; }

# A failed lookup is not read as "not published": the publish below runs
# either way, and the registry rejects a duplicate version.
rc=0
published || rc=$?
if ((rc == PRESENT)); then
  info "${NAME}@${VERSION} is already published"
  exit 0
fi
((rc == LOOKUP_FAILED)) && info "could not check whether ${NAME}@${VERSION} is published; publishing"

if pnpm publish --access restricted --no-git-checks --tag release; then
  info "Published ${NAME}@${VERSION}"
  exit 0
fi

# The registry rejects a duplicate version; if another run published it
# between the check and the publish, the release is still complete.
rc=0
published || rc=$?
((rc == LOOKUP_FAILED)) && die "failed to publish ${NAME}@${VERSION}, and could not check the registry for it"
((rc == PRESENT)) || die "failed to publish ${NAME}@${VERSION}"
info "${NAME}@${VERSION} was published by a concurrent run"
