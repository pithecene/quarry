#!/usr/bin/env bash
# Point the moving release pointers at the highest published release.
# Usage: scripts/release/converge-pointers.sh <npm-package-dir> <image>
#
# Sets npm `latest` and <image>:latest / <image>:slim to the highest release
# tag whose artifacts are published. The result depends only on registry
# state, not on which release run invokes it, so any invocation after a
# release's artifacts are published leaves the pointers correct.
#
# Neither registry offers compare-and-swap on these pointers, so callers
# must serialize invocations (the release workflow runs this in a job under
# a `concurrency` group). Under serialization, a dropped pending invocation
# is harmless: the one that replaces it runs later and converges to state
# that includes everything the dropped one would have seen.
#
# npm and the images are converged independently: each pointer goes to the
# highest release published on its own registry.

set -euo pipefail
# shellcheck source=scripts/release/lib.sh
source "$(dirname "$0")/lib.sh"

PKG_DIR="${1:?usage: converge-pointers.sh <npm-package-dir> <image>}"
IMAGE="${2:?usage: converge-pointers.sh <npm-package-dir> <image>}"
NAME=$(jq -r '.name' "${PKG_DIR}/package.json")

# Release versions, highest first.
VERSIONS=$(released_tags) || die "could not list release tags on origin"
VERSIONS=$(printf '%s\n' "$VERSIONS" | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | sort -rV | sed 's/^v//' || true)
[[ -n "$VERSIONS" ]] || die "no release tags on origin"

npm_published() { npm view "${NAME}@$1" version >/dev/null 2>&1; }
images_pushed() {
  docker buildx imagetools inspect "${IMAGE}:$1" >/dev/null 2>&1 \
    && docker buildx imagetools inspect "${IMAGE}:$1-slim" >/dev/null 2>&1
}

# first_where <predicate>: the highest version for which <predicate> holds.
first_where() {
  local v
  while read -r v; do
    "$1" "$v" && { echo "$v"; return 0; }
  done <<<"$VERSIONS"
  return 1
}

if NPM_TARGET=$(first_where npm_published); then
  npm dist-tag add "${NAME}@${NPM_TARGET}" latest
  info "npm ${NAME} latest -> ${NPM_TARGET}"
else
  info "no release of ${NAME} is published; npm latest unchanged"
fi

if IMAGE_TARGET=$(first_where images_pushed); then
  docker buildx imagetools create -t "${IMAGE}:latest" "${IMAGE}:${IMAGE_TARGET}"
  docker buildx imagetools create -t "${IMAGE}:slim" "${IMAGE}:${IMAGE_TARGET}-slim"
  info "${IMAGE}:latest -> ${IMAGE_TARGET}, :slim -> ${IMAGE_TARGET}-slim"
else
  info "no release images are pushed; ${IMAGE}:latest and :slim unchanged"
fi
