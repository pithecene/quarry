#!/usr/bin/env bash
# Point the moving image tags at the highest release.
# Usage: scripts/release/move-image-aliases.sh <image> <version>
#
# Runs after <image>:<version> and <image>:<version>-slim are pushed.
# :latest and :slim must only move forward even when release runs overlap,
# without a lock:
#
#   1. Write the aliases to <version> only if it is the highest release.
#   2. After writing, re-read the highest release. If a newer one exists and
#      its images are pushed, point the aliases at it.
#
# This is sufficient: a newer release's tag exists before its run writes
# anything, so if our write in (1) landed after the newer run's write, the
# re-read in (2) — which follows our write — sees the newer tag and restores
# it. If the newer images are not pushed yet, the newer run writes them
# later, after ours.

set -euo pipefail
# shellcheck source=scripts/release/lib.sh
source "$(dirname "$0")/lib.sh"

IMAGE="${1:?usage: move-image-aliases.sh <image> <version>}"
VERSION="${2:?usage: move-image-aliases.sh <image> <version>}"

point_aliases_at() {
  docker buildx imagetools create -t "${IMAGE}:latest" "${IMAGE}:$1"
  docker buildx imagetools create -t "${IMAGE}:slim" "${IMAGE}:$1-slim"
  info "${IMAGE}:latest -> $1, ${IMAGE}:slim -> $1-slim"
}

images_pushed() {
  docker buildx imagetools inspect "${IMAGE}:$1" >/dev/null 2>&1 \
    && docker buildx imagetools inspect "${IMAGE}:$1-slim" >/dev/null 2>&1
}

if tag_is_highest "v${VERSION}"; then
  point_aliases_at "$VERSION"
else
  info "v${VERSION} is not the highest release; not moving the aliases to it"
fi

HIGHEST=$(highest_released_version)
if [[ -n "$HIGHEST" && "$HIGHEST" != "$VERSION" ]] && images_pushed "$HIGHEST"; then
  point_aliases_at "$HIGHEST"
fi
