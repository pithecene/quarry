#!/usr/bin/env bash
# Point the moving image tags at a release, if it is still the highest.
# Usage: scripts/release/move-image-aliases.sh <image> <version>
#
# Runs after the versioned images (<image>:<version>, <image>:<version>-slim)
# are pushed. :latest and :slim are moved by a manifest copy that takes
# seconds, and only if <version> is the highest release at that moment: an
# older run overtaken by a newer release mid-build leaves them alone, and a
# newer release can only write them after its own build, so they never move
# backwards.

set -euo pipefail
# shellcheck source=scripts/release/lib.sh
source "$(dirname "$0")/lib.sh"

IMAGE="${1:?usage: move-image-aliases.sh <image> <version>}"
VERSION="${2:?usage: move-image-aliases.sh <image> <version>}"

if ! tag_is_highest "v${VERSION}"; then
  info "v${VERSION} is no longer the highest release; leaving ${IMAGE}:latest and :slim unchanged"
  exit 0
fi

docker buildx imagetools create -t "${IMAGE}:latest" "${IMAGE}:${VERSION}"
docker buildx imagetools create -t "${IMAGE}:slim" "${IMAGE}:${VERSION}-slim"
info "${IMAGE}:latest -> ${VERSION}, ${IMAGE}:slim -> ${VERSION}-slim"
