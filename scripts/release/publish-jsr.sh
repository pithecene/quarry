#!/usr/bin/env bash
# Publish a package to JSR idempotently, with bounded attempts.
# Usage: scripts/release/publish-jsr.sh <package-dir> <version>
#
# Authentication is GitHub OIDC (the job needs `id-token: write` and the JSR
# package linked to this repository). JSR processes uploads server-side and
# that processing can stall indefinitely, so each attempt is time-limited,
# and after a failed attempt JSR's own record decides the outcome: the upload
# may have succeeded while the client's status polling failed.
#
# Tunables (environment): JSR_ATTEMPTS (3), JSR_ATTEMPT_TIMEOUT (480 s),
# JSR_RETRY_DELAY (10 s, doubled per retry).

set -euo pipefail
# shellcheck source=scripts/release/lib.sh
source "$(dirname "$0")/lib.sh"

DIR="${1:?usage: publish-jsr.sh <package-dir> <version>}"
VERSION="${2:?usage: publish-jsr.sh <package-dir> <version>}"
ATTEMPTS="${JSR_ATTEMPTS:-3}"
ATTEMPT_TIMEOUT="${JSR_ATTEMPT_TIMEOUT:-480}"
RETRY_DELAY="${JSR_RETRY_DELAY:-10}"
cd "$DIR"

NAME=$(jq -r '.name' jsr.json)
# Per-version metadata on jsr.io is the record the registry serves to
# installers; the api.jsr.io version endpoint can lag behind it.
META_URL="https://jsr.io/${NAME}/${VERSION}_meta.json"

published() {
  [[ "$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' "$META_URL")" == "200" ]]
}

attempt() {
  timeout "$ATTEMPT_TIMEOUT" npx --yes jsr publish --set-version "$VERSION" && return 0
  published && { info "${NAME}@${VERSION} is on JSR despite the client error"; return 0; }
  return 1
}

if published; then
  info "${NAME}@${VERSION} is already on JSR"
  exit 0
fi

retry "$ATTEMPTS" "$RETRY_DELAY" attempt || die "failed to publish ${NAME}@${VERSION} to JSR"
info "Published ${NAME}@${VERSION} to JSR"
