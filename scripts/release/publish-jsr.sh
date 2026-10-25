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

# jsr_probe: 200 is present and 404 absent. Anything else, including no
# response, is a failed lookup.
jsr_probe() {
  local code
  code=$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' "$META_URL") || true
  case "$code" in
    200) return "$PRESENT" ;;
    404) return "$ABSENT" ;;
  esac
  info "JSR lookup of ${NAME}@${VERSION} failed: HTTP ${code:-none}"
  return "$LOOKUP_FAILED"
}

published() { lookup jsr_probe; }

# After a failed attempt only a confirmed PRESENT counts as published; a
# failed lookup leads to another attempt, which JSR refuses if the version
# exists.
attempt() {
  timeout "$ATTEMPT_TIMEOUT" npx --yes jsr publish --set-version "$VERSION" && return 0
  published && { info "${NAME}@${VERSION} is on JSR despite the client error"; return 0; }
  return 1
}

rc=0
published || rc=$?
if ((rc == PRESENT)); then
  info "${NAME}@${VERSION} is already on JSR"
  exit 0
fi
((rc == LOOKUP_FAILED)) && info "could not check whether ${NAME}@${VERSION} is on JSR; publishing"

retry "$ATTEMPTS" "$RETRY_DELAY" attempt || die "failed to publish ${NAME}@${VERSION} to JSR"
info "Published ${NAME}@${VERSION} to JSR"
