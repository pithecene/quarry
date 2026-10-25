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
# highest release published on its own registry. A registry that can't be
# read leaves its pointer unchanged and fails the job.
#
# Tunables (environment): PROBE_ATTEMPTS (3), PROBE_RETRY_DELAY (5 s,
# doubled per retry) for registry lookups.

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

# Probes come from lib.sh (PRESENT, ABSENT, LOOKUP_FAILED). A lookup that
# fails is never read as "not published", or a pointer would silently move
# to an older release.
image_probe() {
  local out
  out=$(docker buildx imagetools inspect "$1" 2>&1 >/dev/null) && return "$PRESENT"
  [[ "$out" == *": not found"* ]] && return "$ABSENT"
  info "lookup of $1 failed: $(tail -n 1 <<<"$out")"
  return "$LOOKUP_FAILED"
}

npm_published() { lookup npm_probe "$NAME" "$1"; }

images_pushed() {
  lookup image_probe "${IMAGE}:$1" || return
  lookup image_probe "${IMAGE}:$1-slim"
}

# first_where <predicate>: prints the highest version for which <predicate>
# holds. Returns ABSENT if none does, LOOKUP_FAILED as soon as any version
# can't be checked: a lower version is never chosen over one whose state is
# unknown.
first_where() {
  local v rc
  while read -r v; do
    rc=0
    "$1" "$v" || rc=$?
    ((rc == PRESENT)) && { echo "$v"; return "$PRESENT"; }
    ((rc == ABSENT)) || return "$LOOKUP_FAILED"
  done <<<"$VERSIONS"
  return "$ABSENT"
}

# converge <pointer> <predicate> <point> <unknown> <none>: points <pointer>
# at the highest version for which <predicate> holds, by calling <point>
# with it. <unknown> and <none> describe a failed lookup and an empty
# result. A pointer whose target cannot be determined is left unchanged and
# fails.
converge() {
  local pointer="$1" predicate="$2" point="$3" unknown="$4" none="$5" target rc=0
  target=$(first_where "$predicate") || rc=$?
  if ((rc == LOOKUP_FAILED)); then
    info "${unknown}; ${pointer} unchanged"
    return 1
  fi
  if ((rc == ABSENT)); then
    info "${none}; ${pointer} unchanged"
    return 0
  fi
  "$point" "$target" || return 1
  info "${pointer} -> ${target}"
}

point_npm() { npm dist-tag add "${NAME}@$1" latest; }

point_images() {
  docker buildx imagetools create -t "${IMAGE}:latest" "${IMAGE}:$1" || return 1
  docker buildx imagetools create -t "${IMAGE}:slim" "${IMAGE}:$1-slim"
}

# Each registry is attempted even if the other fails; the job still fails
# so the stale pointer is visible and a re-run repairs it.
FAILED=()
converge "npm ${NAME} latest" npm_published point_npm \
  "could not determine which releases of ${NAME} are published" \
  "no release of ${NAME} is published" \
  || FAILED+=("npm latest")
converge "${IMAGE}:latest and :slim" images_pushed point_images \
  "could not determine which release images are pushed" \
  "no release images are pushed" \
  || FAILED+=("GHCR :latest/:slim")
((${#FAILED[@]} == 0)) || die "failed to converge: ${FAILED[*]}"
