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

# Registry probes return 0 (present), 1 (absent) or 2 (lookup failed). Only
# the registry's own "not found" answer counts as absent: an unreachable
# registry or an auth failure must never be read as "not published", or the
# pointer would silently move to an older release.
PRESENT=0
ABSENT=1
LOOKUP_FAILED=2

npm_probe() {
  local out
  if out=$(npm view "${NAME}@$1" version 2>&1); then
    [[ "$(tail -n 1 <<<"$out")" == "$1" ]] && return "$PRESENT"
    return "$ABSENT"
  fi
  [[ "$out" == *E404* ]] && return "$ABSENT"
  info "npm lookup of ${NAME}@$1 failed: $(tail -n 1 <<<"$out")"
  return "$LOOKUP_FAILED"
}

image_probe() {
  local out
  out=$(docker buildx imagetools inspect "$1" 2>&1 >/dev/null) && return "$PRESENT"
  [[ "$out" == *": not found"* ]] && return "$ABSENT"
  info "lookup of $1 failed: $(tail -n 1 <<<"$out")"
  return "$LOOKUP_FAILED"
}

# lookup <probe> <arg>: runs <probe>, retrying only failed lookups, doubling
# the delay between attempts.
lookup() {
  local attempts="${PROBE_ATTEMPTS:-3}" delay="${PROBE_RETRY_DELAY:-5}" n rc
  for ((n = 1; ; n++)); do
    rc=0
    "$@" || rc=$?
    ((rc == LOOKUP_FAILED && n < attempts)) || return "$rc"
    sleep "$delay"
    delay=$((delay * 2))
  done
}

npm_published() { lookup npm_probe "$1"; }

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

converge_npm() {
  local target rc=0
  target=$(first_where npm_published) || rc=$?
  if ((rc == LOOKUP_FAILED)); then
    info "could not determine which releases of ${NAME} are published; npm latest unchanged"
    return 1
  fi
  if ((rc == ABSENT)); then
    info "no release of ${NAME} is published; npm latest unchanged"
    return 0
  fi
  npm dist-tag add "${NAME}@${target}" latest || return 1
  info "npm ${NAME} latest -> ${target}"
}

converge_images() {
  local target rc=0
  target=$(first_where images_pushed) || rc=$?
  if ((rc == LOOKUP_FAILED)); then
    info "could not determine which release images are pushed; ${IMAGE}:latest and :slim unchanged"
    return 1
  fi
  if ((rc == ABSENT)); then
    info "no release images are pushed; ${IMAGE}:latest and :slim unchanged"
    return 0
  fi
  docker buildx imagetools create -t "${IMAGE}:latest" "${IMAGE}:${target}" || return 1
  docker buildx imagetools create -t "${IMAGE}:slim" "${IMAGE}:${target}-slim" || return 1
  info "${IMAGE}:latest -> ${target}, :slim -> ${target}-slim"
}

# Each registry is attempted even if the other fails; the job still fails
# so the stale pointer is visible and a re-run repairs it.
FAILED=()
converge_npm || FAILED+=("npm latest")
converge_images || FAILED+=("GHCR :latest/:slim")
((${#FAILED[@]} == 0)) || die "failed to converge: ${FAILED[*]}"
