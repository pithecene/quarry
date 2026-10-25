#!/usr/bin/env bash
# Shared helpers for scripts/release/*. Sourced, not executed.

# Fail with a GitHub Actions error annotation (plain text outside Actions).
die() {
  echo "::error::$*" >&2
  exit 1
}

info() {
  echo "$*" >&2
}

# is_release_tag <tag>: true for vX.Y.Z.
is_release_tag() {
  [[ "$1" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]
}

# highest_tag: reads tags on stdin (one per line, any extra text ignored),
# prints the highest vX.Y.Z by version order, or nothing if there is none.
highest_tag() {
  grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | sort -V | tail -n 1 || true
}

# released_tags: release tags on origin, one per line. RELEASE_TAGS_FILE
# substitutes a fixed list (tests).
released_tags() {
  if [[ -n "${RELEASE_TAGS_FILE:-}" ]]; then
    cat "$RELEASE_TAGS_FILE"
    return
  fi
  git ls-remote --tags --refs origin 'v*' | sed 's|.*refs/tags/||'
}

# tag_is_highest <tag>: true if <tag> is at least as high as every released
# tag (equal counts, so a re-run of the current release qualifies). Fails
# the caller (non-zero, distinct from "not highest") if tags can't be read.
tag_is_highest() {
  local tags highest
  tags=$(released_tags) || die "could not list release tags on origin"
  highest=$(printf '%s\n' "$tags" | highest_tag)
  [[ -z "$highest" ]] && return 0
  [[ "$(printf '%s\n%s\n' "$highest" "$1" | highest_tag)" == "$1" ]]
}

# retry <attempts> <initial-delay-seconds> <command...>: runs the command
# until it succeeds, doubling the delay between attempts.
retry() {
  local attempts="$1" delay="$2" n
  shift 2
  for ((n = 1; n <= attempts; n++)); do
    info "Attempt ${n}/${attempts}: $*"
    "$@" && return 0
    if ((n < attempts)); then
      info "Retrying in ${delay}s"
      sleep "$delay"
      delay=$((delay * 2))
    fi
  done
  return 1
}

# Registry probes return PRESENT, ABSENT, or LOOKUP_FAILED. Only the
# registry's own "not found" answer counts as absent: an unreachable
# registry or an auth failure must never be read as "not published".
PRESENT=0
ABSENT=1
LOOKUP_FAILED=2

# lookup <probe> <args...>: runs <probe>, retrying only LOOKUP_FAILED,
# doubling the delay between attempts. Returns the probe's last answer.
# Tunables (environment): PROBE_ATTEMPTS (3), PROBE_RETRY_DELAY (5 s).
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

# npm_probe <name> <version>: is <name>@<version> on the npm registry?
npm_probe() {
  local out
  if out=$(npm view "$1@$2" version 2>&1); then
    [[ "$(tail -n 1 <<<"$out")" == "$2" ]] && return "$PRESENT"
    return "$ABSENT"
  fi
  [[ "$out" == *E404* ]] && return "$ABSENT"
  info "npm lookup of $1@$2 failed: $(tail -n 1 <<<"$out")"
  return "$LOOKUP_FAILED"
}
