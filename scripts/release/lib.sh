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
