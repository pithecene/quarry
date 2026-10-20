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

# released_tags: release tags on origin, one per line.
released_tags() {
  git ls-remote --tags --refs origin 'v*' | sed 's|.*refs/tags/||'
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
