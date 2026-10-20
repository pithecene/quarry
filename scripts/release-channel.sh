#!/usr/bin/env bash
# Resolve the release version and the highest released version.
# Usage: scripts/release-channel.sh <tag>   (e.g. v0.13.5)
#
# Prints GITHUB_OUTPUT-style lines:
#   version=0.13.5        this release, without the v
#   highest=0.13.6        highest vX.Y.Z tag on origin, without the v
#
# Releases can be published out of order (a re-released older tag after a
# newer one). Moving pointers — npm's `latest` dist-tag, GHCR's :latest and
# :slim — are never pointed at "this release"; every release run points them
# at `highest` (when its artifacts exist), so whichever run writes last
# writes the right value and no lock is needed.
#
# Exits non-zero if the tag is malformed or origin's tags cannot be read;
# callers must not fall back silently.

set -euo pipefail

TAG="${1:?Usage: release-channel.sh <tag>}"
[[ "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "not a release tag: $TAG" >&2; exit 2; }

TAGS=$(git ls-remote --tags --refs origin 'v*')
HIGHEST=$(printf '%s\n' "$TAGS" \
  | sed 's|.*refs/tags/||' \
  | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' \
  | sort -V | tail -1 || true)
[[ -n "$HIGHEST" ]] || { echo "no release tags found on origin" >&2; exit 3; }

echo "version=${TAG#v}"
echo "highest=${HIGHEST#v}"
echo "Release $TAG; highest released ${HIGHEST}" >&2
