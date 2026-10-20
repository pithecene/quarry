#!/usr/bin/env bash
# Decide which moving channels a release tag may claim.
# Usage: scripts/release-channel.sh <tag>   (e.g. v0.13.6)
#
# Prints GITHUB_OUTPUT-style lines:
#   version=0.13.6
#   latest=true|false     true only if <tag> is the highest vX.Y.Z tag on origin
#   npm_tag=latest|previous
#
# Releases can be published out of order (a re-released older tag after a
# newer one). Moving pointers — npm's `latest` dist-tag, GHCR's :latest and
# :slim — must only ever move to the highest released version, so an older
# release publishes its versioned artifacts without claiming them.

set -euo pipefail

TAG="${1:?Usage: release-channel.sh <tag>}"
[[ "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "not a release tag: $TAG" >&2; exit 2; }

HIGHEST=$(git ls-remote --tags --refs origin 'v*' \
  | sed 's|.*refs/tags/||' \
  | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' \
  | sort -V | tail -1)

echo "version=${TAG#v}"
if [[ "$TAG" == "$HIGHEST" ]]; then
  echo "latest=true"
  echo "npm_tag=latest"
else
  echo "latest=false"
  echo "npm_tag=previous"
fi
echo "Release channel for $TAG (highest $HIGHEST)" >&2
