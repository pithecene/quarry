#!/usr/bin/env bash
# Run the internal check (bosun.yaml) in an image, with Go caches under .bosun.
# Usage: scripts/ci/internal-check.sh <image>

set -euo pipefail

root=$PWD
# A linked worktree's .git names the repository's .git directory, which mounts
# at the same path so that Git finds it in the container.
git_common=$(cd "$(git rev-parse --git-common-dir)" && pwd)
mkdir -p .bosun/go
docker run --rm --pull always -v "$root:/work" -v "$git_common:$git_common" \
  -e GOMODCACHE=/work/.bosun/go/mod -e GOCACHE=/work/.bosun/go/build \
  -e GOLANGCI_LINT_CACHE=/work/.bosun/go/lint \
  "$1" check
