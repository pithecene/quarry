#!/usr/bin/env bash
# Cross-compile quarry for every released platform into quarry/dist.
# Usage: scripts/ci/go-cross.sh <commit>

set -euo pipefail

cd quarry
mkdir -p dist
for pair in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64; do
  GOOS="${pair%/*}" GOARCH="${pair#*/}" CGO_ENABLED=0 \
    go build -ldflags "-X main.commit=$1" -o "dist/quarry_${pair/\//_}" ./cmd/quarry
  echo "✅ $pair"
done
