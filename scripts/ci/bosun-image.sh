#!/usr/bin/env bash
# Print the Bosun image that CI's check job pins.
# Usage: scripts/ci/bosun-image.sh

set -euo pipefail

grep -m1 -oE 'ghcr.io/pithecene/bosun:[^ "]+' .github/workflows/check.yaml
