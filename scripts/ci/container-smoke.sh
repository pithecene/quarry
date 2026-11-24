#!/usr/bin/env bash
# Run a page script in the image as a numeric UID, which has no account, home, or
# cache there, on a read-only root. The browser run proves that the embedded
# executor needs none of them.
# Usage: scripts/ci/container-smoke.sh <image>

set -euo pipefail

image="$1"
data=$(mktemp -d)
# The container's UID owns what it writes, so it also deletes it.
cleanup() {
  docker run --rm --user 54321:54321 --entrypoint find -v "$data:/data" "$image" /data -mindepth 1 -delete
  rmdir "$data"
}
trap cleanup EXIT
chmod 777 "$data"
docker run --rm --user 54321:54321 --read-only \
  --tmpfs /tmp:mode=1777 --shm-size 1g -e HOME=/tmp \
  -v "$PWD/scripts/ci/smoke:/work/scripts:ro" \
  -v "$data:/work/data" \
  "$image" run --script ./scripts/page.ts --run-id smoke \
  --source ci --category smoke --no-browser-reuse \
  --storage-backend fs --storage-path ./data
find "$data" -name end.json -exec jq -e '.outcome == "success"' {} + | grep -qx true
