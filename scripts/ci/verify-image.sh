#!/usr/bin/env bash
# Verify a published image of a release: the full image extracts with its own
# browser, and the slim image with a browser beside it.
# Usage: scripts/ci/verify-image.sh <tag> [-slim]

set -euo pipefail

image="ghcr.io/pithecene/quarry:${1#v}${2:-}"
docker pull --quiet "$image"
docker image inspect --format '{{index .RepoDigests 0}}' "$image"
docker run --rm "$image" version | jq -e --arg version "${1#v}" '.version == $version'
if [[ -z "${2:-}" ]]; then
  scripts/ci/container-smoke.sh "$image"
  exit
fi

# Chrome for Testing has no arm64 build, so the browser is a separate image.
browser=$(docker run --detach chromedp/headless-shell:147.0.7727.24)
trap 'docker rm --force "$browser" > /dev/null' EXIT
# The run shares the browser's network and finds its endpoint there.
docker run --rm --network "container:$browser" --volume "$PWD/scripts/ci/smoke:/work/scripts:ro" \
  --entrypoint sh "$image" -c '
    for _ in $(seq 30); do
      endpoint=$(node -e "fetch(\"http://127.0.0.1:9222/json/version\").then((r) => r.json()).then((v) => console.log(v.webSocketDebuggerUrl), () => {})")
      [ -n "$endpoint" ] && break
      sleep 1
    done
    mkdir /tmp/data
    quarry run --script ./scripts/page.ts --run-id smoke --source ci --category smoke \
      --browser-ws-endpoint "$endpoint" --report /tmp/report.json \
      --storage-backend fs --storage-path /tmp/data >&2
    cat /tmp/report.json' | jq -e '.outcome == "success" and .recorded'
