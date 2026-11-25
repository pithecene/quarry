#!/usr/bin/env bash
# Verify a published image of a release: the full image extracts with its own
# browser, and the slim image with a browser beside it.
# Usage: scripts/ci/verify-image.sh <tag> [-slim]

set -euo pipefail

# The checksum names and the exit report that this reads date from v0.20.0.
if [[ ! "$1" =~ ^v[0-9]+\.([0-9]+)\.[0-9]+$ ]] || [[ "$1" == v0.* && "${BASH_REMATCH[1]}" -lt 20 ]]; then
  echo "expected a release from v0.20.0 on, got $1" >&2
  exit 1
fi
# A release that hangs fails here, whatever its own limits are.
bounded() { perl -e 'alarm shift; exec @ARGV or die' "$@"; }
image="ghcr.io/pithecene/quarry:${1#v}${2:-}"
docker pull --quiet "$image"
docker image inspect --format '{{index .RepoDigests 0}}' "$image"
bounded 60 docker run --rm "$image" version | jq -e --arg version "${1#v}" '.version == $version'
if [[ -z "${2:-}" ]]; then
  bounded 300 scripts/ci/container-smoke.sh "$image"
  exit
fi

# Chrome for Testing has no arm64 build, so the browser is a separate image.
browser=$(docker run --detach chromedp/headless-shell:147.0.7727.24)
trap 'docker rm --force "$browser" > /dev/null' EXIT
# The run shares the browser's network and finds its endpoint there.
bounded 300 docker run --rm --network "container:$browser" --volume "$PWD/scripts/ci/smoke:/work/scripts:ro" \
  --entrypoint sh "$image" -ec '
    endpoint=
    for _ in $(seq 30); do
      endpoint=$(node -e "fetch(\"http://127.0.0.1:9222/json/version\").then((r) => r.json()).then((v) => console.log(v.webSocketDebuggerUrl), () => {})")
      [ -n "$endpoint" ] && break
      sleep 1
    done
    case "$endpoint" in ws://*) ;; *) echo "the browser is not ready" >&2 && exit 1 ;; esac
    mkdir /tmp/data
    quarry run --script ./scripts/page.ts --run-id smoke --source ci --category smoke \
      --browser-ws-endpoint "$endpoint" --report /tmp/report.json \
      --storage-backend fs --storage-path /tmp/data >&2
    cat /tmp/report.json' | jq -e '.outcome == "success" and .recorded and .exit_code == 0'
