#!/usr/bin/env bash
# Verify a published image of a release: the full image extracts with its own
# browser, and the slim image with a browser beside it.
# Usage: scripts/ci/verify-image.sh <tag> [-slim]

set -euo pipefail

# A release records its image digests from v0.20.2 on.
number="(0|[1-9][0-9]*)"
if [[ ! "$1" =~ ^v$number\.$number\.$number$ || ("${BASH_REMATCH[1]}" == 0 && ("${BASH_REMATCH[2]}" -lt 20 || ("${BASH_REMATCH[2]}" == 20 && "${BASH_REMATCH[3]}" -lt 2))) ]]; then
  echo "expected a release from v0.20.2 on, got $1" >&2
  exit 1
fi
# The job that calls this bounds it.
image="ghcr.io/pithecene/quarry:${1#v}${2:-}"
dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
repo=$(gh repo view --json nameWithOwner --jq .nameWithOwner)
run=$(gh run list --repo "$repo" --workflow release.yml --branch "$1" --event push --limit 1 --json databaseId --jq '.[0].databaseId')
gh run download "$run" --repo "$repo" --name image-digests --dir "$dir"
kind=full
if [[ -n "${2:-}" ]]; then kind="${2#-}"; fi
digest=$(jq -er --arg kind "$kind" '.[$kind] | select(test("^sha256:[0-9a-f]{64}$"))' "$dir/image-digests.json")
docker pull --quiet "$image"
docker image inspect "$image" | jq -e --arg ref "ghcr.io/pithecene/quarry@$digest" '.[0].RepoDigests | index($ref) != null'
docker run --rm "$image" version | jq -e --arg version "${1#v}" '.version == $version'
if [[ -z "${2:-}" ]]; then
  scripts/ci/container-smoke.sh "$image"
  exit
fi

# Chrome for Testing has no arm64 build, so the browser is a separate image.
browser=$(docker run --detach chromedp/headless-shell:147.0.7727.24)
trap 'docker rm --force "$browser" > /dev/null; rm -rf "$dir"' EXIT
# The run shares the browser's network and finds its endpoint there.
# shellcheck disable=SC2016 # the container expands the script
docker run --rm --network "container:$browser" --volume "$PWD/scripts/ci/smoke:/work/scripts:ro" \
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
    cat /tmp/report.json' | jq -e '.outcome == "success" and .recorded == true and .exit_code == 0'
