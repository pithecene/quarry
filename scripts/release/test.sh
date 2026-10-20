#!/usr/bin/env bash
# Tests for scripts/release/*. Usage: scripts/release/test.sh
#
# Registry and git interactions are replaced by stub executables on PATH
# whose behaviour is driven by files in a per-case state directory, so every
# branch runs offline and deterministically.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

PASS=0
FAIL=0

# --- stubs -------------------------------------------------------------------

STUBS="$WORK/bin"
mkdir -p "$STUBS"

# npm view <pkg>@<v> version: succeeds iff <v> is listed in npm_versions.
# npm dist-tag add <pkg>@<v> latest: recorded.
cat >"$STUBS/npm" <<'EOF'
#!/usr/bin/env bash
echo "npm $*" >>"$STATE/calls"
case "$1" in
  view) grep -qx "${2##*@}" "$STATE/npm_versions" 2>/dev/null ;;
  dist-tag) exit 0 ;;
  *) exit 2 ;;
esac
EOF

# pnpm publish: behaviour from $STATE/pnpm_mode; publishes 1.2.3.
# "overtaken" simulates a newer release (v1.3.0) being tagged and published
# while this publish is in flight.
cat >"$STUBS/pnpm" <<'EOF'
#!/usr/bin/env bash
echo "pnpm $*" >>"$STATE/calls"
case "$(cat "$STATE/pnpm_mode" 2>/dev/null || echo ok)" in
  ok) echo 1.2.3 >>"$STATE/npm_versions" ;;
  fail) exit 1 ;;
  race) echo 1.2.3 >>"$STATE/npm_versions"; exit 1 ;;
  overtaken) echo 1.2.3 >>"$STATE/npm_versions"; echo 1.3.0 >>"$STATE/npm_versions"; echo v1.3.0 >>"$RELEASE_TAGS_FILE" ;;
esac
EOF

# npx jsr publish: behaviour from $STATE/npx_mode.
cat >"$STUBS/npx" <<'EOF'
#!/usr/bin/env bash
echo "npx $*" >>"$STATE/calls"
case "$(cat "$STATE/npx_mode" 2>/dev/null || echo ok)" in
  ok) touch "$STATE/jsr_published" ;;
  fail) exit 1 ;;
  poll_fail) touch "$STATE/jsr_published"; exit 1 ;;
  hang) sleep 30 ;;
esac
EOF

# docker buildx imagetools inspect <ref>: succeeds iff <ref> is listed in
# images. create -t <alias> <src>: recorded; with docker_mode "overtaken",
# the first create also tags and pushes a newer release (v1.3.0), simulating
# a newer run finishing during this write.
cat >"$STUBS/docker" <<'EOF'
#!/usr/bin/env bash
echo "docker $*" >>"$STATE/calls"
case "$3" in
  inspect) grep -qx "$4" "$STATE/images" 2>/dev/null ;;
  create)
    if [[ "$(cat "$STATE/docker_mode" 2>/dev/null)" == overtaken ]]; then
      rm "$STATE/docker_mode"
      echo v1.3.0 >>"$RELEASE_TAGS_FILE"
      printf '%s\n' ghcr.io/x/y:1.3.0 ghcr.io/x/y:1.3.0-slim >>"$STATE/images"
    fi ;;
esac
EOF

# curl -w '%{http_code}' <url>: 200 iff the version is on JSR.
cat >"$STUBS/curl" <<'EOF'
#!/usr/bin/env bash
if [[ -f "$STATE/jsr_published" ]]; then printf 200; else printf 404; fi
EOF

chmod +x "$STUBS"/*
export PATH="$STUBS:$PATH"

# --- harness -----------------------------------------------------------------

# new_case: fresh state dir and a package dir holding package.json + jsr.json.
new_case() {
  export STATE="$WORK/state.$((PASS + FAIL))"
  PKG="$WORK/pkg.$((PASS + FAIL))"
  mkdir -p "$STATE" "$PKG"
  : >"$STATE/calls"
  printf '{"name":"@scope/pkg","version":"1.2.3"}\n' >"$PKG/package.json"
  printf '{"name":"@scope/pkg"}\n' >"$PKG/jsr.json"
}

calls() { grep -c "^$1 " "$STATE/calls" || true; }

expect() {
  local desc="$1" want="$2"
  shift 2
  local got=0
  "$@" >/dev/null 2>&1 || got=$?
  if { [[ "$want" == ok ]] && ((got == 0)); } || { [[ "$want" == fail ]] && ((got != 0)); }; then
    PASS=$((PASS + 1))
  else
    FAIL=$((FAIL + 1))
    echo "FAIL: $desc (expected $want, exit $got)"
  fi
}

check() {
  local desc="$1"
  shift
  if "$@"; then PASS=$((PASS + 1)); else FAIL=$((FAIL + 1)); echo "FAIL: $desc"; fi
}

# tags <tag>...: the released tags the scripts see (via RELEASE_TAGS_FILE).
tags() { printf '%s\n' "$@" >"$WORK/tags"; }
export RELEASE_TAGS_FILE="$WORK/tags"

# --- assert-highest-tag.sh ---------------------------------------------------

A="$HERE/assert-highest-tag.sh"
tags v0.13.4 v0.13.5 v0.13.6
expect "higher tag passes" ok "$A" v0.13.7
expect "re-run of the highest tag passes" ok "$A" v0.13.6
expect "older tag is rejected" fail "$A" v0.13.5
tags v0.9.0 v0.10.0
expect "versions compare numerically, not lexically" fail "$A" v0.9.1
expect "minor bump above a two-digit minor passes" ok "$A" v0.11.0
tags v1.0.0-rc.1 latest v0.13.6
expect "non-release tags are ignored" ok "$A" v0.13.6
tags
expect "first release passes" ok "$A" v0.1.0
expect "malformed tag is rejected" fail "$A" 0.13.6
expect "missing tag is rejected" fail "$A"
tags v0.13.6
expect "unreadable tag list fails instead of passing" fail env RELEASE_TAGS_FILE="$WORK/missing" "$A" v0.13.7

# --- publish-npm.sh ----------------------------------------------------------

N="$HERE/publish-npm.sh"

new_case
tags v1.2.3
echo 1.2.3 >"$STATE/npm_versions"
expect "npm: already published succeeds" ok "$N" "$PKG"
check "npm: already published does not republish" [ "$(calls pnpm)" -eq 0 ]

new_case
tags v1.2.2 v1.2.3
expect "npm: unpublished version publishes" ok "$N" "$PKG"
check "npm: publishes exactly once" [ "$(calls pnpm)" -eq 1 ]
check "npm: the highest release takes latest" grep -q -- "--tag latest" "$STATE/calls"
check "npm: no dist-tag correction needed" [ "$(grep -c 'dist-tag' "$STATE/calls")" -eq 0 ]

new_case
tags v1.2.3 v1.3.0
echo 1.3.0 >"$STATE/npm_versions"
expect "npm: a release already overtaken still publishes" ok "$N" "$PKG"
check "npm: an overtaken release publishes under previous" grep -q -- "--tag previous" "$STATE/calls"
check "npm: latest is kept on the newer published release" grep -q "dist-tag add @scope/pkg@1.3.0 latest" "$STATE/calls"

new_case
tags v1.2.3 v1.3.0
expect "npm: overtaken by a release not yet published" ok "$N" "$PKG"
check "npm: latest is left for the newer release's own run" [ "$(grep -c 'dist-tag' "$STATE/calls")" -eq 0 ]

new_case
tags v1.2.3
echo overtaken >"$STATE/pnpm_mode"
expect "npm: newer release tagged and published during our publish" ok "$N" "$PKG"
check "npm: the re-check restores latest to the newer release" grep -q "dist-tag add @scope/pkg@1.3.0 latest" "$STATE/calls"

new_case
tags v1.2.3
echo race >"$STATE/pnpm_mode"
expect "npm: concurrent publish counts as success" ok "$N" "$PKG"

new_case
tags v1.2.3
echo fail >"$STATE/pnpm_mode"
expect "npm: real publish failure fails" fail "$N" "$PKG"

new_case
expect "npm: unreadable tag list fails" fail env RELEASE_TAGS_FILE="$WORK/missing" "$N" "$PKG"

# --- publish-jsr.sh ----------------------------------------------------------

J="$HERE/publish-jsr.sh"
export JSR_RETRY_DELAY=0

new_case
touch "$STATE/jsr_published"
expect "jsr: already published succeeds" ok "$J" "$PKG" 1.2.3
check "jsr: already published does not republish" [ "$(calls npx)" -eq 0 ]

new_case
expect "jsr: unpublished version publishes" ok "$J" "$PKG" 1.2.3
check "jsr: publishes exactly once" [ "$(calls npx)" -eq 1 ]

new_case
echo poll_fail >"$STATE/npx_mode"
expect "jsr: upload accepted but status poll failed counts as success" ok "$J" "$PKG" 1.2.3
check "jsr: no retry after the record shows the version" [ "$(calls npx)" -eq 1 ]

new_case
echo fail >"$STATE/npx_mode"
expect "jsr: persistent failure fails" fail env JSR_ATTEMPTS=3 "$J" "$PKG" 1.2.3
check "jsr: retries the configured number of attempts" [ "$(calls npx)" -eq 3 ]

new_case
echo hang >"$STATE/npx_mode"
expect "jsr: a stalled attempt is cut off and the job fails" fail \
  env JSR_ATTEMPTS=2 JSR_ATTEMPT_TIMEOUT=1 "$J" "$PKG" 1.2.3
check "jsr: each stalled attempt is retried" [ "$(calls npx)" -eq 2 ]

# --- move-image-aliases.sh ----------------------------------------------------

M="$HERE/move-image-aliases.sh"
img() { printf '%s\n' "$@" >"$STATE/images"; }

new_case
tags v1.2.2 v1.2.3
img ghcr.io/x/y:1.2.3 ghcr.io/x/y:1.2.3-slim
expect "aliases: the highest release moves them" ok "$M" ghcr.io/x/y 1.2.3
check "aliases: :latest -> the release" grep -q -- "-t ghcr.io/x/y:latest ghcr.io/x/y:1.2.3" "$STATE/calls"
check "aliases: :slim -> the slim release" grep -q -- "-t ghcr.io/x/y:slim ghcr.io/x/y:1.2.3-slim" "$STATE/calls"
check "aliases: written once" [ "$(grep -c ' create ' "$STATE/calls")" -eq 2 ]

new_case
tags v1.2.3 v1.3.0
img ghcr.io/x/y:1.2.3 ghcr.io/x/y:1.2.3-slim ghcr.io/x/y:1.3.0 ghcr.io/x/y:1.3.0-slim
expect "aliases: an overtaken release succeeds" ok "$M" ghcr.io/x/y 1.2.3
check "aliases: never pointed at the overtaken release" [ "$(grep -c 'ghcr.io/x/y:1.2.3' "$STATE/calls")" -eq 0 ]
check "aliases: kept on the newer pushed release" grep -q -- "-t ghcr.io/x/y:latest ghcr.io/x/y:1.3.0" "$STATE/calls"

new_case
tags v1.2.3 v1.3.0
img ghcr.io/x/y:1.2.3 ghcr.io/x/y:1.2.3-slim
expect "aliases: overtaken by a release not yet pushed" ok "$M" ghcr.io/x/y 1.2.3
check "aliases: left for the newer release's own run" [ "$(grep -c ' create ' "$STATE/calls")" -eq 0 ]

new_case
tags v1.2.3
img ghcr.io/x/y:1.2.3 ghcr.io/x/y:1.2.3-slim
echo overtaken >"$STATE/docker_mode"
expect "aliases: newer release finishes during our write" ok "$M" ghcr.io/x/y 1.2.3
check "aliases: the re-check restores :latest to the newer release" \
  [ "$(grep -- ' create -t ghcr.io/x/y:latest' "$STATE/calls" | tail -n 1)" = "docker buildx imagetools create -t ghcr.io/x/y:latest ghcr.io/x/y:1.3.0" ]
check "aliases: the re-check restores :slim to the newer release" \
  [ "$(grep -- ' create -t ghcr.io/x/y:slim' "$STATE/calls" | tail -n 1)" = "docker buildx imagetools create -t ghcr.io/x/y:slim ghcr.io/x/y:1.3.0-slim" ]

new_case
expect "aliases: unreadable tag list fails" fail env RELEASE_TAGS_FILE="$WORK/missing" "$M" ghcr.io/x/y 1.2.3
check "aliases: unreadable tag list moves nothing" [ "$(grep -c ' create ' "$STATE/calls")" -eq 0 ]

# --- result ------------------------------------------------------------------

echo "scripts/release: ${PASS} passed, ${FAIL} failed"
((FAIL == 0))
