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

# fail_lookup <counter>: consumes one from $STATE/<counter> (if set) and
# succeeds, simulating an unreachable registry for that many lookups.
export FAIL_LOOKUP="$WORK/fail-lookup.sh"
cat >"$FAIL_LOOKUP" <<'EOF'
fail_lookup() {
  local n
  n=$(cat "$STATE/$1" 2>/dev/null || echo 0)
  ((n > 0)) || return 1
  echo $((n - 1)) >"$STATE/$1"
}
EOF

# npm view <pkg>@<v> version: prints <v> iff it is listed in npm_versions,
# otherwise fails with E404 (or ECONNREFUSED while npm_lookup_errors lasts).
# npm dist-tag add <pkg>@<v> latest: recorded.
cat >"$STUBS/npm" <<'EOF'
#!/usr/bin/env bash
source "$FAIL_LOOKUP"
echo "npm $*" >>"$STATE/calls"
case "$1" in
  view)
    fail_lookup npm_lookup_errors && { echo "npm error code ECONNREFUSED" >&2; exit 1; }
    grep -qx "${2##*@}" "$STATE/npm_versions" 2>/dev/null && { echo "${2##*@}"; exit 0; }
    echo "npm error code E404" >&2
    exit 1
    ;;
  dist-tag) [[ ! -f "$STATE/npm_dist_tag_fails" ]] ;;
  *) exit 2 ;;
esac
EOF

# pnpm publish: behaviour from $STATE/pnpm_mode; publishes 1.2.3.
cat >"$STUBS/pnpm" <<'EOF'
#!/usr/bin/env bash
echo "pnpm $*" >>"$STATE/calls"
case "$(cat "$STATE/pnpm_mode" 2>/dev/null || echo ok)" in
  ok) echo 1.2.3 >>"$STATE/npm_versions" ;;
  fail) exit 1 ;;
  race) echo 1.2.3 >>"$STATE/npm_versions"; exit 1 ;;
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
# images, otherwise fails with "not found" (or a connection error while
# image_lookup_errors lasts). create -t <alias> <src>: recorded.
cat >"$STUBS/docker" <<'EOF'
#!/usr/bin/env bash
source "$FAIL_LOOKUP"
echo "docker $*" >>"$STATE/calls"
case "$3" in
  inspect)
    fail_lookup image_lookup_errors && { echo "ERROR: failed to do request: connection refused" >&2; exit 1; }
    grep -qx "$4" "$STATE/images" 2>/dev/null && exit 0
    echo "ERROR: $4: not found" >&2
    exit 1
    ;;
  create) exit 0 ;;
esac
EOF

# curl -w '%{http_code}' <url>: 200 iff the version is on JSR, otherwise
# 404; no response (code 000, exit 7) while jsr_lookup_errors lasts.
cat >"$STUBS/curl" <<'EOF'
#!/usr/bin/env bash
source "$FAIL_LOOKUP"
echo "curl $*" >>"$STATE/calls"
fail_lookup jsr_lookup_errors && { printf 000; exit 7; }
if [[ -f "$STATE/jsr_published" ]]; then printf 200; else printf 404; fi
EOF

# gh release view <tag> --json isDraft,isPrerelease -q <expr>: prints the
# release state from $STATE/release ("published", "a draft", "a
# prerelease"); fails with "release not found" when it is absent, or with
# an authentication error when $STATE/release_lookup_fails exists.
cat >"$STUBS/gh" <<'EOF2'
#!/usr/bin/env bash
echo "gh $*" >>"$STATE/calls"
[[ -f "$STATE/release_lookup_fails" ]] && { echo "HTTP 401: Bad credentials" >&2; exit 1; }
[[ -f "$STATE/release" ]] && { cat "$STATE/release"; exit 0; }
echo "release not found" >&2
exit 1
EOF2

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

# --- assert-release-published.sh ---------------------------------------------

P="$HERE/assert-release-published.sh"

new_case
echo published >"$STATE/release"
expect "a published release passes" ok "$P" v1.2.3

new_case
expect "a tag without a release is rejected" fail "$P" v1.2.3

new_case
echo "a draft" >"$STATE/release"
expect "a draft release is rejected" fail "$P" v1.2.3

new_case
echo "a prerelease" >"$STATE/release"
expect "a prerelease is rejected" fail "$P" v1.2.3

new_case
touch "$STATE/release_lookup_fails"
expect "a lookup failure fails" fail "$P" v1.2.3
"$P" v1.2.3 >"$STATE/out" 2>&1 || true
check "a lookup failure is reported as one" grep -q "could not read" "$STATE/out"
check "a lookup failure is not called a missing release" [ "$(grep -c missing "$STATE/out")" -eq 0 ]

expect "missing tag is rejected" fail "$P"

# --- publish-npm.sh ----------------------------------------------------------

N="$HERE/publish-npm.sh"
export PROBE_RETRY_DELAY=0

new_case
echo 1.2.3 >"$STATE/npm_versions"
expect "npm: already published succeeds" ok "$N" "$PKG"
check "npm: already published does not republish" [ "$(calls pnpm)" -eq 0 ]

new_case
expect "npm: unpublished version publishes" ok "$N" "$PKG"
check "npm: publishes exactly once" [ "$(calls pnpm)" -eq 1 ]
check "npm: publishes under the release dist-tag, never latest" grep -q -- "--tag release" "$STATE/calls"
check "npm: publishing never touches latest" [ "$(grep -c 'latest' "$STATE/calls")" -eq 0 ]

new_case
echo race >"$STATE/pnpm_mode"
expect "npm: concurrent publish counts as success" ok "$N" "$PKG"

new_case
echo fail >"$STATE/pnpm_mode"
expect "npm: real publish failure fails" fail "$N" "$PKG"

# Registry lookup failures (default PROBE_ATTEMPTS=3).

new_case
echo 1.2.3 >"$STATE/npm_versions"
echo 2 >"$STATE/npm_lookup_errors"
expect "npm: transient lookup failure is retried" ok "$N" "$PKG"
check "npm: a recovered lookup that finds the version does not publish" [ "$(calls pnpm)" -eq 0 ]

new_case
echo 3 >"$STATE/npm_lookup_errors"
expect "npm: unreadable registry still publishes" ok "$N" "$PKG"
check "npm: unreadable registry publishes once" [ "$(calls pnpm)" -eq 1 ]

new_case
echo fail >"$STATE/pnpm_mode"
echo 6 >"$STATE/npm_lookup_errors"
"$N" "$PKG" >"$STATE/out" 2>&1 || true
check "npm: failed publish with an unreadable registry fails" grep -q "could not check the registry" "$STATE/out"

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

new_case
touch "$STATE/jsr_published"
echo 2 >"$STATE/jsr_lookup_errors"
expect "jsr: transient lookup failure is retried" ok "$J" "$PKG" 1.2.3
check "jsr: a recovered lookup that finds the version does not publish" [ "$(calls npx)" -eq 0 ]

new_case
echo 3 >"$STATE/jsr_lookup_errors"
expect "jsr: unreadable registry still publishes" ok "$J" "$PKG" 1.2.3
check "jsr: unreadable registry publishes once" [ "$(calls npx)" -eq 1 ]

new_case
echo poll_fail >"$STATE/npx_mode"
echo 6 >"$STATE/jsr_lookup_errors"
expect "jsr: an unconfirmed upload is attempted again" ok env JSR_ATTEMPTS=2 "$J" "$PKG" 1.2.3
check "jsr: a failed lookup after an attempt is not read as published" [ "$(calls npx)" -eq 2 ]

# --- converge-pointers.sh -----------------------------------------------------

C="$HERE/converge-pointers.sh"
img() { printf '%s\n' "$@" >"$STATE/images"; }
last_alias() { grep -- " create -t ghcr.io/x/y:$1 " "$STATE/calls" | tail -n 1 | awk '{print $NF}'; }

new_case
tags v1.2.2 v1.2.3 junk v1.3.0-rc.1
printf '%s\n' 1.2.2 1.2.3 >"$STATE/npm_versions"
img ghcr.io/x/y:1.2.2 ghcr.io/x/y:1.2.2-slim ghcr.io/x/y:1.2.3 ghcr.io/x/y:1.2.3-slim
expect "converge: succeeds" ok "$C" "$PKG" ghcr.io/x/y
check "converge: npm latest -> highest published" grep -q "dist-tag add @scope/pkg@1.2.3 latest" "$STATE/calls"
check "converge: :latest -> highest pushed" [ "$(last_alias latest)" = "ghcr.io/x/y:1.2.3" ]
check "converge: :slim -> highest pushed slim" [ "$(last_alias slim)" = "ghcr.io/x/y:1.2.3-slim" ]

new_case
tags v0.9.0 v0.10.0
printf '%s\n' 0.9.0 0.10.0 >"$STATE/npm_versions"
img ghcr.io/x/y:0.9.0 ghcr.io/x/y:0.9.0-slim ghcr.io/x/y:0.10.0 ghcr.io/x/y:0.10.0-slim
expect "converge: numeric version order" ok "$C" "$PKG" ghcr.io/x/y
check "converge: 0.10.0 beats 0.9.0 on npm" grep -q "dist-tag add @scope/pkg@0.10.0 latest" "$STATE/calls"
check "converge: 0.10.0 beats 0.9.0 on GHCR" [ "$(last_alias latest)" = "ghcr.io/x/y:0.10.0" ]

new_case
tags v1.2.3 v1.3.0
echo 1.2.3 >"$STATE/npm_versions"
img ghcr.io/x/y:1.2.3 ghcr.io/x/y:1.2.3-slim
expect "converge: newer release tagged but not yet published" ok "$C" "$PKG" ghcr.io/x/y
check "converge: npm latest stays on the highest published" grep -q "dist-tag add @scope/pkg@1.2.3 latest" "$STATE/calls"
check "converge: :latest stays on the highest pushed" [ "$(last_alias latest)" = "ghcr.io/x/y:1.2.3" ]
check "converge: a not-found answer is not retried" [ "$(grep -c '^npm view' "$STATE/calls")" -eq 2 ]

new_case
tags v1.2.3 v1.3.0
printf '%s\n' 1.2.3 1.3.0 >"$STATE/npm_versions"
img ghcr.io/x/y:1.2.3 ghcr.io/x/y:1.2.3-slim ghcr.io/x/y:1.3.0
expect "converge: registries are converged independently" ok "$C" "$PKG" ghcr.io/x/y
check "converge: npm moves to the newer published release" grep -q "dist-tag add @scope/pkg@1.3.0 latest" "$STATE/calls"
check "converge: images wait for both newer images to be pushed" [ "$(last_alias latest)" = "ghcr.io/x/y:1.2.3" ]

new_case
tags v1.2.3 v1.3.0
printf '%s\n' 1.2.3 1.3.0 >"$STATE/npm_versions"
img ghcr.io/x/y:1.2.3 ghcr.io/x/y:1.2.3-slim ghcr.io/x/y:1.3.0 ghcr.io/x/y:1.3.0-slim
"$C" "$PKG" ghcr.io/x/y >/dev/null 2>&1
"$C" "$PKG" ghcr.io/x/y >/dev/null 2>&1
check "converge: idempotent, invoking run does not matter" [ "$(grep -c 'dist-tag add @scope/pkg@1.3.0 latest' "$STATE/calls")" -eq 2 ]
check "converge: never points at an older release" [ "$(grep -c 'dist-tag add @scope/pkg@1.2.3' "$STATE/calls")" -eq 0 ]

new_case
tags v1.2.3
expect "converge: nothing published leaves pointers unchanged" ok "$C" "$PKG" ghcr.io/x/y
check "converge: no npm write" [ "$(grep -c 'dist-tag' "$STATE/calls")" -eq 0 ]
check "converge: no image write" [ "$(grep -c ' create ' "$STATE/calls")" -eq 0 ]

new_case
tags v1.2.3
echo 1.2.3 >"$STATE/npm_versions"
img ghcr.io/x/y:1.2.3 ghcr.io/x/y:1.2.3-slim
touch "$STATE/npm_dist_tag_fails"
expect "converge: an npm pointer failure fails the job" fail "$C" "$PKG" ghcr.io/x/y
check "converge: images still converge when npm fails" [ "$(last_alias latest)" = "ghcr.io/x/y:1.2.3" ]

new_case
expect "converge: unreadable tag list fails" fail env RELEASE_TAGS_FILE="$WORK/missing" "$C" "$PKG" ghcr.io/x/y
check "converge: unreadable tag list writes nothing" [ "$(grep -c 'dist-tag\| create ' "$STATE/calls")" -eq 0 ]

# Registry lookup failures (default PROBE_ATTEMPTS=3).

new_case
tags v1.2.3 v1.3.0
printf '%s\n' 1.2.3 1.3.0 >"$STATE/npm_versions"
img ghcr.io/x/y:1.2.3 ghcr.io/x/y:1.2.3-slim ghcr.io/x/y:1.3.0 ghcr.io/x/y:1.3.0-slim
echo 2 >"$STATE/npm_lookup_errors"
expect "converge: transient npm lookup failure is retried" ok "$C" "$PKG" ghcr.io/x/y
check "converge: npm latest -> highest after recovery" grep -q "dist-tag add @scope/pkg@1.3.0 latest" "$STATE/calls"

new_case
tags v1.2.3 v1.3.0
printf '%s\n' 1.2.3 1.3.0 >"$STATE/npm_versions"
img ghcr.io/x/y:1.2.3 ghcr.io/x/y:1.2.3-slim ghcr.io/x/y:1.3.0 ghcr.io/x/y:1.3.0-slim
echo 3 >"$STATE/npm_lookup_errors"
expect "converge: persistent npm lookup failure fails the job" fail "$C" "$PKG" ghcr.io/x/y
check "converge: unreadable npm registry writes no npm pointer" [ "$(grep -c 'dist-tag' "$STATE/calls")" -eq 0 ]
check "converge: unreadable npm registry does not fall back to an older version" [ "$(grep -c '^npm view @scope/pkg@1.2.3' "$STATE/calls")" -eq 0 ]
check "converge: images still converge when npm is unreadable" [ "$(last_alias latest)" = "ghcr.io/x/y:1.3.0" ]

new_case
tags v1.2.3 v1.3.0
printf '%s\n' 1.2.3 1.3.0 >"$STATE/npm_versions"
img ghcr.io/x/y:1.2.3 ghcr.io/x/y:1.2.3-slim ghcr.io/x/y:1.3.0 ghcr.io/x/y:1.3.0-slim
echo 2 >"$STATE/image_lookup_errors"
expect "converge: transient image lookup failure is retried" ok "$C" "$PKG" ghcr.io/x/y
check "converge: :latest -> highest after recovery" [ "$(last_alias latest)" = "ghcr.io/x/y:1.3.0" ]

new_case
tags v1.2.3 v1.3.0
printf '%s\n' 1.2.3 1.3.0 >"$STATE/npm_versions"
img ghcr.io/x/y:1.2.3 ghcr.io/x/y:1.2.3-slim ghcr.io/x/y:1.3.0 ghcr.io/x/y:1.3.0-slim
echo 3 >"$STATE/image_lookup_errors"
expect "converge: persistent image lookup failure fails the job" fail "$C" "$PKG" ghcr.io/x/y
check "converge: unreadable image registry writes no image pointer" [ "$(grep -c ' create ' "$STATE/calls")" -eq 0 ]
check "converge: unreadable image registry does not fall back to an older version" [ "$(grep -c 'inspect ghcr.io/x/y:1.2.3' "$STATE/calls")" -eq 0 ]
check "converge: npm still converges when images are unreadable" grep -q "dist-tag add @scope/pkg@1.3.0 latest" "$STATE/calls"

# --- result ------------------------------------------------------------------

echo "scripts/release: ${PASS} passed, ${FAIL} failed"
((FAIL == 0))
