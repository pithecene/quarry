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

# npm view <pkg>@<v> version: succeeds iff the version is "published".
cat >"$STUBS/npm" <<'EOF'
#!/usr/bin/env bash
echo "npm $*" >>"$STATE/calls"
[[ "$1" == "view" ]] || exit 2
[[ -f "$STATE/npm_published" ]]
EOF

# pnpm publish: behaviour from $STATE/pnpm_mode.
cat >"$STUBS/pnpm" <<'EOF'
#!/usr/bin/env bash
echo "pnpm $*" >>"$STATE/calls"
case "$(cat "$STATE/pnpm_mode" 2>/dev/null || echo ok)" in
  ok) touch "$STATE/npm_published" ;;
  fail) exit 1 ;;
  race) touch "$STATE/npm_published"; exit 1 ;;
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

tags() { printf '%s\n' "$@" >"$WORK/tags"; }

# --- assert-highest-tag.sh ---------------------------------------------------

A="$HERE/assert-highest-tag.sh"
tags v0.13.4 v0.13.5 v0.13.6
expect "higher tag passes" ok "$A" v0.13.7 --tags-file "$WORK/tags"
expect "re-run of the highest tag passes" ok "$A" v0.13.6 --tags-file "$WORK/tags"
expect "older tag is rejected" fail "$A" v0.13.5 --tags-file "$WORK/tags"
tags v0.9.0 v0.10.0
expect "versions compare numerically, not lexically" fail "$A" v0.9.1 --tags-file "$WORK/tags"
expect "minor bump above a two-digit minor passes" ok "$A" v0.11.0 --tags-file "$WORK/tags"
tags v1.0.0-rc.1 latest v0.13.6
expect "non-release tags are ignored" ok "$A" v0.13.6 --tags-file "$WORK/tags"
tags
expect "first release passes" ok "$A" v0.1.0 --tags-file "$WORK/tags"
expect "malformed tag is rejected" fail "$A" 0.13.6 --tags-file "$WORK/tags"
expect "missing tag is rejected" fail "$A"

# --- publish-npm.sh ----------------------------------------------------------

N="$HERE/publish-npm.sh"

new_case
touch "$STATE/npm_published"
expect "npm: already published succeeds" ok "$N" "$PKG"
check "npm: already published does not republish" [ "$(calls pnpm)" -eq 0 ]

new_case
expect "npm: unpublished version publishes" ok "$N" "$PKG"
check "npm: publishes exactly once" [ "$(calls pnpm)" -eq 1 ]

new_case
echo race >"$STATE/pnpm_mode"
expect "npm: concurrent publish counts as success" ok "$N" "$PKG"

new_case
echo fail >"$STATE/pnpm_mode"
expect "npm: real publish failure fails" fail "$N" "$PKG"

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

# --- result ------------------------------------------------------------------

echo "scripts/release: ${PASS} passed, ${FAIL} failed"
((FAIL == 0))
