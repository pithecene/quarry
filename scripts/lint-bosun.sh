#!/usr/bin/env bash
# Runs Bosun's Go analyzers over the quarry module at the Bosun commit that
# .bosun-version pins. Bosun is private, so this runs where its repository is
# readable; it fails, never skips, when it is not.
#
# Usage: scripts/lint-bosun.sh [golangci-lint run flags]
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
pin="$(tr -d '[:space:]' < "$root/.bosun-version")"
if ! [[ "$pin" =~ ^[0-9a-f]{40}$ ]]; then
  echo "lint-bosun: .bosun-version must hold a full commit SHA, not '$pin'" >&2
  exit 2
fi
url="${BOSUN_REPO:-git@github.com:pithecene/bosun.git}"
src="$root/.bosun/src"
bin="$root/.bosun/bin/go-analysis"
stamp="$root/.bosun/installed"

if [ ! -d "$src/.git" ]; then
  git clone --quiet --no-checkout "$url" "$src" ||
    { echo "lint-bosun: cannot clone $url; Bosun's analyzers need read access to it" >&2; exit 1; }
fi
if ! git -C "$src" cat-file -e "$pin^{commit}" 2>/dev/null; then
  git -C "$src" fetch --quiet origin ||
    { echo "lint-bosun: cannot fetch $url" >&2; exit 1; }
fi
# Install only when the pin changed, so a repeat run reuses the runner.
if [ "$(cat "$stamp" 2>/dev/null)" != "$pin" ] || [ ! -x "$bin" ]; then
  git -C "$src" checkout --quiet --detach "$pin"
  "$src/tools/install/install.sh" "$src" "$root/.bosun" go-analysis
  echo "$pin" > "$stamp"
fi

cd "$root/quarry"
exec "$bin" run -c .golangci.bosun.yaml "$@" ./...
