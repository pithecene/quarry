#!/usr/bin/env bash
# Verify the published binary of a release on this machine: its checksum, its
# version and commit, and a run that it stores and reads back.
# Usage: scripts/ci/verify-published.sh <tag>

set -euo pipefail

tag="$1"
os=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$(uname -m)" in
  x86_64) arch=amd64 ;;
  arm64 | aarch64) arch=arm64 ;;
  *) echo "unsupported machine: $(uname -m)" >&2 && exit 1 ;;
esac
archive="quarry_${tag}_${os}_${arch}.tar.gz"
# The stored checksums name the archive alone, as a download holds it.
check() { if command -v sha256sum > /dev/null; then sha256sum --check "$1"; else shasum -a 256 -c "$1"; fi; }

dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
gh release download "$tag" --dir "$dir" --pattern "$archive" --pattern "$archive.sha256"
cd "$dir"
check "$archive.sha256"
tar -xzf "$archive"
commit=$(gh api "repos/$GITHUB_REPOSITORY/commits/$tag" --jq .sha)
./quarry version | tee version.json
jq -e --arg version "${tag#v}" --arg commit "$commit" \
  '.version == $version and (.commit as $short | $commit | startswith($short))' version.json

cat > run.mjs << 'SCRIPT'
export default async function run(ctx) {
  for (let n = 0; n < 3; n++) await ctx.emit.item({ item_type: 'x', data: { n } })
  await ctx.emit.runComplete({})
}
SCRIPT
store=(--storage-backend fs --storage-path store)
mkdir store
./quarry run --script ./run.mjs --run-id verify --source ci --category verify \
  --no-browser --report report.json "${store[@]}"
jq -e '.outcome == "success" and .recorded and .exit_code == 0' report.json
day=$(./quarry list runs --format json "${store[@]}" | jq -r '.runs[0].day')
./quarry list events --format json "${store[@]}" \
  --source ci --category verify --day "$day" --run-id verify > events.json
jq -e '[.events[].type] == ["item", "item", "item", "run_complete"]' events.json
echo "✅ $archive on $os/$arch"
