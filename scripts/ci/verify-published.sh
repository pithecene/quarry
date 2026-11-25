#!/usr/bin/env bash
# Verify the published binary of a release on this machine: its checksum, its
# version and commit, and a run that it stores and reads back.
# Usage: scripts/ci/verify-published.sh <tag>

set -euo pipefail

# The checksum names and the exit report that this reads date from v0.20.0.
number="(0|[1-9][0-9]*)"
if [[ ! "$1" =~ ^v$number\.$number\.$number$ || ("${BASH_REMATCH[1]}" == 0 && "${BASH_REMATCH[2]}" -lt 20) ]]; then
  echo "expected a release from v0.20.0 on, got $1" >&2
  exit 1
fi
# The job that calls this bounds it.
tag="$1"
repo=$(gh repo view --json nameWithOwner --jq .nameWithOwner)
os=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$(uname -m)" in
  x86_64) arch=amd64 ;;
  arm64 | aarch64) arch=arm64 ;;
  *) echo "unsupported machine: $(uname -m)" >&2 && exit 1 ;;
esac
archive="quarry_${tag}_${os}_${arch}.tar.gz"
sha256() { if command -v sha256sum > /dev/null; then sha256sum "$1"; else shasum -a 256 "$1"; fi; }

dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
gh release download "$tag" --repo "$repo" --dir "$dir" --pattern "$archive" --pattern "$archive.sha256"
cd "$dir"
# The one entry names the archive alone, as a download holds it.
sha256 "$archive" > expected.sha256
cmp "$archive.sha256" expected.sha256
tar -xzf "$archive"
# Rosetta also runs an amd64 binary, so a run does not show the architecture.
case "$os/$arch" in
  linux/amd64) kind='ELF 64-bit.*x86-64' ;;
  linux/arm64) kind='ELF 64-bit.*aarch64' ;;
  darwin/amd64) kind='Mach-O 64-bit executable x86_64' ;;
  darwin/arm64) kind='Mach-O 64-bit executable arm64' ;;
esac
file -b quarry | grep -E "$kind"
commit=$(gh api "repos/$repo/commits/$tag" --jq .sha)
./quarry version | tee version.json
jq -e --arg version "${tag#v}" --arg commit "$commit" \
  '.version == $version and (.commit | test("^[0-9a-f]{7,40}$")) and (.commit as $short | $commit | startswith($short))' version.json

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
jq -e '.outcome == "success" and .recorded == true and .exit_code == 0' report.json
day=$(./quarry list runs --format json "${store[@]}" | jq -r '.runs[0].day')
./quarry list events --format json "${store[@]}" \
  --source ci --category verify --day "$day" --run-id verify > events.json
jq -e '[.events[] | [.seq, .type, .payload.item_type, .payload.data.n]] ==
  [[1, "item", "x", 0], [2, "item", "x", 1], [3, "item", "x", 2], [4, "run_complete", null, null]]' events.json
echo "✅ $archive on $os/$arch"
