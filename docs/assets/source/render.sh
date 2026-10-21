#!/usr/bin/env bash
# Renders the README banner from splash.html (SVG, set in Space Grotesk and Inter)
# with headless Chromium at 2x, light and dark, as PNG with transparent corners.
#
#   docs/assets/source/render.sh [path-to-chrome-or-chrome-headless-shell]
#
# Needs: a Chromium build, ImageMagick (`magick`), curl. Space Grotesk and Inter
# (SIL OFL) are fetched into a temp directory next to the page; they are not committed.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
out="$(cd "$here/.." && pwd)"
chrome="${1:-$(command -v chrome-headless-shell || command -v chromium || command -v google-chrome)}"
work="$(mktemp -d)"; trap 'rm -rf "$work"' EXIT
cp "$here/splash.html" "$work/"
mkdir -p "$work/fonts"
curl -sfL -o "$work/fonts/SpaceGrotesk.ttf" "https://github.com/google/fonts/raw/main/ofl/spacegrotesk/SpaceGrotesk%5Bwght%5D.ttf"
curl -sfL -o "$work/fonts/Inter.ttf" "https://github.com/google/fonts/raw/main/ofl/inter/Inter%5Bopsz,wght%5D.ttf"
for mode in light dark; do
  "$chrome" --headless --allow-file-access-from-files --hide-scrollbars --force-device-scale-factor=2 \
    --default-background-color=00000000 --virtual-time-budget=5000 --window-size=1280,360 \
    --screenshot="$work/$mode.png" "file://$work/splash.html#$mode" >/dev/null 2>&1
  magick "$work/$mode.png" -strip -define png:compression-level=9 "$out/quarry-banner-$mode.png"
done
echo "rendered into $out"
