#!/usr/bin/env bash
# Renders the README splash from splash.html (a patent drawing in SVG, set in IM Fell English)
# with headless Chromium at 2x: ink on paper for light, a cyanotype blueprint for dark.
#
#   docs/assets/source/render.sh [path-to-chrome-or-chrome-headless-shell]
#
# Needs: a Chromium build, ImageMagick (`magick`), curl. IM Fell English (SIL OFL)
# is fetched into a temp directory next to the page; it is not committed.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
out="$(cd "$here/.." && pwd)"
chrome="${1:-$(command -v chrome-headless-shell || command -v chromium || command -v google-chrome)}"
work="$(mktemp -d)"; trap 'rm -rf "$work"' EXIT
cp "$here/splash.html" "$work/"
mkdir -p "$work/fonts"
curl -sfL -o "$work/fonts/IMFellEnglish.ttf" "https://github.com/google/fonts/raw/main/ofl/imfellenglish/IMFeENrm28P.ttf"
curl -sfL -o "$work/fonts/IMFellEnglish-Italic.ttf" "https://github.com/google/fonts/raw/main/ofl/imfellenglish/IMFeENit28P.ttf"
curl -sfL -o "$work/fonts/IMFellEnglishSC.ttf" "https://github.com/google/fonts/raw/main/ofl/imfellenglishsc/IMFeENsc28P.ttf"
for mode in light dark; do
  "$chrome" --headless --allow-file-access-from-files --hide-scrollbars --force-device-scale-factor=2 \
    --virtual-time-budget=5000 --window-size=1280,560 --screenshot="$work/$mode.png" "file://$work/splash.html#$mode" >/dev/null 2>&1
  magick "$work/$mode.png" -strip -quality 90 -sampling-factor 4:4:4 "$out/quarry-splash-$mode.jpg"
done
echo "rendered into $out"
