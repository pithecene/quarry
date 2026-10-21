#!/usr/bin/env bash
# Renders the README banner: Auguste Lepère's wood engraving "Fontainebleau
# Forest: Sandstone Quarry" (1890; Cleveland Museum of Art 2014.708.s, CC0),
# cropped to the plate and recoloured (ink on parchment for light, navy ink with
# bright cream highlights for dark), under the wordmark in banner.html. Headless Chromium at 2x, PNG.
#
#   docs/assets/source/render.sh [path-to-chrome-or-chrome-headless-shell]
#
# Needs: a Chromium build, ImageMagick (`magick`), pngquant, curl. The engraving
# and the fonts (Space Grotesk, Inter, EB Garamond; SIL OFL) are fetched into a
# temp directory; they are not committed.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
out="$(cd "$here/.." && pwd)"
chrome="${1:-$(command -v chrome-headless-shell || command -v chromium || command -v google-chrome)}"
work="$(mktemp -d)"; trap 'rm -rf "$work"' EXIT
cp "$here/banner.html" "$work/"
mkdir -p "$work/fonts"
curl -sfL -o "$work/fonts/SpaceGrotesk.ttf" "https://github.com/google/fonts/raw/main/ofl/spacegrotesk/SpaceGrotesk%5Bwght%5D.ttf"
curl -sfL -o "$work/fonts/Inter.ttf" "https://github.com/google/fonts/raw/main/ofl/inter/Inter%5Bopsz,wght%5D.ttf"
curl -sfL -o "$work/fonts/EBGaramond.ttf" "https://github.com/google/fonts/raw/main/ofl/ebgaramond/EBGaramond%5Bwght%5D.ttf"
curl -sfL -o "$work/print.jpg" "https://openaccess-cdn.clevelandart.org/2014.708.s/2014.708.s_print.jpg"
# the print scan is 3400x1788; this is the engraved plate inside its border
magick "$work/print.jpg" -crop 3072x1221+177+177 +repage -colorspace Gray -level 6%,94% "$work/gray.png"
magick "$work/gray.png" +level-colors "#2a2219","#f4ecda" "$work/plate-light.png"
# dark keeps the engraving's own shading, mapped from navy ink to a bright cream
# with the midtones pulled down, so lit stone glows and the cuts stay deep
magick "$work/gray.png" -level 4%,96% -gamma 0.8 +level-colors "#0b1020","#f2e3c2" "$work/plate-dark.png"
for mode in light dark; do
  "$chrome" --headless --allow-file-access-from-files --hide-scrollbars --force-device-scale-factor=2 \
    --default-background-color=00000000 --virtual-time-budget=5000 --window-size=1280,480 \
    --screenshot="$work/$mode.png" "file://$work/banner.html#$mode" >/dev/null 2>&1
  pngquant --quality 80-95 --speed 1 --force --output "$out/quarry-banner-$mode.png" "$work/$mode.png"
done
echo "rendered into $out"
