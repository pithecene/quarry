#!/usr/bin/env bash
# Renders the README banner: Auguste Lepère's wood engraving "Fontainebleau
# Forest: Sandstone Quarry" (1890; Cleveland Museum of Art 2014.708.s, CC0),
# cropped to the plate from the museum's full-resolution scan and hand-tinted
# (pale ochre on the lit stone, grey-blue in the shadows) under the wordmark in
# banner.html. Rendered with headless Chromium at 4x and downsampled to 2560x960.
#
#   docs/assets/source/render.sh [path-to-chrome-or-chrome-headless-shell]
#
# Needs: a Chromium build, ImageMagick (`magick`), curl. The scan (~80 MB) and
# the fonts (Space Grotesk, Inter, EB Garamond; SIL OFL) are fetched into a temp
# directory; they are not committed.
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
curl -sfL -o "$work/full.tif" "https://openaccess-cdn.clevelandart.org/2014.708.s/2014.708.s_full.tif"

# the full scan is 7187x3780; this is the engraved plate inside its border
magick "$work/full.tif[0]" -crop 6494x2581+374+374 +repage -colorspace Gray -level 6%,94% "$work/gray.png"
# hand tint: a transparent wash keyed to the engraving's broad tones
magick \( -size 1x90 gradient:"#aeb8c6"-"#c9c3b4" \) \( -size 1x80 gradient:"#c9c3b4"-"#e6c891" \) \
  \( -size 1x86 gradient:"#e6c891"-"#f4ecda" \) -append "$work/wash.png"
magick "$work/gray.png" -blur 0x21 -level 5%,95% "$work/wash.png" -clut "$work/tint.png"
magick "$work/gray.png" +level-colors "#2a2219","#ffffff" "$work/lines-light.png"
magick "$work/tint.png" "$work/lines-light.png" -compose multiply -composite "$work/quarry-light.png"
magick "$work/gray.png" +level-colors "#0b1020","#ffffff" "$work/lines-dark.png"
magick "$work/tint.png" "$work/lines-dark.png" -compose multiply -composite -level 0%,100%,0.85 "$work/quarry-dark.png"

for mode in light dark; do
  "$chrome" --headless --allow-file-access-from-files --hide-scrollbars --force-device-scale-factor=4 \
    --default-background-color=00000000 --virtual-time-budget=15000 --window-size=1280,480 \
    --screenshot="$work/raw-$mode.png" "file://$work/banner.html#quarry-$mode" >/dev/null 2>&1
  magick "$work/raw-$mode.png" -filter Lanczos -resize 2560x960 -strip "$out/quarry-banner-$mode.png"
done
echo "rendered into $out"
