#!/usr/bin/env python3
"""
Build the header brand mark and the favicons from one master image.

    python scripts/prepare_brand_mark.py

The site's mark is a raster illustration with no vector source, so it ships as
PNG rather than being traced. Tracing it would mean redrawing it, and a redrawn
mark is a different mark.

Everything here is derived from `brand/mark-source.png`, which is committed for
the same reason `icons/source/` and `data/comparison_areas.json` are: the build
stays reproducible offline, and the artwork cannot silently move underneath the
site. Re-run this after replacing the master; do not hand-edit the outputs.

Three things this has to get right:

  1. **Crop to the artwork, not the canvas.** The master carries transparent
     margin — 70px on the left, 92px on top — so scaling the full canvas to
     40px would render the disc at 35px and off-centre. The crop is taken from
     the alpha bounding box.
  2. **Square it about the disc.** The opaque region is 1108x1114, not quite
     square. Padding it to a square around its own centre keeps the disc
     centred at every output size; scaling a non-square box into a square one
     would flatten it.
  3. **Resample with LANCZOS.** At 40px this is a 27:1 downsample. NEAREST and
     BILINEAR both drop the thin white shapes (the pier railings, the i360
     mast) rather than averaging them into something visible.

Alpha is preserved throughout — the master's background is genuinely
transparent, which is why the mark can sit on the light and the dark header
without a plate behind it.
"""
import sys
from pathlib import Path

from PIL import Image

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "brand" / "mark-source.png"

# The header renders the mark at 40px (32px under 700px wide). 1x/2x/3x covers
# every display in use; 3x is the ceiling because beyond it the source itself
# stops having detail to give.
# 512 is the share-preview size: it is what Open Graph and Twitter fetch when
# the site is pasted into a message or a local Facebook group, and it is the
# only place the mark is ever seen large. Generated here rather than exported
# by hand so it cannot drift from the header mark it is supposed to be.
MARK_SIZES = [40, 80, 120, 512]

# 32px is the browser-tab icon. 180px is what iOS uses for a home-screen
# bookmark, and is the one size Apple actually asks for by name.
FAVICON = 32
APPLE_TOUCH = 180

# favicon.ico is the last-resort icon for browsers that take neither PNG nor
# SVG links. It is rebuilt here rather than left alone, because an .ico showing
# the previous mark is exactly the drift this script exists to prevent.
ICO_SIZES = [16, 32, 48]


def squared_artwork(im: Image.Image) -> Image.Image:
    """Crop to the opaque artwork and pad it to a square about its own centre."""
    box = im.getchannel("A").getbbox()
    if box is None:
        sys.exit(f"{SOURCE} is fully transparent — nothing to build from.")
    art = im.crop(box)
    side = max(art.size)
    canvas = Image.new("RGBA", (side, side), (0, 0, 0, 0))
    canvas.paste(art, ((side - art.width) // 2, (side - art.height) // 2))
    return canvas


def write(art: Image.Image, size: int, path: Path) -> None:
    art.resize((size, size), Image.LANCZOS).save(path, optimize=True)
    print(f"  {path.relative_to(ROOT)}  {size}x{size}  {path.stat().st_size:,} bytes")


def main() -> None:
    if not SOURCE.exists():
        sys.exit(f"Missing {SOURCE.relative_to(ROOT)} — the master is committed; restore it.")

    master = Image.open(SOURCE).convert("RGBA")
    art = squared_artwork(master)
    print(f"{SOURCE.relative_to(ROOT)}: {master.size[0]}x{master.size[1]} "
          f"canvas, {art.size[0]}x{art.size[1]} of artwork")

    for size in MARK_SIZES:
        write(art, size, ROOT / "brand" / f"mark-{size}.png")
    write(art, FAVICON, ROOT / "favicon-32.png")
    write(art, APPLE_TOUCH, ROOT / "apple-touch-icon.png")

    ico = ROOT / "favicon.ico"
    art.resize((max(ICO_SIZES), max(ICO_SIZES)), Image.LANCZOS).save(
        ico, sizes=[(n, n) for n in ICO_SIZES])
    print(f"  {ico.relative_to(ROOT)}  {'/'.join(str(n) for n in ICO_SIZES)}  "
          f"{ico.stat().st_size:,} bytes")


if __name__ == "__main__":
    main()
