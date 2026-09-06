#!/usr/bin/env python3
"""
Build the map-marker icons the site actually serves, from the masters.

    python scripts/build_icons.py              # rebuild every icon
    python scripts/build_icons.py BHBC-7.png   # just one

Masters live in icons/source/ at whatever size the art was produced —
currently 1536x1024. Those are far too big to serve: the map draws them in a
56px box, so a 1536px source is a 27x oversample in each dimension and about
750x the pixels ever displayed.

That costs almost nothing in bandwidth once compressed, and a great deal in
memory. A browser decodes a PNG to width x height x 4 bytes **regardless of how
few colours the palette holds**, so the twelve masters occupy roughly 70 MB of
decoded bitmap on a phone. Palette-reducing them changes that number not at all.
Resizing takes it to under a megabyte.

So: scale to WEB_PX on the long edge, bake in the drop shadow the CSS used to
apply per marker, and quantise. FASTOCTREE is the only PIL quantiser that keeps
an alpha channel; MEDIANCUT refuses RGBA outright.

The shadow is baked rather than applied in CSS because `filter: drop-shadow()`
forces every marker onto its own compositing layer, and there can be 200+ buses
on screen at once. Baked, it costs nothing at runtime and looks identical.
"""
import argparse
import sys
from pathlib import Path

from PIL import Image, ImageFilter

ROOT = Path(__file__).resolve().parent.parent
SOURCE_DIR = ROOT / "icons" / "source"
OUT_DIR = ROOT / "icons"

# Three times the 56px CSS box, so the asset is pixel-exact on a 3x phone and
# oversampled on nothing. object-fit: contain means a 3:2 bus renders 56x37, so
# scaling the long edge to 168 is 3x the drawn size rather than a guess.
WEB_PX = 168

# The shadow the stylesheet used to draw: drop-shadow(0 2px 4px rgba(0,0,0,.5)).
# A CSS blur radius is twice the Gaussian standard deviation, so 4px CSS is a
# 2px sigma — 6px at 3x, along with the 2px offset.
SHADOW_OFFSET_PX = 6
SHADOW_BLUR_PX = 6
SHADOW_ALPHA = 128


def bake_shadow(im: Image.Image) -> Image.Image:
    """Composite the marker's drop shadow into the image.

    The canvas is not expanded. Every master leaves a wide transparent margin
    around the vehicle — 28px or more below it at this scale — so the shadow has
    room to fall into. Padding instead would move the image centre, and
    `iconTransformForBearing` rotates about that centre, which would tilt every
    bus on the map.
    """
    alpha = im.getchannel("A")
    shadow_mask = Image.new("L", im.size, 0)
    shadow_mask.paste(alpha, (0, SHADOW_OFFSET_PX))
    shadow_mask = shadow_mask.filter(ImageFilter.GaussianBlur(SHADOW_BLUR_PX))
    shadow_mask = shadow_mask.point(lambda v: v * SHADOW_ALPHA // 255)

    shadow = Image.new("RGBA", im.size, (0, 0, 0, 0))
    shadow.putalpha(shadow_mask)
    return Image.alpha_composite(shadow, im)


def shadow_room(im: Image.Image) -> int:
    """Transparent pixels below the vehicle, to check the shadow won't clip."""
    box = im.getchannel("A").point(lambda v: 255 if v > 8 else 0).getbbox()
    return im.height - box[3] if box else 0


def build(master: Path, dest: Path) -> str:
    im = Image.open(master).convert("RGBA")
    im.thumbnail((WEB_PX, WEB_PX), Image.LANCZOS)

    room = shadow_room(im)
    needed = SHADOW_OFFSET_PX + SHADOW_BLUR_PX * 2
    if room < needed:
        print(f"  ! {dest.name}: only {room}px below the vehicle, shadow wants "
              f"{needed}px — it will be clipped", file=sys.stderr)

    out = bake_shadow(im)
    quant = out.quantize(colors=255, method=Image.FASTOCTREE)
    dest.parent.mkdir(parents=True, exist_ok=True)
    quant.save(dest, optimize=True)

    was = master.stat().st_size / 1024
    now = dest.stat().st_size / 1024
    decoded = out.width * out.height * 4 / 1024
    return (f"{dest.name:14s} {im.width:4d}x{im.height:<4d} "
            f"{was:7.0f} KB -> {now:6.1f} KB   decoded {decoded:6.0f} KB")


def main() -> None:
    ap = argparse.ArgumentParser(description="Build served icons from masters.")
    ap.add_argument("names", nargs="*", help="filenames to rebuild (default: all)")
    args = ap.parse_args()

    if not SOURCE_DIR.is_dir():
        sys.exit(f"{SOURCE_DIR} is missing — the masters live there.")

    masters = sorted(SOURCE_DIR.glob("*.png"))
    if args.names:
        wanted = set(args.names)
        masters = [m for m in masters if m.name in wanted]
        missing = wanted - {m.name for m in masters}
        if missing:
            sys.exit(f"No master for: {', '.join(sorted(missing))}")
    if not masters:
        sys.exit("No masters found.")

    total_before = total_after = total_decoded = 0
    for m in masters:
        dest = OUT_DIR / m.name
        print(" ", build(m, dest))
        total_before += m.stat().st_size
        total_after += dest.stat().st_size
        im = Image.open(dest)
        total_decoded += im.width * im.height * 4

    print(f"\n  {len(masters)} icons: {total_before/1024:.0f} KB -> "
          f"{total_after/1024:.0f} KB served, "
          f"{total_decoded/1048576:.2f} MB decoded")


if __name__ == "__main__":
    main()
