#!/usr/bin/env python3
"""
Prepare a bus image for use as a map marker.

    python scripts/prepare_bus_icon.py in.png icons/BHBC-1.png

Three things have to be true of a marker icon, and generated bus art is
rarely born with any of them:

  1. The background must be gone. A white rectangle or a dark glow looks
     fine on its own and terrible over a map.
  2. Faint halo must be gone too. Alpha of 1-30 around the vehicle is
     invisible against white and reads as a smudge over dark tiles.
  3. It must be the same size as the others. Markers are scaled by CSS
     from the canvas, so a bus drawn larger within its canvas renders
     larger on the map — which makes one route's buses look closer than
     another's for no reason.

So: drop near-transparent pixels, crop to what is left, scale to match a
reference icon's content box, and re-centre on the same canvas.

Orientation is not checked and cannot be: the source must already face
EAST (right) at 0 degrees, because createBusIcon applies `bearing - 90`.
A westward-facing source will point every bus backwards.
"""
import argparse
import sys
from pathlib import Path

from PIL import Image

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REFERENCE = ROOT / "icons" / "BHBC.png"
HALO_THRESHOLD = 40      # alpha at or below this is halo, not vehicle


def solid_box(im: Image.Image, threshold: int = 200):
    """Bounding box of genuinely opaque content, ignoring any halo."""
    mask = im.getchannel("A").point(lambda v: 255 if v > threshold else 0)
    box = mask.getbbox()
    if box is None:
        sys.exit("That image has no opaque content — is it fully transparent?")
    return box


def drop_background(im: Image.Image) -> Image.Image:
    """Remove an opaque backdrop, then any faint halo left around the edge."""
    im = im.convert("RGBA")
    if im.getchannel("A").getextrema()[0] == 255:
        # No alpha at all: the backdrop is baked in. Flood from the corners,
        # which is where a backdrop always is and a vehicle never is.
        from PIL import ImageDraw
        seed = im.getpixel((0, 0))[:3]
        flooded = im.copy()
        draw = ImageDraw.floodfill
        for corner in ((0, 0), (im.width - 1, 0),
                       (0, im.height - 1), (im.width - 1, im.height - 1)):
            draw(flooded, corner, (0, 0, 0, 0), thresh=42)
        im = flooded

    # Halo: keep the hard edge, discard the glow that surrounds it.
    a = im.getchannel("A").point(lambda v: 0 if v <= HALO_THRESHOLD else v)
    im.putalpha(a)
    return im


def main() -> None:
    ap = argparse.ArgumentParser(description="Prepare a bus image as a map marker.")
    ap.add_argument("source")
    ap.add_argument("dest")
    ap.add_argument("--reference", default=str(DEFAULT_REFERENCE),
                    help="icon whose content size and position to match")
    args = ap.parse_args()

    src = Image.open(args.source)
    ref = Image.open(args.reference).convert("RGBA")

    cleaned = drop_background(src)
    box = solid_box(cleaned)
    bus = cleaned.crop(box)

    rbox = solid_box(ref)
    target_w = rbox[2] - rbox[0]
    target_h = round(bus.height * target_w / bus.width)
    bus = bus.resize((target_w, target_h), Image.LANCZOS)

    canvas = Image.new("RGBA", ref.size, (0, 0, 0, 0))
    cx = (rbox[0] + rbox[2]) // 2
    cy = (rbox[1] + rbox[3]) // 2
    canvas.paste(bus, (cx - target_w // 2, cy - target_h // 2), bus)

    # Palette + transparency, like the icons already in this directory —
    # these are 1536px wide and there are a lot of them on screen at once.
    # FASTOCTREE is the only quantiser that keeps an alpha channel, which is
    # the whole point here — MEDIANCUT refuses RGBA outright.
    quant = canvas.quantize(colors=255, method=Image.FASTOCTREE)
    Path(args.dest).parent.mkdir(parents=True, exist_ok=True)
    quant.save(args.dest, optimize=True)

    size_kb = Path(args.dest).stat().st_size / 1024
    print(f"{args.dest}: {canvas.width}x{canvas.height} canvas, "
          f"bus {target_w}x{target_h} (reference {target_w}x{rbox[3]-rbox[1]}), "
          f"{size_kb:.0f} KB")


if __name__ == "__main__":
    main()
