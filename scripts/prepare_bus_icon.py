#!/usr/bin/env python3
"""
Prepare a bus image for use as a map marker.

    python scripts/prepare_bus_icon.py in.png icons/source/BHBC-1.png

Writes the master to icons/source/ and then builds the served icon into
icons/ via scripts/build_icons.py, so a new livery gets both without a second
command. Pass --no-build to write only the master.

Three things have to be true of a marker icon, and generated bus art is
rarely born with any of them:

  1. The background must be gone. A white rectangle, a dark glow, or a
     painted transparency checkerboard all look fine on their own and
     terrible over a map. The checkerboard is the common one: an export
     that *looks* transparent but ships two alternating greys.
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

sys.path.insert(0, str(Path(__file__).resolve().parent))

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REFERENCE = ROOT / "icons" / "source" / "BHBC.png"
HALO_THRESHOLD = 40      # alpha at or below this is halo, not vehicle


def solid_box(im: Image.Image, threshold: int = 200):
    """Bounding box of genuinely opaque content, ignoring any halo."""
    mask = im.getchannel("A").point(lambda v: 255 if v > threshold else 0)
    box = mask.getbbox()
    if box is None:
        sys.exit("That image has no opaque content — is it fully transparent?")
    return box


def _is_backdrop(px, sat_max: int, val_min: int) -> bool:
    """Bright and colourless — white, light grey, or a checkerboard square.

    Deliberately not "matches the corner pixel": exports frequently paint a
    transparency checkerboard into the image, so the backdrop is two
    alternating greys rather than one colour, and a seed-matching fill stops
    at the first square boundary.
    """
    r, g, b = px[:3]
    return max(r, g, b) - min(r, g, b) <= sat_max and min(r, g, b) >= val_min


def drop_background(im: Image.Image, sat_max: int = 26, val_min: int = 150) -> Image.Image:
    """Remove an opaque backdrop, then any faint halo left around the edge."""
    im = im.convert("RGBA")
    if im.getchannel("A").getextrema()[0] == 255:
        # No alpha at all: the backdrop is baked in. Clear every backdrop-like
        # pixel reachable from the border, so enclosed light areas — a white
        # flag on the bodywork, a pale window — are left alone.
        px = im.load()
        w, h = im.size
        seen = bytearray(w * h)
        stack = [(x, 0) for x in range(w)] + [(x, h - 1) for x in range(w)] \
              + [(0, y) for y in range(h)] + [(w - 1, y) for y in range(h)]
        while stack:
            x, y = stack.pop()
            if not (0 <= x < w and 0 <= y < h):
                continue
            i = y * w + x
            if seen[i]:
                continue
            seen[i] = 1
            if not _is_backdrop(px[x, y], sat_max, val_min):
                continue
            px[x, y] = (0, 0, 0, 0)
            stack.extend(((x + 1, y), (x - 1, y), (x, y + 1), (x, y - 1)))

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
    ap.add_argument("--no-build", action="store_true",
                    help="write the master only; skip the served-icon build")
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

    # The master keeps full resolution and no palette: it is the thing future
    # sizes get re-exported from, so throwing information away here would be
    # permanent. Quantising and resizing happen in build_icons.py, on the copy
    # the site actually serves.
    dest = Path(args.dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    canvas.save(dest, optimize=True)

    size_kb = dest.stat().st_size / 1024
    print(f"{dest}: {canvas.width}x{canvas.height} canvas, "
          f"bus {target_w}x{target_h} (reference {target_w}x{rbox[3]-rbox[1]}), "
          f"{size_kb:.0f} KB master")

    if args.no_build:
        print("  --no-build: served icon not written. "
              "Run scripts/build_icons.py before the site can use it.")
        return
    # A master with no served counterpart is an icon that 404s on the map, so
    # building is the default rather than a step to remember.
    from build_icons import build, OUT_DIR
    print(" ", build(dest, OUT_DIR / dest.name))


if __name__ == "__main__":
    main()
