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
from functools import lru_cache
from pathlib import Path

from PIL import Image, ImageChops, ImageDraw, ImageFilter, ImageStat

ROOT = Path(__file__).resolve().parent.parent
SOURCE_DIR = ROOT / "icons" / "source"
OUT_DIR = ROOT / "icons"
# Per-bus liveries (asked for 2 November 2026): flat patterns drawn by
# scripts/render_liveries.mjs from scripts/livery_designs.py, each repainted
# over a master below and served from icons/liveries/.
LIVERY_SOURCE_DIR = SOURCE_DIR / "liveries"
LIVERY_OUT_DIR = OUT_DIR / "liveries"

# Three times the 56px CSS box, so the asset is pixel-exact on a 3x phone and
# oversampled on nothing. object-fit: contain means a 3:2 bus renders 56x37, so
# a 168px-wide canvas is 3x the drawn size rather than a guess.
WEB_PX = 168
CANVAS = (WEB_PX, 112)

# Every vehicle to one scale (asked for 1 November 2026). Scaling each master's
# long edge to the canvas drew a vehicle as big as its master happened to frame
# it: the Metrobus double-decker came out 18% longer than Brighton & Hove's and
# the 700's. So each is cropped to the vehicle and drawn at its kind's length.
# The double-deckers are the reference, at the length Brighton & Hove's and the
# 700's already were; a single-decker is a little longer, as on the road, and
# every coach one length, longer again. Lengths of the solid body, without the
# soft edge round it.
LENGTH_PX = {"double": 120, "single": 128, "coach": 137}
VEHICLE_KIND = {
    "SCSO-17.png": "single",
    "CMPA.png": "coach",
    "FLIX.png": "coach",
    "NTXP.png": "coach",
    "TNXB.png": "coach",
}
# The reference buses' middle sits a little below the canvas's, leaving the
# shadow room underneath; across, every vehicle is centred, because the marker
# turns the image about its middle (iconTransformForBearing).
CENTRE_Y = 59


def vehicle_kind(name: str) -> str:
    return VEHICLE_KIND.get(name, "double")


def fit_vehicle(im: Image.Image, kind: str) -> Image.Image:
    """The vehicle alone, its body at its kind's length, centred on the canvas."""
    alpha = im.getchannel("A")
    box = alpha.point(lambda v: 255 if v > 8 else 0).getbbox()      # the vehicle, soft edge and all
    body = alpha.point(lambda v: 255 if v > 160 else 0).getbbox()   # its solid body
    scale = LENGTH_PX[kind] / (body[2] - body[0])
    vehicle = im.crop(box)
    vehicle = vehicle.resize((round(vehicle.width * scale), round(vehicle.height * scale)),
                             Image.LANCZOS)
    # Where the body's middle falls in the cropped, scaled vehicle.
    mid_x = ((body[0] + body[2]) / 2 - box[0]) * scale
    mid_y = ((body[1] + body[3]) / 2 - box[1]) * scale
    out = Image.new("RGBA", CANVAS, (0, 0, 0, 0))
    out.paste(vehicle, (round(CANVAS[0] / 2 - mid_x), round(CENTRE_Y - mid_y)))
    return out

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
    im = fit_vehicle(Image.open(master).convert("RGBA"), vehicle_kind(master.name))

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


# ── Liveries ─────────────────────────────────────────────────────────────────
#
# A livery is drawn by repainting a master: the paint of one master bus is
# found by its colours, and replaced by the livery's pattern with the master's
# own fine shading (panel lines, the edge of the roof) laid back over it.
# Windows, wheels, mirrors and lights are left as drawn. So every livery is the
# same bus as its master at the same scale, and only the paint differs.
#
# Colour families of each master's paint, as Pillow's HSV bands (0-255 each);
# flood-filled from the seeds, so a red seat seen through a window, which
# never touches the paint, stays a seat. The single-decker's swirl logo is
# painted over whole.
_RED = lambda h, s, v: s > 115 and (h <= 14 or h >= 234) and v > 64
_CREAM = lambda h, s, v: s < 97 and v > 184 and 18 <= h <= 53
_ORANGE = lambda h, s, v: s > 115 and 14 < h < 34 and v > 153
_NAVY = lambda h, s, v: s > 89 and 142 <= h <= 181 and v > 31
LIVERY_TEMPLATES = {
    "dd": {"master": "BHBC.png", "families": (_RED, _CREAM, _ORANGE),
           "seeds": ((600, 470),), "logo": None},
    "sd": {"master": "SCSO-17.png", "families": (_NAVY,),
           "seeds": ((700, 580),), "logo": (262, 418, 398, 580)},
}
LIVERY_KIND = {"dd": "double", "sd": "single"}
LIVERY_SHADING = 0.9


def _mask(hsv: Image.Image, test) -> Image.Image:
    """255 where `test(h, s, v)` holds, from a lookup over all three bands."""
    h, s, v = hsv.split()
    hs = Image.merge("RGB", (h, s, v))
    out = Image.new("L", hsv.size, 0)
    px, src = out.load(), hs.load()
    w, ht = hsv.size
    for y in range(ht):
        for x in range(w):
            if test(*src[x, y]):
                px[x, y] = 255
    return out


@lru_cache(maxsize=None)
def livery_template(kind: str):
    """The master for `kind`, the mask of its paint, the fine shading of that
    paint as a float image centred on 0, and the paint's bounding box."""
    t = LIVERY_TEMPLATES[kind]
    master = Image.open(SOURCE_DIR / t["master"]).convert("RGBA")
    opaque = master.getchannel("A").point(lambda a: 255 if a > 127 else 0)
    hsv = master.convert("RGB").convert("HSV")
    families = [ImageChops.multiply(_mask(hsv, f), opaque) for f in t["families"]]
    paint_like = families[0]
    for m in families[1:]:
        paint_like = ImageChops.lighter(paint_like, m)
    if t["logo"]:
        logo = Image.new("L", master.size, 0)
        logo.paste(opaque.crop(t["logo"]), t["logo"][:2])
        paint_like = ImageChops.lighter(paint_like, logo)
    flood = paint_like.copy()
    for seed in t["seeds"]:
        ImageDraw.floodfill(flood, seed, 128)
    paint = flood.point(lambda v: 255 if v == 128 else 0)

    # Each family's luminance less its own median (at half scale, about 128),
    # so the old livery's edges leave no step; then only the fine detail is
    # kept, and none where families meet. `detail` is centred on 128.
    lum = master.convert("L")
    half = Image.new("L", master.size, 128)
    for fam in families:
        fam = ImageChops.multiply(fam, paint)
        if fam.getbbox():
            med = ImageStat.Stat(lum, mask=fam).median[0]
            half.paste(lum.point(lambda v, m=med: max(0, min(255, round(128 + (v - m) / 2)))), mask=fam)
    if t["logo"]:
        half.paste(128, t["logo"])
    detail = ImageChops.subtract(half, half.filter(ImageFilter.GaussianBlur(5)), 1, 128)
    seams = Image.new("L", master.size, 0)
    for fam in families:
        grow, shrink = fam.filter(ImageFilter.MaxFilter(7)), fam.filter(ImageFilter.MinFilter(7))
        seams = ImageChops.lighter(seams, ImageChops.subtract(grow, shrink))
    outline = ImageChops.invert(paint).filter(ImageFilter.MaxFilter(5))
    detail.paste(128, mask=ImageChops.subtract(seams, outline))
    detail.paste(128, mask=ImageChops.invert(paint))
    return master, paint, detail, paint.getbbox()


def repaint(kind: str, pattern: Image.Image) -> Image.Image:
    """The master for `kind` in the livery `pattern`, at the master's size."""
    master, paint, detail, box = livery_template(kind)
    size = (box[2] - box[0], box[3] - box[1])
    region = master.crop(box)
    flat = Image.alpha_composite(region, pattern.convert("RGBA").resize(size, Image.LANCZOS))
    # The half-scale detail, back to full scale and by LIVERY_SHADING.
    gain = 2 * LIVERY_SHADING
    shade = detail.crop(box).point(lambda v: max(0, min(255, round(128 + gain * (v - 128)))))
    bands = [ImageChops.add(c, shade, 1, -128) for c in flat.convert("RGB").split()]
    painted = Image.merge("RGBA", (*bands, region.getchannel("A")))
    out = master.copy()
    out.paste(painted, box[:2], mask=paint.crop(box))
    return out


def build_livery(pattern_path: Path, dest: Path) -> str:
    """A served livery icon from its flat pattern: repainted, then fitted,
    shadowed and quantised as every other icon is. The key's -dd or -sd says
    which master and which length (a single-decker is drawn at its own
    LENGTH_PX, not the master's: the single-deck master is as long as the
    double-deckers)."""
    kind = pattern_path.stem[-2:]
    im = fit_vehicle(repaint(kind, Image.open(pattern_path)), LIVERY_KIND[kind])
    out = bake_shadow(im)
    quant = out.quantize(colors=255, method=Image.FASTOCTREE)
    dest.parent.mkdir(parents=True, exist_ok=True)
    quant.save(dest, optimize=True)
    return f"liveries/{dest.name:30s} {dest.stat().st_size / 1024:6.1f} KB"


def main() -> None:
    ap = argparse.ArgumentParser(description="Build served icons from masters.")
    ap.add_argument("names", nargs="*",
                    help="filenames to rebuild, liveries as liveries/KEY.png (default: all)")
    ap.add_argument("--liveries", action="store_true", help="rebuild only the liveries")
    args = ap.parse_args()

    if not SOURCE_DIR.is_dir():
        sys.exit(f"{SOURCE_DIR} is missing — the masters live there.")

    masters = [] if args.liveries else sorted(SOURCE_DIR.glob("*.png"))
    patterns = sorted(LIVERY_SOURCE_DIR.glob("*.png"))
    if args.names:
        wanted = set(args.names)
        masters = [m for m in masters if m.name in wanted]
        patterns = [p for p in patterns if f"liveries/{p.name}" in wanted]
        missing = wanted - {m.name for m in masters} - {f"liveries/{p.name}" for p in patterns}
        if missing:
            sys.exit(f"No master for: {', '.join(sorted(missing))}")
    if not masters and not patterns:
        sys.exit("No masters found.")

    for p in patterns:
        print(" ", build_livery(p, LIVERY_OUT_DIR / p.name))
    if not masters:
        print(f"\n  {len(patterns)} liveries built into {LIVERY_OUT_DIR.relative_to(ROOT)}/")
        return

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
