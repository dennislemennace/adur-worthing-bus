#!/usr/bin/env python3
"""
Per-bus liveries for the map icons (asked for 2 November 2026).

Each livery is the flat pattern of one side of the bus, drawn over the paint
of an existing master: the double-decker of icons/source/BHBC.png or the
single-decker of icons/source/SCSO-17.png. scripts/build_icons.py repaints the
master in it, keeping the master's windows, wheels, lights and panel lines, so
every livery is the same vehicle at the same scale, facing east, as the
operator icons. A key ends in -dd or -sd, which says which master and, through
build_icons.LENGTH_PX, how long it is drawn.

The patterns are drawn here, from photographs: Brighton & Hove's fleet
history (history.buses.co.uk, used with permission) for Brighton & Hove and
Metrobus, and freely licensed photographs on Wikimedia Commons for Stagecoach
and Compass. A few one-off buses use bustimes.org's own drawing of the livery
(its "right" CSS, used with permission) where no photograph was to hand.

No lettering, as on every icon, with one exception: the 1X and 3X carry their
route number in a coloured pill on the side, as the buses do (asked for
2 November 2026).

The patterns are drawn to a paint box, in that master's pixels:

  double-decker 1081 x 387: upper windows y 20-110, lower windows y 186-277
    (x 106-665, cab 864-1026), skirt below 277, wheels at x 190 and 741,
    front from x 995, windscreen from 1030.
  single-decker 1095 x 297: windows y 61-180 (x 178-1040), wheels at x 252
    and 808, front from x 1040.

The flat patterns are drawn to PNGs by a browser (scripts/render_liveries.mjs)
into icons/source/liveries/, which build_icons.py reads. Rendering needs
headless Chrome; building the icons does not.

    python scripts/livery_designs.py --jobs > /tmp/jobs.json
    node scripts/render_liveries.mjs /tmp/jobs.json icons/source/liveries
    python scripts/build_icons.py
"""
import json
import random
import sys

BOX = {"dd": (1081, 387), "sd": (1095, 297)}


def svg(kind, body, bg):
    w, h = BOX[kind]
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}">'
            f'<rect width="{w}" height="{h}" fill="{bg}"/>{body}</svg>')


def pill(x, y, w, h, fill, text=None, tc="#fff"):
    s = f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{h / 2}" fill="{fill}"/>'
    if text:
        s += (f'<text x="{x + w / 2}" y="{y + h * 0.77}" font-family="Arial Black,Arial,Helvetica,sans-serif" '
              f'font-weight="900" font-size="{h * 0.8}" fill="{tc}" text-anchor="middle">{text}</text>')
    return s


def lean(x_bottom, width, slant, y0, y1, fill):
    """A band leaning forward: its top edge `slant` further on than its foot."""
    return (f'<polygon points="{x_bottom},{y1} {x_bottom + width},{y1} '
            f'{x_bottom + width + slant},{y0} {x_bottom + slant},{y0}" fill="{fill}"/>')


def figures(x0, y0, y1, step=17):
    """Brighton & Hove's rows of coloured figures, as tall rounded bars."""
    cols = ["#e6007e", "#f39200", "#ffd400", "#7ab929", "#009fe3", "#7d4e9e", "#e30613"]
    s = ""
    for i, c in enumerate(cols + cols[:2]):
        top = y0 + ((i * 37) % 55)
        s += f'<rect x="{x0 + i * step}" y="{top}" width="{step - 4}" height="{y1 - top}" rx="{(step - 4) / 2}" fill="{c}"/>'
    return s


def skyline(kind, y, fill):
    """A city skyline along the foot of the bus (Live & Breathe, Above & Beyond)."""
    w, h = BOX[kind]
    r = random.Random(7)
    pts, x = [f"0,{h}", f"0,{y + 30}"], 0
    while x < w:
        bw, bh = r.randint(18, 46), r.randint(0, 46)
        pts += [f"{x},{y + 30 - bh}", f"{x + bw},{y + 30 - bh}"]
        x += bw
    pts += [f"{w},{y + 30}", f"{w},{h}"]
    return f'<polygon points="{" ".join(pts)}" fill="{fill}"/>'


DESIGNS = {}


def add(key, **design):
    assert key.endswith(("-dd", "-sd")), key
    design.setdefault("kind", key[-2:])
    DESIGNS[key] = design


# ── Brighton & Hove ─────────────────────────────────────────────────────────
TEAL, DTEAL = "#45cbcf", "#0b636b"


def bh_teal(kind, extra=""):
    w, h = BOX[kind]
    base = h * 0.78
    return svg(kind,
               f'<path d="M0,{h * 0.65} C{w * 0.11},{h * 0.68} {w * 0.15},{base} {w * 0.28},{base} '
               f'L{w},{base} L{w},{h} L0,{h} Z" fill="{DTEAL}"/>'
               f'<path d="M{w * 0.92},{h * 0.72} L{w},{h * 0.7} L{w},{h} L{w * 0.89},{h} Z" fill="{DTEAL}"/>'
               + figures(18, h * 0.49, h * 0.85) + extra, TEAL)


add("bh-teal-dd", svg=bh_teal("dd"))
add("bh-teal-sd", svg=bh_teal("sd"))
add("bh-teal-1x-dd", svg=bh_teal("dd", pill(150, 118, 430, 58, "#ffffff") + pill(150, 118, 200, 58, "#e5336d", "1X")))
add("bh-teal-3x-dd", svg=bh_teal("dd", pill(100, 118, 480, 58, "#ffffff") + pill(470, 118, 110, 58, "#1f73d8", "3X")))
LB = "#72cbd2"
add("bh-live-breathe-dd", svg=svg("dd", skyline("dd", 300, "#1b435c")
                                  + '<circle cx="610" cy="148" r="30" fill="#f29a1f"/><circle cx="598" cy="140" r="11" fill="#ffd25a"/>', LB))
add("bh-above-beyond-dd", svg=svg("dd", skyline("dd", 300, "#1b435c")
                                  + '<ellipse cx="520" cy="150" rx="70" ry="24" fill="#e9f7f8"/>'
                                    '<circle cx="600" cy="148" r="31" fill="#ffffff"/><circle cx="600" cy="148" r="24" fill="#d9826b"/>', LB))
add("bh-coaster-local-dd", svg=svg("dd",
    '<path d="M0,318 C200,300 380,336 560,318 S900,300 1081,318 L1081,387 L0,387 Z" fill="#2fa9e6"/>'
    '<path d="M0,332 C200,314 380,350 560,332 S900,314 1081,332 L1081,387 L0,387 Z" fill="#0a4d9e"/>', "#8bd23b"))
add("bh-red-dd", svg=svg("dd", '<rect y="372" width="1081" height="15" fill="#9e1a14"/>', "#d9261c"))
add("bh-albion-dd", svg=svg("dd", '<rect y="320" width="1081" height="67" fill="#1a2659"/>'
                                  '<circle cx="905" cy="148" r="28" fill="#ffffff"/><circle cx="905" cy="148" r="20" fill="#005cbf"/>', "#0a5cc0"))
add("bh-albion-foundation-dd", svg=svg("dd", "".join(
    f'<rect x="{x}" y="186" width="44" height="160" fill="#0058c5"/>' for x in range(0, 990, 88))
    + '<rect y="346" width="1081" height="41" fill="#0058c5"/><rect width="1081" height="20" fill="#0058c5"/>', "#ffffff"))
add("bh-pride-dd", svg=svg("dd", "".join(
    f'<path d="M0,{120 + i * 30} C300,{60 + i * 30} 700,{200 + i * 30} 1081,{110 + i * 30} L1081,{140 + i * 30} '
    f'C700,{230 + i * 30} 300,{90 + i * 30} 0,{150 + i * 30} Z" fill="{c}"/>'
    for i, c in enumerate(["#e40303", "#ff8c00", "#ffed00", "#008026", "#004dff", "#750787"])), "#2b9ad6"))
add("bh-beach-dd", svg=svg("dd",
    '<circle cx="150" cy="130" r="58" fill="#ffc21a"/><path d="M0,300 L1081,300 L1081,387 L0,387Z" fill="#0075bb"/>'
    + "".join(f'<path d="M{x},300 q25,-18 50,0 t50,0" stroke="#ffffff" stroke-width="7" fill="none"/>' for x in range(0, 1081, 100))
    + '<ellipse cx="760" cy="150" rx="90" ry="34" fill="#ff7aa8"/>', "#58c0ff"))
add("bh-breeze-downs-dd", svg=svg("dd",
    '<path d="M0,250 C150,170 300,210 450,240 S750,170 1081,230 L1081,387 L0,387Z" fill="#8cc63e"/>'
    '<path d="M0,300 C200,250 380,300 560,290 S900,250 1081,290 L1081,387 L0,387Z" fill="#5a9442"/>'
    '<circle cx="840" cy="140" r="36" fill="#ffd801"/>', "#46c9cf"))
add("bh-amex-dd", svg=svg("dd", "".join(
    f'<path d="M0,{150 + i * 45} C250,{100 + i * 45} 500,{220 + i * 45} 1081,{130 + i * 45}" stroke="#3f63d6" stroke-width="10" fill="none"/>'
    for i in range(5)), "#1b2a8c"))
add("bh-fun-brighton-dd", svg=svg("dd",
    '<path d="M0,180 C200,120 380,260 560,180 S900,120 1081,200 L1081,260 C900,190 700,320 520,240 S200,190 0,250Z" fill="#f3f0a8"/>'
    '<path d="M0,330 C250,300 500,370 1081,320 L1081,387 L0,387Z" fill="#0058c5"/>', "#f37fd8"))
add("bh-getbusy-arts-dd", svg=svg("dd", "".join(
    f'<circle cx="{40 + (i * 97) % 1060}" cy="{40 + (i * 53) % 330}" r="{14 + (i * 7) % 22}" stroke="#111" stroke-width="7" fill="none"/>'
    for i in range(46)), "#f4f4f4"))
add("bh-getbusy-fitter-dd", svg=svg("dd", "".join(
    f'<circle cx="{40 + (i * 97) % 1060}" cy="{40 + (i * 53) % 330}" r="{10 + (i * 7) % 18}" fill="{c}"/>'
    for i, c in enumerate(["#e6007e", "#f39200", "#ffd400", "#7ab929", "#009fe3", "#7d4e9e"] * 5)), "#f7f7f7"))
add("bh-martin-fisher-dd", svg=svg("dd",
    '<path d="M0,200 C200,140 420,300 640,210 S950,150 1081,220 L1081,300 C900,250 700,330 520,290 S200,230 0,290Z" fill="#623a5c"/>'
    '<path d="M0,340 C300,320 600,370 1081,330 L1081,387 L0,387Z" fill="#3c5b95"/>', "#d9739f"))
add("bh-housing-trust-dd", svg=svg("dd",
    '<rect y="186" width="1081" height="201" fill="#d1a045"/>' + figures(200, 200, 330, 26) + figures(560, 210, 330, 26), "#f3eebd"))


def spirit(kind):
    w, h = BOX[kind]
    return svg(kind,
               f'<path d="M0,{h} C{w * 0.35},{h} {w * 0.55},{h * 0.45} {w * 0.95},{h * 0.35} L{w * 0.97},{h * 0.38} '
               f'C{w * 0.6},{h * 0.5} {w * 0.4},{h} {w * 0.25},{h} Z" fill="#cc3f2c"/>'
               f'<path d="M0,{h} C{w * 0.33},{h} {w * 0.52},{h * 0.5} {w * 0.92},{h * 0.4} '
               f'C{w * 0.6},{h * 0.6} {w * 0.4},{h} {w * 0.2},{h} Z" fill="#a7db48"/>', "#164936")


add("bh-spirit-of-sussex-dd", svg=spirit("dd"))
add("bh-spirit-of-sussex-sd", svg=spirit("sd"))


# ── Metrobus ────────────────────────────────────────────────────────────────
def mb_old(kind):
    w, h = BOX[kind]
    return svg(kind,
               f'<path d="M{w * 0.9},{h} C{w * 0.93},{h * 0.6} {w * 0.95},{h * 0.55} {w},{h * 0.55} L{w},{h} Z" fill="#1d4ea3"/>'
               f'<path d="M{w * 0.25},{h} C{w * 0.27},{h * 0.75} {w * 0.29},{h * 0.55} {w * 0.31},{h * 0.35}" '
               f'stroke="#ffe02e" stroke-width="13" fill="none"/>', "#3aa3ea")


def mb_new(kind, hydrogen=False):
    w, h = BOX[kind]
    y0 = h * 0.5 if kind == "dd" else h * 0.45
    s = lean(w * 0.06, w * 0.05, w * 0.07, y0, h, "#1e2e66") + lean(w * 0.11, w * 0.04, w * 0.07, y0, h, "#ffe14d")
    if hydrogen:
        s += lean(w * 0.15, w * 0.035, w * 0.07, y0, h, "#13c47e")
    s += f'<path d="M{w * 0.93},{h} L{w * 0.96},{h * 0.62} L{w},{h * 0.6} L{w},{h}Z" fill="#3fbfff"/>'
    if kind == "dd":
        s += lean(w * 0.03, w * 0.04, w * 0.05, 0, h * 0.08, "#ffe14d")
    return svg(kind, s, "#2a7fd8")


for kind in ("dd", "sd"):
    add(f"mb-{kind}", svg=mb_old(kind))
    add(f"mb-new-{kind}", svg=mb_new(kind))
    add(f"mb-hydrogen-{kind}", svg=mb_new(kind, hydrogen=True))
W, H = BOX["sd"]
add("mb-fastway-blue-sd", svg=svg("sd", f'<rect width="{W}" height="58" fill="#1e3a7a"/>'
    + lean(W * 0.05, W * 0.035, W * 0.06, H * 0.45, H, "#13c47e") + lean(W * 0.085, W * 0.03, W * 0.06, H * 0.45, H, "#3fbfff")
    + lean(W * 0.11, W * 0.025, W * 0.06, 0, 58, "#13c47e")
    + f'<path d="M{W * 0.9},{H} C{W * 0.95},{H * 0.75} {W * 0.97},{H * 0.7} {W},{H * 0.7} L{W},{H}Z" fill="#13c47e"/>', "#2a5fb4"))
add("mb-fastway-silver-sd", svg=svg("sd",
    f'<path d="M0,0 L{W * 0.3},0 C{W * 0.42},{H * 0.3} {W * 0.42},{H * 0.7} {W * 0.3},{H} L0,{H}Z" fill="#2244aa"/>'
    f'<path d="M{W * 0.3},0 C{W * 0.42},{H * 0.3} {W * 0.42},{H * 0.7} {W * 0.3},{H}" stroke="#ffffff" stroke-width="16" fill="none"/>'
    f'<path d="M{W * 0.9},0 L{W},0 L{W},{H} L{W * 0.88},{H} C{W * 0.93},{H * 0.6} {W * 0.93},{H * 0.3} {W * 0.9},0Z" fill="#2244aa"/>', "#b9bcc2"))
# One-offs, in bustimes.org's drawing of each (its "right" CSS).
add("mb-hiring-sd", css="radial-gradient(circle,#e0d60b 35%,#000 35% 45%,#0000 45%),linear-gradient(270deg,#e0790b 50%,#e0d60b 50%)")
add("mb-classic-sd", css="linear-gradient(#104fce 25%,#fde800 25% 75%,#104fce 75%)")
add("mb-southdown-sd", css="linear-gradient(#62cfbc 33.4%,#fff 33.4% 66.7%,#226fbd 66.7%)")
add("mb-red-sd", css="#dc241f")
add("mb-st-catherines-dd", css="radial-gradient(circle at 65% 60%,#eec73a 25%,#0000 25%),conic-gradient(from 270deg at 65%,#e6702c 270deg,#3fbeff 270deg)")
add("mb-morebus-sd", css="radial-gradient(130% 145% at 90% 25%,#0000 60%,#e30315 60%),radial-gradient(circle at 0 140%,#1b399c 92%,#e30315 92%)")
add("mb-carousel-sd", css="linear-gradient(300deg,#ee1d23 35%,#f6ac10 36% 38%,#fff 39% 41%,#e30715 42% 44%,#f7e74a 45% 47%,#ee1d23 48%)")
add("mb-archway-sd", css="conic-gradient(from 180deg at 80% 100%,#fff 105deg,#0000 105deg),linear-gradient(75deg,#fff 20%,#e52421 20% 29%,#0000 29%),conic-gradient(from 270deg at 80% 100%,#e52421 23deg,#fff 23deg)")
add("mb-warrington-sd", css="linear-gradient(285deg,#e10d17 34%,#0357a3 34%)")
add("mb-getbusy-beach-dd", css="linear-gradient(270deg,#f4a0e8 7%,#a7f7f4 7%,#a7f7f4 14%,#f4a0e8 14%,#f4a0e8 20%,#a7f7f4 20%,#a7f7f4 27%,#ffffff 27%,#ffffff 34%,#f4a0e8 34%,#f4a0e8 40%,#f3f7a7 40%,#f3f7a7 47%,#f4a0e8 47%,#f4a0e8 54%,#f3f7a7 54%,#f3f7a7 60%,#f4a0e8 60%,#f4a0e8 67%,#f3f7a7 67%,#f3f7a7 74%,#a7f7f4 74%,#a7f7f4 94%,#ffffff 94%)")
add("mb-pride-dd", css="linear-gradient(270deg,#F2EEE1 6%,#FC3133 6%,#FC3133 17%,#F2EEE1 17%,#F2EEE1 23%,#F8A743 23%,#F8A743 28%,#2D1D78 28%,#2D1D78 39%,#662466 39%,#662466 50%,#000000 50%,#000000 56%,#705950 56%,#705950 62%,#9ABDD8 62%,#9ABDD8 67%,#FF483A 67%,#FF483A 73%,#F296AE 73%,#F296AE 78%,#EBED5E 78%,#EBED5E 84%,#49C766 84%,#49C766 89%,#26256F 89%,#26256F 95%,#FA9736 95%)")


# ── Stagecoach ──────────────────────────────────────────────────────────────
def sc_local(kind):
    w, h = BOX[kind]
    return svg(kind,
               f'<path d="M0,0 L{w * 0.07},0 C{w * 0.02},{h * 0.3} {w * 0.02},{h * 0.7} {w * 0.08},{h} L0,{h}Z" fill="#f7a823"/>'
               f'<path d="M{w * 0.07},0 L{w * 0.13},0 C{w * 0.07},{h * 0.35} {w * 0.08},{h * 0.7} {w * 0.15},{h} L{w * 0.08},{h} '
               f'C{w * 0.02},{h * 0.7} {w * 0.02},{h * 0.3} {w * 0.07},0Z" fill="#00957a"/>'
               f'<path d="M{w * 0.92},0 L{w},0 L{w},{h} L{w * 0.86},{h} C{w * 0.93},{h * 0.7} {w * 0.95},{h * 0.35} {w * 0.92},0Z" fill="#14b4ef"/>'
               f'<rect y="{h * 0.93}" width="{w}" height="{h * 0.07}" fill="#14b4ef"/>', "#ffffff")


def sc_beachball(kind):
    w, h = BOX[kind]
    return svg(kind,
               f'<rect y="{h * 0.86}" width="{w}" height="{h * 0.14}" fill="#1d6fb8"/>'
               f'<path d="M0,0 L{w * 0.1},0 C{w * 0.05},{h * 0.35} {w * 0.12},{h * 0.7} {w * 0.3},{h * 0.86} L0,{h * 0.86}Z" fill="#f39b3c"/>'
               f'<path d="M0,0 L{w * 0.05},0 C{w * 0.01},{h * 0.35} {w * 0.07},{h * 0.7} {w * 0.22},{h * 0.86} L0,{h * 0.86}Z" fill="#e5283b"/>',
               "#ffffff")


def sc_coastliner(kind):
    w, h = BOX[kind]
    return svg(kind,
               f'<path d="M0,{h * 0.3} C{w * 0.12},{h * 0.35} {w * 0.2},{h * 0.7} {w * 0.22},{h} L0,{h}Z" fill="#1d8bd9"/>'
               f'<path d="M{w * 0.1},{h} C{w * 0.13},{h * 0.75} {w * 0.2},{h * 0.55} {w * 0.3},{h * 0.5} '
               f'C{w * 0.24},{h * 0.62} {w * 0.22},{h * 0.8} {w * 0.23},{h}Z" fill="#1d8bd9"/>'
               f'<path d="M{w * 0.93},0 L{w},0 L{w},{h} L{w * 0.6},{h} C{w * 0.8},{h * 0.9} {w * 0.9},{h * 0.5} {w * 0.93},0Z" fill="#1d8bd9"/>',
               "#ffffff")


def sc_wegotyou(kind):
    """Navy with the swirl at the back. The single-decker is the drawn master
    icons/source/SCSO-17.png itself."""
    w, h = BOX[kind]
    cx, cy, r = w * 0.08, h * 0.55, h * 0.3
    return svg(kind,
               f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="#f7a823"/><circle cx="{cx + r * 0.25}" cy="{cy}" r="{r * 0.82}" fill="#00a69c"/>'
               f'<circle cx="{cx + r * 0.15}" cy="{cy + r * 0.1}" r="{r * 0.6}" fill="#1f6fd1"/>'
               f'<circle cx="{cx + r * 0.2}" cy="{cy}" r="{r * 0.42}" fill="#1e2a55"/>', "#1e2a55")


for kind in ("dd", "sd"):
    add(f"sc-local-{kind}", svg=sc_local(kind))
    add(f"sc-beachball-{kind}", svg=sc_beachball(kind))
add("sc-coastliner-dd", svg=sc_coastliner("dd"))
add("sc-wegotyou-dd", svg=sc_wegotyou("dd"))
add("sc-think-brain-health-dd", svg=svg("dd", '<path d="M0,180 L960,180 C990,250 990,330 970,387 L0,387Z" fill="#7a1f8a"/>', "#ffffff"))
add("sc-pride-dd", css="radial-gradient(82% 122% at 25%,#0000 60%,#fff 60% 87%,#07c4ff 87%),radial-gradient(65% 105% at 59% 40%,#fff0 80%,#fff 80% 85%,#0000 85%),conic-gradient(from 140deg at 12% 84%,#5a0066 130deg,#0000 130deg),conic-gradient(from 150deg at 7% 67%,#092a7e 120deg,#0000 120deg),conic-gradient(from 180deg at 7%,#2ab528 90deg,#0000 90deg),conic-gradient(from 180deg at 7% 34%,#e2ed11 90deg,#0000 90deg),radial-gradient(60% 110% at 30% 65%,#fff0 45%,#fff 45% 50%,#0000 45%),conic-gradient(from -135deg at 17% 17%,#f79216 45deg,#f03a22 45deg 220deg,#0000 215deg),radial-gradient(60% 115% at 10% 45%,#fff 55%,#0000 55%),conic-gradient(from 45deg at 68%,#ffafc7 90deg,#0000 90deg),conic-gradient(from 45deg at 60%,#73d7ee 90deg,#0000 90deg),conic-gradient(from 45deg at 52%,#58483a 90deg,#0000 90deg),conic-gradient(from 45deg at 44%,#000 90deg,#0000 90deg),linear-gradient(#f03a22 17%,#f79216 17% 34%,#e2ed11 34% 50%,#2ab528 50% 67%,#092a7e 67% 84%,#5a0066 84%)")
add("sc-cant-miss-us-dd", css="linear-gradient(270deg,#25283e 6%,#5582be 6% 12%,#25283e 12% 17%,#db954b 17% 23%,#25283e 23% 28%,#3e9a93 28% 34%,#25283e 34% 39%,#bb4d57 39% 45%,#25283e 45% 50%,#d59551 50% 56%,#25283e 56% 67%,#00877d 67% 73%,#d59551 73% 78%,#25283e 78% 84%,#d59551 84% 89%,#25283e 89% 95%,#d59551 95%)")


# ── Compass ─────────────────────────────────────────────────────────────────
def compass(kind):
    w, h = BOX[kind]
    return svg(kind,
               f'<rect y="{h * 0.8}" width="{w}" height="{h * 0.2}" fill="#7a0f2a"/>'
               f'<path d="M0,0 L{w * 0.06},0 L{w * 0.13},{h * 0.8} L0,{h * 0.8}Z" fill="#7a0f2a"/>'
               f'<path d="M{w * 0.075},0 L{w * 0.145},{h * 0.74} L{w},{h * 0.74}" stroke="#d4af37" stroke-width="9" fill="none"/>',
               "#ffffff")


for kind in ("dd", "sd"):
    add(f"compass-{kind}", svg=compass(kind))
    add(f"white-{kind}", svg=svg(kind, "", "#f6f6f6"))


def jobs():
    """What render_liveries.mjs draws: one flat PNG per livery. SVG at the
    paint box's own size; bustimes.org CSS in its own 3:2 box, stretched to
    the paint box when the icon is built, as bustimes.org draws it."""
    out = []
    for key, d in DESIGNS.items():
        w, h = BOX[d["kind"]]
        if "svg" in d:
            out.append({"key": key, "svg": d["svg"], "w": w, "h": h})
        else:
            out.append({"key": key, "css": d["css"], "w": 600, "h": 400})
    return out


if __name__ == "__main__":
    if "--jobs" in sys.argv:
        json.dump(jobs(), sys.stdout)
    else:
        print("\n".join(f"{k}  {d['kind']}  {'svg' if 'svg' in d else 'bustimes css'}" for k, d in DESIGNS.items()))
