#!/usr/bin/env python3
"""Draw the QR code the tour's slides and PDF print, as an SVG.

Run by hand when the address changes, not in a workflow: the output is a
committed file (media/tour/qr-tour.svg), so the site gains no dependency.
Needs segno (pure Python, BSD), installed into the venv for this script only:

    .venv/bin/python -m pip install segno
    .venv/bin/python scripts/build_tour_qr.py

Error correction "M" survives a scuffed print; the quiet zone is the four
modules a phone camera needs to find the code.
"""

import sys
from pathlib import Path

import segno

ROOT = Path(__file__).resolve().parent.parent
URL = "https://worthingbrightonbus.co.uk/#view=tour"
OUT = ROOT / "media" / "tour" / "qr-tour.svg"


def main(argv=None):
    url = (argv or sys.argv[1:] or [URL])[0]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    qr = segno.make(url, error="m")
    qr.save(str(OUT), kind="svg", scale=8, border=4, dark="#12202f", light="#ffffff",
            title="QR code for the Worthing Brighton Bus tour", xmldecl=False, svgclass="tour-qr")
    print(f"{OUT.relative_to(ROOT)}: {url} (version {qr.version}, {qr.error})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
