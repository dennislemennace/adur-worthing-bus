#!/usr/bin/env python3
"""A photo for each Discover place, free to use, with its credit.

Discover's places (data/destinations.json) show a small photo in the list and
a larger one when opened (asked for 7 November 2026). Each comes from
Wikimedia Commons under a licence that allows reuse with credit: public
domain, CC0, CC BY or CC BY-SA, never a non-commercial or no-derivatives one.
The credit (author, licence, the file's page) goes into the place's `photo`
and is shown under the photo and on about.html.

Which photo: the one Wikidata names as the place's image (P18), the place
found through its OpenStreetMap object's `wikidata` tag. A place can name a
Commons file by hand (`photo_file`) where that image is not the best of it.
The images are saved here, scaled and cropped, so the site makes no request
to Wikimedia: images/discover/<id>.jpg (720 wide) and <id>-thumb.jpg (a
160 pixel square).

    python scripts/fetch_destination_photos.py [--refresh] [--only id,id]

One request each to Nominatim (the OSM tags, all places at once), Wikidata
and the Commons API, then one image download a place: about twenty-five
requests a run, with a User-Agent as Wikimedia's policy asks (LIMITS.md).
"""

import argparse
import html
import io
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

from PIL import Image, ImageOps

ROOT = Path(__file__).resolve().parent.parent
DEST = ROOT / "data" / "destinations.json"
OUT = ROOT / "images" / "discover"
UA = {"User-Agent": "adur-worthing-bus/1.0 (Discover photos, run by hand; "
                    "+https://github.com/dennislemennace/adur-worthing-bus)"}
FREE = re.compile(r"^(public domain|pd\b|cc0|cc[ -]by(-sa)?[ -]\d(\.\d)?)", re.I)
LARGE_W, THUMB = 720, 160


def get_json(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def get_bytes(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=120) as resp:
        return resp.read()


def osm_wikidata(places):
    """{place id: Q-id} from each place's OpenStreetMap object, in one request."""
    ids = {}
    for p in places:
        kind, _, num = (p.get("osm") or "").partition("/")
        if kind and num:
            ids[f"{kind[0].upper()}{num}"] = p["id"]
    if not ids:
        return {}
    q = urllib.parse.urlencode({"osm_ids": ",".join(ids), "format": "json", "extratags": 1})
    found = get_json(f"https://nominatim.openstreetmap.org/lookup?{q}")
    out = {}
    for hit in found:
        key = f"{hit.get('osm_type', ' ')[0].upper()}{hit.get('osm_id')}"
        qid = (hit.get("extratags") or {}).get("wikidata")
        if key in ids and qid:
            out[ids[key]] = qid
    return out


def wikidata_images(qids):
    """{Q-id: Commons file name} from each item's image (P18)."""
    if not qids:
        return {}
    q = urllib.parse.urlencode({"action": "wbgetentities", "ids": "|".join(sorted(set(qids))),
                                "props": "claims", "format": "json"})
    doc = get_json(f"https://www.wikidata.org/w/api.php?{q}")
    out = {}
    for qid, ent in (doc.get("entities") or {}).items():
        claims = (ent.get("claims") or {}).get("P18") or []
        if claims:
            out[qid] = claims[0]["mainsnak"]["datavalue"]["value"]
    return out


def plain(text):
    """Commons' metadata is HTML: the words only."""
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", text or ""))).strip()


def commons_info(files):
    """{file name: {url, author, licence, licence_url, page}} from the Commons API."""
    titles = [f"File:{f}" for f in files]
    q = urllib.parse.urlencode({"action": "query", "titles": "|".join(titles), "prop": "imageinfo",
                                "iiprop": "url|extmetadata", "iiurlwidth": 1200, "format": "json"})
    doc = get_json(f"https://commons.wikimedia.org/w/api.php?{q}")
    norm = {n["to"]: n["from"] for n in (doc.get("query", {}).get("normalized") or [])}
    out = {}
    for page in (doc.get("query", {}).get("pages") or {}).values():
        info = (page.get("imageinfo") or [None])[0]
        if not info:
            continue
        meta = info.get("extmetadata") or {}
        title = norm.get(page["title"], page["title"])
        out[title.removeprefix("File:")] = {
            "url": info.get("thumburl") or info["url"],
            "author": plain((meta.get("Artist") or {}).get("value")) or "Unknown author",
            "licence": plain((meta.get("LicenseShortName") or {}).get("value")),
            "licence_url": plain((meta.get("LicenseUrl") or {}).get("value")),
            "page": info.get("descriptionurl"),
        }
    return out


def save_images(data, place_id):
    """The large image (720 wide, at most 3:2 tall) and a centred square thumb."""
    img = ImageOps.exif_transpose(Image.open(io.BytesIO(data))).convert("RGB")
    w, h = img.size
    if h > w * 2 / 3:                                   # a tall photo: its middle, 3:2
        top = (h - round(w * 2 / 3)) // 2
        img = img.crop((0, top, w, top + round(w * 2 / 3)))
    large = img.resize((LARGE_W, round(img.height * LARGE_W / img.width)), Image.LANCZOS)
    OUT.mkdir(parents=True, exist_ok=True)
    large.save(OUT / f"{place_id}.jpg", "JPEG", quality=78, optimize=True, progressive=True)
    thumb = ImageOps.fit(img, (THUMB, THUMB), Image.LANCZOS)
    thumb.save(OUT / f"{place_id}-thumb.jpg", "JPEG", quality=80, optimize=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--refresh", action="store_true", help="fetch again for places that have a photo")
    ap.add_argument("--only", help="comma-separated place ids")
    args = ap.parse_args()
    doc = json.loads(DEST.read_text(encoding="utf-8"))
    only = set(args.only.split(",")) if args.only else None
    todo = [p for p in doc["places"] if (args.refresh or not p.get("photo")) and (not only or p["id"] in only)]
    qids = osm_wikidata([p for p in todo if not p.get("photo_file") and not p.get("wikidata")])
    for p in todo:
        if p.get("wikidata"):
            qids[p["id"]] = p["wikidata"]
    time.sleep(1.1)
    images = wikidata_images(list(qids.values()))
    files = {p["id"]: p.get("photo_file") or images.get(qids.get(p["id"])) for p in todo}
    info = commons_info([f for f in files.values() if f])
    problems = []
    for p in todo:
        f = files[p["id"]]
        meta = info.get(f) if f else None
        if not meta:
            problems.append(f"{p['id']}: no photo found (wikidata {qids.get(p['id'])}, file {f})")
            continue
        if not FREE.match(meta["licence"] or ""):
            problems.append(f"{p['id']}: {f} is {meta['licence']!r}, not a licence we can use")
            continue
        save_images(get_bytes(meta["url"]), p["id"])
        time.sleep(0.5)
        p["photo"] = {"src": f"images/discover/{p['id']}.jpg", "thumb": f"images/discover/{p['id']}-thumb.jpg",
                      "file": f, "author": meta["author"], "licence": meta["licence"],
                      "licence_url": meta["licence_url"], "source_url": meta["page"],
                      "alt": p.get("photo", {}).get("alt", "")}
        print(f"  {p['id']}: {f} ({meta['licence']}, {meta['author']})")
    DEST.write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    for line in problems:
        print("  " + line, file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
