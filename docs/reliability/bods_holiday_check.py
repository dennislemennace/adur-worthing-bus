#!/usr/bin/env python3
"""Reproduce the 2026 holiday faults in BODS's regional GTFS. A diagnostic, not part of the build.

    python docs/reliability/bods_holiday_check.py south_east.zip

The zip is BODS's regional GTFS
(https://data.bus-data.dft.gov.uk/timetable/download/gtfs-file/south_east/).

1. Trips scheduled on Tuesday 1 December 2026 against Tuesday 8 December, by operator.
2. Among calendars whose range covers 1 to 31 December, how each treats 1, 24
   and 31 December. A calendar that skips New Year's Eve also skips Christmas Eve,
   so if New Year's Eve were converted correctly, 31 December would pair with 24
   December. If 1 December pairs with it instead, outside school-holiday runs
   (29 and 30 December), New Year's Eve has been put on 1 December.
"""
import csv
import io
import sys
import zipfile
from collections import Counter, defaultdict
from datetime import date

WEEKDAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")


def rows(zf, name):
    with zf.open(name) as handle:
        yield from csv.DictReader(io.TextIOWrapper(handle, encoding="utf-8-sig"))


def main(path):
    zf = zipfile.ZipFile(path)
    print("feed:", next(rows(zf, "feed_info.txt"), {}).get("feed_version"))
    noc = {r["agency_id"]: r.get("agency_noc") or r["agency_name"] for r in rows(zf, "agency.txt")}
    route_noc = {r["route_id"]: noc.get(r["agency_id"], "?") for r in rows(zf, "routes.txt")}
    trips = defaultdict(Counter)
    for r in rows(zf, "trips.txt"):
        trips[r["service_id"]][route_noc.get(r["route_id"], "?")] += 1
    cal = {r["service_id"]: r for r in rows(zf, "calendar.txt")}
    exc = defaultdict(dict)
    for r in rows(zf, "calendar_dates.txt"):
        exc[r["service_id"]][r["date"]] = r["exception_type"]

    def runs(sid, stamp):
        if stamp in exc[sid]:
            return exc[sid][stamp] == "1"
        c = cal.get(sid)
        d = date(int(stamp[:4]), int(stamp[4:6]), int(stamp[6:]))
        return bool(c) and c["start_date"] <= stamp <= c["end_date"] and c[WEEKDAYS[d.weekday()]] == "1"

    per = defaultdict(lambda: [0, 0])
    for sid, by in trips.items():
        on1, on8 = runs(sid, "20261201"), runs(sid, "20261208")
        for op, n in by.items():
            per[op][0] += n * on1
            per[op][1] += n * on8
    big = sorted((v[0] / v[1], op, v) for op, v in per.items() if v[1] >= 200)
    print(f"\n{sum(1 for s, _, _ in big if s < 0.5)} of {len(big)} operators with 200+ trips on "
          "Tue 8 Dec lose over half of them on Tue 1 Dec")
    for share, op, (n1, n8) in big:
        print(f"  {op:6} 1 Dec {n1:6}  8 Dec {n8:6}  {share:5.0%}")

    tab = Counter()
    for sid in trips:
        c = cal.get(sid)
        if not c or not (c["start_date"] <= "20261201" and c["end_date"] >= "20261231"):
            continue
        e = exc[sid]
        school = e.get("20261229") == "2" or e.get("20261230") == "2"
        tab[(e.get("20261201", "-"), e.get("20261224", "-"), e.get("20261231", "-"), school)] += 1
    print("\nCalendars covering December, by exception on (1 Dec, 24 Dec, 31 Dec), 1=added 2=removed:")
    for (d1, d24, d31, school), n in tab.most_common(12):
        print(f"  ({d1}, {d24}, {d31}){' with a school-holiday run' if school else ''}: {n}")
    rem24 = sum(n for k, n in tab.items() if k[1] == "2")
    with1 = sum(n for k, n in tab.items() if k[1] == "2" and k[0] == "2")
    with31 = sum(n for k, n in tab.items() if k[1] == "2" and k[2] == "2" and not k[3])
    print(f"\nOf {rem24} that skip Christmas Eve: {with1} also skip 1 December; "
          f"{with31} also skip 31 December outside a school-holiday run.")


if __name__ == "__main__":
    main(sys.argv[1])
